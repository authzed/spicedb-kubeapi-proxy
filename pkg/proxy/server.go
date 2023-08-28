package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/klog/v2"
)

type Server struct {
	opts          Options
	Handler       http.Handler
	taskHubWorker backend.TaskHubWorker
	spiceDBClient v1.PermissionsServiceClient
}

func NewServer(ctx context.Context, o Options) (*Server, error) {
	s := &Server{opts: o}

	conn, err := o.SpicedbServer.GRPCDialContext(ctx, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	s.spiceDBClient = v1.NewPermissionsServiceClient(conn)
	watchClient := v1.NewWatchServiceClient(conn)

	// durabletask - stores planned dual writes in a sqlite db
	logger := backend.DefaultLogger()
	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN(WriteToSpiceDBAndKubeName, s.WriteToSpiceDBAndKube); err != nil {
		return nil, err
	}
	if err := r.AddActivity(s.WriteToSpiceDB); err != nil {
		return nil, err
	}
	if err := r.AddActivity(s.WriteToKube); err != nil {
		return nil, err
	}
	if err := r.AddActivity(s.CheckKube); err != nil {
		return nil, err
	}

	// note: can use the in-memory sqlite provider by specifying ""
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions("dtx.sqlite"), logger)
	executor := task.NewTaskExecutor(r)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	s.taskHubWorker = backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)
	taskHubClient := backend.NewTaskHubClient(be)

	mux := http.NewServeMux()

	mux.Handle("/readyz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
		w.WriteHeader(http.StatusOK)
	}))

	mux.Handle("/livez", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
		w.WriteHeader(http.StatusOK)
	}))

	transport, err := newTransportForKubeconfig(o.BackendConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	kubeCtx := o.BackendConfig.Contexts[o.BackendConfig.CurrentContext]
	cluster := o.BackendConfig.Clusters[kubeCtx.Cluster]

	clusterProxy := &httputil.ReverseProxy{
		ErrorLog:      nil, // TODO
		FlushInterval: -1,
		Director: func(req *http.Request) {
			req.URL.Host = strings.TrimPrefix(cluster.Server, "https://")
			req.URL.Scheme = "https"
		},
		ModifyResponse: func(response *http.Response) error {
			authzData, ok := AuthzDataFrom(response.Request.Context())
			if !ok {
				return fmt.Errorf("no authz data")
			}
			return authzData.FilterResp(response)
		},
		Transport: transport,
	}

	requestInfoResolver := &request.RequestInfoFactory{
		APIPrefixes: sets.NewString(
			strings.Trim(server.APIGroupPrefix, "/"),
			strings.Trim(server.DefaultLegacyAPIPrefix, "/"),
		),
		GrouplessAPIPrefixes: sets.NewString(
			strings.Trim(server.DefaultLegacyAPIPrefix, "/"),
		),
	}

	scheme := runtime.NewScheme()
	metav1.AddToGroupVersion(scheme, schema.GroupVersion{Group: "", Version: "v1"})
	codecs := serializer.NewCodecFactory(scheme)
	failHandler := genericapifilters.Unauthorized(codecs)

	handler := withAuthorization(clusterProxy, failHandler, s.spiceDBClient, watchClient, taskHubClient)
	handler = withAuthentication(handler, failHandler, s.opts.AuthenticationInfo.Authenticator)
	handler = genericapifilters.WithRequestInfo(handler, requestInfoResolver)
	handler = genericfilters.WithHTTPLogging(handler)
	handler = genericfilters.WithPanicRecovery(handler, requestInfoResolver)
	// TODO: withpriorityandfairness

	mux.Handle("/", handler)
	s.Handler = mux

	return s, nil
}

func (s *Server) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)

	// TODO: errgroup

	go func() {
		if err := s.opts.SpicedbServer.Run(ctx); err != nil {
			klog.FromContext(ctx).Error(err, "failed to run spicedb")
			cancel()
		}
	}()
	go func() {
		if err := s.taskHubWorker.Start(ctx); err != nil {
			klog.FromContext(ctx).Error(err, "failed to run durable task worker")
			cancel()
		}
	}()
	doneCh, _, err := s.opts.ServingInfo.Serve(s.Handler, time.Second*60, ctx.Done())
	if err != nil {
		return err
	}

	<-doneCh
	if err := s.taskHubWorker.Shutdown(context.Background()); err != nil {
		return err
	}
	return nil
}

func newTransportForKubeconfig(config *clientcmdapi.Config) (*http.Transport, error) {
	kubeCtx := config.Contexts[config.CurrentContext]
	cluster := config.Clusters[kubeCtx.Cluster]
	user := config.AuthInfos[kubeCtx.AuthInfo]

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(cluster.CertificateAuthorityData)

	cert, err := tls.X509KeyPair(user.ClientCertificateData, user.ClientKeyData)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate %q or key %q: %w", user.ClientCertificateData, user.ClientKeyData, err)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	return transport, nil
}

func withAuthentication(handler, failed http.Handler, auth authenticator.Request) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		resp, ok, err := auth.AuthenticateRequest(req)
		if err != nil || !ok {
			if err != nil {
				logger := klog.FromContext(req.Context())
				logger.Error(err, "Unable to authenticate the request")
			}
			failed.ServeHTTP(w, req)
			return
		}
		req = req.WithContext(request.WithUser(req.Context(), resp.User))
		handler.ServeHTTP(w, req)
	})
}
