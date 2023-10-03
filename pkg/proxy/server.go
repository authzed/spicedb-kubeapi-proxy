package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"golang.org/x/sync/errgroup"
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
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/authz"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/authz/distributedtx"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

type Server struct {
	opts           Options
	Handler        http.Handler
	WorkflowWorker *distributedtx.Worker
	KubeClient     *kubernetes.Clientset
	Matcher        *rules.Matcher
}

func NewServer(ctx context.Context, o Options) (*Server, error) {
	s := &Server{
		opts: o,
	}

	var err error
	var clusterHost string
	if s.opts.RestConfigFunc == nil {
		return nil, fmt.Errorf("missing kube client REST configuration")
	}

	restConfig, transport, err := s.opts.RestConfigFunc()
	if err != nil {
		return nil, fmt.Errorf("unable to load kube REST config: %w", err)
	}

	s.KubeClient, err = kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}

	clusterHost = restConfig.Host
	klog.FromContext(ctx).WithValues("host", clusterHost).Error(err, "created upstream client")

	mux := http.NewServeMux()

	mux.Handle("/readyz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
		w.WriteHeader(http.StatusOK)
	}))

	mux.Handle("/livez", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
		w.WriteHeader(http.StatusOK)
	}))

	clusterProxy := &httputil.ReverseProxy{
		ErrorLog:      nil, // TODO
		FlushInterval: -1,
		Director: func(req *http.Request) {
			req.URL.Host = strings.TrimPrefix(clusterHost, "https://")
			req.URL.Scheme = "https"
		},
		ModifyResponse: func(response *http.Response) error {
			authzData, ok := authz.AuthzDataFrom(response.Request.Context())
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

	workflowClient, worker, err := distributedtx.SetupWithSQLiteBackend(ctx,
		s.opts.PermissionsClient,
		s.KubeClient.RESTClient(),
		o.WorkflowDatabasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize distributed transaction handling: %w", err)
	}
	s.WorkflowWorker = worker

	s.Matcher = &s.opts.Matcher
	handler, err := authz.WithAuthorization(clusterProxy, failHandler, o.PermissionsClient, o.WatchClient, workflowClient, s.Matcher)
	if err != nil {
		return nil, fmt.Errorf("unable to create authorization handler: %w", err)
	}

	handler = withAuthentication(handler, failHandler, s.opts.AuthenticationInfo.Authenticator)
	handler = genericapifilters.WithRequestInfo(handler, requestInfoResolver)
	handler = genericfilters.WithHTTPLogging(handler)
	handler = genericfilters.WithPanicRecovery(handler, requestInfoResolver)
	// TODO: withpriorityandfairness

	mux.Handle("/", handler)
	s.Handler = mux

	return s, nil
}

func (s *Server) PermissionClient() v1.PermissionsServiceClient {
	return s.opts.PermissionsClient
}

func (s *Server) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var g errgroup.Group

	if s.opts.EmbeddedSpiceDB != nil {
		g.Go(func() error {
			return s.opts.EmbeddedSpiceDB.Run(ctx)
		})
	}
	g.Go(func() error {
		return s.WorkflowWorker.Start(ctx)
	})

	g.Go(func() error {
		done, _, err := s.opts.ServingInfo.Serve(s.Handler, time.Second*60, ctx.Done())
		if err != nil {
			return err
		}
		<-done
		return nil
	})

	if err := g.Wait(); err != nil {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		if err := s.WorkflowWorker.Shutdown(ctx); err != nil {
			return err
		}
		return err
	}

	return nil
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
