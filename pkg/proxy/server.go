package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/api/meta"
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
	diskcached "k8s.io/client-go/discovery/cached/disk"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog/v2"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

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

func NewServer(ctx context.Context, c *CompletedConfig) (*Server, error) {
	if c == nil {
		return nil, fmt.Errorf("nil completed config")
	}

	s := &Server{
		opts: *c.config,
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
	restMapper, err := toRestMapper(restConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create REST mapper: %w", err)
	}

	clusterHost = restConfig.Host
	klog.FromContext(ctx).WithValues("host", clusterHost).Info("created upstream client")

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
			klog.V(3).InfoSDepth(1, "upstream Kubernetes API response",
				"status", response.StatusCode,
				"headers", response.Header)
			responseFilterer, ok := authz.ResponseFiltererFrom(response.Request.Context())
			if !ok {
				return fmt.Errorf("no authz data")
			}
			return responseFilterer.FilterResp(response)
		},
		Transport: transport,
		ErrorHandler: func(writer http.ResponseWriter, h *http.Request, err error) {
			klog.V(3).InfoSDepth(1, "upstream Kubernetes API response", "error", err)
			writer.WriteHeader(http.StatusBadGateway)
		},
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

	workflowClient, worker, err := distributedtx.SetupWithSQLiteBackend(ctx,
		s.opts.PermissionsClient,
		s.KubeClient.RESTClient(),
		c.config.WorkflowDatabasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize distributed transaction handling: %w", err)
	}
	s.WorkflowWorker = worker

	// Matcher is a pointer to an interface to make it easy to swap at runtime in tests
	s.Matcher = &s.opts.Matcher

	scheme := runtime.NewScheme()
	metav1.AddToGroupVersion(scheme, schema.GroupVersion{Group: "", Version: "v1"})
	codecs := serializer.NewCodecFactory(scheme)
	failHandler := genericapifilters.Unauthorized(codecs)

	handler := authz.WithAuthorization(clusterProxy, failHandler, restMapper, c.config.PermissionsClient, c.config.WatchClient, workflowClient, s.Matcher, s.opts.InputExtractor)
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

	if s.opts.SpiceDBOptions.EmbeddedSpiceDB != nil {
		g.Go(func() error {
			return s.opts.SpiceDBOptions.EmbeddedSpiceDB.Run(ctx)
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

			klog.V(3).InfoS("unable to authenticate client", "method", req.Method, "url", req.URL)
			failed.ServeHTTP(w, req)
			return
		}

		klog.V(4).InfoS("request client authenticated", "user", resp.User)
		req = req.WithContext(request.WithUser(req.Context(), resp.User))
		handler.ServeHTTP(w, req)
	})
}

// Note: these helpers are copied from cli-runtime/pkg/genericclioptions/config.go
// and can be removed if we move to config.ConfigFlags

// toRestMapper creates a rest.M from the provided rest.Config.
func toRestMapper(config *rest.Config) (meta.RESTMapper, error) {
	cacheDir := getDefaultCacheDir()

	httpCacheDir := filepath.Join(cacheDir, "http")
	discoveryCacheDir := computeDiscoverCacheDir(filepath.Join(cacheDir, "discovery"), config.Host)

	discoveryClient, err := diskcached.NewCachedDiscoveryClientForConfig(config, discoveryCacheDir, httpCacheDir, 6*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("failed to create discovery client: %w", err)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(discoveryClient)
	expander := restmapper.NewShortcutExpander(mapper, discoveryClient, func(a string) {
		klog.V(3).InfoSDepth(1, "discovery warning", "error", err)
	})
	return expander, nil
}

// getDefaultCacheDir returns default caching directory path.
// it first looks at KUBECACHEDIR env var if it is set, otherwise
// it returns standard kube cache dir.
func getDefaultCacheDir() string {
	if kcd := os.Getenv("KUBECACHEDIR"); kcd != "" {
		return kcd
	}

	return filepath.Join(homedir.HomeDir(), ".kube", "cache")
}

// computeDiscoverCacheDir takes the parentDir and the host and comes up with a "usually non-colliding" name.
func computeDiscoverCacheDir(parentDir, host string) string {
	// strip the optional scheme from host if its there:
	schemelessHost := strings.Replace(strings.Replace(host, "https://", "", 1), "http://", "", 1)
	// now do a simple collapse of non-AZ09 characters.  Collisions are possible but unlikely.  Even if we do collide the problem is short lived
	safeHost := overlyCautiousIllegalFileCharacters.ReplaceAllString(schemelessHost, "_")
	return filepath.Join(parentDir, safeHost)
}

// overlyCautiousIllegalFileCharacters matches characters that *might* not be supported.  Windows is really restrictive, so this is really restrictive
var overlyCautiousIllegalFileCharacters = regexp.MustCompile(`[^(\w/.)]`)
