package proxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

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
	Options
	Handler http.Handler
}

func NewServer(ctx context.Context, o Options) (*Server, error) {
	s := &Server{Options: o}

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
	clusterProxy := &httputil.ReverseProxy{
		ErrorLog: nil, // TODO
		Director: func(req *http.Request) {
			if req.Body != nil {
				bodyBytes, _ := io.ReadAll(req.Body)
				req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
				fmt.Println("request body", string(bodyBytes))
			}

			fmt.Println("request url", req.URL.String())
			fmt.Println("request header", req.Header)
			user, ok := request.UserFrom(req.Context())
			fmt.Println("request user", user, ok)

			// TODO: could default to the kube env vars if running on the
			//  same cluster its proxying
			req.URL.Host = "kubernetes.default:443"
			req.URL.Scheme = "https"
		},
		ModifyResponse: func(response *http.Response) error {
			if response.Body != nil {
				bodyBytes, _ := io.ReadAll(response.Body)
				response.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
				fmt.Println("response body", string(bodyBytes))
			}
			fmt.Println("response header", response.Header)
			return nil
		},
	}
	clusterProxy.Transport = transport

	handler := withAuthentication(clusterProxy, s.AuthenticationInfo.Authenticator)
	requestInfoResolver := &request.RequestInfoFactory{
		APIPrefixes: sets.NewString(
			strings.Trim(server.APIGroupPrefix, "/"),
			strings.Trim(server.DefaultLegacyAPIPrefix, "/"),
		),
		GrouplessAPIPrefixes: sets.NewString(
			strings.Trim(server.DefaultLegacyAPIPrefix, "/"),
		),
	}

	handler = genericapifilters.WithRequestInfo(handler, requestInfoResolver)
	handler = genericfilters.WithHTTPLogging(handler)
	handler = genericfilters.WithPanicRecovery(handler, requestInfoResolver)

	mux.Handle("/", handler)
	s.Handler = mux

	return s, nil
}

func (s *Server) Run(ctx context.Context) error {
	doneCh, _, err := s.Options.ServingInfo.Serve(s.Handler, time.Second*60, ctx.Done())
	if err != nil {
		return err
	}

	<-doneCh
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

func withAuthentication(handler http.Handler, auth authenticator.Request) http.Handler {
	scheme := runtime.NewScheme()
	metav1.AddToGroupVersion(scheme, schema.GroupVersion{Group: "", Version: "v1"})
	codecs := serializer.NewCodecFactory(scheme)
	failed := genericapifilters.Unauthorized(codecs)

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
