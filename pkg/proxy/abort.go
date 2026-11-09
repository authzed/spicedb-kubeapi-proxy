package proxy

import (
	"net/http"

	"k8s.io/klog/v2"
)

// withClientAbortRecovery swallows the http.ErrAbortHandler panic that
// httputil.ReverseProxy raises when it fails to copy a streamed response body,
// but only when the request context has already been canceled, i.e. the client
// went away mid-response.
//
// net/http's HTTP/1 and HTTP/2 servers deliberately recover http.ErrAbortHandler
// without logging it. This proxy, however, wraps its handlers in
// k8s.io/apiserver's WithPanicRecovery filter, which recovers the panic first and
// reports every one of them as an error ("Timeout or abort while handling").
// Recovering client-initiated aborts here restores the net/http behavior for
// them.
//
// This is narrower than net/http, which silences every abort: any other panic,
// and an abort while the client is still connected (for example, the upstream
// Kubernetes API dropping the connection mid-body), is re-raised so that
// WithPanicRecovery keeps reporting it. Unlike net/http, the wrapper then
// returns normally instead of aborting the connection; the client is already
// gone, so there is nothing left to abort.
func withClientAbortRecovery(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			if r == http.ErrAbortHandler && req.Context().Err() != nil { //nolint:errorlint // sentinel panic value, compared by identity like net/http does
				klog.V(3).InfoS("client disconnected while streaming upstream response", "method", req.Method, "URI", req.RequestURI)
				return
			}
			panic(r)
		}()
		handler.ServeHTTP(w, req)
	})
}
