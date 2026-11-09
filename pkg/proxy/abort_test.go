package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/request"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
)

const abortWhileHandlingMsg = "Timeout or abort while handling"

// reportedErrors records what k8s.io/apiserver reports through
// utilruntime.HandleError, which is where WithPanicRecovery reports recovered
// http.ErrAbortHandler panics.
type reportedErrors struct {
	mu   sync.Mutex
	msgs []string
}

func (r *reportedErrors) count(msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, m := range r.msgs {
		if m == msg {
			n++
		}
	}
	return n
}

func recordReportedErrors(t *testing.T) *reportedErrors {
	t.Helper()
	rec := &reportedErrors{}
	prev := utilruntime.ErrorHandlers
	utilruntime.ErrorHandlers = []utilruntime.ErrorHandler{
		func(_ context.Context, _ error, msg string, _ ...any) {
			rec.mu.Lock()
			defer rec.mu.Unlock()
			rec.msgs = append(rec.msgs, msg)
		},
	}
	t.Cleanup(func() { utilruntime.ErrorHandlers = prev })
	return rec
}

// newAbortTestProxy mirrors the proxy's handler chain around the cluster
// ReverseProxy (see NewServer), served over TLS with HTTP/2.
func newAbortTestProxy(t *testing.T, upstream string) *httptest.Server {
	t.Helper()
	target, err := url.Parse(upstream)
	require.NoError(t, err)

	rp := &httputil.ReverseProxy{
		FlushInterval: -1,
		Rewrite:       func(r *httputil.ProxyRequest) { r.SetURL(target) },
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	resolver := &request.RequestInfoFactory{
		APIPrefixes:          sets.NewString("api", "apis"),
		GrouplessAPIPrefixes: sets.NewString("api"),
	}
	handler := withClientAbortRecovery(rp)
	handler = genericfilters.WithHTTPLogging(handler)
	handler = genericfilters.WithPanicRecovery(handler, resolver)

	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// TestClientAbortWhileStreamingIsNotReported covers a client that goes away
// while the proxy is still streaming the upstream response body: the
// ReverseProxy's http.ErrAbortHandler panic must not be reported as an error.
func TestClientAbortWhileStreamingIsNotReported(t *testing.T) {
	rec := recordReportedErrors(t)

	// Upstream that keeps streaming body chunks until the proxy stops reading.
	upstreamDone := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(upstreamDone)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		chunk := bytes.Repeat([]byte("x"), 8<<10)
		for {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}))
	t.Cleanup(upstream.Close)

	proxySrv := newAbortTestProxy(t, upstream.URL)

	ctx, cancel := context.WithCancel(t.Context())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxySrv.URL+"/api/v1/namespaces/default/pods", nil)
	require.NoError(t, err)
	resp, err := proxySrv.Client().Do(req)
	require.NoError(t, err)
	require.Equal(t, "HTTP/2.0", resp.Proto)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, err = io.ReadFull(resp.Body, make([]byte, 1024))
	require.NoError(t, err)

	// The client gives up mid-body.
	cancel()
	_ = resp.Body.Close()

	// Once the upstream handler returns, the proxy has stopped copying and the
	// abort (if any) has been handled.
	select {
	case <-upstreamDone:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream handler did not finish after the client went away")
	}
	require.Never(t, func() bool { return rec.count(abortWhileHandlingMsg) > 0 },
		500*time.Millisecond, 10*time.Millisecond,
		"a client disconnect mid-stream must not be reported as %q", abortWhileHandlingMsg)
}

// TestUpstreamAbortWhileStreamingIsReported is the control: when the upstream
// drops the connection mid-body while the client is still connected, the abort
// is a real failure and must still be reported.
func TestUpstreamAbortWhileStreamingIsReported(t *testing.T) {
	rec := recordReportedErrors(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1048576")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes.Repeat([]byte("x"), 1024))
		w.(http.Flusher).Flush()
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	t.Cleanup(upstream.Close)

	proxySrv := newAbortTestProxy(t, upstream.URL)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, proxySrv.URL+"/api/v1/namespaces/default/pods", nil)
	require.NoError(t, err)
	resp, err := proxySrv.Client().Do(req)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	require.Eventually(t, func() bool { return rec.count(abortWhileHandlingMsg) > 0 },
		5*time.Second, 10*time.Millisecond,
		"an upstream abort with the client still connected must be reported as %q", abortWhileHandlingMsg)
}

// TestWithClientAbortRecoveryRepanics checks that only client-initiated aborts
// are swallowed and every other panic is propagated unchanged.
func TestWithClientAbortRecoveryRepanics(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	otherPanic := errors.New("boom")

	tests := []struct {
		name      string
		ctx       context.Context
		panicWith any
		wantPanic any
	}{
		{name: "abort with client gone is swallowed", ctx: canceledCtx, panicWith: http.ErrAbortHandler, wantPanic: nil},
		{name: "abort with client connected is re-raised", ctx: t.Context(), panicWith: http.ErrAbortHandler, wantPanic: http.ErrAbortHandler},
		{name: "other panic with client gone is re-raised", ctx: canceledCtx, panicWith: otherPanic, wantPanic: otherPanic},
		{name: "other panic with client connected is re-raised", ctx: t.Context(), panicWith: otherPanic, wantPanic: otherPanic},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := withClientAbortRecovery(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				panic(tt.panicWith)
			}))
			req := httptest.NewRequestWithContext(tt.ctx, http.MethodGet, "/api/v1/pods", nil)
			serve := func() { h.ServeHTTP(httptest.NewRecorder(), req) }
			if tt.wantPanic == nil {
				require.NotPanics(t, serve)
			} else {
				require.PanicsWithValue(t, tt.wantPanic, serve)
			}
		})
	}
}
