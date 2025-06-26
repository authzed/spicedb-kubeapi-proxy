package inmemory

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransportBasic(t *testing.T) {
	executed := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executed = true
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message": "hello world"}`))
	})

	transport := New(handler)
	require.NotNil(t, transport)

	req, err := http.NewRequest("GET", "http://example.com/test", nil)
	require.NoError(t, err)

	// Handler should not be executed yet
	require.False(t, executed)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Handler should be executed immediately during RoundTrip
	require.True(t, executed)

	defer resp.Body.Close()

	// Status and headers should be available immediately
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "200 OK", resp.Status)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	// Read the body
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, `{"message": "hello world"}`, string(body))
}

func TestTransportImmediateExecution(t *testing.T) {
	executed := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executed = true
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Custom", "test-value")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"message": "hello"}`))
	})

	transport := New(handler)
	req, err := http.NewRequest("GET", "http://example.com/test", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)

	// Handler should be executed immediately during RoundTrip
	require.True(t, executed)

	// Headers and status should be available immediately
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	require.Equal(t, "test-value", resp.Header.Get("X-Custom"))
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "201 Created", resp.Status)

	// Multiple header access should work
	require.Equal(t, []string{"test-value"}, resp.Header.Values("X-Custom"))

	resp.Body.Close()
}

func TestTransportWithClient(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("healthy"))
		case "/echo":
			body, _ := io.ReadAll(r.Body)
			w.Header().Set("Echo-Method", r.Method)
			w.Write(body)
		default:
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found"))
		}
	})

	client := NewClient(handler)
	require.NotNil(t, client)

	// Test GET request
	resp, err := client.Get("http://example.com/health")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "healthy", string(body))

	// Test POST request
	resp, err = client.Post("http://example.com/echo", "text/plain", strings.NewReader("hello"))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "POST", resp.Header.Get("Echo-Method"))
	require.Equal(t, "hello", string(body))
}

func TestTransportStatusCodes(t *testing.T) {
	testCases := []struct {
		name           string
		handlerStatus  int
		expectedStatus int
	}{
		{"OK", http.StatusOK, http.StatusOK},
		{"Created", http.StatusCreated, http.StatusCreated},
		{"BadRequest", http.StatusBadRequest, http.StatusBadRequest},
		{"Unauthorized", http.StatusUnauthorized, http.StatusUnauthorized},
		{"NotFound", http.StatusNotFound, http.StatusNotFound},
		{"InternalServerError", http.StatusInternalServerError, http.StatusInternalServerError},
		{"NoStatusSet", 0, http.StatusOK}, // Default when no status is set
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.handlerStatus != 0 {
					w.WriteHeader(tc.handlerStatus)
				}
				w.Write([]byte("response"))
			})

			transport := New(handler)
			req, err := http.NewRequest("GET", "http://example.com/test", nil)
			require.NoError(t, err)

			resp, err := transport.RoundTrip(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			// Read body to trigger execution
			io.ReadAll(resp.Body)

			require.Equal(t, tc.expectedStatus, resp.StatusCode)
		})
	}
}

func TestTransportHeaders(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Echo request headers as response headers
		for k, v := range r.Header {
			w.Header()[fmt.Sprintf("Echo-%s", k)] = v
		}
		w.Header().Set("Custom-Header", "custom-value")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("ok"))
	})

	transport := New(handler)
	req, err := http.NewRequest("GET", "http://example.com/test", nil)
	require.NoError(t, err)

	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("X-Custom", "test-value")

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Read body to trigger execution
	io.ReadAll(resp.Body)

	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	require.Equal(t, "custom-value", resp.Header.Get("Custom-Header"))
	require.Equal(t, "Bearer token123", resp.Header.Get("Echo-Authorization"))
	require.Equal(t, "test-value", resp.Header.Get("Echo-X-Custom"))
}

func TestTransportLargeResponse(t *testing.T) {
	// Test with a large response
	largeData := strings.Repeat("abcdefghij", 10000) // 100KB

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(largeData))
	})

	transport := New(handler)
	req, err := http.NewRequest("GET", "http://example.com/large", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, largeData, string(body))
}

func TestTransportMultipleReads(t *testing.T) {
	executionCount := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executionCount++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test response"))
	})

	transport := New(handler)
	req, err := http.NewRequest("GET", "http://example.com/test", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Handler should be executed once during RoundTrip
	require.Equal(t, 1, executionCount)

	// Read in chunks to test multiple reads
	buf := make([]byte, 4)

	// First read
	n, err := resp.Body.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, "test", string(buf))
	require.Equal(t, 1, executionCount) // Still 1

	// Second read
	n, err = resp.Body.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, " res", string(buf))
	require.Equal(t, 1, executionCount) // Still 1

	// Read the rest
	remaining, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ponse", string(remaining))
	require.Equal(t, 1, executionCount) // Still 1
}

func TestTransportRequestBody(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Echo-Content-Length", fmt.Sprintf("%d", len(body)))
		w.Header().Set("Echo-Content-Type", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf("Received: %s", string(body))))
	})

	transport := New(handler)

	testBody := `{"test": "data", "number": 42}`
	req, err := http.NewRequest("POST", "http://example.com/echo", strings.NewReader(testBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Read body to trigger execution
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Echo-Content-Type"))
	require.Equal(t, fmt.Sprintf("%d", len(testBody)), resp.Header.Get("Echo-Content-Length"))
	require.Equal(t, fmt.Sprintf("Received: %s", testBody), string(body))
}

func TestTransportNilHandler(t *testing.T) {
	transport := New(nil)
	req, err := http.NewRequest("GET", "http://example.com/test", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "no handler configured")
}

func TestTransportCloseWithoutRead(t *testing.T) {
	executed := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executed = true
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test"))
	})

	transport := New(handler)
	req, err := http.NewRequest("GET", "http://example.com/test", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)

	// Handler should be executed during RoundTrip
	require.True(t, executed)

	// Close without reading
	err = resp.Body.Close()
	require.NoError(t, err)

	// Try to read after close - should get EOF
	buf := make([]byte, 10)
	n, err := resp.Body.Read(buf)
	require.Equal(t, 0, n)
	require.Equal(t, io.EOF, err)
}

func TestTransportMultipleHeaders(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Add multiple values for the same header
		w.Header().Add("Set-Cookie", "cookie1=value1")
		w.Header().Add("Set-Cookie", "cookie2=value2")
		w.Header().Add("X-Custom", "value1")
		w.Header().Add("X-Custom", "value2")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	transport := New(handler)
	req, err := http.NewRequest("GET", "http://example.com/test", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Read body to trigger execution
	io.ReadAll(resp.Body)

	require.Equal(t, http.StatusOK, resp.StatusCode)

	cookies := resp.Header.Values("Set-Cookie")
	require.Len(t, cookies, 2)
	require.Contains(t, cookies, "cookie1=value1")
	require.Contains(t, cookies, "cookie2=value2")

	customs := resp.Header.Values("X-Custom")
	require.Len(t, customs, 2)
	require.Contains(t, customs, "value1")
	require.Contains(t, customs, "value2")
}

// Benchmark tests
func BenchmarkTransport(b *testing.B) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "ok"}`))
	})

	transport := New(handler)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, err := http.NewRequest("GET", "http://example.com/test", nil)
			if err != nil {
				b.Fatal(err)
			}

			resp, err := transport.RoundTrip(req)
			if err != nil {
				b.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}

func BenchmarkTransportWithBody(b *testing.B) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	})

	transport := New(handler)
	testData := strings.Repeat("test data ", 100) // ~900 bytes

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, err := http.NewRequest("POST", "http://example.com/echo", strings.NewReader(testData))
			if err != nil {
				b.Fatal(err)
			}

			resp, err := transport.RoundTrip(req)
			if err != nil {
				b.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}

func BenchmarkTransportLargeResponse(b *testing.B) {
	// 1MB response to test memory efficiency
	largeData := strings.Repeat("abcdefghij", 100000)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(largeData))
	})

	transport := New(handler)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest("GET", "http://example.com/large", nil)
		resp, _ := transport.RoundTrip(req)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}
