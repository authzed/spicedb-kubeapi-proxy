package proxy

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDecompressingTransport verifies that decompressingTransport transparently
// decompresses Content-Encoding: gzip responses and strips the header.
//
// This prevents the "gzip: invalid header" error that occurs when a real
// http.Transport (used by kubernetes.Clientset) receives a response with
// Content-Encoding: gzip but a body that has already been decompressed by
// FilterResp. The transport would wrap the body in a gzip.Reader and fail.
func TestDecompressingTransport(t *testing.T) {
	t.Parallel()

	plain := []byte(`{"apiVersion":"v1","kind":"Secret","metadata":{"name":"s","namespace":"default"}}`)

	var compressed bytes.Buffer
	gzw := gzip.NewWriter(&compressed)
	_, err := gzw.Write(plain)
	require.NoError(t, err)
	require.NoError(t, gzw.Close())

	t.Run("decompresses gzip and strips header", func(t *testing.T) {
		t.Parallel()

		base := roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        http.Header{"Content-Encoding": []string{"gzip"}, "Content-Type": []string{"application/json"}},
				Body:          io.NopCloser(bytes.NewReader(compressed.Bytes())),
				ContentLength: int64(compressed.Len()),
			}, nil
		})

		resp, err := (&decompressingTransport{base: base}).RoundTrip(&http.Request{}) //nolint: bodyclose
		require.NoError(t, err)
		require.Empty(t, resp.Header.Get("Content-Encoding"),
			"Content-Encoding must be stripped after decompression")
		require.Equal(t, int64(len(plain)), resp.ContentLength)

		got, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, plain, got)
	})

	t.Run("non-gzip response passes through unchanged", func(t *testing.T) {
		t.Parallel()

		base := roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(plain)),
			}, nil
		})

		resp, err := (&decompressingTransport{base: base}).RoundTrip(&http.Request{}) //nolint: bodyclose
		require.NoError(t, err)
		require.Empty(t, resp.Header.Get("Content-Encoding"))

		got, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, plain, got)
	})
}

// roundTripFunc is a helper that implements http.RoundTripper via a function.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
