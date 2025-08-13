// Package inmemory provides an in-memory HTTP transport implementation that
// bypasses the network layer entirely by calling handlers directly in-process.
//
// This is useful for embedding HTTP servers directly into applications,
// testing, and high-performance scenarios where network overhead is undesirable.
package inmemory

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// Transport implements http.RoundTripper by executing handlers directly
// in-process, eliminating network overhead and providing high performance
// for embedded scenarios.
type Transport struct {
	handler http.Handler
}

// New creates a new in-memory transport that will call the provided handler
// directly during RoundTrip execution.
func New(handler http.Handler) *Transport {
	return &Transport{handler: handler}
}

// RoundTrip implements http.RoundTripper by executing the handler immediately
// and creating a response with the captured body data.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.handler == nil {
		return nil, fmt.Errorf("no handler configured")
	}

	// Create the response object
	resp := &http.Response{
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		ContentLength: -1,
		Request:       req,
	}

	// Create capturing response writer
	capturer := &responseWriter{
		headers:  resp.Header,
		response: resp,
	}

	// Execute the handler immediately
	t.handler.ServeHTTP(capturer, req)

	// Set default status if not set
	if resp.StatusCode == 0 {
		resp.StatusCode = http.StatusOK
		resp.Status = "200 OK"
	}

	// Create body reader from captured data
	var bodyReader io.Reader
	if capturer.bodyData != nil {
		bodyReader = bytes.NewReader(capturer.bodyData)
	} else {
		bodyReader = bytes.NewReader([]byte{})
	}

	resp.Body = &responseBody{reader: bodyReader}
	return resp, nil
}

// responseBody implements io.ReadCloser for response body data
type responseBody struct {
	reader io.Reader
	closed bool
}

// Read implements io.Reader
func (b *responseBody) Read(p []byte) (n int, err error) {
	if b.closed {
		return 0, io.EOF
	}

	if b.reader == nil {
		return 0, io.EOF
	}

	return b.reader.Read(p)
}

// Close implements io.Closer
func (b *responseBody) Close() error {
	b.closed = true
	return nil
}

// responseWriter implements http.ResponseWriter by capturing all writes
type responseWriter struct {
	headers  http.Header
	response *http.Response
	bodyData []byte
}

// Header returns the response headers
func (w *responseWriter) Header() http.Header {
	return w.headers
}

// Write captures body data
func (w *responseWriter) Write(data []byte) (int, error) {
	// Set default status on first write if not set
	if w.response.StatusCode == 0 {
		w.WriteHeader(http.StatusOK)
	}

	// Append to body data
	w.bodyData = append(w.bodyData, data...)
	return len(data), nil
}

// WriteHeader sets the response status
func (w *responseWriter) WriteHeader(statusCode int) {
	// Only set status once (standard behavior)
	if w.response.StatusCode != 0 {
		return
	}

	w.response.StatusCode = statusCode
	w.response.Status = fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode))
}

// NewClient creates an http.Client that uses the in-memory transport
func NewClient(handler http.Handler) *http.Client {
	return &http.Client{
		Transport: New(handler),
	}
}
