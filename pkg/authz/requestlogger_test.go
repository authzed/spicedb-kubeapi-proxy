package authz

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestLogger(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		url      string
		body     string
		expected string
	}{
		{
			name:     "GET request without query or body",
			method:   "GET",
			url:      "https://kubernetes.default.svc/api/v1/pods",
			body:     "",
			expected: "GET https://kubernetes.default.svc/api/v1/pods",
		},
		{
			name:     "GET request with query parameters",
			method:   "GET",
			url:      "https://kubernetes.default.svc/api/v1/pods?labelSelector=app%3Dmyapp&limit=10",
			body:     "",
			expected: "GET https://kubernetes.default.svc/api/v1/pods?labelSelector=app%3Dmyapp&limit=10",
		},
		{
			name:     "POST request with body",
			method:   "POST",
			url:      "https://kubernetes.default.svc/api/v1/namespaces/default/pods",
			body:     `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"test"}}`,
			expected: `POST https://kubernetes.default.svc/api/v1/namespaces/default/pods Body: {"apiVersion":"v1","kind":"Pod","metadata":{"name":"test"}}`,
		},
		{
			name:     "PUT request with query and body",
			method:   "PUT",
			url:      "https://kubernetes.default.svc/api/v1/namespaces/default/pods/test?dryRun=All",
			body:     `{"metadata":{"labels":{"env":"prod"}}}`,
			expected: `PUT https://kubernetes.default.svc/api/v1/namespaces/default/pods/test?dryRun=All Body: {"metadata":{"labels":{"env":"prod"}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bodyReader io.Reader
			if tt.body != "" {
				bodyReader = strings.NewReader(tt.body)
			}

			req, err := http.NewRequest(tt.method, tt.url, bodyReader)
			require.NoError(t, err)

			result := requestLogger(req)
			require.Equal(t, tt.expected, result)
		})
	}
}
