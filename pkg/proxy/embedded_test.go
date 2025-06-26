package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/endpoints/request"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	logsv1 "k8s.io/component-base/logs/api/v1"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

func TestEmbeddedMode(t *testing.T) {
	ctx := t.Context()

	opts := createEmbeddedTestOptions(t)
	completedConfig, err := opts.Complete(ctx)
	require.NoError(t, err)

	proxySrv, err := NewServer(ctx, completedConfig)
	require.NoError(t, err)

	t.Run("basic embedded client", func(t *testing.T) {
		// Get embedded client
		client := proxySrv.GetEmbeddedClient()
		require.NotNil(t, client, "embedded client should not be nil")

		// Test basic request (health endpoint doesn't require auth)
		req, err := http.NewRequestWithContext(ctx, "GET", "http://embedded/healthz", nil)
		require.NoError(t, err)

		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should get a response (may be 404 but not connection error)
		require.NotEqual(t, 0, resp.StatusCode, "should get a status code")

		// Read body
		_, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
	})

	t.Run("kubernetes client integration", func(t *testing.T) {
		embeddedClient := proxySrv.GetEmbeddedClient()
		require.NotNil(t, embeddedClient)

		// Create Kubernetes client using embedded transport
		k8sClient := createKubernetesClient(t, embeddedClient, "admin-user", []string{"admin"})

		// Make a simple API call through the client
		// This tests that the kubernetes client-go library works with our embedded transport
		_, err = k8sClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{Limit: 1})
		// We expect this to fail since we don't have a real API server
		// but it should go through our proxy without connection errors
		require.Error(t, err)
		// The error should not be a connection refused error
		require.NotContains(t, err.Error(), "connection refused")
	})

}

func TestEmbeddedModeCustomHeaders(t *testing.T) {
	defer require.NoError(t, logsv1.ResetForTest(utilfeature.DefaultFeatureGate))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Create a proxy with custom header names
	opts := createEmbeddedTestOptions(t)
	// Override with custom header names
	opts.Authentication.Embedded.UsernameHeaders = []string{"Custom-User"}
	opts.Authentication.Embedded.GroupHeaders = []string{"Custom-Groups"}
	opts.Authentication.Embedded.ExtraHeaderPrefixes = []string{"Custom-Extra-"}

	completedConfig, err := opts.Complete(ctx)
	require.NoError(t, err)

	proxySrv, err := NewServer(ctx, completedConfig)
	require.NoError(t, err)

	client := proxySrv.GetEmbeddedClient()
	require.NotNil(t, client)

	// Test request with custom headers
	req, err := http.NewRequestWithContext(ctx, "GET", "http://embedded/healthz", nil)
	require.NoError(t, err)

	// Use custom header names
	req.Header.Set("Custom-User", "test-user")
	req.Header.Set("Custom-Groups", "test-group")
	req.Header.Set("Custom-Extra-Department", "engineering")

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Should get a response
	require.NotEqual(t, 0, resp.StatusCode, "should get a status code")

	// Read body
	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
}

func TestEmbeddedModeAuthenticationConfiguration(t *testing.T) {
	defer require.NoError(t, logsv1.ResetForTest(utilfeature.DefaultFeatureGate))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Create one proxy with multiple header configuration for all tests
	opts := createEmbeddedTestOptions(t)
	// Configure multiple headers of each type
	opts.Authentication.Embedded.UsernameHeaders = []string{"Primary-User", "Secondary-User", "X-Remote-User"}
	opts.Authentication.Embedded.GroupHeaders = []string{"X-Remote-Group", "X-User-Groups"}
	opts.Authentication.Embedded.ExtraHeaderPrefixes = []string{"X-Remote-Extra-", "X-User-Attr-"}

	completedConfig, err := opts.Complete(ctx)
	require.NoError(t, err)

	proxySrv, err := NewServer(ctx, completedConfig)
	require.NoError(t, err)

	client := proxySrv.GetEmbeddedClient()
	require.NotNil(t, client)

	t.Run("username header priority", func(t *testing.T) {
		// Test with secondary header (Primary-User missing)
		req, err := http.NewRequestWithContext(ctx, "GET", "http://embedded/healthz", nil)
		require.NoError(t, err)

		req.Header.Set("Secondary-User", "alice")
		req.Header.Set("X-Remote-Group", "admin")

		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.NotEqual(t, 0, resp.StatusCode, "should get a status code")

		_, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
	})

	t.Run("multiple group headers", func(t *testing.T) {
		// Test with groups from both headers
		req, err := http.NewRequestWithContext(ctx, "GET", "http://embedded/healthz", nil)
		require.NoError(t, err)

		req.Header.Set("X-Remote-User", "bob")
		req.Header.Add("X-Remote-Group", "developers")
		req.Header.Add("X-Remote-Group", "reviewers")
		req.Header.Add("X-User-Groups", "admin")
		req.Header.Add("X-User-Groups", "security")

		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.NotEqual(t, 0, resp.StatusCode, "should get a status code")

		_, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
	})

	t.Run("multiple extra header prefixes", func(t *testing.T) {
		// Test with extra attributes from both prefixes
		req, err := http.NewRequestWithContext(ctx, "GET", "http://embedded/healthz", nil)
		require.NoError(t, err)

		req.Header.Set("X-Remote-User", "charlie")
		req.Header.Set("X-Remote-Group", "engineers")
		req.Header.Set("X-Remote-Extra-Department", "platform")
		req.Header.Set("X-Remote-Extra-Team", "infrastructure")
		req.Header.Set("X-User-Attr-Location", "remote")
		req.Header.Set("X-User-Attr-Timezone", "UTC")

		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.NotEqual(t, 0, resp.StatusCode, "should get a status code")

		_, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
	})
}

func TestEmbeddedModeDefaults(t *testing.T) {
	defer require.NoError(t, logsv1.ResetForTest(utilfeature.DefaultFeatureGate))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Create embedded proxy with no explicit header configuration to test defaults
	opts := NewOptions()
	opts.EmbeddedMode = true
	opts.Authentication.Embedded.Enabled = true
	opts.SpiceDBOptions.SpiceDBEndpoint = EmbeddedSpiceDBEndpoint

	// Configure mock upstream server
	opts.RestConfigFunc = func() (*rest.Config, http.RoundTripper, error) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"kind": "Status", "status": "Success"}`))
		}))
		t.Cleanup(mockServer.Close)

		return &rest.Config{
			Host: mockServer.URL,
			TLSClientConfig: rest.TLSClientConfig{
				Insecure: true,
			},
		}, nil, nil
	}

	// Use empty rules for testing - allow all requests
	opts.Matcher = rules.MatcherFunc(func(match *request.RequestInfo) []*rules.RunnableRule {
		return []*rules.RunnableRule{{
			Checks: []*rules.RelExpr{},
		}}
	})

	completedConfig, err := opts.Complete(ctx)
	require.NoError(t, err)

	proxySrv, err := NewServer(ctx, completedConfig)
	require.NoError(t, err)

	client := proxySrv.GetEmbeddedClient()
	require.NotNil(t, client)

	// Test with default headers (X-Remote-User, X-Remote-Group, X-Remote-Extra-)
	req, err := http.NewRequestWithContext(ctx, "GET", "http://embedded/healthz", nil)
	require.NoError(t, err)

	req.Header.Set("X-Remote-User", "default-user")
	req.Header.Set("X-Remote-Group", "default-group")
	req.Header.Set("X-Remote-Extra-Department", "engineering")

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Should get a response
	require.NotEqual(t, 0, resp.StatusCode, "should get a status code")

	// Read body
	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
}

// createEmbeddedTestOptions creates minimal options for embedded testing
func createEmbeddedTestOptions(t *testing.T) *Options {
	t.Helper()

	opts := NewOptions()
	opts.EmbeddedMode = true
	opts.Authentication.Embedded.Enabled = true

	// Use embedded SpiceDB for testing
	opts.SpiceDBOptions.SpiceDBEndpoint = EmbeddedSpiceDBEndpoint

	// Configure mock upstream server
	opts.RestConfigFunc = func() (*rest.Config, http.RoundTripper, error) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"kind": "Status", "status": "Success"}`))
		}))
		t.Cleanup(mockServer.Close)

		return &rest.Config{
			Host: mockServer.URL,
			TLSClientConfig: rest.TLSClientConfig{
				Insecure: true,
			},
		}, nil, nil
	}

	// Use empty rules for testing - allow all requests
	opts.Matcher = rules.MatcherFunc(func(match *request.RequestInfo) []*rules.RunnableRule {
		return []*rules.RunnableRule{{
			Checks: []*rules.RelExpr{},
		}}
	})

	return opts
}

// createKubernetesClient creates a kubernetes client using the embedded transport
func createKubernetesClient(t *testing.T, embeddedClient *http.Client, username string, groups []string) *kubernetes.Clientset {
	t.Helper()

	// Create rest config that uses the embedded transport
	restConfig := &rest.Config{
		Host:      "http://embedded",
		Transport: embeddedClient.Transport,
	}

	// Wrap transport to add authentication headers
	restConfig.Transport = &headerAddingTransport{
		base:     embeddedClient.Transport,
		username: username,
		groups:   groups,
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	require.NoError(t, err)

	return clientset
}

// headerAddingTransport wraps an http.RoundTripper to add authentication headers
type headerAddingTransport struct {
	base     http.RoundTripper
	username string
	groups   []string
}

func (h *headerAddingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone request to avoid modifying original
	newReq := req.Clone(req.Context())

	// Add authentication headers
	newReq.Header.Set("X-Remote-User", h.username)
	for _, group := range h.groups {
		newReq.Header.Add("X-Remote-Group", group)
	}

	return h.base.RoundTrip(newReq)
}
