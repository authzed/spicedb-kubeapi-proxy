//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	logsv1 "k8s.io/component-base/logs/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

// TestEmbeddedModeIntegration tests the full integration of the embedded proxy mode
// with a real Kubernetes API server.
// Note: this is separate from the e2e tests because the setup/teardown is much less
// involved in embedded mode.
func TestEmbeddedModeIntegration(t *testing.T) {
	defer require.NoError(t, logsv1.ResetForTest(utilfeature.DefaultFeatureGate))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Configure the test environment to download binaries if needed
	configureApiserver(t)

	// Start a real Kubernetes API server using envtest
	testEnv := &envtest.Environment{
		ControlPlaneStopTimeout: 60 * time.Second,
	}

	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testEnv.Stop())
	})

	// Create custom bootstrap content for the test
	bootstrapContent := map[string][]byte{
		"bootstrap.yaml": []byte(`schema: |-
  definition cluster {}
  definition user {}
  definition namespace {
    relation cluster: cluster
    relation creator: user
    relation viewer: user

    permission admin = creator
    permission edit = creator
    permission view = viewer + creator
    permission no_one_at_all = nil
  }
  definition pod {
    relation namespace: namespace
    relation creator: user
    relation viewer: user
    permission edit = creator
    permission view = viewer + creator
  }
  definition testresource {
    relation namespace: namespace
    relation creator: user
    relation viewer: user
    permission edit = creator
    permission view = viewer + creator
  }
  definition lock {
    relation workflow: workflow
  }
  definition workflow {}
relationships: |
`),
	}

	// Create embedded proxy options with embedded mode enabled and custom bootstrap
	opts := proxy.NewOptions(proxy.WithEmbeddedProxy, proxy.WithEmbeddedSpiceDBBootstrap(bootstrapContent))

	// Configure to use the real test API server
	opts.RestConfigFunc = func() (*rest.Config, http.RoundTripper, error) {
		transport, err := rest.TransportFor(cfg)
		if err != nil {
			return nil, nil, err
		}
		// Make a copy to avoid modifying the original
		configCopy := rest.CopyConfig(cfg)
		return configCopy, transport, nil
	}

	// Create simple rules for namespace operations
	createNamespaceRule := proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "namespaces",
				Verbs:        []string{"create"},
			}},
			Update: proxyrule.Update{
				CreateRelationships: []proxyrule.StringOrTemplate{{
					Template: "namespace:{{name}}#creator@user:{{user.name}}",
				}},
			},
		},
	}

	getNamespaceRule := proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "namespaces",
				Verbs:        []string{"get"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "namespace:{{name}}#creator@user:{{user.name}}",
			}},
		},
	}

	matcher, err := rules.NewMapMatcher([]proxyrule.Config{
		createNamespaceRule,
		getNamespaceRule,
	})
	require.NoError(t, err)
	opts.Matcher = matcher

	// Complete the configuration
	completedConfig, err := opts.Complete(ctx)
	require.NoError(t, err)

	// Create the embedded proxy server
	proxySrv, err := proxy.NewServer(ctx, completedConfig)
	require.NoError(t, err)

	// Start the proxy server
	go func() {
		err := proxySrv.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Proxy server failed: %v", err)
		}
	}()

	// Wait for the server to be ready
	require.Eventually(t, func() bool {
		httpClient := proxySrv.GetEmbeddedClient(
			proxy.WithUser("testuser"),
			proxy.WithGroups("users"),
		)
		require.NotNil(t, httpClient)

		kubeClient, err := kubernetes.NewForConfigAndClient(proxy.EmbeddedRestConfig, httpClient)
		require.NoError(t, err)

		// if ns create works, the server is ready
		nsName := "test-namespace-" + fmt.Sprint(time.Now().UnixNano())
		namespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: nsName,
			},
		}
		_, err = kubeClient.CoreV1().Namespaces().Create(ctx, namespace, metav1.CreateOptions{})
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)

	t.Run("namespace creation works with proper authorization", func(t *testing.T) {
		// Get embedded client for testuser
		httpClient := proxySrv.GetEmbeddedClient(
			proxy.WithUser("testuser"),
			proxy.WithGroups("users"),
		)
		require.NotNil(t, httpClient)

		// Create a Kubernetes client using the embedded HTTP client
		kubeClient, err := kubernetes.NewForConfigAndClient(proxy.EmbeddedRestConfig, httpClient)
		require.NoError(t, err)

		// Create a test namespace
		nsName := "test-namespace-" + fmt.Sprint(time.Now().UnixNano())
		namespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: nsName,
			},
		}

		// This should work with proper authorization rules
		createdNs, err := kubeClient.CoreV1().Namespaces().Create(ctx, namespace, metav1.CreateOptions{})
		require.NoError(t, err)

		// Verify namespace was created correctly
		require.Equal(t, nsName, createdNs.Name)
		require.NotEmpty(t, createdNs.ResourceVersion)

		// user can get the namespace they created
		retrievedNs, err := kubeClient.CoreV1().Namespaces().Get(ctx, nsName, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, nsName, retrievedNs.Name)
	})

	t.Run("different users get different clients", func(t *testing.T) {
		// Get embedded clients for different users
		adminClient := proxySrv.GetEmbeddedClient(
			proxy.WithUser("admin"),
			proxy.WithGroups("system:masters"),
		)
		userClient := proxySrv.GetEmbeddedClient(
			proxy.WithUser("testuser"),
			proxy.WithGroups("developers"),
		)

		require.NotNil(t, adminClient)
		require.NotNil(t, userClient)

		// Clients should be different instances
		assert.NotEqual(t, adminClient, userClient)
	})

	t.Run("unauthenticated requests are rejected", func(t *testing.T) {
		// Get embedded client without authentication
		unauthHTTPClient := proxySrv.GetEmbeddedClient()
		require.NotNil(t, unauthHTTPClient)

		// Create a Kubernetes client using the unauthenticated HTTP client
		kubeClient, err := kubernetes.NewForConfigAndClient(proxy.EmbeddedRestConfig, unauthHTTPClient)
		require.NoError(t, err)

		// Try to list namespaces - should be unauthorized
		_, err = kubeClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Unauthorized")
	})
}

// configureApiserver sets up the test environment binaries for envtest
func configureApiserver(t *testing.T) {
	t.Helper()

	// Create a logger compatible with setup-envtest
	log := zap.New(zap.UseDevMode(true))

	// Use the shared setupEnvtest function
	assetsPath := setupEnvtest(log)

	// Set the KUBEBUILDER_ASSETS environment variable
	t.Setenv("KUBEBUILDER_ASSETS", assetsPath)
}
