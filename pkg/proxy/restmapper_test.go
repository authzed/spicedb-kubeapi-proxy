package proxy

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// deploymentGVK is the kind deploymentsGVR resolves to.
var deploymentGVK = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

// countingRESTMapper is a test double that records how many times KindFor is invoked
// and returns a configurable result. It embeds meta.RESTMapper (nil) to satisfy the
// interface; only KindFor is exercised.
type countingRESTMapper struct {
	meta.RESTMapper
	kindForCalls  int
	kindForResult schema.GroupVersionKind
	kindForErr    error
}

func (c *countingRESTMapper) KindFor(schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	c.kindForCalls++
	return c.kindForResult, c.kindForErr
}

// deploymentsGVR is a stock Kubernetes resource used to exercise KindFor. The bug is
// independent of which resource is resolved; any GVR served by discovery will do.
var deploymentsGVR = schema.GroupVersionResource{
	Group:    "apps",
	Version:  "v1",
	Resource: "deployments",
}

// newFakeDiscoveryServer serves a minimal Kubernetes discovery API (core/v1 plus the
// apps group) so we can build a discovery-backed REST mapper in tests.
func newFakeDiscoveryServer(t *testing.T) *httptest.Server {
	t.Helper()
	writeJSON := func(w http.ResponseWriter, obj interface{}) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(obj)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, &metav1.APIVersions{
			TypeMeta: metav1.TypeMeta{Kind: "APIVersions"},
			Versions: []string{"v1"},
		})
	})
	mux.HandleFunc("/api/v1", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, &metav1.APIResourceList{
			TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "configmaps", Namespaced: true, Kind: "ConfigMap", ShortNames: []string{"cm"}},
			},
		})
	})
	mux.HandleFunc("/apis", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, &metav1.APIGroupList{
			TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
			Groups: []metav1.APIGroup{{
				Name:             "apps",
				Versions:         []metav1.GroupVersionForDiscovery{{GroupVersion: "apps/v1", Version: "v1"}},
				PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "apps/v1", Version: "v1"},
			}},
		})
	})
	mux.HandleFunc("/apis/apps/v1", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, &metav1.APIResourceList{
			TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Namespaced: true, Kind: "Deployment", ShortNames: []string{"deploy"}},
			},
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, &metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}})
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestNewCachedRESTMapper_ConcurrentKindForIsRaceFree reproduces the production panic:
// many concurrent requests are driven through a single shared REST mapper, and KindFor
// on the discovery-backed mapper races on the discovery cache write (a read-modify-write
// of the cached object's TypeMeta during encoding), corrupting a string and panicking
// with a nil-pointer dereference inside ParseGroupVersion.
//
// A zero TTL forces the disk discovery cache to be rewritten on every call, which is the
// path that races. Run under `-race` to detect it.
func TestNewCachedRESTMapper_ConcurrentKindForIsRaceFree(t *testing.T) {
	srv := newFakeDiscoveryServer(t)
	cfg := &rest.Config{Host: srv.URL}

	tmp := t.TempDir()
	mapper, err := newCachedRESTMapper(cfg, filepath.Join(tmp, "discovery"), filepath.Join(tmp, "http"), 0)
	require.NoError(t, err)

	const goroutines = 64
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gvk, err := mapper.KindFor(deploymentsGVR)
			if err != nil {
				errs <- err
				return
			}
			if gvk.Kind != "Deployment" {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

// TestCachingRESTMapper_MemoizesSuccessfulKindFor verifies repeated lookups for the
// same resource resolve from the in-process cache rather than re-hitting the
// (expensive, per-call discovery-reading) delegate on every request.
func TestCachingRESTMapper_MemoizesSuccessfulKindFor(t *testing.T) {
	underlying := &countingRESTMapper{kindForResult: deploymentGVK}
	mapper := newCachingRESTMapper(underlying, time.Hour)

	for i := 0; i < 5; i++ {
		gvk, err := mapper.KindFor(deploymentsGVR)
		require.NoError(t, err)
		require.Equal(t, "Deployment", gvk.Kind)
	}

	require.Equal(t, 1, underlying.kindForCalls, "underlying KindFor should be invoked once and memoized thereafter")
}

// TestCachingRESTMapper_DoesNotCacheErrors verifies failed lookups are not cached, so a
// transient discovery error (or a resource type that does not exist yet) is retried
// rather than permanently poisoning the cache.
func TestCachingRESTMapper_DoesNotCacheErrors(t *testing.T) {
	underlying := &countingRESTMapper{kindForErr: errors.New("discovery unavailable")}
	mapper := newCachingRESTMapper(underlying, time.Hour)

	_, err := mapper.KindFor(deploymentsGVR)
	require.Error(t, err)

	// Discovery recovers; the next lookup must hit the underlying mapper again and succeed.
	underlying.kindForErr = nil
	underlying.kindForResult = deploymentGVK

	gvk, err := mapper.KindFor(deploymentsGVR)
	require.NoError(t, err)
	require.Equal(t, "Deployment", gvk.Kind)
	require.Equal(t, 2, underlying.kindForCalls, "error result must not be cached")
}

// TestCachingRESTMapper_RefreshesAfterTTL verifies the in-process cache honors the TTL:
// a cached entry is re-resolved through the delegate once it is older than the TTL, so
// memoization never serves data staler than the underlying discovery cache would.
func TestCachingRESTMapper_RefreshesAfterTTL(t *testing.T) {
	underlying := &countingRESTMapper{kindForResult: deploymentGVK}
	mapper := newCachingRESTMapper(underlying, time.Hour)

	now := time.Unix(1_000_000, 0)
	mapper.now = func() time.Time { return now }

	_, err := mapper.KindFor(deploymentsGVR) // miss -> delegate (calls=1)
	require.NoError(t, err)
	_, err = mapper.KindFor(deploymentsGVR) // hit (calls=1)
	require.NoError(t, err)
	require.Equal(t, 1, underlying.kindForCalls)

	// Within the TTL: still served from cache.
	now = now.Add(59 * time.Minute)
	_, err = mapper.KindFor(deploymentsGVR)
	require.NoError(t, err)
	require.Equal(t, 1, underlying.kindForCalls, "entry within TTL should stay cached")

	// Past the TTL: re-resolved through the delegate.
	now = now.Add(2 * time.Minute) // total 61m > 60m TTL
	_, err = mapper.KindFor(deploymentsGVR)
	require.NoError(t, err)
	require.Equal(t, 2, underlying.kindForCalls, "entry older than TTL should be refreshed")
}
