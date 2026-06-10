package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

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
