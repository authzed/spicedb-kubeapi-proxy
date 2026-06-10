package proxy

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// cachingRESTMapper wraps a meta.RESTMapper to make it safe for concurrent use and to
// memoize KindFor results.
//
// The discovery-backed mapper this wraps (a shortcut expander over a disk-cached
// discovery client) is not safe for concurrent use: on every call the shortcut
// expander calls the discovery client's ServerGroupsAndResources() directly,
// bypassing the DeferredDiscoveryRESTMapper's lock, and when the on-disk cache is
// stale the versioning codec races on the cached object's TypeMeta during encoding.
// All access is therefore serialized through a mutex.
//
// Serialization alone is correct but expensive: the shortcut expander re-reads and
// JSON-decodes one discovery document per group-version on every KindFor call, which
// is costly on CRD-heavy clusters and, under the lock, serializes that work across
// all requests. Successful GVR->GVK lookups are memoized so the steady state is an
// O(1) map read.
//
// Cached entries expire after ttl, which is set to the same TTL as the underlying
// disk discovery cache. Memoization therefore never serves data staler than discovery
// itself would, rather than caching indefinitely. Only successful lookups are cached;
// errors (and not-yet-known resource types) are retried.
type cachingRESTMapper struct {
	delegate meta.RESTMapper
	ttl      time.Duration

	mu    sync.Mutex
	cache map[schema.GroupVersionResource]kindForEntry
}

type kindForEntry struct {
	gvk      schema.GroupVersionKind
	storedAt time.Time
}

func newCachingRESTMapper(delegate meta.RESTMapper, ttl time.Duration) *cachingRESTMapper {
	return &cachingRESTMapper{
		delegate: delegate,
		ttl:      ttl,
		cache:    make(map[schema.GroupVersionResource]kindForEntry),
	}
}

var _ meta.RESTMapper = (*cachingRESTMapper)(nil)

func (m *cachingRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e, ok := m.cache[resource]; ok && time.Since(e.storedAt) < m.ttl {
		return e.gvk, nil
	}

	gvk, err := m.delegate.KindFor(resource)
	if err != nil {
		// Don't cache failures: a transient discovery error or a not-yet-registered
		// resource type must be retried, not memoized.
		return gvk, err
	}
	m.cache[resource] = kindForEntry{gvk: gvk, storedAt: time.Now()}
	return gvk, nil
}

func (m *cachingRESTMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.KindsFor(resource)
}

func (m *cachingRESTMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.ResourceFor(input)
}

func (m *cachingRESTMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.ResourcesFor(input)
}

func (m *cachingRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.RESTMapping(gk, versions...)
}

func (m *cachingRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.RESTMappings(gk, versions...)
}

func (m *cachingRESTMapper) ResourceSingularizer(resource string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.ResourceSingularizer(resource)
}
