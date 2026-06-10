package proxy

import (
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// synchronizedRESTMapper wraps a meta.RESTMapper to make it safe for concurrent use.
//
// The discovery-backed mapper this wraps (a shortcut expander over a disk-cached
// discovery client) is not safe for concurrent use. On every call the shortcut
// expander calls the discovery client's ServerGroupsAndResources() directly,
// bypassing the DeferredDiscoveryRESTMapper's lock. When the on-disk discovery cache
// is stale, that path rewrites the cache, and the versioning codec does a
// read-modify-write on the cached object's TypeMeta (SetGroupVersionKind, then a
// deferred restore). Two concurrent calls race on that field, tearing the APIVersion
// string and panicking with a nil-pointer dereference in schema.ParseGroupVersion.
//
// The proxy shares a single mapper across all request goroutines and calls KindFor
// while filtering every list/get response, so all access is serialized here.
type synchronizedRESTMapper struct {
	mu       sync.Mutex
	delegate meta.RESTMapper
}

func newSynchronizedRESTMapper(delegate meta.RESTMapper) *synchronizedRESTMapper {
	return &synchronizedRESTMapper{delegate: delegate}
}

var _ meta.RESTMapper = (*synchronizedRESTMapper)(nil)

func (m *synchronizedRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.KindFor(resource)
}

func (m *synchronizedRESTMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.KindsFor(resource)
}

func (m *synchronizedRESTMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.ResourceFor(input)
}

func (m *synchronizedRESTMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.ResourcesFor(input)
}

func (m *synchronizedRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.RESTMapping(gk, versions...)
}

func (m *synchronizedRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.RESTMappings(gk, versions...)
}

func (m *synchronizedRESTMapper) ResourceSingularizer(resource string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegate.ResourceSingularizer(resource)
}
