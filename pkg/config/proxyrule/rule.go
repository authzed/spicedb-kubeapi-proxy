package proxyrule

import (
	"errors"
	"io"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

var v1alpha1ProxyRule = metav1.TypeMeta{
	Kind:       "ProxyRule",
	APIVersion: "authzed.com/v1alpha1",
}

const lookahead = 100

// MatchingIDFieldValue is the value specified in LookupResources
// requests to indicate that the request should match the ID of the object
// being processed by the proxy.
const MatchingIDFieldValue = "$"

// Config is a typed wrapper around a Spec.
// In the future Spec may be exposed as a real kube api; this type
// is meant for an on-disk representation given to the proxy on start and omits
// spec/status and other common kube trimmings.
type Config struct {
	metav1.TypeMeta `json:",inline"`
	Spec            `json:",inline"`
}

type LockMode string

const (
	PessimisticLockMode LockMode = "Pessimistic"
	OptimisticLockMode  LockMode = "Optimistic"
)

// Spec defines a single rule for the proxy that matches incoming
// requests to an optional set of checks, an optional set of updares, and an
// optional filter.
type Spec struct {
	Locking    LockMode           `json:"lock,omitempty"`
	Matches    []Match            `json:"match"`
	Checks     []StringOrTemplate `json:"check,omitempty"`
	Must       []StringOrTemplate `json:"must,omitempty"`
	MustNot    []StringOrTemplate `json:"mustNot,omitempty"`
	Updates    []StringOrTemplate `json:"update,omitempty"`
	PreFilters []PreFilter        `json:"prefilter,omitempty"`
	// TODO: PostFilter
}

// Match determines which requests the rule applies to
type Match struct {
	GroupVersion string   `json:"apiVersion"`
	Resource     string   `json:"resource"`
	Verbs        []string `json:"verbs"`
}

// StringOrTemplate either contains a string representing a relationship
// template, or a full RelationshipTemplate definition.
type StringOrTemplate struct {
	Template              string `json:"tpl,inline"`
	*RelationshipTemplate `json:",inline"`
}

// PreFilter defines a LookupResources request to filter the results.
// Prefilters work by generating a list of allowed object (name, namespace)
// pairs ahead of / in parallel with the kube request.
type PreFilter struct {
	// FromObjectIDNameExpr is a Bloblang expression defining how to construct an allowed Name from an
	// LR response.
	FromObjectIDNameExpr string `json:"fromObjectIDNameExpr,omitempty"`

	// FromObjectIDNamespaceExpr is a Bloblang expression defining how to construct an allowed Namespace
	// from an LR  response.
	FromObjectIDNamespaceExpr string `json:"fromObjectIDNamespaceExpr,omitempty"`

	// LookupMatchingResources is a template defining a LookupResources request to filter on.
	// The resourceID must be set to `$`.
	LookupMatchingResources *StringOrTemplate `json:"lookupMatchingResources,optional"`
}

// RelationshipTemplate represents a relationship where some fields may be
// omitted or templated.
type RelationshipTemplate struct {
	Resource ObjectTemplate `json:"resource"`
	Subject  ObjectTemplate `json:"subject"`
}

// ObjectTemplate represents a component of a relationship where some fields may
// be omitted or templated.
type ObjectTemplate struct {
	Type     string `json:"type"`
	ID       string `json:"id"`
	Relation string `json:"relation,omitempty"`
}

func Parse(reader io.Reader) ([]Config, error) {
	decoder := utilyaml.NewYAMLOrJSONDecoder(reader, lookahead)
	var (
		rules []Config
		rule  Config
	)
	for err := decoder.Decode(&rule); !errors.Is(err, io.EOF); err = decoder.Decode(&rule) {
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
		rule = Config{}
	}
	return rules, nil
}
