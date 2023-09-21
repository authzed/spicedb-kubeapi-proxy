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
// requests to an optional set of checks, an optional set of writes, and an
// optional filter.
type Spec struct {
	Locking LockMode           `json:"lock,omitempty"`
	Matches []Match            `json:"match"`
	Checks  []StringOrTemplate `json:"check,omitempty"`
	Writes  []StringOrTemplate `json:"write,omitempty"`
	Filter  []StringOrTemplate `json:"filter,omitempty"`
}

// Match determines which requests the rule applies to
type Match struct {
	GVR   string   `json:"gvr"`
	Verbs []string `json:"verbs"`
}

// StringOrTemplate either contains a string representing a relationship
// template, or a full RelationshipTemplate definition.
type StringOrTemplate struct {
	Template              string `json:"tpl,inline"`
	*RelationshipTemplate `json:",inline"`
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
		rules = make([]Config, 0)
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
