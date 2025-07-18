package proxyrule

import (
	"errors"
	"io"

	"github.com/go-playground/validator/v10"
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
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,inline"`
	Spec              `json:",inline"`
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
	// Locking is the locking mode for this rule. The default is specified on
	// the command line as a flag.
	//
	// If set to "Optimistic", the proxy will use optimistic locking by attempting
	// to perform the update and rolling back if the update fails.
	//
	// If set to "Pessimistic", the proxy will use pessimistic locking by
	// acquiring a lock on the object before performing the update.
	Locking LockMode `json:"lock,omitempty" validate:"omitempty,oneof=Optimistic Pessimistic"`

	// Matches defines the requests that this rule applies to. Cannot be empty.
	Matches []Match `json:"match" validate:"required,min=1,dive"`

	// If defines CEL expressions that must evaluate to true for this rule to apply.
	// All expressions must evaluate to true for the rule to match.
	//
	// Available variables in CEL expressions:
	// - request: request information (verb, resource, apiGroup, apiVersion, name, namespace)
	// - user: user information (name, uid, groups, extra)
	// - object: the Kubernetes object being operated on (for create/update/patch operations)
	// - name: the name of the resource
	// - resourceNamespace: the namespace of the resource
	// - namespacedName: the namespaced name of the resource
	// - headers: HTTP headers from the request
	// - body: the request body (for create/update/patch operations)
	//
	// Example CEL expressions:
	// - "request.verb == 'get'"
	// - "user.name == 'admin'"
	// - "'system:masters' in user.groups"
	// - "resourceNamespace == 'default'"
	// - "request.resource == 'pods' && request.verb in ['get', 'list']"
	If []string `json:"if,omitempty" validate:"omitempty,dive"`

	// Checks are the authorization checks to perform, in SpiceDB, if the request matches.
	// If empty, the request will be allowed without any checks.
	Checks []StringOrTemplate `json:"check,omitempty" validate:"omitempty,dive"`

	// PostChecks are authorization checks to perform after the Kubernetes API call
	// completes successfully, but before returning the response. These only apply
	// to read-only operations (non-write and non-list operations).
	// If any PostCheck returns NO_PERMISSION, the operation will fail.
	PostChecks []StringOrTemplate `json:"postcheck,omitempty" validate:"omitempty,dive"`

	// PreFilters are LookupResources requests to filter the results before any
	// authorization checks are performed. Used for List requests.
	PreFilters []PreFilter `json:"prefilter,omitempty" validate:"omitempty,dive"`

	// PostFilters are authorization checks to filter the results after the
	// Kubernetes API call completes but before returning the response.
	// Used for List requests. If a PostFilter is set and a PreFilter
	// is missing, the LookupResources call is skipped.
	PostFilters []PostFilter `json:"postfilter,omitempty" validate:"omitempty,dive"`

	// Update contains the updates to perform if the request matches, the checks succeed,
	// and this is a write operation of some kind (Create, Update, or Delete).
	Update Update `json:"update,omitempty" validate:"omitempty"`
}

// Update is an update to perform against the SpiceDB relationships.
type Update struct {
	// PreconditionExists defines the relationships that must exist for the update
	// operation to be succeed. Equivalent of Preconditions in SpiceDB.
	// To specify dynamic portions of the relationship, use the following dollar
	// sign values:
	// - `$resourceType` for the resource type
	// - `$resourceID` for the resource ID
	// - `$resourceRelation` for the relation
	// - `$subjectType` for the subject type
	// - `$subjectID` for the subject ID
	// - `$subjectRelation` for the subject relation
	PreconditionExists []StringOrTemplate `json:"preconditionExists,omitempty" validate:"omitempty,dive"`

	// PreconditionDoesNotExist defines the relationships that must not exist for the update
	// operation to succeed. Equivalent of Preconditions in SpiceDB.
	// To specify dynamic portions of the relationship, use the following dollar
	// sign values:
	// - `$resourceType` for the resource type
	// - `$resourceID` for the resource ID
	// - `$resourceRelation` for the relation
	// - `$subjectType` for the subject type
	// - `$subjectID` for the subject ID
	// - `$subjectRelation` for the subject relation
	PreconditionDoesNotExist []StringOrTemplate `json:"preconditionDoesNotExist,omitempty" validate:"omitempty,dive"`

	// CreateRelationships defines the specific relationships to create in SpiceDB.
	CreateRelationships []StringOrTemplate `json:"creates,omitempty" validate:"omitempty,dive,required_without=touches deletes deleteByFilter"`

	// TouchRelationships defines the specific relationships to touch in SpiceDB.
	TouchRelationships []StringOrTemplate `json:"touches,omitempty" validate:"omitempty,dive,required_without=creates deletes deleteByFilter"`

	// DeleteRelationships defines the specific relationships to delete in SpiceDB.
	DeleteRelationships []StringOrTemplate `json:"deletes,omitempty" validate:"omitempty,dive,required_without=creates touches deleteByFilter"`

	// DeleteByFilter defines a filter to delete relationships in SpiceDB.
	// This is a more flexible way to delete relationships than specifying
	// DeleteRelationships, as it allows for dynamic portions of the relationship
	// to be specified.
	// To specify dynamic portions of the relationship, use the following dollar
	// sign values:
	// - `$resourceType` for the resource type
	// - `$resourceID` for the resource ID
	// - `$resourceRelation` for the relation
	// - `$subjectType` for the subject type
	// - `$subjectID` for the subject ID
	// - `$subjectRelation` for the subject relation
	DeleteByFilter []StringOrTemplate `json:"deleteByFilter,omitempty" validate:"omitempty,dive,required_without=creates touches deletes"`
}

// Match determines which requests the rule applies to
type Match struct {
	GroupVersion string `json:"apiVersion" validate:"required"`
	Resource     string `json:"resource" validate:"required"`

	// The Kubernetes verb to match.
	// See: `verbs` in https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.24/ for reference.
	Verbs []string `json:"verbs" validate:"required,min=1,dive,oneof=get list watch create update patch delete"`
}

// StringOrTemplate either contains a string representing a relationship
// template, or a full RelationshipTemplate definition.
type StringOrTemplate struct {
	Template              string `json:"tpl,inline" validate:"omitempty,min=1,required_without=RelationshipTemplate"`
	*RelationshipTemplate `json:",inline" validate:"omitempty,required_without=tpl"`
}

// PreFilter defines a LookupResources request to filter the results.
// Prefilters work by generating a list of allowed object (name, namespace)
// pairs ahead of / in parallel with the kube request.
type PreFilter struct {
	// FromObjectIDNameExpr is a Bloblang expression defining how to construct an allowed Name from an
	// LR response.
	FromObjectIDNameExpr string `json:"fromObjectIDNameExpr,omitempty" validate:"omitempty,min=1"`

	// FromObjectIDNamespaceExpr is a Bloblang expression defining how to construct an allowed Namespace
	// from an LR  response.
	FromObjectIDNamespaceExpr string `json:"fromObjectIDNamespaceExpr,omitempty" validate:"omitempty,min=1"`

	// LookupMatchingResources is a template defining a LookupResources request to filter on.
	// The resourceID must be set to `$`.
	LookupMatchingResources *StringOrTemplate `json:"lookupMatchingResources,optional" validate:"omitempty"`
}

// PostFilter defines authorization checks to filter the results after the
// Kubernetes API call completes. PostFilters work by checking permissions
// for each returned object and filtering out objects without permission.
type PostFilter struct {
	// CheckPermissionTemplate is a template defining a CheckPermission request to filter on.
	// This template will be applied to each object in the response to determine if it should be included.
	// Use object fields like {{metadata.name}} and {{metadata.namespace}} in the template.
	CheckPermissionTemplate *StringOrTemplate `json:"checkPermissionTemplate" validate:"required"`
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
	validate := validator.New()

	var (
		rules []Config
		rule  Config
	)
	for err := decoder.Decode(&rule); !errors.Is(err, io.EOF); err = decoder.Decode(&rule) {
		if err != nil {
			return nil, err
		}

		if err := validate.Struct(rule); err != nil {
			return nil, err
		}

		rules = append(rules, rule)
		rule = Config{}
	}
	return rules, nil
}
