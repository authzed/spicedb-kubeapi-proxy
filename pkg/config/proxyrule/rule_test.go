package proxyrule

import (
	"strings"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRuleParsing(t *testing.T) {
	tests := []struct {
		name        string
		config      string
		expectRules []Config
		expectErr   error
	}{
		{
			name: "single rule (yaml)",
			config: `
apiVersion: authzed.com/v1alpha1
kind: ProxyRule
metadata:
  name: example-rule
lock: Pessimistic
match:
- apiVersion: authzed.com/v1alpha1
  resource: spicedbclusters
  verbs: ["create"]
check:
- resource:
    type: org
    id: metadata.labels.org
    relation: manage-cluster
  subject:
    type: user
    id: request.user
update:
  creates:
  - resource:
      type: spicedbclusters
      id: metadata.name
      relation: org
    subject:
      type: org
      id: metadata.labels.org
  - resource:
      type: spicedbclusters
      id: metadata.name
      relation: creator
    subject:
      type: user
      id: request.user
`,
			expectRules: []Config{{
				TypeMeta: v1alpha1ProxyRule,
				ObjectMeta: v1.ObjectMeta{
					Name: "example-rule",
				},
				Spec: Spec{
					Locking: PessimisticLockMode,
					Matches: []Match{{
						GroupVersion: "authzed.com/v1alpha1",
						Resource:     "spicedbclusters",
						Verbs:        []string{"create"},
					}},
					Checks: []StringOrTemplate{{
						RelationshipTemplate: &RelationshipTemplate{
							Resource: ObjectTemplate{
								Type:     "org",
								ID:       "metadata.labels.org",
								Relation: "manage-cluster",
							},
							Subject: ObjectTemplate{
								Type: "user",
								ID:   "request.user",
							},
						},
					}},
					Update: Update{
						CreateRelationships: []StringOrTemplate{{
							RelationshipTemplate: &RelationshipTemplate{
								Resource: ObjectTemplate{
									Type:     "spicedbclusters",
									ID:       "metadata.name",
									Relation: "org",
								},
								Subject: ObjectTemplate{
									Type: "org",
									ID:   "metadata.labels.org",
								},
							},
						}, {
							RelationshipTemplate: &RelationshipTemplate{
								Resource: ObjectTemplate{
									Type:     "spicedbclusters",
									ID:       "metadata.name",
									Relation: "creator",
								},
								Subject: ObjectTemplate{
									Type: "user",
									ID:   "request.user",
								},
							},
						}},
					},
				},
			}},
		},
		{
			name: "single compact rule (yaml)",
			config: `
apiVersion: authzed.com/v1alpha1
kind: ProxyRule
lock: Pessimistic
match:
- apiVersion: authzed.com/v1alpha1
  resource: spicedbclusters
  verbs: ["create"]
check:
- tpl: "org:{{metadata.labels.org}}#manage-cluster@user:{{request.user}}"
update:
  creates:
  - tpl: "spicedbclusters:{{metadata.name}}#org@org:{{metadata.labels.org}}"
  - tpl: "spicedbclusters:{{metadata.name}}#creator@user:{{request.user}}"
`,
			expectRules: []Config{{
				TypeMeta: v1alpha1ProxyRule,
				Spec: Spec{
					Locking: PessimisticLockMode,
					Matches: []Match{{
						GroupVersion: "authzed.com/v1alpha1",
						Resource:     "spicedbclusters",
						Verbs:        []string{"create"},
					}},
					Checks: []StringOrTemplate{{
						Template: "org:{{metadata.labels.org}}#manage-cluster@user:{{request.user}}",
					}},
					Update: Update{
						CreateRelationships: []StringOrTemplate{{
							Template: "spicedbclusters:{{metadata.name}}#org@org:{{metadata.labels.org}}",
						}, {
							Template: "spicedbclusters:{{metadata.name}}#creator@user:{{request.user}}",
						}},
					},
				},
			}},
		},
		{
			name: "multiple compact rules (yaml)",
			config: `
apiVersion: authzed.com/v1alpha1
kind: ProxyRule
lock: Pessimistic
match:
- apiVersion: authzed.com/v1alpha1
  resource: spicedbclusters
  verbs: ["create"]
check:
- tpl: "org:{{metadata.labels.org}}#manage-cluster@user:{{request.user}}"
update:
  creates:
  - tpl: "spicedbclusters:{{metadata.name}}#org@org:{{metadata.labels.org}}"
  - tpl: "spicedbclusters:{{metadata.name}}#creator@user:{{request.user}}"
---
apiVersion: authzed.com/v1alpha1
kind: ProxyRule
lock: Pessimistic
match:
- apiVersion: authzed.com/v1alpha1
  resource: spicedbclusters
  verbs: ["list"]
prefilter:
- fromObjectIDNameExpr: "{{response.ResourceObjectID}}"
  fromObjectIDNamespaceExpr: "{{request.Namespace}}"
  lookupMatchingResources:
    tpl: "spicedbclusters:$#view@user:{{request.user}}"
`,
			expectRules: []Config{{
				TypeMeta: v1alpha1ProxyRule,
				Spec: Spec{
					Locking: PessimisticLockMode,
					Matches: []Match{{
						GroupVersion: "authzed.com/v1alpha1",
						Resource:     "spicedbclusters",
						Verbs:        []string{"create"},
					}},
					Checks: []StringOrTemplate{{
						Template: "org:{{metadata.labels.org}}#manage-cluster@user:{{request.user}}",
					}},
					Update: Update{
						CreateRelationships: []StringOrTemplate{{
							Template: "spicedbclusters:{{metadata.name}}#org@org:{{metadata.labels.org}}",
						}, {
							Template: "spicedbclusters:{{metadata.name}}#creator@user:{{request.user}}",
						}},
					},
				},
			}, {
				TypeMeta: v1alpha1ProxyRule,
				Spec: Spec{
					Locking: PessimisticLockMode,
					Matches: []Match{{
						GroupVersion: "authzed.com/v1alpha1",
						Resource:     "spicedbclusters",
						Verbs:        []string{"list"},
					}},
					PreFilters: []PreFilter{{
						FromObjectIDNameExpr:      "{{response.ResourceObjectID}}",
						FromObjectIDNamespaceExpr: "{{request.Namespace}}",
						LookupMatchingResources:   &StringOrTemplate{Template: "spicedbclusters:$#view@user:{{request.user}}"},
					}},
				},
			}},
		},
		{
			name: "rule with deleteByFilter (yaml)",
			config: `
apiVersion: authzed.com/v1alpha1
kind: ProxyRule
lock: Pessimistic
match:
- apiVersion: authzed.com/v1alpha1
  resource: spicedbclusters
  verbs: ["delete"]
check:
- tpl: "org:{{metadata.labels.org}}#manage-cluster@user:{{request.user}}"
update:
  deleteByFilter:
  - tpl: "spicedbclusters:{{metadata.name}}#*@*"
`,
			expectRules: []Config{{
				TypeMeta: v1alpha1ProxyRule,
				Spec: Spec{
					Locking: PessimisticLockMode,
					Matches: []Match{{
						GroupVersion: "authzed.com/v1alpha1",
						Resource:     "spicedbclusters",
						Verbs:        []string{"delete"},
					}},
					Checks: []StringOrTemplate{{
						Template: "org:{{metadata.labels.org}}#manage-cluster@user:{{request.user}}",
					}},
					Update: Update{
						DeleteByFilter: []StringOrTemplate{{
							Template: "spicedbclusters:{{metadata.name}}#*@*",
						}},
					},
				},
			}},
		},
		{
			name: "rule with mixed update operations including deleteByFilter",
			config: `
apiVersion: authzed.com/v1alpha1
kind: ProxyRule
lock: Optimistic
match:
- apiVersion: authzed.com/v1alpha1
  resource: spicedbclusters
  verbs: ["update"]
check:
- tpl: "org:{{metadata.labels.org}}#manage-cluster@user:{{request.user}}"
update:
  creates:
  - tpl: "spicedbclusters:{{metadata.name}}#creator@user:{{request.user}}"
  deletes:
  - tpl: "spicedbclusters:{{metadata.name}}#old-creator@user:{{request.oldUser}}"
  deleteByFilter:
  - tpl: "spicedbclusters:{{metadata.name}}#viewer@*"
  - resource:
      type: spicedbclusters
      id: metadata.name
      relation: editor
    subject:
      type: user
      id: "*"
`,
			expectRules: []Config{{
				TypeMeta: v1alpha1ProxyRule,
				Spec: Spec{
					Locking: OptimisticLockMode,
					Matches: []Match{{
						GroupVersion: "authzed.com/v1alpha1",
						Resource:     "spicedbclusters",
						Verbs:        []string{"update"},
					}},
					Checks: []StringOrTemplate{{
						Template: "org:{{metadata.labels.org}}#manage-cluster@user:{{request.user}}",
					}},
					Update: Update{
						CreateRelationships: []StringOrTemplate{{
							Template: "spicedbclusters:{{metadata.name}}#creator@user:{{request.user}}",
						}},
						DeleteRelationships: []StringOrTemplate{{
							Template: "spicedbclusters:{{metadata.name}}#old-creator@user:{{request.oldUser}}",
						}},
						DeleteByFilter: []StringOrTemplate{{
							Template: "spicedbclusters:{{metadata.name}}#viewer@*",
						}, {
							RelationshipTemplate: &RelationshipTemplate{
								Resource: ObjectTemplate{
									Type:     "spicedbclusters",
									ID:       "metadata.name",
									Relation: "editor",
								},
								Subject: ObjectTemplate{
									Type: "user",
									ID:   "*",
								},
							},
						}},
					},
				},
			}},
		},
		{
			name: "rule with deleteByFilter using dollar variables",
			config: `
apiVersion: authzed.com/v1alpha1
kind: ProxyRule
lock: Pessimistic
match:
- apiVersion: authzed.com/v1alpha1
  resource: spicedbclusters
  verbs: ["delete"]
update:
  deleteByFilter:
  - tpl: "$resourceType:$resourceID#$resourceRelation@$subjectType:$subjectID"
`,
			expectRules: []Config{{
				TypeMeta: v1alpha1ProxyRule,
				Spec: Spec{
					Locking: PessimisticLockMode,
					Matches: []Match{{
						GroupVersion: "authzed.com/v1alpha1",
						Resource:     "spicedbclusters",
						Verbs:        []string{"delete"},
					}},
					Update: Update{
						DeleteByFilter: []StringOrTemplate{{
							Template: "$resourceType:$resourceID#$resourceRelation@$subjectType:$subjectID",
						}},
					},
				},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rules, err := Parse(strings.NewReader(tt.config))
			require.Equal(t, tt.expectErr, err, "%v", err)
			require.Equal(t, tt.expectRules, rules)
		})
	}
}

func TestValidation(t *testing.T) {
	validate := validator.New()
	validate.RegisterStructValidation(validateStringOrTemplate, StringOrTemplate{})

	t.Run("Config validation", func(t *testing.T) {
		// Valid config
		validConfig := Config{
			TypeMeta: v1alpha1ProxyRule,
			Spec: Spec{
				Matches: []Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
			},
		}
		err := validate.Struct(validConfig)
		require.NoError(t, err)
	})

	t.Run("Spec validation", func(t *testing.T) {
		tests := []struct {
			name      string
			spec      Spec
			expectErr bool
		}{
			{
				name: "valid spec",
				spec: Spec{
					Matches: []Match{{
						GroupVersion: "v1",
						Resource:     "pods",
						Verbs:        []string{"get"},
					}},
				},
				expectErr: false,
			},
			{
				name: "invalid lock mode",
				spec: Spec{
					Locking: "Invalid",
					Matches: []Match{{
						GroupVersion: "v1",
						Resource:     "pods",
						Verbs:        []string{"get"},
					}},
				},
				expectErr: true,
			},
			{
				name: "valid lock mode - Optimistic",
				spec: Spec{
					Locking: OptimisticLockMode,
					Matches: []Match{{
						GroupVersion: "v1",
						Resource:     "pods",
						Verbs:        []string{"get"},
					}},
				},
				expectErr: false,
			},
			{
				name: "valid lock mode - Pessimistic",
				spec: Spec{
					Locking: PessimisticLockMode,
					Matches: []Match{{
						GroupVersion: "v1",
						Resource:     "pods",
						Verbs:        []string{"get"},
					}},
				},
				expectErr: false,
			},
			{
				name: "missing matches",
				spec: Spec{
					Matches: []Match{},
				},
				expectErr: true,
			},
			{
				name: "nil matches",
				spec: Spec{
					Matches: nil,
				},
				expectErr: true,
			},
			{
				name: "valid CEL expressions",
				spec: Spec{
					Matches: []Match{{
						GroupVersion: "v1",
						Resource:     "pods",
						Verbs:        []string{"get"},
					}},
					If: []string{"request.verb == 'get'", "user.name == 'admin'"},
				},
				expectErr: false,
			},
			{
				name: "empty CEL expression",
				spec: Spec{
					Matches: []Match{{
						GroupVersion: "v1",
						Resource:     "pods",
						Verbs:        []string{"get"},
					}},
					If: []string{""},
				},
				expectErr: false, // Empty strings are allowed due to omitempty
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := validate.Struct(tt.spec)
				if tt.expectErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("Match validation", func(t *testing.T) {
		tests := []struct {
			name      string
			match     Match
			expectErr bool
		}{
			{
				name: "valid match",
				match: Match{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get", "list"},
				},
				expectErr: false,
			},
			{
				name: "missing GroupVersion",
				match: Match{
					Resource: "pods",
					Verbs:    []string{"get"},
				},
				expectErr: true,
			},
			{
				name: "missing Resource",
				match: Match{
					GroupVersion: "v1",
					Verbs:        []string{"get"},
				},
				expectErr: true,
			},
			{
				name: "missing Verbs",
				match: Match{
					GroupVersion: "v1",
					Resource:     "pods",
				},
				expectErr: true,
			},
			{
				name: "empty Verbs",
				match: Match{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{},
				},
				expectErr: true,
			},
			{
				name: "invalid verb",
				match: Match{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"invalid"},
				},
				expectErr: true,
			},
			{
				name: "valid verbs",
				match: Match{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get", "list", "watch", "create", "update", "patch", "delete"},
				},
				expectErr: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := validate.Struct(tt.match)
				if tt.expectErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("Update validation", func(t *testing.T) {
		tests := []struct {
			name      string
			update    Update
			expectErr bool
		}{
			{
				name: "valid update with creates",
				update: Update{
					CreateRelationships: []StringOrTemplate{{
						Template: "pod:test#view@user:admin",
					}},
				},
				expectErr: false,
			},
			{
				name: "valid update with touches",
				update: Update{
					TouchRelationships: []StringOrTemplate{{
						Template: "pod:test#view@user:admin",
					}},
				},
				expectErr: false,
			},
			{
				name: "valid update with deletes",
				update: Update{
					DeleteRelationships: []StringOrTemplate{{
						Template: "pod:test#view@user:admin",
					}},
				},
				expectErr: false,
			},
			{
				name: "valid update with deleteByFilter",
				update: Update{
					DeleteByFilter: []StringOrTemplate{{
						Template: "pod:test#view@*",
					}},
				},
				expectErr: false,
			},
			{
				name: "valid update with preconditions",
				update: Update{
					PreconditionExists: []StringOrTemplate{{
						Template: "pod:test#exist@user:admin",
					}},
					PreconditionDoesNotExist: []StringOrTemplate{{
						Template: "pod:test#not-exist@user:admin",
					}},
					CreateRelationships: []StringOrTemplate{{
						Template: "pod:test#view@user:admin",
					}},
				},
				expectErr: false,
			},
			{
				name: "mixed operations",
				update: Update{
					CreateRelationships: []StringOrTemplate{{
						Template: "pod:test#view@user:admin",
					}},
					TouchRelationships: []StringOrTemplate{{
						Template: "pod:test#edit@user:admin",
					}},
					DeleteRelationships: []StringOrTemplate{{
						Template: "pod:test#old@user:admin",
					}},
					DeleteByFilter: []StringOrTemplate{{
						Template: "pod:test#temp@*",
					}},
				},
				expectErr: false,
			},
			{
				name:      "empty update - should pass due to omitempty",
				update:    Update{},
				expectErr: false, // Empty updates are valid due to omitempty
			},
			{
				name: "empty update arrays - should pass due to omitempty",
				update: Update{
					CreateRelationships: []StringOrTemplate{},
					TouchRelationships:  []StringOrTemplate{},
					DeleteRelationships: []StringOrTemplate{},
					DeleteByFilter:      []StringOrTemplate{},
				},
				expectErr: false, // Empty arrays are valid due to omitempty
			},
			{
				name: "valid update with tupleSet",
				update: Update{
					CreateRelationships: []StringOrTemplate{{
						TupleSet: `object.spec.containers.map_each("pod:" + namespacedName + "#has-container@container:" + this.name)`,
					}},
				},
				expectErr: false,
			},
			{
				name: "invalid - tupleSet and tpl together",
				update: Update{
					CreateRelationships: []StringOrTemplate{{
						Template: "pod:test#view@user:admin",
						TupleSet: `object.spec.containers.map_each("pod:" + namespacedName + "#has-container@container:" + this.name)`,
					}},
				},
				expectErr: true,
			},
			{
				name: "invalid - tupleSet and RelationshipTemplate together",
				update: Update{
					CreateRelationships: []StringOrTemplate{{
						TupleSet: `object.spec.containers.map_each("pod:" + namespacedName + "#has-container@container:" + this.name)`,
						RelationshipTemplate: &RelationshipTemplate{
							Resource: ObjectTemplate{Type: "pod", ID: "test"},
							Subject:  ObjectTemplate{Type: "user", ID: "admin"},
						},
					}},
				},
				expectErr: true,
			},
			{
				name: "valid example configuration - deployment with containers",
				update: Update{
					CreateRelationships: []StringOrTemplate{
						{Template: "deployment:{{namespacedName}}#creator@user:{{user.name}}"},
						{Template: "deployment:{{namespacedName}}#namespace@namespace:{{namespace}}"},
						{TupleSet: `["server", "config-reloader", "proxy-sidecar"].map_each("deployment:default/example#has-container@container:" + this)`},
						{TupleSet: `[].map_each("deployment:default/example#has-init-container@container:" + this)`},
					},
				},
				expectErr: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := validate.Struct(tt.update)
				if tt.expectErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("StringOrTemplate validation", func(t *testing.T) {
		tests := []struct {
			name      string
			sot       StringOrTemplate
			expectErr bool
		}{
			{
				name: "valid template string",
				sot: StringOrTemplate{
					Template: "pod:test#view@user:admin",
				},
				expectErr: false,
			},
			{
				name: "valid relationship template",
				sot: StringOrTemplate{
					RelationshipTemplate: &RelationshipTemplate{
						Resource: ObjectTemplate{
							Type: "pod",
							ID:   "test",
						},
						Subject: ObjectTemplate{
							Type: "user",
							ID:   "admin",
						},
					},
				},
				expectErr: false,
			},
			{
				name: "empty template string",
				sot: StringOrTemplate{
					Template: "",
				},
				expectErr: true, // Empty template requires at least one field
			},
			{
				name: "both template and relationship template",
				sot: StringOrTemplate{
					Template: "pod:test#view@user:admin",
					RelationshipTemplate: &RelationshipTemplate{
						Resource: ObjectTemplate{
							Type: "pod",
							ID:   "test",
						},
						Subject: ObjectTemplate{
							Type: "user",
							ID:   "admin",
						},
					},
				},
				expectErr: true, // Should fail due to mutual exclusion
			},
			{
				name:      "neither template nor relationship template",
				sot:       StringOrTemplate{},
				expectErr: true, // Empty structs should fail - at least one field required
			},
			{
				name: "relationship template with empty resource type",
				sot: StringOrTemplate{
					RelationshipTemplate: &RelationshipTemplate{
						Resource: ObjectTemplate{
							Type: "",
							ID:   "test",
						},
						Subject: ObjectTemplate{
							Type: "user",
							ID:   "admin",
						},
					},
				},
				expectErr: false, // ObjectTemplate has no validation tags
			},
			{
				name: "relationship template with empty subject type",
				sot: StringOrTemplate{
					RelationshipTemplate: &RelationshipTemplate{
						Resource: ObjectTemplate{
							Type: "pod",
							ID:   "test",
						},
						Subject: ObjectTemplate{
							Type: "",
							ID:   "admin",
						},
					},
				},
				expectErr: false, // ObjectTemplate has no validation tags
			},
			{
				name: "test required_without validation - only template",
				sot: StringOrTemplate{
					Template: "pod:test#view@user:admin",
				},
				expectErr: false, // Template is present, so RelationshipTemplate not required
			},
			{
				name: "test required_without validation - only relationship template",
				sot: StringOrTemplate{
					RelationshipTemplate: &RelationshipTemplate{
						Resource: ObjectTemplate{
							Type: "pod",
							ID:   "test",
						},
						Subject: ObjectTemplate{
							Type: "user",
							ID:   "admin",
						},
					},
				},
				expectErr: false, // RelationshipTemplate is present, so template not required
			},
			{
				name: "valid tupleSet only",
				sot: StringOrTemplate{
					TupleSet: `["test1", "test2"].map_each("resource:" + this + "#rel@user:admin")`,
				},
				expectErr: false,
			},
			{
				name: "empty tupleSet string",
				sot: StringOrTemplate{
					TupleSet: "",
				},
				expectErr: true, // Empty tupleSet requires at least one field
			},
			{
				name: "both template and tupleSet",
				sot: StringOrTemplate{
					Template: "pod:test#view@user:admin",
					TupleSet: `["test"]`,
				},
				expectErr: true, // Should fail due to mutual exclusion
			},
			{
				name: "both tupleSet and relationship template",
				sot: StringOrTemplate{
					TupleSet: `["test"]`,
					RelationshipTemplate: &RelationshipTemplate{
						Resource: ObjectTemplate{
							Type: "pod",
							ID:   "test",
						},
						Subject: ObjectTemplate{
							Type: "user",
							ID:   "admin",
						},
					},
				},
				expectErr: true, // Should fail due to mutual exclusion
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := validate.Struct(tt.sot)
				if tt.expectErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("PreFilter validation", func(t *testing.T) {
		tests := []struct {
			name      string
			preFilter PreFilter
			expectErr bool
		}{
			{
				name: "valid prefilter",
				preFilter: PreFilter{
					FromObjectIDNameExpr:      "{{response.ResourceObjectID}}",
					FromObjectIDNamespaceExpr: "{{request.Namespace}}",
					LookupMatchingResources: &StringOrTemplate{
						Template: "pod:$#view@user:admin",
					},
				},
				expectErr: false,
			},
			{
				name: "empty name expression",
				preFilter: PreFilter{
					FromObjectIDNameExpr:      "",
					FromObjectIDNamespaceExpr: "{{request.Namespace}}",
					LookupMatchingResources: &StringOrTemplate{
						Template: "pod:$#view@user:admin",
					},
				},
				expectErr: false, // Empty strings are allowed due to omitempty
			},
			{
				name: "empty namespace expression",
				preFilter: PreFilter{
					FromObjectIDNameExpr:      "{{response.ResourceObjectID}}",
					FromObjectIDNamespaceExpr: "",
					LookupMatchingResources: &StringOrTemplate{
						Template: "pod:$#view@user:admin",
					},
				},
				expectErr: false, // Empty strings are allowed due to omitempty
			},
			{
				name: "minimal valid prefilter",
				preFilter: PreFilter{
					FromObjectIDNameExpr: "{{response.ResourceObjectID}}",
				},
				expectErr: false,
			},
			{
				name: "minimal valid prefilter with namespace",
				preFilter: PreFilter{
					FromObjectIDNamespaceExpr: "{{request.Namespace}}",
				},
				expectErr: false,
			},
			{
				name:      "empty prefilter",
				preFilter: PreFilter{},
				expectErr: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := validate.Struct(tt.preFilter)
				if tt.expectErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("Integration validation tests", func(t *testing.T) {
		// Test validation through the Parse function (integration test)
		tests := []struct {
			name      string
			config    string
			expectErr bool
		}{
			{
				name: "valid config with all fields",
				config: `
apiVersion: authzed.com/v1alpha1
kind: ProxyRule
metadata:
  name: example-rule
lock: Optimistic
match:
- apiVersion: v1
  resource: pods
  verbs: ["get", "list"]
if:
- "request.verb == 'get'"
- "user.name == 'admin'"
check:
- tpl: "namespace:{{resourceNamespace}}#view@user:{{user.name}}"
prefilter:
- fromObjectIDNameExpr: "{{response.ResourceObjectID}}"
  fromObjectIDNamespaceExpr: "{{request.Namespace}}"
  lookupMatchingResources:
    tpl: "pod:$#view@user:{{user.name}}"
update:
  preconditionExists:
  - tpl: "namespace:{{resourceNamespace}}#exist@user:{{user.name}}"
  creates:
  - tpl: "pod:{{name}}#creator@user:{{user.name}}"
`,
				expectErr: false,
			},
			{
				name: "invalid - missing required matches",
				config: `
apiVersion: authzed.com/v1alpha1
kind: ProxyRule
metadata:
  name: example-rule
lock: Optimistic
check:
- tpl: "namespace:{{resourceNamespace}}#view@user:{{user.name}}"
`,
				expectErr: true,
			},
			{
				name: "invalid - invalid lock mode",
				config: `
apiVersion: authzed.com/v1alpha1
kind: ProxyRule
metadata:
	name: example-rule
lock: Invalid
match:
- apiVersion: v1
  resource: pods
  verbs: ["get"]
`,
				expectErr: true,
			},
			{
				name: "invalid - invalid verb",
				config: `
apiVersion: authzed.com/v1alpha1
kind: ProxyRule
metadata:
  name: example-rule
match:
- apiVersion: v1
  resource: pods
  verbs: ["invalid"]
`,
				expectErr: true,
			},
			{
				name: "invalid - missing required fields in match",
				config: `
apiVersion: authzed.com/v1alpha1
kind: ProxyRule
metadata:
	name: example-rule
match:
- resource: pods
  verbs: ["get"]
`,
				expectErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := Parse(strings.NewReader(tt.config))
				if tt.expectErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})
}
