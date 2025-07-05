package proxyrule

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
