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
					Updates: []StringOrTemplate{{
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
					Updates: []StringOrTemplate{{
						Template: "spicedbclusters:{{metadata.name}}#org@org:{{metadata.labels.org}}",
					}, {
						Template: "spicedbclusters:{{metadata.name}}#creator@user:{{request.user}}",
					}},
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
					Updates: []StringOrTemplate{{
						Template: "spicedbclusters:{{metadata.name}}#org@org:{{metadata.labels.org}}",
					}, {
						Template: "spicedbclusters:{{metadata.name}}#creator@user:{{request.user}}",
					}},
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rules, err := Parse(strings.NewReader(tt.config))
			require.Equal(t, tt.expectErr, err, "%v", err)
			require.Equal(t, tt.expectRules, rules)
		})
	}
}
