package rules

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/bloblang"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
)

// mustCompileBloblang is a helper function for tests
func mustCompileBloblang(expr string) *bloblang.Executor {
	exec, err := bloblang.Parse(expr)
	if err != nil {
		panic(fmt.Sprintf("failed to compile bloblang expression %q: %v", expr, err))
	}
	return exec
}

func TestParseRelString(t *testing.T) {
	tests := []struct {
		name    string
		tpl     string
		want    *UncompiledRelExpr
		wantErr bool
	}{
		{
			name: "basic",
			tpl:  "test:1#rel@stuff:2",
			want: &UncompiledRelExpr{
				ResourceType:     "test",
				ResourceID:       "1",
				ResourceRelation: "rel",
				SubjectType:      "stuff",
				SubjectID:        "2",
			},
		},
		{
			name: "subject relation",
			tpl:  "test:1#rel@stuff:2#optional",
			want: &UncompiledRelExpr{
				ResourceType:     "test",
				ResourceID:       "1",
				ResourceRelation: "rel",
				SubjectType:      "stuff",
				SubjectID:        "2",
				SubjectRelation:  "optional",
			},
		},
		{
			name: "templated ids",
			tpl:  "org:{{.metadata.labels.org}}#audit-cluster@user:{{.request.user}}",
			want: &UncompiledRelExpr{
				ResourceType:     "org",
				ResourceID:       "{{.metadata.labels.org}}",
				ResourceRelation: "audit-cluster",
				SubjectType:      "user",
				SubjectID:        "{{.request.user}}",
			},
		},
		{
			name: "templated everything",
			tpl:  "{{.kind}}:{{.metadata.labels.org}}#{{.metadata.labels.rel}}@{{.request.userKind}}:{{.request.user}}#{{request.usersubjectrel}}",
			want: &UncompiledRelExpr{
				ResourceType:     "{{.kind}}",
				ResourceID:       "{{.metadata.labels.org}}",
				ResourceRelation: "{{.metadata.labels.rel}}",
				SubjectType:      "{{.request.userKind}}",
				SubjectID:        "{{.request.user}}",
				SubjectRelation:  "{{request.usersubjectrel}}",
			},
		},
		{
			name: "templated with bloblang features that use special characters",
			tpl:  "org:{{locations[?state == 'WA'].name | sort(@)[0]}}#audit-cluster@user:{{sort_by(Contents, &Date)[*].{Key: Key, Size: Size}}}",
			want: &UncompiledRelExpr{
				ResourceType:     "org",
				ResourceID:       "{{locations[?state == 'WA'].name | sort(@)[0]}}",
				ResourceRelation: "audit-cluster",
				SubjectType:      "user",
				SubjectID:        "{{sort_by(Contents, &Date)[*].{Key: Key, Size: Size}}}",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseRelSring(tt.tpl)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseRelSring() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseRelSring() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompileBloblangExpression(t *testing.T) {
	tests := []struct {
		name           string
		expr           string
		data           []byte
		want           any
		wantCompileErr string
		wantSearchErr  string
	}{
		{
			name: "no expression",
			expr: "hello",
			data: []byte(`{"matters": "not"}`),
			want: "hello",
		},
		{
			name: "maybe a typo, treated as a literal",
			expr: "hello }}",
			data: []byte(`{"matters": "not"}`),
			want: "hello ",
		},
		{
			name: "expression over data",
			expr: "{{matters}}",
			data: []byte(`{"matters": "yes"}`),
			want: "yes",
		},
		{
			name: "non-matching expression",
			expr: "{{matters}}",
			data: []byte(`{"virus": "veryyes"}`),
			want: nil,
		},
		{
			name:           "invalid expression",
			expr:           "{{.matters}}",
			data:           []byte(`{"matters": "yes"}`),
			wantCompileErr: "expected import, map, or assignment",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := CompileBloblangExpression(tt.expr)
			if len(tt.wantCompileErr) > 0 {
				require.Contains(t, err.Error(), tt.wantCompileErr)
				return
			} else {
				require.NoError(t, err)
			}
			var data interface{}
			require.NoError(t, json.Unmarshal(tt.data, &data))
			got, searchErr := expr.Query(data)

			// TODO: not sure how to trigger a search error
			if len(tt.wantSearchErr) > 0 {
				require.Contains(t, searchErr.Error(), tt.wantSearchErr)
				return
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCompile(t *testing.T) {
	testDataBytes := []byte(`{
		"metadata": {
			"name": "testName",
			"namespace": "",
			"labels": {"org": "testOrg"}
		},
		"namespace": "",
		"user": {
          "name": "testUser",
          "groups": ["testGroup"]
        },
        "request": {
          "user": "testUser",
          "group": "testGroup"
        },
        "response": {
          "ResourceObjectID": "foo"
        }
	}`)
	var testData any
	require.NoError(t, json.Unmarshal(testDataBytes, &testData))

	type result struct {
		checks          []ResolvedRel
		postchecks      []ResolvedRel
		creates         []ResolvedRel
		touches         []ResolvedRel
		deletes         []ResolvedRel
		deletesByFilter []ResolvedRel
		mustExist       []ResolvedRel
		mustNotExist    []ResolvedRel
		filters         []ResolvedPreFilter
	}

	mustQuery := func(executor *bloblang.Executor) any {
		val, err := executor.Query(testData)
		require.NoError(t, err)
		return val
	}

	requireEqualUnderTestData := func(exprs []RelationshipExpr, res []ResolvedRel) {
		for i, expr := range exprs {
			// For these tests, we only expect RelExpr, not TupleSetExpr
			c, ok := expr.(*RelExpr)
			require.True(t, ok, "Expected RelExpr in test, got %T", expr)

			require.Equal(t, res[i].ResourceType, mustQuery(c.ResourceType))
			require.Equal(t, res[i].ResourceID, mustQuery(c.ResourceID))
			require.Equal(t, res[i].ResourceRelation, mustQuery(c.ResourceRelation))
			require.Equal(t, res[i].SubjectType, mustQuery(c.SubjectType))
			require.Equal(t, res[i].SubjectID, mustQuery(c.SubjectID))
			if c.SubjectRelation != nil {
				require.Equal(t, res[i].SubjectRelation, mustQuery(c.SubjectRelation))
			}
		}
	}

	requireFilterEqualUnderTestData := func(t *testing.T, filter []*PreFilter, res []ResolvedPreFilter) {
		for i, f := range filter {
			require.Equal(t, f.LookupType, res[i].LookupType)
			require.Equal(t, mustQuery(res[i].NameFromObjectID), mustQuery(f.NameFromObjectID))
			require.Equal(t, mustQuery(res[i].NamespaceFromObjectID), mustQuery(f.NamespaceFromObjectID))
			requireEqualUnderTestData([]RelationshipExpr{f.Rel}, []ResolvedRel{*res[i].Rel})
		}
	}

	tests := []struct {
		name    string
		config  proxyrule.Config
		want    result
		wantErr error
	}{
		{
			name: "write rule",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "example.com/v1alpha1",
					Resource:     "wardles",
					Verbs:        []string{"create"},
				}},
				Checks: []proxyrule.StringOrTemplate{{
					Template: "org:{{metadata.labels.org}}#manage-wardles@user:{{user.name}}",
				}},
				Update: proxyrule.Update{
					CreateRelationships: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#org@org:{{metadata.labels.org}}",
					}, {
						Template: "wardles:{{metadata.name}}#creator@user:{{user.name}}",
					}},
				},
			}},
			want: result{
				checks: []ResolvedRel{{
					ResourceType:     "org",
					ResourceID:       "testOrg",
					ResourceRelation: "manage-wardles",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				creates: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "org",
					SubjectType:      "org",
					SubjectID:        "testOrg",
				}, {
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "creator",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				filters: []ResolvedPreFilter{},
			},
		},
		{
			name: "list rule",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "example.com/v1alpha1",
					Resource:     "wardles",
					Verbs:        []string{"list"},
				}},
				Checks: []proxyrule.StringOrTemplate{{
					Template: "org:{{metadata.labels.org}}#audit-wardles@group:{{user.groups.index(0)}}#member",
				}},
				PreFilters: []proxyrule.PreFilter{{
					FromObjectIDNameExpr: "{{response.ResourceObjectID}}",
					LookupMatchingResources: &proxyrule.StringOrTemplate{
						RelationshipTemplate: &proxyrule.RelationshipTemplate{
							Resource: proxyrule.ObjectTemplate{
								Type:     "wardles",
								ID:       "$",
								Relation: "view",
							},
							Subject: proxyrule.ObjectTemplate{
								Type: "user",
								ID:   "{{user.name}}",
							},
						},
					},
				}},
			}},
			want: result{
				checks: []ResolvedRel{{
					ResourceType:     "org",
					ResourceID:       "testOrg",
					ResourceRelation: "audit-wardles",
					SubjectType:      "group",
					SubjectID:        "testGroup",
					SubjectRelation:  "member",
				}},
				filters: []ResolvedPreFilter{{
					LookupType:            LookupTypeResource,
					NameFromObjectID:      mustCompileBloblang("response.ResourceObjectID"),
					NamespaceFromObjectID: mustCompileBloblang(`""`),
					Rel: &ResolvedRel{
						ResourceType:     "wardles",
						ResourceID:       "$",
						ResourceRelation: "view",
						SubjectType:      "user",
						SubjectID:        "testUser",
					},
				}},
			},
		},
		{
			name: "rule with touches and deletes",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "example.com/v1alpha1",
					Resource:     "wardles",
					Verbs:        []string{"update"},
				}},
				Checks: []proxyrule.StringOrTemplate{{
					Template: "org:{{metadata.labels.org}}#manage-wardles@user:{{user.name}}",
				}},
				Update: proxyrule.Update{
					TouchRelationships: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#last-modified@user:{{user.name}}",
					}},
					DeleteRelationships: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#temp-access@user:*",
					}},
				},
			}},
			want: result{
				checks: []ResolvedRel{{
					ResourceType:     "org",
					ResourceID:       "testOrg",
					ResourceRelation: "manage-wardles",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				touches: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "last-modified",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				deletes: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "temp-access",
					SubjectType:      "user",
					SubjectID:        "*",
				}},
				filters: []ResolvedPreFilter{},
			},
		},
		{
			name: "rule with preconditions",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "example.com/v1alpha1",
					Resource:     "wardles",
					Verbs:        []string{"delete"},
				}},
				Checks: []proxyrule.StringOrTemplate{{
					Template: "org:{{metadata.labels.org}}#manage-wardles@user:{{user.name}}",
				}},
				Update: proxyrule.Update{
					PreconditionExists: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#creator@user:{{user.name}}",
					}},
					PreconditionDoesNotExist: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#protected@user:*",
					}},
					DeleteRelationships: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#creator@user:{{user.name}}",
					}},
				},
			}},
			want: result{
				checks: []ResolvedRel{{
					ResourceType:     "org",
					ResourceID:       "testOrg",
					ResourceRelation: "manage-wardles",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				mustExist: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "creator",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				mustNotExist: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "protected",
					SubjectType:      "user",
					SubjectID:        "*",
				}},
				deletes: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "creator",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				filters: []ResolvedPreFilter{},
			},
		},
		{
			name: "complex rule with all update types",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "example.com/v1alpha1",
					Resource:     "wardles",
					Verbs:        []string{"patch"},
				}},
				Checks: []proxyrule.StringOrTemplate{{
					Template: "org:{{metadata.labels.org}}#manage-wardles@user:{{user.name}}",
				}},
				Update: proxyrule.Update{
					PreconditionExists: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#exists@user:*",
					}},
					CreateRelationships: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#patched-by@user:{{user.name}}",
					}},
					TouchRelationships: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#last-modified@user:{{user.name}}",
					}},
					DeleteRelationships: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#old-data@user:*",
					}},
				},
			}},
			want: result{
				checks: []ResolvedRel{{
					ResourceType:     "org",
					ResourceID:       "testOrg",
					ResourceRelation: "manage-wardles",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				mustExist: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "exists",
					SubjectType:      "user",
					SubjectID:        "*",
				}},
				creates: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "patched-by",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				touches: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "last-modified",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				deletes: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "old-data",
					SubjectType:      "user",
					SubjectID:        "*",
				}},
				filters: []ResolvedPreFilter{},
			},
		},
		{
			name: "rule with delete by filter",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "example.com/v1alpha1",
					Resource:     "wardles",
					Verbs:        []string{"delete"},
				}},
				Checks: []proxyrule.StringOrTemplate{{
					Template: "org:{{metadata.labels.org}}#manage-wardles@user:{{user.name}}",
				}},
				Update: proxyrule.Update{
					DeleteByFilter: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#*@user:{{user.name}}",
					}},
				},
			}},
			want: result{
				checks: []ResolvedRel{{
					ResourceType:     "org",
					ResourceID:       "testOrg",
					ResourceRelation: "manage-wardles",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				deletesByFilter: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "*",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				filters: []ResolvedPreFilter{},
			},
		},
		{
			name: "rule with multiple delete by filter operations",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "example.com/v1alpha1",
					Resource:     "wardles",
					Verbs:        []string{"update"},
				}},
				Checks: []proxyrule.StringOrTemplate{{
					Template: "org:{{metadata.labels.org}}#manage-wardles@user:{{user.name}}",
				}},
				Update: proxyrule.Update{
					DeleteByFilter: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#temp-access@user:*",
					}, {
						Template: "wardles:{{metadata.name}}#legacy-perm@group:*",
					}},
				},
			}},
			want: result{
				checks: []ResolvedRel{{
					ResourceType:     "org",
					ResourceID:       "testOrg",
					ResourceRelation: "manage-wardles",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				deletesByFilter: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "temp-access",
					SubjectType:      "user",
					SubjectID:        "*",
				}, {
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "legacy-perm",
					SubjectType:      "group",
					SubjectID:        "*",
				}},
				filters: []ResolvedPreFilter{},
			},
		},
		{
			name: "rule with delete by filter and other operations",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "example.com/v1alpha1",
					Resource:     "wardles",
					Verbs:        []string{"patch"},
				}},
				Checks: []proxyrule.StringOrTemplate{{
					Template: "org:{{metadata.labels.org}}#manage-wardles@user:{{user.name}}",
				}},
				Update: proxyrule.Update{
					CreateRelationships: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#updated-by@user:{{user.name}}",
					}},
					TouchRelationships: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#last-modified@user:{{user.name}}",
					}},
					DeleteRelationships: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#old-state@user:{{user.name}}",
					}},
					DeleteByFilter: []proxyrule.StringOrTemplate{{
						Template: "wardles:{{metadata.name}}#temp-*@user:*",
					}},
				},
			}},
			want: result{
				checks: []ResolvedRel{{
					ResourceType:     "org",
					ResourceID:       "testOrg",
					ResourceRelation: "manage-wardles",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				creates: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "updated-by",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				touches: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "last-modified",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				deletes: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "old-state",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				deletesByFilter: []ResolvedRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "temp-*",
					SubjectType:      "user",
					SubjectID:        "*",
				}},
				filters: []ResolvedPreFilter{},
			},
		},
		{
			name: "get rule with postchecks",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
				Checks: []proxyrule.StringOrTemplate{{
					Template: "org:{{metadata.labels.org}}#view-pods@user:{{user.name}}",
				}},
				PostChecks: []proxyrule.StringOrTemplate{{
					Template: "pod:{{metadata.name}}#read-after@user:{{user.name}}",
				}, {
					Template: "namespace:{{namespace}}#audit-access@user:{{user.name}}",
				}},
			}},
			want: result{
				checks: []ResolvedRel{{
					ResourceType:     "org",
					ResourceID:       "testOrg",
					ResourceRelation: "view-pods",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				postchecks: []ResolvedRel{{
					ResourceType:     "pod",
					ResourceID:       "testName",
					ResourceRelation: "read-after",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}, {
					ResourceType:     "namespace",
					ResourceID:       "",
					ResourceRelation: "audit-access",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				filters: []ResolvedPreFilter{},
			},
		},
		{
			name: "prefilter with non-$ resource ID",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "example.com/v1alpha1",
					Resource:     "wardles",
					Verbs:        []string{"list"},
				}},
				PreFilters: []proxyrule.PreFilter{{
					FromObjectIDNameExpr: "{{response.ResourceObjectID}}",
					LookupMatchingResources: &proxyrule.StringOrTemplate{
						RelationshipTemplate: &proxyrule.RelationshipTemplate{
							Resource: proxyrule.ObjectTemplate{
								Type:     "wardles",
								ID:       "invalid-id",
								Relation: "view",
							},
							Subject: proxyrule.ObjectTemplate{
								Type: "user",
								ID:   "{{user.name}}",
							},
						},
					},
				}},
			}},
			wantErr: fmt.Errorf("LookupMatchingResources resourceID must be set to $ to match all resources, got \"invalid-id\""),
		},
		{
			name: "postcheck with get verb should succeed",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
				PostChecks: []proxyrule.StringOrTemplate{{
					Template: "pod:{{metadata.name}}#audit@user:{{user.name}}",
				}},
			}},
			want: result{
				postchecks: []ResolvedRel{{
					ResourceType:     "pod",
					ResourceID:       "testName",
					ResourceRelation: "audit",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				filters: []ResolvedPreFilter{},
			},
		},
		{
			name: "postcheck with create verb should fail",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"create"},
				}},
				PostChecks: []proxyrule.StringOrTemplate{{
					Template: "pod:{{metadata.name}}#audit@user:{{user.name}}",
				}},
			}},
			wantErr: fmt.Errorf("PostCheck operations cannot be used with verb \"create\". PostChecks only apply to read-only operations like 'get'"),
		},
		{
			name: "postcheck with list verb should fail",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"list"},
				}},
				PostChecks: []proxyrule.StringOrTemplate{{
					Template: "pod:{{metadata.name}}#audit@user:{{user.name}}",
				}},
			}},
			wantErr: fmt.Errorf("PostCheck operations cannot be used with verb \"list\". PostChecks only apply to read-only operations like 'get'"),
		},
		{
			name: "postcheck with watch verb should fail",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"watch"},
				}},
				PostChecks: []proxyrule.StringOrTemplate{{
					Template: "pod:{{metadata.name}}#audit@user:{{user.name}}",
				}},
			}},
			wantErr: fmt.Errorf("PostCheck operations cannot be used with verb \"watch\". PostChecks only apply to read-only operations like 'get'"),
		},
		{
			name: "postcheck with update verb should fail",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"update"},
				}},
				PostChecks: []proxyrule.StringOrTemplate{{
					Template: "pod:{{metadata.name}}#audit@user:{{user.name}}",
				}},
			}},
			wantErr: fmt.Errorf("PostCheck operations cannot be used with verb \"update\". PostChecks only apply to read-only operations like 'get'"),
		},
		{
			name: "postcheck with patch verb should fail",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"patch"},
				}},
				PostChecks: []proxyrule.StringOrTemplate{{
					Template: "pod:{{metadata.name}}#audit@user:{{user.name}}",
				}},
			}},
			wantErr: fmt.Errorf("PostCheck operations cannot be used with verb \"patch\". PostChecks only apply to read-only operations like 'get'"),
		},
		{
			name: "postcheck with delete verb should fail",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"delete"},
				}},
				PostChecks: []proxyrule.StringOrTemplate{{
					Template: "pod:{{metadata.name}}#audit@user:{{user.name}}",
				}},
			}},
			wantErr: fmt.Errorf("PostCheck operations cannot be used with verb \"delete\". PostChecks only apply to read-only operations like 'get'"),
		},
		{
			name: "postcheck with mixed verbs should fail",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get", "create"},
				}},
				PostChecks: []proxyrule.StringOrTemplate{{
					Template: "pod:{{metadata.name}}#audit@user:{{user.name}}",
				}},
			}},
			wantErr: fmt.Errorf("PostCheck operations cannot be used with verb \"create\". PostChecks only apply to read-only operations like 'get'"),
		},
		{
			name: "prefilter with template expression evaluating to literal value",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GroupVersion: "example.com/v1alpha1",
					Resource:     "wardles",
					Verbs:        []string{"list"},
				}},
				PreFilters: []proxyrule.PreFilter{{
					FromObjectIDNameExpr: "{{response.ResourceObjectID}}",
					LookupMatchingResources: &proxyrule.StringOrTemplate{
						RelationshipTemplate: &proxyrule.RelationshipTemplate{
							Resource: proxyrule.ObjectTemplate{
								Type:     "wardles",
								ID:       `{{"literal-value"}}`,
								Relation: "view",
							},
							Subject: proxyrule.ObjectTemplate{
								Type: "user",
								ID:   "{{user.name}}",
							},
						},
					},
				}},
			}},
			wantErr: fmt.Errorf("LookupMatchingResources resourceID must be set to $ to match all resources, got \"literal-value\""),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Compile(tt.config)
			if tt.wantErr != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr.Error())
				return
			}
			require.NoError(t, err)

			requireEqualUnderTestData(got.Checks, tt.want.checks)
			requireEqualUnderTestData(got.PostChecks, tt.want.postchecks)
			if got.Update != nil {
				if got.Update.Creates != nil {
					requireEqualUnderTestData(got.Update.Creates, tt.want.creates)
				}
				if got.Update.Touches != nil {
					requireEqualUnderTestData(got.Update.Touches, tt.want.touches)
				}
				if got.Update.Deletes != nil {
					requireEqualUnderTestData(got.Update.Deletes, tt.want.deletes)
				}
				if got.Update.DeletesByFilter != nil {
					requireEqualUnderTestData(got.Update.DeletesByFilter, tt.want.deletesByFilter)
				}
				if got.Update.MustExist != nil {
					requireEqualUnderTestData(got.Update.MustExist, tt.want.mustExist)
				}
				if got.Update.MustNotExist != nil {
					requireEqualUnderTestData(got.Update.MustNotExist, tt.want.mustNotExist)
				}
			}
			requireFilterEqualUnderTestData(t, got.PreFilter, tt.want.filters)
		})
	}
}

func TestCELConditions(t *testing.T) {
	tests := []struct {
		name    string
		config  proxyrule.Config
		input   *ResolveInput
		want    bool
		wantErr bool
	}{
		{
			name: "no CEL conditions - should pass",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
			}},
			input: &ResolveInput{
				Request: &request.RequestInfo{Verb: "get", Resource: "pods"},
			},
			want: true,
		},
		{
			name: "CEL condition on request verb - should pass",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
				If: []string{"request.verb == 'get'"},
			}},
			input: &ResolveInput{
				Request: &request.RequestInfo{Verb: "get", Resource: "pods"},
			},
			want: true,
		},
		{
			name: "CEL condition on request verb - should fail",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
				If: []string{"request.verb == 'create'"},
			}},
			input: &ResolveInput{
				Request: &request.RequestInfo{Verb: "get", Resource: "pods"},
			},
			want: false,
		},
		{
			name: "CEL condition on user name - should pass",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
				If: []string{"user.name == 'admin'"},
			}},
			input: &ResolveInput{
				Request: &request.RequestInfo{Verb: "get", Resource: "pods"},
				User:    &user.DefaultInfo{Name: "admin"},
			},
			want: true,
		},
		{
			name: "CEL condition on user name - should fail",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
				If: []string{"user.name == 'admin'"},
			}},
			input: &ResolveInput{
				Request: &request.RequestInfo{Verb: "get", Resource: "pods"},
				User:    &user.DefaultInfo{Name: "user"},
			},
			want: false,
		},
		{
			name: "multiple CEL conditions - all should pass",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
				If: []string{
					"request.verb == 'get'",
					"user.name == 'admin'",
					"request.resource == 'pods'",
				},
			}},
			input: &ResolveInput{
				Request: &request.RequestInfo{Verb: "get", Resource: "pods"},
				User:    &user.DefaultInfo{Name: "admin"},
			},
			want: true,
		},
		{
			name: "multiple CEL conditions - one should fail",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
				If: []string{
					"request.verb == 'get'",
					"user.name == 'admin'",
					"request.resource == 'secrets'",
				},
			}},
			input: &ResolveInput{
				Request: &request.RequestInfo{Verb: "get", Resource: "pods"},
				User:    &user.DefaultInfo{Name: "admin"},
			},
			want: false,
		},
		{
			name: "CEL condition on namespace - should pass",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
				If: []string{"resourceNamespace == 'default'"},
			}},
			input: &ResolveInput{
				Request:   &request.RequestInfo{Verb: "get", Resource: "pods"},
				Namespace: "default",
			},
			want: true,
		},
		{
			name: "CEL condition on user groups - should pass",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
				If: []string{"'system:masters' in user.groups"},
			}},
			input: &ResolveInput{
				Request: &request.RequestInfo{Verb: "get", Resource: "pods"},
				User:    &user.DefaultInfo{Name: "admin", Groups: []string{"system:masters", "system:authenticated"}},
			},
			want: true,
		},
		{
			name: "CEL condition with invalid syntax - should error",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
				If: []string{"invalid syntax =="},
			}},
			input: &ResolveInput{
				Request: &request.RequestInfo{Verb: "get", Resource: "pods"},
			},
			wantErr: true,
		},
		{
			name: "CEL condition returning non-boolean - should error during compilation",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Matches: []proxyrule.Match{{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"get"},
				}},
				If: []string{"request.verb"},
			}},
			input: &ResolveInput{
				Request: &request.RequestInfo{Verb: "get", Resource: "pods"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compiled, err := Compile(tt.config)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			result, err := EvaluateCELConditions(compiled.IfConditions, tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.want, result)
		})
	}
}

func TestFilterRulesWithCELConditions(t *testing.T) {
	// Create test rules with different CEL conditions
	ruleAlwaysTrue, err := Compile(proxyrule.Config{Spec: proxyrule.Spec{
		Matches: []proxyrule.Match{{GroupVersion: "v1", Resource: "pods", Verbs: []string{"get"}}},
		If:      []string{"true"},
	}})
	require.NoError(t, err)

	ruleAlwaysFalse, err := Compile(proxyrule.Config{Spec: proxyrule.Spec{
		Matches: []proxyrule.Match{{GroupVersion: "v1", Resource: "pods", Verbs: []string{"get"}}},
		If:      []string{"false"},
	}})
	require.NoError(t, err)

	ruleUserAdmin, err := Compile(proxyrule.Config{Spec: proxyrule.Spec{
		Matches: []proxyrule.Match{{GroupVersion: "v1", Resource: "pods", Verbs: []string{"get"}}},
		If:      []string{"user.name == 'admin'"},
	}})
	require.NoError(t, err)

	ruleNoCEL, err := Compile(proxyrule.Config{Spec: proxyrule.Spec{
		Matches: []proxyrule.Match{{GroupVersion: "v1", Resource: "pods", Verbs: []string{"get"}}},
	}})
	require.NoError(t, err)

	tests := []struct {
		name  string
		rules []*RunnableRule
		input *ResolveInput
		want  int
	}{
		{
			name:  "no rules",
			rules: []*RunnableRule{},
			input: &ResolveInput{},
			want:  0,
		},
		{
			name:  "rule with no CEL conditions",
			rules: []*RunnableRule{ruleNoCEL},
			input: &ResolveInput{},
			want:  1,
		},
		{
			name:  "rule always true",
			rules: []*RunnableRule{ruleAlwaysTrue},
			input: &ResolveInput{},
			want:  1,
		},
		{
			name:  "rule always false",
			rules: []*RunnableRule{ruleAlwaysFalse},
			input: &ResolveInput{},
			want:  0,
		},
		{
			name:  "mixed rules with admin user - admin rule should pass",
			rules: []*RunnableRule{ruleAlwaysFalse, ruleUserAdmin, ruleNoCEL},
			input: &ResolveInput{User: &user.DefaultInfo{Name: "admin"}},
			want:  2, // ruleUserAdmin and ruleNoCEL should pass
		},
		{
			name:  "mixed rules with regular user - admin rule should fail",
			rules: []*RunnableRule{ruleAlwaysFalse, ruleUserAdmin, ruleNoCEL},
			input: &ResolveInput{User: &user.DefaultInfo{Name: "user"}},
			want:  1, // only ruleNoCEL should pass
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := FilterRulesWithCELConditions(tt.rules, tt.input)
			require.NoError(t, err)
			require.Len(t, result, tt.want)
		})
	}
}

func TestMapMatcherMatch(t *testing.T) {
	m, err := NewMapMatcher([]proxyrule.Config{
		{Spec: proxyrule.Spec{
			Locking: proxyrule.PessimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "example.com/v1alpha1",
				Resource:     "wardles",
				Verbs:        []string{"create"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "org:{{metadata.labels.org}}#manage-wardles@user:{{request.user}}",
			}},
			Update: proxyrule.Update{
				CreateRelationships: []proxyrule.StringOrTemplate{{
					Template: "wardles:{{metadata.name}}#org@org:{{metadata.labels.org}}",
				}, {
					Template: "wardles:{{metadata.name}}#creator@user:{{request.user}}",
				}},
			},
		}}, {Spec: proxyrule.Spec{
			Locking: proxyrule.PessimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "example.com/v1alpha1",
				Resource:     "wardles",
				Verbs:        []string{"list", "watch"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "org:{{metadata.labels.org}}#audit-wardles@group:{{request.group}}#member",
			}},
			PreFilters: []proxyrule.PreFilter{{
				FromObjectIDNameExpr: "response.ResourceObjectID",
				LookupMatchingResources: &proxyrule.StringOrTemplate{
					RelationshipTemplate: &proxyrule.RelationshipTemplate{
						Resource: proxyrule.ObjectTemplate{
							Type:     "wardles",
							ID:       "$",
							Relation: "view",
						},
						Subject: proxyrule.ObjectTemplate{
							Type: "user",
							ID:   "{{request.user}}",
						},
					},
				},
			}},
		}},
	})
	require.NoError(t, err)

	tests := []struct {
		name        string
		match       *request.RequestInfo
		wantChecks  int
		wantCreates int
		wantFilters int
	}{
		{
			name: "matching create request",
			match: &request.RequestInfo{
				APIGroup:   "example.com",
				APIVersion: "v1alpha1",
				Resource:   "wardles",
				Verb:       "create",
			},
			wantChecks:  1,
			wantCreates: 2,
		},
		{
			name: "non-matching create request",
			match: &request.RequestInfo{
				APIGroup:   "example.com",
				APIVersion: "v1alpha1",
				Resource:   "foobars",
				Verb:       "create",
			},
		},
		{
			name: "matching list request",
			match: &request.RequestInfo{
				APIGroup:   "example.com",
				APIVersion: "v1alpha1",
				Resource:   "wardles",
				Verb:       "list",
			},
			wantChecks:  1,
			wantFilters: 1,
		},
		{
			name: "matching watch request",
			match: &request.RequestInfo{
				APIGroup:   "example.com",
				APIVersion: "v1alpha1",
				Resource:   "wardles",
				Verb:       "watch",
			},
			wantChecks:  1,
			wantFilters: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := m.Match(tt.match)
			var totalCheck, totalCreate, totalFilter int
			for _, r := range got {
				totalCheck += len(r.Checks)
				if r.Update != nil && r.Update.Creates != nil {
					totalCreate += len(r.Update.Creates)
				}
				totalFilter += len(r.PreFilter)
			}
			require.Equal(t, tt.wantChecks, totalCheck)
			require.Equal(t, tt.wantCreates, totalCreate)
			require.Equal(t, tt.wantFilters, totalFilter)
		})
	}
}

func TestNormalizeToBloblangTypes(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  any
	}{
		{
			name:  "string - no change",
			input: "hello",
			want:  "hello",
		},
		{
			name:  "int - no change",
			input: 42,
			want:  42,
		},
		{
			name:  "bool - no change",
			input: true,
			want:  true,
		},
		{
			name:  "nil - no change",
			input: nil,
			want:  nil,
		},
		{
			name:  "simple map[string]any - no change",
			input: map[string]any{"key": "value"},
			want:  map[string]any{"key": "value"},
		},
		{
			name:  "simple []any - no change",
			input: []any{"item1", "item2"},
			want:  []any{"item1", "item2"},
		},
		{
			name:  "[]string to []any",
			input: []string{"str1", "str2", "str3"},
			want:  []any{"str1", "str2", "str3"},
		},
		{
			name: "nested map[string]any",
			input: map[string]any{
				"level1": map[string]any{
					"level2": "value",
					"array":  []string{"a", "b", "c"},
				},
			},
			want: map[string]any{
				"level1": map[string]any{
					"level2": "value",
					"array":  []any{"a", "b", "c"},
				},
			},
		},
		{
			name: "nested []any with maps",
			input: []any{
				map[string]any{"key": "value1"},
				[]string{"nested", "array"},
				"plain string",
			},
			want: []any{
				map[string]any{"key": "value1"},
				[]any{"nested", "array"},
				"plain string",
			},
		},
		{
			name: "complex nested structure",
			input: map[string]any{
				"users": []string{"alice", "bob", "charlie"},
				"metadata": map[string]any{
					"labels": map[string]any{
						"env":   "production",
						"teams": []string{"backend", "frontend"},
					},
					"annotations": []any{
						map[string]any{"key": "value"},
						"simple annotation",
					},
				},
				"count":   123,
				"enabled": true,
			},
			want: map[string]any{
				"users": []any{"alice", "bob", "charlie"},
				"metadata": map[string]any{
					"labels": map[string]any{
						"env":   "production",
						"teams": []any{"backend", "frontend"},
					},
					"annotations": []any{
						map[string]any{"key": "value"},
						"simple annotation",
					},
				},
				"count":   123,
				"enabled": true,
			},
		},
		{
			name:  "empty map",
			input: map[string]any{},
			want:  map[string]any{},
		},
		{
			name:  "empty []any",
			input: []any{},
			want:  []any{},
		},
		{
			name:  "empty []string",
			input: []string{},
			want:  []any{},
		},
		{
			name: "deeply nested []string conversion",
			input: map[string]any{
				"level1": []any{
					map[string]any{
						"level2": []string{"deep", "nested", "strings"},
					},
				},
			},
			want: map[string]any{
				"level1": []any{
					map[string]any{
						"level2": []any{"deep", "nested", "strings"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeToBloblangTypes(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestResolveRel(t *testing.T) {
	tests := []struct {
		name    string
		expr    *UncompiledRelExpr
		input   *ResolveInput
		want    *ResolvedRel
		wantErr error
	}{
		{
			name: "basic",
			expr: &UncompiledRelExpr{
				ResourceType: "{{user.name}}",
			},
			input: &ResolveInput{
				Request: nil,
				User:    &user.DefaultInfo{Name: "testUser"},
				Object:  nil,
			},
			want: &ResolvedRel{
				ResourceType: "testUser",
			},
		},
		{
			name: "field not found",
			expr: &UncompiledRelExpr{
				ResourceType: "{{object.foo}}",
			},
			input: &ResolveInput{
				Request: nil,
				User:    &user.DefaultInfo{Name: "testUser"},
				Object:  nil,
			},
			wantErr: fmt.Errorf("error resolving relationship: empty resource type"),
		},
		{
			name: "fully templated",
			expr: &UncompiledRelExpr{
				ResourceType:     "{{request.resource}}",
				ResourceID:       "{{object.metadata.name}}",
				ResourceRelation: "{{object.metadata.labels.rel}}",
				SubjectType:      "{{object.metadata.labels.usertype}}",
				SubjectID:        "{{user.name}}",
				SubjectRelation:  "{{object.metadata.labels.sr}}",
			},
			input: &ResolveInput{
				Request: &request.RequestInfo{Resource: "foobars"},
				User:    &user.DefaultInfo{Name: "testUser"},
				Object: &metav1.PartialObjectMetadata{
					ObjectMeta: metav1.ObjectMeta{
						Name: "testfoo",
						Labels: map[string]string{
							"usertype": "user",
							"rel":      "testrel",
							"sr":       "testsubjectrel",
						},
					},
				},
			},
			want: &ResolvedRel{
				ResourceType:     "foobars",
				ResourceID:       "testfoo",
				ResourceRelation: "testrel",
				SubjectType:      "user",
				SubjectID:        "testUser",
				SubjectRelation:  "testsubjectrel",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := compileUnparsedRelExpr(tt.expr)
			require.NoError(t, err)

			got, err := ResolveRel(expr, tt.input)
			if tt.wantErr != nil {
				require.ErrorContains(t, err, tt.wantErr.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestTupleSetGeneration(t *testing.T) {
	tests := []struct {
		name       string
		tupleSet   string
		input      *ResolveInput
		want       []*ResolvedRel
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:     "basic container iteration",
			tupleSet: `["server", "config-reloader", "proxy-sidecar"].map_each("deployment:default/test-deployment#has-container@container:" + this)`,
			input: &ResolveInput{
				Name:           "test-deployment",
				Namespace:      "default",
				NamespacedName: "default/test-deployment",
				Object: &metav1.PartialObjectMetadata{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-deployment",
						Namespace: "default",
					},
				},
				Body: []byte(`{
							"spec": {
								"template": {
									"spec": {
										"containers": [
											{"name": "server"},
											{"name": "config-reloader"},
											{"name": "proxy-sidecar"}
										]
									}
								}
							}
						}`),
			},
			want: []*ResolvedRel{
				{
					ResourceType:     "deployment",
					ResourceID:       "default/test-deployment",
					ResourceRelation: "has-container",
					SubjectType:      "container",
					SubjectID:        "server",
				},
				{
					ResourceType:     "deployment",
					ResourceID:       "default/test-deployment",
					ResourceRelation: "has-container",
					SubjectType:      "container",
					SubjectID:        "config-reloader",
				},
				{
					ResourceType:     "deployment",
					ResourceID:       "default/test-deployment",
					ResourceRelation: "has-container",
					SubjectType:      "container",
					SubjectID:        "proxy-sidecar",
				},
			},
		},
		{
			name:     "empty container list",
			tupleSet: `[].map_each("deployment:default/test-deployment#has-container@container:" + this)`,
			input: &ResolveInput{
				Name:           "test-deployment",
				Namespace:      "default",
				NamespacedName: "default/test-deployment",
				Object: &metav1.PartialObjectMetadata{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-deployment",
						Namespace: "default",
					},
				},
				Body: []byte(`{
							"spec": {
								"template": {
									"spec": {
										"containers": []
									}
								}
							}
						}`),
			},
			want: []*ResolvedRel{},
		},
		{
			name:     "with filter",
			tupleSet: `["server", "proxy-sidecar"].filter(this != "proxy-sidecar").map_each("deployment:default/test-deployment#has-container@container:" + this)`,
			input: &ResolveInput{
				Name:           "test-deployment",
				Namespace:      "default",
				NamespacedName: "default/test-deployment",
				Object: &metav1.PartialObjectMetadata{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-deployment",
						Namespace: "default",
					},
				},
				Body: []byte(`{
							"spec": {
								"template": {
									"spec": {
										"containers": [
											{"name": "server"},
											{"name": "proxy-sidecar"}
										]
									}
								}
							}
						}`),
			},
			want: []*ResolvedRel{
				{
					ResourceType:     "deployment",
					ResourceID:       "default/test-deployment",
					ResourceRelation: "has-container",
					SubjectType:      "container",
					SubjectID:        "server",
				},
			},
		},
		{
			name:       "non-array result",
			tupleSet:   `"single-string"`,
			input:      &ResolveInput{NamespacedName: "test"},
			wantErr:    true,
			wantErrMsg: "tuple set expression must return an array",
		},
		{
			name:       "invalid relationship string",
			tupleSet:   `["invalid-relationship-format"]`,
			input:      &ResolveInput{NamespacedName: "test"},
			wantErr:    true,
			wantErrMsg: "error parsing relationship string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor, err := CompileTupleSetExpression(tt.tupleSet)
			require.NoError(t, err)

			tupleSetExpr := &TupleSetExpr{
				Expression: executor,
			}

			got, err := tupleSetExpr.GenerateRelationships(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrMsg != "" {
					require.Contains(t, err.Error(), tt.wantErrMsg)
				}
				return
			}

			require.NoError(t, err)
			if tt.want == nil && got == nil {
				return // both nil is fine
			}
			if len(tt.want) == 0 && len(got) == 0 {
				return // both empty is fine
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCompileWithTupleSet(t *testing.T) {
	config := proxyrule.Config{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ProxyRule",
			APIVersion: "authzed.com/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "deployment-containers",
		},
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "apps/v1",
				Resource:     "deployments",
				Verbs:        []string{"create"},
			}},
			Update: proxyrule.Update{
				CreateRelationships: []proxyrule.StringOrTemplate{
					{
						Template: "deployment:{{namespacedName}}#creator@user:{{user.name}}",
					},
					{
						TupleSet: `object.spec.template.spec.containers.map_each("deployment:" + namespacedName + "#has-container@container:" + this.name)`,
					},
				},
			},
		},
	}

	rule, err := Compile(config)
	require.NoError(t, err)
	require.NotNil(t, rule.Update)
	require.Len(t, rule.Update.Creates, 2)

	// First should be a regular RelExpr
	_, ok := rule.Update.Creates[0].(*RelExpr)
	require.True(t, ok, "First create rule should be a RelExpr")

	// Second should be a TupleSetExpr
	_, ok = rule.Update.Creates[1].(*TupleSetExpr)
	require.True(t, ok, "Second create rule should be a TupleSetExpr")
}

func TestConvertToBloblangInput(t *testing.T) {
	tests := []struct {
		name    string
		input   *ResolveInput
		want    map[string]any
		wantErr bool
	}{
		{
			name: "basic input with user extra fields",
			input: &ResolveInput{
				Name:           "test-pod",
				Namespace:      "default",
				NamespacedName: "default/test-pod",
				Request: &request.RequestInfo{
					Verb:       "create",
					APIGroup:   "v1",
					APIVersion: "v1",
					Resource:   "pods",
					Name:       "test-pod",
					Namespace:  "default",
				},
				User: &user.DefaultInfo{
					Name:   "test-user",
					UID:    "uid123",
					Groups: []string{"group1", "group2"},
					Extra: map[string][]string{
						"department": {"engineering", "security"},
						"role":       {"admin"},
						"project":    {"alpha", "beta", "gamma"},
					},
				},
				Headers: map[string][]string{
					"Authorization": {"Bearer token123"},
					"Content-Type":  {"application/json"},
					"X-Custom":      {"value1", "value2"},
				},
			},
			want: map[string]any{
				"name":           "test-pod",
				"namespace":      "default",
				"namespacedName": "default/test-pod",
				"resourceId":     "default/test-pod",
				"request": map[string]any{
					"verb":       "create",
					"apiGroup":   "v1",
					"apiVersion": "v1",
					"resource":   "pods",
					"name":       "test-pod",
					"namespace":  "default",
				},
				"user": map[string]any{
					"name":   "test-user",
					"uid":    "uid123",
					"groups": []any{"group1", "group2"},
					"extra": map[string]any{
						"department": []any{"engineering", "security"},
						"role":       []any{"admin"},
						"project":    []any{"alpha", "beta", "gamma"},
					},
				},
				"headers": map[string]any{
					"Authorization": []any{"Bearer token123"},
					"Content-Type":  []any{"application/json"},
					"X-Custom":      []any{"value1", "value2"},
				},
			},
		},
		{
			name: "user extra with nested access patterns",
			input: &ResolveInput{
				Name:           "test-resource",
				Namespace:      "system",
				NamespacedName: "system/test-resource",
				User: &user.DefaultInfo{
					Name:   "admin-user",
					UID:    "admin123",
					Groups: []string{"system:masters", "system:authenticated"},
					Extra: map[string][]string{
						"scopes":      {"read", "write", "admin"},
						"permissions": {"create", "update", "delete"},
						"metadata":    {"env=prod", "team=platform"},
					},
				},
				Headers: map[string][]string{
					"X-Forwarded-For": {"192.168.1.1", "10.0.0.1"},
					"User-Agent":      {"kubectl/v1.28.0"},
				},
			},
			want: map[string]any{
				"name":           "test-resource",
				"namespace":      "system",
				"namespacedName": "system/test-resource",
				"resourceId":     "system/test-resource",
				"user": map[string]any{
					"name":   "admin-user",
					"uid":    "admin123",
					"groups": []any{"system:masters", "system:authenticated"},
					"extra": map[string]any{
						"scopes":      []any{"read", "write", "admin"},
						"permissions": []any{"create", "update", "delete"},
						"metadata":    []any{"env=prod", "team=platform"},
					},
				},
				"headers": map[string]any{
					"X-Forwarded-For": []any{"192.168.1.1", "10.0.0.1"},
					"User-Agent":      []any{"kubectl/v1.28.0"},
				},
			},
		},
		{
			name: "object metadata with nested structure",
			input: &ResolveInput{
				Name:           "my-deployment",
				Namespace:      "production",
				NamespacedName: "production/my-deployment",
				Object: &metav1.PartialObjectMetadata{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-deployment",
						Namespace: "production",
						Labels: map[string]string{
							"app":         "frontend",
							"version":     "v1.2.3",
							"environment": "prod",
						},
						Annotations: map[string]string{
							"deploy.kubernetes.io/revision":                    "1",
							"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"apps/v1","kind":"Deployment"}`,
						},
					},
				},
				User: &user.DefaultInfo{
					Name: "deploy-user",
					Extra: map[string][]string{
						"buildinfo": {"commit=abc123", "branch=main"},
					},
				},
				Headers: map[string][]string{
					"X-Build-Id": {"12345"},
				},
			},
			want: map[string]any{
				"name":           "my-deployment",
				"namespace":      "production",
				"namespacedName": "production/my-deployment",
				"resourceId":     "production/my-deployment",
				"user": map[string]any{
					"name":   "deploy-user",
					"uid":    "",
					"groups": []any{},
					"extra": map[string]any{
						"buildinfo": []any{"commit=abc123", "branch=main"},
					},
				},
				"object": map[string]any{
					"metadata": map[string]any{
						"name":      "my-deployment",
						"namespace": "production",
						"labels": map[string]any{
							"app":         "frontend",
							"version":     "v1.2.3",
							"environment": "prod",
						},
						"annotations": map[string]any{
							"deploy.kubernetes.io/revision":                    "1",
							"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"apps/v1","kind":"Deployment"}`,
						},
						"creationTimestamp": nil,
					},
				},
				"metadata": map[string]any{
					"name":      "my-deployment",
					"namespace": "production",
					"labels": map[string]any{
						"app":         "frontend",
						"version":     "v1.2.3",
						"environment": "prod",
					},
					"annotations": map[string]any{
						"deploy.kubernetes.io/revision":                    "1",
						"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"apps/v1","kind":"Deployment"}`,
					},
					"creationTimestamp": nil,
				},
				"headers": map[string]any{
					"X-Build-Id": []any{"12345"},
				},
			},
		},
		{
			name: "empty user extra and headers",
			input: &ResolveInput{
				Name:           "simple-pod",
				Namespace:      "default",
				NamespacedName: "default/simple-pod",
				User: &user.DefaultInfo{
					Name:   "simple-user",
					UID:    "simple123",
					Groups: []string{},
					Extra:  map[string][]string{},
				},
				Headers: map[string][]string{},
			},
			want: map[string]any{
				"name":           "simple-pod",
				"namespace":      "default",
				"namespacedName": "default/simple-pod",
				"resourceId":     "default/simple-pod",
				"user": map[string]any{
					"name":   "simple-user",
					"uid":    "simple123",
					"groups": []any{},
					"extra":  map[string]any{},
				},
				"headers": map[string]any{},
			},
		},
		{
			name: "nil user and headers",
			input: &ResolveInput{
				Name:           "basic-resource",
				Namespace:      "test",
				NamespacedName: "test/basic-resource",
				User:           nil,
				Headers:        nil,
			},
			want: map[string]any{
				"name":           "basic-resource",
				"namespace":      "test",
				"namespacedName": "test/basic-resource",
				"resourceId":     "test/basic-resource",
				"headers":        map[string]any{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToBloblangInput(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestConvertToBloblangInputWithBloblangExpressions(t *testing.T) {
	tests := []struct {
		name           string
		input          *ResolveInput
		expression     string
		expectedResult any
		wantErr        bool
	}{
		{
			name: "access user extra field by index",
			input: &ResolveInput{
				User: &user.DefaultInfo{
					Name: "test-user",
					Extra: map[string][]string{
						"department": {"engineering", "security"},
						"role":       {"admin"},
						"project":    {"alpha", "beta", "gamma"},
					},
				},
			},
			expression:     "user.extra.department.index(0)",
			expectedResult: "engineering",
		},
		{
			name: "access user extra field by index - second element",
			input: &ResolveInput{
				User: &user.DefaultInfo{
					Name: "test-user",
					Extra: map[string][]string{
						"scopes": {"read", "write", "admin"},
					},
				},
			},
			expression:     "user.extra.scopes.index(2)",
			expectedResult: "admin",
		},
		{
			name: "access header by key and index",
			input: &ResolveInput{
				Headers: map[string][]string{
					"X-Custom":      {"value1", "value2", "value3"},
					"Authorization": {"Bearer token123"},
				},
			},
			expression:     "headers.\"X-Custom\".index(1)",
			expectedResult: "value2",
		},
		{
			name: "access single header value",
			input: &ResolveInput{
				Headers: map[string][]string{
					"Content-Type": {"application/json"},
				},
			},
			expression:     "headers.\"Content-Type\".index(0)",
			expectedResult: "application/json",
		},
		{
			name: "access user groups by index",
			input: &ResolveInput{
				User: &user.DefaultInfo{
					Name:   "test-user",
					Groups: []string{"group1", "group2", "system:masters"},
				},
			},
			expression:     "user.groups.index(2)",
			expectedResult: "system:masters",
		},
		{
			name: "access nested object metadata",
			input: &ResolveInput{
				Object: &metav1.PartialObjectMetadata{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-pod",
						Labels: map[string]string{
							"app": "frontend",
							"env": "prod",
						},
					},
				},
			},
			expression:     "object.metadata.labels.app",
			expectedResult: "frontend",
		},
		{
			name: "access request info",
			input: &ResolveInput{
				Request: &request.RequestInfo{
					Verb:     "create",
					Resource: "pods",
					APIGroup: "v1",
				},
			},
			expression:     "request.verb",
			expectedResult: "create",
		},
		{
			name: "complex expression with multiple field access",
			input: &ResolveInput{
				User: &user.DefaultInfo{
					Name: "admin",
					Extra: map[string][]string{
						"permissions": {"read", "write", "admin"},
					},
				},
				Request: &request.RequestInfo{
					Verb: "create",
				},
			},
			expression:     "user.extra.permissions.index(2) + \"-\" + request.verb",
			expectedResult: "admin-create",
		},
		{
			name: "access non-existent field",
			input: &ResolveInput{
				User: &user.DefaultInfo{
					Name: "test-user",
					Extra: map[string][]string{
						"department": {"engineering"},
					},
				},
			},
			expression:     "user.extra.nonexistent.index(0)",
			expectedResult: nil,
			wantErr:        true,
		},
		{
			name: "access out of bounds index",
			input: &ResolveInput{
				User: &user.DefaultInfo{
					Name: "test-user",
					Extra: map[string][]string{
						"department": {"engineering"},
					},
				},
			},
			expression:     "user.extra.department.index(5)",
			expectedResult: nil,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert the input to bloblang format
			bloblangInput, err := convertToBloblangInput(tt.input)
			require.NoError(t, err)

			// Compile the bloblang expression
			executor, err := customBloblangEnv.Parse(tt.expression)
			require.NoError(t, err)

			// Execute the expression
			result, err := executor.Query(bloblangInput)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			require.Equal(t, tt.expectedResult, result)
		})
	}
}
