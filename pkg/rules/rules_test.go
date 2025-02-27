package rules

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/kyverno/go-jmespath"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
)

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
			name: "templated with jmespath features that use special characters",
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

func TestCompileJMESPathExpression(t *testing.T) {
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
			name:           "invalid expression",
			expr:           "{{.matters}}",
			data:           []byte(`{"matters": "yes"}`),
			wantCompileErr: "Invalid token",
		},
		{
			name: "non-matching expression",
			expr: "{{matters}}",
			data: []byte(`{"virus": "veryyes"}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := CompileJMESPathExpression(tt.expr)
			if len(tt.wantCompileErr) > 0 {
				require.Contains(t, err.Error(), tt.wantCompileErr)
				return
			} else {
				require.NoError(t, err)
			}
			var data interface{}
			require.NoError(t, json.Unmarshal(tt.data, &data))
			got, searchErr := expr.Search(data)

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
			"labels": {"org": "testOrg"}
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
		checks  []ResolvedRel
		updates []ResolvedRel
		filters []ResolvedPreFilter
	}

	mustSearch := func(path *jmespath.JMESPath) any {
		val, err := path.Search(testData)
		require.NoError(t, err)
		return val
	}

	requireEqualUnderTestData := func(exprs []*RelExpr, res []ResolvedRel) {
		for i, c := range exprs {
			require.Equal(t, res[i].ResourceType, mustSearch(c.ResourceType))
			require.Equal(t, res[i].ResourceID, mustSearch(c.ResourceID))
			require.Equal(t, res[i].ResourceRelation, mustSearch(c.ResourceRelation))
			require.Equal(t, res[i].SubjectType, mustSearch(c.SubjectType))
			require.Equal(t, res[i].SubjectID, mustSearch(c.SubjectID))
			if c.SubjectRelation != nil {
				require.Equal(t, res[i].SubjectRelation, mustSearch(c.SubjectRelation))
			}
		}
	}

	requireFilterEqualUnderTestData := func(t *testing.T, filter []*PreFilter, res []ResolvedPreFilter) {
		for i, f := range filter {
			require.Equal(t, f.LookupType, res[i].LookupType)
			require.Equal(t, mustSearch(res[i].Name), mustSearch(f.Name))
			require.Equal(t, mustSearch(res[i].Namespace), mustSearch(f.Namespace))
			requireEqualUnderTestData([]*RelExpr{f.Rel}, []ResolvedRel{*res[i].Rel})
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
					Template: "org:{{metadata.labels.org}}#manage-wardles@user:{{request.user}}",
				}},
				Updates: []proxyrule.StringOrTemplate{{
					Template: "wardles:{{metadata.name}}#org@org:{{metadata.labels.org}}",
				}, {
					Template: "wardles:{{metadata.name}}#creator@user:{{request.user}}",
				}},
			}},
			want: result{
				checks: []ResolvedRel{{
					ResourceType:     "org",
					ResourceID:       "testOrg",
					ResourceRelation: "manage-wardles",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				updates: []ResolvedRel{{
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
					Template: "org:{{metadata.labels.org}}#audit-wardles@group:{{request.group}}#member",
				}},
				PreFilters: []proxyrule.PreFilter{{
					Name: "response.ResourceObjectID",
					ByResource: &proxyrule.StringOrTemplate{
						RelationshipTemplate: &proxyrule.RelationshipTemplate{
							Resource: proxyrule.ObjectTemplate{
								Type:     "wardles",
								ID:       "$resourceID",
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
					LookupType: LookupTypeResource,
					Name:       jmespath.MustCompile("response.ResourceObjectID"),
					Namespace:  jmespath.MustCompile("''"),
					Rel: &ResolvedRel{
						ResourceType:     "wardles",
						ResourceID:       "$resourceID",
						ResourceRelation: "view",
						SubjectType:      "user",
						SubjectID:        "testUser",
					},
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Compile(tt.config)
			require.Equal(t, tt.wantErr, err)

			requireEqualUnderTestData(got.Checks, tt.want.checks)
			requireEqualUnderTestData(got.Updates, tt.want.updates)
			requireFilterEqualUnderTestData(t, got.PreFilter, tt.want.filters)
		})
	}
}

func TestMapMatcherMatch(t *testing.T) {
	m, err := NewMapMatcher([]proxyrule.Config{{Spec: proxyrule.Spec{
		Locking: proxyrule.PessimisticLockMode,
		Matches: []proxyrule.Match{{
			GroupVersion: "example.com/v1alpha1",
			Resource:     "wardles",
			Verbs:        []string{"create"},
		}},
		Checks: []proxyrule.StringOrTemplate{{
			Template: "org:{{metadata.labels.org}}#manage-wardles@user:{{request.user}}",
		}},
		Updates: []proxyrule.StringOrTemplate{{
			Template: "wardles:{{metadata.name}}#org@org:{{metadata.labels.org}}",
		}, {
			Template: "wardles:{{metadata.name}}#creator@user:{{request.user}}",
		}},
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
			Name: "response.ResourceObjectID",
			ByResource: &proxyrule.StringOrTemplate{
				RelationshipTemplate: &proxyrule.RelationshipTemplate{
					Resource: proxyrule.ObjectTemplate{
						Type:     "wardles",
						ID:       "$resourceID",
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
		wantUpdates int
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
			wantUpdates: 2,
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
			var totalCheck, totalUpdate, totalFilter int
			for _, r := range got {
				totalCheck += len(r.Checks)
				totalUpdate += len(r.Updates)
				totalFilter += len(r.PreFilter)
			}
			require.Equal(t, tt.wantChecks, totalCheck)
			require.Equal(t, tt.wantUpdates, totalUpdate)
			require.Equal(t, tt.wantFilters, totalFilter)
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
				ResourceType: "{{user.Name}}",
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
				ResourceType:     "{{request.Resource}}",
				ResourceID:       "{{object.metadata.name}}",
				ResourceRelation: "{{object.metadata.labels.rel}}",
				SubjectType:      "{{object.metadata.labels.usertype}}",
				SubjectID:        "{{user.Name}}",
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
						}},
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
