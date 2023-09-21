package rules

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/jmespath/go-jmespath"
	"github.com/stretchr/testify/require"

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
        }
	}`)
	var testData any
	require.NoError(t, json.Unmarshal(testDataBytes, &testData))

	type resultRel UncompiledRelExpr
	type result struct {
		checks  []resultRel
		writes  []resultRel
		filters []resultRel
	}

	mustSearch := func(path *jmespath.JMESPath) any {
		val, err := path.Search(testData)
		require.NoError(t, err)
		return val
	}

	requireEqualUnderTestData := func(exprs []*RelExpr, res []resultRel) {
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
					GVR:   "example.com/v1alpha1/wardles",
					Verbs: []string{"create"},
				}},
				Checks: []proxyrule.StringOrTemplate{{
					Template: "org:{{metadata.labels.org}}#manage-wardles@user:{{request.user}}",
				}},
				Writes: []proxyrule.StringOrTemplate{{
					Template: "wardles:{{metadata.name}}#org@org:{{metadata.labels.org}}",
				}, {
					Template: "wardles:{{metadata.name}}#creator@user:{{request.user}}",
				}},
			}},
			want: result{
				checks: []resultRel{{
					ResourceType:     "org",
					ResourceID:       "testOrg",
					ResourceRelation: "manage-wardles",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
				writes: []resultRel{{
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
				filters: []resultRel{},
			},
		},
		{
			name: "list rule",
			config: proxyrule.Config{Spec: proxyrule.Spec{
				Locking: proxyrule.PessimisticLockMode,
				Matches: []proxyrule.Match{{
					GVR:   "example.com/v1alpha1/wardles",
					Verbs: []string{"list"},
				}},
				Checks: []proxyrule.StringOrTemplate{{
					Template: "org:{{metadata.labels.org}}#audit-wardles@group:{{request.group}}#member",
				}},
				Filter: []proxyrule.StringOrTemplate{{
					RelationshipTemplate: &proxyrule.RelationshipTemplate{
						Resource: proxyrule.ObjectTemplate{
							Type:     "wardles",
							ID:       "{{metadata.name}}",
							Relation: "view",
						},
						Subject: proxyrule.ObjectTemplate{
							Type: "user",
							ID:   "{{request.user}}",
						},
					},
				}},
			}},
			want: result{
				checks: []resultRel{{
					ResourceType:     "org",
					ResourceID:       "testOrg",
					ResourceRelation: "audit-wardles",
					SubjectType:      "group",
					SubjectID:        "testGroup",
					SubjectRelation:  "member",
				}},
				filters: []resultRel{{
					ResourceType:     "wardles",
					ResourceID:       "testName",
					ResourceRelation: "view",
					SubjectType:      "user",
					SubjectID:        "testUser",
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Compile(tt.config)
			require.Equal(t, tt.wantErr, err)

			requireEqualUnderTestData(got.Checks, tt.want.checks)
			requireEqualUnderTestData(got.Writes, tt.want.writes)
			requireEqualUnderTestData(got.Filter, tt.want.filters)
		})
	}
}

func TestMapMatcherMatch(t *testing.T) {
	m, err := NewMapMatcher([]proxyrule.Config{{Spec: proxyrule.Spec{
		Locking: proxyrule.PessimisticLockMode,
		Matches: []proxyrule.Match{{
			GVR:   "example.com/v1alpha1/wardles",
			Verbs: []string{"create"},
		}},
		Checks: []proxyrule.StringOrTemplate{{
			Template: "org:{{metadata.labels.org}}#manage-wardles@user:{{request.user}}",
		}},
		Writes: []proxyrule.StringOrTemplate{{
			Template: "wardles:{{metadata.name}}#org@org:{{metadata.labels.org}}",
		}, {
			Template: "wardles:{{metadata.name}}#creator@user:{{request.user}}",
		}},
	}}, {Spec: proxyrule.Spec{
		Locking: proxyrule.PessimisticLockMode,
		Matches: []proxyrule.Match{{
			GVR:   "example.com/v1alpha1/wardles",
			Verbs: []string{"list", "watch"},
		}},
		Checks: []proxyrule.StringOrTemplate{{
			Template: "org:{{metadata.labels.org}}#audit-wardles@group:{{request.group}}#member",
		}},
		Filter: []proxyrule.StringOrTemplate{{
			RelationshipTemplate: &proxyrule.RelationshipTemplate{
				Resource: proxyrule.ObjectTemplate{
					Type:     "wardles",
					ID:       "{{metadata.name}}",
					Relation: "view",
				},
				Subject: proxyrule.ObjectTemplate{
					Type: "user",
					ID:   "{{request.user}}",
				},
			},
		}},
	}},
	})
	require.NoError(t, err)

	tests := []struct {
		name        string
		match       RequestMeta
		wantChecks  int
		wantWrites  int
		wantFilters int
	}{
		{
			name: "matching create request",
			match: RequestMeta{
				GVR:  "example.com/v1alpha1/wardles",
				Verb: "create",
			},
			wantChecks: 1,
			wantWrites: 2,
		},
		{
			name: "non-matching create request",
			match: RequestMeta{
				GVR:  "example.com/v1alpha1/foobars",
				Verb: "create",
			},
		},
		{
			name: "matching list request",
			match: RequestMeta{
				GVR:  "example.com/v1alpha1/wardles",
				Verb: "list",
			},
			wantChecks:  1,
			wantFilters: 1,
		},
		{
			name: "matching watch request",
			match: RequestMeta{
				GVR:  "example.com/v1alpha1/wardles",
				Verb: "watch",
			},
			wantChecks:  1,
			wantFilters: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := m.Match(tt.match)
			var totalCheck, totalWrite, totalFilter int
			for _, r := range got {
				totalCheck += len(r.Checks)
				totalWrite += len(r.Writes)
				totalFilter += len(r.Filter)
			}
			require.Equal(t, tt.wantChecks, totalCheck)
			require.Equal(t, tt.wantWrites, totalWrite)
			require.Equal(t, tt.wantFilters, totalFilter)
		})
	}
}
