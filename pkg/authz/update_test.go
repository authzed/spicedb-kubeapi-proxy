package authz

import (
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

func TestFilterFromRel(t *testing.T) {
	tests := []struct {
		name        string
		rel         *rules.ResolvedRel
		expected    *v1.RelationshipFilter
		expectError bool
	}{
		{
			name: "all concrete values",
			rel: &rules.ResolvedRel{
				ResourceType:     "namespace",
				ResourceID:       "default",
				ResourceRelation: "viewer",
				SubjectType:      "user",
				SubjectID:        "alice",
				SubjectRelation:  "",
			},
			expected: &v1.RelationshipFilter{
				ResourceType:       "namespace",
				OptionalResourceId: "default",
				OptionalRelation:   "viewer",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "user",
					OptionalSubjectId: "alice",
					OptionalRelation:  nil,
				},
			},
			expectError: false,
		},
		{
			name: "all template variables",
			rel: &rules.ResolvedRel{
				ResourceType:     "$resourceType",
				ResourceID:       "$resourceID",
				ResourceRelation: "$resourceRelation",
				SubjectType:      "$subjectType",
				SubjectID:        "$subjectID",
				SubjectRelation:  "$subjectRelation",
			},
			expected: &v1.RelationshipFilter{
				ResourceType:          "",
				OptionalResourceId:    "",
				OptionalRelation:      "",
				OptionalSubjectFilter: nil,
			},
			expectError: false,
		},
		{
			name: "mixed concrete and template values; subject type is required so error is raised",
			rel: &rules.ResolvedRel{
				ResourceType:     "namespace",
				ResourceID:       "$resourceID",
				ResourceRelation: "viewer",
				SubjectType:      "$subjectType",
				SubjectID:        "alice",
				SubjectRelation:  "",
			},
			expectError: true,
		},
		{
			name: "subject relation with concrete value",
			rel: &rules.ResolvedRel{
				ResourceType:     "namespace",
				ResourceID:       "default",
				ResourceRelation: "viewer",
				SubjectType:      "group",
				SubjectID:        "admins",
				SubjectRelation:  "member",
			},
			expected: &v1.RelationshipFilter{
				ResourceType:       "namespace",
				OptionalResourceId: "default",
				OptionalRelation:   "viewer",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "group",
					OptionalSubjectId: "admins",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "member",
					},
				},
			},
			expectError: false,
		},
		{
			name: "subject relation with template variable",
			rel: &rules.ResolvedRel{
				ResourceType:     "namespace",
				ResourceID:       "default",
				ResourceRelation: "viewer",
				SubjectType:      "group",
				SubjectID:        "admins",
				SubjectRelation:  "$subjectRelation",
			},
			expected: &v1.RelationshipFilter{
				ResourceType:       "namespace",
				OptionalResourceId: "default",
				OptionalRelation:   "viewer",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "group",
					OptionalSubjectId: "admins",
					OptionalRelation:  nil,
				},
			},
			expectError: false,
		},
		{
			name: "empty subject relation",
			rel: &rules.ResolvedRel{
				ResourceType:     "namespace",
				ResourceID:       "default",
				ResourceRelation: "viewer",
				SubjectType:      "user",
				SubjectID:        "alice",
				SubjectRelation:  "",
			},
			expected: &v1.RelationshipFilter{
				ResourceType:       "namespace",
				OptionalResourceId: "default",
				OptionalRelation:   "viewer",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "user",
					OptionalSubjectId: "alice",
					OptionalRelation:  nil,
				},
			},
			expectError: false,
		},
		{
			name: "invalid dollar in resourceType",
			rel: &rules.ResolvedRel{
				ResourceType:     "namespace$invalid",
				ResourceID:       "default",
				ResourceRelation: "viewer",
				SubjectType:      "user",
				SubjectID:        "alice",
				SubjectRelation:  "",
			},
			expected:    nil,
			expectError: true,
		},
		{
			name: "invalid dollar in resourceID",
			rel: &rules.ResolvedRel{
				ResourceType:     "namespace",
				ResourceID:       "default$invalid",
				ResourceRelation: "viewer",
				SubjectType:      "user",
				SubjectID:        "alice",
				SubjectRelation:  "",
			},
			expected:    nil,
			expectError: true,
		},
		{
			name: "invalid dollar in resourceRelation",
			rel: &rules.ResolvedRel{
				ResourceType:     "namespace",
				ResourceID:       "default",
				ResourceRelation: "viewer$invalid",
				SubjectType:      "user",
				SubjectID:        "alice",
				SubjectRelation:  "",
			},
			expected:    nil,
			expectError: true,
		},
		{
			name: "invalid dollar in subjectType",
			rel: &rules.ResolvedRel{
				ResourceType:     "namespace",
				ResourceID:       "default",
				ResourceRelation: "viewer",
				SubjectType:      "user$invalid",
				SubjectID:        "alice",
				SubjectRelation:  "",
			},
			expected:    nil,
			expectError: true,
		},
		{
			name: "invalid dollar in subjectID",
			rel: &rules.ResolvedRel{
				ResourceType:     "namespace",
				ResourceID:       "default",
				ResourceRelation: "viewer",
				SubjectType:      "user",
				SubjectID:        "alice$invalid",
				SubjectRelation:  "",
			},
			expected:    nil,
			expectError: true,
		},
		{
			name: "invalid dollar in subjectRelation",
			rel: &rules.ResolvedRel{
				ResourceType:     "namespace",
				ResourceID:       "default",
				ResourceRelation: "viewer",
				SubjectType:      "user",
				SubjectID:        "alice",
				SubjectRelation:  "member$invalid",
			},
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := filterFromRel(tt.rel)
			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestFilterFromRel_SubjectFilterCreation(t *testing.T) {
	tests := []struct {
		name                string
		subjectType         string
		subjectID           string
		subjectRelation     string
		expectSubjectFilter bool
	}{
		{
			name:                "all subject fields are template variables",
			subjectType:         "$subjectType",
			subjectID:           "$subjectID",
			subjectRelation:     "",
			expectSubjectFilter: false,
		},
		{
			name:                "subject type is concrete",
			subjectType:         "user",
			subjectID:           "$subjectID",
			subjectRelation:     "",
			expectSubjectFilter: true,
		},
		{
			name:                "subject ID is concrete",
			subjectType:         "something",
			subjectID:           "alice",
			subjectRelation:     "",
			expectSubjectFilter: true,
		},
		{
			name:                "subject relation is concrete",
			subjectType:         "something",
			subjectID:           "$subjectID",
			subjectRelation:     "member",
			expectSubjectFilter: true,
		},
		{
			name:                "subject relation is template variable",
			subjectType:         "something",
			subjectID:           "$subjectID",
			subjectRelation:     "$subjectRelation",
			expectSubjectFilter: true,
		},
		{
			name:                "empty subject type should not create filter by itself",
			subjectType:         "",
			subjectID:           "$subjectID",
			subjectRelation:     "",
			expectSubjectFilter: false,
		},
		{
			name:                "empty subject ID should not create filter by itself",
			subjectType:         "$subjectType",
			subjectID:           "",
			subjectRelation:     "",
			expectSubjectFilter: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &rules.ResolvedRel{
				ResourceType:     "namespace",
				ResourceID:       "default",
				ResourceRelation: "viewer",
				SubjectType:      tt.subjectType,
				SubjectID:        tt.subjectID,
				SubjectRelation:  tt.subjectRelation,
			}

			result, err := filterFromRel(rel)
			require.NoError(t, err)

			if tt.expectSubjectFilter {
				require.NotNil(t, result.OptionalSubjectFilter, "expected subject filter to be created")
			} else {
				require.Nil(t, result.OptionalSubjectFilter, "expected subject filter to be nil")
			}
		})
	}
}

func TestValidateFieldForDollarUsage(t *testing.T) {
	tests := []struct {
		name            string
		field           string
		fieldName       string
		allowedTemplate string
		expectError     bool
	}{
		{
			name:            "no dollar sign",
			field:           "namespace",
			fieldName:       "resourceType",
			allowedTemplate: "$resourceType",
			expectError:     false,
		},
		{
			name:            "valid template variable",
			field:           "$resourceType",
			fieldName:       "resourceType",
			allowedTemplate: "$resourceType",
			expectError:     false,
		},
		{
			name:            "invalid dollar usage",
			field:           "namespace$invalid",
			fieldName:       "resourceType",
			allowedTemplate: "$resourceType",
			expectError:     true,
		},
		{
			name:            "dollar at beginning but not valid template",
			field:           "$invalid",
			fieldName:       "resourceType",
			allowedTemplate: "$resourceType",
			expectError:     true,
		},
		{
			name:            "multiple dollars",
			field:           "$resource$Type",
			fieldName:       "resourceType",
			allowedTemplate: "$resourceType",
			expectError:     true,
		},
		{
			name:            "empty field",
			field:           "",
			fieldName:       "resourceType",
			allowedTemplate: "$resourceType",
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFieldForDollarUsage(tt.field, tt.fieldName, tt.allowedTemplate)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "invalid use of '$'")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
