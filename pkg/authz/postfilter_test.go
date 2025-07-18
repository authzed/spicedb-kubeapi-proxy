package authz

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

// mockPermissionsClient implements a mock permissions client for testing
type mockPermissionsClient struct {
	responses map[string]*v1.CheckPermissionResponse
}

func (m *mockPermissionsClient) CheckPermission(ctx context.Context, req *v1.CheckPermissionRequest, opts ...grpc.CallOption) (*v1.CheckPermissionResponse, error) {
	// Create a key from the request to match against responses
	key := req.Resource.ObjectType + ":" + req.Resource.ObjectId + "#" + req.Permission + "@" + req.Subject.Object.ObjectType + ":" + req.Subject.Object.ObjectId
	if resp, ok := m.responses[key]; ok {
		return resp, nil
	}
	// Default to no permission
	return &v1.CheckPermissionResponse{
		Permissionship: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
	}, nil
}

func (m *mockPermissionsClient) CheckBulkPermissions(ctx context.Context, req *v1.CheckBulkPermissionsRequest, opts ...grpc.CallOption) (*v1.CheckBulkPermissionsResponse, error) {
	pairs := make([]*v1.CheckBulkPermissionsPair, 0, len(req.Items))

	for _, item := range req.Items {
		// Create a key from the request item to match against responses
		key := item.Resource.ObjectType + ":" + item.Resource.ObjectId + "#" + item.Permission + "@" + item.Subject.Object.ObjectType + ":" + item.Subject.Object.ObjectId

		var permissionship v1.CheckPermissionResponse_Permissionship
		if resp, ok := m.responses[key]; ok {
			permissionship = resp.Permissionship
		} else {
			// Default to no permission
			permissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION
		}

		pair := &v1.CheckBulkPermissionsPair{
			Request: item,
			Response: &v1.CheckBulkPermissionsPair_Item{
				Item: &v1.CheckBulkPermissionsResponseItem{
					Permissionship: permissionship,
				},
			},
		}
		pairs = append(pairs, pair)
	}

	return &v1.CheckBulkPermissionsResponse{
		Pairs: pairs,
	}, nil
}

func (m *mockPermissionsClient) LookupResources(ctx context.Context, req *v1.LookupResourcesRequest, opts ...grpc.CallOption) (v1.PermissionsService_LookupResourcesClient, error) {
	panic("not implemented")
}

func (m *mockPermissionsClient) LookupSubjects(ctx context.Context, req *v1.LookupSubjectsRequest, opts ...grpc.CallOption) (v1.PermissionsService_LookupSubjectsClient, error) {
	panic("not implemented")
}

func (m *mockPermissionsClient) ExpandPermissionTree(ctx context.Context, req *v1.ExpandPermissionTreeRequest, opts ...grpc.CallOption) (*v1.ExpandPermissionTreeResponse, error) {
	panic("not implemented")
}

func (m *mockPermissionsClient) WriteRelationships(ctx context.Context, req *v1.WriteRelationshipsRequest, opts ...grpc.CallOption) (*v1.WriteRelationshipsResponse, error) {
	panic("not implemented")
}

func (m *mockPermissionsClient) DeleteRelationships(ctx context.Context, req *v1.DeleteRelationshipsRequest, opts ...grpc.CallOption) (*v1.DeleteRelationshipsResponse, error) {
	panic("not implemented")
}

func (m *mockPermissionsClient) ReadRelationships(ctx context.Context, req *v1.ReadRelationshipsRequest, opts ...grpc.CallOption) (v1.PermissionsService_ReadRelationshipsClient, error) {
	panic("not implemented")
}

func (m *mockPermissionsClient) ExportBulkRelationships(ctx context.Context, req *v1.ExportBulkRelationshipsRequest, opts ...grpc.CallOption) (v1.PermissionsService_ExportBulkRelationshipsClient, error) {
	panic("not implemented")
}

func (m *mockPermissionsClient) ImportBulkRelationships(ctx context.Context, opts ...grpc.CallOption) (v1.PermissionsService_ImportBulkRelationshipsClient, error) {
	panic("not implemented")
}

func TestPostFilterCompilation(t *testing.T) {
	config := proxyrule.Config{
		Spec: proxyrule.Spec{
			Locking: proxyrule.PessimisticLockMode,
			Matches: []proxyrule.Match{
				{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"list"},
				},
			},
			PostFilters: []proxyrule.PostFilter{
				{
					CheckPermissionTemplate: &proxyrule.StringOrTemplate{
						Template: "pod:{{name}}#view@user:{{user.name}}",
					},
				},
			},
		},
	}

	rule, err := rules.Compile(config)
	require.NoError(t, err)
	require.NotNil(t, rule)
	require.Len(t, rule.PostFilter, 1)
	require.NotNil(t, rule.PostFilter[0].Rel)
}

func TestPostFilterCompilationError(t *testing.T) {
	config := proxyrule.Config{
		Spec: proxyrule.Spec{
			Locking: proxyrule.PessimisticLockMode,
			Matches: []proxyrule.Match{
				{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"list"},
				},
			},
			PostFilters: []proxyrule.PostFilter{
				{
					// Missing CheckPermissionTemplate should cause error
				},
			},
		},
	}

	_, err := rules.Compile(config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "post-filter must have CheckPermissionTemplate defined")
}

func TestFilterListResponse(t *testing.T) {
	// Create test data
	listResponse := map[string]any{
		"apiVersion": "v1",
		"kind":       "PodList",
		"items": []any{
			map[string]any{
				"metadata": map[string]any{
					"name":      "pod1",
					"namespace": "default",
				},
			},
			map[string]any{
				"metadata": map[string]any{
					"name":      "pod2",
					"namespace": "default",
				},
			},
		},
	}

	responseJSON, err := json.Marshal(listResponse)
	require.NoError(t, err)

	recorder := &responseRecorder{
		statusCode: 200,
		body:       responseJSON,
	}

	// Create mock client that allows access to pod1 but not pod2
	mockClient := &mockPermissionsClient{
		responses: map[string]*v1.CheckPermissionResponse{
			"pod:pod1#view@user:testuser": {
				Permissionship: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			},
			"pod:pod2#view@user:testuser": {
				Permissionship: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
			},
		},
	}

	// Create rules with PostFilter
	config := proxyrule.Config{
		Spec: proxyrule.Spec{
			Locking: proxyrule.PessimisticLockMode,
			Matches: []proxyrule.Match{
				{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"list"},
				},
			},
			PostFilters: []proxyrule.PostFilter{
				{
					CheckPermissionTemplate: &proxyrule.StringOrTemplate{
						Template: "pod:{{name}}#view@user:{{user.name}}",
					},
				},
			},
		},
	}

	filteredRules, err := rules.Compile(config)
	require.NoError(t, err)

	input := &rules.ResolveInput{
		Request: &request.RequestInfo{
			Verb: "list",
		},
		User: &user.DefaultInfo{
			Name: "testuser",
		},
	}

	// Filter the response
	err = filterListResponse(t.Context(), recorder, []*rules.RunnableRule{filteredRules}, input, mockClient)
	require.NoError(t, err)

	// Parse the filtered response
	var filteredResponse map[string]any
	err = json.Unmarshal(recorder.body, &filteredResponse)
	require.NoError(t, err)

	// Check that only pod1 is included
	items, ok := filteredResponse["items"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	firstItem, ok := items[0].(map[string]any)
	require.True(t, ok)
	metadata, ok := firstItem["metadata"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "pod1", metadata["name"])
}

func TestFilterItemsWithBulkPermissions(t *testing.T) {
	// Create mock client that allows access to testpod1 but not testpod2
	mockClient := &mockPermissionsClient{
		responses: map[string]*v1.CheckPermissionResponse{
			"pod:testpod1#view@user:testuser": {
				Permissionship: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			},
			"pod:testpod2#view@user:testuser": {
				Permissionship: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
			},
		},
	}

	// Create rules with PostFilter
	config := proxyrule.Config{
		Spec: proxyrule.Spec{
			Locking: proxyrule.PessimisticLockMode,
			Matches: []proxyrule.Match{
				{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"list"},
				},
			},
			PostFilters: []proxyrule.PostFilter{
				{
					CheckPermissionTemplate: &proxyrule.StringOrTemplate{
						Template: "pod:{{name}}#view@user:{{user.name}}",
					},
				},
			},
		},
	}

	filteredRules, err := rules.Compile(config)
	require.NoError(t, err)

	input := &rules.ResolveInput{
		Request: &request.RequestInfo{
			Verb: "list",
		},
		User: &user.DefaultInfo{
			Name: "testuser",
		},
	}

	items := []any{
		map[string]any{
			"metadata": map[string]any{
				"name":      "testpod1",
				"namespace": "default",
			},
		},
		map[string]any{
			"metadata": map[string]any{
				"name":      "testpod2",
				"namespace": "default",
			},
		},
	}

	// Test that only allowed items are returned
	allowedItems, err := filterItemsWithBulkPermissions(t.Context(), items, []*rules.RunnableRule{filteredRules}, input, mockClient)
	require.NoError(t, err)
	require.Len(t, allowedItems, 1)

	// Check that only testpod1 is included
	firstItem, ok := allowedItems[0].(map[string]any)
	require.True(t, ok)
	metadata, ok := firstItem["metadata"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "testpod1", metadata["name"])

	// Test with empty items
	emptyItems, err := filterItemsWithBulkPermissions(t.Context(), []any{}, []*rules.RunnableRule{filteredRules}, input, mockClient)
	require.NoError(t, err)
	require.Empty(t, emptyItems)
}

func TestShouldRunPostFilters(t *testing.T) {
	// Create rules with PostFilter
	config := proxyrule.Config{
		Spec: proxyrule.Spec{
			Locking: proxyrule.PessimisticLockMode,
			Matches: []proxyrule.Match{
				{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"list"},
				},
			},
			PostFilters: []proxyrule.PostFilter{
				{
					CheckPermissionTemplate: &proxyrule.StringOrTemplate{
						Template: "pod:{{name}}#view@user:{{user.name}}",
					},
				},
			},
		},
	}

	rulesWithPostFilter, err := rules.Compile(config)
	require.NoError(t, err)

	// Create rules without PostFilter
	configNoPostFilter := proxyrule.Config{
		Spec: proxyrule.Spec{
			Locking: proxyrule.PessimisticLockMode,
			Matches: []proxyrule.Match{
				{
					GroupVersion: "v1",
					Resource:     "pods",
					Verbs:        []string{"list"},
				},
			},
		},
	}

	rulesWithoutPostFilter, err := rules.Compile(configNoPostFilter)
	require.NoError(t, err)

	// Test that PostFilters should run for list with PostFilter rules
	require.True(t, shouldRunPostFilters("list", []*rules.RunnableRule{rulesWithPostFilter}))

	// Test that PostFilters should not run for list without PostFilter rules
	require.False(t, shouldRunPostFilters("list", []*rules.RunnableRule{rulesWithoutPostFilter}))

	// Test that PostFilters should not run for non-list verbs
	require.False(t, shouldRunPostFilters("get", []*rules.RunnableRule{rulesWithPostFilter}))
	require.False(t, shouldRunPostFilters("create", []*rules.RunnableRule{rulesWithPostFilter}))
	require.False(t, shouldRunPostFilters("update", []*rules.RunnableRule{rulesWithPostFilter}))
	require.False(t, shouldRunPostFilters("delete", []*rules.RunnableRule{rulesWithPostFilter}))
	require.False(t, shouldRunPostFilters("watch", []*rules.RunnableRule{rulesWithPostFilter}))
}
