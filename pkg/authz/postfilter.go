package authz

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

// filterListResponse filters the list response by checking permissions for each object using bulk permission checking
func filterListResponse(ctx context.Context, recorder *responseRecorder, filteredRules []*rules.RunnableRule, input *rules.ResolveInput, permissionsClient v1.PermissionsServiceClient) error {
	// Parse the response body as JSON
	var listResponse map[string]interface{}
	if err := json.Unmarshal(recorder.body, &listResponse); err != nil {
		return fmt.Errorf("failed to parse list response: %w", err)
	}

	// Extract items array
	items, ok := listResponse["items"].([]interface{})
	if !ok {
		// If there's no items array, return the original response
		return nil
	}

	if len(items) == 0 {
		// No items to filter
		return nil
	}

	// Get the allowed items using bulk permission checking
	allowedItems, err := filterItemsWithBulkPermissions(ctx, items, filteredRules, input, permissionsClient)
	if err != nil {
		return fmt.Errorf("failed to filter items with bulk permissions: %w", err)
	}

	// Update the response with filtered items
	listResponse["items"] = allowedItems

	// Marshal the filtered response back to JSON
	filteredJSON, err := json.Marshal(listResponse)
	if err != nil {
		return fmt.Errorf("failed to marshal filtered response: %w", err)
	}

	// Update the recorder body
	recorder.SetBody(filteredJSON)

	return nil
}

// filterItemsWithBulkPermissions filters items using bulk permission checking for better performance
func filterItemsWithBulkPermissions(ctx context.Context, items []interface{}, filteredRules []*rules.RunnableRule, input *rules.ResolveInput, permissionsClient v1.PermissionsServiceClient) ([]interface{}, error) {
	if len(items) == 0 {
		return items, nil
	}

	// Build bulk permission check requests
	var bulkItems []*v1.CheckBulkPermissionsRequestItem
	var itemToRequestMap = make(map[int][]int) // maps item index to request indices

	for itemIndex, item := range items {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		// Convert item to PartialObjectMetadata
		var objectMeta *metav1.PartialObjectMetadata
		if metadata, ok := itemMap["metadata"].(map[string]interface{}); ok {
			objectMeta = &metav1.PartialObjectMetadata{}
			if name, ok := metadata["name"].(string); ok {
				objectMeta.Name = name
			}
			if namespace, ok := metadata["namespace"].(string); ok {
				objectMeta.Namespace = namespace
			}
		}

		// Create a new input with the current item data
		itemInput := rules.NewResolveInput(input.Request, input.User, objectMeta, nil, nil)

		// Create permission check requests for all PostFilter rules
		for _, r := range filteredRules {
			for _, f := range r.PostFilter {
				rel, err := rules.ResolveRel(f.Rel, itemInput)
				if err != nil {
					klog.V(3).ErrorS(err, "failed to resolve PostFilter relation", "item", itemMap)
					continue // Skip this check but don't fail the entire operation
				}

				requestItem := &v1.CheckBulkPermissionsRequestItem{
					Resource: &v1.ObjectReference{
						ObjectType: rel.ResourceType,
						ObjectId:   rel.ResourceID,
					},
					Permission: rel.ResourceRelation,
					Subject: &v1.SubjectReference{
						Object: &v1.ObjectReference{
							ObjectType: rel.SubjectType,
							ObjectId:   rel.SubjectID,
						},
						OptionalRelation: rel.SubjectRelation,
					},
				}

				requestIndex := len(bulkItems)
				bulkItems = append(bulkItems, requestItem)

				// Track which requests belong to which item
				itemToRequestMap[itemIndex] = append(itemToRequestMap[itemIndex], requestIndex)
			}
		}
	}

	if len(bulkItems) == 0 {
		// No permission checks needed, return all items
		return items, nil
	}

	// Make the bulk permission check
	bulkReq := &v1.CheckBulkPermissionsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
		},
		Items: bulkItems,
	}

	bulkResp, err := permissionsClient.CheckBulkPermissions(ctx, bulkReq)
	if err != nil {
		return nil, fmt.Errorf("failed to check bulk permissions: %w", err)
	}

	klog.V(3).InfoSDepth(1, "PostFilter CheckBulkPermissions", "request_count", len(bulkItems), "response_count", len(bulkResp.Pairs))

	// Process the results and filter items
	var allowedItems []interface{}

	for itemIndex, item := range items {
		requestIndices, hasChecks := itemToRequestMap[itemIndex]
		if !hasChecks {
			// No permission checks for this item, include it
			allowedItems = append(allowedItems, item)
			continue
		}

		// Check if all permission checks for this item passed
		allPassed := true
		for _, requestIndex := range requestIndices {
			if requestIndex >= len(bulkResp.Pairs) {
				klog.V(3).ErrorS(nil, "bulk response missing expected pair", "request_index", requestIndex)
				allPassed = false
				break
			}

			pair := bulkResp.Pairs[requestIndex]
			if pair.GetError() != nil {
				klog.V(3).ErrorS(nil, "permission check error in bulk response", "error", pair.GetError())
				allPassed = false
				break
			}

			responseItem := pair.GetItem()
			if responseItem == nil || responseItem.Permissionship != v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
				allPassed = false
				break
			}
		}

		if allPassed {
			allowedItems = append(allowedItems, item)
		}
	}

	klog.V(3).InfoSDepth(1, "PostFilter allowed items", "count", len(allowedItems))
	return allowedItems, nil
}
