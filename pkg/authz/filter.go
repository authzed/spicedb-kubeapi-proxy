package authz

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

// filterResponse filters responses by fetching a list of allowed objects in
// parallel with the request
func filterResponse(ctx context.Context, matchingRules []*rules.RunnableRule, input *rules.ResolveInput, authzData *AuthzData, client v1.PermissionsServiceClient, watchClient v1.WatchServiceClient) error {
	// Check if any rule has PostFilters
	hasPostFilter := false
	for _, r := range matchingRules {
		if len(r.PostFilter) > 0 {
			hasPostFilter = true
			break
		}
	}

	// If PostFilter is set but no PreFilter, skip LookupResources call
	if hasPostFilter {
		hasPreFilter := false
		for _, r := range matchingRules {
			if len(r.PreFilter) > 0 {
				hasPreFilter = true
				break
			}
		}
		if !hasPreFilter {
			// Skip LookupResources call when PostFilter is set but PreFilter is missing
			authzData.Lock()
			authzData.skipPreFilter = true
			authzData.Unlock()
			return nil
		}
	}

	for _, r := range matchingRules {
		for _, f := range r.PreFilter {
			f := f
			rel, err := rules.ResolveRel(f.Rel, input)
			if err != nil {
				return err
			}

			filter := &rules.ResolvedPreFilter{
				LookupType:            f.LookupType,
				Rel:                   rel,
				NameFromObjectID:      f.NameFromObjectID,
				NamespaceFromObjectID: f.NamespaceFromObjectID,
			}

			switch input.Request.Verb {
			case "list":
				filterList(ctx, client, filter, input, authzData)
				// only one filter allowed per request
				return nil
			case "watch":
				filterWatch(ctx, client, watchClient, filter, input, authzData)
				// only one filter allowed per request
				return nil
			}
		}
	}
	return nil
}

// alreadyAuthorized adds the input object to the authorized list.
// This is used for requests where the `check` on the rule is expected to authorize
// a single object for filtering, i.e. for `get`, `update`, or `patch`
// (where update/patch don't create the object).
func alreadyAuthorized(input *rules.ResolveInput, authzData *AuthzData) {
	if !slices.Contains([]string{"get", "update", "patch"}, input.Request.Verb) {
		return
	}
	close(authzData.allowedNNC)
	close(authzData.removedNNC)
	authzData.Lock()
	defer authzData.Unlock()

	authzData.allowedNN[types.NamespacedName{Name: input.Name, Namespace: input.Namespace}] = struct{}{}
}

type wrapper struct {
	ResourceID string `json:"resourceId"`
	SubjectID  string `json:"subjectId"`
}

func filterList(ctx context.Context, client v1.PermissionsServiceClient, filter *rules.ResolvedPreFilter, input *rules.ResolveInput, authzData *AuthzData) {
	authzData.Lock()
	go func() {
		defer authzData.Unlock()
		defer close(authzData.allowedNNC)
		defer close(authzData.removedNNC)

		if filter.Rel.ResourceID != proxyrule.MatchingIDFieldValue {
			handleFilterListError(ctx, errors.New("filterList called with non-$ resource ID"))
			return
		}

		req := &v1.LookupResourcesRequest{
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
			},
			ResourceObjectType: filter.Rel.ResourceType,
			Permission:         filter.Rel.ResourceRelation,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: filter.Rel.SubjectType,
					ObjectId:   filter.Rel.SubjectID,
				},
				OptionalRelation: filter.Rel.SubjectRelation,
			},
		}
		klog.FromContext(ctx).V(3).Info("LookupResources", "request", req)
		lr, err := client.LookupResources(ctx, req)
		if err != nil {
			handleFilterListError(ctx, err)
			return
		}
		for {
			resp, err := lr.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				handleFilterListError(ctx, err)
				return
			}

			if resp.Permissionship != v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION {
				klog.FromContext(ctx).V(3).Info("denying conditional resource in list", "resource_type", filter.Rel.ResourceType, "resource_id", resp.ResourceObjectId, "condition", resp.PartialCaveatInfo.String())
				continue
			}

			byteIn, err := json.Marshal(wrapper{ResourceID: resp.ResourceObjectId})
			if err != nil {
				handleFilterListError(ctx, err)
				return
			}
			var data any
			if err := json.Unmarshal(byteIn, &data); err != nil {
				handleFilterListError(ctx, err)
				return
			}

			klog.FromContext(ctx).V(4).Info("received list filter event", "event", string(byteIn))
			name, err := filter.NameFromObjectID.Query(data)
			if err != nil {
				handleFilterListError(ctx, err)
				return
			}
			if name == nil || len(name.(string)) == 0 {
				klog.FromContext(ctx).V(3).Info("unable to determine name for resource", "event", string(byteIn))
				return
			}

			namespace, err := filter.NamespaceFromObjectID.Query(data)
			if err != nil {
				handleFilterListError(ctx, err)
				return
			}
			if namespace == nil {
				// Convert input to Bloblang format
				inputData := convertInputToBloblangData(input)
				namespace, err = filter.NamespaceFromObjectID.Query(inputData)
				if err != nil {
					handleFilterListError(ctx, err)
					return
				}
			}
			if namespace == nil {
				namespace = ""
			}

			authzData.allowedNN[types.NamespacedName{
				Name:      name.(string),
				Namespace: namespace.(string),
			}] = struct{}{}

			klog.FromContext(ctx).V(3).Info("allowed resource in list/LR response", "resource_type", filter.Rel.ResourceType, "resource_id", resp.ResourceObjectId)
		}
	}()
}

func handleFilterListError(ctx context.Context, err error) {
	klog.FromContext(ctx).V(3).Error(err, "error on filterList")
}

func filterWatch(ctx context.Context, client v1.PermissionsServiceClient, watchClient v1.WatchServiceClient, filter *rules.ResolvedPreFilter, input *rules.ResolveInput, authzData *AuthzData) {
	go func() {
		defer close(authzData.allowedNNC)
		defer close(authzData.removedNNC)

		watchResource, err := watchClient.Watch(ctx, &v1.WatchRequest{
			OptionalObjectTypes: []string{filter.Rel.ResourceType},
		})
		if err != nil {
			klog.FromContext(ctx).V(3).Error(err, "error on filterWatch")
			return
		}

		for {
			resp, err := watchResource.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				klog.FromContext(ctx).V(3).Error(err, "error on watchResource.Recv")
				return
			}

			for _, u := range resp.Updates {
				cr, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
					},
					Resource: &v1.ObjectReference{
						ObjectType: filter.Rel.ResourceType,
						// TODO: should swap out subject id if in subject mode
						ObjectId: u.Relationship.Resource.ObjectId,
					},
					Permission: filter.Rel.ResourceRelation,
					Subject: &v1.SubjectReference{
						Object: &v1.ObjectReference{
							ObjectType: filter.Rel.SubjectType,
							ObjectId:   filter.Rel.SubjectID,
						},
						OptionalRelation: filter.Rel.SubjectRelation,
					},
				})
				if err != nil {
					klog.FromContext(ctx).V(3).Error(err, "error on CheckPermission")
					return
				}

				byteIn, err := json.Marshal(wrapper{ResourceID: u.Relationship.Resource.ObjectId, SubjectID: u.Relationship.Subject.Object.ObjectId})
				if err != nil {
					klog.FromContext(ctx).V(3).Error(err, "error marshaling wrapper in filterWatch")
					return
				}
				var data any
				if err := json.Unmarshal(byteIn, &data); err != nil {
					klog.FromContext(ctx).V(3).Error(err, "error unmarshaling data in filterWatch")
					return
				}

				name, err := filter.NameFromObjectID.Query(data)
				if err != nil {
					klog.FromContext(ctx).V(3).Error(err, "error on filter.Name.Query")
					return
				}

				if name == nil || len(name.(string)) == 0 {
					return
				}

				namespace, err := filter.NamespaceFromObjectID.Query(data)
				if err != nil {
					klog.FromContext(ctx).V(3).Error(err, "error on filter.Namespace.Query")
					return
				}
				if namespace == nil {
					namespace = ""
				}

				nn := types.NamespacedName{Name: name.(string), Namespace: namespace.(string)}

				// TODO: this should really be over a single channel to prevent
				//  races on add/remove
				if cr.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
					authzData.allowedNNC <- nn
				} else {
					authzData.removedNNC <- nn
				}
			}
		}
	}()
}

// convertInputToBloblangData converts ResolveInput to a format suitable for Bloblang
func convertInputToBloblangData(input *rules.ResolveInput) any {
	// Convert to a map structure that Bloblang can navigate
	data := map[string]any{
		"name":           input.Name,
		"namespace":      input.Namespace,
		"namespacedName": input.NamespacedName,
		"headers":        input.Headers,
	}

	// Convert request info to map
	if input.Request != nil {
		data["request"] = map[string]any{
			"verb":       input.Request.Verb,
			"apiGroup":   input.Request.APIGroup,
			"apiVersion": input.Request.APIVersion,
			"resource":   input.Request.Resource,
			"name":       input.Request.Name,
			"namespace":  input.Request.Namespace,
		}
	}

	// Convert user info to map
	if input.User != nil {
		data["user"] = map[string]any{
			"name":   input.User.Name,
			"uid":    input.User.UID,
			"groups": input.User.Groups,
			"extra":  input.User.Extra,
		}
	}

	if input.Object != nil {
		// Convert ObjectMeta to a simpler map structure for Bloblang
		labels := make(map[string]any)
		if input.Object.Labels != nil {
			for k, v := range input.Object.Labels {
				labels[k] = v
			}
		}

		objectData := map[string]any{
			"metadata": map[string]any{
				"name":      input.Object.Name,
				"namespace": input.Object.Namespace,
				"labels":    labels,
			},
		}
		data["object"] = objectData
		// Also add metadata directly for easier access
		data["metadata"] = objectData["metadata"]
	}

	if len(input.Body) > 0 {
		data["body"] = input.Body
	}

	return data
}
