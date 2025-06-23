package authz

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"

	"github.com/kyverno/go-jmespath"
	"k8s.io/klog/v2"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

// filterResponse filters responses by fetching a list of allowed objects in
// parallel with the request
func filterResponse(ctx context.Context, matchingRules []*rules.RunnableRule, input *rules.ResolveInput, authzData *AuthzData, client v1.PermissionsServiceClient, watchClient v1.WatchServiceClient) error {
	for _, r := range matchingRules {
		for _, f := range r.PreFilter {
			f := f
			rel, err := rules.ResolveRel(f.Rel, input)
			if err != nil {
				return err
			}

			filter := &rules.ResolvedPreFilter{
				LookupType: f.LookupType,
				Rel:        rel,
				Name:       f.Name,
				Namespace:  f.Namespace,
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
	go func() {
		authzData.Lock()
		defer authzData.Unlock()
		defer close(authzData.allowedNNC)
		defer close(authzData.removedNNC)

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
		klog.V(3).InfoSDepth(1, "LookupResources", "request", req)
		lr, err := client.LookupResources(ctx, req)
		if err != nil {
			handleFilterListError(err)
			return
		}
		for {
			resp, err := lr.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				handleFilterListError(err)
				return
			}

			if resp.Permissionship != v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION {
				klog.V(3).InfoS("denying conditional resource in list", "resource_type", filter.Rel.ResourceType, "resource_id", resp.ResourceObjectId, "condition", resp.PartialCaveatInfo.String())
				continue
			}

			byteIn, err := json.Marshal(wrapper{ResourceID: resp.ResourceObjectId})
			if err != nil {
				handleFilterListError(err)
				return
			}
			var data any
			if err := json.Unmarshal(byteIn, &data); err != nil {
				handleFilterListError(err)
				return
			}

			klog.V(4).InfoS("received list filter event", "event", string(byteIn))
			name, err := filter.Name.Search(data)
			if err != nil {
				handleFilterListError(err)
				return
			}
			if name == nil || len(name.(string)) == 0 {
				klog.V(3).InfoS("unable to determine name for resource", "event", string(byteIn))
				return
			}

			namespace, err := filter.Namespace.Search(data)
			if err != nil {
				if _, ok := err.(jmespath.NotFoundError); !ok {
					handleFilterListError(err)
					return
				}
			}
			if namespace == nil {
				namespace, err = filter.Namespace.Search(input)
				if err != nil {
					handleFilterListError(err)
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

			klog.V(3).InfoS("allowed resource in list/LR response", "resource_type", filter.Rel.ResourceType, "resource_id", resp.ResourceObjectId)
		}
	}()
}

func handleFilterListError(err error) {
	klog.V(3).ErrorS(err, "error on filterList")
}

func filterWatch(ctx context.Context, client v1.PermissionsServiceClient, watchClient v1.WatchServiceClient, filter *rules.ResolvedPreFilter, input *rules.ResolveInput, authzData *AuthzData) {
	go func() {
		defer close(authzData.allowedNNC)
		defer close(authzData.removedNNC)

		watchResource, err := watchClient.Watch(ctx, &v1.WatchRequest{
			OptionalObjectTypes: []string{filter.Rel.ResourceType},
		})
		if err != nil {
			klog.V(3).ErrorS(err, "error on filterWatch")
			return
		}

		for {
			resp, err := watchResource.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				klog.V(3).ErrorS(err, "error on watchResource.Recv")
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
					klog.V(3).ErrorS(err, "error on CheckPermission")
					return
				}

				byteIn, err := json.Marshal(wrapper{ResourceID: u.Relationship.Resource.ObjectId, SubjectID: u.Relationship.Subject.Object.ObjectId})
				if err != nil {
					klog.V(3).ErrorS(err, "error marshaling wrapper in filterWatch")
					return
				}
				var data any
				if err := json.Unmarshal(byteIn, &data); err != nil {
					klog.V(3).ErrorS(err, "error unmarshaling data in filterWatch")
					return
				}

				name, err := filter.Name.Search(data)
				if err != nil {
					klog.V(3).ErrorS(err, "error on filter.Name.Search")
					return
				}

				if name == nil || len(name.(string)) == 0 {
					return
				}

				namespace, err := filter.Namespace.Search(data)
				if err != nil {
					klog.V(3).ErrorS(err, "error on filter.Namespace.Search")
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
