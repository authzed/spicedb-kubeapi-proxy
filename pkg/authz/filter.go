package authz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

// filterResponse filters responses by fetching a list of allowed objects in
// parallel with the request
func filterResponse(ctx context.Context, matchingRules []*rules.RunnableRule, input *rules.ResolveInput, authzData *AuthzData, client v1.PermissionsServiceClient, watchClient v1.WatchServiceClient) error {
	for _, r := range matchingRules {
		for _, f := range r.PreFilter {
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
				filterList(ctx, client, filter, authzData)
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

func authorizeGet(input *rules.ResolveInput, authzData *AuthzData) {
	if input.Request.Verb != "get" {
		return
	}
	close(authzData.allowedNNC)
	close(authzData.removedNNC)
	authzData.Lock()
	defer authzData.Unlock()
	// gets aren't really filtered, but we add them to the allowed list so that
	// downstream can know it's passed all configured checks.
	authzData.allowedNN[types.NamespacedName{Name: input.Name, Namespace: input.Namespace}] = struct{}{}
}

type wrapper struct {
	ResourceID string `json:"resourceId"`
	SubjectID  string `json:"subjectId"`
}

func filterList(ctx context.Context, client v1.PermissionsServiceClient, filter *rules.ResolvedPreFilter, authzData *AuthzData) {
	go func() {
		authzData.Lock()
		defer authzData.Unlock()
		defer close(authzData.allowedNNC)
		defer close(authzData.removedNNC)

		lr, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true},
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
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		for {
			resp, err := lr.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				fmt.Println(err)
				return
			}

			byteIn, err := json.Marshal(wrapper{ResourceID: resp.ResourceObjectId})
			if err != nil {
				fmt.Println(err)
				return
			}
			var data any
			if err := json.Unmarshal(byteIn, &data); err != nil {
				fmt.Println(err)
				return
			}

			fmt.Println("GOT WATCH FILTER EVENT", string(byteIn))
			name, err := filter.Name.Search(data)
			if err != nil {
				fmt.Println(err)
				return
			}
			if name == nil || len(name.(string)) == 0 {
				return
			}
			namespace, err := filter.Namespace.Search(data)
			if err != nil {
				fmt.Println(err)
				return
			}
			if namespace == nil {
				namespace = ""
			}
			fmt.Println("NAMENS", name, namespace)

			// TODO: check permissionship?
			authzData.allowedNN[types.NamespacedName{
				Name:      name.(string),
				Namespace: namespace.(string),
			}] = struct{}{}
			fmt.Println("allowed", resp.ResourceObjectId)
		}
	}()
}

func filterWatch(ctx context.Context, client v1.PermissionsServiceClient, watchClient v1.WatchServiceClient, filter *rules.ResolvedPreFilter, input *rules.ResolveInput, authzData *AuthzData) {
	go func() {
		defer close(authzData.allowedNNC)
		defer close(authzData.removedNNC)

		watchResource, err := watchClient.Watch(ctx, &v1.WatchRequest{
			OptionalObjectTypes: []string{filter.Rel.ResourceType},
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		for {
			resp, err := watchResource.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				fmt.Println(err)
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
					fmt.Println(err)
					return
				}

				byteIn, err := json.Marshal(wrapper{ResourceID: u.Relationship.Resource.ObjectId, SubjectID: u.Relationship.Subject.Object.ObjectId})
				if err != nil {
					fmt.Println(err)
					return
				}
				var data any
				if err := json.Unmarshal(byteIn, &data); err != nil {
					fmt.Println(err)
					return
				}
				fmt.Println(data)
				fmt.Println("RESPONSE", string(byteIn))

				name, err := filter.Name.Search(data)
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Println("GOT NAME", name)
				if name == nil || len(name.(string)) == 0 {
					return
				}
				namespace, err := filter.Namespace.Search(data)
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Println("GOT NAMESPACE", namespace)
				if namespace == nil {
					namespace = ""
				}
				nn := types.NamespacedName{Name: name.(string), Namespace: namespace.(string)}

				// TODO: this should really be over a single channel to prevent
				//  races on add/remove
				fmt.Println(u.Relationship.Resource.ObjectId, cr.Permissionship)
				if cr.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
					authzData.allowedNNC <- nn
				} else {
					authzData.removedNNC <- nn
				}
			}
		}
	}()
}