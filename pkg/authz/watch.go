package authz

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

type watchResultTracker struct {
	foundChanged chan resultChange
}

type resultChange struct {
	allowed        bool
	namespacedName types.NamespacedName
}

// RunWatch starts a watch on the specified resource type and checks permissions for each update.
func RunWatch(ctx context.Context, watchClient v1.WatchServiceClient, checkClient v1.PermissionsServiceClient, tracker *watchResultTracker, config *rules.ResolvedPreFilter, input *rules.ResolveInput) {
	klog.FromContext(ctx).V(3).Info("starting watch for resource type", "resourceType", config.Rel.ResourceType)
	watchResource, err := watchClient.Watch(ctx, &v1.WatchRequest{
		OptionalObjectTypes: []string{config.Rel.ResourceType},
	})
	if err != nil {
		klog.FromContext(ctx).V(3).Error(err, "error on RunWatch")
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
			klog.FromContext(ctx).V(4).Info("received watch update", "update", u)
			cr, err := checkClient.CheckPermission(ctx, &v1.CheckPermissionRequest{
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
				Resource: &v1.ObjectReference{
					ObjectType: config.Rel.ResourceType,
					// TODO: should swap out subject id if in subject mode
					ObjectId: u.Relationship.Resource.ObjectId,
				},
				Permission: config.Rel.ResourceRelation,
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: config.Rel.SubjectType,
						ObjectId:   config.Rel.SubjectID,
					},
					OptionalRelation: config.Rel.SubjectRelation,
				},
			})
			if err != nil {
				klog.FromContext(ctx).V(3).Error(err, "error on CheckPermission")
				return
			}

			byteIn, err := json.Marshal(wrapper{ResourceID: u.Relationship.Resource.ObjectId, SubjectID: u.Relationship.Subject.Object.ObjectId})
			if err != nil {
				klog.FromContext(ctx).V(3).Error(err, "error marshaling wrapper in RunWatch")
				return
			}
			var data any
			if err := json.Unmarshal(byteIn, &data); err != nil {
				klog.FromContext(ctx).V(3).Error(err, "error unmarshaling data in RunWatch")
				return
			}

			name, err := config.NameFromObjectID.Query(data)
			if err != nil {
				klog.FromContext(ctx).V(3).Error(err, "error on config.Name.Query")
				return
			}

			if name == nil || len(name.(string)) == 0 {
				return
			}

			namespace, err := config.NamespaceFromObjectID.Query(data)
			if err != nil {
				klog.FromContext(ctx).V(3).Error(err, "error on config.Namespace.Query")
				return
			}
			if namespace == nil {
				namespace = ""
			}

			nn := types.NamespacedName{Name: name.(string), Namespace: namespace.(string)}
			if cr.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
				tracker.foundChanged <- resultChange{allowed: true, namespacedName: nn}
			} else {
				tracker.foundChanged <- resultChange{allowed: false, namespacedName: nn}
			}
		}
	}
}
