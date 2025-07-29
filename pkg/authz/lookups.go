package authz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/pkg/genutil/mapz"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

type prefilterResult struct {
	allAllowed     bool
	allowedResults *mapz.Set[types.NamespacedName]
	err            error
}

func (pr *prefilterResult) IsAllowed(namespace, name string) bool {
	if pr.allAllowed {
		return true
	}
	if pr.allowedResults == nil {
		return false
	}
	return pr.allowedResults.Has(types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	})
}

type wrapper struct {
	ResourceID string `json:"resourceId"`
	SubjectID  string `json:"subjectId"`
}

func runLookupResources(ctx context.Context, client v1.PermissionsServiceClient, filter *rules.ResolvedPreFilter, input *rules.ResolveInput) (*prefilterResult, error) {
	if filter.Rel.ResourceID != proxyrule.MatchingIDFieldValue {
		klog.FromContext(ctx).V(3).Info("preFilter called with non-$ resource ID", "resource_id", filter.Rel.ResourceID)
		return nil, errors.New("preFilter called with non-$ resource ID")
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
		return nil, fmt.Errorf("error create LookupResources client: %w", err)
	}

	filterResult := &prefilterResult{
		allowedResults: mapz.NewSet[types.NamespacedName](),
	}

	for {
		resp, err := lr.Recv()
		if errors.Is(err, io.EOF) {
			klog.FromContext(ctx).V(3).Info("finished receiving LookupResources response", "request", req)
			return filterResult, nil
		}

		if err != nil {
			return nil, err
		}

		if resp.Permissionship != v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION {
			klog.FromContext(ctx).V(3).Info("skipping conditional resource in list", "resource_type", filter.Rel.ResourceType, "resource_id", resp.ResourceObjectId, "condition", resp.PartialCaveatInfo.String())
			continue
		}

		byteIn, err := json.Marshal(wrapper{ResourceID: resp.ResourceObjectId})
		if err != nil {
			return nil, fmt.Errorf("error marshalling LookupResources response: %w", err)
		}

		var data any
		if err := json.Unmarshal(byteIn, &data); err != nil {
			return nil, fmt.Errorf("error unmarshalling LookupResources response: %w", err)
		}

		klog.FromContext(ctx).V(4).Info("received list filter event", "event", string(byteIn))
		name, err := filter.NameFromObjectID.Query(data)
		if err != nil {
			return nil, fmt.Errorf("error querying name from object ID: %w", err)
		}

		if name == nil || len(name.(string)) == 0 {
			klog.FromContext(ctx).V(3).Info("unable to determine name for resource", "event", string(byteIn))
			return nil, errors.New("unable to determine name for resource")
		}

		namespace, err := filter.NamespaceFromObjectID.Query(data)
		if err != nil {
			return nil, fmt.Errorf("error querying namespace from object ID: %w", err)
		}

		if namespace == nil {
			// Convert input to Bloblang format
			inputData := convertInputToBloblangData(input)
			namespace, err = filter.NamespaceFromObjectID.Query(inputData)
			if err != nil {
				return nil, fmt.Errorf("error querying namespace from object ID: %w", err)
			}
		}
		if namespace == nil {
			namespace = ""
		}

		filterResult.allowedResults.Add(types.NamespacedName{
			Name:      name.(string),
			Namespace: namespace.(string),
		})

		klog.FromContext(ctx).V(3).Info("found allowed resource in LookupResources response", "resource_type", filter.Rel.ResourceType, "resource_id", resp.ResourceObjectId)
	}
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
