package authz

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

// checkRelationships performs authorization checks for a slice of relationships
// Always uses bulk CheckBulkPermissions API for consistency and performance
func checkRelationships(ctx context.Context, client v1.PermissionsServiceClient, resolvedRels []*rules.ResolvedRel, checkType string) error {
	if len(resolvedRels) == 0 {
		return nil
	}

	// Always use bulk check - works for single or multiple relationships
	items := make([]*v1.CheckBulkPermissionsRequestItem, len(resolvedRels))
	for i, rel := range resolvedRels {
		items[i] = &v1.CheckBulkPermissionsRequestItem{
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
	}

	bulkReq := &v1.CheckBulkPermissionsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
		},
		Items: items,
	}

	bulkResp, err := client.CheckBulkPermissions(ctx, bulkReq)
	if err != nil {
		return err
	}

	// All checks must pass
	for i, pair := range bulkResp.Pairs {
		if pair.GetError() != nil {
			rel := resolvedRels[i]
			return fmt.Errorf("bulk %s error for %s:%s#%s@%s:%s: %v",
				checkType, rel.ResourceType, rel.ResourceID, rel.ResourceRelation,
				rel.SubjectType, rel.SubjectID, pair.GetError())
		}

		responseItem := pair.GetItem()
		if responseItem == nil || responseItem.Permissionship != v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
			rel := resolvedRels[i]
			return fmt.Errorf("bulk %s failed for %s:%s#%s@%s:%s",
				checkType, rel.ResourceType, rel.ResourceID, rel.ResourceRelation,
				rel.SubjectType, rel.SubjectID)
		}
	}

	return nil
}

var ErrUnauthorized = errors.New("unauthorized operation")

func runAllMatchingChecks(ctx context.Context, matchingRules []*rules.RunnableRule, input *rules.ResolveInput, client v1.PermissionsServiceClient) error {
	var checkGroup errgroup.Group

	// issue checks for all matching rules
	for _, r := range matchingRules {
		for _, c := range r.Checks {
			c := c
			checkGroup.Go(func() error {
				resolvedRels, err := c.GenerateRelationships(input)
				if err != nil {
					return err
				}

				return checkRelationships(ctx, client, resolvedRels, "check")
			})
		}
	}
	return checkGroup.Wait()
}

func runAllMatchingPostChecks(ctx context.Context, matchingRules []*rules.RunnableRule, input *rules.ResolveInput, client v1.PermissionsServiceClient) error {
	var postCheckGroup errgroup.Group

	// issue postchecks for all matching rules
	for _, r := range matchingRules {
		for _, c := range r.PostChecks {
			c := c
			postCheckGroup.Go(func() error {
				resolvedRels, err := c.GenerateRelationships(input)
				if err != nil {
					return err
				}

				return checkRelationships(ctx, client, resolvedRels, "postcheck")
			})
		}
	}
	return postCheckGroup.Wait()
}
