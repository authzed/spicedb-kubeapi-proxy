package authz

import (
	"context"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

func runAllMatchingChecks(ctx context.Context, matchingRules []*rules.RunnableRule, input *rules.ResolveInput, client v1.PermissionsServiceClient) error {
	var checkGroup errgroup.Group

	// issue checks for all matching rules
	for _, r := range matchingRules {
		for _, c := range r.Checks {
			checkGroup.Go(func() error {
				rel, err := rules.ResolveRel(c, input)
				if err != nil {
					return err
				}
				resp, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true},
					},
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
				})
				if err != nil {
					return err
				}
				if resp.Permissionship != v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
					return fmt.Errorf("failed runAllMatchingChecks for %v", rel)
				}
				return nil
			})
		}
	}
	return checkGroup.Wait()
}
