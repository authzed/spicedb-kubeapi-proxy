package authz

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/klog/v2"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

var ErrUnauthorized = errors.New("unauthorized operation")

func runAllMatchingChecks(ctx context.Context, matchingRules []*rules.RunnableRule, input *rules.ResolveInput, client v1.PermissionsServiceClient) error {
	var checkGroup errgroup.Group

	// issue checks for all matching rules
	for _, r := range matchingRules {
		for _, c := range r.Checks {
			c := c
			checkGroup.Go(func() error {
				rel, err := rules.ResolveRel(c, input)
				if err != nil {
					return err
				}
				req := &v1.CheckPermissionRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
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
				}
				resp, err := client.CheckPermission(ctx, req)
				klog.V(3).InfoSDepth(1, "CheckPermission", "request", req, "response", resp, "error", err)
				if err != nil {
					return fmt.Errorf("failed runAllMatchingChecks: %w", err)
				}
				if resp.Permissionship != v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
					return ErrUnauthorized
				}
				return nil
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
				rel, err := rules.ResolveRel(c, input)
				if err != nil {
					return err
				}
				req := &v1.CheckPermissionRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
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
				}
				resp, err := client.CheckPermission(ctx, req)
				klog.V(3).InfoSDepth(1, "PostCheckPermission", "request", req, "response", resp, "error", err)
				if err != nil {
					return fmt.Errorf("failed runAllMatchingPostChecks: %w", err)
				}
				if resp.Permissionship != v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
					return ErrUnauthorized
				}
				return nil
			})
		}
	}
	return postCheckGroup.Wait()
}
