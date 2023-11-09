package authz

import (
	"context"
	"fmt"
	"net/http"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/cschleiden/go-workflows/client"
	"github.com/google/uuid"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/authz/distributedtx"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

// write performs a dual write according to the passed rule
func write(ctx context.Context, w http.ResponseWriter, r *rules.RunnableRule, input *rules.ResolveInput, workflowClient *client.Client) error {
	writeRels := make([]*v1.Relationship, 0, len(r.Writes))
	for _, write := range r.Writes {
		write := write
		rel, err := rules.ResolveRel(write, input)
		if err != nil {
			return fmt.Errorf("unable to resolve write rule (%v): %w", rel, err)
		}
		writeRels = append(writeRels, &v1.Relationship{
			Resource: &v1.ObjectReference{
				ObjectType: rel.ResourceType,
				ObjectId:   rel.ResourceID,
			},
			Relation: rel.ResourceRelation,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: rel.SubjectType,
					ObjectId:   rel.SubjectID,
				},
				OptionalRelation: rel.SubjectRelation,
			},
		})
	}

	preconditions := make([]*v1.Precondition, 0, len(r.Must)+len(r.MustNot))
	for _, precondition := range r.Must {
		rel, err := rules.ResolveRel(precondition, input)
		if err != nil {
			return fmt.Errorf("unable to resolve must rule (%v): %w", rel, err)
		}
		p := preconditionFromRel(rel)
		p.Operation = v1.Precondition_OPERATION_MUST_MATCH
		preconditions = append(preconditions, p)
	}
	for _, precondition := range r.MustNot {
		rel, err := rules.ResolveRel(precondition, input)
		if err != nil {
			return fmt.Errorf("unable to resolve must not rule (%v): %w", rel, err)
		}
		p := preconditionFromRel(rel)
		p.Operation = v1.Precondition_OPERATION_MUST_NOT_MATCH
		preconditions = append(preconditions, p)
	}

	resp, err := dualWrite(ctx, workflowClient, input, writeRels, preconditions, r.LockMode)
	if err != nil {
		return fmt.Errorf("dual write failed: %w", err)
	}

	// this can happen if there are un-recoverable failures in the
	// workflow execution
	if resp.Body == nil {
		return fmt.Errorf("empty response from dual write: %w", err)
	}

	// write response
	w.Header().Set("Content-Type", resp.ContentType)
	w.WriteHeader(resp.StatusCode)
	if _, err := w.Write(resp.Body); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}
	return nil
}

// getWriteRule returns the first matching rule with `write` defined
func getWriteRule(matchingRules []*rules.RunnableRule) *rules.RunnableRule {
	for _, r := range matchingRules {
		if len(r.Writes) > 0 {
			// we can only do one dual-write per request without some way of
			// marking some writes async.
			return r
		}
	}
	return nil
}

// dualWrite configures the dtx for writing to kube and spicedb and waits for
// the response
func dualWrite(ctx context.Context, workflowClient *client.Client, input *rules.ResolveInput, rels []*v1.Relationship, preconditions []*v1.Precondition, lockMode proxyrule.LockMode) (*distributedtx.KubeResp, error) {
	writeInput := &distributedtx.WriteObjInput{
		RequestInfo:   input.Request,
		UserInfo:      input.User,
		Body:          input.Body,
		Header:        input.Headers,
		Rels:          rels,
		Preconditions: preconditions,
	}
	if input.Object != nil {
		writeInput.ObjectMeta = &input.Object.ObjectMeta
	}

	workflow, err := distributedtx.WorkflowForLockMode(string(lockMode))
	if err != nil {
		return nil, fmt.Errorf("couldn't create worklfow for dual write: %w", err)
	}
	id, err := workflowClient.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: uuid.NewString(),
	}, workflow, writeInput)
	if err != nil {
		return nil, fmt.Errorf("couldn't create new workflow instance for dual write: %w", err)
	}
	resp, err := client.GetWorkflowResult[distributedtx.KubeResp](ctx, workflowClient, id, distributedtx.DefaultWorkflowTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to get dual write result: %w", err)
	}
	return &resp, nil
}

func preconditionFromRel(rel *rules.ResolvedRel) *v1.Precondition {
	p := &v1.Precondition{
		Filter: &v1.RelationshipFilter{
			ResourceType: rel.ResourceType,
		},
	}
	if rel.ResourceID != "*" {
		p.Filter.OptionalResourceId = rel.ResourceID
	}
	if rel.ResourceRelation != "*" {
		p.Filter.OptionalRelation = rel.ResourceRelation
	}
	if rel.SubjectType != "*" || rel.SubjectID != "*" || rel.SubjectRelation != "*" {
		p.Filter.OptionalSubjectFilter = &v1.SubjectFilter{}
	}
	if rel.SubjectType != "*" {
		p.Filter.OptionalSubjectFilter.SubjectType = rel.SubjectType
	}
	if rel.SubjectID != "*" {
		p.Filter.OptionalSubjectFilter.OptionalSubjectId = rel.SubjectID
	}
	if rel.SubjectRelation != "*" && rel.SubjectRelation != "" {
		p.Filter.OptionalSubjectFilter.OptionalRelation = &v1.SubjectFilter_RelationFilter{
			Relation: rel.SubjectRelation,
		}
	}
	return p
}
