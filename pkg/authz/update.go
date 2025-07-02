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

func relsFromExprs(exprs []*rules.RelExpr, input *rules.ResolveInput) ([]*v1.Relationship, error) {
	rels := make([]*v1.Relationship, 0, len(exprs))
	for _, expr := range exprs {
		rel, err := rules.ResolveRel(expr, input)
		if err != nil {
			return nil, fmt.Errorf("unable to resolve write rule (%v): %w", rel, err)
		}
		rels = append(rels, &v1.Relationship{
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
	return rels, nil
}

// performUpdate performs a dual update according to the passed rule
func performUpdate(ctx context.Context, w http.ResponseWriter, r *rules.RunnableRule, input *rules.ResolveInput, workflowClient *client.Client) error {
	preconditions := make([]*v1.Precondition, 0, len(r.Update.MustExist)+len(r.Update.MustNotExist))

	createRels, err := relsFromExprs(r.Update.Creates, input)
	if err != nil {
		return fmt.Errorf("unable to resolve create relationships: %w", err)
	}

	touchRels, err := relsFromExprs(r.Update.Touches, input)
	if err != nil {
		return fmt.Errorf("unable to resolve touch relationships: %w", err)
	}

	deleteRels, err := relsFromExprs(r.Update.Deletes, input)
	if err != nil {
		return fmt.Errorf("unable to resolve delete relationships: %w", err)
	}

	for _, precondition := range r.Update.MustExist {
		rel, err := rules.ResolveRel(precondition, input)
		if err != nil {
			return fmt.Errorf("unable to resolve must rule (%v): %w", rel, err)
		}
		p := preconditionFromRel(rel)
		p.Operation = v1.Precondition_OPERATION_MUST_MATCH
		preconditions = append(preconditions, p)
	}
	for _, precondition := range r.Update.MustNotExist {
		rel, err := rules.ResolveRel(precondition, input)
		if err != nil {
			return fmt.Errorf("unable to resolve must not rule (%v): %w", rel, err)
		}
		p := preconditionFromRel(rel)
		p.Operation = v1.Precondition_OPERATION_MUST_NOT_MATCH
		preconditions = append(preconditions, p)
	}

	resp, err := dualWrite(ctx, workflowClient, input, createRels, touchRels, deleteRels, preconditions, r.LockMode)
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

// getSingleUpdateRule returns the first matching rule with `update` defined
func getSingleUpdateRule(matchingRules []*rules.RunnableRule) (*rules.RunnableRule, error) {
	rulesWithUpdates := make([]*rules.RunnableRule, 0, len(matchingRules))
	for _, r := range matchingRules {
		if r.Update != nil {
			rulesWithUpdates = append(rulesWithUpdates, r)
		}
	}

	if len(rulesWithUpdates) == 0 {
		return nil, nil
	}

	if len(rulesWithUpdates) > 1 {
		return nil, fmt.Errorf("multiple write rules matched: %v", rulesWithUpdates)
	}

	return rulesWithUpdates[0], nil
}

// dualWrite configures the dtx for writing to kube and spicedb and waits for
// the response
func dualWrite(
	ctx context.Context,
	workflowClient *client.Client,
	input *rules.ResolveInput,
	createRels []*v1.Relationship,
	touchRels []*v1.Relationship,
	deleteRels []*v1.Relationship,
	preconditions []*v1.Precondition,
	lockMode proxyrule.LockMode,
) (*distributedtx.KubeResp, error) {
	writeInput := &distributedtx.WriteObjInput{
		RequestInfo:         input.Request,
		UserInfo:            input.User,
		Body:                input.Body,
		Header:              input.Headers,
		Preconditions:       preconditions,
		CreateRelationships: createRels,
		TouchRelationships:  touchRels,
		DeleteRelationships: deleteRels,
	}
	if input.Object != nil {
		writeInput.ObjectMeta = &input.Object.ObjectMeta
	}

	workflow, err := distributedtx.WorkflowForLockMode(string(lockMode))
	if err != nil {
		return nil, fmt.Errorf("couldn't create workflow for dual write: %w", err)
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
	if rel.ResourceID != "$resourceID" {
		p.Filter.OptionalResourceId = rel.ResourceID
	}
	if rel.ResourceRelation != "$resourceRelation" {
		p.Filter.OptionalRelation = rel.ResourceRelation
	}
	if rel.SubjectType != "$subjectType" || rel.SubjectID != "$subjectID" || rel.SubjectRelation != "$subjectRelation" {
		p.Filter.OptionalSubjectFilter = &v1.SubjectFilter{}
	}
	if rel.SubjectType != "$subjectType" {
		p.Filter.OptionalSubjectFilter.SubjectType = rel.SubjectType
	}
	if rel.SubjectID != "$subjectID" {
		p.Filter.OptionalSubjectFilter.OptionalSubjectId = rel.SubjectID
	}
	if rel.SubjectRelation != "$subjectRelation" && rel.SubjectRelation != "" {
		p.Filter.OptionalSubjectFilter.OptionalRelation = &v1.SubjectFilter_RelationFilter{
			Relation: rel.SubjectRelation,
		}
	}
	return p
}
