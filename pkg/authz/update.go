package authz

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/workflow"
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
		relationship := &v1.Relationship{
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
		}
		if err := relationship.Validate(); err != nil {
			return nil, fmt.Errorf("invalid relationship `%s`: %w", relationship, err)
		}
		rels = append(rels, relationship)
	}
	return rels, nil
}

// performUpdate performs a dual update according to the passed rule
func performUpdate(ctx context.Context, w http.ResponseWriter, r *rules.RunnableRule, input *rules.ResolveInput, requestURI string, workflowClient *client.Client) error {
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
		filterFromRel, err := filterFromRel(rel)
		if err != nil {
			return fmt.Errorf("unable to create filter from relationship (%v): %w", rel, err)
		}
		p := &v1.Precondition{
			Filter: filterFromRel,
		}
		p.Operation = v1.Precondition_OPERATION_MUST_MATCH
		preconditions = append(preconditions, p)
	}
	for _, precondition := range r.Update.MustNotExist {
		rel, err := rules.ResolveRel(precondition, input)
		if err != nil {
			return fmt.Errorf("unable to resolve must not rule (%v): %w", rel, err)
		}
		filterFromRel, err := filterFromRel(rel)
		if err != nil {
			return fmt.Errorf("unable to create filter from relationship (%v): %w", rel, err)
		}
		p := &v1.Precondition{
			Filter: filterFromRel,
		}
		p.Operation = v1.Precondition_OPERATION_MUST_NOT_MATCH
		preconditions = append(preconditions, p)
	}

	deleteByFilter := make([]*v1.RelationshipFilter, 0, len(r.Update.DeletesByFilter))
	for _, deleteByFilterExpr := range r.Update.DeletesByFilter {
		rel, err := rules.ResolveRel(deleteByFilterExpr, input)
		if err != nil {
			return fmt.Errorf("unable to resolve delete by filter (%v): %w", deleteByFilterExpr, err)
		}

		filter, err := filterFromRel(rel)
		if err != nil {
			return fmt.Errorf("unable to create filter from relationship (%v): %w", rel, err)
		}
		deleteByFilter = append(deleteByFilter, filter)
	}

	resp, err := dualWrite(ctx, workflowClient, input, requestURI, createRels, touchRels, deleteRels, preconditions, deleteByFilter, r.LockMode)
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
	requestURI string,
	createRels []*v1.Relationship,
	touchRels []*v1.Relationship,
	deleteRels []*v1.Relationship,
	preconditions []*v1.Precondition,
	deleteByFilter []*v1.RelationshipFilter,
	lockMode proxyrule.LockMode,
) (*distributedtx.KubeResp, error) {
	writeInput := &distributedtx.WriteObjInput{
		RequestInfo:         input.Request,
		RequestURI:          requestURI,
		UserInfo:            input.User,
		Body:                input.Body,
		Header:              input.Headers,
		Preconditions:       preconditions,
		CreateRelationships: createRels,
		TouchRelationships:  touchRels,
		DeleteRelationships: deleteRels,
		DeleteByFilter:      deleteByFilter,
	}
	if input.Object != nil {
		writeInput.ObjectMeta = &input.Object.ObjectMeta
	}

	wf, err := distributedtx.WorkflowForLockMode(string(lockMode))
	if err != nil {
		return nil, fmt.Errorf("couldn't create workflow for dual write: %w", err)
	}
	id, err := workflowClient.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: uuid.NewString(),
	}, wf, writeInput)
	if err != nil {
		return nil, fmt.Errorf("couldn't create new workflow instance for dual write: %w", err)
	}
	resp, err := client.GetWorkflowResult[distributedtx.KubeResp](ctx, workflowClient, id, distributedtx.DefaultWorkflowTimeout)
	if err != nil {
		var panicError *workflow.PanicError
		if errors.As(err, &panicError) {
			// If the workflow panicked, we return the panic error, with the stack trace.
			return nil, fmt.Errorf("workflow had a panic: %w\nstack: %s", panicError, panicError.Stack())
		}

		return nil, fmt.Errorf("failed to get dual write result: %w", err)
	}
	return &resp, nil
}

func validateFieldForDollarUsage(field, fieldName string, allowedTemplate string) error {
	if !strings.Contains(field, "$") {
		return nil
	}
	if field == allowedTemplate {
		return nil
	}
	return fmt.Errorf("invalid use of '$' in %s field '%s': only '%s' is allowed", fieldName, field, allowedTemplate)
}

func filterFromRel(rel *rules.ResolvedRel) (*v1.RelationshipFilter, error) {
	if err := validateFieldForDollarUsage(rel.ResourceType, "resourceType", "$resourceType"); err != nil {
		return nil, err
	}
	if err := validateFieldForDollarUsage(rel.ResourceID, "resourceID", "$resourceID"); err != nil {
		return nil, err
	}
	if err := validateFieldForDollarUsage(rel.ResourceRelation, "resourceRelation", "$resourceRelation"); err != nil {
		return nil, err
	}
	if err := validateFieldForDollarUsage(rel.SubjectType, "subjectType", "$subjectType"); err != nil {
		return nil, err
	}
	if err := validateFieldForDollarUsage(rel.SubjectID, "subjectID", "$subjectID"); err != nil {
		return nil, err
	}
	if err := validateFieldForDollarUsage(rel.SubjectRelation, "subjectRelation", "$subjectRelation"); err != nil {
		return nil, err
	}

	f := &v1.RelationshipFilter{}

	if rel.ResourceType != "$resourceType" {
		f.ResourceType = rel.ResourceType
	}
	if rel.ResourceID != "$resourceID" {
		f.OptionalResourceId = rel.ResourceID
	}
	if rel.ResourceRelation != "$resourceRelation" {
		f.OptionalRelation = rel.ResourceRelation
	}
	var needsSubjectFilter bool

	if rel.SubjectType != "$subjectType" && rel.SubjectType != "" {
		needsSubjectFilter = true
	}
	if rel.SubjectID != "$subjectID" && rel.SubjectID != "" {
		needsSubjectFilter = true
	}
	if rel.SubjectRelation != "$subjectRelation" && rel.SubjectRelation != "" {
		needsSubjectFilter = true
	}

	if needsSubjectFilter {
		f.OptionalSubjectFilter = &v1.SubjectFilter{}

		if rel.SubjectType != "$subjectType" && rel.SubjectType != "" {
			f.OptionalSubjectFilter.SubjectType = rel.SubjectType
		}
		if rel.SubjectID != "$subjectID" && rel.SubjectID != "" {
			f.OptionalSubjectFilter.OptionalSubjectId = rel.SubjectID
		}
		if rel.SubjectRelation != "$subjectRelation" && rel.SubjectRelation != "" {
			f.OptionalSubjectFilter.OptionalRelation = &v1.SubjectFilter_RelationFilter{
				Relation: rel.SubjectRelation,
			}
		}
	}

	if err := f.Validate(); err != nil {
		return nil, fmt.Errorf("invalid relationship filter: %w", err)
	}

	return f, nil
}
