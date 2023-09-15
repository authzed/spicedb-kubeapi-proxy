package distributedtx

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"net/http"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/cespare/xxhash/v2"
	"github.com/cschleiden/go-workflows/workflow"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	lockResourceType                         = "lock"
	lockRelationName                         = "workflow"
	workflowResourceType                     = "workflow"
	MaxKubeAttempts                          = 5
	DefaultLockMode                          = StrategyPessimisticWriteToSpiceDBAndKube
	StrategyOptimisticWriteToSpiceDBAndKube  = "OptimisticWriteToSpiceDBAndKube"
	StrategyPessimisticWriteToSpiceDBAndKube = "PessimisticWriteToSpiceDBAndKube"
	DefaultWorkflowTimeout                   = time.Second * 30
)

var KubeBackoff = wait.Backoff{
	Duration: 100 * time.Millisecond,
	Factor:   2,
	Jitter:   0.1,
	Steps:    MaxKubeAttempts,
}

type CreateObjInput struct {
	RequestInfo *request.RequestInfo
	UserInfo    *user.DefaultInfo
	ObjectMeta  *metav1.ObjectMeta
	Body        []byte
}

func (input *CreateObjInput) validate() error {
	if input.UserInfo.GetName() == "" {
		return fmt.Errorf("missing user info in CreateObjectInput")
	}

	if input.ObjectMeta.Name == "" {
		return fmt.Errorf("missing object meta in CreateObjectInput")
	}

	// TODO more validation

	return nil
}

func (input *CreateObjInput) toKubeReqInput() *KubeReqInput {
	return &KubeReqInput{
		RequestInfo: input.RequestInfo,
		ObjectMeta:  input.ObjectMeta,
		Body:        input.Body,
	}
}

type RollbackRelationships []*v1.Relationship

func NewRollbackRelationships(rels ...*v1.Relationship) *RollbackRelationships {
	r := RollbackRelationships(rels)
	return &r
}

func (r *RollbackRelationships) WithRels(relationships ...*v1.Relationship) *RollbackRelationships {
	*r = append(*r, relationships...)
	return r
}

func (r *RollbackRelationships) Cleanup(ctx workflow.Context) {
	updates := make([]*v1.RelationshipUpdate, 0, len(*r))
	for _, rel := range *r {
		rel := rel
		updates = append(updates, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
			Relationship: rel,
		})
	}

	for {
		f := workflow.ExecuteActivity[*v1.WriteRelationshipsResponse](ctx,
			workflow.DefaultActivityOptions,
			activityHandler.WriteToSpiceDB,
			&v1.WriteRelationshipsRequest{Updates: updates})

		if _, err := f.Get(ctx); err != nil {
			fmt.Println("error deleting lock tuple", err)
			continue
		}
		// no error, delete succeeded, exit loop
		break
	}
}

// PessimisticWriteToSpiceDBAndKube ensures that a write exists in both SpiceDB
// and kube, or neither, using locks. It prevents multiple users from writing
// the same object/fields at the same time
func PessimisticWriteToSpiceDBAndKube(ctx workflow.Context, input *CreateObjInput) (*KubeResp, error) {
	if err := input.validate(); err != nil {
		return nil, fmt.Errorf("invalid input to PessimisticWriteToSpiceDBAndKube: %w", err)
	}

	instance := workflow.WorkflowInstance(ctx)
	// this is hardcoded for namespaces for now; should be configurable and
	// come from the workflow input
	resourceRel := SpiceDBNamespaceRel(input)
	clusterRel := SpiceDBClusterRel(input)
	resourceLockRel := ResourceLockRel(input, instance.InstanceID)

	// tuples to remove when the workflow is complete.
	// in some cases we will roll back the input, in all cases we remove
	// the lock when complete.
	rollback := NewRollbackRelationships(resourceLockRel)

	arg := &v1.WriteRelationshipsRequest{
		OptionalPreconditions: []*v1.Precondition{
			resourceLockDoesNotExist(resourceLockRel),
			clusterLockDoesNotExist(clusterRel),
		},
		Updates: []*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: resourceRel,
		}, {
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: resourceLockRel,
		}, {
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: clusterRel,
		}}}

	_, err := workflow.ExecuteActivity[*v1.WriteRelationshipsResponse](ctx,
		workflow.DefaultActivityOptions,
		activityHandler.WriteToSpiceDB,
		arg).Get(ctx)
	if err != nil {
		// request failed for some reason
		fmt.Println("spicedb write failed", err)

		rollback.WithRels(resourceRel, clusterRel).Cleanup(ctx)

		// if the spicedb write fails, report it as a kube conflict error
		// we return this for any error, not just lock conflicts, so that the
		// user will attempt to retry instead of the workflow (nothing from the
		// workflow has succeeded, so there's not much use in retrying automatically).
		return KubeConflict(err, input), nil
	}

	backoff := wait.Backoff{
		Duration: KubeBackoff.Duration,
		Factor:   KubeBackoff.Factor,
		Jitter:   KubeBackoff.Jitter,
		Steps:    KubeBackoff.Steps,
		Cap:      KubeBackoff.Cap,
	}
	for i := 0; i <= MaxKubeAttempts; i++ {
		// Attempt to write to kube
		out, err := workflow.ExecuteActivity[*KubeResp](ctx,
			workflow.DefaultActivityOptions,
			activityHandler.WriteToKube,
			input.toKubeReqInput()).Get(ctx)
		if err != nil {
			// didn't get a response from kube, try again
			fmt.Println("kube write failed", err)
			time.Sleep(backoff.Step())
			continue
		}

		details := out.Err.ErrStatus.Details
		if details != nil && details.RetryAfterSeconds > 0 {
			time.Sleep(time.Duration(out.Err.ErrStatus.Details.RetryAfterSeconds) * time.Second)
			continue
		}

		if out.StatusCode == http.StatusConflict || out.StatusCode == http.StatusCreated {
			rollback.Cleanup(ctx)
			return out, nil
		}

		// some other status code is some other type of error, remove
		// the original tuple and the lock tuple
		rollback.WithRels(resourceRel, clusterRel).Cleanup(ctx)
		return out, nil
	}

	rollback.WithRels(resourceRel, clusterRel).Cleanup(ctx)
	return nil, fmt.Errorf("failed to communicate with kubernetes after %d attempts", MaxKubeAttempts)
}

// OptimisticWriteToSpiceDBAndKube ensures that a write exists in both SpiceDB and kube,
// or neither. It attempts to perform the writes and rolls back if errors are
// encountered, leaving the user to retry on write conflicts.
func OptimisticWriteToSpiceDBAndKube(ctx workflow.Context, input *CreateObjInput) (*KubeResp, error) {
	if err := input.validate(); err != nil {
		return nil, fmt.Errorf("invalid input to PessimisticWriteToSpiceDBAndKube: %w", err)
	}

	// TODO: this could optionally use dry-run to preflight the kube request

	// this is hardcoded for namespaces for now; should be configurable and
	// come from the workflow input
	spiceDBRelationship := SpiceDBNamespaceRel(input)
	clusterRelationship := SpiceDBClusterRel(input)
	rollback := NewRollbackRelationships(spiceDBRelationship, clusterRelationship)

	arg := &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: spiceDBRelationship,
		}, {
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: clusterRelationship,
		}},
	}
	_, err := workflow.ExecuteActivity[*v1.WriteRelationshipsResponse](ctx,
		workflow.DefaultActivityOptions,
		activityHandler.WriteToSpiceDB,
		arg).Get(ctx)
	if err != nil {
		rollback.Cleanup(ctx)
		fmt.Println("SpiceDB WRITE ERR", err)
		// report spicedb write errors as conflicts
		return KubeConflict(err, input), nil
	}

	out, err := workflow.ExecuteActivity[*KubeResp](ctx,
		workflow.DefaultActivityOptions,
		activityHandler.WriteToKube,
		input.toKubeReqInput()).Get(ctx)
	if err != nil {
		// if there's an error, might need to roll back the spicedb write

		// check if object exists - the activity may have failed, but the write to Kube could have succeeded
		exists, err := workflow.ExecuteActivity[bool](ctx,
			workflow.DefaultActivityOptions,
			activityHandler.CheckKubeResource,
			input.toKubeReqInput()).Get(ctx)
		if err != nil {
			return nil, err
		}

		// if the object doesn't exist, clean up the spicedb write
		if !exists {
			rollback.Cleanup(ctx)
			return nil, err
		}
	}

	return out, nil
}

// ResourceLockRel generates a relationship representing a worfklow's lock over a
// specific resource in kube.
func ResourceLockRel(input *CreateObjInput, workflowID string) *v1.Relationship {
	lockKey := input.RequestInfo.Path + "/" + input.ObjectMeta.GetName() + "/" + input.RequestInfo.Verb
	lockHash := fmt.Sprintf("%x", xxhash.Sum64String(lockKey))
	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: lockResourceType,
			ObjectId:   lockHash,
		},
		Relation: lockRelationName,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: workflowResourceType,
				ObjectId:   workflowID,
			},
		},
	}
}

// SpiceDBNamespaceRel returns a tuple to write when creating a namespace.
// This is hardcoded for namespaces for now; should be configurable and
// come from the workflow input
func SpiceDBNamespaceRel(input *CreateObjInput) *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: "namespace",
			ObjectId:   input.ObjectMeta.Name,
		},
		Relation: "creator",
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: "user",
				ObjectId:   input.UserInfo.GetName(),
			},
		},
	}
}

// SpiceDBClusterRel returns a tuple to write when creating a namespace.
// This is hardcoded for namespaces for now; should be configurable and
// come from the workflow input
func SpiceDBClusterRel(input *CreateObjInput) *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: "namespace",
			ObjectId:   input.ObjectMeta.Name,
		},
		Relation: "cluster",
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: "cluster",
				ObjectId:   "cluster", // only one cluster
			},
		},
	}
}

// KubeConflict wraps an error and turns it into a standard kube conflict
// response.
func KubeConflict(err error, input *CreateObjInput) *KubeResp {
	var out KubeResp
	statusError := k8serrors.NewConflict(schema.GroupResource{
		Group:    input.RequestInfo.APIGroup,
		Resource: input.RequestInfo.Resource,
	}, input.ObjectMeta.Name, err)
	out.StatusCode = http.StatusConflict
	out.Err = *statusError
	out.Body, _ = json.Marshal(statusError)
	return &out
}

func resourceLockDoesNotExist(lockRel *v1.Relationship) *v1.Precondition {
	return &v1.Precondition{
		Operation: v1.Precondition_OPERATION_MUST_NOT_MATCH,
		Filter: &v1.RelationshipFilter{
			ResourceType:          lockResourceType,
			OptionalResourceId:    lockRel.Resource.ObjectId,
			OptionalRelation:      lockRelationName,
			OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: workflowResourceType},
		},
	}
}

func clusterLockDoesNotExist(clusterRelationship *v1.Relationship) *v1.Precondition {
	return &v1.Precondition{
		Operation: v1.Precondition_OPERATION_MUST_NOT_MATCH,
		Filter: &v1.RelationshipFilter{
			ResourceType:       clusterRelationship.Resource.ObjectType,
			OptionalResourceId: clusterRelationship.Resource.ObjectId,
			OptionalRelation:   clusterRelationship.Relation,
			OptionalSubjectFilter: &v1.SubjectFilter{
				SubjectType:       clusterRelationship.Subject.Object.ObjectType,
				OptionalSubjectId: clusterRelationship.Subject.Object.ObjectType,
			},
		},
	}
}

func WorkflowForLockMode(lockMode string) (any, error) {
	if lockMode == "" {
		return nil, fmt.Errorf("lock mode is undefined")
	}

	if !(lockMode == StrategyPessimisticWriteToSpiceDBAndKube || lockMode == StrategyOptimisticWriteToSpiceDBAndKube) {
		return nil, fmt.Errorf("unexpected lock mode: %s", lockMode)
	}

	f := PessimisticWriteToSpiceDBAndKube
	if lockMode == StrategyOptimisticWriteToSpiceDBAndKube {
		f = OptimisticWriteToSpiceDBAndKube
	}

	return f, nil
}