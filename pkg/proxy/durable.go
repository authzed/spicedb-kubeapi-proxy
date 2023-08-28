package proxy

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/microsoft/durabletask-go/task"
	"github.com/pingcap/failpoint"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const WriteToSpiceDBAndKubeName = "WriteToSpiceDBAndKube"

type CreateObjInput struct {
	RequestInfo *request.RequestInfo
	UserInfo    *user.DefaultInfo
	ObjectMeta  *metav1.ObjectMeta
	Body        []byte
}

type KubeReqInput struct {
	RequestInfo *request.RequestInfo
	ObjectMeta  *metav1.ObjectMeta
	Body        []byte
}

type KubeResp struct {
	Body        []byte
	ContentType string
	StatusCode  int
}

// WriteToSpiceDBAndKube ensures that a write exists in both SpiceDB and kube,
// or neither.
func (s *Server) WriteToSpiceDBAndKube(ctx *task.OrchestrationContext) (any, error) {
	var input CreateObjInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}

	var resp v1.WriteRelationshipsResponse
	if err := ctx.CallActivity(s.WriteToSpiceDB, task.WithActivityInput(&v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{{
			Operation: v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: &v1.Relationship{
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
			},
		}},
	})).Await(&resp); err != nil {
		return nil, err
	}

	var out KubeResp
	if err := ctx.CallActivity(s.WriteToKube, task.WithActivityInput(&KubeReqInput{
		RequestInfo: input.RequestInfo,
		ObjectMeta:  input.ObjectMeta,
		Body:        input.Body,
	})).Await(&out); err != nil {
		// if there's an error, might need to rollback the spicedb write

		// check if object exists - we might have failed the write task but
		// succeeded in writing to kube
		var exists bool
		if err := ctx.CallActivity(s.CheckKube, task.WithActivityInput(&KubeReqInput{
			RequestInfo: input.RequestInfo,
			ObjectMeta:  input.ObjectMeta,
			Body:        input.Body,
		})).Await(&exists); err != nil {
			return nil, err
		}

		// if the object doesn't exist, clean up the spicedb write
		if !exists {
			if err := ctx.CallActivity(s.WriteToSpiceDB, task.WithActivityInput(&v1.WriteRelationshipsRequest{
				Updates: []*v1.RelationshipUpdate{{
					Operation: v1.RelationshipUpdate_OPERATION_DELETE,
					Relationship: &v1.Relationship{
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
					},
				}},
			})).Await(&resp); err != nil {
				return nil, err
			}
			return nil, err
		}
	}
	return out, nil
}

func (s *Server) WriteToSpiceDB(ctx task.ActivityContext) (any, error) {
	var req v1.WriteRelationshipsRequest
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	return s.spiceDBClient.WriteRelationships(ctx.Context(), &req)
}

func (s *Server) WriteToKube(ctx task.ActivityContext) (any, error) {
	restConfig, err := clientcmd.NewDefaultClientConfig(*s.opts.BackendConfig, nil).ClientConfig()
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}

	var req KubeReqInput
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}

	failpoint.Inject("panicKubeWrite", func() {
		panic("failed to write to kube")
	})

	kreq := client.RESTClient().Post().
		RequestURI(req.RequestInfo.Path).
		Body(req.Body)
	if len(req.RequestInfo.Namespace) > 0 {
		kreq = kreq.Namespace(req.RequestInfo.Namespace)
	}
	res := kreq.Do(ctx.Context())

	failpoint.Inject("panicKubeReadResp", func() {
		panic("write succeeded, but crashed before the status could be read")
	})

	body, err := res.Raw()
	if err != nil {
		return nil, err
	}
	resp := KubeResp{
		Body: body,
	}
	res.StatusCode(&resp.StatusCode)
	res.ContentType(&resp.ContentType)
	return resp, nil
}

func (s *Server) CheckKube(ctx task.ActivityContext) (any, error) {
	restConfig, err := clientcmd.NewDefaultClientConfig(*s.opts.BackendConfig, nil).ClientConfig()
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}

	var req KubeReqInput
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}

	// TODO: this is somewhat janky
	res := client.RESTClient().Get().RequestURI(req.RequestInfo.Path + "/" + req.ObjectMeta.GetName()).Do(ctx.Context())
	return !k8serrors.IsNotFound(res.Error()), nil
}
