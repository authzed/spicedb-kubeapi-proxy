package distributedtx

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/rest"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/failpoints"
)

type ExecutionInput interface {
	GetInput(resultPtr any) error
}

type ExecutionContext interface {
	ExecutionInput
	Context() context.Context
}

type IdentifiableExecutionInput interface {
	ExecutionInput
	ID() string
}

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
	Err         k8serrors.StatusError
}

type Handler struct {
	PermissionClient v1.PermissionsServiceClient
	KubeClient       rest.Interface
}

// WriteToSpiceDB writes relationships to spicedb and returns any errors.
func (h *Handler) WriteToSpiceDB(ctx ExecutionContext) (any, error) {
	var req v1.WriteRelationshipsRequest
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	failpoints.FailPoint("panicWriteSpiceDB")
	out, err := h.PermissionClient.WriteRelationships(ctx.Context(), &req)
	failpoints.FailPoint("panicSpiceDBReadResp")
	return out, err
}

// WriteToKube peforms a Kube API Server POST, specified in a KubeReqInput propagated via the task.ActivityContext arg
func (h *Handler) WriteToKube(ctx ExecutionContext) (any, error) {
	var req KubeReqInput
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}

	failpoints.FailPoint("panicKubeWrite")

	kreq := h.KubeClient.Post().
		RequestURI(req.RequestInfo.Path).
		Body(req.Body)
	if len(req.RequestInfo.Namespace) > 0 {
		kreq = kreq.Namespace(req.RequestInfo.Namespace)
	}
	res := kreq.Do(ctx.Context())

	failpoints.FailPoint("panicKubeReadResp")

	resp := KubeResp{}
	body, err := res.Raw()
	if kerr, ok := err.(*k8serrors.StatusError); ok {
		resp.Err = *kerr
	}
	resp.Body = body
	res.StatusCode(&resp.StatusCode)
	res.ContentType(&resp.ContentType)
	return resp, nil
}

func (h *Handler) CheckKube(ctx ExecutionContext) (any, error) {
	var req KubeReqInput
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}

	// TODO: this is somewhat janky
	res := h.KubeClient.Get().RequestURI(req.RequestInfo.Path + "/" + req.ObjectMeta.GetName()).Do(ctx.Context())
	return !k8serrors.IsNotFound(res.Error()), nil
}
