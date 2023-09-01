package proxy

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/microsoft/durabletask-go/task"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/failpoints"
)

// WriteToSpiceDB writes relationships to spicedb and returns any errors.
func (s *Server) WriteToSpiceDB(ctx task.ActivityContext) (any, error) {
	var req v1.WriteRelationshipsRequest
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	failpoints.FailPoint("panicWriteSpiceDB")
	out, err := s.PermissionClient().WriteRelationships(ctx.Context(), &req)
	failpoints.FailPoint("panicSpiceDBReadResp")
	return out, err
}

// WriteToKube peforms a Kube API Server POST, specified in a KubeReqInput propagated via the task.ActivityContext arg
func (s *Server) WriteToKube(ctx task.ActivityContext) (any, error) {
	var req KubeReqInput
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}

	failpoints.FailPoint("panicKubeWrite")

	kreq := s.KubeClient.RESTClient().Post().
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

func (s *Server) CheckKube(ctx task.ActivityContext) (any, error) {
	var req KubeReqInput
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}

	// TODO: this is somewhat janky
	res := s.KubeClient.RESTClient().Get().RequestURI(req.RequestInfo.Path + "/" + req.ObjectMeta.GetName()).Do(ctx.Context())
	return !k8serrors.IsNotFound(res.Error()), nil
}
