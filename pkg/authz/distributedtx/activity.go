package distributedtx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/rest"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/failpoints"
)

type KubeReqInput struct {
	RequestURI  string
	RequestInfo *request.RequestInfo
	Header      http.Header
	ObjectMeta  *metav1.ObjectMeta
	Body        []byte
}

type KubeResp struct {
	Body        []byte
	ContentType string
	StatusCode  int
	Err         k8serrors.StatusError
}

type ActivityHandler struct {
	PermissionClient v1.PermissionsServiceClient
	KubeClient       rest.Interface
}

// WriteToSpiceDB writes relationships to spicedb and returns any errors.
func (h *ActivityHandler) WriteToSpiceDB(ctx context.Context, input *v1.WriteRelationshipsRequest) (*v1.WriteRelationshipsResponse, error) {
	failpoints.FailPoint("panicWriteSpiceDB")
	out, err := h.PermissionClient.WriteRelationships(ctx, input)
	failpoints.FailPoint("panicSpiceDBReadResp")
	return out, err
}

// ReadRelationships reads relationships from spicedb and returns any errors.
func (h *ActivityHandler) ReadRelationships(ctx context.Context, input *v1.ReadRelationshipsRequest) ([]*v1.ReadRelationshipsResponse, error) {
	failpoints.FailPoint("panicReadSpiceDB")
	resp, err := h.PermissionClient.ReadRelationships(ctx, input)
	failpoints.FailPoint("panicSpiceDBReadResp")
	if err != nil {
		return nil, err
	}

	results := make([]*v1.ReadRelationshipsResponse, 0)
	for {
		resp, err := resp.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return results, nil // end of stream
			}
			return results, err
		}

		results = append(results, resp)
	}
}

// WriteToKube performs a Kube API Server POST, specified in a KubeReqInput
func (h *ActivityHandler) WriteToKube(ctx context.Context, req *KubeReqInput) (*KubeResp, error) {
	failpoints.FailPoint("panicKubeWrite")

	var verb string
	switch req.RequestInfo.Verb {
	case "put":
		verb = http.MethodPut

	case "patch":
		verb = http.MethodPatch

	case "post":
		verb = http.MethodPost

	case "update":
		verb = http.MethodPut

	case "delete":
		verb = http.MethodDelete

	case "create":
		verb = http.MethodPost

	default:
		return nil, fmt.Errorf("unsupported kube verb: %s", req.RequestInfo.Verb)
	}

	if req.RequestURI == "" {
		return nil, fmt.Errorf("request URI must be specified for kube write")
	}

	kreq := h.KubeClient.Verb(verb).RequestURI(req.RequestURI).Body(req.Body)
	for h, v := range req.Header {
		kreq.SetHeader(h, v...)
	}

	res := kreq.Do(ctx)

	failpoints.FailPoint("panicKubeReadResp")

	resp := KubeResp{}
	body, err := res.Raw()
	var nonKerr error
	kerr := &k8serrors.StatusError{}
	if errors.As(err, &kerr) {
		resp.Err = *kerr
		resp.StatusCode = int(kerr.Status().Code)
	} else {
		nonKerr = err
		res.StatusCode(&resp.StatusCode)
	}

	resp.Body = body
	res.ContentType(&resp.ContentType)

	return &resp, nonKerr
}

func (h *ActivityHandler) CheckKubeResource(ctx context.Context, req *KubeReqInput) (bool, error) {
	// TODO: this is somewhat janky - may not work for all request types
	uri := req.RequestInfo.Path + "/" + req.ObjectMeta.GetName()
	res := h.KubeClient.Get().RequestURI(uri).Do(ctx)
	err := res.Error()
	if err == nil {
		return true, nil // resource found
	}

	if k8serrors.IsNotFound(err) {
		return false, nil // resource does not exist
	}

	return false, err // some other kube or network error; we can't tell
}

// just used to be able to reference methods for activity invocations
var activityHandler ActivityHandler
