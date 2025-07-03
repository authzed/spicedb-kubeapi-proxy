package distributedtx

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/spicedb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/workflow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/rest/fake"
)

func TestWorkflow(t *testing.T) {
	for name, workflowFunc := range map[string]func(ctx workflow.Context, input *WriteObjInput) (*KubeResp, error){
		StrategyPessimisticWriteToSpiceDBAndKube: PessimisticWriteToSpiceDBAndKube,
		StrategyOptimisticWriteToSpiceDBAndKube:  OptimisticWriteToSpiceDBAndKube,
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			srv, err := spicedb.NewServer(ctx, "")
			require.NoError(t, err)
			go func() {
				require.NoError(t, srv.Run(ctx))
			}()

			dialCtx, err := srv.GRPCDialContext(ctx)
			require.NoError(t, err)

			psc := v1.NewPermissionsServiceClient(dialCtx)

			kubeClient := &fake.RESTClient{
				Client: fake.CreateHTTPClient(func(request *http.Request) (*http.Response, error) {
					header := http.Header{}
					header.Set("Content-Type", runtime.ContentTypeJSON)
					resp := &http.Response{
						Header:     header,
						StatusCode: http.StatusCreated,
						Body:       io.NopCloser(strings.NewReader(`{"hi":"myfriend"}`)),
					}
					return resp, nil
				}),
				NegotiatedSerializer: &serializer.CodecFactory{},
			}

			require.NoError(t, err)
			workflowClient, worker, err := SetupWithMemoryBackend(ctx, psc, kubeClient)
			require.NoError(t, err)
			require.NoError(t, worker.Start(ctx))
			defer func() {
				require.NoError(t, worker.Shutdown(ctx))
			}()

			id, err := workflowClient.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
				InstanceID: uuid.NewString(),
			}, workflowFunc, &WriteObjInput{
				RequestInfo: &request.RequestInfo{Verb: "create"},
				UserInfo:    &user.DefaultInfo{Name: "janedoe"},
				ObjectMeta:  &metav1.ObjectMeta{Name: "my_object_meta"},
				CreateRelationships: []*v1.Relationship{{
					Resource: &v1.ObjectReference{
						ObjectType: "namespace",
						ObjectId:   "my_object_meta",
					},
					Relation: "creator",
					Subject: &v1.SubjectReference{Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "janedoe",
					}},
				}},
				Body: []byte("{}"),
			})
			require.NoError(t, err)

			resp, err := client.GetWorkflowResult[KubeResp](ctx, workflowClient, id, DefaultWorkflowTimeout)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Empty(t, resp.Err, "workflow returned error: %s", resp.Err)
			require.Equal(t, `{"hi":"myfriend"}`, string(resp.Body))
			require.Equal(t, http.StatusCreated, resp.StatusCode)
			require.Equal(t, runtime.ContentTypeJSON, resp.ContentType)

			cpr, err := psc.CheckPermission(ctx, &v1.CheckPermissionRequest{
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
				Resource: &v1.ObjectReference{
					ObjectType: "namespace",
					ObjectId:   "my_object_meta",
				},
				Permission: "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "janedoe",
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, cpr.Permissionship)
		})
	}

}
