package proxy

import (
	"bufio"
	"bytes"
	"context"
	ejson "encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/endpoints/request"
)

func withAuthorization(handler, failed http.Handler, spicedbClient v1.PermissionsServiceClient, watchClient v1.WatchServiceClient, taskHubClient backend.TaskHubClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		requestInfo, ok := request.RequestInfoFrom(req.Context())
		if !ok {
			failed.ServeHTTP(w, req)
			return
		}
		userInfo, ok := request.UserFrom(req.Context())
		if !ok {
			failed.ServeHTTP(w, req)
			return
		}

		if (requestInfo.Path == "/api" || requestInfo.Path == "/apis") && requestInfo.Verb == "get" {
			req = req.WithContext(WithAuthzData(req.Context(), &AuthzData{}))
			handler.ServeHTTP(w, req)
			return
		}

		// CREATE namespace
		if requestInfo.Resource == "namespaces" &&
			requestInfo.Verb == "create" &&
			requestInfo.APIVersion == "v1" &&
			requestInfo.APIGroup == "" {

			body, err := io.ReadAll(req.Body)
			if err != nil {
				failed.ServeHTTP(w, req)
				return
			}

			pom := metav1.PartialObjectMetadata{}
			decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(body), 100)
			if err := decoder.Decode(&pom); err != nil {
				failed.ServeHTTP(w, req)
				return
			}

			id, err := taskHubClient.ScheduleNewOrchestration(ctx, WriteToSpiceDBAndKubeName, api.WithInput(CreateObjInput{
				RequestInfo: requestInfo,
				UserInfo:    userInfo.(*user.DefaultInfo),
				ObjectMeta:  &pom.ObjectMeta,
				Body:        body,
			}))
			if err != nil {
				fmt.Println(err)
				failed.ServeHTTP(w, req)
				return
			}
			metadata, err := taskHubClient.WaitForOrchestrationCompletion(ctx, id)
			if err != nil {
				fmt.Println(err)
				failed.ServeHTTP(w, req)
				return
			}
			// TODO: needed?
			if metadata.FailureDetails != nil {
				fmt.Println(metadata.FailureDetails.GetErrorMessage())
				failed.ServeHTTP(w, req)
				return
			}

			req.Body = io.NopCloser(bytes.NewBuffer(body))

			fmt.Println("pom", pom)

			allowed := make(chan string)
			req = req.WithContext(WithAuthzData(req.Context(), &AuthzData{
				allowedNameC: allowed,
				allowedNames: map[string]struct{}{pom.ObjectMeta.Name: {}},
			}))
			close(allowed)

			var resp KubeResp
			if err := json.Unmarshal([]byte(metadata.SerializedOutput), &resp); err != nil {
				fmt.Println(err)
				failed.ServeHTTP(w, req)
				return
			}
			// this can happen if there are un-recoverable failures in the
			// durable fn execution
			if resp.Body == nil {
				failed.ServeHTTP(w, req)
				return
			}

			w.Header().Set("Content-Type", resp.ContentType)
			w.WriteHeader(resp.StatusCode)
			if _, err := w.Write(resp.Body); err != nil {
				fmt.Println(err)
				failed.ServeHTTP(w, req)
				return
			}
			return
		}

		authzData := AuthzData{
			allowedNameC: make(chan string),
			allowedNames: map[string]struct{}{},
		}

		// GET namespace
		if requestInfo.Resource == "namespaces" &&
			requestInfo.Verb == "get" &&
			requestInfo.APIVersion == "v1" &&
			requestInfo.APIGroup == "" &&
			requestInfo.Name != "" {
			go func() {
				cr, err := spicedbClient.CheckPermission(ctx, &v1.CheckPermissionRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true},
					},
					Resource: &v1.ObjectReference{
						ObjectType: "namespace",
						ObjectId:   requestInfo.Name,
					},
					Permission: "view",
					Subject: &v1.SubjectReference{
						Object: &v1.ObjectReference{
							ObjectType: "user",
							ObjectId:   userInfo.GetName(),
						},
					},
				})
				if err != nil {
					fmt.Println(err)
					failed.ServeHTTP(w, req)
					return
				}
				fmt.Println(cr.Permissionship)
				if cr.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
					authzData.allowedNameC <- requestInfo.Name
				}
				close(authzData.allowedNameC)
			}()
			go func() {
				authzData.Lock()
				defer authzData.Unlock()
				for n := range authzData.allowedNameC {
					authzData.allowedNames[n] = struct{}{}
				}
			}()
		}

		// LIST namespace
		if requestInfo.Resource == "namespaces" &&
			requestInfo.Verb == "list" &&
			requestInfo.APIVersion == "v1" &&
			requestInfo.APIGroup == "" {

			go func() {
				lr, err := spicedbClient.LookupResources(ctx, &v1.LookupResourcesRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true},
					},
					ResourceObjectType: "namespace",
					Permission:         "view",
					Subject: &v1.SubjectReference{
						Object: &v1.ObjectReference{
							ObjectType: "user",
							ObjectId:   userInfo.GetName(),
						},
					},
				})
				if err != nil {
					fmt.Println(err)
					failed.ServeHTTP(w, req)
					return
				}
				for {
					resp, err := lr.Recv()
					if errors.Is(err, io.EOF) {
						break
					}

					if err != nil {
						fmt.Println(err)
						failed.ServeHTTP(w, req)
						return
					}
					authzData.allowedNameC <- resp.ResourceObjectId
					fmt.Println("allowed", resp.ResourceObjectId)
				}
				close(authzData.allowedNameC)
			}()
			go func() {
				authzData.Lock()
				defer authzData.Unlock()
				for n := range authzData.allowedNameC {
					authzData.allowedNames[n] = struct{}{}
				}
			}()
		}

		// WATCH namespaces
		if requestInfo.Resource == "namespaces" &&
			requestInfo.Verb == "watch" &&
			requestInfo.APIVersion == "v1" &&
			requestInfo.APIGroup == "" {

			go func() {
				watchNs, err := watchClient.Watch(ctx, &v1.WatchRequest{
					OptionalObjectTypes: []string{"namespace"},
				})
				if err != nil {
					fmt.Println(err)
					failed.ServeHTTP(w, req)
					return
				}
				for {
					resp, err := watchNs.Recv()
					if errors.Is(err, io.EOF) {
						break
					}

					if err != nil {
						fmt.Println(err)
						failed.ServeHTTP(w, req)
						return
					}

					for _, u := range resp.Updates {
						if u.Operation == v1.RelationshipUpdate_OPERATION_TOUCH || u.Operation == v1.RelationshipUpdate_OPERATION_CREATE {
							// do a check
							cr, err := spicedbClient.CheckPermission(ctx, &v1.CheckPermissionRequest{
								Consistency: &v1.Consistency{
									Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
									// TODO
									// Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true},
								},
								Resource: &v1.ObjectReference{
									ObjectType: "namespace",
									ObjectId:   u.Relationship.Resource.ObjectId,
								},
								Permission: "view",
								Subject: &v1.SubjectReference{
									Object: &v1.ObjectReference{
										ObjectType: "user",
										ObjectId:   userInfo.GetName(),
									},
								},
							})
							if err != nil {
								fmt.Println(err)
								failed.ServeHTTP(w, req)
								return
							}
							fmt.Println(u.Relationship.Resource.ObjectId, cr.Permissionship)
							if cr.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
								fmt.Println("sending allowed name", u.Relationship.Resource.ObjectId)
								authzData.allowedNameC <- u.Relationship.Resource.ObjectId
							}
						}
					}
				}
				close(authzData.allowedNameC)
			}()
		}

		req = req.WithContext(WithAuthzData(req.Context(), &authzData))

		handler.ServeHTTP(w, req)
	})
}

type requestAuthzData int

const requestAuthzDataKey requestAuthzData = iota

// WithAuthzData returns a copy of parent in which the authzdata value is set
func WithAuthzData(parent context.Context, info *AuthzData) context.Context {
	return context.WithValue(parent, requestAuthzDataKey, info)
}

// AuthzDataFrom returns the value of the authzdata key on the ctx
func AuthzDataFrom(ctx context.Context) (*AuthzData, bool) {
	info, ok := ctx.Value(requestAuthzDataKey).(*AuthzData)
	return info, ok
}

type AuthzData struct {
	sync.RWMutex
	allowedNameC       chan string
	allowedNames       map[string]struct{}
	allowedNamespaces  map[string]struct{}
	allowAllNamespaces bool

	buffered [][]byte
}

type listOrObjectMeta struct {
	ResourceVersion            string                      `json:"resourceVersion,omitempty" protobuf:"bytes,2,opt,name=resourceVersion"`
	Continue                   string                      `json:"continue,omitempty" protobuf:"bytes,3,opt,name=continue"`
	RemainingItemCount         *int64                      `json:"remainingItemCount,omitempty" protobuf:"bytes,4,opt,name=remainingItemCount"`
	Name                       string                      `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`
	GenerateName               string                      `json:"generateName,omitempty" protobuf:"bytes,2,opt,name=generateName"`
	Namespace                  string                      `json:"namespace,omitempty" protobuf:"bytes,3,opt,name=namespace"`
	SelfLink                   string                      `json:"selfLink,omitempty" protobuf:"bytes,4,opt,name=selfLink"`
	UID                        types.UID                   `json:"uid,omitempty" protobuf:"bytes,5,opt,name=uid,casttype=k8s.io/kubernetes/pkg/types.UID"`
	Generation                 int64                       `json:"generation,omitempty" protobuf:"varint,7,opt,name=generation"`
	CreationTimestamp          metav1.Time                 `json:"creationTimestamp,omitempty" protobuf:"bytes,8,opt,name=creationTimestamp"`
	DeletionTimestamp          *metav1.Time                `json:"deletionTimestamp,omitempty" protobuf:"bytes,9,opt,name=deletionTimestamp"`
	DeletionGracePeriodSeconds *int64                      `json:"deletionGracePeriodSeconds,omitempty" protobuf:"varint,10,opt,name=deletionGracePeriodSeconds"`
	Labels                     map[string]string           `json:"labels,omitempty" protobuf:"bytes,11,rep,name=labels"`
	Annotations                map[string]string           `json:"annotations,omitempty" protobuf:"bytes,12,rep,name=annotations"`
	OwnerReferences            []metav1.OwnerReference     `json:"ownerReferences,omitempty" patchStrategy:"merge" patchMergeKey:"uid" protobuf:"bytes,13,rep,name=ownerReferences"`
	Finalizers                 []string                    `json:"finalizers,omitempty" patchStrategy:"merge" protobuf:"bytes,14,rep,name=finalizers"`
	ManagedFields              []metav1.ManagedFieldsEntry `json:"managedFields,omitempty" protobuf:"bytes,17,rep,name=managedFields"`
}

func (m *listOrObjectMeta) ToListMeta() metav1.ListMeta {
	return metav1.ListMeta{
		ResourceVersion:    m.ResourceVersion,
		Continue:           m.Continue,
		RemainingItemCount: m.RemainingItemCount,
	}
}

func (m *listOrObjectMeta) ToObjectMeta() metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:                       m.Name,
		GenerateName:               m.GenerateName,
		Namespace:                  m.Namespace,
		UID:                        m.UID,
		ResourceVersion:            m.ResourceVersion,
		Generation:                 m.Generation,
		CreationTimestamp:          m.CreationTimestamp,
		DeletionTimestamp:          m.DeletionTimestamp,
		DeletionGracePeriodSeconds: m.DeletionGracePeriodSeconds,
		Labels:                     m.Labels,
		Annotations:                m.Annotations,
		OwnerReferences:            m.OwnerReferences,
		Finalizers:                 m.Finalizers,
		ManagedFields:              m.ManagedFields,
	}
}

type partialObjectOrList struct {
	metav1.TypeMeta  `json:",inline"`
	listOrObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items            []metav1.PartialObjectMetadata `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (p *partialObjectOrList) IsList() bool {
	return p.Items != nil
}

func (p *partialObjectOrList) ToPartialObjectMetadata() *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{
		TypeMeta:   p.TypeMeta,
		ObjectMeta: p.ToObjectMeta(),
	}
}

func (p *partialObjectOrList) ToPartialObjectMetadataList() *metav1.PartialObjectMetadataList {
	return &metav1.PartialObjectMetadataList{
		TypeMeta: p.TypeMeta,
		ListMeta: p.ToListMeta(),
		Items:    p.Items,
	}
}

func (d *AuthzData) FilterResp(resp *http.Response) error {
	info, ok := request.RequestInfoFrom(resp.Request.Context())
	if !ok {
		return fmt.Errorf("no info")
	}

	if (info.Path == "/api" || info.Path == "/apis") && info.Verb == "get" {
		return nil
	}

	if info.Verb == "watch" {
		return d.FilterWatch(resp)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println(string(body))

	pom := partialObjectOrList{}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(body), 100).Decode(&pom); err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Printf("obj pom %#v\n", pom)

	var filtered []byte
	switch {
	case pom.GroupVersionKind().GroupKind() == schema.GroupKind{Group: "meta.k8s.io", Kind: "Table"}:
		filtered, err = d.FilterTable(body)
	case pom.IsList():
		filtered, err = d.FilterList(body)
	default:
		// filter single object
		filtered, err = d.FilterObject(pom.ToPartialObjectMetadata(), body)
	}

	resp.Body = io.NopCloser(bytes.NewBuffer(filtered))
	resp.Header["Content-Length"] = []string{fmt.Sprint(len(filtered))}
	if len(filtered) == 0 {
		resp.StatusCode = http.StatusNotFound
	}
	return nil
}

func (d *AuthzData) FilterWatch(resp *http.Response) error {
	originalRespBody := resp.Body
	var newRespBody *io.PipeWriter
	resp.Body, newRespBody = io.Pipe()

	go func() {
		fmt.Println("running watcher")

		events := make(chan []byte)
		go func() {
			fmt.Println("watching for chunks")
			for {
				if resp.Request.Context().Err() != nil {
					fmt.Println("context canceled, stopping chunk watch")
					break
				}
				// TODO: isprefix?
				chunk, _, err := bufio.NewReader(originalRespBody).ReadLine()
				fmt.Println(string(chunk))
				if err != nil {
					fmt.Println(err)
					break
				}
				fmt.Println("sending chunk")
				events <- chunk
				fmt.Println()
			}
		}()

		type Event struct {
			Type   string
			Object ejson.RawMessage
		}

		writeChunk := func(chunk []byte) error {
			fmt.Println("writing chunk to resp body")
			_, err := newRespBody.Write(chunk)
			if err != nil {
				fmt.Println(err)
				return err
			}
			return nil
		}

		bufferedEvents := make(map[string][]byte, 0)
		for {
			select {
			case chunk := <-events:
				event := Event{}
				if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(chunk), 100).Decode(&event); err != nil {
					fmt.Println(err)
				}
				if event.Type == string(watch.Added) || event.Type == string(watch.Modified) {
					pom := metav1.PartialObjectMetadata{}

					if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(event.Object), 100).Decode(&pom); err != nil {
						fmt.Println(err)
						break
					}

					fmt.Println("got watch event", pom)

					// TODO: non-table
					switch pom.GroupVersionKind().GroupKind() {
					case schema.GroupKind{Group: "meta.k8s.io", Kind: "Table"}:
						table := metav1.Table{}
						if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(event.Object), 100).Decode(&table); err != nil {
							fmt.Println(err)
							break
						}

						for _, r := range table.Rows {
							pom := metav1.PartialObjectMetadata{}
							decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(r.Object.Raw), 100)
							if err := decoder.Decode(&pom); err != nil {
								fmt.Println(err)
								break
							}
							d.RLock()
							_, ok := d.allowedNames[pom.ObjectMeta.Name]
							d.RUnlock()

							fmt.Println("found in allowed:", pom.ObjectMeta.Name, ok)
							if ok {
								if err := writeChunk(chunk); err != nil {
									break
								}
							} else {
								bufferedEvents[pom.Name] = chunk
							}
						}
					}
				}
			case name := <-d.allowedNameC:
				fmt.Println("recv allowed name", name)
				fmt.Println("bufferedEvents on name recv", bufferedEvents)

				d.Lock()
				d.allowedNames[name] = struct{}{}
				d.Unlock()

				if chunk, ok := bufferedEvents[name]; ok {
					err := writeChunk(chunk)
					if err != nil {
						break
					} else {
						delete(bufferedEvents, name)
					}
				}
			case <-resp.Request.Context().Done():
				fmt.Println("request canceled")
				return
			}
		}
	}()

	return nil
}

func (d *AuthzData) FilterTable(body []byte) ([]byte, error) {
	table := metav1.Table{}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(body), 100).Decode(&table); err != nil {
		fmt.Println(err)
		return nil, err
	}

	// wait for all allowednames to be synced
	d.RLock()
	defer d.RUnlock()

	allowedRows := make([]metav1.TableRow, 0)
	for _, r := range table.Rows {
		pom := metav1.PartialObjectMetadata{}
		decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(r.Object.Raw), 100)
		if err := decoder.Decode(&pom); err != nil {
			fmt.Println(err)
			return nil, err
		}
		if _, ok := d.allowedNames[pom.ObjectMeta.Name]; ok {
			allowedRows = append(allowedRows, r)
		}
	}

	table.Rows = allowedRows

	return json.Marshal(table)
}

func (d *AuthzData) FilterList(body []byte) ([]byte, error) {
	list := metav1.List{}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(body), 100).Decode(&list); err != nil {
		fmt.Println(err)
		return nil, err
	}

	// wait for all allowednames to be synced
	d.RLock()
	defer d.RUnlock()

	allowedItems := make([]runtime.RawExtension, 0)
	for _, item := range list.Items {
		pom := metav1.PartialObjectMetadata{}
		decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(item.Raw), 100)
		if err := decoder.Decode(&pom); err != nil {
			fmt.Println(err)
			return nil, err
		}
		if _, ok := d.allowedNames[pom.ObjectMeta.Name]; ok {
			allowedItems = append(allowedItems, item)
		}
	}

	list.Items = allowedItems

	return json.Marshal(list)

}

func (d *AuthzData) FilterObject(pom *metav1.PartialObjectMetadata, body []byte) ([]byte, error) {
	// wait for all allowednames to be synced
	<-d.allowedNameC
	d.RLock()
	defer d.RUnlock()

	if _, ok := d.allowedNames[pom.Name]; ok {
		return body, nil
	}
	return nil, fmt.Errorf("unauthorized")
}
