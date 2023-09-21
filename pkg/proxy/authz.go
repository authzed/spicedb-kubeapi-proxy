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
	"slices"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy/distributedtx"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/cschleiden/go-workflows/client"
	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
)

func WithAuthorization(handler, failed http.Handler, permissionsClient v1.PermissionsServiceClient, watchClient v1.WatchServiceClient, workflowClient client.Client, matcher *rules.Matcher) (http.Handler, error) {
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

		input := &rules.ResolveInput{
			Request: requestInfo,
			User:    userInfo.(*user.DefaultInfo),
		}

		// create/update requests should contain an object body, parse it and
		// include in the input
		var body []byte
		if slices.Contains([]string{"create", "update", "patch"}, requestInfo.Verb) {
			pom := metav1.PartialObjectMetadata{}
			var err error
			body, err = io.ReadAll(req.Body)
			if err != nil {
				failed.ServeHTTP(w, req)
				return
			}
			decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(body), 100)
			if err := decoder.Decode(&pom); err != nil {
				failed.ServeHTTP(w, req)
				return
			}
			input.Object = &pom
		}

		matchingRules := (*matcher).Match(requestInfo)

		if err := check(ctx, matchingRules, input, permissionsClient); err != nil {
			failed.ServeHTTP(w, req)
			return
		}

		// do a write
		// TODO/NOTE: this assumes all `writes` are dual writes; i.e. we only
		//  write into spicedb when there is a corresponding write to kube.
		//  There may be use cases for writes to spicedb on kube reads, which
		//  would require a different path here.
		for _, r := range matchingRules {
			if len(r.Writes) == 0 {
				break
			}
			writeRels := make([]*v1.Relationship, 0, len(r.Writes))
			for _, c := range r.Writes {
				rel, err := rules.ResolveRel(c, input)
				if err != nil {
					failed.ServeHTTP(w, req)
					return
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

			preconditionFromRel := func(rel *rules.ResolvedRel) *v1.Precondition {
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
			for _, c := range r.Must {
				rel, err := rules.ResolveRel(c, input)
				if err != nil {
					failed.ServeHTTP(w, req)
					return
				}
				p := preconditionFromRel(rel)
				p.Operation = v1.Precondition_OPERATION_MUST_MATCH
				preconditions = append(preconditions, p)
			}
			for _, c := range r.MustNot {
				rel, err := rules.ResolveRel(c, input)
				if err != nil {
					failed.ServeHTTP(w, req)
					return
				}
				p := preconditionFromRel(rel)
				p.Operation = v1.Precondition_OPERATION_MUST_NOT_MATCH
				preconditions = append(preconditions, p)
			}

			dualWrite := func() (*distributedtx.KubeResp, error) {
				workflow, err := distributedtx.WorkflowForLockMode(string(r.LockMode))
				if err != nil {
					return nil, err
				}
				id, err := workflowClient.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
					InstanceID: uuid.NewString(),
				}, workflow, &distributedtx.WriteObjInput{
					RequestInfo:   requestInfo,
					UserInfo:      userInfo.(*user.DefaultInfo),
					ObjectMeta:    &input.Object.ObjectMeta,
					Rels:          writeRels,
					Preconditions: preconditions,
					Body:          body,
				})
				if err != nil {
					return nil, err
				}
				resp, err := client.GetWorkflowResult[distributedtx.KubeResp](ctx, workflowClient, id, distributedtx.DefaultWorkflowTimeout)
				if err != nil {
					return nil, err
				}
				return &resp, nil
			}
			resp, err := dualWrite()
			if err != nil {
				failed.ServeHTTP(w, req)
				return
			}

			// this can happen if there are un-recoverable failures in the
			// workflow execution
			if resp.Body == nil {
				failed.ServeHTTP(w, req)
				return
			}

			// we can only do one dual-write per request without some way of
			// marking some writes async.
			w.Header().Set("Content-Type", resp.ContentType)
			w.WriteHeader(resp.StatusCode)
			if _, err := w.Write(resp.Body); err != nil {
				fmt.Println(err)
				failed.ServeHTTP(w, req)
				return
			}

			// TODO: are there any use-cases for running the filter step
			//  after a write?
			return
		}

		authzData := &AuthzData{
			allowedNNC: make(chan types.NamespacedName),
			allowedNN:  map[types.NamespacedName]struct{}{},
		}

		if err := filter(ctx, matchingRules, input, authzData, permissionsClient, watchClient); err != nil {
			failed.ServeHTTP(w, req)
			return
		}

		req = req.WithContext(WithAuthzData(req.Context(), authzData))

		handler.ServeHTTP(w, req)
	}), nil
}

func check(ctx context.Context, matchingRules []*rules.RunnableRule, input *rules.ResolveInput, client v1.PermissionsServiceClient) error {
	var checkGroup errgroup.Group

	// issue checks for all matching rules
	for _, r := range matchingRules {
		for _, c := range r.Checks {
			checkGroup.Go(func() error {
				rel, err := rules.ResolveRel(c, input)
				if err != nil {
					return err
				}
				resp, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
					// TODO: other consistency options require a way to input revisions;
					//  perhaps as an annotation on the kube object?
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true},
					},
					Resource: &v1.ObjectReference{
						ObjectType: rel.ResourceType,
						ObjectId:   rel.ResourceID,
					},
					Permission: rel.ResourceRelation,
					Subject: &v1.SubjectReference{
						Object: &v1.ObjectReference{
							ObjectType: rel.SubjectType,
							ObjectId:   rel.SubjectID,
						},
						OptionalRelation: rel.SubjectRelation,
					},
				})
				if err != nil {
					return err
				}
				if resp.Permissionship != v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
					return fmt.Errorf("failed check for %v", rel)
				}
				return nil
			})
		}
	}
	return checkGroup.Wait()
}

func filter(ctx context.Context, matchingRules []*rules.RunnableRule, input *rules.ResolveInput, authzData *AuthzData, client v1.PermissionsServiceClient, watchClient v1.WatchServiceClient) error {
	// filter responses by fetching a list of allowed objects in parallel with
	// the request
	for _, r := range matchingRules {
		for _, f := range r.Filter {
			rel, err := rules.ResolveRel(f, input)
			if err != nil {
				return err
			}

			switch input.Request.Verb {
			case "get":
				filterGet(ctx, client, rel, input, authzData)
			case "list":
				filterList(ctx, client, rel, input, authzData)
			case "watch":
				filterWatch(ctx, client, watchClient, rel, input, authzData)
			}
		}
	}
	return nil
}

func filterGet(ctx context.Context, client v1.PermissionsServiceClient, rel *rules.ResolvedRel, input *rules.ResolveInput, authzData *AuthzData) {
	go func() {
		cr, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true},
			},
			Resource: &v1.ObjectReference{
				ObjectType: rel.ResourceType,
				ObjectId:   rel.ResourceID,
			},
			Permission: rel.ResourceRelation,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: rel.SubjectType,
					ObjectId:   rel.SubjectID,
				},
				OptionalRelation: rel.SubjectRelation,
			},
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(cr.Permissionship)
		// TODO: this will mark an object that matches any filter as allowed, should
		//   probably change to check all filters.
		if cr.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
			// namespace requests have name and namespace set to the namespace,
			// this normalizes it to match other cluster-scoped objects
			namespace := input.Request.Namespace
			if input.Request.Resource == "namespaces" {
				namespace = ""
			}
			authzData.allowedNNC <- types.NamespacedName{Name: input.Request.Name, Namespace: namespace}
		}
		close(authzData.allowedNNC)
	}()
	go func() {
		authzData.Lock()
		defer authzData.Unlock()
		for n := range authzData.allowedNNC {
			authzData.allowedNN[n] = struct{}{}
		}
	}()
}

func filterList(ctx context.Context, client v1.PermissionsServiceClient, rel *rules.ResolvedRel, input *rules.ResolveInput, authzData *AuthzData) {
	// TODO: filter should handle LS as well
	go func() {
		lr, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true},
			},
			ResourceObjectType: rel.ResourceType,
			Permission:         rel.ResourceRelation,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: rel.SubjectType,
					ObjectId:   rel.SubjectID,
				},
				OptionalRelation: rel.SubjectRelation,
			},
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		for {
			resp, err := lr.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				fmt.Println(err)
				return
			}
			// TODO: this will mark an object that matches any filter as allowed, should
			//   probably change to check all filters.

			// namespace requests have name and namespace set to the namespace,
			// this normalizes it to match other cluster-scoped objects
			namespace := input.Request.Namespace
			if input.Request.Resource == "namespaces" {
				namespace = ""
			}
			authzData.allowedNNC <- types.NamespacedName{Name: resp.ResourceObjectId, Namespace: namespace}
			fmt.Println("allowed", resp.ResourceObjectId)
		}
		close(authzData.allowedNNC)
	}()
	go func() {
		authzData.Lock()
		defer authzData.Unlock()
		for n := range authzData.allowedNNC {
			authzData.allowedNN[n] = struct{}{}
		}
	}()
}

func filterWatch(ctx context.Context, client v1.PermissionsServiceClient, watchClient v1.WatchServiceClient, rel *rules.ResolvedRel, input *rules.ResolveInput, authzData *AuthzData) {
	go func() {
		watchNs, err := watchClient.Watch(ctx, &v1.WatchRequest{
			OptionalObjectTypes: []string{rel.ResourceType},
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		for {
			resp, err := watchNs.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				fmt.Println(err)
				return
			}

			for _, u := range resp.Updates {
				if u.Operation == v1.RelationshipUpdate_OPERATION_TOUCH || u.Operation == v1.RelationshipUpdate_OPERATION_CREATE {
					// do a check
					cr, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
						Consistency: &v1.Consistency{
							Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
							// TODO
							// Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true},
						},
						Resource: &v1.ObjectReference{
							ObjectType: rel.ResourceType,
							// TODO: how should this be specified in the filter spec?
							ObjectId: u.Relationship.Resource.ObjectId,
						},
						Permission: rel.ResourceRelation,
						Subject: &v1.SubjectReference{
							Object: &v1.ObjectReference{
								ObjectType: rel.SubjectType,
								ObjectId:   rel.SubjectID,
							},
							OptionalRelation: rel.SubjectRelation,
						},
					})
					if err != nil {
						fmt.Println(err)
						return
					}
					// TODO: this will mark an object that matches any filter as allowed, should
					//   probably change to check all filters.

					// namespace requests have name and namespace set to the namespace,
					// this normalizes it to match other cluster-scoped objects
					namespace := input.Request.Namespace
					if input.Request.Resource == "namespaces" {
						namespace = ""
					}

					fmt.Println(u.Relationship.Resource.ObjectId, cr.Permissionship)
					if cr.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
						fmt.Println("sending allowed name", u.Relationship.Resource.ObjectId)
						authzData.allowedNNC <- types.NamespacedName{Name: u.Relationship.Resource.ObjectId, Namespace: namespace}
					}
				}
			}
		}
		close(authzData.allowedNNC)
	}()
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
	allowedNNC chan types.NamespacedName
	allowedNN  map[types.NamespacedName]struct{}
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

	// FIXME we are not returning the err here, and if added, no e2e test passes because "FilterObject"
	// returns "unauthorized" error
	if err != nil {
		fmt.Println(err)
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

		bufferedEvents := make(map[types.NamespacedName][]byte, 0)
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
							_, ok := d.allowedNN[types.NamespacedName{Name: pom.ObjectMeta.Name, Namespace: pom.ObjectMeta.Namespace}]
							d.RUnlock()

							fmt.Println("found in allowed:", pom.ObjectMeta.Name, ok)
							if ok {
								if err := writeChunk(chunk); err != nil {
									break
								}
							} else {
								bufferedEvents[types.NamespacedName{Name: pom.ObjectMeta.Name, Namespace: pom.ObjectMeta.Namespace}] = chunk
							}
						}
					}
				}
			case nn := <-d.allowedNNC:
				fmt.Println("recv allowed", nn)
				fmt.Println("bufferedEvents on nn recv", bufferedEvents)

				d.Lock()
				d.allowedNN[nn] = struct{}{}
				d.Unlock()

				if chunk, ok := bufferedEvents[nn]; ok {
					err := writeChunk(chunk)
					if err != nil {
						break
					} else {
						delete(bufferedEvents, nn)
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
		if _, ok := d.allowedNN[types.NamespacedName{Name: pom.ObjectMeta.Name, Namespace: pom.ObjectMeta.Namespace}]; ok {
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
		if _, ok := d.allowedNN[types.NamespacedName{Name: pom.ObjectMeta.Name, Namespace: pom.ObjectMeta.Namespace}]; ok {
			allowedItems = append(allowedItems, item)
		}
	}

	list.Items = allowedItems

	return json.Marshal(list)
}

func (d *AuthzData) FilterObject(pom *metav1.PartialObjectMetadata, body []byte) ([]byte, error) {
	// wait for all allowednames to be synced
	<-d.allowedNNC
	d.RLock()
	defer d.RUnlock()

	if _, ok := d.allowedNN[types.NamespacedName{Name: pom.ObjectMeta.Name, Namespace: pom.ObjectMeta.Namespace}]; ok {
		return body, nil
	}
	return nil, fmt.Errorf("unauthorized")
}
