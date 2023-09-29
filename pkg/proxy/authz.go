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

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/cschleiden/go-workflows/client"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy/distributedtx"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
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

		// create/update requests should contain an object body, parse it and
		// include in the input
		var body []byte
		var object *metav1.PartialObjectMetadata
		if slices.Contains([]string{"create", "update", "patch"}, requestInfo.Verb) {
			var err error
			body, err = io.ReadAll(req.Body)
			if err != nil {
				failed.ServeHTTP(w, req)
				return
			}
			decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(body), 100)
			var pom metav1.PartialObjectMetadata
			if err := decoder.Decode(&pom); err != nil {
				failed.ServeHTTP(w, req)
				return
			}
			object = &pom
		}
		input := rules.NewResolveInput(requestInfo, userInfo.(*user.DefaultInfo), object)

		matchingRules := (*matcher).Match(requestInfo)

		if err := check(ctx, matchingRules, input, permissionsClient); err != nil {
			failed.ServeHTTP(w, req)
			return
		}

		// NOTE: this assumes all `writes` are dual writes; i.e. we only
		//  write into spicedb when there is a corresponding write to kube.
		//  There may be use cases for writes to spicedb on kube reads, which
		//  would require a different path here.
		for _, r := range matchingRules {
			if len(r.Writes) == 0 {
				break
			}
			writeRels := make([]*v1.Relationship, 0, len(r.Writes))
			for _, write := range r.Writes {
				rel, err := rules.ResolveRel(write, input)
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
			for _, precondition := range r.Must {
				rel, err := rules.ResolveRel(precondition, input)
				if err != nil {
					failed.ServeHTTP(w, req)
					return
				}
				p := preconditionFromRel(rel)
				p.Operation = v1.Precondition_OPERATION_MUST_MATCH
				preconditions = append(preconditions, p)
			}
			for _, precondition := range r.MustNot {
				rel, err := rules.ResolveRel(precondition, input)
				if err != nil {
					failed.ServeHTTP(w, req)
					return
				}
				p := preconditionFromRel(rel)
				p.Operation = v1.Precondition_OPERATION_MUST_NOT_MATCH
				preconditions = append(preconditions, p)
			}

			writeInput := &distributedtx.WriteObjInput{
				RequestInfo:   requestInfo,
				UserInfo:      userInfo.(*user.DefaultInfo),
				Header:        req.Header.Clone(),
				Rels:          writeRels,
				Preconditions: preconditions,
				Body:          body,
			}
			if input.Object != nil {
				writeInput.ObjectMeta = &input.Object.ObjectMeta
			}
			dualWrite := func() (*distributedtx.KubeResp, error) {
				workflow, err := distributedtx.WorkflowForLockMode(string(r.LockMode))
				if err != nil {
					return nil, err
				}
				id, err := workflowClient.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
					InstanceID: uuid.NewString(),
				}, workflow, writeInput)
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

			return
		}

		authzData := &AuthzData{
			allowedNNC: make(chan types.NamespacedName),
			removedNNC: make(chan types.NamespacedName),
			allowedNN:  map[types.NamespacedName]struct{}{},
		}

		authorizeGet(input, authzData)

		if err := filterResponse(ctx, matchingRules, input, authzData, permissionsClient, watchClient); err != nil {
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

// filterResponse filters responses by fetching a list of allowed objects in
// parallel with the request
func filterResponse(ctx context.Context, matchingRules []*rules.RunnableRule, input *rules.ResolveInput, authzData *AuthzData, client v1.PermissionsServiceClient, watchClient v1.WatchServiceClient) error {
	for _, r := range matchingRules {
		for _, f := range r.PreFilter {

			rel, err := rules.ResolveRel(f.Rel, input)
			if err != nil {
				return err
			}

			filter := &rules.ResolvedPreFilter{
				LookupType: f.LookupType,
				Rel:        rel,
				Name:       f.Name,
				Namespace:  f.Namespace,
			}

			switch input.Request.Verb {
			case "list":
				filterList(ctx, client, filter, authzData)
			case "watch":
				filterWatch(ctx, client, watchClient, filter, input, authzData)
			}
		}
	}
	return nil
}

func authorizeGet(input *rules.ResolveInput, authzData *AuthzData) {
	if input.Request.Verb != "get" {
		return
	}
	close(authzData.allowedNNC)
	close(authzData.removedNNC)
	authzData.Lock()
	defer authzData.Unlock()
	// gets aren't really filtered, but we add them to the allowed list so that
	// downstream can know it's passed all configured checks.
	authzData.allowedNN[types.NamespacedName{Name: input.Name, Namespace: input.Namespace}] = struct{}{}
}

type wrapper struct {
	ResourceID string `json:"resourceId"`
	SubjectID  string `json:"subjectId"`
}

func filterList(ctx context.Context, client v1.PermissionsServiceClient, filter *rules.ResolvedPreFilter, authzData *AuthzData) {
	go func() {
		authzData.Lock()
		defer authzData.Unlock()
		defer close(authzData.allowedNNC)
		defer close(authzData.removedNNC)

		lr, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true},
			},
			ResourceObjectType: filter.Rel.ResourceType,
			Permission:         filter.Rel.ResourceRelation,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: filter.Rel.SubjectType,
					ObjectId:   filter.Rel.SubjectID,
				},
				OptionalRelation: filter.Rel.SubjectRelation,
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
			// TODO: this will mark an object that matches any filterResponse as allowed, should
			//   probably change to check all filters.

			byteIn, err := json.Marshal(wrapper{ResourceID: resp.ResourceObjectId})
			if err != nil {
				fmt.Println(err)
				return
			}
			var data any
			if err := json.Unmarshal(byteIn, &data); err != nil {
				fmt.Println(err)
				return
			}

			fmt.Println("GOT WATCH FILTER EVENT", string(byteIn))
			name, err := filter.Name.Search(data)
			if err != nil {
				fmt.Println(err)
				return
			}
			if name == nil || len(name.(string)) == 0 {
				return
			}
			namespace, err := filter.Namespace.Search(data)
			if err != nil {
				fmt.Println(err)
				return
			}
			if namespace == nil {
				namespace = ""
			}
			fmt.Println("NAMENS", name, namespace)

			// TODO: check permissionship?
			authzData.allowedNN[types.NamespacedName{
				Name:      name.(string),
				Namespace: namespace.(string),
			}] = struct{}{}
			fmt.Println("allowed", resp.ResourceObjectId)
		}
	}()
}

func filterWatch(ctx context.Context, client v1.PermissionsServiceClient, watchClient v1.WatchServiceClient, filter *rules.ResolvedPreFilter, input *rules.ResolveInput, authzData *AuthzData) {
	go func() {
		defer close(authzData.allowedNNC)
		defer close(authzData.removedNNC)

		watchResource, err := watchClient.Watch(ctx, &v1.WatchRequest{
			OptionalObjectTypes: []string{filter.Rel.ResourceType},
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		for {
			resp, err := watchResource.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				fmt.Println(err)
				return
			}

			for _, u := range resp.Updates {
				cr, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
					},
					Resource: &v1.ObjectReference{
						ObjectType: filter.Rel.ResourceType,
						// TODO: should swap out subject id if in subject mode
						ObjectId: u.Relationship.Resource.ObjectId,
					},
					Permission: filter.Rel.ResourceRelation,
					Subject: &v1.SubjectReference{
						Object: &v1.ObjectReference{
							ObjectType: filter.Rel.SubjectType,
							ObjectId:   filter.Rel.SubjectID,
						},
						OptionalRelation: filter.Rel.SubjectRelation,
					},
				})
				if err != nil {
					fmt.Println(err)
					return
				}
				// TODO: this will mark an object that matches any filterResponse as allowed, should
				//   probably change to check all filters.

				byteIn, err := json.Marshal(wrapper{ResourceID: u.Relationship.Resource.ObjectId, SubjectID: u.Relationship.Subject.Object.ObjectId})
				if err != nil {
					fmt.Println(err)
					return
				}
				var data any
				if err := json.Unmarshal(byteIn, &data); err != nil {
					fmt.Println(err)
					return
				}
				fmt.Println(data)
				fmt.Println("RESPONSE", string(byteIn))

				name, err := filter.Name.Search(data)
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Println("GOT NAME", name)
				if name == nil || len(name.(string)) == 0 {
					return
				}
				namespace, err := filter.Namespace.Search(data)
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Println("GOT NAMESPACE", namespace)
				if namespace == nil {
					namespace = ""
				}
				nn := types.NamespacedName{Name: name.(string), Namespace: namespace.(string)}

				// TODO: this should really be over a single channel to prevent
				//  races on add/remove
				fmt.Println(u.Relationship.Resource.ObjectId, cr.Permissionship)
				if cr.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
					authzData.allowedNNC <- nn
				} else {
					authzData.removedNNC <- nn
				}
			}
		}
	}()
}

// normalizedNamespace returns the namespace for the request. Namespace requests
// have name and namespace set to the namespace, this normalizes it to match
// other cluster-scoped objects
func normalizedNamespace(info *request.RequestInfo) string {
	namespace := info.Namespace
	if info.Resource == "namespaces" {
		namespace = ""
	}
	return namespace
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
	removedNNC chan types.NamespacedName
	allowedNN  map[types.NamespacedName]struct{}
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
		// filterResponse single object
		filtered, err = d.FilterObject(pom.ToPartialObjectMetadata(), body)
	}

	if err != nil {
		var merr error
		filtered, merr = json.Marshal(k8serrors.NewUnauthorized(err.Error()))
		if merr != nil {
			return merr
		}
		resp.StatusCode = http.StatusUnauthorized
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
				chunk, _, err := bufio.NewReader(originalRespBody).ReadLine()
				fmt.Println(string(chunk))
				if err != nil {
					fmt.Println(err)
					break
				}
				fmt.Printf("sending chunk, %s\n", string(chunk))
				events <- chunk
				fmt.Println()
			}
		}()

		type Event struct {
			Type   string           `json:"type"`
			Object ejson.RawMessage `json:"object"`
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

		bufferedEvents := make(map[types.NamespacedName][]byte)
		for {
			select {
			case chunk := <-events:
				event := Event{}
				if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(chunk), 100).Decode(&event); err != nil {
					fmt.Println(err)
					fmt.Println(string(chunk))
				}
				if event.Type == string(watch.Added) || event.Type == string(watch.Modified) {
					pom := metav1.PartialObjectMetadata{}

					if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(event.Object), 100).Decode(&pom); err != nil {
						fmt.Println(err)
						break
					}

					fmt.Println("got watch event", pom)

					// unwrap object if the response is a Table
					gk := pom.GroupVersionKind().GroupKind()
					if gk.Group == "meta.k8s.io" && gk.Kind == "Table" {
						table := metav1.Table{}
						if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(event.Object), 100).Decode(&table); err != nil {
							fmt.Println(err)
							break
						}
						for _, r := range table.Rows {
							rowpom := metav1.PartialObjectMetadata{}
							decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(r.Object.Raw), 100)
							if err := decoder.Decode(&rowpom); err != nil {
								fmt.Println(err)
								break
							}
							pom = rowpom
							break
						}
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
			case nn := <-d.allowedNNC:
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
			case nn := <-d.removedNNC:
				d.Lock()
				delete(d.allowedNN, nn)
				d.Unlock()

				if _, ok := bufferedEvents[nn]; ok {
					delete(bufferedEvents, nn)
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
