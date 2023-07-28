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

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/endpoints/request"
)

func withAuthorization(handler, failed http.Handler, spicedbClient v1.PermissionsServiceClient, watchClient v1.WatchServiceClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		requestInfo, ok := request.RequestInfoFrom(req.Context())
		if !ok {
			failed.ServeHTTP(w, req)
			return
		}
		fmt.Println(requestInfo)
		user, ok := request.UserFrom(req.Context())
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

			req.Body = io.NopCloser(bytes.NewBuffer(body))

			fmt.Println("pom", pom)
			resp, err := spicedbClient.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
				Updates: []*v1.RelationshipUpdate{{
					Operation: v1.RelationshipUpdate_OPERATION_TOUCH,
					Relationship: &v1.Relationship{
						Resource: &v1.ObjectReference{
							ObjectType: "namespace",
							ObjectId:   pom.ObjectMeta.Name,
						},
						Relation: "creator",
						Subject: &v1.SubjectReference{
							Object: &v1.ObjectReference{
								ObjectType: "user",
								ObjectId:   user.GetName(),
							},
						},
					},
				}},
			})
			if err != nil {
				fmt.Println(err)
				failed.ServeHTTP(w, req)
				return
			}
			fmt.Println(resp)

			allowed := make(chan string)
			req = req.WithContext(WithAuthzData(req.Context(), &AuthzData{
				allowedNameC: allowed,
				allowedNames: map[string]struct{}{pom.ObjectMeta.Name: {}},
			}))
			close(allowed)

			handler.ServeHTTP(w, req)
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
							ObjectId:   user.GetName(),
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
							ObjectId:   user.GetName(),
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
										ObjectId:   user.GetName(),
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

	pom := metav1.PartialObjectMetadata{}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(body), 100).Decode(&pom); err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Printf("obj pom %#v\n", pom)

	var filtered []byte
	switch pom.GroupVersionKind().GroupKind() {
	case schema.GroupKind{Group: "meta.k8s.io", Kind: "Table"}:
		filtered, err = d.FilterTable(body)
	// TODO: metav1.List
	default:
		// filter single object
		filtered, err = d.FilterObject(&pom, body)
	}

	resp.Body = io.NopCloser(bytes.NewBuffer(filtered))
	resp.Header["Content-Length"] = []string{fmt.Sprint(len(filtered))}
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
