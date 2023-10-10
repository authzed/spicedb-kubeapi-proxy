package authz

import (
	"bufio"
	"bytes"
	"context"
	ejson "encoding/json"
	"fmt"
	"io"
	"k8s.io/klog/v2"
	"net/http"
	"sync"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/request"
)

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

	if alwaysAllow(info) {
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
	klog.V(3).InfoS("upstream response object", "name", pom.Name, "namespace", pom.Namespace, "kind", pom.Kind, "item_count", len(pom.Items))
	klog.V(4).InfoS("upstream response object detail", "pom", pom)

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
			klog.V(3).InfoS("allowed resource in list", "kind", pom.TypeMeta.Kind, "resource", pom.ObjectMeta.Namespace+"/"+pom.ObjectMeta.Name)
		} else {
			klog.V(3).InfoS("denied resource in list", "kind", pom.TypeMeta.Kind, "resource", pom.ObjectMeta.Namespace+"/"+pom.ObjectMeta.Name)
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
		klog.V(3).InfoS("allowed resource get", "kind", pom.TypeMeta.Kind, "resource", pom.ObjectMeta.Namespace+"/"+pom.ObjectMeta.Name)
		return body, nil
	}

	klog.V(3).InfoS("denied resource get", "kind", pom.TypeMeta.Kind, "resource", pom.ObjectMeta.Namespace+"/"+pom.ObjectMeta.Name)
	return nil, fmt.Errorf("unauthorized")
}
