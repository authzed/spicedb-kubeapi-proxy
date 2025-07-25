package authz

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/types"
	kjson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
)

// TODO: do we need to do extra work to load upstream types into the scheme?
var codecs = serializer.NewCodecFactory(scheme.Scheme)

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
	restMapper    meta.RESTMapper
	allowedNNC    chan types.NamespacedName
	removedNNC    chan types.NamespacedName
	allowedNN     map[types.NamespacedName]struct{} // GUARDED_BY(RWMutex)
	skipPreFilter bool
}

func (d *AuthzData) FilterResp(resp *http.Response) error {
	info, ok := request.RequestInfoFrom(resp.Request.Context())
	if !ok {
		return fmt.Errorf("no info")
	}

	if alwaysAllow(info) {
		return nil
	}

	gvk, err := d.restMapper.KindFor(schema.GroupVersionResource{
		Group:    info.APIGroup,
		Version:  info.APIVersion,
		Resource: info.Resource,
	})
	if err != nil {
		return fmt.Errorf("failed to get GVK for %s: %w", info.Resource, err)
	}
	recognized := scheme.Scheme.Recognizes(gvk)

	if info.Verb == "watch" {
		return d.FilterWatch(resp, recognized)
	}

	switch {
	case resp.StatusCode >= 400 && resp.StatusCode <= 499:
		return nil
	case resp.StatusCode >= 500 && resp.StatusCode <= 599:
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Use proper Kubernetes content negotiation for stream decoding
	contentType := resp.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return fmt.Errorf("failed to parse content type %s: %w", contentType, err)
	}

	var filteredBody bytes.Buffer
	var filterErr error

	// detect if it's a table request
	// TODO: see if there's a helper for this
	if strings.Contains(resp.Request.Header.Get("Accept"), "as=Table") {
		// TODO: match the other new filter apis, pass object
		filtered, err := d.FilterTable(body)
		filteredBody.Write(filtered)
		return writeResp(filteredBody, err, resp)
	}

	// Create a proper client negotiator for decoding
	negotiator := runtime.NewClientNegotiator(codecs, schema.GroupVersion{
		Group:   gvk.Group,
		Version: gvk.Version,
	})
	decoder, err := negotiator.Decoder(mediaType, params)
	if err != nil {
		return fmt.Errorf("failed to get decoder for %s: %w", mediaType, err)
	}
	encoder, err := negotiator.Encoder(contentType, params)
	if err != nil {
		return fmt.Errorf("failed to get encoder for %s: %w", contentType, err)
	}

	// If the object is proto encoded but not in the scheme, we can't decode it.
	// Built-in types can be proto-encoded, custom types won't be, but perhaps
	// this will change in the future - seeing this error on a new version of
	// kube will require investigation to see what has changed with encoding.
	if !recognized && strings.Contains(mediaType, "proto") {
		return fmt.Errorf("unsupported media type %s for gvk %s", mediaType, gvk.String())
	}

	switch len(info.Parts) {
	// if there's 1 part in the url (i.e. "pods"), we assume it's a list
	case 1:
		if !strings.HasSuffix(gvk.Kind, "List") {
			gvk.Kind = gvk.Kind + "List"
		}

		var typedList runtime.Object

		if recognized {
			var err error
			typedList, err = scheme.Scheme.New(gvk)
			if err != nil {
				return fmt.Errorf("failed to create new object of type %s: %w", gvk, err)
			}
		} else {
			// custom types
			typedList = &unstructured.UnstructuredList{}
		}
		_, _, err = decoder.Decode(body, &gvk, typedList)
		if err != nil {
			return fmt.Errorf("failed to decode response body: %w", err)
		}
		filterErr = d.FilterList(typedList)
		if filterErr != nil {
			break
		}
		if recognized {
			filterErr = encoder.Encode(typedList, &filteredBody)
		} else {
			// custom types
			filterErr = unstructured.UnstructuredJSONScheme.Encode(typedList, &filteredBody)
		}
		if filterErr != nil {
			break
		}

	// if there's 2 or parts in the url (i.e. "pods/foo", "pods/foo/status"), we assume it's a single object
	default:
		var typedObj runtime.Object

		if recognized {
			var err error
			typedObj, err = scheme.Scheme.New(gvk)
			if err != nil {
				return fmt.Errorf("failed to create new object of type %s: %w", gvk, err)
			}
		} else {
			// custom types
			typedObj = &unstructured.Unstructured{}
		}
		_, _, err = decoder.Decode(body, &gvk, typedObj)
		if err != nil {
			return fmt.Errorf("failed to decode response body: %w", err)
		}

		filterErr = d.FilterObject(typedObj)
		if filterErr != nil {
			break
		}
		filteredBody = *bytes.NewBuffer(body)
	}

	return writeResp(filteredBody, filterErr, resp)
}

func writeResp(filteredBody bytes.Buffer, filterErr error, resp *http.Response) error {
	// if there was an error, replace the body with an error message
	if filterErr != nil {
		filteredBody.Reset()
		var merr error
		errBody, merr := kjson.Marshal(k8serrors.NewUnauthorized(filterErr.Error()))
		if merr != nil {
			return merr
		}
		filteredBody.Write(errBody)
		resp.StatusCode = http.StatusUnauthorized
	}

	resp.Body = io.NopCloser(&filteredBody)
	resp.Header["Content-Length"] = []string{fmt.Sprint(filteredBody.Len())}
	if filteredBody.Len() == 0 {
		resp.StatusCode = http.StatusNotFound
	}
	return nil
}

type decodedWatchEvent struct {
	watch.Event
	raw []byte
	gvk *schema.GroupVersionKind
}

func (d *AuthzData) FilterWatch(resp *http.Response, recognized bool) error {
	originalRespBody := resp.Body
	var newRespBody *io.PipeWriter
	resp.Body, newRespBody = io.Pipe()

	// Use proper Kubernetes content negotiation for stream decoding
	contentType := resp.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return fmt.Errorf("failed to parse content type %s: %w", contentType, err)
	}

	// Create a proper client negotiator for stream decoding
	negotiator := runtime.NewClientNegotiator(codecs, schema.GroupVersion{})
	_, streamingSerializer, framer, err := negotiator.StreamDecoder(mediaType, params)
	if err != nil {
		return fmt.Errorf("failed to get stream decoder for %s: %w", mediaType, err)
	}
	if streamingSerializer == nil || framer == nil {
		return fmt.Errorf("no streaming serializer or framer found for content type %s", contentType)
	}

	go func() {
		defer func() {
			klog.V(4).InfoS("closing watch body")
			// may have already been closed if there was an error
			_ = newRespBody.Close()
		}()

		klog.V(3).InfoS("running watcher")
		events := make(chan decodedWatchEvent)
		done := make(chan struct{})

		// Create frame capturing reader
		capturingReader := newFrameCapturingReader(originalRespBody)
		defer func() {
			if err := capturingReader.Close(); err != nil {
				klog.V(3).ErrorS(err, "error closing reader")
			}
		}()

		// Monitor context cancellation and close resources
		go func() {
			<-resp.Request.Context().Done()
			klog.V(3).InfoS("context canceled, closing resources")
			if err := capturingReader.Close(); err != nil {
				klog.V(3).ErrorS(err, "error closing reader")
			}
			close(done)
			klog.V(4).InfoS("done closing")
		}()

		eventDecoder := streaming.NewDecoder(framer.NewFrameReader(capturingReader), streamingSerializer)
		defer func() {
			if err := eventDecoder.Close(); err != nil {
				klog.V(3).ErrorS(err, "error closing event decoder")
			}
		}()

		writeChunk := func(chunk []byte) error {
			klog.V(4).InfoS("writing chunk to resp body", "size", len(chunk))
			_, err := newRespBody.Write(chunk)
			if err != nil {
				klog.V(3).ErrorS(err, "error writing chunk to response body")
				return err
			}
			return nil
		}

		go func() {
			defer close(events)
			klog.V(4).InfoS("watching for chunks")
			for {
				select {
				case <-done:
					klog.V(4).InfoS("stopping chunk watch due to cancellation")
					return
				default:
				}

				// Start capturing bytes for this frame
				capturingReader.startCapture()

				var watchEvent metav1.WatchEvent
				obj, gvk, err := eventDecoder.Decode(nil, &watchEvent)

				// Finish capturing and get the raw bytes for this frame
				rawBytes := capturingReader.finishCapture()

				if err != nil {
					klog.V(3).ErrorS(err, "decode error", "captured_bytes", len(rawBytes))
					return
				}

				// Watch can send Status messages instead of errors.
				// These will pass through directly to the client.
				if gvk.Kind == "Status" && gvk.Version == "v1" {
					klog.V(3).InfoS("got status event, passing through")
					if err := writeChunk(rawBytes); err != nil {
						klog.V(3).ErrorS(err, "error writing status watch event")
					}
					return
				}

				if obj != &watchEvent {
					klog.V(3).InfoS("unexpected decode result")
					continue
				}

				var actualObj runtime.Object
				var itemGVK *schema.GroupVersionKind
				if recognized {
					actualObj, itemGVK, err = scheme.Codecs.UniversalDeserializer().Decode(watchEvent.Object.Raw, nil, nil)
				} else {
					// custom types
					actualObj, itemGVK, err = unstructured.UnstructuredJSONScheme.Decode(watchEvent.Object.Raw, nil, nil)
				}
				if err != nil {
					continue
				}
				klog.V(4).InfoS("got watch event", "object", actualObj, "gvk", itemGVK)

				decoded := decodedWatchEvent{
					raw: rawBytes,
					gvk: gvk,
					Event: watch.Event{
						Type:   watch.EventType(watchEvent.Type),
						Object: actualObj,
					},
				}

				select {
				case events <- decoded:
					klog.V(4).InfoS("sent watch event")
				case <-done:
					return
				}
			}
		}()

		bufferedEvents := make(map[types.NamespacedName]decodedWatchEvent)
		for {
			defer func() { klog.V(4).InfoS("watch event writer closed") }()
			select {
			case event, ok := <-events:
				if !ok {
					klog.V(4).InfoS("events channel closed")
					return
				}
				select {
				case <-done:
					klog.V(4).InfoS("stopping event watch due to cancellation")
					return
				default:
				}

				// this is likely an error message or status we just need to
				// pass through
				if event.gvk == nil {
					if err := writeChunk(event.raw); err != nil {
						klog.V(3).ErrorS(err, "error writing chunk for nil gvk event")
					}
					continue
				}

				if event.Type == watch.Added || event.Type == watch.Modified {
					var pom metav1.PartialObjectMetadata

					// Try to get metadata from the decoded object
					if accessor, err := meta.Accessor(event.Object); err == nil {
						pom.Name = accessor.GetName()
						pom.Namespace = accessor.GetNamespace()
					} else {
						klog.V(3).InfoS("could not get object metadata")
						continue
					}
					klog.V(4).InfoS("got watch event", "name", pom.Name, "namespace", pom.Namespace)

					// Handle Table unwrapping if needed
					if event.gvk.Group == "meta.k8s.io" && event.gvk.Kind == "Table" {
						for _, r := range event.Object.(*metav1.Table).Rows {
							var rowpom metav1.PartialObjectMetadata
							if err := json.Unmarshal(r.Object.Raw, &rowpom); err != nil {
								klog.V(3).ErrorS(err, "error unmarshaling row object")
								continue
							}
							pom = rowpom
							break
						}
					}

					d.RLock()
					_, ok := d.allowedNN[types.NamespacedName{Name: pom.Name, Namespace: pom.Namespace}]
					d.RUnlock()

					klog.V(4).InfoS("checked if resource is allowed", "name", pom.Name, "namespace", pom.Namespace, "allowed", ok)
					if ok {
						if err := writeChunk(event.raw); err != nil {
							break
						}
					} else {
						bufferedEvents[types.NamespacedName{Name: pom.Name, Namespace: pom.Namespace}] = event
					}
				}
			case nn := <-d.allowedNNC:
				d.Lock()
				d.allowedNN[nn] = struct{}{}
				d.Unlock()

				if chunk, ok := bufferedEvents[nn]; ok {
					err := writeChunk(chunk.raw)
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

				delete(bufferedEvents, nn)
			case <-done:
				klog.V(4).InfoS("stopping event processing due to cancellation")
				return
			}
		}
	}()

	return nil
}

func (d *AuthzData) FilterTable(body []byte) ([]byte, error) {
	// NOTE: as of kube 1.33, tables are always json encoded.
	// this may change in the future.
	table := metav1.Table{}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(body), 100).Decode(&table); err != nil {
		klog.V(3).ErrorS(err, "error decoding table")
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
			klog.V(3).ErrorS(err, "error decoding partial object metadata from table row")
			return nil, err
		}
		if _, ok := d.allowedNN[types.NamespacedName{Name: pom.Name, Namespace: pom.Namespace}]; ok {
			allowedRows = append(allowedRows, r)
		}
	}

	table.Rows = allowedRows

	return kjson.Marshal(table)
}

func (d *AuthzData) FilterList(originalObj runtime.Object) error {
	// wait for all allowednames to be synced
	d.RLock()
	defer d.RUnlock()

	allowedItems := make([]runtime.Object, 0)

	if err := meta.EachListItem(originalObj, func(item runtime.Object) error {
		objMeta, err := meta.Accessor(item)
		if err != nil {
			return fmt.Errorf("failed to get object metadata: %w", err)
		}

		if d.skipPreFilter {
			// If skipPreFilter is set, we allow all items
			allowedItems = append(allowedItems, item)
			klog.V(3).InfoS("skipping pre-filter, allowing all items", "resource", types.NamespacedName{Name: objMeta.GetName(), Namespace: objMeta.GetNamespace()}.String())
			return nil
		}

		nn := types.NamespacedName{Name: objMeta.GetName(), Namespace: objMeta.GetNamespace()}
		if _, ok := d.allowedNN[nn]; ok {
			// Item is allowed, include it in the filtered list
			allowedItems = append(allowedItems, item)
			klog.V(3).InfoS("allowed resource in list", "resource", nn.String())
			return nil
		} else {
			klog.V(3).InfoS("denied resource in list", "resource", nn.String())
			return nil
		}
	}); err != nil {
		klog.V(3).ErrorS(err, "failed to iterate over original list items")
	}

	return meta.SetList(originalObj, allowedItems)
}

// FilterObject checks if the object is in the allowedNN map and returns an error if not.
func (d *AuthzData) FilterObject(obj runtime.Object) error {
	// wait for all allowednames to be synced
	<-d.allowedNNC
	d.RLock()
	defer d.RUnlock()

	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return fmt.Errorf("failed to get object metadata: %w", err)
	}
	if _, ok := d.allowedNN[types.NamespacedName{Name: objMeta.GetName(), Namespace: objMeta.GetNamespace()}]; ok {
		klog.V(3).InfoS("allowed resource get", "resource", types.NamespacedName{Name: objMeta.GetName(), Namespace: objMeta.GetNamespace()}.String())
		return nil
	}

	klog.V(3).InfoS("denied resource get", "resource", types.NamespacedName{Name: objMeta.GetName(), Namespace: objMeta.GetNamespace()}.String())
	return fmt.Errorf("unauthorized")
}
