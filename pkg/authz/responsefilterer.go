package authz

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	kjson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/ctxkey"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

// TODO: do we need to do extra work to load upstream types into the scheme?
var codecs = serializer.NewCodecFactory(scheme.Scheme)

var responseFiltererKey = ctxkey.New[*ResponseFilterer]()

const prefilterTimeout = 10 * time.Second

// WithResponseFilterer returns a copy of parent in which the responseFilterer value is set
func WithResponseFilterer(parent context.Context, info *ResponseFilterer) context.Context {
	return context.WithValue(parent, responseFiltererKey, info)
}

// ResponseFiltererFrom returns the value of the responseFilterer key on the ctx
func ResponseFiltererFrom(ctx context.Context) (*ResponseFilterer, bool) {
	return responseFiltererKey.Value(ctx)
}

// NewResponseFilterer creates a new ResponseFilterer with the given parameters.
// It is used to filter the response based on the rules and authz data.
func NewResponseFilterer(restMapper meta.RESTMapper, input *rules.ResolveInput, filteredRules []*rules.RunnableRule, client v1.PermissionsServiceClient) (*ResponseFilterer, error) {
	return &ResponseFilterer{
		restMapper: restMapper,
		input:      input,

		filteredRules: filteredRules,
		client:        client,

		prefilteredStarted: false,
		preFilterCompleted: make(chan prefilterResult, 1),
	}, nil
}

// NewEmptyResponseFilterer creates a new ResponseFilterer that does not run any pre-filters.
func NewEmptyResponseFilterer(restMapper meta.RESTMapper, input *rules.ResolveInput) *ResponseFilterer {
	rf := &ResponseFilterer{
		restMapper: restMapper,
		input:      input,

		filteredRules: nil,
		client:        nil,

		prefilteredStarted: true,
		preFilterCompleted: make(chan prefilterResult, 1),
	}
	rf.preFilterCompleted <- prefilterResult{allAllowed: true} // signal that pre-filters are completed, as this is an empty call
	return rf
}

// ResponseFilterer is used to filter the response based on the rules and authz data.
type ResponseFilterer struct {
	restMapper meta.RESTMapper
	input      *rules.ResolveInput

	filteredRules []*rules.RunnableRule
	client        v1.PermissionsServiceClient

	prefilteredStarted bool
	preFilterCompleted chan prefilterResult
}

// RunPreFilters runs the pre-filters for the request, if any. If not invoked before the response is filtered,
// an error will be returned by the FilterResp method.
func (rf *ResponseFilterer) RunPreFilters(req *http.Request) error {
	if rf.prefilteredStarted {
		return fmt.Errorf("pre-filters already started, cannot run again")
	}

	rf.prefilteredStarted = true

	prefilterRules := preFilterRules(rf.filteredRules)
	if len(prefilterRules) == 0 {
		// No pre-filters to run, just signal completion
		rf.preFilterCompleted <- prefilterResult{allAllowed: true}
		return nil
	}

	if len(prefilterRules) > 1 {
		return fmt.Errorf("multiple pre-filters found, only one is allowed per request")
	}

	if len(prefilterRules[0].PreFilter) == 0 {
		return fmt.Errorf("pre-filter rule has no filters defined")
	}

	if len(prefilterRules[0].PreFilter) > 1 {
		return fmt.Errorf("pre-filter rule has multiple filters defined, only one is allowed")
	}

	f := prefilterRules[0].PreFilter[0]
	rel, err := rules.ResolveRel(f.Rel, rf.input)
	if err != nil {
		return err
	}

	filter := &rules.ResolvedPreFilter{
		LookupType:            f.LookupType,
		Rel:                   rel,
		NameFromObjectID:      f.NameFromObjectID,
		NamespaceFromObjectID: f.NamespaceFromObjectID,
	}

	// Run LookupResources for the prefilter rule and write the results to the channel.
	go func() {
		klog.FromContext(req.Context()).V(3).Info("running pre-filter", "request", req, "filter", filter)

		result, err := runLookupResources(req.Context(), rf.client, filter, rf.input)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				klog.FromContext(req.Context()).V(3).Info("context cancelation when running pre-filter", "request", req)
			} else {
				klog.FromContext(req.Context()).Error(err, "error running pre-filter", "request", req)
			}

			rf.preFilterCompleted <- prefilterResult{
				err: err,
			}
			return
		}
		rf.preFilterCompleted <- *result
	}()
	return nil
}

// FilterResp filters the response based on the rules and authz data
// It reads the response body, decodes it, applies the filters, and writes the filtered
// response back to the original response object.
func (rf *ResponseFilterer) FilterResp(resp *http.Response) error {
	if !rf.prefilteredStarted {
		// If pre-filters were not started, we cannot filter the response.
		return fmt.Errorf("pre-filters were not started, cannot filter response")
	}

	// Wait for pre-filter to complete, if any.
	select {
	case <-resp.Request.Context().Done():
		return resp.Request.Context().Err()

	case <-time.After(prefilterTimeout):
		return fmt.Errorf("pre-filter timed out")

	case result := <-rf.preFilterCompleted:
		if result.err != nil {
			return fmt.Errorf("pre-filter error: %w", result.err)
		}

		// Filter the response based on the found prefiltered results, if any.
		info, ok := request.RequestInfoFrom(resp.Request.Context())
		if !ok {
			return fmt.Errorf("no info")
		}

		if alwaysAllow(info) {
			return nil
		}

		gvk, err := rf.restMapper.KindFor(schema.GroupVersionResource{
			Group:    info.APIGroup,
			Version:  info.APIVersion,
			Resource: info.Resource,
		})
		if err != nil {
			return fmt.Errorf("failed to get GVK for %s: %w", info.Resource, err)
		}
		recognized := scheme.Scheme.Recognizes(gvk)

		if info.Verb == "watch" {
			return fmt.Errorf("watch requests are not currently supported")
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
			filtered, err := rf.filterTable(body, result)
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
			filterErr = rf.filterList(typedList, result)
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

			filterErr = rf.filterObject(typedObj, result)
			if filterErr != nil {
				break
			}
			filteredBody = *bytes.NewBuffer(body)
		}

		return writeResp(filteredBody, filterErr, resp)
	}
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

func (rf *ResponseFilterer) filterTable(body []byte, result prefilterResult) ([]byte, error) {
	// NOTE: as of kube 1.33, tables are always json encoded.
	// this may change in the future.
	table := metav1.Table{}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(body), 100).Decode(&table); err != nil {
		klog.V(3).ErrorS(err, "error decoding table")
		return nil, err
	}

	allowedRows := make([]metav1.TableRow, 0)
	for _, r := range table.Rows {
		pom := metav1.PartialObjectMetadata{}
		decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(r.Object.Raw), 100)
		if err := decoder.Decode(&pom); err != nil {
			klog.V(3).ErrorS(err, "error decoding partial object metadata from table row")
			return nil, err
		}
		if result.IsAllowed(pom.Namespace, pom.Name) {
			allowedRows = append(allowedRows, r)
		}
	}

	table.Rows = allowedRows

	return kjson.Marshal(table)
}

func (rf *ResponseFilterer) filterList(originalObj runtime.Object, result prefilterResult) error {
	allowedItems := make([]runtime.Object, 0)

	if err := meta.EachListItem(originalObj, func(item runtime.Object) error {
		objMeta, err := meta.Accessor(item)
		if err != nil {
			return fmt.Errorf("failed to get object metadata: %w", err)
		}

		nn := types.NamespacedName{Name: objMeta.GetName(), Namespace: objMeta.GetNamespace()}
		if result.IsAllowed(nn.Namespace, nn.Name) {
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

// filterObject checks if the object is in the allowedNN map and returns an error if not.
func (rf *ResponseFilterer) filterObject(obj runtime.Object, result prefilterResult) error {
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return fmt.Errorf("failed to get object metadata: %w", err)
	}
	if result.IsAllowed(objMeta.GetNamespace(), objMeta.GetName()) {
		klog.V(3).InfoS("allowed resource get", "resource", types.NamespacedName{Name: objMeta.GetName(), Namespace: objMeta.GetNamespace()}.String())
		return nil
	}

	klog.V(3).InfoS("denied resource get", "resource", types.NamespacedName{Name: objMeta.GetName(), Namespace: objMeta.GetNamespace()}.String())
	return fmt.Errorf("unauthorized")
}
