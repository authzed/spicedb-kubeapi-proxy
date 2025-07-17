package authz

import (
	"context"
	"fmt"
	"net/http"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/klog/v2"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/cschleiden/go-workflows/client"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/endpoints/request"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

func WithAuthorization(handler, failed http.Handler, restMapper meta.RESTMapper, permissionsClient v1.PermissionsServiceClient, watchClient v1.WatchServiceClient, workflowClient *client.Client, matcher *rules.Matcher, inputExtractor rules.ResolveInputExtractor) (http.Handler, error) {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		input, err := inputExtractor.ExtractFromHttp(req)
		if err != nil {
			handleError(w, failed, req, err)
			return
		}

		// some non-resource requests are always allowed
		if alwaysAllow(input.Request) {
			req = req.WithContext(WithAuthzData(req.Context(), &AuthzData{}))
			handler.ServeHTTP(w, req)
			return
		}

		matchingRules := (*matcher).Match(input.Request)
		if len(matchingRules) == 0 {
			klog.FromContext(ctx).V(3).Info(
				"request did not match any authorization rule",
				"verb", input.Request.Verb,
				"APIGroup", input.Request.APIGroup,
				"APIVersion", input.Request.APIVersion,
				"Resource", input.Request.Resource)
			handleError(w, failed, req, fmt.Errorf("request did not match any authorization rule"))
			return
		}

		// Apply CEL condition filtering
		filteredRules, err := rules.FilterRulesWithCELConditions(matchingRules, input)
		if err != nil {
			klog.FromContext(ctx).V(2).Error(err, "error evaluating CEL conditions", "input", input)
			handleError(w, failed, req, err)
			return
		}

		if len(filteredRules) == 0 {
			klog.FromContext(ctx).V(3).Info(
				"request matched authorization rule/s but failed CEL conditions",
				"verb", input.Request.Verb,
				"APIGroup", input.Request.APIGroup,
				"APIVersion", input.Request.APIVersion,
				"Resource", input.Request.Resource)
			handleError(w, failed, req, fmt.Errorf("request matched authorization rule/s but failed CEL conditions"))
			return
		}

		klog.FromContext(ctx).V(3).Info(
			"request matched authorization rule/s and passed CEL conditions",
			"verb", input.Request.Verb,
			"APIGroup", input.Request.APIGroup,
			"APIVersion", input.Request.APIVersion,
			"Resource", input.Request.Resource)
		klog.FromContext(ctx).V(4).Info("authorization input details", "input", input)

		// run all checks for this request
		if err := runAllMatchingChecks(ctx, filteredRules, input, permissionsClient); err != nil {
			klog.FromContext(ctx).V(2).Error(err, "input failed authorization checks", "input", input)
			handleError(w, failed, req, err)
			return
		}
		klog.FromContext(ctx).V(3).Info("input passed all authorization checks", "input", input)

		// if this request is a write, perform the dual write and return
		rule, err := getSingleUpdateRule(filteredRules)
		if err != nil {
			klog.FromContext(ctx).V(2).Error(err, "unable to get single update rule", "input", input)
			handleError(w, failed, req, err)
			return
		}

		if rule != nil {
			klog.FromContext(ctx).V(4).Info("single update rule", "rule", rule)
			if err := performUpdate(ctx, w, rule, input, req.RequestURI, workflowClient); err != nil {
				klog.FromContext(ctx).V(2).Error(err, "failed to perform update", "input", input)
				handleError(w, failed, req, err)
				return
			}
			return
		} else {
			klog.FromContext(ctx).V(4).Info("no update rule found for request")
		}

		// all other requests are filtered by matching rules
		authzData := &AuthzData{
			restMapper: restMapper,
			allowedNNC: make(chan types.NamespacedName),
			removedNNC: make(chan types.NamespacedName),
			allowedNN:  map[types.NamespacedName]struct{}{},
		}
		alreadyAuthorized(input, authzData)
		if err := filterResponse(ctx, filteredRules, input, authzData, permissionsClient, watchClient); err != nil {
			failed.ServeHTTP(w, req)
			return
		}

		// filters run in parallel with the upstream request and backfill the
		// allowed object list while the kube request is running.

		req = req.WithContext(WithAuthzData(req.Context(), authzData))

		// Check if this request needs PostChecks (non-write and non-list operations)
		if shouldRunPostChecks(input.Request.Verb) {
			// Create a wrapper that runs PostChecks after the handler completes
			postCheckHandler := createPostCheckHandler(handler, failed, ctx, filteredRules, input, permissionsClient)
			postCheckHandler.ServeHTTP(w, req)
		} else if shouldRunPostFilters(input.Request.Verb, filteredRules) {
			// Create a wrapper that runs PostFilters for list operations
			postFilterHandler := createPostFilterHandler(handler, failed, ctx, filteredRules, input, permissionsClient)
			postFilterHandler.ServeHTTP(w, req)
		} else {
			handler.ServeHTTP(w, req)
		}
	}), nil
}

func handleError(w http.ResponseWriter, failHandler http.Handler, req *http.Request, err error) {
	failHandler.ServeHTTP(w, req)
}

// alwaysAllow allows unfiltered access to api metadata
func alwaysAllow(info *request.RequestInfo) bool {
	return (info.Path == "/api" || info.Path == "/apis" || info.Path == "/openapi/v2") && info.Verb == "get"
}

// shouldRunPostChecks determines if PostChecks should run for this verb.
// PostChecks only apply to non-write and non-list operations.
func shouldRunPostChecks(verb string) bool {
	switch verb {
	case "get":
		return true
	case "create", "update", "patch", "delete", "list", "watch":
		return false
	default:
		return false
	}
}

// shouldRunPostFilters determines if PostFilters should run for this verb and rules.
// PostFilters only apply to list operations when PostFilters are defined.
func shouldRunPostFilters(verb string, rules []*rules.RunnableRule) bool {
	switch verb {
	case "list":
		// Check if any rule has PostFilters
		for _, r := range rules {
			if len(r.PostFilter) > 0 {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// createPostCheckHandler creates a handler that runs PostChecks after the upstream handler completes
func createPostCheckHandler(handler, failed http.Handler, ctx context.Context, filteredRules []*rules.RunnableRule, input *rules.ResolveInput, permissionsClient v1.PermissionsServiceClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Create a response recorder to capture the response
		recorder := &responseRecorder{}

		// Call the upstream handler and capture the response.
		handler.ServeHTTP(recorder, req)

		// Only run PostChecks if the upstream request succeeded (2xx status)
		if recorder.statusCode >= 200 && recorder.statusCode < 300 {
			// Run PostChecks
			if err := runAllMatchingPostChecks(ctx, filteredRules, input, permissionsClient); err != nil {
				klog.FromContext(ctx).V(2).Error(err, "input failed post-authorization checks", "input", input)
				// Return the original error handler instead of the successful response
				failed.ServeHTTP(w, req)
				return
			}
			klog.FromContext(ctx).V(3).Info("input passed all post-authorization checks", "input", input)

			// Only write the successful response if PostChecks passed
			recorder.emitResponseToWriter(w)
		} else {
			// Write the error response from upstream
			recorder.emitResponseToWriter(w)
		}
	})
}

// createPostFilterHandler creates a handler that runs PostFilters for list operations after the upstream handler completes
func createPostFilterHandler(handler, failed http.Handler, ctx context.Context, filteredRules []*rules.RunnableRule, input *rules.ResolveInput, permissionsClient v1.PermissionsServiceClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Create a response recorder to capture the response
		recorder := &responseRecorder{}

		// Call the upstream handler and capture the response.
		handler.ServeHTTP(recorder, req)

		// Only run PostFilters if the upstream request succeeded (2xx status)
		if recorder.statusCode >= 200 && recorder.statusCode < 300 {
			if input.Request.Verb == "list" {
				// Handle list operations
				if err := filterListResponse(ctx, recorder, filteredRules, input, permissionsClient); err != nil {
					klog.FromContext(ctx).V(2).Error(err, "failed to filter list response", "input", input)
					failed.ServeHTTP(w, req)
					return
				}
			}

			// Write the filtered response
			recorder.emitResponseToWriter(w)
		} else {
			// Write the error response from upstream
			recorder.emitResponseToWriter(w)
		}
	})
}

// responseRecorder captures the response status code and body without writing to the underlying ResponseWriter
type responseRecorder struct {
	statusCode int
	body       []byte
	headers    http.Header
}

// Header implements http.ResponseWriter.
func (r *responseRecorder) Header() http.Header {
	if r.headers == nil {
		r.headers = make(http.Header)
	}
	return r.headers
}

// Write implements http.ResponseWriter.
func (r *responseRecorder) Write(data []byte) (int, error) {
	if r.statusCode == 0 {
		r.statusCode = 200
	}
	r.body = append(r.body, data...)
	return len(data), nil
}

// WriteHeader implements http.ResponseWriter.
func (r *responseRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
}

func (r *responseRecorder) SetBody(body []byte) {
	r.body = body

	r.headers = make(http.Header)
	r.headers.Set("Content-Type", "application/json")
}

// emitResponseToWriter writes the captured response to the provided ResponseWriter
func (r *responseRecorder) emitResponseToWriter(w http.ResponseWriter) {
	// Copy headers to the target ResponseWriter
	if r.headers != nil {
		for key, values := range r.headers {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
	}

	// Write status code (default to 200 if not set)
	statusCode := r.statusCode
	if statusCode == 0 {
		statusCode = 200
	}
	w.WriteHeader(statusCode)

	// Write body
	if len(r.body) > 0 {
		if _, err := w.Write(r.body); err != nil {
			klog.Error(err, "failed to write response body", "status_code", statusCode, "body_length", len(r.body))
		}
	}
}

var _ http.ResponseWriter = &responseRecorder{}
