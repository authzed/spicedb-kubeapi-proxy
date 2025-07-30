package authz

import (
	"context"
	"fmt"
	"net/http"
	"slices"

	"github.com/cschleiden/go-workflows/client"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

var updateVerbs = []string{"create", "update", "patch", "delete"}

// WithAuthorization wraps the provided handler with authorization logic.
func WithAuthorization(handler, failed http.Handler, restMapper meta.RESTMapper, permissionsClient v1.PermissionsServiceClient, watchClient v1.WatchServiceClient, workflowClient *client.Client, matcher *rules.Matcher, inputExtractor rules.ResolveInputExtractor) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Extract the request info from the context.
		input, err := inputExtractor.ExtractFromHttp(req)
		if err != nil {
			handleError(w, failed, req, err)
			return
		}

		// Some non-resource requests (such as API checks) are always allowed.
		if alwaysAllow(input.Request) {
			filterer := NewEmptyResponseFilterer(restMapper, input)
			req = req.WithContext(WithResponseFilterer(req.Context(), filterer))
			handler.ServeHTTP(w, req)
			return
		}

		// Otherwise, we need to match rule(s) against the request.
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

		klog.FromContext(ctx).V(2).Info("matched rules", "rules", lo.Map(matchingRules, ruleToString))

		// Apply CEL condition filtering
		filteredRules, err := rules.FilterRulesWithCELConditions(matchingRules, input)
		if err != nil {
			klog.FromContext(ctx).V(2).Error(err, "error evaluating CEL conditions", "input", input)
			handleError(w, failed, req, err)
			return
		}

		klog.FromContext(ctx).V(2).Info("filtered rules", "rules", lo.Map(filteredRules, ruleToString))
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
		inputKeyValues := input.ToKeyValues()
		klog.FromContext(ctx).V(4).Info("authorization input details", inputKeyValues...)

		// Run all checks for this request
		if err := runAllMatchingChecks(ctx, filteredRules, input, permissionsClient); err != nil {
			klog.FromContext(ctx).V(2).Info("input failed authorization checks", inputKeyValues...)
			handleError(w, failed, req, err)
			return
		}
		klog.FromContext(ctx).V(3).Info("input passed all authorization checks", inputKeyValues...)

		// If this request has an update rule, we need to perform the update of the relationships and the
		// write to Kubernetes via the workflow engine.
		updateRule, err := singleUpdateRule(filteredRules)
		if err != nil {
			klog.FromContext(ctx).V(2).Error(err, "unable to get single update rule", inputKeyValues...)
			handleError(w, failed, req, err)
			return
		}

		if updateRule != nil {
			// Ensure this an update operation.
			if !slices.Contains(updateVerbs, input.Request.Verb) {
				err := fmt.Errorf("update rule found but request verb is not create, update, or patch: %s", input.Request.Verb)
				klog.FromContext(ctx).V(2).Error(err, "invalid request verb for update rule", inputKeyValues...)
				handleError(w, failed, req, err)
				return
			}

			klog.FromContext(ctx).V(4).Info("single update rule", "rule", updateRule)
			if err := performUpdate(ctx, w, updateRule, input, req.RequestURI, workflowClient); err != nil {
				klog.FromContext(ctx).V(2).Error(err, "failed to perform update", inputKeyValues...)
				handleError(w, failed, req, err)
			}
			return
		}

		klog.FromContext(ctx).V(4).Info("no update rule found for request; performing directly")

		// If this is a watch request, we need to handle it differently, as it is a long-running operation.
		if input.Request.Verb == "watch" {
			foundWatchRule, err := singlePreFilterRule(filteredRules)
			if err != nil {
				klog.FromContext(ctx).V(2).Error(err, "error getting single pre-filter rule", inputKeyValues...)
				handleError(w, failed, req, err)
				return
			}

			if foundWatchRule == nil {
				klog.FromContext(ctx).V(2).Info("no watch rule found for request", inputKeyValues...)
				handleError(w, failed, req, fmt.Errorf("no watch rule found for request"))
				return
			}

			responseFilterer, err := NewResponseFiltererForWatch(restMapper, input, foundWatchRule, watchClient, permissionsClient)
			if err != nil {
				klog.FromContext(ctx).V(2).Error(err, "failed to create response filterer", inputKeyValues...)
				handleError(w, failed, req, err)
				return
			}

			req = req.WithContext(WithResponseFilterer(req.Context(), responseFilterer))

			// Kick off the watch request.
			if err := responseFilterer.RunWatcher(req); err != nil {
				klog.FromContext(ctx).V(2).Error(err, "failed to run watcher", inputKeyValues...)
				handleError(w, failed, req, err)
				return
			}

			handler.ServeHTTP(w, req)
			return
		}

		// All other requests are filtered by matching rules.
		responseFilterer, err := NewResponseFilterer(restMapper, input, filteredRules, permissionsClient)
		if err != nil {
			klog.FromContext(ctx).V(2).Error(err, "failed to create response filterer", inputKeyValues...)
			handleError(w, failed, req, err)
			return
		}

		// Add the response filterer to the request context so that the response can be filtered later, if applicable.
		req = req.WithContext(WithResponseFilterer(req.Context(), responseFilterer))

		// Run the pre-filters, if any.
		if err := responseFilterer.RunPreFilters(req); err != nil {
			klog.FromContext(ctx).V(2).Error(err, "failed to run pre-filters", inputKeyValues...)
			handleError(w, failed, req, err)
			return
		}

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
	})
}

func ruleToString(item *rules.RunnableRule, index int) string {
	return item.Name
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
