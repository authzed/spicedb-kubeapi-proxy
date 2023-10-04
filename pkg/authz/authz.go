package authz

import (
	"context"
	"net/http"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/cschleiden/go-workflows/client"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/endpoints/request"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

func WithAuthorization(handler, failed http.Handler, permissionsClient v1.PermissionsServiceClient, watchClient v1.WatchServiceClient, workflowClient *client.Client, matcher *rules.Matcher) (http.Handler, error) {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		input, err := rules.NewResolveInputFromHttp(req)
		if err != nil {
			failed.ServeHTTP(w, req)
			return
		}

		// some non-resource requests are always allowed
		if alwaysAllow(input.Request) {
			req = req.WithContext(WithAuthzData(req.Context(), &AuthzData{}))
			handler.ServeHTTP(w, req)
			return
		}

		matchingRules := (*matcher).Match(input.Request)

		// run all checks for this request
		if err := runAllMatchingChecks(ctx, matchingRules, input, permissionsClient); err != nil {
			failed.ServeHTTP(w, req)
			return
		}

		// if this request is a write, perform the dual write and return
		if rule := getWriteRule(matchingRules); rule != nil {
			if err := write(ctx, w, rule, input, workflowClient); err != nil {
				failed.ServeHTTP(w, req)
				return
			}
			return
		}

		// all other requests are filtered by matching rules
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

		// filters run in parallel with the upstream request and backfill the
		// allowed object list while the kube request is running.

		req = req.WithContext(WithAuthzData(req.Context(), authzData))

		handler.ServeHTTP(w, req)
	}), nil
}

// alwaysAllow allows unfiltered access to api metadata
func alwaysAllow(info *request.RequestInfo) bool {
	return (info.Path == "/api" || info.Path == "/apis" || info.Path == "/openapi/v2") && info.Verb == "get"
}