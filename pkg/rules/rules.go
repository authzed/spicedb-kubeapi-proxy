package rules

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"github.com/warpstreamlabs/bento/public/bloblang"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
)

var (
	codecs = serializer.NewCodecFactory(scheme.Scheme)
)

// RequestMeta uniquely identifies the type of request, and is used to find
// matching rules.
type RequestMeta struct {
	Verb       string
	APIGroup   string
	APIVersion string
	Resource   string
}

// A Matcher holds a set of matching rules in memory for fast matching against
// incoming requests.
// Currently there is only a hash map implementation; you could imagine
// more interesting ways of matching requests.
type Matcher interface {
	Match(match *request.RequestInfo) []*RunnableRule
}

// MatcherFunc is a function type that implements Matcher
type MatcherFunc func(match *request.RequestInfo) []*RunnableRule

func (f MatcherFunc) Match(match *request.RequestInfo) []*RunnableRule {
	return f(match)
}

// MapMatcher stores rules in a map keyed on GVR and Verb
type MapMatcher map[RequestMeta][]*RunnableRule

// NewMapMatcher creates a MapMatcher for a set of rules
func NewMapMatcher(configRules []proxyrule.Config) (MapMatcher, error) {
	matchingRules := make(map[RequestMeta][]*RunnableRule, 0)
	for _, r := range configRules {
		for _, m := range r.Matches {
			for _, v := range m.Verbs {
				gv, err := schema.ParseGroupVersion(m.GroupVersion)
				if err != nil {
					return nil, fmt.Errorf("couldn't parse gv %q: %w", m.GroupVersion, err)
				}
				meta := RequestMeta{
					APIGroup:   gv.Group,
					APIVersion: gv.Version,
					Resource:   m.Resource,
					Verb:       v,
				}
				if _, ok := matchingRules[meta]; !ok {
					matchingRules[meta] = make([]*RunnableRule, 0)
				}
				rules, err := Compile(r)
				if err != nil {
					return nil, fmt.Errorf("couldn't compile rule %s: %w", r.String(), err)
				}
				matchingRules[meta] = append(matchingRules[meta], rules)
			}
		}
	}
	return matchingRules, nil
}

func (m MapMatcher) Match(match *request.RequestInfo) []*RunnableRule {
	return m[RequestMeta{
		Verb:       match.Verb,
		APIGroup:   match.APIGroup,
		APIVersion: match.APIVersion,
		Resource:   match.Resource,
	}]
}

// UncompiledRelExpr represents a relationship template expression that hasn't
// been converted to RelExpr yet.
type UncompiledRelExpr struct {
	ResourceType     string
	ResourceID       string
	ResourceRelation string
	SubjectType      string
	SubjectID        string
	SubjectRelation  string
}

// RelExpr represents a relationship with optional Bloblang
// expressions for field values.
type RelExpr struct {
	ResourceType     *bloblang.Executor
	ResourceID       *bloblang.Executor
	ResourceRelation *bloblang.Executor
	SubjectType      *bloblang.Executor
	SubjectID        *bloblang.Executor
	SubjectRelation  *bloblang.Executor
}

// ResolvedRel holds values after all expressions have been evaluated.
// It has the same structure as string templates in UncompiledRelExpr, but
// with resolved values.
type ResolvedRel UncompiledRelExpr

// ResolveInputExtractor defines how ResolveInput are extracted from requests.
// This interface exists so that tests can easily fake the request data.
type ResolveInputExtractor interface {
	ExtractFromHttp(req *http.Request) (*ResolveInput, error)
}

// ResolveInputExtractorFunc is a function type that implements ResolveInputExtractor
type ResolveInputExtractorFunc func(req *http.Request) (*ResolveInput, error)

func (f ResolveInputExtractorFunc) ExtractFromHttp(req *http.Request) (*ResolveInput, error) {
	return f(req)
}

// ResolveInput is the data fed into RelExpr to be evaluated.
type ResolveInput struct {
	Name           string                        `json:"name"`
	Namespace      string                        `json:"namespace"`
	NamespacedName string                        `json:"namespacedName"`
	Request        *request.RequestInfo          `json:"request"`
	User           *user.DefaultInfo             `json:"user"`
	Object         *metav1.PartialObjectMetadata `json:"object"`
	Body           []byte                        `json:"body"`
	Headers        http.Header                   `json:"headers"`
}

func NewResolveInputFromHttp(req *http.Request) (*ResolveInput, error) {
	requestInfo, ok := request.RequestInfoFrom(req.Context())
	if !ok {
		return nil, fmt.Errorf("unable to get request info from request")
	}
	userInfo, ok := request.UserFrom(req.Context())
	if !ok {
		return nil, fmt.Errorf("unable to get user info from request")
	}

	// create/update requests should contain an object body, parse it and
	// include in the input
	var body []byte
	var object *metav1.PartialObjectMetadata
	if slices.Contains([]string{"create", "update", "patch"}, requestInfo.Verb) {
		var err error
		body, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("unable to read request body: %w", err)
		}
		var pom metav1.PartialObjectMetadata
		_, _, err = codecs.UniversalDeserializer().Decode(body, nil, &pom)
		if err != nil {
			return nil, fmt.Errorf("unable to decode request body as kube object: %w", err)
		}

		object = &pom

		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	return NewResolveInput(requestInfo, userInfo.(*user.DefaultInfo), object, body, req.Header.Clone()), nil
}

// NewResolveInput creates a ResolveInput with normalized fields.
func NewResolveInput(req *request.RequestInfo, user *user.DefaultInfo, object *metav1.PartialObjectMetadata, body []byte, headers http.Header) *ResolveInput {
	var name, namespace, namespacedName string

	// default to object
	if object != nil {
		name = object.Name
		namespace = object.Namespace
	}
	// fallback to request
	if len(name) == 0 {
		name = req.Name
	}
	if len(namespace) == 0 {
		namespace = req.Namespace
	}

	// the request on a namespace resource has both name and namespace set to
	// the namespace name, clear out the namespace so it matches other cluster
	// scoped objects.
	if req.Resource == "namespaces" {
		namespace = ""
	}

	if len(namespace) > 0 {
		namespacedName = types.NamespacedName{Name: name, Namespace: namespace}.String()
	} else {
		namespacedName = name
	}
	return &ResolveInput{
		Name:           name,
		Namespace:      namespace,
		NamespacedName: namespacedName,
		Request:        req,
		User:           user,
		Object:         object,
		Body:           body,
		Headers:        headers,
	}
}

func ResolveRel(expr *RelExpr, input *ResolveInput) (*ResolvedRel, error) {
	// Convert input to interface{} for Bloblang
	data, err := convertToBloblangInput(input)
	if err != nil {
		return nil, fmt.Errorf("error converting input to bloblang input: %w", err)
	}

	rt, err := expr.ResourceType.Query(data)
	if err != nil {
		return nil, fmt.Errorf("error resolving relationship: %w", err)
	}
	if rt == nil {
		return nil, fmt.Errorf("error resolving relationship: empty resource type")
	}
	ri, err := expr.ResourceID.Query(data)
	if err != nil {
		return nil, fmt.Errorf("error resolving relationship: %w", err)
	}
	if ri == nil {
		return nil, fmt.Errorf("error resolving relationship: empty resource id")
	}
	rr, err := expr.ResourceRelation.Query(data)
	if err != nil {
		return nil, fmt.Errorf("error resolving relationship: %w", err)
	}
	if rr == nil {
		return nil, fmt.Errorf("error resolving relationship: empty relation")
	}
	st, err := expr.SubjectType.Query(data)
	if err != nil {
		return nil, fmt.Errorf("error resolving relationship: %w", err)
	}
	if st == nil {
		return nil, fmt.Errorf("error resolving relationship: empty subject type")
	}
	si, err := expr.SubjectID.Query(data)
	if err != nil {
		return nil, fmt.Errorf("error resolving relationship: %w", err)
	}
	if si == nil {
		return nil, fmt.Errorf("error resolving relationship: empty subject id")
	}

	rel := &ResolvedRel{
		ResourceType:     rt.(string),
		ResourceID:       ri.(string),
		ResourceRelation: rr.(string),
		SubjectType:      st.(string),
		SubjectID:        si.(string),
	}
	if expr.SubjectRelation != nil {
		sr, err := expr.SubjectRelation.Query(data)
		if err != nil {
			return nil, fmt.Errorf("error resolving relationship: %w", err)
		}
		if sr == nil {
			return nil, fmt.Errorf("error resolving relationship: empty subject relation")
		}
		rel.SubjectRelation = sr.(string)
	}

	return rel, nil
}

// convertToBloblangInput converts ResolveInput to a format suitable for Bloblang
func convertToBloblangInput(input *ResolveInput) (map[string]any, error) {
	// Convert to a map structure that Bloblang can navigate
	data := map[string]any{
		"name":           input.Name,
		"namespace":      input.Namespace,
		"namespacedName": input.NamespacedName,
		"resourceId":     input.NamespacedName, // Add resourceId field for compatibility
		"headers":        input.Headers,
	}

	// Convert request info to map
	if input.Request != nil {
		data["request"] = map[string]any{
			"verb":       input.Request.Verb,
			"apiGroup":   input.Request.APIGroup,
			"apiVersion": input.Request.APIVersion,
			"resource":   input.Request.Resource,
			"name":       input.Request.Name,
			"namespace":  input.Request.Namespace,
		}
	}

	// Convert user info to map
	if input.User != nil {
		data["user"] = map[string]any{
			"name":   input.User.Name,
			"uid":    input.User.UID,
			"groups": input.User.Groups,
			"extra":  input.User.Extra,
		}
	}

	if input.Object != nil {
		// Convert ObjectMeta to a simpler map structure for Bloblang
		labels := make(map[string]any)
		if input.Object.ObjectMeta.Labels != nil {
			for k, v := range input.Object.ObjectMeta.Labels {
				labels[k] = v
			}
		}

		obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(input.Object)
		if err != nil {
			return nil, fmt.Errorf("error converting object metadata to unstructured: %w", err)
		}

		objectData := map[string]any{
			"metadata": obj["metadata"],
		}

		data["object"] = objectData
		data["metadata"] = objectData["metadata"]
	}

	if len(input.Body) > 0 {
		data["body"] = input.Body
	}

	return data, nil
}

// RunnableRule is a set of checks, writes, and filters with fully compiled
// expressions for building and matching relationships.
type RunnableRule struct {
	LockMode  proxyrule.LockMode
	Checks    []*RelExpr
	Update    *UpdateSet
	PreFilter []*PreFilter
}

type UpdateSet struct {
	MustExist    []*RelExpr
	MustNotExist []*RelExpr
	Creates      []*RelExpr
	Touches      []*RelExpr
	Deletes      []*RelExpr
}

// LookupType defines whether an LR or LS request is made for a filter
type LookupType uint8

const (
	LookupTypeResource = iota
)

// PreFilter defines a filter that returns values that will be used to filter
// the name/namespace of the kube objects.
type PreFilter struct {
	LookupType
	NameFromObjectID, NamespaceFromObjectID *bloblang.Executor
	Rel                                     *RelExpr
}

// ResolvedPreFilter contains a resolved Rel that determines how to make the
// LR / LS request. Name and Namespace are still Bloblang expressions because
// they operate over the LR / LS response.
type ResolvedPreFilter struct {
	LookupType
	NameFromObjectID, NamespaceFromObjectID *bloblang.Executor
	Rel                                     *ResolvedRel
}

// Compile creates a RunnableRule from a passed in config object. String
// templates are parsed into relationship template expressions and any
// Bloblang expressions are pre-compiled and stored.
func Compile(config proxyrule.Config) (*RunnableRule, error) {
	runnable := &RunnableRule{
		LockMode: config.Locking,
	}
	var err error
	runnable.Checks, err = compileStringOrObjTemplates(config.Checks)
	if err != nil {
		return nil, err
	}

	var updateSet *UpdateSet

	if config.Update.PreconditionExists != nil {
		if updateSet == nil {
			updateSet = &UpdateSet{}
		}

		must, err := compileStringOrObjTemplates(config.Update.PreconditionExists)
		if err != nil {
			return nil, err
		}
		updateSet.MustExist = must
	}

	if config.Update.PreconditionDoesNotExist != nil {
		if updateSet == nil {
			updateSet = &UpdateSet{}
		}

		mustNot, err := compileStringOrObjTemplates(config.Update.PreconditionDoesNotExist)
		if err != nil {
			return nil, err
		}
		updateSet.MustNotExist = mustNot
	}

	if config.Update.CreateRelationships != nil {
		if updateSet == nil {
			updateSet = &UpdateSet{}
		}

		creates, err := compileStringOrObjTemplates(config.Update.CreateRelationships)
		if err != nil {
			return nil, err
		}
		updateSet.Creates = creates
	}

	if config.Update.TouchRelationships != nil {
		if updateSet == nil {
			updateSet = &UpdateSet{}
		}

		touches, err := compileStringOrObjTemplates(config.Update.TouchRelationships)
		if err != nil {
			return nil, err
		}
		updateSet.Touches = touches
	}

	if config.Update.DeleteRelationships != nil {
		if updateSet == nil {
			updateSet = &UpdateSet{}
		}

		deletes, err := compileStringOrObjTemplates(config.Update.DeleteRelationships)
		if err != nil {
			return nil, err
		}

		updateSet.Deletes = deletes
	}

	runnable.Update = updateSet

	for _, f := range config.PreFilters {
		name, err := CompileBloblangExpression(f.FromObjectIDNameExpr)
		if err != nil {
			return nil, err
		}
		namespace, err := CompileBloblangExpression(f.FromObjectIDNamespaceExpr)
		if err != nil {
			return nil, fmt.Errorf("failed to compile bloblang: %w", err)
		}
		filter := &PreFilter{
			NameFromObjectID:      name,
			NamespaceFromObjectID: namespace,
		}
		if f.LookupMatchingResources != nil {
			byResource, err := compileStringOrObjTemplates([]proxyrule.StringOrTemplate{*f.LookupMatchingResources})
			if err != nil {
				return nil, err
			}
			if len(byResource) != 1 {
				return nil, fmt.Errorf("pre-filter must have exactly one LookupMatchingResources template")
			}

			processedResourceID, err := byResource[0].ResourceID.Query(map[string]any{"resourceId": "$"})
			if err != nil {
				return nil, fmt.Errorf("error processing resource ID in LookupMatchingResources: %w", err)
			}

			if processedResourceID != proxyrule.MatchingIDFieldValue {
				return nil, fmt.Errorf("LookupMatchingResources resourceID must be set to $ to match all resources, got %q", processedResourceID)
			}

			filter.Rel = byResource[0]
			filter.LookupType = LookupTypeResource
		} else {
			return nil, fmt.Errorf("pre-filter must have LookupMatchingResources defined")
		}

		runnable.PreFilter = append(runnable.PreFilter, filter)
	}

	return runnable, nil
}

// compileStringOrObjTemplates converts a list of StringOrTemplate into a
// list of compiled RelExpr.
func compileStringOrObjTemplates(tmpls []proxyrule.StringOrTemplate) ([]*RelExpr, error) {
	exprs := make([]*RelExpr, 0, len(tmpls))
	for _, c := range tmpls {
		var tpl *UncompiledRelExpr
		if len(c.Template) > 0 {
			var err error
			tpl, err = ParseRelSring(c.Template)
			if err != nil {
				return nil, err
			}
		} else {
			tpl = &UncompiledRelExpr{
				ResourceType:     c.Resource.Type,
				ResourceID:       c.Resource.ID,
				ResourceRelation: c.Resource.Relation,
				SubjectType:      c.Subject.Type,
				SubjectID:        c.Subject.ID,
				SubjectRelation:  c.Subject.Relation,
			}
		}
		expr, err := compileUnparsedRelExpr(tpl)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}
	return exprs, nil
}

// compileUnparsedRelExpr pre-compiles all expressions in an UncompiledRelExpr
func compileUnparsedRelExpr(u *UncompiledRelExpr) (*RelExpr, error) {
	expr := RelExpr{}
	var err error
	expr.ResourceType, err = CompileBloblangExpression(u.ResourceType)
	if err != nil {
		return nil, fmt.Errorf("error compiling resource type %q: %w", u.ResourceType, err)
	}
	expr.ResourceID, err = CompileBloblangExpression(u.ResourceID)
	if err != nil {
		return nil, fmt.Errorf("error compiling resource id %q: %w", u.ResourceID, err)
	}
	expr.ResourceRelation, err = CompileBloblangExpression(u.ResourceRelation)
	if err != nil {
		return nil, fmt.Errorf("error compiling resource relation %q: %w", u.ResourceRelation, err)
	}
	expr.SubjectType, err = CompileBloblangExpression(u.SubjectType)
	if err != nil {
		return nil, fmt.Errorf("error compiling subject type %q: %w", u.SubjectType, err)
	}
	expr.SubjectID, err = CompileBloblangExpression(u.SubjectID)
	if err != nil {
		return nil, fmt.Errorf("error compiling subject id %q: %w", u.SubjectID, err)
	}
	if len(u.SubjectRelation) > 0 {
		expr.SubjectRelation, err = CompileBloblangExpression(u.SubjectRelation)
		if err != nil {
			return nil, fmt.Errorf("error compiling subject relation %q: %w", u.SubjectRelation, err)
		}
	}
	return &expr, nil
}

// CompileBloblangExpression checks to see if its argument is an expression of
// the form `{{ ... }}` where ... is a Bloblang expression. If the argument
// doesn't appear to be an expression, it is returned as a literal expression.
func CompileBloblangExpression(expr string) (*bloblang.Executor, error) {
	// Special case for empty string
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return customBloblangEnv.Parse(`""`)
	}

	expr, hasPrefix := strings.CutPrefix(expr, "{{")
	expr, hasSuffix := strings.CutSuffix(expr, "}}")
	if !hasPrefix || !hasSuffix {
		// Return the expression that returns a literal
		// This makes downstream processing simple (everything is an expression)
		// but is low-hanging fruit for optimization if needed in the future.
		if expr == "" {
			return customBloblangEnv.Parse(`""`)
		}
		return customBloblangEnv.Parse(`"` + expr + `"`)
	}
	return customBloblangEnv.Parse(expr)
}

var relRegex = regexp.MustCompile(
	`^(?P<resourceType>(.*?)):(?P<resourceID>.*?)#(?P<resourceRel>.*?)@(?P<subjectType>(.*?)):(?P<subjectID>.*?)(#(?P<subjectRel>.*?))?$`,
)

// ParseRelSring parses a string representation of a relationship template
// expression.
func ParseRelSring(tpl string) (*UncompiledRelExpr, error) {
	groups := relRegex.FindStringSubmatch(tpl)
	if len(groups) == 0 {
		return nil, fmt.Errorf("invalid template")
	}
	parsed := UncompiledRelExpr{
		ResourceType:     groups[slices.Index(relRegex.SubexpNames(), "resourceType")],
		ResourceID:       groups[slices.Index(relRegex.SubexpNames(), "resourceID")],
		ResourceRelation: groups[slices.Index(relRegex.SubexpNames(), "resourceRel")],
		SubjectType:      groups[slices.Index(relRegex.SubexpNames(), "subjectType")],
		SubjectID:        groups[slices.Index(relRegex.SubexpNames(), "subjectID")],
	}
	subjectRelIndex := slices.Index(relRegex.SubexpNames(), "subjectRel")
	if len(groups[subjectRelIndex]) > 0 {
		parsed.SubjectRelation = groups[subjectRelIndex]
	}
	return &parsed, nil
}
