package rules

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"github.com/kyverno/go-jmespath"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
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

// RelExpr represents a relationship with optional JMESExpr
// expressions for field values.
type RelExpr struct {
	ResourceType     *jmespath.JMESPath
	ResourceID       *jmespath.JMESPath
	ResourceRelation *jmespath.JMESPath
	SubjectType      *jmespath.JMESPath
	SubjectID        *jmespath.JMESPath
	SubjectRelation  *jmespath.JMESPath
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
		decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewBuffer(body), 100)
		var pom metav1.PartialObjectMetadata
		if err := decoder.Decode(&pom); err != nil {
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
	// It would be nice to not have to marshal/unmarshal this data, it might
	// be saner to document a nested map format and use it directly as input.
	byteIn, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("error converting input: %w", err)
	}
	var data any
	if err := json.Unmarshal(byteIn, &data); err != nil {
		return nil, fmt.Errorf("error converting input: %w", err)
	}

	rt, err := expr.ResourceType.Search(data)
	if err != nil {
		return nil, fmt.Errorf("error resolving relationship: %w", err)
	}
	if rt == nil {
		return nil, fmt.Errorf("error resolving relationship: empty resource type")
	}
	ri, err := expr.ResourceID.Search(data)
	if err != nil {
		return nil, fmt.Errorf("error resolving relationship: %w", err)
	}
	if ri == nil {
		return nil, fmt.Errorf("error resolving relationship: empty resource id")
	}
	rr, err := expr.ResourceRelation.Search(data)
	if err != nil {
		return nil, fmt.Errorf("error resolving relationship: %w", err)
	}
	if rr == nil {
		return nil, fmt.Errorf("error resolving relationship: empty relation")
	}
	st, err := expr.SubjectType.Search(data)
	if err != nil {
		return nil, fmt.Errorf("error resolving relationship: %w", err)
	}
	if st == nil {
		return nil, fmt.Errorf("error resolving relationship: empty subject type")
	}
	si, err := expr.SubjectID.Search(data)
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
		sr, err := expr.SubjectRelation.Search(data)
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

// RunnableRule is a set of checks, writes, and filters with fully compiled
// expressions for building and matching relationships.
type RunnableRule struct {
	LockMode  proxyrule.LockMode
	Checks    []*RelExpr
	Must      []*RelExpr
	MustNot   []*RelExpr
	Writes    []*RelExpr
	PreFilter []*PreFilter
}

// LookupType defines whether an LR or LS request is made for a filter
type LookupType uint8

const (
	LookupTypeResource = iota
	LookupTypeSubject
)

// PreFilter defines a filter that returns values that will be used to filter
// the name/namespace of the kube objects.
type PreFilter struct {
	LookupType
	Name, Namespace *jmespath.JMESPath
	Rel             *RelExpr
}

// ResolvedPreFilter contains a resolved Rel that determines how to make the
// LR / LS request. Name and Namespace are still jmespath expressions because
// the operate over the LR / LS response.
type ResolvedPreFilter struct {
	LookupType
	Name, Namespace *jmespath.JMESPath
	Rel             *ResolvedRel
}

// Compile creates a RunnableRule from a passed in config object. String
// templates are parsed into relationship template expressions and any
// jmespath expressions are pre-compiled and stored.
func Compile(config proxyrule.Config) (*RunnableRule, error) {
	runnable := &RunnableRule{
		LockMode: config.Locking,
	}
	var err error
	runnable.Checks, err = compileStringOrObjTemplates(config.Checks)
	if err != nil {
		return nil, err
	}
	runnable.Must, err = compileStringOrObjTemplates(config.Must)
	if err != nil {
		return nil, err
	}
	runnable.MustNot, err = compileStringOrObjTemplates(config.MustNot)
	if err != nil {
		return nil, err
	}
	runnable.Writes, err = compileStringOrObjTemplates(config.Writes)
	if err != nil {
		return nil, err
	}
	for _, f := range config.PreFilters {
		name, err := jmespath.Compile(f.Name)
		if err != nil {
			return nil, err
		}
		name.Register(splitName)
		name.Register(splitNamespace)
		if f.Namespace == "" {
			// this will compile to the jmespath expression returning ""
			f.Namespace = "''"
		}
		namespace, err := jmespath.Compile(f.Namespace)
		if err != nil {
			return nil, fmt.Errorf("failed to compile jmespath: %w", err)
		}
		namespace.Register(splitName)
		namespace.Register(splitNamespace)
		filter := &PreFilter{
			Name:      name,
			Namespace: namespace,
		}
		if f.ByResource != nil {
			byResource, err := compileStringOrObjTemplates([]proxyrule.StringOrTemplate{*f.ByResource})
			if err != nil {
				return nil, err
			}
			filter.Rel = byResource[0]
			filter.LookupType = LookupTypeResource
		}
		if f.BySubject != nil {
			bySubject, err := compileStringOrObjTemplates([]proxyrule.StringOrTemplate{*f.BySubject})
			if err != nil {
				return nil, err
			}
			filter.Rel = bySubject[0]
			filter.LookupType = LookupTypeSubject
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
	expr.ResourceType, err = CompileJMESPathExpression(u.ResourceType)
	if err != nil {
		return nil, fmt.Errorf("error compiling resource type %q: %w", u.ResourceType, err)
	}
	expr.ResourceID, err = CompileJMESPathExpression(u.ResourceID)
	if err != nil {
		return nil, fmt.Errorf("error compiling resource id %q: %w", u.ResourceID, err)
	}
	expr.ResourceRelation, err = CompileJMESPathExpression(u.ResourceRelation)
	if err != nil {
		return nil, fmt.Errorf("error compiling resource relation %q: %w", u.ResourceRelation, err)
	}
	expr.SubjectType, err = CompileJMESPathExpression(u.SubjectType)
	if err != nil {
		return nil, fmt.Errorf("error compiling subject type %q: %w", u.SubjectType, err)
	}
	expr.SubjectID, err = CompileJMESPathExpression(u.SubjectID)
	if err != nil {
		return nil, fmt.Errorf("error compiling subject id %q: %w", u.SubjectID, err)
	}
	if len(u.SubjectRelation) > 0 {
		expr.SubjectRelation, err = CompileJMESPathExpression(u.SubjectRelation)
		if err != nil {
			return nil, fmt.Errorf("error compiling subject relation %q: %w", u.SubjectRelation, err)
		}
	}
	return &expr, nil
}

// CompileJMESPathExpression checks to see if its argument is an expression of
// the form `{{ ... }}` where ... is a JMESPath expression. If the argument
// doesn't appear to be an expression, it is returned as a literal expression.
func CompileJMESPathExpression(expr string) (*jmespath.JMESPath, error) {
	expr = strings.TrimSpace(expr)
	expr, hasPrefix := strings.CutPrefix(expr, "{{")
	expr, hasSuffix := strings.CutSuffix(expr, "}}")
	if !hasPrefix || !hasSuffix {
		// Return the expression that returns a literal
		// This makes downstream processing simple (everything is an expression)
		// but is low-hanging fruit for optimization if needed in the future.
		return jmespath.Compile("'" + expr + "'")
	}
	return jmespath.Compile(expr)
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

var splitName = jmespath.FunctionEntry{
	Name: "splitName",
	Arguments: []jmespath.ArgSpec{
		{Types: []jmespath.JpType{jmespath.JpString}},
	},
	Handler: func(arguments []any) (any, error) {
		_, name, ok := strings.Cut(arguments[0].(string), "/")
		if !ok {
			return arguments[0].(string), nil
		}
		return name, nil
	},
}

var splitNamespace = jmespath.FunctionEntry{
	Name: "splitNamespace",
	Arguments: []jmespath.ArgSpec{
		{Types: []jmespath.JpType{jmespath.JpString}},
	},
	Handler: func(arguments []any) (any, error) {
		namespace, _, ok := strings.Cut(arguments[0].(string), "/")
		if !ok {
			return "", nil
		}
		return namespace, nil
	},
}
