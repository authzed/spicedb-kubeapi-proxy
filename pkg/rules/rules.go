package rules

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/jmespath/go-jmespath"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
)

// RequestMeta uniquely identifies the type of request, and is used to find
// matching rules.
type RequestMeta struct {
	GVR  string
	Verb string
}

// A Matcher holds a set of matching rules in memory for fast matching against
// incoming requests.
// Currently there is only a hash map implementation; you could imagine
// more interesting ways of matching requests.
type Matcher interface {
	Match(match RequestMeta) []*RunnableRule
}

// MapMatcher stores rules in a map keyed on GVR and Verb
type MapMatcher map[RequestMeta][]*RunnableRule

// NewMapMatcher creates a MapMatcher for a set of rules
func NewMapMatcher(configRules []proxyrule.Config) (MapMatcher, error) {
	matchingRules := make(map[RequestMeta][]*RunnableRule, 0)
	for _, r := range configRules {
		for _, m := range r.Matches {
			for _, v := range m.Verbs {
				meta := RequestMeta{
					GVR:  m.GVR,
					Verb: v,
				}
				if _, ok := matchingRules[meta]; !ok {
					matchingRules[meta] = make([]*RunnableRule, 0)
				}
				rules, err := Compile(r)
				if err != nil {
					return nil, err
				}
				matchingRules[meta] = append(matchingRules[meta], rules)
			}
		}
	}
	return matchingRules, nil
}

func (m MapMatcher) Match(match RequestMeta) []*RunnableRule {
	return m[match]
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

// RunnableRule is a set of checks, writes, and filters with fully compiled
// expressions for building and matching relationships.
type RunnableRule struct {
	Checks []*RelExpr
	Writes []*RelExpr
	Filter []*RelExpr
}

// Compile creates a RunnableRule from a passed in config object. String
// templates are parsed into relationship template expressions and any
// jmespath expressions are pre-compiled and stored.
func Compile(config proxyrule.Config) (*RunnableRule, error) {
	runnable := &RunnableRule{}
	var err error
	runnable.Checks, err = compileStringOrObjTemplates(config.Checks)
	if err != nil {
		return nil, err
	}
	runnable.Writes, err = compileStringOrObjTemplates(config.Writes)
	if err != nil {
		return nil, err
	}
	runnable.Filter, err = compileStringOrObjTemplates(config.Filter)
	if err != nil {
		return nil, err
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
