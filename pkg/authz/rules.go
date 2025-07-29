package authz

import (
	"fmt"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

func findRules(unfiltered []*rules.RunnableRule, matcher func(*rules.RunnableRule) bool) []*rules.RunnableRule {
	matchingRules := make([]*rules.RunnableRule, 0, len(unfiltered))
	for _, r := range unfiltered {
		if matcher(r) {
			matchingRules = append(matchingRules, r)
		}
	}
	return matchingRules
}

// singleUpdateRule returns the first matching rule with `update` defined, nil if there are no such rules,
// and an error if there are multiple such rules.
func singleUpdateRule(matchingRules []*rules.RunnableRule) (*rules.RunnableRule, error) {
	rulesWithUpdates := findRules(matchingRules, func(r *rules.RunnableRule) bool {
		return r.Update != nil
	})

	if len(rulesWithUpdates) == 0 {
		return nil, nil
	}

	if len(rulesWithUpdates) > 1 {
		return nil, fmt.Errorf("multiple write rules matched: %v", rulesWithUpdates)
	}

	return rulesWithUpdates[0], nil
}

func preFilterRules(matchingRules []*rules.RunnableRule) []*rules.RunnableRule {
	return findRules(matchingRules, func(r *rules.RunnableRule) bool {
		return len(r.PreFilter) > 0
	})
}

func postFilterRules(matchingRules []*rules.RunnableRule) []*rules.RunnableRule {
	return findRules(matchingRules, func(r *rules.RunnableRule) bool {
		return len(r.PostFilter) > 0
	})
}

func singlePreFilterRule(matchingRules []*rules.RunnableRule) (*rules.RunnableRule, error) {
	rulesWithPreFilter := preFilterRules(matchingRules)

	if len(rulesWithPreFilter) == 0 {
		return nil, nil
	}

	if len(rulesWithPreFilter) > 1 {
		return nil, fmt.Errorf("multiple pre-filter rules matched: %v", rulesWithPreFilter)
	}

	return rulesWithPreFilter[0], nil
}
