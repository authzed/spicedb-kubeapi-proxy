package authz

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

func TestFindRules(t *testing.T) {
	// Create test rules
	rule1 := &rules.RunnableRule{Name: "rule1", Update: &rules.UpdateSet{}}
	rule2 := &rules.RunnableRule{Name: "rule2"}
	rule3 := &rules.RunnableRule{Name: "rule3", Update: &rules.UpdateSet{}}
	rule4 := &rules.RunnableRule{Name: "rule4", PreFilter: []*rules.PreFilter{{}}}

	allRules := []*rules.RunnableRule{rule1, rule2, rule3, rule4}

	tests := []struct {
		name       string
		unfiltered []*rules.RunnableRule
		matcher    func(*rules.RunnableRule) bool
		expected   []*rules.RunnableRule
	}{
		{
			name:       "find rules with update",
			unfiltered: allRules,
			matcher: func(r *rules.RunnableRule) bool {
				return r.Update != nil
			},
			expected: []*rules.RunnableRule{rule1, rule3},
		},
		{
			name:       "find rules with prefilter",
			unfiltered: allRules,
			matcher: func(r *rules.RunnableRule) bool {
				return len(r.PreFilter) > 0
			},
			expected: []*rules.RunnableRule{rule4},
		},
		{
			name:       "find rules with specific name",
			unfiltered: allRules,
			matcher: func(r *rules.RunnableRule) bool {
				return r.Name == "rule2"
			},
			expected: []*rules.RunnableRule{rule2},
		},
		{
			name:       "no matching rules",
			unfiltered: allRules,
			matcher: func(r *rules.RunnableRule) bool {
				return r.Name == "nonexistent"
			},
			expected: []*rules.RunnableRule{},
		},
		{
			name:       "empty input",
			unfiltered: []*rules.RunnableRule{},
			matcher: func(r *rules.RunnableRule) bool {
				return true
			},
			expected: []*rules.RunnableRule{},
		},
		{
			name:       "all rules match",
			unfiltered: allRules,
			matcher: func(r *rules.RunnableRule) bool {
				return true
			},
			expected: allRules,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findRules(tt.unfiltered, tt.matcher)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSingleUpdateRule(t *testing.T) {
	// Create test rules
	ruleWithUpdate1 := &rules.RunnableRule{Name: "rule1", Update: &rules.UpdateSet{}}
	ruleWithUpdate2 := &rules.RunnableRule{Name: "rule2", Update: &rules.UpdateSet{}}
	ruleWithoutUpdate := &rules.RunnableRule{Name: "rule3"}

	tests := []struct {
		name          string
		matchingRules []*rules.RunnableRule
		expectedRule  *rules.RunnableRule
		expectedError bool
		errorContains string
	}{
		{
			name:          "single rule with update",
			matchingRules: []*rules.RunnableRule{ruleWithUpdate1, ruleWithoutUpdate},
			expectedRule:  ruleWithUpdate1,
			expectedError: false,
		},
		{
			name:          "no rules with update",
			matchingRules: []*rules.RunnableRule{ruleWithoutUpdate},
			expectedRule:  nil,
			expectedError: false,
		},
		{
			name:          "multiple rules with update",
			matchingRules: []*rules.RunnableRule{ruleWithUpdate1, ruleWithUpdate2},
			expectedRule:  nil,
			expectedError: true,
			errorContains: "multiple write rules matched",
		},
		{
			name:          "empty input",
			matchingRules: []*rules.RunnableRule{},
			expectedRule:  nil,
			expectedError: false,
		},
		{
			name:          "nil input",
			matchingRules: nil,
			expectedRule:  nil,
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := singleUpdateRule(tt.matchingRules)

			if tt.expectedError {
				require.Error(t, err)
				require.Nil(t, result)
				if tt.errorContains != "" {
					require.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedRule, result)
			}
		})
	}
}

func TestPreFilterRules(t *testing.T) {
	// Create test rules
	ruleWithPreFilter := &rules.RunnableRule{
		Name:      "rule1",
		PreFilter: []*rules.PreFilter{{}},
	}
	ruleWithoutPreFilter := &rules.RunnableRule{
		Name:      "rule2",
		PreFilter: []*rules.PreFilter{},
	}
	ruleWithNilPreFilter := &rules.RunnableRule{
		Name:      "rule3",
		PreFilter: nil,
	}
	ruleWithMultiplePreFilters := &rules.RunnableRule{
		Name: "rule4",
		PreFilter: []*rules.PreFilter{
			{},
			{},
		},
	}

	tests := []struct {
		name          string
		matchingRules []*rules.RunnableRule
		expected      []*rules.RunnableRule
	}{
		{
			name: "mixed rules with and without prefilters",
			matchingRules: []*rules.RunnableRule{
				ruleWithPreFilter,
				ruleWithoutPreFilter,
				ruleWithNilPreFilter,
				ruleWithMultiplePreFilters,
			},
			expected: []*rules.RunnableRule{
				ruleWithPreFilter,
				ruleWithMultiplePreFilters,
			},
		},
		{
			name:          "no rules with prefilters",
			matchingRules: []*rules.RunnableRule{ruleWithoutPreFilter, ruleWithNilPreFilter},
			expected:      []*rules.RunnableRule{},
		},
		{
			name:          "all rules have prefilters",
			matchingRules: []*rules.RunnableRule{ruleWithPreFilter, ruleWithMultiplePreFilters},
			expected:      []*rules.RunnableRule{ruleWithPreFilter, ruleWithMultiplePreFilters},
		},
		{
			name:          "empty input",
			matchingRules: []*rules.RunnableRule{},
			expected:      []*rules.RunnableRule{},
		},
		{
			name:          "nil input",
			matchingRules: nil,
			expected:      []*rules.RunnableRule{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := preFilterRules(tt.matchingRules)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestPostFilterRules(t *testing.T) {
	// Create test rules
	ruleWithPostFilter := &rules.RunnableRule{
		Name:       "rule1",
		PostFilter: []*rules.PostFilter{{}},
	}
	ruleWithoutPostFilter := &rules.RunnableRule{
		Name:       "rule2",
		PostFilter: []*rules.PostFilter{},
	}
	ruleWithNilPostFilter := &rules.RunnableRule{
		Name:       "rule3",
		PostFilter: nil,
	}
	ruleWithMultiplePostFilters := &rules.RunnableRule{
		Name: "rule4",
		PostFilter: []*rules.PostFilter{
			{},
			{},
		},
	}

	tests := []struct {
		name          string
		matchingRules []*rules.RunnableRule
		expected      []*rules.RunnableRule
	}{
		{
			name: "mixed rules with and without postfilters",
			matchingRules: []*rules.RunnableRule{
				ruleWithPostFilter,
				ruleWithoutPostFilter,
				ruleWithNilPostFilter,
				ruleWithMultiplePostFilters,
			},
			expected: []*rules.RunnableRule{
				ruleWithPostFilter,
				ruleWithMultiplePostFilters,
			},
		},
		{
			name:          "no rules with postfilters",
			matchingRules: []*rules.RunnableRule{ruleWithoutPostFilter, ruleWithNilPostFilter},
			expected:      []*rules.RunnableRule{},
		},
		{
			name:          "all rules have postfilters",
			matchingRules: []*rules.RunnableRule{ruleWithPostFilter, ruleWithMultiplePostFilters},
			expected:      []*rules.RunnableRule{ruleWithPostFilter, ruleWithMultiplePostFilters},
		},
		{
			name:          "empty input",
			matchingRules: []*rules.RunnableRule{},
			expected:      []*rules.RunnableRule{},
		},
		{
			name:          "nil input",
			matchingRules: nil,
			expected:      []*rules.RunnableRule{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := postFilterRules(tt.matchingRules)
			require.Equal(t, tt.expected, result)
		})
	}
}
