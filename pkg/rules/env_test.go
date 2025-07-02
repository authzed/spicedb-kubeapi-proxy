package rules

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitNameFunction(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected any
		wantErr  bool
	}{
		{
			name:     "valid namespace/name",
			input:    "kube-system/my-pod",
			expected: "my-pod",
		},
		{
			name:     "no slash - returns input",
			input:    "just-a-name",
			expected: "just-a-name",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "slash at end",
			input:    "namespace/",
			expected: "",
		},
		{
			name:     "multiple slashes - splits on first",
			input:    "namespace/name/extra",
			expected: "name/extra",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := CompileBloblangExpression(`{{ split_name(resourceId) }}`)
			require.NoError(t, err)

			data := map[string]any{
				"resourceId": tt.input,
			}

			result, err := expr.Query(data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestSplitNameFunctionErrors(t *testing.T) {
	tests := []struct {
		name      string
		bloblang  string
		data      map[string]any
		wantError string
	}{
		{
			name:     "no arguments",
			bloblang: `{{ split_name() }}`,
			data:     map[string]any{},
			wantError: "splitName function expects exactly 1 argument",
		},
		{
			name:     "too many arguments",
			bloblang: `{{ split_name("arg1", "arg2") }}`,
			data:     map[string]any{},
			wantError: "splitName function expects exactly 1 argument",
		},
		{
			name:     "non-string argument",
			bloblang: `{{ split_name(123) }}`,
			data:     map[string]any{},
			wantError: "splitName function expects string argument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := CompileBloblangExpression(tt.bloblang)
			require.NoError(t, err)

			_, err = expr.Query(tt.data)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantError)
		})
	}
}

func TestSplitNamespaceFunction(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected any
		wantErr  bool
	}{
		{
			name:     "valid namespace/name",
			input:    "kube-system/my-pod",
			expected: "kube-system",
		},
		{
			name:     "no slash - returns empty string",
			input:    "just-a-name",
			expected: "",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "slash at start",
			input:    "/name",
			expected: "",
		},
		{
			name:     "multiple slashes - splits on first",
			input:    "namespace/name/extra",
			expected: "namespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := CompileBloblangExpression(`{{ split_namespace(resourceId) }}`)
			require.NoError(t, err)

			data := map[string]any{
				"resourceId": tt.input,
			}

			result, err := expr.Query(data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestSplitNamespaceFunctionErrors(t *testing.T) {
	tests := []struct {
		name      string
		bloblang  string
		data      map[string]any
		wantError string
	}{
		{
			name:     "no arguments",
			bloblang: `{{ split_namespace() }}`,
			data:     map[string]any{},
			wantError: "splitNamespace function expects exactly 1 argument",
		},
		{
			name:     "too many arguments",
			bloblang: `{{ split_namespace("arg1", "arg2") }}`,
			data:     map[string]any{},
			wantError: "splitNamespace function expects exactly 1 argument",
		},
		{
			name:     "non-string argument",
			bloblang: `{{ split_namespace(123) }}`,
			data:     map[string]any{},
			wantError: "splitNamespace function expects string argument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := CompileBloblangExpression(tt.bloblang)
			require.NoError(t, err)

			_, err = expr.Query(tt.data)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantError)
		})
	}
}