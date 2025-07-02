package rules

import (
	"fmt"
	"strings"

	"github.com/warpstreamlabs/bento/public/bloblang"
)

var (
	// Custom Bloblang environment with splitName and splitNamespace functions
	customBloblangEnv *bloblang.Environment
)

func init() {
	// Create custom Bloblang environment with splitName and splitNamespace functions
	customBloblangEnv = bloblang.NewEnvironment()
	
	// Register split_name function
	err := customBloblangEnv.RegisterFunction("split_name", func(args ...any) (bloblang.Function, error) {
		return func() (any, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("splitName function expects exactly 1 argument")
			}
			input, ok := args[0].(string)
			if !ok {
				return nil, fmt.Errorf("splitName function expects string argument")
			}
			_, name, ok := strings.Cut(input, "/")
			if !ok {
				return input, nil
			}
			return name, nil
		}, nil
	})
	if err != nil {
		panic(fmt.Sprintf("failed to register split_name function: %v", err))
	}
	
	// Register split_namespace function
	err = customBloblangEnv.RegisterFunction("split_namespace", func(args ...any) (bloblang.Function, error) {
		return func() (any, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("splitNamespace function expects exactly 1 argument")
			}
			input, ok := args[0].(string)
			if !ok {
				return nil, fmt.Errorf("splitNamespace function expects string argument")
			}
			namespace, _, ok := strings.Cut(input, "/")
			if !ok {
				return "", nil
			}
			return namespace, nil
		}, nil
	})
	if err != nil {
		panic(fmt.Sprintf("failed to register split_namespace function: %v", err))
	}
}