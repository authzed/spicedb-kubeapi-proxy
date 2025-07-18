//go:build mage

package main

import (
	"fmt"
	"github.com/magefile/mage/mg"
)

type Lint mg.Namespace

// Golangcilint Run golangci-lint
func (Lint) Golangcilint() error {
	fmt.Println("running golangci-lint")
	return RunSh("go", WithV())("run", "github.com/golangci/golangci-lint/v2/cmd/golangci-lint", "run", "--fix")
}
