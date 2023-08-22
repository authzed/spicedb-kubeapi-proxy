//go:build mage
// +build mage

package main

import (
	"github.com/magefile/mage/mg"
)

type Test mg.Namespace

// Unit runs the unit tests
func (Test) Unit() error {
	return RunSh("go", WithV())("test", "./...")
}

// E2e runs the end-to-end tests against a real apiserver
func (Test) E2e() error {
	return RunSh("go", Tool())(
		"run",
		"github.com/onsi/ginkgo/v2/ginkgo",
		"--tags=e2e",
		"-r",
		"-vv",
		"--fail-fast",
		"--randomize-all",
		"../e2e",
	)
}
