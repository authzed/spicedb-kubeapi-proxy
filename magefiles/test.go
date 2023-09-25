//go:build mage
// +build mage

package main

import (
	"github.com/magefile/mage/mg"
)

type Test mg.Namespace

// All runs both unit and e2e tests
func (t Test) All() error {
	mg.Deps(t.Unit, t.E2e)

	return nil
}

// Unit runs the unit tests.
func (Test) Unit() error {
	return RunSh("go", WithV())("test", "./...")
}

// E2e runs the end-to-end tests against a real apiserver.
func (Test) E2e() error {
	return RunSh("go", Tool())(
		"run",
		"github.com/onsi/ginkgo/v2/ginkgo",
		"--tags=e2e,failpoints",
		"-r",
		"-vv",
		"--fail-fast",
		"--randomize-all",
		"../e2e",
	)
}
