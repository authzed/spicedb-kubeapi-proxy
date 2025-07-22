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
func (t Test) Unit() error {
	// TODO add -race
	args := []string{"test", "-count=1"}
	args = append(args, t.coverageFlags()...)
	args = append(args, "./...")
	return RunSh("go", WithV())(args...)
}

// E2e runs the end-to-end tests against a real apiserver.
func (t Test) E2e() error {
	args := []string{"run", "github.com/onsi/ginkgo/v2/ginkgo"}
	args = append(args, t.coverageFlags()...)
	args = append(args,
		"--tags=e2e,failpoints",
		"-r",
		"-vv",
		"--fail-fast",
		"--randomize-all")
	args = append(args, "../e2e")
	return RunSh("go", Tool())(args...)
}

func (t Test) coverageFlags() []string {
	return []string{"-coverpkg=./...", "-covermode=atomic", "-coverprofile=coverage.txt"}
}
