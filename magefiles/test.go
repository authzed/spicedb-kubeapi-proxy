//go:build mage
// +build mage

package main

import (
	"github.com/magefile/mage/mg"
)

type Test mg.Namespace

// Runs the end-to-end tests against a real apiserver
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
