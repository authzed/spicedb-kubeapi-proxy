//go:build mage
// +build mage

package main

import (
	"fmt"

	"github.com/magefile/mage/mg"
)

type Test mg.Namespace

// Unit runs the unit tests.
func (Test) Unit() error {
	return RunSh("go", WithV())("test", "./...")
}

// E2e runs the end-to-end tests against a real apiserver.
func (t Test) E2e() error {
	var f Failpoints
	if err := f.Enable(); err != nil {
		return err
	}
	testErr := RunSh("go", Tool())(
		"run",
		"github.com/onsi/ginkgo/v2/ginkgo",
		"--tags=e2e",
		"-r",
		"-vv",
		"--fail-fast",
		"--randomize-all",
		"../e2e",
	)
	if err := f.Disable(); err != nil {
		fmt.Println(err)
	}
	return testErr
}

type Failpoints mg.Namespace

// Enable transforms the codebase to inject failpoints.
// Generally this is only called by the e2e task, but you can run it manually
// to look at generated code or to leave the codebase in a state ready for e2e
// (i.e. to use a debugger).
func (Failpoints) Enable() error {
	return RunSh("go", Tool())(
		"run",
		"github.com/pingcap/failpoint/failpoint-ctl",
		"enable",
		"../",
	)
}

// Disable transforms the codebase to remove failpoint injection.
func (Failpoints) Disable() error {
	return RunSh("go", Tool())(
		"run",
		"github.com/pingcap/failpoint/failpoint-ctl",
		"disable",
		"../",
	)
}
