//go:build mage

package main

import (
	"fmt"

	"github.com/magefile/mage/mg"
)

type Lint mg.Namespace

// All Run all linters
func (l Lint) All() error {
	mg.Deps(l.Go)
	return nil
}

// Go Run all go linters
func (l Lint) Go() error {
	mg.Deps(l.Gofumpt, l.Golangcilint, l.GoModTidy)
	return nil
}

// Gofumpt Run gofumpt
func (Lint) Gofumpt() error {
	return RunSh("go", WithV())("tool", "gofumpt", "-l", "-w", ".")
}

// Golangcilint Run golangci-lint
func (Lint) Golangcilint() error {
	fmt.Println("running golangci-lint")
	return RunSh("go", WithV())("tool", "golangci-lint", "run", "--fix")
}

// GoModTidy Runs go mod tidy
func (Lint) GoModTidy() error {
	fmt.Println("running go mod tidy")
	return RunSh("go", WithV())("mod", "tidy")
}

// Vulncheck Run vulncheck
func (Lint) Vulncheck() error {
	fmt.Println("running vulncheck")
	return RunSh("go", WithV())("tool", "govulncheck", "./...")
}

// Trivy Runs Trivy security scanner in filesystem mode
func (Lint) Trivy() error {
	fmt.Println("⚠️ trivy is temporarily disabled due to a conflict with docker v28.0.0+incompatible, brought in by buf")
	// TODO reenable once trivy fixes the breaking change
	return nil
	return RunSh("go", WithV())("tool", "trivy", "fs",
		"--scanners", "vuln",
		"--severity", "MEDIUM,HIGH,CRITICAL",
		"--db-repository", "public.ecr.aws/aquasecurity/trivy-db,mirror.gcr.io/aquasec/trivy-db:2,ghcr.io/aquasecurity/trivy-db:2",
		"--ignore-unfixed",
		"--exit-code", "1",
		".")
}
