//go:build mage
// +build mage

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	kocmd "github.com/google/ko/pkg/commands"
	koopts "github.com/google/ko/pkg/commands/options"
	kopublish "github.com/google/ko/pkg/publish"
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"github.com/onsi/gomega/gexec"
	"golang.org/x/exp/slices"
	"sigs.k8s.io/kind/pkg/apis/config/v1alpha4"

	kind "sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/cluster/nodeutils"
	"sigs.k8s.io/kind/pkg/cmd"
	"sigs.k8s.io/kind/pkg/fs"
)

func checkBinary(name, reason, install string, args []string) error {
	if !hasBinary(name) {
		var installSuggestion string
		if install != "" {
			installSuggestion = fmt.Sprintf(" - install with: %s", install)
		}
		return fmt.Errorf("%s must be installed to %s%s", name, reason, installSuggestion)
	}
	err := sh.Run(name, args...)
	if err == nil || sh.ExitStatus(err) == 0 {
		return nil
	}
	return err
}

func checkDocker() error {
	return checkBinary("docker", "run kind", "brew install --cask docker", []string{"ps"})
}

func checkKustomizer() error {
	return checkBinary("kustomizer", "deploy", "brew install stefanprodan/tap/kustomizer", nil)
}

func hasBinary(binaryName string) bool {
	_, err := exec.LookPath(binaryName)
	return err == nil
}

func kindConfig(ports map[int32]int32) *v1alpha4.Cluster {
	config := &v1alpha4.Cluster{
		TypeMeta: v1alpha4.TypeMeta{
			Kind:       "Cluster",
			APIVersion: "kind.x-k8s.io/v1alpha4",
		},
		Nodes: []v1alpha4.Node{{
			Role:              v1alpha4.ControlPlaneRole,
			ExtraPortMappings: []v1alpha4.PortMapping{},
		}},
	}
	for host, container := range ports {
		config.Nodes[0].ExtraPortMappings = append(config.Nodes[0].ExtraPortMappings, v1alpha4.PortMapping{
			HostPort:      host,
			ContainerPort: container,
			Protocol:      v1alpha4.PortMappingProtocolTCP,
		})
	}
	return config
}

func provisionKind(name string, images []string, archives []string, config *v1alpha4.Cluster) (string, func() error, func(string) error, error) {
	fmt.Sprintf("provisiong kind cluster %s", name)
	provider := kind.NewProvider(
		kind.ProviderWithLogger(cmd.NewLogger()),
	)

	kubeconfig := fmt.Sprintf("%s.kubeconfig", name)

	deprovision := func() error {
		if err := provider.Delete(name, kubeconfig); err != nil {
			return err
		}
		if err := os.Remove(kubeconfig); err != nil {
			return err
		}
		return nil
	}
	exportLogs := func(dir string) error {
		return provider.CollectLogs(name, dir)
	}

	var existing []string
	existing, err := provider.List()
	if err != nil {
		return kubeconfig, deprovision, exportLogs, err
	}

	if slices.Contains(existing, name) {
		err := provider.ExportKubeConfig(name, kubeconfig, false)
		return kubeconfig, deprovision, exportLogs, err
	}

	err = provider.Create(name,
		kind.CreateWithWaitForReady(5*time.Minute),
		kind.CreateWithV1Alpha4Config(config),
	)
	if err != nil {
		err = fmt.Errorf("failed to create kind controller: %w", err)
		return kubeconfig, deprovision, exportLogs, err
	}
	err = provider.ExportKubeConfig(name, kubeconfig, false)
	if err != nil {
		err = fmt.Errorf("failed to export kubeconfig: %w", err)
		return kubeconfig, deprovision, exportLogs, err
	}

	nodes, err := provider.ListNodes(name)
	if err != nil {
		return kubeconfig, deprovision, exportLogs, fmt.Errorf("failed to list kind nodes: %w", err)
	}

	if len(images) > 0 {
		dir, err := fs.TempDir("", "images-tar")
		if err != nil {
			return kubeconfig, deprovision, exportLogs, fmt.Errorf("failed to create tempdir for images: %w", err)
		}
		defer os.RemoveAll(dir)

		imagesTarPath := filepath.Join(dir, "images.tar")

		cmd := exec.Command("docker", append([]string{"save", "-o", imagesTarPath}, images...)...)
		session, err := gexec.Start(cmd, os.Stdout, os.Stderr)
		if err != nil {
			return kubeconfig, deprovision, exportLogs, err
		}
		<-session.Exited
		if c := session.ExitCode(); c != 0 {
			return kubeconfig, deprovision, exportLogs, fmt.Errorf("unexpected error code when saving images: %d", c)
		}

		archives = append(archives, imagesTarPath)
	}

	if len(archives) > 0 {
		for _, archive := range archives {
			if archive == "" {
				continue
			}
			fmt.Printf("loading %s onto nodes\n", archive)
			for _, node := range nodes {
				fd, err := os.Open(archive)
				if err != nil {
					return kubeconfig, deprovision, exportLogs, fmt.Errorf("error opening archive %q: %w", archive, err)
				}
				err = nodeutils.LoadImageArchive(node, fd)
				if err != nil {
					return kubeconfig, deprovision, exportLogs, fmt.Errorf("error loading image archive %q to node %q: %w", archive, node, err)
				}
				if err := fd.Close(); err != nil {
					return kubeconfig, deprovision, exportLogs, fmt.Errorf("error loading image archive %q to node %q: %w", archive, node, err)
				}
			}
		}
	}

	return kubeconfig, deprovision, exportLogs, nil
}

// GetFreePort is a helper used to get a free TCP port on the host
func GetFreePort(listenAddr string) (int32, error) {
	dummyListener, err := net.Listen("tcp", net.JoinHostPort(listenAddr, "0"))
	if err != nil {
		return 0, err
	}
	defer dummyListener.Close()
	port := dummyListener.Addr().(*net.TCPAddr).Port
	return int32(port), nil
}

// overrideEnv gets the current env value, overrides it, and returns a function that can
// set it back to the original value.
func overrideEnv(varName string, value string) func() {
	old := os.Getenv(varName)
	os.Setenv(varName, value)
	return func() {
		os.Setenv(varName, old)
	}
}

// makeGoKindImage builds an image for a Go binary and loads it into kind
func makeGoKindImage(ctx context.Context, clusterName string, imageName string, workingDir string, buildPath string) (string, error) {
	defer overrideEnv("KIND_CLUSTER_NAME", clusterName)()
	defer overrideEnv("GOOS", "linux")()
	defer overrideEnv("GOARCH", runtime.GOARCH)()
	log.SetOutput(os.Stdout)
	opts := koopts.BuildOptions{}
	if err := opts.LoadConfig(); err != nil {
		return "", err
	}
	opts.Platforms = []string{}
	opts.WorkingDirectory = workingDir
	opts.ConcurrentBuilds = runtime.NumCPU()
	builder, err := kocmd.NewBuilder(ctx, &opts)
	if err != nil {
		return "", err
	}
	result, err := builder.Build(ctx, buildPath)
	if err != nil {
		return "", err
	}

	namer := koopts.MakeNamer(&koopts.PublishOptions{
		DockerRepo:      kopublish.KindDomain,
		BaseImportPaths: true,
	})
	publisher, err := kopublish.NewCaching(kopublish.NewKindPublisher(namer, nil))
	if err != nil {
		return "", err
	}

	name, err := publisher.Publish(ctx, result, imageName)
	if err != nil {
		return "", err
	}
	return name.String(), nil
}

// runOptions is a set of options to be applied with ExecSh.
type runOptions struct {
	cmd            string
	args           []string
	dir            string
	env            map[string]string
	stderr, stdout io.Writer
}

// RunOpt applies an option to a runOptions set.
type RunOpt func(*runOptions)

// WithV sets stderr and stdout the standard streams
func WithV() RunOpt {
	return func(options *runOptions) {
		options.stdout = os.Stdout
		options.stderr = os.Stderr
	}
}

// WithEnv sets the env passed in env vars.
func WithEnv(env map[string]string) RunOpt {
	return func(options *runOptions) {
		if options.env == nil {
			options.env = make(map[string]string)
		}
		for k, v := range env {
			options.env[k] = v
		}
	}
}

// WithStderr sets the stderr stream.
func WithStderr(w io.Writer) RunOpt {
	return func(options *runOptions) {
		options.stderr = w
	}
}

// WithStdout sets the stdout stream.
func WithStdout(w io.Writer) RunOpt {
	return func(options *runOptions) {
		options.stdout = w
	}
}

// WithDir sets the working directory for the command.
func WithDir(dir string) RunOpt {
	return func(options *runOptions) {
		options.dir = dir
	}
}

// WithArgs appends command arguments.
func WithArgs(args ...string) RunOpt {
	return func(options *runOptions) {
		if options.args == nil {
			options.args = make([]string, 0, len(args))
		}
		options.args = append(options.args, args...)
	}
}

func Tool() RunOpt {
	return func(options *runOptions) {
		WithDir("magefiles")(options)
		WithV()(options)
	}
}

// RunSh returns a function that calls ExecSh, only returning errors.
func RunSh(cmd string, options ...RunOpt) func(args ...string) error {
	run := ExecSh(cmd, options...)
	return func(args ...string) error {
		_, err := run(args...)
		return err
	}
}

// ExecSh returns a function that executes the command, piping its stdout and
// stderr according to the config options. If the command fails, it will return
// an error that, if returned from a target or mg.Deps call, will cause mage to
// exit with the same code as the command failed with.
//
// ExecSh takes a variable list of RunOpt objects to configure how the command
// is executed. See RunOpt docs for more details.
//
// Env vars configured on the command override the current environment variables
// set (which are also passed to the command). The cmd and args may include
// references to environment variables in $FOO format, in which case these will be
// expanded before the command is run.
//
// Ran reports if the command ran (rather than was not found or not executable).
// Code reports the exit code the command returned if it ran. If err == nil, ran
// is always true and code is always 0.
func ExecSh(cmd string, options ...RunOpt) func(args ...string) (bool, error) {
	opts := runOptions{
		cmd: cmd,
	}
	for _, o := range options {
		o(&opts)
	}

	if opts.stdout == nil && mg.Verbose() {
		opts.stdout = os.Stdout
	}

	return func(args ...string) (bool, error) {
		expand := func(s string) string {
			s2, ok := opts.env[s]
			if ok {
				return s2
			}
			return os.Getenv(s)
		}
		cmd = os.Expand(cmd, expand)
		finalArgs := append(opts.args, args...)
		for i := range finalArgs {
			finalArgs[i] = os.Expand(finalArgs[i], expand)
		}
		ran, code, err := run(opts.dir, opts.env, opts.stdout, opts.stderr, cmd, finalArgs...)

		if err == nil {
			return ran, nil
		}
		if ran {
			return ran, mg.Fatalf(code, `running "%s %s" failed with exit code %d`, cmd, strings.Join(args, " "), code)
		}
		return ran, fmt.Errorf(`failed to run "%s %s: %v"`, cmd, strings.Join(args, " "), err)
	}
}

func run(dir string, env map[string]string, stdout, stderr io.Writer, cmd string, args ...string) (ran bool, code int, err error) {
	c := exec.Command(cmd, args...)
	c.Env = os.Environ()
	for k, v := range env {
		c.Env = append(c.Env, k+"="+v)
	}
	c.Dir = dir
	c.Stderr = stderr
	c.Stdout = stdout
	c.Stdin = os.Stdin

	var quoted []string
	for i := range args {
		quoted = append(quoted, fmt.Sprintf("%q", args[i]))
	}
	// To protect against logging from doing exec in global variables
	if mg.Verbose() {
		log.Println("exec:", cmd, strings.Join(quoted, " "))
	}
	err = c.Run()
	return sh.CmdRan(err), sh.ExitStatus(err), err
}
