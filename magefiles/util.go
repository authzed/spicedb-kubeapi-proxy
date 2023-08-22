//go:build mage
// +build mage

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	kocmd "github.com/google/ko/pkg/commands"
	koopts "github.com/google/ko/pkg/commands/options"
	kopublish "github.com/google/ko/pkg/publish"
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
