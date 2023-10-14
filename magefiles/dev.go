//go:build mage
// +build mage

package main

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	kind "sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/cmd"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/yaml"
)

type Dev mg.Namespace

const (
	proxyNodePort                 = 30443
	clusterName                   = "spicedb-kubeapi-proxy"
	kubeCfgClusterName            = "kind-" + clusterName
	kubeconfigPath                = clusterName + ".kubeconfig"
	developmentKubeConfigFileName = "dev.kubeconfig"

	kubeContextLocalProxy      = "local"
	kubeContextClusterProxy    = "proxy"
	kubeContextUpstreamCluster = "admin"
)

// Run runs the proxy locally against the development cluster - requires dev:up
func (d Dev) Run() error {
	fmt.Println("✅  starting local proxy! To talk to the proxy with kubectl:")
	fmt.Printf("  export KUBECONFIG=$(pwd)/%s\n", developmentKubeConfigFileName)
	fmt.Println("  kubectx local")
	fmt.Println("  kubectl --insecure-skip-tls-verify get namespace")
	return sh.RunV("go", "run",
		"./cmd/spicedb-kubeapi-proxy/main.go",
		"--bind-address=127.0.0.1",
		"--secure-port=8443",
		"--backend-kubeconfig", "spicedb-kubeapi-proxy.kubeconfig",
		"--client-ca-file", "client-ca.crt",
		"--spicedb-endpoint", "embedded://",
		"--rule-config", "./deploy/rules.yaml")
}

// Up brings up a dev cluster with the proxy installed.
func (d Dev) Up(ctx context.Context) error {
	var proxyHostPort int32
	if _, err := os.Stat(kubeconfigPath); err != nil {
		proxyHostPort, err = GetFreePort("localhost")
		if err != nil {
			return err
		}
		_, _, _, err = provisionKind(clusterName, nil, nil, kindConfig(map[int32]int32{proxyHostPort: proxyNodePort}))
		if err != nil {
			return err
		}

		fmt.Println("Generated kubeconfig for Kind cluster: " + kubeconfigPath)
	}

	fmt.Println("Writing Kubeconfig for generated Kind cluster as Secret, will be used by the proxy")
	if err := writeUpstreamKubeconfigAsSecret(ctx, kubeconfigPath); err != nil {
		return err
	}

	imageName, err := makeGoKindImage(ctx, clusterName, "spicedb-kubeapi-proxy", ".", "./cmd/spicedb-kubeapi-proxy")
	if err != nil {
		return err
	}
	if err := updateKustomizationImageTags(imageName); err != nil {
		return err
	}

	if err := apply(); err != nil {
		return err
	}

	if err := generateDevKubeconfig(ctx, fmt.Sprintf("https://127.0.0.1:%d", proxyHostPort)); err != nil {
		return err
	}

	fmt.Println("✅ development environment ready! To talk to the proxy with kubectl:")
	fmt.Printf("   export KUBECONFIG=$(pwd)/%s\n", developmentKubeConfigFileName)
	fmt.Println("   kubectx proxy")
	fmt.Println("   kubectl --context proxy get namespace")
	fmt.Printf("ℹ️  you can also run the proxy locally with: %q\n", "mage dev:run")
	return nil
}

// apply applies kubernetes manifest to an existing running environment, and blocks until all resources are ready
func apply() error {
	return sh.RunV("kustomizer", "apply", "inventory", "spicedb-kubeapi-proxy", "-k", "./deploy", "--prune", "--wait", "--timeout", "5m")
}

// generateDevKubeconfig writes locally a kubeconfig for development purposes, containing context to have kube clients
// either talk to the Kind Kubernetes API server, or to the spicedb-kubeapi-proxy server.
func generateDevKubeconfig(ctx context.Context, proxyHost string) error {
	if _, err := os.Stat(developmentKubeConfigFileName); err == nil {
		fmt.Println("development kubeconfig found, skipping generation")
		return nil
	}

	fmt.Println("no development kubeconfig found, generating with proxy listening at " + proxyHost)
	clientset, err := clientSetForKubeConfig(kubeconfigPath)
	if err != nil {
		return err
	}

	serverCACertBytes, err := getBytesFromSecretField(ctx, clientset, "spicedb-kubeapi-proxy", "spicedb-kubeapi-proxy-server-cert", "ca.crt")
	if err != nil {
		return err
	}

	clientCACertBytes, err := getBytesFromSecretField(ctx, clientset, "spicedb-kubeapi-proxy", "rakis-client-cert", "ca.crt")
	if err != nil {
		return err
	}

	// write it locally in case we need to run the proxy locally with the same CA cert as the dev env
	wd, err := os.Getwd()
	caPath := path.Join(wd, "client-ca.crt")
	if err := os.WriteFile(caPath, clientCACertBytes, 0o600); err != nil {
		fmt.Printf("unable to cache proxy client CA certificate in host machine: %s\n", err.Error())
	}

	clientCertBytes, err := getBytesFromSecretField(ctx, clientset, "spicedb-kubeapi-proxy", "rakis-client-cert", "tls.crt")
	if err != nil {
		return err
	}

	certPath := path.Join(wd, "client-cert.crt")
	if err := os.WriteFile(certPath, clientCertBytes, 0o600); err != nil {
		fmt.Printf("unable to cache proxy client certificate in host machine: %s\n", err.Error())
	}

	clientKeyBytes, err := getBytesFromSecretField(ctx, clientset, "spicedb-kubeapi-proxy", "rakis-client-cert", "tls.key")
	if err != nil {
		return err
	}

	keyPath := path.Join(wd, "client-key.crt")
	if err := os.WriteFile(keyPath, clientKeyBytes, 0o600); err != nil {
		fmt.Printf("unable to cache proxy client key in host machine: %s\n", err.Error())
	}

	developmentKubeConfig := clientcmdapi.NewConfig()
	proxyCluster := clientcmdapi.NewCluster()
	proxyCluster.CertificateAuthorityData = serverCACertBytes
	proxyCluster.Server = proxyHost
	developmentKubeConfig.Clusters[kubeContextClusterProxy] = proxyCluster
	localCluster := clientcmdapi.NewCluster()
	localCluster.CertificateAuthorityData = serverCACertBytes
	localCluster.Server = "https://127.0.0.1:8443"
	developmentKubeConfig.Clusters[kubeContextLocalProxy] = localCluster
	user := clientcmdapi.NewAuthInfo()
	user.ClientCertificateData = clientCertBytes
	user.ClientKeyData = clientKeyBytes
	developmentKubeConfig.AuthInfos[kubeContextClusterProxy] = user
	kubeCtx := clientcmdapi.NewContext()
	kubeCtx.Cluster = kubeContextClusterProxy
	kubeCtx.AuthInfo = kubeContextClusterProxy
	developmentKubeConfig.Contexts[kubeContextClusterProxy] = kubeCtx
	developmentKubeConfig.CurrentContext = kubeContextClusterProxy
	localCtx := clientcmdapi.NewContext()
	localCtx.Cluster = kubeContextLocalProxy
	localCtx.AuthInfo = kubeContextClusterProxy
	developmentKubeConfig.Contexts[kubeContextLocalProxy] = localCtx

	// add admin config to the same kubeconfig (easier to switch)
	adminConfig, err := clientcmd.LoadFromFile(kubeconfigPath)
	if err != nil {
		return err
	}

	developmentKubeConfig.Clusters[kubeContextUpstreamCluster] = adminConfig.Clusters[kubeCfgClusterName]
	developmentKubeConfig.AuthInfos[kubeContextUpstreamCluster] = adminConfig.AuthInfos[kubeCfgClusterName]
	adminCtx := clientcmdapi.NewContext()
	adminCtx.Cluster = kubeContextUpstreamCluster
	adminCtx.AuthInfo = kubeContextUpstreamCluster
	developmentKubeConfig.Contexts[kubeContextUpstreamCluster] = adminCtx

	if err := clientcmd.WriteToFile(*developmentKubeConfig, developmentKubeConfigFileName); err != nil {
		return err
	}

	fmt.Println("development kubeconfig generated as " + developmentKubeConfigFileName)
	return nil
}

// getBytesFromSecretField retrieves the data bytes from a secret with the speficied namespace, secret name and field names.
// Since all certs are generated in the development kind cluster by certmanager, this is used to retrieve what's needed
// to generate both proxy and development kubeconfigs
func getBytesFromSecretField(ctx context.Context, clientset *kubernetes.Clientset, namespace, secretName, field string) ([]byte, error) {
	secret, err := clientset.CoreV1().
		Secrets(namespace).
		Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	fieldData, ok := secret.Data[field]
	if !ok {
		return nil, fmt.Errorf("could not get %s from secret %s", field, secret)
	}
	return fieldData, nil
}

// Clean deletes the dev stack cluster.
func (d Dev) Clean() error {
	provider := kind.NewProvider(
		kind.ProviderWithLogger(cmd.NewLogger()),
	)
	if err := provider.Delete(clusterName, ""); err != nil {
		return err
	}
	_ = os.Remove(kubeconfigPath)
	_ = os.Remove(developmentKubeConfigFileName)
	_ = os.Remove("client-ca.crt")
	_ = os.Remove("client-cert.crt")
	return nil
}

// updateKustomizationImageTags is used to update the development kustomization with a newly built proxy image SHA
func updateKustomizationImageTags(image string) error {
	kustomizeFile, err := os.Open("deploy/kustomization.yaml")
	if err != nil {
		return err
	}

	decoder := utilyaml.NewYAMLOrJSONDecoder(kustomizeFile, 100)
	var kustomization types.Kustomization
	if err := decoder.Decode(&kustomization); err != nil {
		return err
	}
	if err := kustomizeFile.Close(); err != nil {
		return err
	}
	newName, newTag, ok := strings.Cut(image, ":")
	if !ok {
		return fmt.Errorf("error splitting image name: %s", image)
	}
	kustomization.Images[0].NewTag = newTag
	kustomization.Images[0].NewName = newName
	kustomizationBytes, err := yaml.Marshal(kustomization)
	if err != nil {
		return err
	}

	return os.WriteFile("deploy/kustomization.yaml", kustomizationBytes, 0o600)
}

// writeUpstreamKubeconfigAsSecret writes the generated Kind cluster kubeconfig into the cluster as a Secret. This
// is necessary for the proxy to be able to auth against the upstream Kube API
func writeUpstreamKubeconfigAsSecret(ctx context.Context, kubeconfigPath string) error {
	clientset, err := clientSetForKubeConfig(kubeconfigPath)
	if err != nil {
		return err
	}

	// create kubeconfig secret
	kubeconfigBytes, err := os.ReadFile(kubeconfigPath)
	if err != nil {
		return err
	}
	_, err = clientset.CoreV1().Namespaces().Apply(ctx,
		applycorev1.Namespace("spicedb-kubeapi-proxy"),
		metav1.ApplyOptions{FieldManager: "localdev", Force: true})
	if err != nil {
		return err
	}

	_, err = clientset.CoreV1().Secrets("spicedb-kubeapi-proxy").Apply(ctx,
		applycorev1.Secret("rebac-proxy-kubeconfig", "spicedb-kubeapi-proxy").
			WithStringData(map[string]string{
				"kubeconfig": string(kubeconfigBytes),
			}),
		metav1.ApplyOptions{FieldManager: "localdev", Force: true})

	return err
}

// clientSetForKubeConfig returns a kube client based on a provided kubeconfig path
func clientSetForKubeConfig(kubeconfigPath string) (*kubernetes.Clientset, error) {
	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, err
	}

	// create the clientset
	return kubernetes.NewForConfig(config)
}
