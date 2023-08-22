//go:build mage
// +build mage

package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	_ "embed"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"strings"
	"time"

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
	proxyNodePort           = 30443
	clusterName             = "spicedb-kubeapi-proxy"
	kubeCfgClusterName      = "kind-" + clusterName
	kubeconfigPath          = clusterName + ".kubeconfig"
	generatedKubeConfigPath = "proxy.kubeconfig"
)

// Up brings up a dev cluster with the proxy installed
func (Dev) Up(ctx context.Context) error {
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
		fmt.Println(kubeconfigPath)
	}
	imageName, err := makeGoKindImage(ctx, clusterName, "spicedb-kubeapi-proxy", ".", "./cmd/spicedb-kubeapi-proxy")
	if err != nil {
		return err
	}
	if err := updateKustomizationImageTags(imageName); err != nil {
		return err
	}

	if _, err := os.Stat(generatedKubeConfigPath); err != nil {
		fmt.Println("no proxy kubeconfig found, generating")
		if err := generateCertsAndKubeConfig(ctx, kubeconfigPath, proxyHostPort); err != nil {
			return err
		}
	}

	return sh.RunV("kustomizer", "apply", "inventory", "spicedb-kubeapi-proxy", "-k", "./deploy", "--prune", "--wait")
}

// Clean deletes the dev stack cluster
func (d Dev) Clean() error {
	provider := kind.NewProvider(
		kind.ProviderWithLogger(cmd.NewLogger()),
	)
	if err := provider.Delete(clusterName, ""); err != nil {
		return err
	}
	_ = os.Remove(kubeconfigPath)
	return nil
}

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

func generateCertsAndKubeConfig(ctx context.Context, kubeconfigPath string, proxyHostPort int32) error {
	fmt.Println("generating certs")
	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return err
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	// create kubeconfig secret
	kubeconfigBytes, err := os.ReadFile(kubeconfigPath)
	if err != nil {
		return err
	}
	_, err = clientset.CoreV1().Secrets("kube-system").Apply(ctx,
		applycorev1.Secret("rebac-proxy-kubeconfig", "kube-system").
			WithStringData(map[string]string{
				"kubeconfig": string(kubeconfigBytes),
			}),
		metav1.ApplyOptions{FieldManager: "localdev", Force: true})
	if err != nil {
		return err
	}

	// create the serving CA - this will sign the proxy's TLS certs and is
	// what goes in the CA data in the proxy's kubeconfig entry
	serverCA := &x509.Certificate{
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 1, 1),
		SerialNumber:          big.NewInt(0),
		Subject:               pkix.Name{Organization: []string{"authzed rebac proxy"}, CommonName: "authzed rebac"},
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	serverCaPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serverCaPublicKey := &serverCaPrivateKey.PublicKey
	serverCaCertBytes, err := x509.CreateCertificate(rand.Reader, serverCA, serverCA, serverCaPublicKey, serverCaPrivateKey)
	if err != nil {
		return err
	}
	serverCaCert, err := x509.ParseCertificate(serverCaCertBytes)
	if err != nil {
		return err
	}

	var serverCaPem bytes.Buffer
	err = pem.Encode(&serverCaPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serverCaCert.Raw,
	})
	if err != nil {
		return err
	}

	// create the server keypair, the certs the proxy uses to serve requests
	serverCertData := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 1, 1),
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", "node"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverCertPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serverCertPublicKey := &serverCertPrivateKey.PublicKey
	serverCertBytes, err := x509.CreateCertificate(rand.Reader, serverCertData, serverCaCert, serverCertPublicKey, serverCaPrivateKey)
	if err != nil {
		return err
	}
	serverCert, err := x509.ParseCertificate(serverCertBytes)
	if err != nil {
		return err
	}

	serverKeyBytes, err := x509.MarshalECPrivateKey(serverCertPrivateKey)
	if err != nil {
		return err
	}

	var serverKeyPem bytes.Buffer
	err = pem.Encode(&serverKeyPem, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: serverKeyBytes,
	})
	if err != nil {
		return err
	}

	var serverCertPem bytes.Buffer
	err = pem.Encode(&serverCertPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serverCert.Raw,
	})
	if err != nil {
		return err
	}

	_, err = clientset.CoreV1().Secrets("kube-system").Apply(ctx,
		applycorev1.Secret("rebac-proxy-tls", "kube-system").
			WithStringData(map[string]string{
				"proxy.crt": serverCertPem.String(),
				"proxy.key": serverKeyPem.String(),
			}),
		metav1.ApplyOptions{FieldManager: "localdev", Force: true})
	if err != nil {
		return err
	}

	// create the client CA - this is used to authenticate proxy users using
	// client cert auth
	clientCA := &x509.Certificate{
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 1, 1),
		SerialNumber:          big.NewInt(0),
		Subject:               pkix.Name{Organization: []string{"authzed rebac proxy"}, CommonName: "authzed rebac"},
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	clientCaPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	clientCaPublicKey := &clientCaPrivateKey.PublicKey
	clientCaCertBytes, err := x509.CreateCertificate(rand.Reader, clientCA, clientCA, clientCaPublicKey, clientCaPrivateKey)
	if err != nil {
		return err
	}
	clientCaCert, err := x509.ParseCertificate(clientCaCertBytes)
	if err != nil {
		return err
	}

	var clientCaPem bytes.Buffer
	err = pem.Encode(&clientCaPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: clientCaCert.Raw,
	})
	if err != nil {
		return err
	}

	_, err = clientset.CoreV1().Secrets("kube-system").Apply(ctx,
		applycorev1.Secret("rebac-proxy-request-client-ca", "kube-system").
			WithStringData(map[string]string{
				"ca.crt": clientCaPem.String(),
			}),
		metav1.ApplyOptions{FieldManager: "localdev", Force: true})
	if err != nil {
		return err
	}

	// generate client certs - this is used to authenticate a proxy client
	// with the proxy. The `CommonName` of the cert is treated as the username.
	rakisUserCertData := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"authzed"},
			CommonName:   "rakis",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 1, 1),
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		DNSNames:              []string{"rakis"},
	}
	rootUserPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	rootUserPublicKey := &rootUserPrivateKey.PublicKey
	rootUserCertBytes, err := x509.CreateCertificate(rand.Reader, rakisUserCertData, clientCaCert, rootUserPublicKey, clientCaPrivateKey)
	if err != nil {
		return err
	}
	rootUserCert, err := x509.ParseCertificate(rootUserCertBytes)
	if err != nil {
		return err
	}

	var clientKeyPem bytes.Buffer
	rootKeyBytes, err := x509.MarshalECPrivateKey(rootUserPrivateKey)
	if err != nil {
		return err
	}
	err = pem.Encode(&clientKeyPem, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: rootKeyBytes,
	})
	if err != nil {
		return err
	}

	var clientCertPem bytes.Buffer
	err = pem.Encode(&clientCertPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: rootUserCert.Raw,
	})
	if err != nil {
		return err
	}

	const (
		proxyCtxName = "proxy"
		adminCtxName = "admin"
	)
	proxyConfig := clientcmdapi.NewConfig()
	cluster := clientcmdapi.NewCluster()
	cluster.CertificateAuthorityData = serverCaPem.Bytes()
	cluster.Server = fmt.Sprintf("https://127.0.0.1:%d", proxyHostPort)
	proxyConfig.Clusters[proxyCtxName] = cluster
	user := clientcmdapi.NewAuthInfo()
	user.ClientCertificateData = clientCertPem.Bytes()
	user.ClientKeyData = clientKeyPem.Bytes()
	proxyConfig.AuthInfos[proxyCtxName] = user
	kubeCtx := clientcmdapi.NewContext()
	kubeCtx.Cluster = proxyCtxName
	kubeCtx.AuthInfo = proxyCtxName
	proxyConfig.Contexts[proxyCtxName] = kubeCtx
	proxyConfig.CurrentContext = proxyCtxName

	// add admin config to the same kubeconfig (easier to switch)
	adminConfig, err := clientcmd.LoadFromFile(kubeconfigPath)
	if err != nil {
		return err
	}
	proxyConfig.Clusters[adminCtxName] = adminConfig.Clusters[kubeCfgClusterName]
	proxyConfig.AuthInfos[adminCtxName] = adminConfig.AuthInfos[kubeCfgClusterName]
	adminCtx := clientcmdapi.NewContext()
	adminCtx.Cluster = adminCtxName
	adminCtx.AuthInfo = adminCtxName
	proxyConfig.Contexts[adminCtxName] = adminCtx

	if err := clientcmd.WriteToFile(*proxyConfig, generatedKubeConfigPath); err != nil {
		return err
	}
	return nil
}
