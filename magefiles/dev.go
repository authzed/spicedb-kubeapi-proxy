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
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"golang.org/x/mod/sumdb/dirhash"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/kind/pkg/cmd"
	dockerimageload "sigs.k8s.io/kind/pkg/cmd/kind/load/docker-image"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/yaml"
)

type Dev mg.Namespace

const proxyNodePort = 30443

func (Dev) Bootstrap(ctx context.Context) error {
	clusterName := "rebac-proxy-dev"

	var kubeconfigPath string
	var proxyHostPort int32
	if _, err := os.Stat(clusterName + ".kubeconfig"); err != nil {
		proxyHostPort, err = GetFreePort("localhost")
		if err != nil {
			return err
		}

		kubeconfigPath, _, _, err = provisionKind(clusterName, nil, nil, kindConfig(map[int32]int32{proxyHostPort: proxyNodePort}))
		if err != nil {
			return err
		}

		fmt.Println(kubeconfigPath)
	}

	imageName, err := updateKustomizationImageTags()
	if err != nil {
		return err
	}
	if err := sh.RunV("docker", "build", "-t", imageName, "."); err != nil {
		return err
	}
	loadCmd := dockerimageload.NewCommand(cmd.NewLogger(), cmd.StandardIOStreams())
	if err := loadCmd.Flags().Set("name", clusterName); err != nil {
		return err
	}
	if err := loadCmd.RunE(loadCmd, []string{imageName}); err != nil {
		return err
	}

	if _, err := os.Stat("rebac-proxy.kubeconfig"); err != nil {
		if err := generateCertsAndKubeConfig(ctx, kubeconfigPath, proxyHostPort); err != nil {
			return err
		}
	}

	return sh.RunV("kustomizer", "apply", "inventory", "rebac", "-k", "./deploy", "--prune", "--wait")
}

func updateKustomizationImageTags() (string, error) {
	cmdHash, err := dirhash.HashDir("cmd", "devcmd", dirhash.DefaultHash)
	if err != nil {
		return "", err
	}
	pkgHash, err := dirhash.HashDir("pkg", "devpkg", dirhash.DefaultHash)
	if err != nil {
		return "", err
	}
	tag := xxhash.Sum64String(cmdHash + pkgHash)

	kustomizeFile, err := os.Open("deploy/kustomization.yaml")
	if err != nil {
		return "", err
	}

	decoder := utilyaml.NewYAMLOrJSONDecoder(kustomizeFile, 100)
	var kustomization types.Kustomization
	if err := decoder.Decode(&kustomization); err != nil {
		return "", err
	}
	if err := kustomizeFile.Close(); err != nil {
		return "", err
	}
	kustomization.Images[0].NewTag = fmt.Sprintf("%x", tag)
	kustomizationBytes, err := yaml.Marshal(kustomization)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("authzed/kube-rebac-proxy:%x", tag), os.WriteFile("deploy/kustomization.yaml", kustomizationBytes, 0o600)
}

func generateCertsAndKubeConfig(ctx context.Context, kubeconfigPath string, proxyHostPort int32) error {
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

	proxyConfig := clientcmdapi.NewConfig()
	cluster := clientcmdapi.NewCluster()
	cluster.CertificateAuthorityData = serverCaPem.Bytes()
	cluster.Server = fmt.Sprintf("https://127.0.0.1:%d", proxyHostPort)
	proxyConfig.Clusters["rebac-proxy"] = cluster
	user := clientcmdapi.NewAuthInfo()
	user.ClientCertificateData = clientCertPem.Bytes()
	user.ClientKeyData = clientKeyPem.Bytes()
	proxyConfig.AuthInfos["rebac-proxy"] = user
	kubeCtx := clientcmdapi.NewContext()
	kubeCtx.Cluster = "rebac-proxy"
	kubeCtx.AuthInfo = "rebac-proxy"
	proxyConfig.Contexts["rebac-proxy"] = kubeCtx
	proxyConfig.CurrentContext = "rebac-proxy"
	if err := clientcmd.WriteToFile(*proxyConfig, "rebac-proxy.kubeconfig"); err != nil {
		return err
	}
	return nil
}

// // Cert generates local certs and ensures they are trusted by local trust stores.
// func (d Dev) Cert() error {
// 	if certsInstalled() {
// 		return nil
// 	}
// 	fmt.Println("dev certs not found")
// 	return d.Regen_cert()
// }
//
// // Regen_cert regenerates local certs even if they already exist.
// func (Dev) Regen_cert() error {
// 	fmt.Println("generating certs...")
//
// 	certDir, err := outputDir("magefiles", "go", "run", "filippo.io/mkcert", "-CAROOT")
// 	if err != nil {
// 		return err
// 	}
//
// 	if err := runDirV("magefiles", "sudo", "go", "run", "filippo.io/mkcert", "-install"); err != nil {
// 		return err
// 	}
//
// 	for src, dst := range map[string]string{
// 		filepath.Join(certDir, "rootCA-key.pem"): devTLSKeyPath,
// 		filepath.Join(certDir, "rootCA.pem"):     devTLSCertPath,
// 	} {
// 		data, err := os.ReadFile(src)
// 		if err != nil {
// 			return err
// 		}
// 		err = os.WriteFile(dst, data, 0o600)
// 		if err != nil {
// 			return fmt.Errorf("you may need to run with sudo, i.e. `mage dev:regen_cert`: %w", err)
// 		}
// 	}
// 	return nil
// }
//
// func certsInstalled() bool {
// 	_, err := os.Stat(devTLSCertPath)
// 	if err != nil {
// 		return false
// 	}
// 	_, err = os.Stat(devTLSKeyPath)
// 	return err == nil
// }
//
// func genTrust(ctx context.Context, kubeconfigPath string) error {
// 	req := csr.CertificateRequest{
// 		KeyRequest: csr.NewKeyRequest(),
// 		Hosts: []string{
// 			"rebac-proxy.kube-system.svc.cluster.local",
// 			"127.0.0.1",
// 		},
// 		CN: "rebac-proxy.kube-system.svc.cluster.local",
// 	}
//
// 	var key, csrPEM []byte
// 	g := &csr.Generator{Validator: genkey.Validator}
// 	csrPEM, key, err := g.ProcessRequest(&req)
// 	if err != nil {
// 		key = nil
// 		return err
// 	}
//
// 	// use the current context in kubeconfig
// 	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
// 	if err != nil {
// 		return err
// 	}
//
// 	// create the clientset
// 	clientset, err := kubernetes.NewForConfig(config)
// 	if err != nil {
// 		return err
// 	}
//
// 	kubeCSR, err := clientset.CertificatesV1().CertificateSigningRequests().Create(ctx, &certv1.CertificateSigningRequest{
// 		ObjectMeta: metav1.ObjectMeta{Name: "rebac-proxy.kube-system"},
// 		Spec: certv1.CertificateSigningRequestSpec{
// 			Request: []byte(base64.StdEncoding.EncodeToString(csrPEM)),
// 			Usages: []certv1.KeyUsage{
// 				certv1.UsageDigitalSignature,
// 				certv1.UsageKeyEncipherment,
// 				certv1.UsageServerAuth,
// 			},
// 		},
// 	}, metav1.CreateOptions{})
// 	if err != nil {
// 		return err
// 	}
// 	kubeCSR.Status.Conditions = append(kubeCSR.Status.Conditions, certv1.CertificateSigningRequestCondition{Type: certv1.CertificateApproved})
// 	kubeCSR, err = clientset.CertificatesV1().CertificateSigningRequests().UpdateApproval(ctx, kubeCSR.Name, kubeCSR, metav1.UpdateOptions{})
// 	if err != nil {
// 		return err
// 	}
//
// 	for len(kubeCSR.Status.Certificate) == 0 {
// 		fmt.Println("waiting for cert")
// 		kubeCSR, err = clientset.CertificatesV1().CertificateSigningRequests().Get(ctx, kubeCSR.Name, metav1.GetOptions{})
// 		if err != nil {
// 			return err
// 		}
// 	}
//
// 	_, err = clientset.CoreV1().Secrets("kube-system").Create(ctx, &corev1.Secret{
// 		ObjectMeta: metav1.ObjectMeta{Name: "rebac-proxy-tls", Namespace: "kube-system"},
// 		StringData: map[string]string{
// 			"cert": string(kubeCSR.Status.Certificate),
// 			"key":  string(key),
// 		},
// 	}, metav1.CreateOptions{})
//
// 	return nil
// }
