//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/gob"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/go-logr/zapr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/afero"
	"go.uber.org/zap"
	"k8s.io/client-go/discovery/cached/disk"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/kubernetes/pkg/controller/garbagecollector"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/tools/setup-envtest/env"
	"sigs.k8s.io/controller-runtime/tools/setup-envtest/remote"
	"sigs.k8s.io/controller-runtime/tools/setup-envtest/store"
	"sigs.k8s.io/controller-runtime/tools/setup-envtest/versions"
	"sigs.k8s.io/controller-runtime/tools/setup-envtest/workflows"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy/distributedtx"
)

var (
	testEnv  *envtest.Environment
	proxySrv *proxy.Server

	// adminUser is configured for the un-proxied apiserver
	adminUser *envtest.AuthenticatedUser

	// clientCA represents the proxy's CA cert used to authenticate client
	// certs. It has methods that can be used to stamp out client certs for
	// a user.
	clientCA *ClientCA
)

func TestEndToEnd(t *testing.T) {
	RunSpecs(t, "proxy tests")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	RegisterFailHandler(Fail)
	SetDefaultEventuallyTimeout(1 * time.Minute)
	SetDefaultEventuallyPollingInterval(1 * time.Second)
	SetDefaultConsistentlyDuration(5 * time.Second)
	SetDefaultConsistentlyPollingInterval(100 * time.Millisecond)

	// this runs only once, no matter how many processes are running tests
	testEnv = &envtest.Environment{
		ControlPlaneStopTimeout: 3 * time.Minute,
	}

	ConfigureApiserver()

	config, err := testEnv.Start()
	Expect(err).To(Succeed())
	DeferCleanup(testEnv.Stop)
	DisableClientRateLimits(config)

	ctx, cancel := context.WithCancel(context.Background())
	DeferCleanup(cancel)
	StartKubeGC(ctx, config)

	// TODO: bind cluster-admin to admin user

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	Expect(enc.Encode(config)).To(Succeed())
	return buf.Bytes()
}, func(ctx context.Context, rc []byte) {
	dec := gob.NewDecoder(bytes.NewReader(rc))
	var config rest.Config
	Expect(dec.Decode(&config)).To(Succeed())

	user, err := testEnv.AddUser(
		envtest.User{Name: "admin", Groups: []string{"system:masters"}},
		&config,
	)
	Expect(err).To(Succeed())
	adminUser = user

	kubecfgBytes, err := adminUser.KubeConfig()
	Expect(err).To(Succeed())
	backendCfg, err := clientcmd.Load(kubecfgBytes)
	Expect(err).To(Succeed())

	port, err := GetFreePort("localhost")
	Expect(err).To(Succeed())
	clientCA = GenerateClientCA(port)

	opts := proxy.NewOptions()
	opts.BackendConfig = backendCfg
	opts.RuleConfigFile = "rules.yaml"
	opts.SecureServing.BindPort = port
	opts.SpiceDBEndpoint = proxy.EmbeddedSpiceDBEndpoint
	opts.SecureServing.BindAddress = net.ParseIP("127.0.0.1")
	opts.Authentication.BuiltInOptions.ClientCert.ClientCA = clientCA.Path()
	Expect(opts.Complete(ctx)).To(Succeed())
	proxySrv, err = proxy.NewServer(ctx, *opts)
	Expect(err).To(Succeed())

	// speed up backoff for tests
	distributedtx.KubeBackoff.Duration = 1 * time.Microsecond

	ctx, cancel := context.WithCancel(context.Background())
	DeferCleanup(cancel)
	go func() {
		defer GinkgoRecover()
		Expect(proxySrv.Run(ctx))
	}()
})

func ConfigureApiserver() {
	logCfg := zap.NewDevelopmentConfig()
	logCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	zapLog, err := logCfg.Build()
	Expect(err).To(Succeed())
	log := zapr.NewLogger(zapLog)

	e := &env.Env{
		Log: log,
		Client: &remote.Client{
			Log:    log,
			Bucket: "kubebuilder-tools",
			Server: "storage.googleapis.com",
		},
		Version: versions.Spec{
			Selector:    versions.TildeSelector{},
			CheckLatest: false,
		},
		VerifySum:     true,
		ForceDownload: false,
		Platform: versions.PlatformItem{
			Platform: versions.Platform{
				OS:   runtime.GOOS,
				Arch: runtime.GOARCH,
			},
		},
		FS:    afero.Afero{Fs: afero.NewOsFs()},
		Store: store.NewAt("../testbin"),
		Out:   os.Stdout,
	}
	e.Version, err = versions.FromExpr("~1.27.0")
	Expect(err).To(Succeed())

	workflows.Use{
		UseEnv:      true,
		PrintFormat: env.PrintOverview,
		AssetsPath:  "../testbin",
	}.Do(e)

	Expect(os.Setenv("KUBEBUILDER_ASSETS", fmt.Sprintf("../testbin/k8s/%s-%s-%s", e.Version.AsConcrete(), e.Platform.OS, e.Platform.Arch))).To(Succeed())
	DeferCleanup(os.Unsetenv, "KUBEBUILDER_ASSETS")
}

func StartKubeGC(ctx context.Context, restConfig *rest.Config) {
	kclient, err := kubernetes.NewForConfig(restConfig)
	Expect(err).To(Succeed())
	mclient, err := metadata.NewForConfig(restConfig)
	Expect(err).To(Succeed())

	cacheDir, err := os.MkdirTemp("", "kubecache")
	Expect(err).To(Succeed())
	httpCacheDir := filepath.Join(cacheDir, "http")
	discoveryCacheDir := filepath.Join(cacheDir, "discovery")
	cachedDiscovery, err := disk.NewCachedDiscoveryClientForConfig(restConfig, discoveryCacheDir, httpCacheDir, 5*time.Minute)
	Expect(err).To(Succeed())

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(cachedDiscovery)

	sharedInformers := informers.NewSharedInformerFactory(kclient, 0)
	metadataInformers := metadatainformer.NewSharedInformerFactory(mclient, 0)

	started := make(chan struct{})
	gcController, err := garbagecollector.NewGarbageCollector(kclient, mclient, mapper, nil, informerfactory.NewInformerFactory(sharedInformers, metadataInformers), started)
	Expect(err).To(Succeed())

	sharedInformers.Start(ctx.Done())
	metadataInformers.Start(ctx.Done())
	sharedInformers.WaitForCacheSync(ctx.Done())
	metadataInformers.WaitForCacheSync(ctx.Done())
	close(started)

	go gcController.Run(ctx, 1)
	go gcController.Sync(ctx, cachedDiscovery, 30*time.Second)
}

// DisableClientRateLimits removes rate limiting against the apiserver
func DisableClientRateLimits(restConfig *rest.Config) {
	restConfig.Burst = 2000
	restConfig.QPS = -1
}

// GetFreePort is a helper used to get a free TCP port on the host
func GetFreePort(listenAddr string) (int, error) {
	dummyListener, err := net.Listen("tcp", net.JoinHostPort(listenAddr, "0"))
	if err != nil {
		return 0, err
	}
	defer dummyListener.Close()
	port := dummyListener.Addr().(*net.TCPAddr).Port
	return port, nil
}

// ClientCA is a helper type that can stamp out user-specific client certs
// for tests.
type ClientCA struct {
	host       string
	port       int
	caCert     *x509.Certificate
	privateKey *ecdsa.PrivateKey
	filePath   string
}

func GenerateClientCA(port int) *ClientCA {
	clientCA := &x509.Certificate{
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 1, 1),
		SerialNumber:          big.NewInt(0),
		Subject:               pkix.Name{Organization: []string{"spicedb kubeapi proxy"}, CommonName: "spicedb kubeapi proxy"},
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	clientCaPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	Expect(err).To(Succeed())
	clientCaPublicKey := &clientCaPrivateKey.PublicKey
	clientCaCertBytes, err := x509.CreateCertificate(rand.Reader, clientCA, clientCA, clientCaPublicKey, clientCaPrivateKey)
	Expect(err).To(Succeed())
	clientCaCert, err := x509.ParseCertificate(clientCaCertBytes)
	Expect(err).To(Succeed())

	var clientCaPem bytes.Buffer
	err = pem.Encode(&clientCaPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: clientCaCert.Raw,
	})
	Expect(err).To(Succeed())

	dir := GinkgoT().TempDir()
	caPath := path.Join(dir, "client-ca.crt")

	Expect(os.WriteFile(caPath, clientCaPem.Bytes(), 0o600)).To(Succeed())

	return &ClientCA{
		host:       "localhost",
		port:       port,
		caCert:     clientCaCert,
		privateKey: clientCaPrivateKey,
		filePath:   caPath,
	}
}

func (c *ClientCA) Path() string {
	return c.filePath
}

// GenerateClientCerts generates certs is used to authenticate a proxy client
// with the proxy. The `CommonName` of the cert is treated as the username.
// It returns a kube config with the client certs and auth info for the user.
func (c *ClientCA) GenerateUserConfig(user string) *clientcmdapi.Config {
	userCertData := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"authzed"},
			CommonName:   user,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 1, 1),
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		DNSNames:              []string{user},
	}
	rootUserPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	Expect(err).To(Succeed())
	rootUserPublicKey := &rootUserPrivateKey.PublicKey
	rootUserCertBytes, err := x509.CreateCertificate(rand.Reader, userCertData, c.caCert, rootUserPublicKey, c.privateKey)
	Expect(err).To(Succeed())
	rootUserCert, err := x509.ParseCertificate(rootUserCertBytes)
	Expect(err).To(Succeed())

	var clientKeyPem bytes.Buffer
	rootKeyBytes, err := x509.MarshalECPrivateKey(rootUserPrivateKey)
	Expect(err).To(Succeed())
	err = pem.Encode(&clientKeyPem, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: rootKeyBytes,
	})
	Expect(err).To(Succeed())

	var clientCertPem bytes.Buffer
	err = pem.Encode(&clientCertPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: rootUserCert.Raw,
	})
	Expect(err).To(Succeed())

	proxyConfig := clientcmdapi.NewConfig()
	cluster := clientcmdapi.NewCluster()
	// TODO: instead of hardcoding this path, just generate serving certs
	// in a tempdir and pass the path. This is where the apiserver generates
	// self-signed certs if you don't pass any in.
	cluster.CertificateAuthority = "apiserver.local.config/certificates/tls.crt"
	cluster.Server = fmt.Sprintf("https://%s:%d", c.host, c.port)
	proxyConfig.Clusters[user] = cluster
	userInfo := clientcmdapi.NewAuthInfo()
	userInfo.ClientCertificateData = clientCertPem.Bytes()
	userInfo.ClientKeyData = clientKeyPem.Bytes()
	proxyConfig.AuthInfos[user] = userInfo
	kubeCtx := clientcmdapi.NewContext()
	kubeCtx.Cluster = user
	kubeCtx.AuthInfo = user
	proxyConfig.Contexts[user] = kubeCtx
	proxyConfig.CurrentContext = user
	return proxyConfig
}
