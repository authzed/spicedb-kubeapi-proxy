package proxy

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

func TestRequestHeaderAuth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	userInfo := runProxyRequest(t, ctx, map[string][]string{
		"X-Remote-User":  {"paul"},
		"X-Remote-Group": {"muaddib"},
	})

	require.Equal(t, "paul", userInfo.GetName())
	require.Equal(t, "muaddib", userInfo.GetGroups()[0])
}

// headerAdder is an http.RoundTripper that adds additional headers to the request
type headerAdder struct {
	headers map[string][]string

	rt http.RoundTripper
}

func (h *headerAdder) RoundTrip(req *http.Request) (*http.Response, error) {
	for k, vv := range h.headers {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	return h.rt.RoundTrip(req)
}

func runProxyRequest(t testing.TB, ctx context.Context, headers map[string][]string) *user.DefaultInfo {
	port, err := GetFreePort("localhost")
	require.NoError(t, err)

	certStore := GenerateCertStore(t, port)

	clientConfig, err := clientcmd.NewDefaultClientConfig(
		*certStore.GenerateUserConfig("service"), nil,
	).ClientConfig()
	require.NoError(t, err)

	wt := clientConfig.WrapTransport
	clientConfig.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		if wt != nil {
			rt = wt(rt)
		}
		return &headerAdder{
			headers: headers,
			rt:      rt,
		}
	}
	require.NoError(t, err)
	client, err := kubernetes.NewForConfig(clientConfig)
	require.NoError(t, err)

	headerOpts := &options.RequestHeaderAuthenticationOptions{
		ClientCAFile:        certStore.Path(),
		UsernameHeaders:     []string{"X-Remote-User"},
		GroupHeaders:        []string{"X-Remote-Group"},
		ExtraHeaderPrefixes: []string{"X-Remote-Extra-"},
		AllowedNames:        []string{"service"},
	}

	opts := NewOptions()
	opts.RestConfigFunc = func() (*rest.Config, http.RoundTripper, error) {
		ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprintf(w, `{
				"apiVersion": "v1",
				"data": {},
				"kind": "ConfigMap",
				"metadata": {
					"name": "auth",
					"namespace": "ns"
				}
			}`)
		}))
		ts.EnableHTTP2 = true
		ts.StartTLS()
		t.Cleanup(ts.Close)
		rc := &rest.Config{
			Host: ts.URL,
			TLSClientConfig: rest.TLSClientConfig{
				Insecure: true,
			},
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AddCert(ts.Certificate())

		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{
			RootCAs: caCertPool,
		}

		return rc, transport, nil
	}
	opts.SpiceDBEndpoint = EmbeddedSpiceDBEndpoint
	opts.SecureServing.ServerCert.CertKey = certStore.servingCertKey
	opts.SecureServing.BindAddress = net.ParseIP("127.0.0.1")
	opts.SecureServing.BindPort = port
	opts.Authentication.BuiltInOptions.RequestHeader = headerOpts
	opts.Matcher = rules.MatcherFunc(func(match *request.RequestInfo) []*rules.RunnableRule {
		return []*rules.RunnableRule{{
			Checks: []*rules.RelExpr{},
		}}
	})

	var info user.Info
	opts.inputExtractor = rules.ResolveInputExtractorFunc(func(req *http.Request) (*rules.ResolveInput, error) {
		userInfo, ok := request.UserFrom(req.Context())
		if !ok {
			return nil, fmt.Errorf("unable to get user info from request")
		}
		info = userInfo
		return rules.NewResolveInputFromHttp(req)
	})

	require.NoError(t, opts.Complete(ctx))
	proxySrv, err := NewServer(ctx, *opts)
	require.NoError(t, err)

	go func() {
		proxySrv.Run(ctx)
	}()

	_, err = client.CoreV1().ConfigMaps("ns").Get(ctx, "auth", metav1.GetOptions{})
	require.NoError(t, err)
	return info.(*user.DefaultInfo)
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

// CertStore is a helper type that can stamp out user-specific client certs
// for tests.
type CertStore struct {
	t              testing.TB
	host           string
	port           int
	servingCABytes []byte
	clientCaCert   *x509.Certificate
	privateKey     *ecdsa.PrivateKey
	servingCertKey options.CertKey
	filePath       string
}

func GenerateCertStore(t testing.TB, port int) *CertStore {
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
	require.NoError(t, err)
	clientCaPublicKey := &clientCaPrivateKey.PublicKey
	clientCaCertBytes, err := x509.CreateCertificate(rand.Reader, clientCA, clientCA, clientCaPublicKey, clientCaPrivateKey)
	require.NoError(t, err)
	clientCaCert, err := x509.ParseCertificate(clientCaCertBytes)
	require.NoError(t, err)

	var clientCaPem bytes.Buffer
	err = pem.Encode(&clientCaPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: clientCaCert.Raw,
	})
	require.NoError(t, err)

	dir := t.TempDir()
	caPath := path.Join(dir, "client-ca.crt")

	require.NoError(t, os.WriteFile(caPath, clientCaPem.Bytes(), 0o600))

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
	require.NoError(t, err)
	serverCaPublicKey := &serverCaPrivateKey.PublicKey
	serverCaCertBytes, err := x509.CreateCertificate(rand.Reader, serverCA, serverCA, serverCaPublicKey, serverCaPrivateKey)
	require.NoError(t, err)

	serverCaCert, err := x509.ParseCertificate(serverCaCertBytes)
	require.NoError(t, err)

	var serverCaPem bytes.Buffer
	err = pem.Encode(&serverCaPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serverCaCert.Raw,
	})
	require.NoError(t, err)

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
	require.NoError(t, err)

	serverCertPublicKey := &serverCertPrivateKey.PublicKey
	serverCertBytes, err := x509.CreateCertificate(rand.Reader, serverCertData, serverCaCert, serverCertPublicKey, serverCaPrivateKey)
	require.NoError(t, err)

	serverCert, err := x509.ParseCertificate(serverCertBytes)
	require.NoError(t, err)

	serverKeyBytes, err := x509.MarshalECPrivateKey(serverCertPrivateKey)
	require.NoError(t, err)

	var serverKeyPem bytes.Buffer
	err = pem.Encode(&serverKeyPem, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: serverKeyBytes,
	})
	require.NoError(t, err)

	servingDir := t.TempDir()
	proxyKeyPath := path.Join(servingDir, "proxy.key")
	require.NoError(t, os.WriteFile(proxyKeyPath, serverKeyPem.Bytes(), 0o600))

	var serverCertPem bytes.Buffer
	err = pem.Encode(&serverCertPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serverCert.Raw,
	})
	require.NoError(t, err)

	proxyCertPath := path.Join(servingDir, "proxy.crt")
	require.NoError(t, os.WriteFile(proxyCertPath, serverCertPem.Bytes(), 0o600))

	return &CertStore{
		host:           "localhost",
		port:           port,
		clientCaCert:   clientCaCert,
		privateKey:     clientCaPrivateKey,
		filePath:       caPath,
		servingCABytes: serverCaPem.Bytes(),
		servingCertKey: options.CertKey{
			CertFile: proxyCertPath,
			KeyFile:  proxyKeyPath,
		},
	}
}

func (c *CertStore) Path() string {
	return c.filePath
}

// GenerateClientCerts generates certs is used to authenticate a proxy client
// with the proxy. The `CommonName` of the cert is treated as the username.
// It returns a kube config with the client certs and auth info for the user.
func (c *CertStore) GenerateUserConfig(user string) *clientcmdapi.Config {
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
	require.NoError(c.t, err)
	rootUserPublicKey := &rootUserPrivateKey.PublicKey
	rootUserCertBytes, err := x509.CreateCertificate(rand.Reader, userCertData, c.clientCaCert, rootUserPublicKey, c.privateKey)
	require.NoError(c.t, err)
	rootUserCert, err := x509.ParseCertificate(rootUserCertBytes)
	require.NoError(c.t, err)

	var clientKeyPem bytes.Buffer
	rootKeyBytes, err := x509.MarshalECPrivateKey(rootUserPrivateKey)
	require.NoError(c.t, err)
	err = pem.Encode(&clientKeyPem, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: rootKeyBytes,
	})
	require.NoError(c.t, err)

	var clientCertPem bytes.Buffer
	err = pem.Encode(&clientCertPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: rootUserCert.Raw,
	})
	require.NoError(c.t, err)

	proxyConfig := clientcmdapi.NewConfig()
	cluster := clientcmdapi.NewCluster()

	cluster.CertificateAuthorityData = c.servingCABytes
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
