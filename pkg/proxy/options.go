package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	genericapiserver "k8s.io/apiserver/pkg/server"
	apiserveroptions "k8s.io/apiserver/pkg/server/options"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/component-base/logs"
	logsv1 "k8s.io/component-base/logs/api/v1"
	"k8s.io/klog/v2"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/spicedb"
)

const (
	defaultWorkflowDatabasePath = "/tmp/dtx.sqlite"
	EmbeddedSpiceDBEndpoint     = "embedded://"
	defaultDialerTimeout        = 5 * time.Second
)

type Options struct {
	SecureServing  apiserveroptions.SecureServingOptionsWithLoopback
	Authentication Authentication
	Logs           *logs.Options

	BackendKubeconfigPath string
	RestConfigFunc        func() (*rest.Config, *http.Transport, error)
	OverrideUpstream      bool
	UseInClusterConfig    bool
	RuleConfigFile        string
	Matcher               rules.Matcher

	CertDir string

	AuthenticationInfo    genericapiserver.AuthenticationInfo
	ServingInfo           *genericapiserver.SecureServingInfo
	AdditionalAuthEnabled bool

	WatchClient       v1.WatchServiceClient
	PermissionsClient v1.PermissionsServiceClient
	SpiceDBEndpoint   string
	EmbeddedSpiceDB   server.RunnableServer
	insecure          bool
	skipVerifyCA      bool
	token             string
	spicedbCAPath     string

	WorkflowDatabasePath string
	LockMode             string
}

const tlsCertificatePairName = "tls"

func NewOptions() *Options {
	o := &Options{
		SecureServing:  *apiserveroptions.NewSecureServingOptions().WithLoopback(),
		Authentication: *NewAuthentication(),
		Logs:           logsv1.NewLoggingConfiguration(),
	}
	o.Logs.Verbosity = logsv1.VerbosityLevel(2)
	o.SecureServing.BindPort = 443
	o.SecureServing.ServerCert.PairName = tlsCertificatePairName
	return o
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.SecureServing.AddFlags(fs)
	o.Authentication.AddFlags(fs)
	logsv1.AddFlags(o.Logs, fs)
	fs.StringVar(&o.WorkflowDatabasePath, "workflow-database-path", defaultWorkflowDatabasePath, "Path for the file representing the SQLite database used for the workflow engine.")
	fs.BoolVar(&o.OverrideUpstream, "override-upstream", true, "if true, uses the environment to pick the upstream apiserver address instead of what is listed in --backend-kubeconfig. This simplifies kubeconfig management when running the proxy in the same cluster as the upstream.")
	fs.BoolVar(&o.UseInClusterConfig, "use-in-cluster-config", false, "if true, uses the local cluster as the upstream and gets the configuration from the environment.")
	fs.StringVar(&o.BackendKubeconfigPath, "backend-kubeconfig", o.BackendKubeconfigPath, "The path to the kubeconfig to proxy connections to. It should authenticate the user with cluster-admin permission.")
	fs.StringVar(&o.SpiceDBEndpoint, "spicedb-endpoint", "localhost:50051", "Defines the endpoint endpoint to the SpiceDB authorizing proxy operations. if embedded:// is specified, an in memory ephemeral instance created.")
	fs.BoolVar(&o.insecure, "spicedb-insecure", false, "If set to true uses the insecure transport configuration for gRPC. Set to false by default.")
	fs.BoolVar(&o.skipVerifyCA, "spicedb-skip-verify-ca", false, "If set to true backend certificate trust chain is not verified. Set to false by default.")
	fs.StringVar(&o.token, "spicedb-token", "", "Specifies the preshared key to use with the remote SpiceDB")
	fs.StringVar(&o.spicedbCAPath, "spicedb-ca-path", "", "If set, looks in the given directory for CAs to trust when connecting to SpiceDB.")
	fs.StringVar(&o.RuleConfigFile, "rule-config", "", "The path to a file containing proxy rule configuration")
}

func (o *Options) Complete(ctx context.Context) error {
	if err := logsv1.ValidateAndApply(o.Logs, utilfeature.DefaultFeatureGate); err != nil {
		return err
	}

	var err error
	if o.RestConfigFunc == nil {
		switch o.UseInClusterConfig {
		case true:
			o.RestConfigFunc = func() (*rest.Config, *http.Transport, error) {
				conf, err := rest.InClusterConfig()
				return conf, nil, err
			}

			klog.FromContext(ctx).Info("running in-cluster, loaded ambient kube config")
		default:
			backendConfig, err := o.configFromPath()
			if err != nil {
				return fmt.Errorf("couldn't load kubeconfig from path: %w", err)
			}

			o.RestConfigFunc = func() (*rest.Config, *http.Transport, error) {
				conf, err := clientcmd.NewDefaultClientConfig(*backendConfig, nil).ClientConfig()
				if err != nil {
					return nil, nil, err
				}

				transport, err := NewTransportForKubeconfig(backendConfig)
				if err != nil {
					return nil, nil, fmt.Errorf("unable to create file kubeconfig transport: %w", err)
				}

				return conf, transport, err
			}

			klog.FromContext(ctx).WithValues("kubeconfig", o.BackendKubeconfigPath).Error(err, "running with provided kube config file")
		}
	}

	if o.Matcher == nil {
		ruleFile, err := os.Open(o.RuleConfigFile)
		if err != nil {
			return fmt.Errorf("couldn't open rule config file: %w", err)
		}
		ruleConfigs, err := proxyrule.Parse(ruleFile)
		if err != nil {
			return fmt.Errorf("couldn't parse rule config file: %w", err)
		}
		o.Matcher, err = rules.NewMapMatcher(ruleConfigs)
		if err != nil {
			return fmt.Errorf("couldn't compile rule configs: %w", err)
		}
	}

	if !filepath.IsAbs(o.SecureServing.ServerCert.CertDirectory) {
		o.SecureServing.ServerCert.CertDirectory = filepath.Join(o.CertDir, o.SecureServing.ServerCert.CertDirectory)
	}

	if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", []string{"kubernetes.default.svc", "kubernetes.default", "kubernetes"}, nil); err != nil {
		return err
	}

	var loopbackClientConfig *rest.Config
	if err := o.SecureServing.ApplyTo(&o.ServingInfo, &loopbackClientConfig); err != nil {
		return err
	}
	if err := o.Authentication.ApplyTo(&o.AuthenticationInfo, o.ServingInfo); err != nil {
		return err
	}

	o.AdditionalAuthEnabled = o.Authentication.AdditionalAuthEnabled()

	spicedbURl, err := url.Parse(o.SpiceDBEndpoint)
	if err != nil {
		return fmt.Errorf("unable to parse SpiceDB endpoint URL: %w", err)
	}

	var conn *grpc.ClientConn
	if spicedbURl.Scheme == "embedded" {
		klog.FromContext(ctx).WithValues("spicedb-endpoint", o.SpiceDBEndpoint).Info("using embedded SpiceDB")
		o.EmbeddedSpiceDB, err = spicedb.NewServer(ctx)
		if err != nil {
			return fmt.Errorf("unable to stand up embedded SpiceDB: %w", err)
		}

		conn, err = o.EmbeddedSpiceDB.GRPCDialContext(ctx, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("unable to open gRPC connection with embedded SpiceDB: %w", err)
		}
	} else {
		klog.FromContext(ctx).WithValues("spicedb-endpoint", o.SpiceDBEndpoint).
			WithValues("spicedb-insecure", o.insecure).
			WithValues("spicedb-skip-verify-ca", o.skipVerifyCA).
			WithValues("spicedb-ca-path", o.spicedbCAPath).
			Info("using remote SpiceDB")
		var opts []grpc.DialOption
		if o.insecure {
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
			opts = append(opts, grpcutil.WithInsecureBearerToken(o.token))
		} else {
			opts = append(opts, grpcutil.WithBearerToken(o.token))
			verification := grpcutil.VerifyCA
			if o.skipVerifyCA {
				verification = grpcutil.SkipVerifyCA
			}
			var certs grpc.DialOption
			if len(o.spicedbCAPath) > 0 {
				certs, err = grpcutil.WithCustomCerts(verification, o.spicedbCAPath)
				if err != nil {
					return fmt.Errorf("unable to load custom certificates: %w", err)
				}
			} else {
				certs, err = grpcutil.WithSystemCerts(verification)
				if err != nil {
					return fmt.Errorf("unable to load system certificates: %w", err)
				}
			}

			opts = append(opts, certs)
		}
		opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig}))

		timeoutCtx, cancel := context.WithTimeout(ctx, defaultDialerTimeout)
		defer cancel()
		conn, err = grpc.DialContext(timeoutCtx, o.SpiceDBEndpoint, opts...)
		if err != nil {
			return fmt.Errorf("unable to open gRPC connection to remote SpiceDB at %s: %w", o.SpiceDBEndpoint, err)
		}
	}

	o.PermissionsClient = v1.NewPermissionsServiceClient(conn)
	o.WatchClient = v1.NewWatchServiceClient(conn)

	return nil
}

func (o *Options) configFromPath() (*clientcmdapi.Config, error) {

	if !filepath.IsAbs(o.BackendKubeconfigPath) {
		pwd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("couldn't load kubeconfig: %w", err)
		}
		o.BackendKubeconfigPath = filepath.Join(pwd, o.BackendKubeconfigPath)
	}

	backendConfig, err := clientcmd.LoadFromFile(o.BackendKubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("couldn't load kubeconfig: %w", err)
	}

	// This uses the in-cluster config to get the URI of the upstream cluster
	// This useful when testing with a provide kubeconfig file and make it reach out to the API.
	// It's also useful in a scenario where you want to use it in a cluster without RBAC enabled.
	//
	// If running in-cluster, prefer the --use-in-cluster-config flag
	if o.OverrideUpstream {
		host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
		addr := "https://" + net.JoinHostPort(host, port)
		for i := range backendConfig.Clusters {
			backendConfig.Clusters[i].Server = addr
		}
	}

	return backendConfig, nil
}

func (o *Options) Validate() []error {
	var errs []error

	if len(o.BackendKubeconfigPath) == 0 {
		errs = append(errs, fmt.Errorf("--backend-kubeconfig is required"))
	}

	if len(o.RuleConfigFile) == 0 {
		errs = append(errs, fmt.Errorf("--rule-config is required"))
	}

	errs = append(errs, o.SecureServing.Validate()...)
	errs = append(errs, o.Authentication.Validate()...)

	return errs
}

func NewTransportForKubeconfig(config *clientcmdapi.Config) (*http.Transport, error) {
	kubeCtx := config.Contexts[config.CurrentContext]
	cluster := config.Clusters[kubeCtx.Cluster]
	user := config.AuthInfos[kubeCtx.AuthInfo]

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(cluster.CertificateAuthorityData)

	cert, err := tls.X509KeyPair(user.ClientCertificateData, user.ClientKeyData)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate %q or key %q: %w", user.ClientCertificateData, user.ClientKeyData, err)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	return transport, nil
}
