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
	"strings"
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

//go:generate go run github.com/ecordell/optgen -output zz_spicedb_options.go . SpiceDBOptions

type Options struct {
	SecureServing  apiserveroptions.SecureServingOptionsWithLoopback `debugmap:"hidden"`
	Authentication Authentication                                    `debugmap:"hidden"`
	Logs           *logs.Options                                     `debugmap:"hidden"`

	// TODO: use genericclioptions.ConfigFlags instead of this?
	BackendKubeconfigPath string                                          `debugmap:"visible"`
	RestConfigFunc        func() (*rest.Config, http.RoundTripper, error) `debugmap:"hidden"`
	OverrideUpstream      bool                                            `debugmap:"visible"`
	UseInClusterConfig    bool                                            `debugmap:"visible"`

	RuleConfigFile string        `debugmap:"visible"`
	Matcher        rules.Matcher `debugmap:"hidden"`

	SpiceDBOptions SpiceDBOptions `debugmap:"visible"`

	CertDir string `debugmap:"visible"`

	AuthenticationInfo    genericapiserver.AuthenticationInfo `debugmap:"hidden"`
	ServingInfo           *genericapiserver.SecureServingInfo `debugmap:"hidden"`
	AdditionalAuthEnabled bool                                `debugmap:"visible"`

	InputExtractor rules.ResolveInputExtractor `debugmap:"hidden"`

	WorkflowDatabasePath string `debugmap:"visible"`
	LockMode             string `debugmap:"visible"`

	WatchClient       v1.WatchServiceClient       `debugmap:"hidden"`
	PermissionsClient v1.PermissionsServiceClient `debugmap:"hidden"`
}

type SpiceDBOptions struct {
	SpiceDBEndpoint            string                `debugmap:"visible"`
	EmbeddedSpiceDB            server.RunnableServer `debugmap:"hidden"`
	Insecure                   bool                  `debugmap:"sensitive"`
	SkipVerifyCA               bool                  `debugmap:"visible"`
	SecureSpiceDBTokensBySpace string                `debugmap:"sensitive"`
	SpicedbCAPath              string                `debugmap:"visible"`
}

func NewSpiceDBOptions() SpiceDBOptions {
	return SpiceDBOptions{
		SpiceDBEndpoint:            "localhost:50051",
		Insecure:                   false,
		SkipVerifyCA:               false,
		SecureSpiceDBTokensBySpace: "somepresharedkey",
		SpicedbCAPath:              "",
	}
}

func (so *SpiceDBOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&so.SpiceDBEndpoint, "spicedb-endpoint", "localhost:50051", "Defines the endpoint endpoint to the SpiceDB authorizing proxy operations. if embedded:// is specified, an in memory ephemeral instance created.")
	fs.BoolVar(&so.Insecure, "spicedb-insecure", false, "If set to true uses the insecure transport configuration for gRPC. Set to false by default.")
	fs.BoolVar(&so.SkipVerifyCA, "spicedb-skip-verify-ca", false, "If set to true backend certificate trust chain is not verified. Set to false by default.")
	fs.StringVar(&so.SecureSpiceDBTokensBySpace, "spicedb-token", "", "Specifies the preshared key to use with the remote SpiceDB")
	fs.StringVar(&so.SpicedbCAPath, "spicedb-ca-path", "", "If set, looks in the given directory for CAs to trust when connecting to SpiceDB.")
}

const tlsCertificatePairName = "tls"

func NewOptions() *Options {
	o := &Options{
		SecureServing:  *apiserveroptions.NewSecureServingOptions().WithLoopback(),
		Authentication: *NewAuthentication(),
		SpiceDBOptions: NewSpiceDBOptions(),
		Logs:           logsv1.NewLoggingConfiguration(),
	}
	o.Logs.Verbosity = logsv1.VerbosityLevel(2)
	o.SecureServing.BindPort = 443
	o.SecureServing.ServerCert.PairName = tlsCertificatePairName
	return o
}

func (o *Options) FromRESTConfig(restConfig *rest.Config) *Options {
	o.OverrideUpstream = false
	o.UseInClusterConfig = false
	o.RestConfigFunc = func() (*rest.Config, http.RoundTripper, error) {
		if restConfig == nil {
			return nil, nil, fmt.Errorf("nil REST config provided")
		}

		transport, err := rest.TransportFor(restConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create transport: %w", err)
		}
		return restConfig, transport, nil
	}
	return o
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.SecureServing.AddFlags(fs)
	o.Authentication.AddFlags(fs)
	o.SpiceDBOptions.AddFlags(fs)
	logsv1.AddFlags(o.Logs, fs)

	fs.StringVar(&o.WorkflowDatabasePath, "workflow-database-path", defaultWorkflowDatabasePath, "Path for the file representing the SQLite database used for the workflow engine.")
	fs.BoolVar(&o.OverrideUpstream, "override-upstream", true, "if true, uses the environment to pick the upstream apiserver address instead of what is listed in --backend-kubeconfig. This simplifies kubeconfig management when running the proxy in the same cluster as the upstream.")
	fs.BoolVar(&o.UseInClusterConfig, "use-in-cluster-config", false, "if true, uses the local cluster as the upstream and gets the configuration from the environment.")
	fs.StringVar(&o.BackendKubeconfigPath, "backend-kubeconfig", o.BackendKubeconfigPath, "The path to the kubeconfig to proxy connections to. It should authenticate the user with cluster-admin permission.")
	fs.StringVar(&o.RuleConfigFile, "rule-config", "", "The path to a file containing proxy rule configuration")
}

type CompletedConfig struct {
	config *Options
}

func (o *Options) Complete(ctx context.Context) (*CompletedConfig, error) {
	if err := logsv1.ValidateAndApply(o.Logs, utilfeature.DefaultFeatureGate); err != nil {
		return nil, err
	}

	var err error
	if o.RestConfigFunc == nil {
		switch o.UseInClusterConfig {
		case true:
			o.RestConfigFunc = func() (*rest.Config, http.RoundTripper, error) {
				conf, err := rest.InClusterConfig()
				if err != nil {
					return nil, nil, err
				}

				transport, err := rest.TransportFor(conf)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to build transport for in-cluster config: %w", err)
				}

				return conf, transport, err
			}

			klog.FromContext(ctx).Info("running in-cluster, loaded ambient kube config")
		default:
			backendConfig, err := o.configFromPath()
			if err != nil {
				return nil, fmt.Errorf("couldn't load kubeconfig from path: %w", err)
			}

			o.RestConfigFunc = func() (*rest.Config, http.RoundTripper, error) {
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
			return nil, fmt.Errorf("couldn't open rule config file: %w", err)
		}
		ruleConfigs, err := proxyrule.Parse(ruleFile)
		if err != nil {
			return nil, fmt.Errorf("couldn't parse rule config file: %w", err)
		}
		o.Matcher, err = rules.NewMapMatcher(ruleConfigs)
		if err != nil {
			return nil, fmt.Errorf("couldn't compile rule configs: %w", err)
		}
	}
	if o.InputExtractor == nil {
		o.InputExtractor = rules.ResolveInputExtractorFunc(rules.NewResolveInputFromHttp)
	}

	if !filepath.IsAbs(o.SecureServing.ServerCert.CertDirectory) {
		o.SecureServing.ServerCert.CertDirectory = filepath.Join(o.CertDir, o.SecureServing.ServerCert.CertDirectory)
	}

	if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", []string{"kubernetes.default.svc", "kubernetes.default", "kubernetes"}, nil); err != nil {
		return nil, err
	}

	var loopbackClientConfig *rest.Config
	if err := o.SecureServing.ApplyTo(&o.ServingInfo, &loopbackClientConfig); err != nil {
		return nil, err
	}
	if err := o.Authentication.ApplyTo(ctx, &o.AuthenticationInfo, o.ServingInfo); err != nil {
		return nil, err
	}

	o.AdditionalAuthEnabled = o.Authentication.AdditionalAuthEnabled()

	spicedbURl, err := url.Parse(o.SpiceDBOptions.SpiceDBEndpoint)
	if err != nil {
		return nil, fmt.Errorf("unable to parse SpiceDB endpoint URL: %w", err)
	}

	var conn *grpc.ClientConn
	if spicedbURl.Scheme == "embedded" {
		klog.FromContext(ctx).WithValues("spicedb-endpoint", spicedbURl).Info("using embedded SpiceDB")
		o.SpiceDBOptions.EmbeddedSpiceDB, err = spicedb.NewServer(ctx, spicedbURl.Path)
		if err != nil {
			return nil, fmt.Errorf("unable to stand up embedded SpiceDB: %w", err)
		}

		conn, err = o.SpiceDBOptions.EmbeddedSpiceDB.GRPCDialContext(ctx, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("unable to open gRPC connection with embedded SpiceDB: %w", err)
		}
	} else {
		klog.FromContext(ctx).WithValues("spicedb-endpoint", o.SpiceDBOptions.SpiceDBEndpoint).
			WithValues("spicedb-insecure", o.SpiceDBOptions.Insecure).
			WithValues("spicedb-skip-verify-ca", o.SpiceDBOptions.SkipVerifyCA).
			WithValues("spicedb-ca-path", o.SpiceDBOptions.SpicedbCAPath).
			Info("using remote SpiceDB")
		var opts []grpc.DialOption

		tokens := strings.Split(o.SpiceDBOptions.SecureSpiceDBTokensBySpace, ",")
		if len(tokens) == 0 {
			return nil, fmt.Errorf("no SpiceDB token defined")
		}

		token := strings.TrimSpace(tokens[0])
		if o.SpiceDBOptions.Insecure {
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
			opts = append(opts, grpcutil.WithInsecureBearerToken(token))
		} else {
			opts = append(opts, grpcutil.WithBearerToken(token))
			verification := grpcutil.VerifyCA
			if o.SpiceDBOptions.SkipVerifyCA {
				verification = grpcutil.SkipVerifyCA
			}
			var certs grpc.DialOption
			if len(o.SpiceDBOptions.SpicedbCAPath) > 0 {
				certs, err = grpcutil.WithCustomCerts(verification, o.SpiceDBOptions.SpicedbCAPath)
				if err != nil {
					return nil, fmt.Errorf("unable to load custom certificates: %w", err)
				}
			} else {
				certs, err = grpcutil.WithSystemCerts(verification)
				if err != nil {
					return nil, fmt.Errorf("unable to load system certificates: %w", err)
				}
			}

			opts = append(opts, certs)
		}
		opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig}))

		timeoutCtx, cancel := context.WithTimeout(ctx, defaultDialerTimeout)
		defer cancel()
		conn, err = grpc.DialContext(timeoutCtx, o.SpiceDBOptions.SpiceDBEndpoint, opts...)
		if err != nil {
			return nil, fmt.Errorf("unable to open gRPC connection to remote SpiceDB at %s: %w", o.SpiceDBOptions.SpiceDBEndpoint, err)
		}
	}

	if o.PermissionsClient == nil {
		o.PermissionsClient = v1.NewPermissionsServiceClient(conn)
	}

	if o.WatchClient == nil {
		o.WatchClient = v1.NewWatchServiceClient(conn)
	}

	return &CompletedConfig{o}, nil
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

	if len(o.BackendKubeconfigPath) == 0 && !o.UseInClusterConfig {
		errs = append(errs, fmt.Errorf("either --backend-kubeconfig or --use-in-cluster-config must be specified"))
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
