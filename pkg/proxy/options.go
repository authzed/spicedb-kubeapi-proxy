package proxy

import (
	"context"
	"fmt"
	"net"
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
	BackendConfig         *clientcmdapi.Config
	OverrideUpstream      bool

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
	fs.StringVar(&o.BackendKubeconfigPath, "backend-kubeconfig", o.BackendKubeconfigPath, "The path to the kubeconfig to proxy connections to. It should authenticate the user with cluster-admin permission.")
	fs.StringVar(&o.SpiceDBEndpoint, "spicedb-endpoint", "localhost:50051", "Defines the endpoint endpoint to the SpiceDB authorizing proxy operations. if embedded:// is specified, an in memory ephemeral instance created.")
	fs.BoolVar(&o.insecure, "spicedb-insecure", false, "If set to true uses the insecure transport configuration for gRPC. Set to false by default.")
	fs.BoolVar(&o.skipVerifyCA, "spicedb-skip-verify-ca", false, "If set to true backend certificate trust chain is not verified. Set to false by default.")
	fs.StringVar(&o.token, "spicedb-token", "", "specifies the preshared key to use with the remote SpiceDB")
}

func (o *Options) Complete(ctx context.Context) error {
	if err := logsv1.ValidateAndApply(o.Logs, utilfeature.DefaultFeatureGate); err != nil {
		return err
	}
	var err error
	if o.BackendConfig == nil {
		// TODO: load ambient kube config if running in a cluster and no explict
		// kubeconfig given
		if !filepath.IsAbs(o.BackendKubeconfigPath) {
			pwd, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("couldn't load kubeconfig: %w", err)
			}
			o.BackendKubeconfigPath = filepath.Join(pwd, o.BackendKubeconfigPath)
		}
		o.BackendConfig, err = clientcmd.LoadFromFile(o.BackendKubeconfigPath)
		if err != nil {
			return fmt.Errorf("couldn't load kubeconfig: %w", err)
		}
		// This uses the in-cluster config to get the URI
		// In the future we may want options that use the full in-cluster config
		// instead of requiring a kubeconfig.
		if o.OverrideUpstream {
			host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
			addr := "https://" + net.JoinHostPort(host, port)
			for i := range o.BackendConfig.Clusters {
				o.BackendConfig.Clusters[i].Server = addr
			}
		}
		klog.FromContext(ctx).WithValues("kubeconfig", o.BackendKubeconfigPath).Error(err, "loaded backend kube config")
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

			certs, err := grpcutil.WithSystemCerts(verification)
			if err != nil {
				return fmt.Errorf("unable to load system certificates: %w", err)
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

func (o *Options) Validate() []error {
	var errs []error

	if len(o.BackendKubeconfigPath) == 0 {
		errs = append(errs, fmt.Errorf("--backend-kubeconfig is required"))
	}

	errs = append(errs, o.SecureServing.Validate()...)
	errs = append(errs, o.Authentication.Validate()...)

	return errs
}
