package proxy

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/spf13/pflag"
	genericapiserver "k8s.io/apiserver/pkg/server"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/component-base/logs"
	logsv1 "k8s.io/component-base/logs/api/v1"

	apiserveroptions "k8s.io/apiserver/pkg/server/options"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/spicedb"
)

type Options struct {
	SecureServing  apiserveroptions.SecureServingOptionsWithLoopback
	Authentication Authentication
	Logs           *logs.Options

	BackendKubeconfigPath string
	BackendConfig         *clientcmdapi.Config

	CertDir string

	AuthenticationInfo    genericapiserver.AuthenticationInfo
	ServingInfo           *genericapiserver.SecureServingInfo
	AdditionalAuthEnabled bool

	SpicedbServer server.RunnableServer
	SpiceDBClient any
}

func NewOptions() *Options {
	o := &Options{
		SecureServing:  *apiserveroptions.NewSecureServingOptions().WithLoopback(),
		Authentication: *NewAuthentication(),
		Logs:           logsv1.NewLoggingConfiguration(),
	}
	o.Logs.Verbosity = logsv1.VerbosityLevel(2)
	o.SecureServing.BindPort = 443
	o.SecureServing.ServerCert.PairName = "proxy"
	return o
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.SecureServing.AddFlags(fs)
	o.Authentication.AddFlags(fs)
	logsv1.AddFlags(o.Logs, fs)
	fs.StringVar(&o.BackendKubeconfigPath, "backend-kubeconfig", o.BackendKubeconfigPath, "The path to the kubeconfig to proxy connections to. It should authenticate the user with cluster-admin permission.")
}

func (o *Options) Complete(ctx context.Context) error {
	if err := logsv1.ValidateAndApply(o.Logs, utilfeature.DefaultFeatureGate); err != nil {
		return err
	}
	if !filepath.IsAbs(o.BackendKubeconfigPath) {
		pwd, err := os.Getwd()
		if err != nil {
			return err
		}
		o.BackendKubeconfigPath = filepath.Join(pwd, o.BackendKubeconfigPath)
	}

	if !filepath.IsAbs(o.SecureServing.ServerCert.CertDirectory) {
		o.SecureServing.ServerCert.CertDirectory = filepath.Join(o.CertDir, o.SecureServing.ServerCert.CertDirectory)
	}

	var err error
	o.BackendConfig, err = clientcmd.LoadFromFile(o.BackendKubeconfigPath)
	if err != nil {
		return err
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

	o.SpicedbServer, err = spicedb.NewServer(ctx)
	if err != nil {
		return err
	}

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
