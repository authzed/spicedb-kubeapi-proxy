package main

import (
	"context"
	goflags "flag"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/errors"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	utilflag "k8s.io/component-base/cli/flag"
	_ "k8s.io/component-base/logs/json/register"
	"k8s.io/component-base/version"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy"
)

func main() {
	ctx := genericapiserver.SetupSignalContext()

	pflag.CommandLine.SetNormalizeFunc(utilflag.WordSepNormalizeFunc)
	pflag.CommandLine.AddGoFlagSet(goflags.CommandLine)

	os.Exit(cli.Run(NewProxyCommand(ctx)))
}

func NewProxyCommand(ctx context.Context) *cobra.Command {
	options := proxy.NewOptions()
	cmd := &cobra.Command{
		Use:   "spicedb-kubeapi-proxy",
		Short: "Authorizes Kube api requests with SpiceDB.",
		Long: `spicedb-kubeapi-proxy is a reverse proxy that authorizes Kube api 
requests with SpiceDB and keeps relationship data up to date. The proxy handles
TLS termination and authentication with a backend kube apiserver.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(ctx); err != nil {
				return err
			}
			if errs := options.Validate(); errs != nil {
				return errors.NewAggregate(errs)
			}

			server, err := proxy.NewServer(ctx, *options)
			if err != nil {
				return err
			}

			return server.Run(ctx)
		},
	}

	options.AddFlags(cmd.Flags())

	if v := version.Get().String(); len(v) == 0 {
		cmd.Version = "<unknown>"
	} else {
		cmd.Version = v
	}

	return cmd
}
