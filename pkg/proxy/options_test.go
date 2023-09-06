package proxy

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/clientcmd"
	logsv1 "k8s.io/component-base/logs/api/v1"
)

func TestKubeConfig(t *testing.T) {
	defer require.NoError(t, logsv1.ResetForTest(utilfeature.DefaultFeatureGate))

	opts := optionsForTesting(t)
	opts.SpiceDBEndpoint = EmbeddedSpiceDBEndpoint
	err := opts.Complete(context.Background())
	require.NoError(t, err)

	require.NoError(t, logsv1.ResetForTest(utilfeature.DefaultFeatureGate))
	opts = optionsForTesting(t)
	opts.BackendKubeconfigPath = uuid.NewString()
	err = opts.Complete(context.Background())
	require.ErrorContains(t, err, "couldn't load kubeconfig")
	require.ErrorContains(t, err, opts.BackendKubeconfigPath)
}

func TestEmbeddedSpiceDB(t *testing.T) {
	opts := optionsForTesting(t)
	opts.SpiceDBEndpoint = EmbeddedSpiceDBEndpoint
	err := opts.Complete(context.Background())
	require.NoError(t, err)
	require.NotNil(t, opts.EmbeddedSpiceDB)
	require.NotNil(t, opts.PermissionsClient)
	require.NotNil(t, opts.WatchClient)
}

func TestRemoteSpiceDB(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, addr := newTCPSpiceDB(t, ctx)
	go func() {
		if err := srv.Run(ctx); err != nil {
			require.NoError(t, err)
		}
	}()

	opts := optionsForTesting(t)
	opts.SpiceDBEndpoint = addr
	opts.insecure = true
	opts.token = "foobar"
	err := opts.Complete(context.Background())

	require.NoError(t, err)
	require.Nil(t, opts.EmbeddedSpiceDB)
	require.NotNil(t, opts.PermissionsClient)
	require.NotNil(t, opts.WatchClient)

	_, err = opts.PermissionsClient.CheckPermission(ctx, &v1.CheckPermissionRequest{})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

func optionsForTesting(t *testing.T) *Options {
	t.Helper()

	require.NoError(t, logsv1.ResetForTest(utilfeature.DefaultFeatureGate))
	opts := NewOptions()
	opts.SecureServing.BindPort = getFreePort(t, "127.0.0.1")
	opts.SecureServing.BindAddress = net.ParseIP("127.0.0.1")
	opts.BackendKubeconfigPath = kubeConfigForTest(t)
	require.Empty(t, opts.Validate())
	return opts
}

func getFreePort(t *testing.T, listenAddr string) int {
	t.Helper()

	dummyListener, err := net.Listen("tcp", net.JoinHostPort(listenAddr, "0"))
	require.NoError(t, err)

	defer require.NoError(t, dummyListener.Close())
	port := dummyListener.Addr().(*net.TCPAddr).Port
	return port
}

func newTCPSpiceDB(t *testing.T, ctx context.Context) (server.RunnableServer, string) {
	t.Helper()

	ds, err := datastore.NewDatastore(ctx,
		datastore.DefaultDatastoreConfig().ToOption(),
		datastore.WithRequestHedgingEnabled(false),
	)
	require.NoError(t, err)

	port := getFreePort(t, "localhost")
	address := fmt.Sprintf("localhost:%d", port)

	configOpts := []server.ConfigOption{
		server.WithGRPCServer(util.GRPCServerConfig{
			Network: "tcp",
			Address: address,
			Enabled: true,
		}),
		server.WithPresharedSecureKey("foobar"),
		server.WithHTTPGateway(util.HTTPServerConfig{HTTPEnabled: false}),
		server.WithMetricsAPI(util.HTTPServerConfig{HTTPEnabled: false}),
		// disable caching since it's all in memory
		server.WithDispatchCacheConfig(server.CacheConfig{Enabled: false, Metrics: false}),
		server.WithNamespaceCacheConfig(server.CacheConfig{Enabled: false, Metrics: false}),
		server.WithClusterDispatchCacheConfig(server.CacheConfig{Enabled: false, Metrics: false}),
		server.WithDatastore(ds),
	}

	srv, err := server.NewConfigWithOptionsAndDefaults(configOpts...).Complete(ctx)
	require.NoError(t, err)

	return srv, address
}

func kubeConfigForTest(t *testing.T) string {
	t.Helper()

	c, err := clientcmd.NewDefaultClientConfigLoadingRules().Load()
	require.NoError(t, err)
	f, err := os.CreateTemp("", "spicedb-kubeapi-proxy")
	require.NoError(t, err)

	err = clientcmd.WriteToFile(*c, f.Name())
	require.NoError(t, err)

	return f.Name()
}
