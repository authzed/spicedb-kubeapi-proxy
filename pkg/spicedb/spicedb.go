package spicedb

import (
	"context"
	_ "embed"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/util"
)

//go:embed bootstrap.yaml
var bootstrap []byte

func NewServer(ctx context.Context, bootstrapFilePath string, bootstrapContent map[string][]byte) (server.RunnableServer, error) {
	bootstrapOption := datastore.SetBootstrapFileContents(map[string][]byte{"schema": bootstrap})
	if len(bootstrapContent) > 0 {
		bootstrapOption = datastore.SetBootstrapFileContents(bootstrapContent)
	} else if len(bootstrapFilePath) > 0 {
		bootstrapOption = datastore.SetBootstrapFiles([]string{bootstrapFilePath})
	}
	return server.NewConfigWithOptionsAndDefaults(
		server.WithGRPCServer(util.GRPCServerConfig{
			Network:    util.BufferedNetwork,
			Enabled:    true,
			BufferSize: 10 * humanize.MiByte,
		}),
		server.WithDispatchServer(util.GRPCServerConfig{Enabled: false}),
		server.WithDispatchUpstreamAddr(""),
		server.WithHTTPGatewayUpstreamAddr(""),
		server.WithDispatchMaxDepth(50),
		server.WithMaximumUpdatesPerWrite(1000),
		server.WithMaximumPreconditionCount(1000),
		server.WithMaxCaveatContextSize(1000000),
		server.WithMaxRelationshipContextSize(1000000),
		server.WithSchemaPrefixesRequired(false),
		server.WithHTTPGateway(util.HTTPServerConfig{HTTPEnabled: false}),
		server.WithMetricsAPI(util.HTTPServerConfig{HTTPEnabled: false}),
		server.WithSilentlyDisableTelemetry(true),
		server.WithDispatchClusterMetricsEnabled(false),
		server.WithDispatchClientMetricsEnabled(false),
		server.WithDispatchCacheConfig(server.CacheConfig{Enabled: false, Metrics: false}),
		server.WithNamespaceCacheConfig(server.CacheConfig{Enabled: false, Metrics: false}),
		server.WithClusterDispatchCacheConfig(server.CacheConfig{Enabled: false, Metrics: false}),
		// Disable metrics on the stored-schema cache. With Metrics: true (the default) the
		// cache registers Prometheus collectors named "stored_schema" on the global
		// registry, so creating more than one embedded instance in a single process (as the
		// tests do) fails with "duplicate metrics collector registration attempted". With
		// Metrics: false the cache is built without registering anything.
		server.WithStoredSchemaCacheConfig(server.CacheConfig{
			Name:        "stored_schema",
			Enabled:     true,
			Metrics:     false,
			NumCounters: 1_000,
			MaxCost:     "32MiB",
		}),
		server.WithEnableRelationshipExpiration(true),
		server.WithDatastoreConfig(
			*datastore.NewConfigWithOptionsAndDefaults().WithOptions(
				datastore.WithEngine(datastore.MemoryEngine),
				bootstrapOption,
				datastore.WithRequestHedgingEnabled(false),
				datastore.WithGCWindow(24*time.Hour),
			),
		),
		server.WithGRPCAuthFunc(func(ctx context.Context) (context.Context, error) { return ctx, nil }),
	).Complete(ctx)
}
