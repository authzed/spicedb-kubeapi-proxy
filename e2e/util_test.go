//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"os"
	goruntime "runtime"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/go-logr/logr"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"github.com/spf13/afero"
	"sigs.k8s.io/controller-runtime/tools/setup-envtest/env"
	"sigs.k8s.io/controller-runtime/tools/setup-envtest/remote"
	"sigs.k8s.io/controller-runtime/tools/setup-envtest/store"
	"sigs.k8s.io/controller-runtime/tools/setup-envtest/versions"
	"sigs.k8s.io/controller-runtime/tools/setup-envtest/workflows"
)

// GetAllTuples collects all tuples matching the filter from SpiceDB
func GetAllTuples(ctx context.Context, filter *v1.RelationshipFilter) []*v1.ReadRelationshipsResponse {
	client, err := proxySrv.PermissionClient().ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
		Consistency:        &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
		RelationshipFilter: filter,
	})
	Expect(err).To(Succeed())
	results := make([]*v1.ReadRelationshipsResponse, 0)
	for resp, err := client.Recv(); err != io.EOF; resp, err = client.Recv() {
		Expect(err).To(Succeed())
		results = append(results, resp)
	}
	return results
}

// RelRespToStrings converts a slice of *v1.ReadRelationshipsResponse to a slice
// of tuple strings.
func RelRespToStrings(relResps []*v1.ReadRelationshipsResponse) []string {
	return lo.Map(relResps, func(item *v1.ReadRelationshipsResponse, _ int) string {
		return tuple.MustV1RelString(item.Relationship)
	})
}

// WriteTuples writes the given relationships to SpiceDB
func WriteTuples(ctx context.Context, rels []*v1.Relationship) {
	updates := make([]*v1.RelationshipUpdate, 0, len(rels))
	for _, rel := range rels {
		updates = append(updates, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: rel,
		})
	}

	_, err := proxySrv.PermissionClient().WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
		Updates: updates,
	})
	Expect(err).To(Succeed())
}

// setupEnvtest sets up the Kubernetes test binaries using setup-envtest
func setupEnvtest(log logr.Logger) string {
	e := &env.Env{
		Log: log,
		Client: &remote.HTTPClient{
			Log:      log,
			IndexURL: remote.DefaultIndexURL,
		},
		Version: versions.Spec{
			Selector:    versions.TildeSelector{},
			CheckLatest: false,
		},
		VerifySum:     true,
		ForceDownload: false,
		Platform: versions.PlatformItem{
			Platform: versions.Platform{
				OS:   goruntime.GOOS,
				Arch: goruntime.GOARCH,
			},
		},
		FS:    afero.Afero{Fs: afero.NewOsFs()},
		Store: store.NewAt("../testbin"),
		Out:   os.Stdout,
	}

	version, err := versions.FromExpr("~1.33.0")
	if err != nil {
		panic(fmt.Sprintf("failed to parse version: %v", err))
	}
	e.Version = version

	workflows.Use{
		UseEnv:      true,
		PrintFormat: env.PrintOverview,
		AssetsPath:  "../testbin",
	}.Do(e)

	return fmt.Sprintf("../testbin/k8s/%s-%s-%s", e.Version.AsConcrete(), e.Platform.OS, e.Platform.Arch)
}
