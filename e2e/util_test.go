//go:build e2e

package e2e

import (
	"context"
	"io"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
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
