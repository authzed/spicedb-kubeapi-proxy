package rules

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	"github.com/authzed/spicedb/pkg/zedtoken"
	"github.com/shopspring/decimal"
)

func MustIntZedToken(t int64) string {
	return zedtoken.MustNewFromRevision(revision.NewFromDecimal(decimal.NewFromInt(t))).String()
}

func TestConsistencyFromHeader(t *testing.T) {
	tests := []struct {
		name    string
		headers http.Header
		want    *v1.Consistency
	}{
		{
			name: "defaults to minimize latency",
			want: MinimizeLatency,
		},
		{
			name: "fully consistent",
			headers: map[string][]string{
				ConsistencyHeaderKey: {FullyConsistentHeaderValue},
			},
			want: FullyConsistent,
		},
		{
			name: "at exact snapshot",
			headers: map[string][]string{
				ConsistencyHeaderKey: {AtExactSnapshotHeaderValuePrefix + MustIntZedToken(5)},
			},
			want: &v1.Consistency{
				Requirement: &v1.Consistency_AtExactSnapshot{
					AtExactSnapshot: &v1.ZedToken{Token: MustIntZedToken(5)},
				},
			},
		},
		{
			name: "at least as fresh",
			headers: map[string][]string{
				ConsistencyHeaderKey: {AtLeastAsFreshHeaderValuePrefix + MustIntZedToken(3)},
			},
			want: &v1.Consistency{
				Requirement: &v1.Consistency_AtLeastAsFresh{
					AtLeastAsFresh: &v1.ZedToken{Token: MustIntZedToken(3)},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tt.headers {
				for _, vv := range v {
					h.Set(k, vv)
				}
			}
			if got := ConsistencyFromHeaders(h); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConsistencyFromHeaders() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWatchDelayFromHeaders(t *testing.T) {
	tests := []struct {
		name    string
		headers http.Header
		want    time.Duration
	}{
		{
			name: "no headers",
			want: time.Duration(0),
		},
		{
			name:    "bad fmt",
			headers: map[string][]string{WatchDelayKey: {"123qwewe"}},
		},
		{
			name:    "set delay",
			headers: map[string][]string{WatchDelayKey: {"6s"}},
			want:    time.Second * 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tt.headers {
				for _, vv := range v {
					h.Set(k, vv)
				}
			}
			if got := WatchDelayFromHeaders(h); got != tt.want {
				t.Errorf("WatchDelayFromHeaders() = %v, want %v", got, tt.want)
			}
		})
	}
}
