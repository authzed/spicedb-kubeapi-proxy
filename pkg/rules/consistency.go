package rules

import (
	"net/http"
	"strings"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

const (
	WatchDelayKey                    = "SpiceDB-Watch-Delay"
	ConsistencyHeaderKey             = "SpiceDB-Consistency"
	FullyConsistentHeaderValue       = "Full"
	AtExactSnapshotHeaderValuePrefix = "Exact "
	AtLeastAsFreshHeaderValuePrefix  = "At-Least "
)

var MinimizeLatency = &v1.Consistency{
	Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true},
}

var FullyConsistent = &v1.Consistency{
	Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
}

// ConsistencyFromHeaders returns a consistency block given a consistency
// header. Defaults to Minimize Latency, otherwise returns the first match in order:
//   - Fully Consistent
//   - At Exact Snapshot
//   - At Least As Fresh
//   - Minimize Latency
func ConsistencyFromHeaders(headers http.Header) *v1.Consistency {
	consistencyValue := headers.Get(ConsistencyHeaderKey)
	if len(consistencyValue) == 0 {
		return MinimizeLatency
	}

	switch consistencyValue[0] {
	case 'F':
		fallthrough
	case 'f':
		return FullyConsistent
	case 'E':
		fallthrough
	case 'e':
		return &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: &v1.ZedToken{Token: strings.TrimPrefix(consistencyValue, AtExactSnapshotHeaderValuePrefix)},
			},
		}
	case 'A':
		fallthrough
	case 'a':
		return &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: &v1.ZedToken{Token: strings.TrimPrefix(consistencyValue, AtLeastAsFreshHeaderValuePrefix)},
			},
		}
	}

	return MinimizeLatency
}

// WatchDelayFromHeaders returns a time duration from the delay header.
// This is used to delay sending the check request to SpiceDB on a watch event,
// so that non-fully-consistent modes can be used with watch.
func WatchDelayFromHeaders(headers http.Header) time.Duration {
	delay := headers.Get(WatchDelayKey)
	if len(delay) == 0 {
		return 0
	}

	duration, err := time.ParseDuration(delay)
	if err != nil {
		return 0
	}
	return duration
}
