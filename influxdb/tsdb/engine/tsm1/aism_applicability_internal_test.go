package tsm1

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/influxql/query"
)

func TestAISMCoveredBlockBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []IndexEntry
		lo, hi  int64
		offset  time.Duration
		want    bool
	}{
		{"empty", nil, 0, 99, 0, false},
		{"inclusive_query_end", []IndexEntry{{MinTime: 0, MaxTime: 9}}, 0, 9, 0, true},
		{"exclusive_bucket_end", []IndexEntry{{MinTime: 0, MaxTime: 10}}, 0, 99, 0, false},
		{"query_clips_start", []IndexEntry{{MinTime: 0, MaxTime: 9}}, 1, 99, 0, false},
		{"query_clips_end", []IndexEntry{{MinTime: 0, MaxTime: 9}}, 0, 8, 0, false},
		{"before_query", []IndexEntry{{MinTime: 0, MaxTime: 9}}, 10, 99, 0, false},
		{"after_query", []IndexEntry{{MinTime: 10, MaxTime: 19}}, 0, 9, 0, false},
		{"negative_time", []IndexEntry{{MinTime: -10, MaxTime: -1}}, -10, 99, 0, true},
		{"offset_crosses_bucket", []IndexEntry{{MinTime: 0, MaxTime: 9}}, 0, 99, 5, false},
		{"offset_fits_bucket", []IndexEntry{{MinTime: 5, MaxTime: 14}}, 0, 99, 5, true},
		{"singleton_at_boundary", []IndexEntry{{MinTime: 10, MaxTime: 10}}, 0, 99, 0, true},
		{"later_covered_block", []IndexEntry{{MinTime: 0, MaxTime: 19}, {MinTime: 20, MaxTime: 29}}, 0, 99, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt := query.IteratorOptions{StartTime: tc.lo, EndTime: tc.hi, Interval: query.Interval{Duration: 10, Offset: tc.offset}}
			if got := aismHasCoveredBlock(tc.entries, opt); got != tc.want {
				t.Fatalf("covered=%v, want %v", got, tc.want)
			}
		})
	}
}
