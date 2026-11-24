package tsm1_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
	"github.com/influxdata/influxql"
)

func TestAISMNoCoveredBlockUsesNative(t *testing.T) {
	t.Setenv("INFLUXDB_AISM_BUILD", "1")
	e, err := NewEngine(t, "tsi1")
	if err != nil {
		t.Fatal(err)
	}
	e.CompactionPlan = &mockPlanner{}
	ctx := context.Background()
	if err := e.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("i", influxql.Integer); err != nil {
		t.Fatal(err)
	}
	if err := e.CreateSeriesIfNotExists([]byte("cpu,host=h"), []byte("cpu"), models.NewTags(map[string]string{"host": "h"})); err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	for i := 0; i < 4000; i++ {
		fmt.Fprintf(&text, "cpu,host=h i=%di %d\n", i%127, int64(i)*int64(10*time.Second))
	}
	if err := e.WritePointsString(text.String()); err != nil {
		t.Fatal(err)
	}
	e.MustWriteSnapshot()
	opt := query.IteratorOptions{Expr: influxql.MustParseExpr("sum(i)"), Interval: query.Interval{Duration: time.Hour}, Dimensions: []string{"host"}, Ascending: true, StartTime: 0, EndTime: int64(12 * time.Hour), Fill: influxql.NoFill, Authorizer: query.OpenAuthorizer}
	type row struct {
		time, value int64
		n           uint32
	}
	read := func(mode string) ([]row, *aism.Trace) {
		t.Helper()
		qctx, tr := aism.WithTrace(ctx, mode)
		it, err := e.CreateIterator(qctx, "cpu", opt)
		if err != nil || it == nil {
			t.Fatalf("iterator: %v", err)
		}
		defer it.Close()
		var rows []row
		for {
			p, err := it.(query.IntegerIterator).Next()
			if err != nil {
				t.Fatal(err)
			}
			if p == nil {
				return rows, tr
			}
			rows = append(rows, row{p.Time, p.Value, p.Aggregated})
		}
	}
	for _, clipped := range []bool{false, true} {
		if clipped {
			opt.Interval.Duration = 12 * time.Hour
			opt.StartTime, opt.EndTime = int64(100*time.Second), int64(500*time.Second)
		}
		want, native := read("native")
		for _, mode := range []string{"leaf", "full", "cross"} {
			got, tr := read(mode)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s clipped=%v: native=%v indexed=%v", mode, clipped, want, got)
			}
			var trace struct {
				Reasons map[string]uint64 `json:"fallback_reasons"`
			}
			if err := json.Unmarshal([]byte(tr.JSON(0)), &trace); err != nil {
				t.Fatal(err)
			}
			if tr.UsedSeries.Load() != 0 || tr.RawDecoded.Load() != 0 || tr.Visited[0].Load() != 0 || tr.NativeDecoded.Load() != native.NativeDecoded.Load() || trace.Reasons["no_covered_block"] == 0 {
				t.Fatalf("unproductive index execution: %s", tr.JSON(0))
			}
		}
		got, raw := read("raw")
		if !reflect.DeepEqual(got, want) || raw.RawDecoded.Load() == 0 {
			t.Fatalf("forced raw control changed: %s", raw.JSON(0))
		}
	}
	// A wider original-style bucket must still use summaries, including after reopen.
	opt.StartTime, opt.EndTime = 0, int64(12*time.Hour)
	opt.Interval.Duration = 6 * time.Hour
	for _, reopen := range []bool{false, true} {
		if reopen {
			if err := e.Reopen(); err != nil {
				t.Fatal(err)
			}
			e.CompactionPlan = &mockPlanner{}
		}
		want, _ := read("native")
		got, tr := read("cross")
		if !reflect.DeepEqual(got, want) || tr.Merged[0].Load() == 0 {
			t.Fatalf("useful summaries rejected: %s", tr.JSON(0))
		}
	}
}
