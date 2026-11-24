package tsm1_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

func TestAISMEngineLifecycle(t *testing.T) {
	t.Setenv("INFLUXDB_AISM_BUILD", "1")
	e, err := NewEngine(t, "tsi1")
	if err != nil {
		t.Fatal(err)
	}
	e.CompactionPlan = &mockPlanner{}
	ctx := context.Background()
	if err = e.Open(ctx); err != nil {
		t.Fatal(err)
	}
	for name, typ := range map[string]influxql.DataType{"i": influxql.Integer, "f": influxql.Float, "b": influxql.Boolean} {
		if _, _, err := e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists(name, typ); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.CreateSeriesIfNotExists([]byte("cpu,hostname=h"), []byte("cpu"), models.NewTags(map[string]string{"hostname": "h"})); err != nil {
		t.Fatal(err)
	}
	const n = 70000
	const start = int64(86400) * int64(time.Second)
	// Sparse booleans deliberately have fewer physical blocks than the integers.
	var lines []string
	for i := 0; i < n; i++ {
		fields := fmt.Sprintf("i=%di,f=%.3f", i%127, float64(i%17)/8)
		if i%3 == 0 {
			fields += fmt.Sprintf(",b=%t", i%2 == 0)
		}
		lines = append(lines, fmt.Sprintf("cpu,hostname=h %s %d", fields, start+int64(i)*int64(time.Second)))
		if len(lines) == 1000 {
			p, err := models.ParsePointsString(strings.Join(lines, "\n"))
			if err != nil {
				t.Fatal(err)
			}
			if err = e.WritePoints(ctx, p); err != nil {
				t.Fatal(err)
			}
			lines = lines[:0]
		}
		if i+1 == 30000 {
			e.MustWriteSnapshot()
		}
	}
	e.MustWriteSnapshot()
	type row struct {
		T int64
		V interface{}
		N uint32
	}
	read := func(mode, field, op string, width, offset time.Duration) ([]row, *aism.Trace) {
		t.Helper()
		qctx, tr := aism.WithTrace(ctx, mode)
		opt := query.IteratorOptions{Expr: influxql.MustParseExpr(op + "(" + field + ")"), Interval: query.Interval{Duration: width, Offset: offset}, Dimensions: []string{"hostname"}, Condition: influxql.MustParseExpr("hostname = 'h'"), Ascending: true, StartTime: start, EndTime: start + (n+10)*int64(time.Second), Fill: influxql.NoFill, Authorizer: query.OpenAuthorizer}
		it, err := e.CreateIterator(qctx, "cpu", opt)
		if err != nil || it == nil {
			t.Fatalf("%s %s %s iterator: %v", mode, field, op, err)
		}
		defer it.Close()
		var out []row
		for {
			switch x := it.(type) {
			case query.IntegerIterator:
				p, err := x.Next()
				if err != nil {
					t.Fatal(err)
				}
				if p == nil {
					return out, tr
				}
				out = append(out, row{p.Time, p.Value, p.Aggregated})
			case query.FloatIterator:
				p, err := x.Next()
				if err != nil {
					t.Fatal(err)
				}
				if p == nil {
					return out, tr
				}
				out = append(out, row{p.Time, p.Value, p.Aggregated})
			case query.BooleanIterator:
				p, err := x.Next()
				if err != nil {
					t.Fatal(err)
				}
				if p == nil {
					return out, tr
				}
				out = append(out, row{p.Time, p.Value, p.Aggregated})
			default:
				t.Fatalf("unexpected iterator %T", it)
			}
		}
	}
	check := func(field, op string, width, offset time.Duration) (*aism.Trace, *aism.Trace) {
		t.Helper()
		want, _ := read("native", field, op, width, offset)
		var traces []*aism.Trace
		for _, mode := range []string{"raw", "leaf", "full"} {
			got, tr := read(mode, field, op, width, offset)
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("%s(%s), mode %s, native=%v indexed=%v", op, field, mode, want, got)
			}
			traces = append(traces, tr)
		}
		return traces[1], traces[2]
	}
	leaf, full := check("i", "sum", 24*time.Hour, 0)
	if full.CrossFileMerged[1].Load() == 0 || full.Merged[1].Load() == 0 || leaf.Merged[0].Load() <= full.Merged[1].Load() || full.RawDecoded.Load() != 0 {
		t.Fatalf("leaf %s full %s", leaf.JSON(0), full.JSON(0))
	}
	fileRows, fileOnly := read("file", "i", "sum", 24*time.Hour, 0)
	nativeRows, _ := read("native", "i", "sum", 24*time.Hour, 0)
	if !reflect.DeepEqual(fileRows, nativeRows) || fileOnly.Merged[1].Load() != 0 || fileOnly.Merged[0].Load() != leaf.Merged[0].Load() {
		t.Fatalf("file-local control %s", fileOnly.JSON(0))
	}
	if err = e.Reopen(); err != nil {
		t.Fatal(err)
	}
	e.CompactionPlan = &mockPlanner{}
	_, full = check("i", "sum", 24*time.Hour, 0)
	if full.CrossFileMerged[1].Load() == 0 {
		t.Fatal("reopen lost cross-file records")
	}
	for _, f := range []string{"i", "f", "b"} {
		for _, op := range []string{"count", "sum", "mean", "min", "max", "first", "last"} {
			if f == "b" && op != "count" && op != "first" && op != "last" {
				continue
			}
			check(f, op, 3*time.Hour, 17*time.Second)
		}
	}
	if err := e.WritePointsString(fmt.Sprintf("cpu,hostname=h i=7i %d", start+n*int64(time.Second))); err != nil {
		t.Fatal(err)
	}
	_, full = check("i", "sum", 24*time.Hour, 0)
	if full.CachePoints.Load() != 1 || full.Merged[1].Load() == 0 {
		t.Fatal("disjoint buffered tail did not join the indexed prefix")
	}
	// The newest value at the same timestamp lives in cache, then a second TSM generation.
	if err := e.WritePointsString(fmt.Sprintf("cpu,hostname=h i=-9i %d", start)); err != nil {
		t.Fatal(err)
	}
	_, full = check("i", "sum", 24*time.Hour, 0)
	if full.UsedSeries.Load() != 0 {
		t.Fatal("read stale summary over cache")
	}
	e.MustWriteSnapshot()
	_, full = check("i", "sum", 24*time.Hour, 0)
	if full.UsedSeries.Load() != 0 {
		t.Fatal("read stale summary over newer TSM")
	}
	if err = e.DeleteSeriesRange(ctx, &seriesIterator{keys: [][]byte{[]byte("cpu,hostname=h")}}, start+int64(time.Hour), start+2*int64(time.Hour)); err != nil {
		t.Fatal(err)
	}
	check("i", "sum", time.Hour, 0)
	var paths []string
	for _, f := range e.FileStore.Files() {
		paths = append(paths, f.Path())
	}
	out, err := e.Compactor.CompactFull(paths, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err = e.FileStore.Replace(paths, out); err != nil {
		t.Fatal(err)
	}
	_, full = check("i", "sum", 24*time.Hour, 0)
	if full.Merged[1].Load() == 0 {
		t.Fatalf("compaction did not install hierarchy: %s", full.JSON(0))
	}
	if err = e.Reopen(); err != nil {
		t.Fatal(err)
	}
	_, full = check("i", "sum", 24*time.Hour, 0)
	if full.Merged[1].Load() == 0 {
		t.Fatal("reopen did not recover hierarchy")
	}
	// Only one generation remains. The tombstone alone must disable the
	// previously usable parent, even though its immutable TSM is unchanged.
	if err = e.DeleteSeriesRange(ctx, &seriesIterator{keys: [][]byte{[]byte("cpu,hostname=h")}}, start+4*int64(time.Hour), start+5*int64(time.Hour)); err != nil {
		t.Fatal(err)
	}
	_, full = check("i", "sum", 24*time.Hour, 0)
	if full.UsedSeries.Load() != 0 || full.NativeDecoded.Load() == 0 {
		t.Fatal("expected native decoding over partial deletion after reopen")
	}
}
