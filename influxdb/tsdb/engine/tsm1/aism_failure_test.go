package tsm1_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

func TestAISMOptionalIndexPublicationFailureKeepsNativeData(t *testing.T) {
	t.Setenv("INFLUXDB_AISM_BUILD", "1")
	e, err := NewEngine(t, "tsi1")
	if err != nil {
		t.Fatal(err)
	}
	e.CompactionPlan = &mockPlanner{}
	if err = e.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err = e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("i", influxql.Integer); err != nil {
		t.Fatal(err)
	}
	if err = e.CreateSeriesIfNotExists([]byte("cpu,hostname=h"), []byte("cpu"), models.NewTags(map[string]string{"hostname": "h"})); err != nil {
		t.Fatal(err)
	}
	if err = e.WritePointsString("cpu,hostname=h i=7i 1\ncpu,hostname=h i=11i 2"); err != nil {
		t.Fatal(err)
	}
	// A directory at the optional publication destination makes rename fail.
	blocked := filepath.Join(e.Compactor.Dir, "000000001-000000001.tsm.aism.tmp")
	if err = os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	e.MustWriteSnapshot()
	files := e.FileStore.Files()
	if len(files) != 1 {
		t.Fatal("native snapshot was not installed")
	}
	ctx, trace := aism.WithTrace(context.Background(), "full")
	opt := query.IteratorOptions{Expr: influxql.MustParseExpr("sum(i)"), Interval: query.Interval{Duration: time.Hour}, Dimensions: []string{"hostname"}, Ascending: true, StartTime: 0, EndTime: int64(time.Hour), Fill: influxql.NoFill, Authorizer: query.OpenAuthorizer}
	it, err := e.CreateIterator(ctx, "cpu", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	value, err := it.(query.IntegerIterator).Next()
	if err != nil || value == nil || value.Value != 18 {
		t.Fatal("optional failure damaged native query", value, err)
	}
	if trace.UsedSeries.Load() != 0 {
		t.Fatalf("used an unpublished index: blocked=%s installed=%s", blocked, files[0].Path())
	}
	it.Close()
	if err = e.Reopen(); err != nil {
		t.Fatal(err)
	}
	e.CompactionPlan = &mockPlanner{}
	ctx, _ = aism.WithTrace(context.Background(), "native")
	it, err = e.CreateIterator(ctx, "cpu", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	value, err = it.(query.IntegerIterator).Next()
	if err != nil || value == nil || value.Value != 18 {
		t.Fatal("native durability depended on optional index")
	}
}

func TestAISMFastCompactionCopiesBlocksAndRebindsAddresses(t *testing.T) {
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
	if _, _, err = e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("i", influxql.Integer); err != nil {
		t.Fatal(err)
	}
	if err = e.CreateSeriesIfNotExists([]byte("cpu,hostname=h"), []byte("cpu"), models.NewTags(map[string]string{"hostname": "h"})); err != nil {
		t.Fatal(err)
	}
	for first, last := 0, 30000; first < 70000; first, last = 30000, 70000 {
		var lines strings.Builder
		for i := first; i < last; i++ {
			fmt.Fprintf(&lines, "cpu,hostname=h i=%di %d\n", i+1, int64(i+1)*int64(time.Second))
		}
		if err = e.WritePointsString(lines.String()); err != nil {
			t.Fatal(err)
		}
		e.MustWriteSnapshot()
		if last == 70000 {
			break
		}
	}
	key := []byte("cpu,hostname=h#!~#i")
	digests := map[int64][32]byte{}
	var paths []string
	for _, f := range e.FileStore.Files() {
		paths = append(paths, f.Path())
		for _, entry := range f.Entries(key) {
			_, b, err := f.(*tsm1.TSMReader).ReadBytes(&entry, nil)
			if err != nil {
				t.Fatal(err)
			}
			digests[entry.MinTime] = sha256.Sum256(b)
		}
	}
	out, err := e.Compactor.CompactFast(paths, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err = e.FileStore.Replace(paths, out); err != nil {
		t.Fatal(err)
	}
	blocks := 0
	for _, f := range e.FileStore.Files() {
		for _, entry := range f.Entries(key) {
			_, b, err := f.(*tsm1.TSMReader).ReadBytes(&entry, nil)
			if err != nil {
				t.Fatal(err)
			}
			if digests[entry.MinTime] != sha256.Sum256(b) {
				t.Fatal("fast compaction changed an original full block")
			}
			blocks++
		}
	}
	if blocks != 70 {
		t.Fatal("wrong copied block count", blocks)
	}
	qctx, trace := aism.WithTrace(ctx, "full")
	opt := query.IteratorOptions{Expr: influxql.MustParseExpr("sum(i)"), Interval: query.Interval{Duration: 24 * time.Hour}, Dimensions: []string{"hostname"}, Ascending: true, StartTime: 0, EndTime: int64(24 * time.Hour), Fill: influxql.NoFill, Authorizer: query.OpenAuthorizer}
	it, err := e.CreateIterator(qctx, "cpu", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	p, err := it.(query.IntegerIterator).Next()
	if err != nil || p == nil || p.Value != 70000*70001/2 || trace.Merged[1].Load() != 2 || trace.RawDecoded.Load() != 0 {
		t.Fatalf("wrong rebound query: %v %v %s", p, err, trace.JSON(0))
	}
}

func TestAISMTagSetFallbackDoesNotCountDiscardedSummaries(t *testing.T) {
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
	if _, _, err = e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("i", influxql.Integer); err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"a", "z"} {
		if err = e.CreateSeriesIfNotExists([]byte("cpu,hostname="+host), []byte("cpu"), models.NewTags(map[string]string{"hostname": host})); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.WritePointsString("cpu,hostname=a i=7i 1"); err != nil {
		t.Fatal(err)
	}
	e.MustWriteSnapshot()
	if err = e.WritePointsString("cpu,hostname=z i=11i 2"); err != nil {
		t.Fatal(err)
	}
	qctx, trace := aism.WithTrace(ctx, "full")
	opt := query.IteratorOptions{Expr: influxql.MustParseExpr("sum(i)"), Interval: query.Interval{Duration: time.Hour}, Ascending: true, StartTime: 0, EndTime: int64(time.Hour), Fill: influxql.NoFill, Authorizer: query.OpenAuthorizer}
	it, err := e.CreateIterator(qctx, "cpu", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	p, err := it.(query.IntegerIterator).Next()
	if err != nil || p == nil || p.Value != 18 || trace.UsedSeries.Load() != 0 || trace.Merged[0].Load() != 0 || trace.DiscardedSummaries.Load() == 0 {
		t.Fatalf("discarded results reported as accepted: %v %v %s", p, err, trace.JSON(0))
	}
}
