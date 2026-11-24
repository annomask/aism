package tsm1_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAISMShardGroupLifecycle(t *testing.T) {
	t.Setenv("INFLUXDB_AISM_BUILD", "1")
	root, ctx := t.TempDir(), context.Background()
	var store *tsdb.Store
	open := func() {
		store = tsdb.NewStore(filepath.Join(root, "data"))
		store.EngineOptions.IndexVersion = "tsi1"
		store.EngineOptions.Config.WALDir = filepath.Join(root, "wal")
		require.NoError(t, store.Open(ctx))
		for _, id := range store.ShardIDs() {
			store.Shard(id).SetCompactionsEnabled(false)
		}
	}
	open()
	defer func() { require.NoError(t, store.Close()) }()
	engine := func(id uint64) *tsm1.Engine {
		e, err := store.Shard(id).Engine()
		require.NoError(t, err)
		return e.(*tsm1.Engine)
	}
	write := func(id uint64, text string) {
		points, err := models.ParsePointsString(text)
		require.NoError(t, err)
		require.NoError(t, store.WriteToShard(ctx, id, points))
	}
	for id := uint64(1); id <= 2; id++ {
		require.NoError(t, store.CreateShard(ctx, "bucket", "autogen", id, true))
		store.Shard(id).SetCompactionsEnabled(false)
		var text strings.Builder
		n := 40000
		if id == 1 {
			n = 30000
		}
		for i := 0; i < n; i++ {
			for _, lane := range []string{"a", "b"} {
				fmt.Fprintf(&text, "cpu,host=h,lane=%s i=%di,f=%g,u=%du,b=%t %d\n", lane, i%127, float64(i%17)/8, uint64(1)<<63+uint64(i%31), i%2 == 0, (int64(id-1)*30000+int64(i))*int64(time.Second))
			}
		}
		write(id, text.String())
		require.NoError(t, engine(id).WriteSnapshot())
	}
	type row struct {
		Time  int64
		Tags  string
		Value any
	}
	read := func(mode, expression string, width time.Duration, dimensions []string) ([]row, *aism.Trace) {
		qctx, trace := aism.WithTrace(ctx, mode)
		opt := query.IteratorOptions{Expr: influxql.MustParseExpr(expression), Interval: query.Interval{Duration: width}, Dimensions: dimensions, Ascending: true, StartTime: 0, EndTime: 80000 * int64(time.Second), Fill: influxql.NoFill, Authorizer: query.OpenAuthorizer}
		it, err := store.ShardGroup(store.ShardIDs()).CreateIterator(qctx, &influxql.Measurement{Name: "cpu"}, opt)
		require.NoError(t, err)
		if it == nil {
			return nil, trace
		}
		defer it.Close()
		var rows []row
		for {
			switch it := it.(type) {
			case query.IntegerIterator:
				p, err := it.Next()
				require.NoError(t, err)
				if p == nil {
					return rows, trace
				}
				rows = append(rows, row{p.Time, p.Tags.ID(), p.Value})
			case query.UnsignedIterator:
				p, err := it.Next()
				require.NoError(t, err)
				if p == nil {
					return rows, trace
				}
				rows = append(rows, row{p.Time, p.Tags.ID(), p.Value})
			case query.FloatIterator:
				p, err := it.Next()
				require.NoError(t, err)
				if p == nil {
					return rows, trace
				}
				rows = append(rows, row{p.Time, p.Tags.ID(), p.Value})
			case query.BooleanIterator:
				p, err := it.Next()
				require.NoError(t, err)
				if p == nil {
					return rows, trace
				}
				rows = append(rows, row{p.Time, p.Tags.ID(), p.Value})
			default:
				t.Fatalf("unexpected iterator %T", it)
			}
		}
	}
	check := func(expression string, width time.Duration, dimensions []string) *aism.Trace {
		want, _ := read("native", expression, width, dimensions)
		got, trace := read("cross", expression, width, dimensions)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%s native=%v cross=%v trace=%s", expression, want, got, trace.JSON(0))
		}
		return trace
	}
	day := 24 * time.Hour
	dims := []string{"host", "lane"}
	waitReady := func() {
		require.Eventually(t, func() bool {
			_, tr := read("cross", "count(i)", day, dims)
			return tr.CrossCatalogReady.Load()
		}, 10*time.Second, 10*time.Millisecond)
	}
	waitReady()
	trace := check("count(i)", day, dims)
	require.Positive(t, trace.CrossShardMerged[1].Load(), trace.JSON(0))
	require.Zero(t, trace.RawDecoded.Load(), trace.JSON(0))
	_, full := read("full", "count(i)", day, dims)
	require.Zero(t, full.Merged[1].Load())
	for _, field := range []string{"i", "f", "u", "b"} {
		for _, op := range []string{"count", "sum", "mean", "min", "max", "first", "last"} {
			if field == "b" && op != "count" && op != "first" && op != "last" {
				continue
			}
			for _, dimensions := range [][]string{dims, {"host"}, nil} {
				check(op+"("+field+")", day, dimensions)
				check(op+"("+field+")", time.Hour, dimensions)
			}
		}
	}
	catalog := filepath.Join(root, "data", "bucket", "autogen", "cross-shard.aism")
	_, err := os.Stat(catalog)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	open()
	waitReady()
	trace = check("count(i)", day, dims)
	require.Positive(t, trace.CrossCatalogLoads.Load(), trace.JSON(0))
	require.Positive(t, trace.CrossShardMerged[1].Load(), trace.JSON(0))
	write(1, "cpu,host=h,lane=a i=-99i 0")
	trace = check("sum(i)", day, dims)
	require.Zero(t, trace.CrossShardMerged[1].Load(), trace.JSON(0))
	require.NoError(t, engine(1).WriteSnapshot())
	trace = check("sum(i)", day, dims)
	require.Zero(t, trace.CrossShardMerged[1].Load(), trace.JSON(0))
	require.NoError(t, engine(2).DeleteSeriesRange(ctx, &seriesIterator{keys: [][]byte{[]byte("cpu,host=h,lane=a")}}, 50000*int64(time.Second), 50100*int64(time.Second)))
	check("sum(i)", day, dims)
	for _, id := range []uint64{1, 2} {
		e := engine(id)
		var paths []string
		for _, f := range e.FileStore.Files() {
			paths = append(paths, f.Path())
		}
		e.Compactor.Open()
		out, err := e.Compactor.CompactFull(paths, zap.NewNop())
		require.NoError(t, err)
		require.NoError(t, e.FileStore.Replace(paths, out))
	}
	trace = check("sum(i)", day, dims)
	waitReady()
	trace = check("sum(i)", day, dims)
	require.Positive(t, trace.CrossShardMerged[1].Load(), trace.JSON(0))
	for _, damage := range []string{"corrupt", "missing", "publish_failure"} {
		require.NoError(t, store.Close())
		if damage == "corrupt" {
			require.NoError(t, os.WriteFile(catalog, []byte("damaged"), 0600))
		} else {
			require.NoError(t, os.Remove(catalog))
			if damage == "publish_failure" {
				require.NoError(t, os.Mkdir(catalog, 0700))
			}
		}
		open()
		waitReady()
		trace = check("sum(i)", day, dims)
		require.Positive(t, trace.CrossShardMerged[1].Load(), trace.JSON(0))
		if damage == "publish_failure" {
			require.NoError(t, store.Close())
			info, err := os.Stat(catalog)
			require.NoError(t, err)
			require.True(t, info.IsDir(), "publication unexpectedly replaced the blocking directory")
			require.NoError(t, os.Remove(catalog))
			open()
		}
	}
	require.NoError(t, store.DeleteShard(1))
	check("sum(i)", day, dims)
	require.NoError(t, store.CreateShard(ctx, "bucket", "autogen", 1, true))
	store.Shard(1).SetCompactionsEnabled(false)
	write(1, "cpu,host=h,lane=a i=9999i 0")
	require.NoError(t, engine(1).WriteSnapshot())
	check("sum(i)", day, dims)
}
