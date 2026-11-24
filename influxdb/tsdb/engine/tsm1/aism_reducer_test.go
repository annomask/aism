package tsm1_test

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
)

// Check that merging across shards preserves the native multi-series SUM.
func TestAISMCrossShardFloatSumReducerBoundary(t *testing.T) {
	t.Setenv("INFLUXDB_AISM_BUILD", "1")
	ctx, root := context.Background(), t.TempDir()
	store := tsdb.NewStore(filepath.Join(root, "data"))
	store.EngineOptions.IndexVersion = "tsi1"
	store.EngineOptions.Config.WALDir = filepath.Join(root, "wal")
	require.NoError(t, store.Open(ctx))
	defer store.Close()
	for id := uint64(1); id <= 2; id++ {
		require.NoError(t, store.CreateShard(ctx, "bucket", "autogen", id, true))
		store.Shard(id).SetCompactionsEnabled(false)
		large := float64(1 << 54)
		if id == 2 {
			large = -large
		}
		stamp := int64(id) * int64(time.Second)
		text := fmt.Sprintf("cpu,host=a f=%.1f %d\ncpu,host=b f=1.0 %d\n", large, stamp, stamp)
		points, err := models.ParsePointsString(text)
		require.NoError(t, err)
		require.NoError(t, store.WriteToShard(ctx, id, points))
		engine, err := store.Shard(id).Engine()
		require.NoError(t, err)
		require.NoError(t, engine.(*tsm1.Engine).WriteSnapshot())
	}
	read := func(mode string, dimensions []string) ([]float64, *aism.Trace) {
		qctx, trace := aism.WithTrace(ctx, mode)
		opt := query.IteratorOptions{Expr: influxql.MustParseExpr("sum(f)"), Interval: query.Interval{Duration: time.Hour}, Dimensions: dimensions, Ascending: true, StartTime: 0, EndTime: int64(time.Hour) - 1, Fill: influxql.NoFill, Authorizer: query.OpenAuthorizer}
		it, err := store.ShardGroup([]uint64{1, 2}).CreateIterator(qctx, &influxql.Measurement{Name: "cpu"}, opt)
		require.NoError(t, err)
		require.NotNil(t, it)
		defer it.Close()
		var rows []float64
		for {
			p, err := it.(query.FloatIterator).Next()
			require.NoError(t, err)
			if p == nil {
				return rows, trace
			}
			rows = append(rows, p.Value)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, tr := read("cross", []string{"host"})
		if tr.CrossCatalogReady.Load() && tr.UsedSeries.Load() == 2 && tr.IteratorCalls.Load() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cross catalog not ready: %s", tr.JSON(0))
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, dims := range [][]string{nil, {"host"}} {
		want, _ := read("native", dims)
		for _, mode := range []string{"full", "cross"} {
			got, tr := read(mode, dims)
			t.Logf("dimensions=%v mode=%s native=%v actual=%v trace=%s", dims, mode, want, got, tr.JSON(0))
			if !reflect.DeepEqual(want, got) {
				t.Errorf("SUM differs from native for dimensions=%v mode=%s: native=%v actual=%v", dims, mode, want, got)
			}
		}
	}
}
