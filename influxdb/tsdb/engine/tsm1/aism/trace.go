package aism

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type traceKey struct{}
type CrossShardSample struct {
	Level      int    `json:"level"`
	Bucket     int64  `json:"bucket"`
	Start      int64  `json:"start"`
	End        int64  `json:"end"`
	Points     uint64 `json:"points"`
	FirstShard uint64 `json:"first_shard"`
	LastShard  uint64 `json:"last_shard"`
}

type Trace struct {
	ApplicabilityNS                                                 atomic.Uint64
	CrossCatalogReady                                               atomic.Bool
	CrossCatalogSources, CrossCatalogBuildNS                        atomic.Uint64
	sampleMu                                                        sync.Mutex
	samples                                                         []CrossShardSample
	DiscardedSummaries                                              atomic.Uint64
	CaptureNS, ReadNS, DecodeNS, CacheNS, WalkNS                    atomic.Uint64
	Visited                                                         [MaxLevels]atomic.Uint64
	IndexPagesDecoded, IndexColumnsDecoded, IndexPayloadBytes       atomic.Uint64
	fallbackMu                                                      sync.Mutex
	fallbackReasons                                                 map[string]uint64
	CachePoints                                                     atomic.Uint64
	Mode                                                            string
	IteratorCalls, UsedSeries, Fallbacks, NativeDecoded, RawDecoded atomic.Uint64
	Merged                                                          [MaxLevels]atomic.Uint64
	CrossFileMerged                                                 [MaxLevels]atomic.Uint64
	CrossShardMerged                                                [MaxLevels]atomic.Uint64
	CrossCatalogNS, CrossCatalogLoads, CrossCatalogBuilds           atomic.Uint64
}

func (t *Trace) RecordCrossShard(level int, bucket int64, state State, first, last uint64) {
	if t == nil {
		return
	}
	t.sampleMu.Lock()
	if len(t.samples) < 16 {
		t.samples = append(t.samples, CrossShardSample{level, bucket, state.Start, state.End, state.N, first, last})
	}
	t.sampleMu.Unlock()
}

// RecordFallback runs once per unsupported series expression.
func (t *Trace) RecordFallback(reason string) {
	t.fallbackMu.Lock()
	if t.fallbackReasons == nil {
		t.fallbackReasons = make(map[string]uint64)
	}
	t.fallbackReasons[reason]++
	t.fallbackMu.Unlock()
}

func FromContext(ctx context.Context) *Trace {
	t, _ := ctx.Value(traceKey{}).(*Trace)
	return t
}
func WithTrace(ctx context.Context, mode string) (context.Context, *Trace) {
	t := &Trace{Mode: mode}
	return context.WithValue(ctx, traceKey{}, t), t
}
func Mode(ctx context.Context) string {
	if t := FromContext(ctx); t != nil && t.Mode != "" {
		return t.Mode
	}
	return os.Getenv("INFLUXDB_AISM_MODE")
}
func Level(mode string) int {
	switch mode {
	case "full", "file", "cross":
		return MaxLevels
	case "leaf":
		return 1
	case "raw":
		return 0
	default:
		return -1
	}
}
func Enabled() bool {
	return Level(os.Getenv("INFLUXDB_AISM_MODE")) >= 0 || os.Getenv("INFLUXDB_AISM_BUILD") == "1"
}
func (t *Trace) JSON(elapsed time.Duration) string {
	t.sampleMu.Lock()
	samples := append([]CrossShardSample(nil), t.samples...)
	t.sampleMu.Unlock()
	t.fallbackMu.Lock()
	reasons := make(map[string]uint64, len(t.fallbackReasons))
	for k, v := range t.fallbackReasons {
		reasons[k] = v
	}
	t.fallbackMu.Unlock()
	var merged, cross, shards, visited [MaxLevels]uint64
	highest := 0
	for i := range merged {
		visited[i] = t.Visited[i].Load()
		merged[i] = t.Merged[i].Load()
		cross[i] = t.CrossFileMerged[i].Load()
		shards[i] = t.CrossShardMerged[i].Load()
		if merged[i] > 0 {
			highest = i + 1
		}
	}
	native, raw := t.NativeDecoded.Load(), t.RawDecoded.Load()
	data, _ := json.Marshal(map[string]any{
		"cross_catalog_ready": t.CrossCatalogReady.Load(), "cross_catalog_sources": t.CrossCatalogSources.Load(), "cross_catalog_last_build_ns": t.CrossCatalogBuildNS.Load(),
		"cross_shard_samples": samples,
		"cross_shard_merged":  shards, "cross_catalog_ns": t.CrossCatalogNS.Load(), "cross_catalog_loads": t.CrossCatalogLoads.Load(), "cross_catalog_builds": t.CrossCatalogBuilds.Load(),
		"index_records_visited": visited, "index_pages_decoded": t.IndexPagesDecoded.Load(), "index_columns_decoded": t.IndexColumnsDecoded.Load(), "index_payload_bytes": t.IndexPayloadBytes.Load(),
		"stage_ns": map[string]uint64{"query_summary_applicability": t.ApplicabilityNS.Load(), "query_capture_view": t.CaptureNS.Load(), "query_tsm_read": t.ReadNS.Load(), "query_block_decode": t.DecodeNS.Load(), "query_cache_reduce": t.CacheNS.Load(), "query_tree_walk_inclusive": t.WalkNS.Load()},
		"mode":     t.Mode, "fallback_reasons": reasons, "server_ns": elapsed.Nanoseconds(), "iterator_calls": t.IteratorCalls.Load(),
		"used_series": t.UsedSeries.Load(), "fallbacks": t.Fallbacks.Load(), "merged": merged, "cross_file_merged": cross, "highest": highest,
		"discarded_summaries": t.DiscardedSummaries.Load(), "cache_points": t.CachePoints.Load(), "decoded": native + raw, "native_decoded": native, "raw_decoded": raw,
	})
	return string(data)
}

// mergeWork publishes one completed tag-set attempt. Work discarded before a
// native fallback remains visible without claiming accepted summary results.
func (t *Trace) MergeWork(source *Trace, accepted bool) {
	if t == nil || source == nil {
		return
	}
	if accepted {
		source.sampleMu.Lock()
		t.sampleMu.Lock()
		for _, sample := range source.samples {
			if len(t.samples) < 16 {
				t.samples = append(t.samples, sample)
			}
		}
		t.sampleMu.Unlock()
		source.sampleMu.Unlock()
	}
	for i := 0; i < MaxLevels; i++ {
		t.Visited[i].Add(source.Visited[i].Load())
		if accepted {
			t.Merged[i].Add(source.Merged[i].Load())
			t.CrossFileMerged[i].Add(source.CrossFileMerged[i].Load())
			t.CrossShardMerged[i].Add(source.CrossShardMerged[i].Load())
		} else {
			t.DiscardedSummaries.Add(source.Merged[i].Load())
		}
	}
	if accepted {
		t.UsedSeries.Add(source.UsedSeries.Load())
		t.CachePoints.Add(source.CachePoints.Load())
	}
	t.RawDecoded.Add(source.RawDecoded.Load())
	t.NativeDecoded.Add(source.NativeDecoded.Load())
	t.IndexPagesDecoded.Add(source.IndexPagesDecoded.Load())
	t.IndexColumnsDecoded.Add(source.IndexColumnsDecoded.Load())
	t.IndexPayloadBytes.Add(source.IndexPayloadBytes.Load())
	t.CaptureNS.Add(source.CaptureNS.Load())
	t.ApplicabilityNS.Add(source.ApplicabilityNS.Load())
	t.ReadNS.Add(source.ReadNS.Load())
	t.DecodeNS.Add(source.DecodeNS.Load())
	t.CacheNS.Add(source.CacheNS.Load())
	t.WalkNS.Add(source.WalkNS.Load())
	source.fallbackMu.Lock()
	defer source.fallbackMu.Unlock()
	t.fallbackMu.Lock()
	defer t.fallbackMu.Unlock()
	if t.fallbackReasons == nil {
		t.fallbackReasons = make(map[string]uint64)
	}
	for reason, n := range source.fallbackReasons {
		t.fallbackReasons[reason] += n
	}
}
