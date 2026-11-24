package tsm1

import (
	"context"
	"path/filepath"
	"sort"
	"time"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
	"github.com/influxdata/influxql"
)

// CreateGroupIterator is an optional engine capability invoked before shard
// fanout. An unsuccessful attempt closes every read lease before returning.
func (e *Engine) CreateGroupIterator(ctx context.Context, name string, opt query.IteratorOptions, group []tsdb.Engine) (query.Iterator, bool, error) {
	if aism.Mode(ctx) != "cross" || len(group) < 2 {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	parent := aism.FromContext(ctx)
	fallback := func(reason string) (query.Iterator, bool, error) {
		if parent != nil {
			parent.RecordFallback(reason)
		}
		return nil, false, nil
	}
	call, ok := opt.Expr.(*influxql.Call)
	if !ok || len(call.Args) != 1 || aismOps[call.Name] == 0 || opt.Interval.IsZero() || !opt.Ascending || opt.Fill != influxql.NoFill || opt.Limit != 0 || opt.Offset != 0 || opt.SLimit != 0 || opt.SOffset != 0 || len(opt.Aux) != 0 || opt.Dedupe {
		return fallback("cross_unsupported_query")
	}
	ref, ok := call.Args[0].(*influxql.VarRef)
	if !ok {
		return fallback("cross_unsupported_query")
	}
	// Native MEAN divides inside each shard and multiplies those rounded means
	// during the outer merge. A single global division is not bit-equivalent.
	if call.Name == "mean" {
		return fallback("cross_mean_reducer_boundary")
	}
	b := findAISMBucket(e.FileStore)
	if b == nil {
		return fallback("cross_unregistered_bucket")
	}
	engines := make([]*Engine, 0, len(group))
	indexes := make([]tsdb.Index, 0, len(group))
	var owner *Engine
	var kind influxql.DataType
	seen := make(map[*Engine]bool, len(group))
	for _, engine := range group {
		member, ok := engine.(*Engine)
		if !ok || member.sfile != e.sfile || filepath.Dir(member.path) != b.dir || seen[member] {
			return fallback("cross_incompatible_shards")
		}
		seen[member] = true
		engines = append(engines, member)
		indexes = append(indexes, member.index)
		if fields := member.fieldset.FieldsByString(name); fields != nil {
			if field := fields.Field(ref.Val); field != nil {
				if owner != nil && kind != field.Type {
					return fallback("cross_field_type_conflict")
				}
				owner, kind = member, field.Type
			}
		}
	}
	if owner == nil {
		return fallback("field_metadata")
	}
	started := time.Now()
	version := b.current()
	if version == nil {
		// Publication is asynchronous. Queries never wait for a catalog build.
		b.signal()
	}
	if parent != nil {
		parent.CrossCatalogNS.Add(uint64(time.Since(started)))
		if version != nil && version.loaded {
			parent.CrossCatalogLoads.Add(1)
		}
	}
	if version == nil {
		return fallback("cross_catalog_unavailable")
	}
	if parent != nil {
		parent.CrossCatalogReady.Store(true)
		parent.CrossCatalogSources.Store(uint64(version.sources))
		parent.CrossCatalogBuildNS.Store(version.buildNS)
	}
	indexSet := tsdb.IndexSet{Indexes: indexes, SeriesFile: e.sfile}
	tags, err := indexSet.TagSets(e.sfile, []byte(name), opt)
	if err != nil {
		return nil, true, err
	}
	// Per-series exactness does not prove equivalence after changing the order
	// of the native shard and series reducers. Keep that native reduction order.
	if call.Name == "sum" && kind == influxql.Float {
		for _, tag := range tags {
			if len(tag.SeriesKeys) > 1 {
				return fallback("cross_float_sum_reducer_boundary")
			}
		}
	}
	accepted := false
	ctx, local := aism.WithTrace(ctx, "cross")
	defer func() { parent.MergeWork(local, accepted) }()
	var iters []query.Iterator
	defer func() {
		if !accepted {
			query.Iterators(iters).Close()
		}
	}()
	for _, tag := range tags {
		if len(tag.SeriesKeys) != len(tag.Filters) {
			return fallback("cross_series_filters")
		}
		for i, key := range tag.SeriesKeys {
			if err := ctx.Err(); err != nil {
				return nil, true, err
			}
			select {
			case <-opt.InterruptCh:
				return nil, true, query.ErrQueryInterrupted
			default:
			}
			if opt.MaxSeriesN > 0 && len(iters) >= opt.MaxSeriesN {
				return fallback("cross_series_limit")
			}
			series := *tag
			series.SeriesKeys = []string{key}
			series.Filters = []influxql.Expr{tag.Filters[i]}
			started := time.Now()
			view, stable := captureAISMGroup(engines, SeriesFieldKeyBytes(key, ref.Val), opt.StartTime, opt.EndTime, version)
			local.CaptureNS.Add(uint64(time.Since(started)))
			if !stable {
				return fallback("cross_unstable_view")
			}
			itr, handled, err := owner.aismSeriesView(ctx, name, ref, call, &series, opt, view)
			view.Close()
			if err != nil {
				return nil, true, err
			}
			if !handled {
				return nil, false, nil
			}
			if itr != nil {
				iters = append(iters, itr)
			}
		}
	}
	itr, err := query.Iterators(iters).Merge(opt)
	if err != nil {
		return nil, true, err
	}
	accepted = true
	if parent != nil {
		parent.IteratorCalls.Add(1)
	}
	return itr, true, nil
}

// Cache, tombstone and file publications must all fit inside the version-vector
// interval. After validation, owned cache copies and pinned TSM readers survive
// later mutations. The database still supplies its native per-series isolation.
func captureAISMGroup(engines []*Engine, key []byte, lo, hi int64, version *aismBucketVersion) (*aismReadView, bool) {
	return captureAISMGroupAfterCapture(engines, key, lo, hi, version, nil)
}

func captureAISMGroupAfterCapture(engines []*Engine, key []byte, lo, hi int64, version *aismBucketVersion, afterCapture func()) (*aismReadView, bool) {
	for attempt := 0; attempt < 2; attempt++ {
		revisions := make([]uint64, len(engines))
		for i, engine := range engines {
			var stable bool
			revisions[i], stable = engine.FileStore.aismVisibility.startRead()
			if !stable {
				return nil, false
			}
		}
		view := &aismReadView{catalog: version.catalog, contains: make(map[TSMFile]bool), tombstones: make(map[TSMFile][]TimeRange), shards: make(map[TSMFile]uint64)}
		for _, engine := range engines {
			part, ok := engine.captureAISMView(key, lo, hi)
			if !ok {
				view.Close()
				return nil, false
			}
			view.cached = append(view.cached, part.cached...)
			view.files = append(view.files, part.files...)
			for _, file := range part.files {
				view.contains[file] = part.contains[file]
				view.tombstones[file] = part.tombstones[file]
				view.shards[file] = engine.id
			}
			// Ownership of the references transfers to the combined view.
			part.files = nil
			part.Close()
		}
		if afterCapture != nil {
			afterCapture()
		}
		stable := true
		for i, engine := range engines {
			stable = engine.FileStore.aismVisibility.valid(revisions[i]) && stable
			// A closed store may have a stable, empty file list and residual cache.
			// Bind the captured source to the catalog generation, including empty
			// stores, so an unload cannot silently drop its disk contribution.
			generation, present := version.generations[engine.FileStore]
			engine.FileStore.mu.RLock()
			bound := present && engine.FileStore.aismGeneration == generation
			engine.FileStore.mu.RUnlock()
			stable = bound && stable
		}
		if stable {
			sort.Slice(view.cached, func(i, j int) bool { return view.cached[i].UnixNano() < view.cached[j].UnixNano() })
			for i := 1; i < len(view.cached); i++ {
				if view.cached[i-1].UnixNano() == view.cached[i].UnixNano() {
					view.Close()
					return nil, false
				}
			}
			return view, true
		}
		view.Close()
	}
	return nil, false
}
