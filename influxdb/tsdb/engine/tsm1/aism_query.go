package tsm1

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"math"
	"runtime"
	"sort"
	"time"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
	"github.com/influxdata/influxql"
)

var aismOps = map[string]aism.Op{"count": aism.Count, "sum": aism.Sum, "mean": aism.Mean, "min": aism.Min, "max": aism.Max, "first": aism.First, "last": aism.Last}

type aismInput struct {
	file TSMFile
	tree *aism.Tree
}

// A parent can only fit a query window if its physical leaves fit as well.
// Consult the pinned native block directory before paying for a tree walk and
// generic raw decoding that cannot skip any data. Bounds are inclusive in TSM
// entries and the query range, but the bucket end returned by Window is exclusive.
func aismHasCoveredBlock(entries []IndexEntry, opt query.IteratorOptions) bool {
	i := sort.Search(len(entries), func(i int) bool { return entries[i].MinTime >= opt.StartTime })
	for ; i < len(entries) && entries[i].MinTime <= opt.EndTime; i++ {
		entry := entries[i]
		if entry.MaxTime > opt.EndTime {
			continue
		}
		_, end := opt.Window(entry.MinTime)
		if entry.MaxTime < end {
			return true
		}
	}
	return false
}

// Each source series keeps the native reducer boundary before tag-set merging.
func (e *Engine) aismTagSet(ctx context.Context, name string, ref *influxql.VarRef, call *influxql.Call, t *query.TagSet, opt query.IteratorOptions) (query.Iterator, bool, error) {
	if aism.Level(aism.Mode(ctx)) < 0 {
		return nil, false, nil
	}
	if len(t.SeriesKeys) <= 1 {
		return e.aismSeries(ctx, name, ref, call, t, opt)
	}
	if len(t.Filters) != len(t.SeriesKeys) {
		return nil, false, nil
	}
	accepted := false
	if parent := aism.FromContext(ctx); parent != nil {
		var local *aism.Trace
		ctx, local = aism.WithTrace(ctx, aism.Mode(ctx))
		defer func() { parent.MergeWork(local, accepted) }()
	}
	var inputs []query.Iterator
	for i, key := range t.SeriesKeys {
		series := *t
		series.SeriesKeys = []string{key}
		series.Filters = []influxql.Expr{t.Filters[i]}
		it, handled, err := e.aismSeries(ctx, name, ref, call, &series, opt)
		if err != nil {
			query.Iterators(inputs).Close()
			return nil, true, err
		}
		if !handled {
			query.Iterators(inputs).Close()
			return nil, false, nil
		}
		if it != nil {
			inputs = append(inputs, it)
		}
	}
	accepted = true
	return query.NewParallelMergeIterator(inputs, opt, runtime.GOMAXPROCS(0)), true, nil
}

func (e *Engine) aismSeries(ctx context.Context, name string, ref *influxql.VarRef, call *influxql.Call, t *query.TagSet, opt query.IteratorOptions) (query.Iterator, bool, error) {
	return e.aismSeriesView(ctx, name, ref, call, t, opt, nil)
}

func (e *Engine) aismSeriesView(ctx context.Context, name string, ref *influxql.VarRef, call *influxql.Call, t *query.TagSet, opt query.IteratorOptions, supplied *aismReadView) (query.Iterator, bool, error) {
	mode := aism.Mode(ctx)
	level := aism.Level(mode)
	op := aismOps[call.Name]
	trace := aism.FromContext(ctx)
	fallback := func(reason string) (query.Iterator, bool, error) {
		if trace != nil {
			trace.RecordFallback(reason)
		}
		return nil, false, nil
	}
	if level < 0 {
		return nil, false, nil
	}

	if op == 0 || ref == nil || len(call.Args) != 1 || opt.StartTime > opt.EndTime || opt.Interval.Duration <= 0 || !opt.Ascending || len(opt.Aux) > 0 || opt.Limit != 0 || opt.Offset != 0 || opt.Dedupe || len(t.SeriesKeys) != 1 || len(t.Filters) != 1 || opt.Fill != influxql.NoFill {
		return fallback("unsupported_query")
	}
	if filter := t.Filters[0]; filter != nil {
		lit, ok := filter.(*influxql.BooleanLiteral)
		if !ok || !lit.Val {
			return fallback("value_filter")
		}
	}
	fields := e.fieldset.FieldsByString(name)
	if fields == nil {
		return fallback("field_metadata")
	}
	field := fields.Field(ref.Val)
	if field == nil || ref.Type != influxql.Unknown && ref.Type != influxql.AnyField && ref.Type != field.Type {
		return fallback("unsupported_query")
	}
	kind := aism.Integer
	switch field.Type {
	case influxql.Integer:
	case influxql.Unsigned:
		kind = aism.Unsigned
	case influxql.Float:
		kind = aism.Float
	case influxql.Boolean:
		if op != aism.Count && op != aism.First && op != aism.Last {
			return fallback("unsupported_query")
		}
		kind = aism.Boolean
	default:
		return fallback("unsupported_query")
	}
	key := SeriesFieldKeyBytes(t.SeriesKeys[0], ref.Val)
	var captureStart time.Time
	if trace != nil {
		captureStart = time.Now()
	}
	view, ok := supplied, true
	if view == nil {
		view, ok = e.captureAISMView(key, opt.StartTime, opt.EndTime)
	}
	if trace != nil {
		trace.CaptureNS.Add(uint64(time.Since(captureStart)))
	}
	if !ok {
		return fallback("unstable_view")
	}
	defer view.Close()
	cached, files, catalog := view.cached, view.files, view.catalog
	var inputs []aismInput
	var bounds aism.Precision
	points := uint64(len(cached))
	for _, v := range cached {
		switch v.(type) {
		case IntegerValue:
			if kind != aism.Integer {
				return fallback("unsupported_query")
			}
		case UnsignedValue:
			if kind != aism.Unsigned {
				return fallback("unsupported_query")
			}
		case BooleanValue:
			if kind != aism.Boolean {
				return fallback("unsupported_query")
			}
		case FloatValue:
			if kind != aism.Float {
				return fallback("unsupported_query")
			}
			bounds.Include(aism.Point(kind, v.UnixNano(), aismBits(v)).Precision)
		default:
			return fallback("unsupported_query")
		}
	}
	for _, f := range files {
		if !view.contains[f] {
			continue
		}
		r, ok := f.(*TSMReader)
		if !ok || r.aismIndex == nil {
			return fallback("missing_index")
		}
		tree := r.aismIndex[string(key)]
		if tree == nil || tree.Kind != kind {
			return fallback("missing_field_summary")
		}
		s := tree.Summary
		if s.End < opt.StartTime || s.Start > opt.EndTime {
			continue
		}
		i := sort.Search(len(cached), func(i int) bool { return cached[i].UnixNano() >= s.Start })
		if i < len(cached) && cached[i].UnixNano() <= s.End {
			return fallback("cache_overlap")
		}
		for _, d := range view.tombstones[f] {
			if d.Min <= opt.EndTime && d.Max >= opt.StartTime {
				return fallback("tombstone")
			}
		}
		inputs = append(inputs, aismInput{f, tree})
		bounds.Include(s.Precision)
		points += s.N
	}
	if len(inputs) == 0 {
		return fallback("no_disk_summary")
	}
	if op == aism.Mean && points > math.MaxUint32 {
		return fallback("aggregate_count_limit")
	}
	if kind == aism.Float && !bounds.Usable(op, points) {
		return fallback("float_order")
	}
	sort.Slice(inputs, func(i, j int) bool { return inputs[i].tree.Summary.Start < inputs[j].tree.Summary.Start })
	for i := 1; i < len(inputs); i++ {
		if inputs[i-1].tree.Summary.End >= inputs[i].tree.Summary.Start {
			return fallback("tsm_overlap")
		}
	}
	if level > 0 {
		var start time.Time
		if trace != nil {
			start = time.Now()
		}
		covered := false
		var entries []IndexEntry
		for _, input := range inputs {
			input.file.ReadEntries(key, &entries)
			if aismHasCoveredBlock(entries, opt) {
				covered = true
				break
			}
		}
		if trace != nil {
			trace.ApplicabilityNS.Add(uint64(time.Since(start)))
		}
		if !covered {
			return fallback("no_covered_block")
		}
	}
	walkStats := &aism.WalkStats{Op: op}
	var merged, cross, crossShard [aism.MaxLevels]uint64
	var rawDecoded, cachePoints uint64
	var readNS, decodeNS, cacheNS uint64
	defer func() {
		if trace == nil {
			return
		}
		for i := range merged {
			trace.Merged[i].Add(merged[i])
			trace.CrossFileMerged[i].Add(cross[i])
			trace.CrossShardMerged[i].Add(crossShard[i])
			trace.Visited[i].Add(walkStats.Visited[i])
		}
		trace.IndexPagesDecoded.Add(walkStats.PagesDecoded)
		trace.IndexColumnsDecoded.Add(walkStats.ColumnsDecoded)
		trace.IndexPayloadBytes.Add(walkStats.PayloadBytes)
		trace.ReadNS.Add(readNS)
		trace.DecodeNS.Add(decodeNS)
		trace.CacheNS.Add(cacheNS)
		trace.RawDecoded.Add(rawDecoded)
		trace.CachePoints.Add(cachePoints)
	}()
	executionFailure := func(err error) (query.Iterator, bool, error) {
		if ctx.Err() != nil || err == query.ErrQueryInterrupted {
			return nil, true, err
		}
		merged = [aism.MaxLevels]uint64{}
		cross = [aism.MaxLevels]uint64{}
		crossShard = [aism.MaxLevels]uint64{}
		return fallback("index_execution_error")
	}
	var rows []aism.Result
	fold := func(bucket int64, s aism.State) {
		if len(rows) == 0 || rows[len(rows)-1].Bucket != bucket {
			rows = append(rows, aism.Result{})
		}
		rows[len(rows)-1].Fold(kind, op, bucket, s)
	}
	check := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-opt.InterruptCh:
			return query.ErrQueryInterrupted
		default:
			return nil
		}
	}
	use := func(l int, bucket int64, s aism.State) error {
		if err := check(); err != nil {
			return err
		}
		fold(bucket, s)
		merged[l-1]++
		return nil
	}
	consumeCache := func(values Values) error {
		if trace != nil {
			start := time.Now()
			defer func() { cacheNS += uint64(time.Since(start)) }()
		}
		for i, v := range values {
			if i%1024 == 0 {
				if err := check(); err != nil {
					return err
				}
			}
			ts := v.UnixNano()
			bucket, _ := opt.Window(ts)
			fold(bucket, aism.Point(kind, ts, aismBits(v)))
		}
		cachePoints += uint64(len(values))
		return nil
	}
	var buf []Value
	raw := func(file TSMFile, b aism.Block, s aism.State) error {
		if err := check(); err != nil {
			return err
		}
		entry := IndexEntry{MinTime: s.Start, MaxTime: s.End, Offset: b.Offset, Size: b.Size}
		var started time.Time
		if trace != nil {
			started = time.Now()
		}
		checksum, encoded, err := file.(*TSMReader).ReadBytes(&entry, nil)
		if trace != nil {
			readNS += uint64(time.Since(started))
			started = time.Now()
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(encoded) != checksum || sha256.Sum256(encoded) != b.Digest {
			return fmt.Errorf("aggregate block identity mismatch at %d", b.Offset)
		}
		values, err := DecodeBlock(encoded, buf)
		if trace != nil {
			decodeNS += uint64(time.Since(started))
		}

		if err != nil {
			return err
		}
		buf = values
		rawDecoded++
		var start, end int64
		for _, v := range values {
			ts := v.UnixNano()
			if ts < opt.StartTime || ts > opt.EndTime {
				continue
			}
			if ts < start || ts >= end || start == end {
				start, end = opt.Window(ts)
			}
			fold(start, aism.Point(kind, ts, aismBits(v)))
		}
		return nil
	}
	cut := func(ts int64) error {
		n := sort.Search(len(cached), func(i int) bool { return cached[i].UnixNano() >= ts })
		err := consumeCache(cached[:n])
		cached = cached[n:]
		return err
	}
	if err := cut(inputs[0].tree.Summary.Start); err != nil {
		return executionFailure(err)
	}
	var shard *aism.SeriesTree
	if mode != "file" && (len(cached) == 0 || cached[0].UnixNano() > inputs[len(inputs)-1].tree.Summary.End) {
		candidate := catalog.Series(string(key))
		if candidate != nil {
			trees := make([]*aism.Tree, len(inputs))
			for i := range inputs {
				trees[i] = inputs[i].tree
			}
			if candidate.Matches(opt.StartTime, opt.EndTime, trees) {
				shard = candidate
			}
		}
	}
	var walkStart time.Time
	if trace != nil {
		walkStart = time.Now()
		defer func() { trace.WalkNS.Add(uint64(time.Since(walkStart))) }()
	}
	if shard != nil {
		owners := make(map[*aism.Tree]TSMFile, len(inputs))
		for _, in := range inputs {
			owners[in.tree] = in.file
		}
		useShard := func(l int, b int64, s aism.State, spansFiles bool) error {
			if err := use(l, b, s); err != nil {
				return err
			}
			if spansFiles {
				cross[l-1]++
				if view.shards != nil {
					first := sort.Search(len(inputs), func(i int) bool { return inputs[i].tree.Summary.End >= s.Start })
					last := sort.Search(len(inputs), func(i int) bool { return inputs[i].tree.Summary.End >= s.End })
					if first < len(inputs) && last < len(inputs) && view.shards[inputs[first].file] != view.shards[inputs[last].file] {
						crossShard[l-1]++
						if trace != nil {
							trace.RecordCrossShard(l, b, s, view.shards[inputs[first].file], view.shards[inputs[last].file])
						}
					}
				}
			}
			return nil
		}
		if err := shard.WalkWithStats(opt.StartTime, opt.EndTime, level, opt.Window, useShard, func(t *aism.Tree, b aism.Block, s aism.State) error { return raw(owners[t], b, s) }, walkStats); err != nil {
			return executionFailure(err)
		}
	} else {
		for _, in := range inputs {
			if err := cut(in.tree.Summary.Start); err != nil {
				return executionFailure(err)
			}
			if err := in.tree.WalkWithStats(opt.StartTime, opt.EndTime, level, opt.Window, use, func(b aism.Block, s aism.State) error { return raw(in.file, b, s) }, walkStats); err != nil {
				return executionFailure(err)
			}
		}
	}
	if err := consumeCache(cached); err != nil {
		return executionFailure(err)
	}
	if trace != nil {
		trace.UsedSeries.Add(1)
	}
	_, seriesTags := models.ParseKey([]byte(t.SeriesKeys[0]))
	tags := query.NewTags(seriesTags.Map())
	tags = tags.Subset(opt.GetDimensions())
	if opt.StripName {
		name = ""
	}
	return aismResults(name, tags, kind, op, rows), true, nil
}

type aismIterator[P any] struct {
	points        []P
	pos           int
	logicalPoints int
}

func (it *aismIterator[P]) Stats() query.IteratorStats {
	n := it.logicalPoints
	if n == 0 {
		n = it.pos
	}
	return query.IteratorStats{SeriesN: 1, PointN: n}
}
func (it *aismIterator[P]) Close() error {
	it.pos = 0
	it.points = nil
	return nil
}
func (it *aismIterator[P]) Next() (*P, error) {
	if it.pos == len(it.points) {
		return nil, nil
	}
	p := &it.points[it.pos]
	it.pos++
	return p, nil
}
func aismResults(name string, tags query.Tags, k aism.Kind, op aism.Op, rows []aism.Result) query.Iterator {
	logical := uint64(0)
	for _, r := range rows {
		logical += r.N
	}
	if op == aism.Count || k == aism.Integer && op != aism.Mean {
		p := make([]query.IntegerPoint, len(rows))
		for i, r := range rows {
			p[i] = query.IntegerPoint{Name: name, Tags: tags, Time: r.Time, Value: int64(r.Value), Aggregated: uint32(r.N)}
		}
		return &aismIterator[query.IntegerPoint]{points: p, logicalPoints: int(logical)}
	}
	if k == aism.Unsigned && op != aism.Mean {
		p := make([]query.UnsignedPoint, len(rows))
		for i, r := range rows {
			p[i] = query.UnsignedPoint{Name: name, Tags: tags, Time: r.Time, Value: r.Value, Aggregated: uint32(r.N)}
		}
		return &aismIterator[query.UnsignedPoint]{points: p, logicalPoints: int(logical)}
	}
	if k == aism.Boolean {
		p := make([]query.BooleanPoint, len(rows))
		for i, r := range rows {
			p[i] = query.BooleanPoint{Name: name, Tags: tags, Time: r.Time, Value: r.Value != 0, Aggregated: uint32(r.N)}
		}
		return &aismIterator[query.BooleanPoint]{points: p, logicalPoints: int(logical)}
	}
	p := make([]query.FloatPoint, len(rows))
	for i, r := range rows {
		v := math.Float64frombits(r.Value)
		if k == aism.Integer {
			v = float64(int64(r.Value))
		} else if k == aism.Unsigned {
			v = float64(r.Value)
		}
		if op == aism.Mean {
			v /= float64(r.N)
		}
		p[i] = query.FloatPoint{Name: name, Tags: tags, Time: r.Time, Value: v, Aggregated: uint32(r.N)}
	}
	return &aismIterator[query.FloatPoint]{points: p, logicalPoints: int(logical)}
}
