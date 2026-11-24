package tsm1

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"math"
	"os"

	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
)

// aismHashFile hashes exactly the bytes successfully accepted by the data file.
// The native writer retains its flush, sync, close and removal interfaces.
type aismHashFile struct {
	*os.File
	digest hash.Hash
}

func (f *aismHashFile) Write(b []byte) (int, error) {
	n, err := f.File.Write(b)
	f.digest.Write(b[:n])
	return n, err
}

func aismValueState(values []Value) aism.State {
	var s aism.State
	if !aism.Enabled() || len(values) == 0 {
		return s
	}
	var kind aism.Kind
	switch values[0].(type) {
	case IntegerValue:
		kind = aism.Integer
	case UnsignedValue:
		kind = aism.Unsigned
	case FloatValue:
		kind = aism.Float
	case BooleanValue:
		kind = aism.Boolean
	}
	if kind == 0 {
		return s
	}
	for _, v := range values {
		s.Merge(kind, aism.Point(kind, v.UnixNano(), aismBits(v)))
	}
	return s
}

// aismArrayState sees the final deduplicated and tombstone-filtered chunk used
// by the native encoder. It never reads or decodes a newly written output.
func aismArrayState(array any) aism.State {
	var s aism.State
	if !aism.Enabled() {
		return s
	}
	add := func(k aism.Kind, t int64, v uint64) { s.Merge(k, aism.Point(k, t, v)) }
	switch a := array.(type) {
	case *tsdb.IntegerArray:
		for i, v := range a.Values {
			add(aism.Integer, a.Timestamps[i], uint64(v))
		}
	case *tsdb.UnsignedArray:
		for i, v := range a.Values {
			add(aism.Unsigned, a.Timestamps[i], v)
		}
	case *tsdb.FloatArray:
		for i, v := range a.Values {
			add(aism.Float, a.Timestamps[i], math.Float64bits(v))
		}
	case *tsdb.BooleanArray:
		for i, v := range a.Values {
			var b uint64
			if v {
				b = 1
			}
			add(aism.Boolean, a.Timestamps[i], b)
		}
	}
	return s
}

func (c *cacheKeyIterator) aismState() aism.State { return c.blocks[c.i][0].aism }
func (c *tsmBatchKeyIterator) aismState() aism.State {
	b := c.merged[0]
	if b.aism.N != 0 {
		return b.aism
	}
	if len(b.b) == 0 || aismKind(b.b[0]) == 0 {
		return aism.State{}
	}
	sources := make([]aism.Index, 0, len(c.readers))
	for _, r := range c.readers {
		sources = append(sources, r.aismIndex)
	}
	entry := IndexEntry{MinTime: b.minTime, MaxTime: b.maxTime}
	if s, ok := existingLeaf(sources, string(b.key), entry, sha256.Sum256(b.b)); ok {
		return s
	}
	// Old native files may not carry trustworthy summaries. This is an input
	// block fallback; output bytes are never reread by the streaming builder.
	values, err := DecodeBlock(b.b, nil)
	if err != nil {
		return aism.State{}
	}
	return aismValueState(values)
}

type aismOutput struct {
	cache   *aism.PageCache
	sources []aism.Index
	index   aism.Index
	key     string
	builder *aism.Builder
	failed  bool
}

func (o *aismOutput) finishKey() {
	if o.builder == nil {
		return
	}
	if !o.failed {
		if t, err := o.builder.Finish(); err == nil {
			o.index[o.key] = t
		}
	}
	o.builder = nil
}
func (o *aismOutput) push(key []byte, kind aism.Kind, state aism.State, entry IndexEntry, encoded []byte) {
	if o.key != string(key) {
		o.finishKey()
		o.key = string(key)
		o.failed = false
	}
	if o.builder == nil {
		if o.cache == nil {
			o.cache = aism.NewPageCache(4 << 20)
		}
		var sources []*aism.Tree
		for _, index := range o.sources {
			if tree := index[string(key)]; tree != nil {
				sources = append(sources, tree)
			}
		}
		o.builder = aism.NewStreamingBuilder(kind, sources, o.cache)
	}
	if o.failed {
		return
	}
	if state.N == 0 || state.Start != entry.MinTime || state.End != entry.MaxTime {
		if os.Getenv("INFLUXDB_AISM_DEBUG") == "1" {
			fmt.Fprintf(os.Stderr, "AISM output mismatch key=%s state=%+v entry=%+v\n", key, state, entry)
		}
		o.failed = true
		return
	}
	if err := o.builder.Push(aism.Node{State: state}, aism.Block{Offset: entry.Offset, Size: entry.Size, Digest: sha256.Sum256(encoded)}); err != nil {
		if os.Getenv("INFLUXDB_AISM_DEBUG") == "1" {
			fmt.Fprintf(os.Stderr, "AISM output builder key=%s error=%v\n", key, err)
		}
		o.failed = true
	}
}

func (c *tsmBatchKeyIterator) aismSourceIndexes() []aism.Index {
	sources := make([]aism.Index, 0, len(c.readers))
	for _, r := range c.readers {
		sources = append(sources, r.aismIndex)
	}
	return sources
}
