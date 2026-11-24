package tsm1

import (
	"sort"
	"sync"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
)

// Every mutable component is copied before the visibility revision is checked.
// Pinned readers keep original block offsets valid after native replacement.
type aismReadView struct {
	cached     Values
	files      []TSMFile
	catalog    *aism.Catalog
	contains   map[TSMFile]bool
	tombstones map[TSMFile][]TimeRange
	shards     map[TSMFile]uint64
	once       sync.Once
}

func (v *aismReadView) Close() {
	if v == nil {
		return
	}
	v.once.Do(func() {
		for _, file := range v.files {
			file.Unref()
		}
	})
}

// captureAISMView returns false before emitting any result when a stable native
// view cannot be obtained. The caller can then construct the original cursor.
func (e *Engine) captureAISMView(key []byte, lo, hi int64) (*aismReadView, bool) {
	return e.captureAISMViewAfterCache(key, lo, hi, nil)
}

// The optional callback provides a deterministic interleaving point for tests.
func (e *Engine) captureAISMViewAfterCache(key []byte, lo, hi int64, afterCache func()) (*aismReadView, bool) {
	for attempt := 0; attempt < 2; attempt++ {
		revision, stable := e.FileStore.aismVisibility.startRead()
		if !stable {
			return nil, false
		}
		v := &aismReadView{
			contains:   make(map[TSMFile]bool),
			tombstones: make(map[TSMFile][]TimeRange),
		}
		v.cached = e.Cache.Values(key)
		first := sort.Search(len(v.cached), func(i int) bool { return v.cached[i].UnixNano() >= lo })
		last := sort.Search(len(v.cached), func(i int) bool { return v.cached[i].UnixNano() > hi })
		v.cached = v.cached[first:last]
		if afterCache != nil {
			afterCache()
		}
		if !e.FileStore.pinAISMView(v, key, lo, hi) {
			v.Close()
			return nil, false
		}
		for _, file := range v.files {
			v.contains[file] = file.Contains(key)
			if r, ok := file.(*TSMReader); ok {
				ranges, safe := r.aismTombstoneSnapshot(key)
				if !safe {
					v.Close()
					return nil, false
				}
				v.tombstones[file] = ranges
			} else if v.contains[file] {
				v.Close()
				return nil, false
			}
		}
		if e.FileStore.aismVisibility.valid(revision) {
			return v, true
		}
		v.Close()
	}
	return nil, false
}

func (f *FileStore) pinAISMView(v *aismReadView, key []byte, lo, hi int64) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.newReadersBlocked() {
		return false
	}
	for _, file := range f.files {
		if file.OverlapsTimeRange(lo, hi) {
			file.Ref()
			v.files = append(v.files, file)
		}
	}
	if f.aismCatalogGeneration == f.aismGeneration {
		v.catalog = f.aismCatalog
	}
	return true
}

func (e *Engine) clearAISMSnapshot(success bool) {
	change := e.FileStore.aismVisibility.begin()
	defer change.finish()
	e.Cache.ClearSnapshot(success)
}

// The native API returns a borrowed range slice. Copy while holding the index
// lock, since checking a revision after copying cannot repair a Go data race.
func (r *TSMReader) aismTombstoneSnapshot(key []byte) ([]TimeRange, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if index, ok := r.index.(*indirectIndex); ok {
		index.mu.RLock()
		defer index.mu.RUnlock()
		return append([]TimeRange(nil), index.tombstones[string(key)]...), true
	}
	return nil, false
}
