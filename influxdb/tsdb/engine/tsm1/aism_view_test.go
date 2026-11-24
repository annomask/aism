package tsm1

import (
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb"
)

func TestAISMReadViewRejectsPartialPublication(t *testing.T) {
	e := &Engine{Cache: NewCache(1<<20, tsdb.EngineTags{}), FileStore: NewFileStore(t.TempDir(), tsdb.EngineTags{})}
	key := []byte("cpu,hostname=h#!~#i")
	if err := e.Cache.Write(key, Values{NewIntegerValue(3, 7), NewIntegerValue(8, 11)}); err != nil {
		t.Fatal(err)
	}
	first, second := e.FileStore.aismVisibility.begin(), e.FileStore.aismVisibility.begin()
	if v, ok := e.captureAISMView(key, 4, 12); ok || v != nil {
		t.Fatal("captured a partial publication")
	}
	first.finish()
	if v, ok := e.captureAISMView(key, 4, 12); ok || v != nil {
		t.Fatal("captured while a parallel deletion remained active")
	}
	second.finish()
	v, ok := e.captureAISMView(key, 4, 12)
	if !ok {
		t.Fatal("could not capture a stable view")
	}
	defer v.Close()
	if len(v.cached) != 1 || v.cached[0].UnixNano() != 8 || v.cached[0].Value() != int64(11) {
		t.Fatalf("wrong cache crop: %v", v.cached)
	}
	v.Close()
}

func TestAISMTombstoneSnapshotOwnsRanges(t *testing.T) {
	index := NewIndirectIndex()
	index.tombstones = map[string][]TimeRange{"k": {{Min: 10, Max: 20}}}
	r := &TSMReader{index: index}
	ranges, safe := r.aismTombstoneSnapshot([]byte("k"))
	if !safe {
		t.Fatal("native index snapshot rejected")
	}
	index.mu.Lock()
	index.tombstones["k"][0].Max = 40
	index.mu.Unlock()
	if !reflect.DeepEqual(ranges, []TimeRange{{Min: 10, Max: 20}}) {
		t.Fatal("snapshot shared the mutable tombstone backing array")
	}
}

func TestAISMReadViewRetriesFlushBetweenCacheAndFiles(t *testing.T) {
	t.Setenv("INFLUXDB_AISM_BUILD", "1")
	e := &Engine{Cache: NewCache(1<<20, tsdb.EngineTags{}), FileStore: NewFileStore(t.TempDir(), tsdb.EngineTags{})}
	key := []byte("cpu,hostname=h#!~#i")
	values := Values{NewIntegerValue(3, 7), NewIntegerValue(8, 11)}
	if err := e.Cache.Write(key, values); err != nil {
		t.Fatal(err)
	}
	defer e.FileStore.Close()
	var once sync.Once
	v, ok := e.captureAISMViewAfterCache(key, 0, 20, func() {
		once.Do(func() {
			change := e.FileStore.aismVisibility.begin()
			defer change.finish()
			snapshot, err := e.Cache.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(e.FileStore.dir, "000000001-000000001.tsm.tmp")
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			w, err := NewTSMWriter(f)
			if err != nil {
				t.Fatal(err)
			}
			if err = w.Write(key, snapshot.Values(key)); err != nil {
				t.Fatal(err)
			}
			if err = w.WriteIndex(); err != nil {
				t.Fatal(err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			if err = e.FileStore.Replace(nil, []string{path}); err != nil {
				t.Fatal(err)
			}
			e.Cache.ClearSnapshot(true)
		})
	})
	if !ok || v == nil {
		t.Fatal("stable retry failed")
	}
	defer v.Close()
	if len(v.cached) != 0 || len(v.files) != 1 {
		t.Fatal("duplicated snapshot across cache and TSM")
	}
	r := v.files[0].(*TSMReader)
	if atomic.LoadInt64(&r.refs) != 1 {
		t.Fatal("missing data reference")
	}
	// A replacement can remove the active file while this query retains its old
	// reader. The original physical offsets must remain readable through it.
	old := r.Path()
	mapping := &r.accessor.(*mmapAccessor).b[0]
	if err := e.FileStore.Replace([]string{old}, nil); err != nil {
		t.Fatal(err)
	}
	if &r.accessor.(*mmapAccessor).b[0] != mapping {
		t.Fatal("replacement moved a pinned mapping")
	}
	entries := r.Entries(key)
	got, err := r.ReadAt(&entries[0], nil)
	if err != nil || len(got) != 2 || got[0].Value() != int64(7) {
		t.Fatal("lost pinned data", err)
	}
	v.Close()
	v.Close()
	if atomic.LoadInt64(&r.refs) != 0 {
		t.Fatal("reference leaked")
	}
}

func TestAISMReadViewRetriesDeletionAndOwnsSnapshot(t *testing.T) {
	t.Setenv("INFLUXDB_AISM_BUILD", "1")
	e := &Engine{Cache: NewCache(1<<20, tsdb.EngineTags{}), FileStore: NewFileStore(t.TempDir(), tsdb.EngineTags{})}
	defer e.FileStore.Close()
	key := []byte("cpu,hostname=h#!~#i")
	path := filepath.Join(e.FileStore.dir, "000000001-000000001.tsm.tmp")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewTSMWriter(f)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Write(key, Values{NewIntegerValue(3, 7), NewIntegerValue(8, 11)}); err != nil {
		t.Fatal(err)
	}
	if err = w.WriteIndex(); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if err = e.FileStore.Replace(nil, []string{path}); err != nil {
		t.Fatal(err)
	}
	r := e.FileStore.Files()[0].(*TSMReader)
	var once sync.Once
	v, ok := e.captureAISMViewAfterCache(key, 0, 20, func() {
		once.Do(func() {
			if err := r.DeleteRange([][]byte{key}, 3, 3); err != nil {
				t.Fatal(err)
			}
		})
	})
	if !ok {
		t.Fatal("deletion retry failed")
	}
	defer v.Close()
	ranges := v.tombstones[r]
	if !reflect.DeepEqual(ranges, []TimeRange{{Min: 3, Max: 3}}) {
		t.Fatal("lost delete", ranges)
	}
	if err := r.DeleteRange([][]byte{key}, 4, 4); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ranges, []TimeRange{{Min: 3, Max: 3}}) {
		t.Fatal("changed old snapshot")
	}
}
