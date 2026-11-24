package tsm1

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
	"go.uber.org/zap"
)

// The registry coordinates only registration and publication notifications.
// Queries, native writes, file IO and tree construction never hold its lock.
var aismBuckets = struct {
	sync.Mutex
	items map[string]*aismBucket
}{items: make(map[string]*aismBucket)}

type aismBucket struct {
	dir     string
	mu      sync.Mutex
	members map[*FileStore]bool
	epoch   uint64
	build   sync.Mutex
	version atomic.Pointer[aismBucketVersion]
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
}

type aismBucketMember struct {
	store      *FileStore
	generation uint64
}

type aismBucketVersion struct {
	epoch       uint64
	members     []aismBucketMember
	generations map[*FileStore]uint64
	catalog     *aism.Catalog
	loaded      bool
	buildNS     uint64
	sources     int
}

func registerAISMBucket(f *FileStore) {
	if !aism.Enabled() {
		return
	}
	dir := filepath.Dir(f.dir)
	aismBuckets.Lock()
	b := aismBuckets.items[dir]
	if b == nil {
		b = &aismBucket{dir: dir, members: make(map[*FileStore]bool), wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
		aismBuckets.items[dir] = b
		go b.run()
	}
	b.mu.Lock()
	if !b.members[f] {
		b.members[f] = true
		b.epoch++
	}
	b.mu.Unlock()
	aismBuckets.Unlock()
	b.signal()
}

func unregisterAISMBucket(f *FileStore) {
	aismBuckets.Lock()
	b := aismBuckets.items[filepath.Dir(f.dir)]
	empty := false
	if b != nil {
		b.mu.Lock()
		delete(b.members, f)
		b.epoch++
		empty = len(b.members) == 0
		b.mu.Unlock()
		if empty {
			delete(aismBuckets.items, b.dir)
			close(b.stop)
		} else {
			b.signal()
		}
	}
	aismBuckets.Unlock()
	if empty {
		<-b.done
	}
}

func findAISMBucket(f *FileStore) *aismBucket {
	aismBuckets.Lock()
	b := aismBuckets.items[filepath.Dir(f.dir)]
	aismBuckets.Unlock()
	return b
}

func notifyAISMBucket(f *FileStore) {
	if b := findAISMBucket(f); b != nil {
		b.signal()
	}
}

func (b *aismBucket) signal() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

func (b *aismBucket) run() {
	defer close(b.done)
	for {
		select {
		case <-b.stop:
			return
		case <-b.wake:
			b.refresh()
		}
	}
}

func (b *aismBucket) valid(v *aismBucketVersion) bool {
	if v == nil {
		return false
	}
	b.mu.Lock()
	valid := b.epoch == v.epoch && len(b.members) == len(v.members) && len(b.members) != 0
	b.mu.Unlock()
	if !valid {
		return false
	}
	for _, m := range v.members {
		m.store.mu.RLock()
		valid = m.store.aismGeneration == m.generation && !m.store.newReadersBlocked()
		m.store.mu.RUnlock()
		if !valid {
			return false
		}
	}
	return true
}

func (b *aismBucket) current() *aismBucketVersion {
	v := b.version.Load()
	if b.valid(v) {
		return v
	}
	return nil
}

// refresh joins the database's complete registered file set, independent of
// query ranges and SQL windows. Only builders wait on build; native IO does not.
func (b *aismBucket) refresh() (result *aismBucketVersion) {
	b.build.Lock()
	defer b.build.Unlock()
	if v := b.current(); v != nil {
		return v
	}
	started := time.Now()
	defer func() {
		if value := recover(); value != nil {
			if aism.ReadFailure(value) == nil {
				panic(value)
			}
			result = nil
		}
	}()
	b.mu.Lock()
	v := &aismBucketVersion{epoch: b.epoch, generations: make(map[*FileStore]uint64)}
	for f := range b.members {
		v.members = append(v.members, aismBucketMember{store: f})
	}
	b.mu.Unlock()
	if len(v.members) == 0 {
		return nil
	}
	sort.Slice(v.members, func(i, j int) bool { return v.members[i].store.dir < v.members[j].store.dir })
	registered := make(map[string]bool, len(v.members))
	for _, m := range v.members {
		registered[filepath.Base(m.store.dir)] = true
	}
	// During startup, do not overwrite the complete persisted catalog with a
	// partial set while other shard engines are still opening.
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		if _, err := strconv.ParseUint(entry.Name(), 10, 64); err != nil || !entry.IsDir() || registered[entry.Name()] {
			continue
		}
		files, err := filepath.Glob(filepath.Join(b.dir, entry.Name(), "*.tsm"))
		if err != nil || len(files) > 0 {
			return nil
		}
	}
	var sources []aism.Source
	for i := range v.members {
		m := &v.members[i]
		f := m.store
		f.mu.RLock()
		m.generation = f.aismGeneration
		v.generations[f] = m.generation
		readers := make([]*TSMReader, 0, len(f.files))
		for _, file := range f.files {
			if r, ok := file.(*TSMReader); ok && r.aismIndex != nil {
				readers = append(readers, r)
			}
		}
		f.mu.RUnlock()
		for _, r := range readers {
			r.aismIdentity.Do(func() { r.aismID = aism.Identify(r.aismIndex) })
			name := filepath.Base(f.dir) + "/" + filepath.Base(r.Path())
			sources = append(sources, aism.Source{Name: name, ID: r.aismID, Index: r.aismIndex})
		}
	}
	path := filepath.Join(b.dir, "cross-shard.aism")
	previous := b.version.Load()
	var old *aism.Catalog
	if previous != nil {
		old = previous.catalog
	} else {
		v.catalog, _ = aism.LoadCatalog(path, sources)
		v.loaded = v.catalog != nil
	}
	if v.catalog == nil {
		v.catalog = aism.BuildCatalog(sources, old)
	}
	if !b.valid(v) {
		b.signal()
		return nil
	}
	v.buildNS, v.sources = uint64(time.Since(started)), len(sources)
	b.version.Store(v)
	if !v.loaded {
		if err := aism.SaveCatalog(path, v.catalog); err != nil {
			v.members[0].store.logger.Warn("Cross-shard aggregate catalog not persisted", zap.String("path", path), zap.Error(err))
		}
	}
	return v
}
