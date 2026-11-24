package tsm1

import (
	"fmt"
	"path/filepath"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
	"go.uber.org/zap"
)

// The native file-set generation owns its catalog. Only immutable in-memory
// indexes escape the store lock. Queries pin physical readers separately.
func (f *FileStore) refreshAISM() {
	defer notifyAISMBucket(f)
	defer func() {
		if value := recover(); value != nil {
			if failure := aism.ReadFailure(value); failure != nil {
				f.logger.Warn("Aggregate catalog read failed", zap.Error(failure))
			} else {
				panic(value)
			}
		}
	}()
	if !aism.Enabled() {
		return
	}
	type member struct {
		reader *TSMReader
		name   string
	}
	f.mu.RLock()
	gen, prev := f.aismGeneration, f.aismCatalog
	path := filepath.Join(f.dir, "shard.aism")
	members := make([]member, 0, len(f.files))
	for _, file := range f.files {
		if r, ok := file.(*TSMReader); ok && r.aismIndex != nil {
			members = append(members, member{r, filepath.Base(r.Path())})
		}
	}
	f.mu.RUnlock()
	if len(members) == 0 {
		return
	}
	sources := make([]aism.Source, 0, len(members))
	for _, m := range members {
		r := m.reader
		r.aismIdentity.Do(func() { r.aismID = aism.Identify(r.aismIndex) })
		sources = append(sources, aism.Source{Name: m.name, ID: r.aismID, Index: r.aismIndex})
	}

	var catalog *aism.Catalog
	if prev == nil {
		catalog, _ = aism.LoadCatalog(path, sources)
	}
	loaded := catalog != nil
	if !loaded {
		catalog = aism.BuildCatalog(sources, prev)
	}

	// Serialize only competing publications. Neither queries nor native file
	// replacement take this mutex. An outdated build cannot displace a newer one.
	f.aismPublish.Lock()
	defer f.aismPublish.Unlock()
	f.mu.Lock()
	if f.aismGeneration != gen {
		f.mu.Unlock()
		return
	}
	f.aismCatalog = catalog
	f.aismCatalogGeneration = gen
	f.mu.Unlock()
	if !loaded {
		if err := aism.SaveCatalog(path, catalog); err != nil {
			f.logger.Warn("Shard aggregate catalog not persisted", zap.String("path", path), zap.Error(err))
		}
	}
}

// BuildAISMShard prepares all file companions and the cross-file directory of
// one stopped shard. The caller supplies its directory, never a live engine.
func BuildAISMShard(dir string) (int, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.tsm"))
	if err != nil {
		return 0, err
	}
	if len(paths) == 0 {
		return 0, fmt.Errorf("no TSM files in %s", dir)
	}
	sources := make([]aism.Source, 0, len(paths))
	for _, path := range paths {
		idx, err := buildAISMFile(path)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", path, err)
		}
		sources = append(sources, aism.Source{Name: filepath.Base(path), ID: aism.Identify(idx), Index: idx})
	}
	path := filepath.Join(dir, "shard.aism")
	if _, err := aism.LoadCatalog(path, sources); err == nil {
		return len(paths), nil
	}
	if err := aism.SaveCatalog(path, aism.BuildCatalog(sources, nil)); err != nil {
		return 0, err
	}
	return len(paths), nil
}
