package tsm1

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
	"go.uber.org/zap"
)

// BuildAISMIndex builds a companion for an existing, offline TSM snapshot.
func BuildAISMIndex(path string) error {
	if !strings.HasSuffix(path, ".tsm") {
		return errors.New("expected a .tsm file")
	}
	_, err := buildAISMFile(path)
	return err
}

func aismSidecarPath(path string) string {
	if strings.HasSuffix(path, ".tsm.tmp") {
		return strings.TrimSuffix(path, ".tmp") + ".aism.tmp"
	}
	return path + ".aism"
}

func buildAISMFile(path string) (aism.Index, error) {
	return buildAISMFromSnapshot(path, nil)
}

func buildAISMFromSnapshot(path string, snapshot *Cache) (aism.Index, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r, err := newTSMReader(fd, nil, true)
	if err != nil {
		fd.Close()
		return nil, err
	}
	defer r.Close()
	if r.aismIndex != nil {
		return r.aismIndex, nil
	}
	if snapshot == nil {
		return r.writeAISM(fd, nil)
	}
	return r.writeAISMValues(fd, nil, func(key []byte) []Value { return snapshot.values(key) })
}

// Snapshot outputs are immutable and owned by this writer before publication.
// Fold the immutable cache values at the physical block boundaries before publication.
func (f *FileStore) prepareSnapshotAISM(paths []string, snapshot *Cache) map[string]aism.Index {
	if !aism.Enabled() {
		return nil
	}
	prepared := make(map[string]aism.Index, len(paths))
	for _, path := range paths {
		if !strings.HasSuffix(path, ".tsm.tmp") {
			continue
		}
		idx, err := buildAISMFromSnapshot(path, snapshot)
		if err != nil {
			f.logger.Warn("Aggregate index unavailable", zap.String("path", path), zap.Error(err))
			idx = nil
		}
		prepared[path] = idx
	}
	return prepared
}

func aismKind(typ byte) aism.Kind {
	switch typ {
	case BlockInteger:
		return aism.Integer
	case BlockFloat64:
		return aism.Float
	case BlockBoolean:
		return aism.Boolean
	case BlockUnsigned:
		return aism.Unsigned
	}
	return 0
}
func aismBits(v Value) uint64 {
	switch p := v.(type) {
	case IntegerValue:
		return uint64(p.value)
	case UnsignedValue:
		return p.value
	case FloatValue:
		return math.Float64bits(p.value)
	case BooleanValue:
		if p.value {
			return 1
		}
		return 0
	default:
		panic("unexpected TSM aggregate field type")
	}
}

func existingLeaf(sources []aism.Index, key string, entry IndexEntry, digest [32]byte) (state aism.State, found bool) {
	defer func() {
		if value := recover(); value != nil {
			if aism.ReadFailure(value) != nil {
				state, found = aism.State{}, false
			} else {
				panic(value)
			}
		}
	}()
	for _, idx := range sources {
		t := idx[key]
		if t == nil {
			continue
		}
		leaves := t.LeafCount()
		i := sort.Search(leaves, func(i int) bool { return t.NodeAt(0, i).Start >= entry.MinTime })
		if i < leaves && t.NodeAt(0, i).Start == entry.MinTime && t.NodeAt(0, i).End == entry.MaxTime && t.BlockAt(i).Digest == digest {
			return t.NodeAt(0, i).State, true
		}
	}
	return aism.State{}, false
}
func (r *TSMReader) writeAISM(fd *os.File, sources []aism.Index) (aism.Index, error) {
	return r.writeAISMValues(fd, sources, nil)
}

func (r *TSMReader) writeAISMValues(fd *os.File, sources []aism.Index, snapshot func([]byte) []Value) (aism.Index, error) {
	idx := make(aism.Index)
	var entries []IndexEntry
	var values []Value
	for n := 0; n < r.KeyCount(); n++ {
		key, typ, list := r.Key(n, &entries)
		kind := aismKind(typ)
		if kind == 0 || len(list) == 0 {
			continue
		}
		id := string(key)
		var cached []Value
		if snapshot != nil {
			cached = snapshot(key)
		}
		leaves := make([]aism.Node, 0, len(list))
		blocks := make([]aism.Block, 0, len(list))
		for _, entry := range list {
			if len(leaves) > 0 && entry.MinTime <= leaves[len(leaves)-1].End {
				break
			}
			checksum, b, err := r.ReadBytes(&entry, nil)
			if err != nil {
				return nil, err
			}
			if len(b) == 0 || b[0] != typ || crc32.ChecksumIEEE(b) != checksum {
				return nil, fmt.Errorf("invalid TSM block at %d", entry.Offset)
			}
			digest := sha256.Sum256(b)
			state, found := existingLeaf(sources, id, entry, digest)
			if !found {
				var blockValues []Value
				if snapshot != nil {
					first := sort.Search(len(cached), func(i int) bool { return cached[i].UnixNano() >= entry.MinTime })
					last := sort.Search(len(cached), func(i int) bool { return cached[i].UnixNano() > entry.MaxTime })
					blockValues = cached[first:last]
				} else {
					values, err = DecodeBlock(b, values)
					if err != nil {
						return nil, err
					}
					blockValues = values
				}
				for _, v := range blockValues {
					ts := v.UnixNano()
					if state.N > 0 && ts <= state.End {
						state.N = 0
						break
					}
					state.Merge(kind, aism.Point(kind, ts, aismBits(v)))
				}
				if state.N == 0 || state.Start != entry.MinTime || state.End != entry.MaxTime {
					break
				}
			}
			leaves = append(leaves, aism.Node{State: state})
			blocks = append(blocks, aism.Block{Offset: entry.Offset, Size: entry.Size, Digest: digest})
		}
		// Unordered or duplicate physical timestamps require the native cursor.
		if len(leaves) != len(list) {
			continue
		}
		tree, err := aism.Build(kind, leaves, blocks)
		if err != nil {
			return nil, err
		}
		idx[id] = tree
	}
	return idx, aism.Save(aismSidecarPath(fd.Name()), fd, idx)
}

// The immutable input indexes can outlive their readers. No file-store lock
// is needed while reading output blocks or writing the sidecar.
func (f *FileStore) aismSources(paths []string) []aism.Index {
	wanted := make(map[string]bool, len(paths))
	for _, p := range paths {
		wanted[p] = true
	}
	var out []aism.Index
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, file := range f.files {
		if r, ok := file.(*TSMReader); ok && r.aismIndex != nil && wanted[r.Path()] {
			out = append(out, r.aismIndex)
		}
	}
	return out
}

// validateAISMAddresses binds every leaf to the authoritative native block
// directory before publication. No data block is decoded by this check.
func (r *TSMReader) validateAISMAddresses() {
	defer func() {
		if value := recover(); value != nil {
			if aism.ReadFailure(value) != nil {
				r.aismIndex = nil
			} else {
				panic(value)
			}
		}
	}()
	for key, tree := range r.aismIndex {
		typ, err := r.Type([]byte(key))
		entries := r.Entries([]byte(key))
		valid := err == nil && aismKind(typ) == tree.Kind && len(entries) == tree.LeafCount()
		for i, e := range entries {
			if !valid {
				break
			}
			n, b := tree.NodeAt(0, i), tree.BlockAt(i)
			valid = n.Start == e.MinTime && n.End == e.MaxTime && b.Offset == e.Offset && b.Size == e.Size
		}
		if !valid {
			delete(r.aismIndex, key)
		}
	}
}
