package aism

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func pagedFixture(t *testing.T, n int) *Tree {
	t.Helper()
	tree, _ := fixture(t, n, Integer)
	dir := t.TempDir()
	data, err := os.Create(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	if _, err = data.WriteString("immutable native contents"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "data.aism")
	if err = Save(path, data, Index{"key": tree}); err != nil {
		t.Fatal(err)
	}
	idx, err := Load(path, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx["key"].Levels) != 0 || len(idx["key"].Blocks) != 0 {
		t.Fatal("loaded expanded arrays")
	}
	return idx["key"]
}
func TestPagedProjectionAndHighestRootAccess(t *testing.T) {
	tree := pagedFixture(t, 4097)
	UseCache(Index{"key": tree}, NewPageCache(0))
	for _, limit := range []int{1, MaxLevels} {
		stats := &WalkStats{Op: Count}
		var count uint64
		if err := tree.WalkWithStats(tree.Summary.Start, tree.Summary.End, limit, func(int64) (int64, int64) { return -100000, 100000 }, func(_ int, _ int64, s State) error { count += s.N; return nil }, func(Block, State) error { t.Fatal("unexpected raw decode"); return nil }, stats); err != nil {
			t.Fatal(err)
		}
		if count != tree.Summary.N {
			t.Fatal("count mismatch")
		}
		if limit == MaxLevels && (stats.Visited[0] != 0 || stats.Visited[1] != 0 || stats.ColumnsDecoded != 2 || stats.PagesDecoded != 1) {
			t.Fatal("highest root read expanded descendants", stats)
		}
		if limit == 1 && stats.ColumnsDecoded != 2*uint64((4097+pageRows-1)/pageRows) {
			t.Fatal("COUNT read unrequested columns", stats)
		}
	}
}
func TestPageCacheBudgetAndCompressedReuse(t *testing.T) {
	old := pagedFixture(t, 600)
	cache := NewPageCache(512)
	UseCache(Index{"key": old}, cache)
	for i := 0; i < old.LeafCount(); i++ {
		_ = old.NodeAt(0, i)
	}
	if cache.bytes > cache.limit {
		t.Fatal("cache exceeded payload budget")
	}
	next, _ := fixture(t, 600, Integer)
	idx := Index{"key": next}
	ReusePages(idx, []Index{{"key": old}})
	raw := encodeColumn(next, 0, 0, pageRows, 1)
	col, ok := next.reusable[payloadKey{1, pageRows, sha256.Sum256(raw)}]
	if !ok || !bytes.Equal(col.bytes, old.pages.levels[0].pages[0].columns[1].bytes) {
		t.Fatal("unchanged compressed column not retained")
	}
	// The reused payload owns its mapping even after its source tree is released.
	old = nil
	runtime.GC()
	decoded, err := zlibBytes(col.bytes, pageRows*8)
	runtime.KeepAlive(col.owner)
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatal("reused mapping did not survive", err)
	}
}
func zlibBytes(b []byte, n int) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(io.LimitReader(r, int64(n+1)))
}

func TestCatalogRejectsValidChecksumWithBrokenCoverage(t *testing.T) {
	a, b := source(t, "a", 0, 30, 0), source(t, "b", 30, 40, 0)
	sources := []Source{a, b}
	c := BuildCatalog(sources, nil)
	path := filepath.Join(t.TempDir(), "shard.aism")
	s := c.Series(testKey)
	s.logical.roots = s.logical.roots[:1]
	if err := SaveCatalog(path, c); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCatalog(path, sources); err == nil {
		t.Fatal("accepted incomplete coverage with a valid checksum")
	}
	c = BuildCatalog(sources, nil)
	if err := SaveCatalog(path, c); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, 7, 15, len(original) / 2, len(original) - 1} {
		if err := os.WriteFile(path, original[:size], 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadCatalog(path, sources); err == nil {
			t.Fatal("accepted truncated catalog", size)
		}
	}
	forged := append([]byte(nil), original...)
	binary.BigEndian.PutUint32(forged[len(shardMagic):], ^uint32(0))
	sum := sha256.Sum256(forged[:len(forged)-sha256.Size])
	copy(forged[len(forged)-sha256.Size:], sum[:])
	if err := os.WriteFile(path, forged, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCatalog(path, sources); err == nil {
		t.Fatal("accepted oversized catalog allocation")
	}
}

func TestPagedRuntimeDamageReturnsError(t *testing.T) {
	for _, truncate := range []bool{false, true} {
		t.Run(map[bool]string{false: "corrupt", true: "truncate"}[truncate], func(t *testing.T) {
			tree := pagedFixture(t, 70)
			UseCache(Index{"key": tree}, NewPageCache(0))
			col := tree.pages.levels[1].pages[0].columns[0]
			path := col.owner.file.Name()
			f, err := os.OpenFile(path, os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if truncate {
				err = f.Truncate(col.offset)
			} else {
				_, err = f.WriteAt([]byte{0}, col.offset)
			}
			if closeErr := f.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				t.Fatal(err)
			}
			err = tree.WalkWithStats(tree.Summary.Start, tree.Summary.End, MaxLevels,
				func(int64) (int64, int64) { return -100000, 100000 },
				func(int, int64, State) error { t.Fatal("damaged root accepted"); return nil },
				func(Block, State) error { t.Fatal("damaged root decoded"); return nil },
				&WalkStats{Op: Count})
			if err == nil {
				t.Fatal("runtime index damage did not return an error")
			}
		})
	}
}
