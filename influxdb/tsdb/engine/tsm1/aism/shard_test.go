package aism

import (
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

const testKey = "cpu,hostname=h#!~#i"

func source(t *testing.T, name string, start, count int, delta int64) Source {
	t.Helper()
	var leaves []Node
	var blocks []Block
	for j := 0; j < count; j++ {
		i := int64(start + j)
		s := Point(Integer, i*4, uint64(i+delta))
		s.Merge(Integer, Point(Integer, i*4+2, uint64(i+delta+1)))
		var b [16]byte
		binary.BigEndian.PutUint64(b[:8], uint64(i))
		binary.BigEndian.PutUint64(b[8:], uint64(delta))
		leaves = append(leaves, Node{State: s})
		blocks = append(blocks, Block{Offset: int64(j)*64 + 5, Size: 64, Digest: sha256.Sum256(b[:])})
	}
	tree, err := Build(Integer, leaves, blocks)
	if err != nil {
		t.Fatal(err)
	}
	idx := Index{testKey: tree}
	return Source{Name: name, ID: Identify(idx), Index: idx}
}

type work struct {
	Merged, Cross [MaxLevels]int
	Decoded       int
}

func collect(t *testing.T, sources []Source, tree *SeriesTree, lo, hi, width, offset int64, level int) (map[int64]uint64, work) {
	t.Helper()
	var w work
	rows := map[int64]uint64{}
	window := func(ts int64) (int64, int64) {
		b := (ts - offset) / width
		if ts < offset && (ts-offset)%width != 0 {
			b--
		}
		start := b*width + offset
		return start, start + width
	}
	fold := func(level int, b int64, s State, cross bool) error {
		w.Merged[level-1]++
		if cross {
			w.Cross[level-1]++
		}
		rows[b] += s.Total
		return nil
	}
	owners := map[*Tree]bool{}
	for _, s := range sources {
		owners[s.Index[testKey]] = true
	}
	raw := func(tree *Tree, b Block, s State) error {
		w.Decoded++
		if !owners[tree] {
			t.Fatal("raw callback retained an obsolete reader")
		}
		j := (b.Offset - 5) / 64
		if tree.Blocks[j] != b || tree.Levels[0][j].State != s {
			t.Fatal("raw callback used another file's offset")
		}
		for _, p := range []struct {
			t int64
			v uint64
		}{{s.Start, s.FirstValue}, {s.End, s.LastValue}} {
			if p.t >= lo && p.t <= hi {
				k, _ := window(p.t)
				rows[k] += p.v
			}
		}
		return nil
	}
	if err := tree.Walk(lo, hi, level, window, fold, raw); err != nil {
		t.Fatal(err)
	}
	return rows, w
}

func TestShardCrossFileCoverAndRecovery(t *testing.T) {
	a, b := source(t, "a.tsm", 0, 30, 0), source(t, "b.tsm", 30, 40, 0)
	sources := []Source{b, a}
	c := BuildCatalog(sources, nil)
	s := c.Series(testKey)
	if s == nil || !s.Matches(0, 300, []*Tree{a.Index[testKey], b.Index[testKey]}) || s.Matches(0, 300, []*Tree{a.Index[testKey]}) {
		t.Fatal("wrong active membership")
	}
	path := filepath.Join(t.TempDir(), "shard.aism")
	if err := SaveCatalog(path, c); err != nil {
		t.Fatal(err)
	}
	reopened, err := LoadCatalog(path, []Source{a, b})
	if err != nil {
		t.Fatal(err)
	}
	s = reopened.Series(testKey)
	want, raw := collect(t, sources, s, 0, 279, 1000, 0, 0)
	leafRows, leaf := collect(t, sources, s, 0, 279, 1000, 0, 1)
	fullRows, full := collect(t, sources, s, 0, 279, 1000, 0, MaxLevels)
	if want[0] != 4900 || !reflect.DeepEqual(want, leafRows) || !reflect.DeepEqual(want, fullRows) || raw.Decoded != 70 || leaf.Merged[0] != 70 || full.Merged[1] != 2 || full.Cross[1] != 1 || full.Decoded != 0 {
		t.Fatalf("rows=%v raw=%+v leaf=%+v full=%+v", want, raw, leaf, full)
	}
	t.Logf("two files of 30 and 40 blocks: raw=%d leaf=%v full=%v cross-file=%v sum=%d", raw.Decoded, leaf.Merged, full.Merged, full.Cross, want[0])
	for _, r := range [][4]int64{{2, 274, 19, 7}, {0, 100, 500, 0}, {121, 126, 2, 1}, {120, 120, 1, 0}} {
		got, _ := collect(t, sources, s, r[0], r[1], r[2], r[3], MaxLevels)
		want, _ := collect(t, sources, s, r[0], r[1], r[2], r[3], 0)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("partial coverage %v: got %v want %v", r, got, want)
		}
	}
	next := source(t, "b.tsm", 30, 40, 10)
	if _, err := LoadCatalog(path, []Source{a, next}); err == nil {
		t.Fatal("accepted changed contents under same filename")
	}
	if BuildCatalog([]Source{a, source(t, "overlap.tsm", 29, 42, 0)}, c).Series(testKey) != nil {
		t.Fatal("accepted overlap")
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt([]byte("bad"), 12); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err = LoadCatalog(path, sources); err == nil {
		t.Fatal("accepted corrupt catalog")
	}
}

func TestShardCompactionRebindsAndReusesCleanParents(t *testing.T) {
	a, b := source(t, "old-a.tsm", 0, 30, 0), source(t, "old-b.tsm", 30, 40, 0)
	old := BuildCatalog([]Source{a, b}, nil)
	// The same encoded blocks have new file boundaries, names and offsets.
	x, y := source(t, "new-x.tsm", 0, 50, 0), source(t, "new-y.tsm", 50, 20, 0)
	src := []Source{x, y}
	c := BuildCatalog(src, old)
	if c.reused[1] != 2 {
		t.Fatalf("copied blocks: reused %v", c.reused)
	}
	_, w := collect(t, src, c.Series(testKey), 0, 279, 1000, 0, MaxLevels)
	if w.Cross[1] != 1 {
		t.Fatal(w)
	}
	got, _ := collect(t, src, c.Series(testKey), 1, 274, 19, 7, MaxLevels)
	want, _ := collect(t, src, c.Series(testKey), 1, 274, 19, 7, 0)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("wrong reader after compaction")
	}
	changed := []Source{x, source(t, "changed.tsm", 50, 20, 20)}
	next := BuildCatalog(changed, c)
	if next.reused[1] != 1 {
		t.Fatal("reused stale parent or refolded clean parent", next.reused)
	}
	got, _ = collect(t, changed, next.Series(testKey), 0, 279, 1000, 0, MaxLevels)
	if got[0] != 5700 {
		t.Fatal(got)
	}
}

func TestShardThreeLevelsAndPartialFilePins(t *testing.T) {
	src := []Source{source(t, "x", 0, 1400, 0), source(t, "y", 1400, 1500, 0), source(t, "z", 2900, 1300, 0)}
	c := BuildCatalog(src, nil)
	s := c.Series(testKey)
	if !s.Matches(5600, 7000, []*Tree{src[1].Index[testKey]}) {
		t.Fatal("intersecting reader rejected")
	}
	for _, width := range []int64{100000, 8500, 217, 3} {
		want, r := collect(t, src, s, 1, 16797, width, 1, 0)
		got, w := collect(t, src, s, 1, 16797, width, 1, MaxLevels)
		if !reflect.DeepEqual(got, want) {
			t.Fatal(width, r, w)
		}
	}
	_, w := collect(t, src, s, 0, 16800, 100000, 0, MaxLevels)
	if w.Merged[2] == 0 || w.Cross[2] == 0 || w.Decoded != 0 {
		t.Fatal(w)
	}
}
