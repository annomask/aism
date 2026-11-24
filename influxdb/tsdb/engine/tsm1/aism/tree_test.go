package aism

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func fixture(t *testing.T, n int, k Kind) (*Tree, [][]State) {
	t.Helper()
	leaves := make([]Node, n)
	blocks := make([]Block, n)
	points := make([][]State, n)
	for i := range leaves {
		for j := 0; j < 3; j++ {
			stamp := int64(-11 + 10*i + j*2)
			v := uint64(int64((i+j)%13 - 5))
			if k == Float {
				v = math.Float64bits(float64((i+j)%11) / 8)
			}
			if k == Boolean {
				v = uint64((i + j) % 2)
			}
			p := Point(k, stamp, v)
			points[i] = append(points[i], p)
			leaves[i].Merge(k, p)
		}
		blocks[i].Offset = int64(i)
	}
	tr, err := Build(k, leaves, blocks)
	if err != nil {
		t.Fatal(err)
	}
	return tr, points
}
func TestAISMHighestCovered(t *testing.T) {
	tree, _ := fixture(t, 70, Integer)
	want := []uint32{35, 35}
	for i, v := range tree.Levels[1] {
		if v.Children != want[i] {
			t.Fatalf("group %d has %d children", i, v.Children)
		}
	}
	for _, c := range []struct{ limit, leaf, upper, raw int }{{0, 0, 0, 70}, {1, 70, 0, 0}, {5, 0, 2, 0}} {
		var a, b, d int
		var sum uint64
		err := tree.Walk(-11, 700, c.limit, func(int64) (int64, int64) { return -11, 701 }, func(l int, _ int64, s State) error {
			sum += s.Total
			if l == 1 {
				a++
			} else {
				b++
			}
			return nil
		}, func(_ Block, s State) error { sum += s.Total; d++; return nil })
		if err != nil || a != c.leaf || b != c.upper || d != c.raw || sum != tree.Summary.Total {
			t.Fatalf("mode %d: leaf=%d upper=%d decode=%d sum=%d: %v", c.limit, a, b, d, sum, err)
		}
		t.Logf("limit=%d leaves=%d parents=%d raw_callbacks=%d", c.limit, a, b, d)
	}
}
func TestAISMOffsetsSparseTypesAndTies(t *testing.T) {
	for _, k := range []Kind{Integer, Float, Boolean} {
		tr, points := fixture(t, 151, k)
		for _, op := range []Op{Count, Sum, Mean, Min, Max, First, Last} {
			if k == Boolean && op != Count && op != First && op != Last {
				continue
			}
			for _, width := range []int64{3, 31, 601, 2000} {
				win := func(ts int64) (int64, int64) {
					d := (ts - 7) % width
					if d < 0 {
						d += width
					}
					return ts - d, ts - d + width
				}
				want := map[int64]Result{}
				fold := func(m map[int64]Result, b int64, s State) { r := m[b]; r.Fold(k, op, b, s); m[b] = r }
				for _, block := range points {
					for _, p := range block {
						if p.Start >= -8 && p.Start <= 1422 {
							b, _ := win(p.Start)
							fold(want, b, p)
						}
					}
				}
				for _, limit := range []int{0, 1, 5} {
					got := map[int64]Result{}
					err := tr.Walk(-8, 1422, limit, win, func(_ int, b int64, s State) error { fold(got, b, s); return nil }, func(block Block, _ State) error {
						for _, p := range points[block.Offset] {
							if p.Start >= -8 && p.Start <= 1422 {
								b, _ := win(p.Start)
								fold(got, b, p)
							}
						}
						return nil
					})
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Fatalf("kind %d op %d width %d limit %d", k, op, width, limit)
					}
				}
			}
		}
	}
}
func TestAISMReopenAndWrongFile(t *testing.T) {
	tr, _ := fixture(t, 4100, Integer)
	if len(tr.Levels) != 3 {
		t.Fatal(len(tr.Levels))
	}
	dir := t.TempDir()
	data, err := os.Create(filepath.Join(dir, "test.tsm"))
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	if _, err = data.WriteString("file one"); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "test.tsm.aism")
	if err = Save(p, data, Index{"key": tr}); err != nil {
		t.Fatal(err)
	}
	reopened, err := Load(p, data)
	if err != nil || Identify(reopened) != Identify(Index{"key": tr}) {
		t.Fatal("reopen mismatch", err)
	}
	if _, err = data.WriteAt([]byte("file two"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err = Load(p, data); err == nil {
		t.Fatal("accepted different TSM contents of equal size")
	}
	if _, err = data.WriteAt([]byte("file one"), 0); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 42
	if err = os.WriteFile(p, bytes.Clone(b), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Load(p, data); err == nil {
		t.Fatal("accepted damaged index")
	}
}
func TestAISMPartialThreeLevelCoverage(t *testing.T) {
	tree, points := fixture(t, Fanout*Fanout+1, Integer)
	lo, hi := tree.Summary.Start+1, tree.Summary.End
	window := func(int64) (int64, int64) { return -1000000, 1000000 }
	execute := func(limit int) (Result, [MaxLevels]int, int) {
		var result Result
		var counts [MaxLevels]int
		var decoded int
		err := tree.Walk(lo, hi, limit, window,
			func(level int, bucket int64, state State) error {
				counts[level-1]++
				result.Fold(Integer, Sum, bucket, state)
				return nil
			},
			func(block Block, state State) error {
				decoded++
				for _, point := range points[block.Offset] {
					if point.Start >= lo && point.Start <= hi {
						result.Fold(Integer, Sum, -1000000, point)
					}
				}
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
		return result, counts, decoded
	}
	want, _, all := execute(0)
	got, merged, partial := execute(MaxLevels)
	if got != want || all != len(points) || partial != 1 || merged[0] == 0 || merged[1] == 0 || merged[2] == 0 {
		t.Fatalf("raw=%+v full=%+v all=%d partial=%d merged=%v", want, got, all, partial, merged)
	}
	t.Logf("partial window: raw=%d full=%d merged=%v", all, partial, merged)
}

func TestAISMNonAssociativeFloatFallback(t *testing.T) {
	for _, values := range [][]float64{{1e16, 1, -1e16}, {1, 1e17}, {math.Inf(1)}, {math.NaN()}, {math.Copysign(0, -1)}} {
		var p Precision
		for _, v := range values {
			p.Include(precision(math.Float64bits(v)))
		}
		if p.Usable(Sum, uint64(len(values))) || p.Usable(Mean, uint64(len(values))) {
			t.Fatal(values)
		}
	}
	var p Precision
	for _, v := range []float64{.25, -.5, 12, -1.125} {
		p.Include(precision(math.Float64bits(v)))
	}
	if !p.Usable(Mean, 1000000) {
		t.Fatal("fixed-point inputs should be usable")
	}
}
