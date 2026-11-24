package aism

import (
	"fmt"
	"reflect"
	"testing"
)

func TestBuilderBoundariesAndStreamingBatches(t *testing.T) {
	for _, count := range []int{1, 32, 64, 65, 70, 96, 127, 128, 129, 4096, 4097, 8193} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			want, _ := fixture(t, count, Integer)
			for _, batch := range []int{1, 7, 64, 95, 1000} {
				b := NewBuilder(Integer)
				for first := 0; first < count; first += batch {
					for i := first; i < min(count, first+batch); i++ {
						if err := b.Push(want.Levels[0][i], want.Blocks[i]); err != nil {
							t.Fatal(err)
						}
						for _, pending := range b.pending {
							if len(pending) >= 2*Fanout {
								t.Fatal("unbounded pending suffix")
							}
						}
					}
				}
				got, err := b.Finish()
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("batch %d: %v", batch, err)
				}
				if _, err := b.Finish(); err == nil {
					t.Fatal("finished twice")
				}
			}
			for _, layer := range want.Levels[1:] {
				for _, n := range layer {
					if n.Children < 32 || n.Children > 64 {
						t.Fatal(n.Children)
					}
				}
			}
			if count == 65 && (want.Levels[1][0].Children != 32 || want.Levels[1][1].Children != 33) {
				t.Fatal("wrong final balance")
			}
			if count == 4097 && (len(want.Levels) != 3 || len(want.Levels[2]) != 2) {
				t.Fatal("wrong root growth")
			}
		})
	}
	b := NewBuilder(Integer)
	if err := b.Push(Node{State: Point(Integer, 1, 1)}, Block{}); err != nil {
		t.Fatal(err)
	}
	if err := b.Push(Node{State: Point(Integer, 1, 2)}, Block{}); err == nil {
		t.Fatal("accepted repeated timestamp")
	}
	if _, err := b.Finish(); err == nil {
		t.Fatal("recovered a failed builder")
	}
}

func TestJoinPreservesHistoryAndUntouchedBranches(t *testing.T) {
	a, b, d := source(t, "a", 0, 30, 0), source(t, "b", 30, 40, 0), source(t, "d", 70, 30, 0)
	c := BuildCatalog([]Source{a, b}, nil)
	old := c.Series(testKey).logical.levels[0][0]
	next := BuildCatalog([]Source{d, b, a}, c)
	roots := next.Series(testKey).logical.levels[0]
	if len(roots) != 3 || roots[0] != old {
		t.Fatal("rewrote an unchanged subtree")
	}
	for i, n := range []int{35, 32, 33} {
		if int(roots[i].Children) != n {
			t.Fatal("lost stable join boundary", i)
		}
	}
	once := source(t, "one", 0, 100, 0).Index[testKey]
	if once.Levels[1][0].Children != 64 || once.Levels[1][1].Children != 36 {
		t.Fatal("one flush should retain its own construction history")
	}
	if err := validateCoverage(next.Series(testKey)); err != nil {
		t.Fatal(err)
	}
}

func TestStreamingBuilderMatchesMaterializedHierarchy(t *testing.T) {
	for _, n := range []int{64, 65, 70, 255, 256, 257, 4096, 4097} {
		want, _ := fixture(t, n, Integer)
		b := NewStreamingBuilder(Integer, nil, NewPageCache(1<<20))
		for i := 0; i < n; i++ {
			if err := b.Push(want.NodeAt(0, i), want.BlockAt(i)); err != nil {
				t.Fatal(err)
			}
			for _, nodes := range b.pageNodes {
				if len(nodes) >= pageRows {
					t.Fatal("unbounded output page")
				}
			}
			if len(b.tree.Levels) != 0 || len(b.tree.Blocks) != 0 {
				t.Fatal("streaming builder retained raw output")
			}
		}
		got, err := b.Finish()
		if err != nil {
			t.Fatal(err)
		}
		if err = validateTree(got); err != nil {
			t.Fatal(err)
		}
		if treeIdentity(got) != treeIdentity(want) {
			t.Fatal("streaming layout differs", n)
		}
	}
}

func TestStreamingBuilderReleasesFinishedWorkspace(t *testing.T) {
	input, _ := fixture(t, 257, Integer)
	b := NewStreamingBuilder(Integer, nil, nil)
	for i := 0; i < input.LeafCount(); i++ {
		if err := b.Push(input.NodeAt(0, i), input.BlockAt(i)); err != nil {
			t.Fatal(err)
		}
	}
	tree, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if tree == &b.tree || b.encoder != nil || b.pageBlocks != nil || b.compressed.Cap() != 0 {
		t.Fatal("finished tree retained builder workspace")
	}
	for _, nodes := range b.pageNodes {
		if nodes != nil {
			t.Fatal("finished page retained raw nodes")
		}
	}
	if treeIdentity(tree) != treeIdentity(input) {
		t.Fatal("workspace release changed the tree")
	}
}

func TestJoinSegmentsAtMaximumHeight(t *testing.T) {
	// Abstract completed roots exercise the capacity boundary without allocating
	// the billions of descendants represented by a full five-level forest.
	tree := &Tree{Kind: Integer}
	roots := func(first, count int) forest {
		out := make(forest, count)
		for i := range out {
			out[i] = recordRef{level: MaxLevels, branch: &forestNode{
				state:     Point(Integer, int64(first+i), uint64(first+i)),
				firstTree: tree, lastTree: tree,
			}}
		}
		return out
	}
	left, right := roots(1, Fanout), roots(Fanout+1, 1)
	s := &SeriesTree{parts: []part{{tree: tree}}}
	if err := s.appendForest(left); err != nil {
		t.Fatal(err)
	}
	if err := s.appendForest(right); err != nil {
		t.Fatal(err)
	}
	if len(s.forests) != 2 || len(s.forests[0]) != Fanout || len(s.forests[1]) != 1 {
		t.Fatal("capacity overflow was not segmented")
	}
	if s.forests[0][0].branch != left[0].branch || s.forests[1][0].branch != right[0].branch {
		t.Fatal("capacity segmentation rewrote complete roots")
	}
	var count uint64
	err := s.WalkWithStats(0, Fanout+2, MaxLevels,
		func(int64) (int64, int64) { return 0, Fanout + 2 },
		func(level int, _ int64, state State, _ bool) error {
			if level != MaxLevels {
				t.Fatal("unexpected capacity level", level)
			}
			count += state.N
			return nil
		},
		func(*Tree, Block, State) error { t.Fatal("descended into complete root"); return nil }, nil)
	if err != nil || count != Fanout+1 {
		t.Fatal("segmented query lost roots", count, err)
	}
}
