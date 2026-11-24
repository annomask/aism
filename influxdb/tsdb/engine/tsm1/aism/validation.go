package aism

import "errors"

// validateTree checks complete level coverage at recovery, including summaries.
// Queries rely on these immutable child ranges and never revalidate all leaves.
func validateTree(t *Tree) error {
	invalid := errors.New("invalid aggregate tree")
	if t == nil || t.Kind < Integer || t.Kind > Unsigned || t.LevelCount() == 0 || t.LevelCount() > MaxLevels || t.LeafCount() == 0 || t.RowCount(t.LevelCount()-1) > Fanout {
		return invalid
	}
	if t.pages == nil && len(t.Blocks) != t.LeafCount() {
		return invalid
	}
	var summary State
	var previous int64
	for i := 0; i < t.LeafCount(); i++ {
		n := t.NodeAt(0, i)
		if n.N == 0 || n.Start > n.End || n.Children != 0 || int(n.Child) != i || i > 0 && previous >= n.Start || n.MinTime < n.Start || n.MinTime > n.End || n.MaxTime < n.Start || n.MaxTime > n.End {
			return invalid
		}
		if summary.N+n.N < summary.N {
			return invalid
		}
		summary.Merge(t.Kind, n.State)
		previous = n.End
	}
	if summary != t.Summary {
		return invalid
	}
	for level := 1; level < t.LevelCount(); level++ {
		child := uint64(0)
		lower := t.RowCount(level - 1)
		if t.RowCount(level) == 0 {
			return invalid
		}
		for i := 0; i < t.RowCount(level); i++ {
			n := t.NodeAt(level, i)
			end := uint64(n.Child) + uint64(n.Children)
			if uint64(n.Child) != child || n.Children < Fanout/2 || n.Children > Fanout || end > uint64(lower) {
				return invalid
			}
			var state State
			for j := uint64(n.Child); j < end; j++ {
				state.Merge(t.Kind, t.NodeAt(level-1, int(j)).State)
			}
			if state != n.State {
				return invalid
			}
			child = end
		}
		if child != uint64(lower) {
			return invalid
		}
	}
	return nil
}
