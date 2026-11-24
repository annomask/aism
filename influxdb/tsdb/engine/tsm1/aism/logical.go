package aism

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

// logicalNode has no file identity, row offset, or child address. Its children
// are the next level's records contained in State's time range.
type logicalNode struct {
	State
	Children uint32
}
type logicalRoot struct {
	Level      uint8
	Start, End int64
}
type logicalTree struct {
	content [32]byte
	levels  [MaxLevels - 1][]*logicalNode
	roots   []logicalRoot
}

// contentIdentity intentionally excludes file partitioning and block addresses.
// Encoded content is included so overwrites cannot pass as physical relocation.
func (s *SeriesTree) contentIdentity() [32]byte {
	h := sha256.New()
	binary.Write(h, binary.BigEndian, s.parts[0].tree.Kind)
	for _, p := range s.parts {
		for i := 0; i < p.tree.LeafCount(); i++ {
			binary.Write(h, binary.BigEndian, p.tree.NodeAt(0, i).State)
			b := p.tree.BlockAt(i)
			h.Write(b.Digest[:])
		}
	}
	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id
}

func (s *SeriesTree) freeze(content [32]byte) error {
	l := &logicalTree{content: content}
	var visit func(recordRef) error
	visit = func(r recordRef) error {
		if r.level == 1 {
			return nil
		}
		children, err := r.children()
		if err != nil {
			return err
		}
		l.levels[r.level-2] = append(l.levels[r.level-2], &logicalNode{r.state(), uint32(len(children))})
		for _, ch := range children {
			if err := visit(ch); err != nil {
				return err
			}
		}
		return nil
	}
	for _, f := range s.forests {
		for _, r := range f {
			n := r.state()
			l.roots = append(l.roots, logicalRoot{r.level, n.Start, n.End})
			if err := visit(r); err != nil {
				return err
			}
		}
	}
	s.logical = l
	s.forests = nil // Builder references never escape into the published catalog.
	return nil
}

func (s *SeriesTree) logicalNode(r logicalRoot) (*logicalNode, error) {
	if r.Level < 2 || r.Level > MaxLevels || s.logical == nil {
		return nil, errForest
	}
	nodes := s.logical.levels[r.Level-2]
	i := sort.Search(len(nodes), func(i int) bool { return nodes[i].Start >= r.Start })
	if i == len(nodes) || nodes[i].Start != r.Start || nodes[i].End != r.End {
		return nil, errForest
	}
	return nodes[i], nil
}

// children resolves a logical range against the current level directory. The
// final level is supplied by current native readers, never by saved addresses.
func (s *SeriesTree) logicalChildren(r logicalRoot) ([]logicalRoot, error) {
	n, err := s.logicalNode(r)
	if err != nil {
		return nil, err
	}
	if n.Children < Fanout/2 || n.Children > Fanout {
		return nil, errForest
	}
	out := make([]logicalRoot, 0, n.Children)
	if r.Level == 2 {
		begin := sort.Search(len(s.parts), func(i int) bool { return s.parts[i].tree.Summary.End >= r.Start })
		for _, p := range s.parts[begin:] {
			if p.tree.Summary.Start > r.End {
				break
			}
			t := p.tree
			i := sort.Search(t.LeafCount(), func(i int) bool { return t.NodeAt(0, i).End >= r.Start })
			for ; i < t.LeafCount(); i++ {
				ch := t.NodeAt(0, i)
				if ch.Start > r.End {
					break
				}
				out = append(out, logicalRoot{1, ch.Start, ch.End})
			}
		}
	} else {
		nodes := s.logical.levels[r.Level-3]
		i := sort.Search(len(nodes), func(i int) bool { return nodes[i].End >= r.Start })
		for ; i < len(nodes) && nodes[i].Start <= r.End; i++ {
			out = append(out, logicalRoot{r.Level - 1, nodes[i].Start, nodes[i].End})
		}
	}
	if len(out) != int(n.Children) || len(out) < Fanout/2 || len(out) > Fanout || out[0].Start != r.Start || out[len(out)-1].End != r.End {
		return nil, errForest
	}
	for i, ch := range out {
		if ch.Start > ch.End || i > 0 && out[i-1].End >= ch.Start {
			return nil, errForest
		}
	}
	return out, nil
}

// restoreShape is used only for content changes. It retains existing logical
// boundaries, refolds aggregates, then joins new time ranges with joinForests.
func (s *SeriesTree) restoreShape(old *SeriesTree, counts *[MaxLevels]uint64) bool {
	if old == nil || old.logical == nil {
		return false
	}
	lo, hi := old.logical.roots[0].Start, old.logical.roots[len(old.logical.roots)-1].End
	if s.countRange(lo, hi) != old.leaves {
		return false
	}
	view := &SeriesTree{parts: s.parts, logical: old.logical}
	var restore func(logicalRoot) (recordRef, error)
	restore = func(r logicalRoot) (recordRef, error) {
		if r.Level == 1 {
			n, ok := s.leafAt(r.Start)
			if !ok || n.state().End != r.End {
				return recordRef{}, errForest
			}
			return n, nil
		}
		children, err := view.logicalChildren(r)
		if err != nil {
			return recordRef{}, err
		}
		refs := make(forest, len(children))
		for i, ch := range children {
			refs[i], err = restore(ch)
			if err != nil {
				return recordRef{}, err
			}
		}
		next := newParent(s.parts[0].tree.Kind, refs)
		previous, _ := old.logicalNode(r)
		if next.state() == previous.State {
			counts[r.Level-1]++
		}
		return next, nil
	}
	for _, p := range s.parts {
		if p.tree.Summary.Start < lo {
			if err := s.appendRange(p.tree, p.tree.Summary.Start, lo-1); err != nil {
				return false
			}
		}
	}
	for _, r := range old.logical.roots {
		next, err := restore(r)
		if err != nil || s.appendForest(forest{next}) != nil {
			return false
		}
	}
	for _, p := range s.parts {
		if p.tree.Summary.End > hi {
			if err := s.appendRange(p.tree, hi+1, p.tree.Summary.End); err != nil {
				return false
			}
		}
	}
	return true
}

func (s *SeriesTree) walkLogical(lo, hi int64, limit int, window func(int64) (int64, int64), use func(int, int64, State, bool) error, raw func(*Tree, Block, State) error, stats *WalkStats) error {
	if lo > hi {
		return nil
	}
	var visit func(logicalRoot) error
	visit = func(r logicalRoot) error {
		if r.End < lo || r.Start > hi {
			return nil
		}
		var state State
		var leaf recordRef
		if r.Level == 1 {
			var ok bool
			leaf, ok = s.leafAt(r.Start)
			if !ok || leaf.state().End != r.End {
				return errForest
			}
			state = leaf.tree.node(0, int(leaf.index), stats).State
		} else {
			n, err := s.logicalNode(r)
			if err != nil {
				return err
			}
			state = n.State
			if stats != nil {
				stats.Visited[r.Level-1]++
			}
		}
		if int(r.Level) <= limit && r.Start >= lo && r.End <= hi {
			b, e := window(r.Start)
			if r.End < e {
				first := sort.Search(len(s.parts), func(i int) bool { return s.parts[i].tree.Summary.End >= r.Start })
				cross := first < len(s.parts) && s.parts[first].tree.Summary.End < r.End
				return use(int(r.Level), b, state, cross)
			}
		}
		if r.Level == 1 {
			return raw(leaf.tree, leaf.tree.block(int(leaf.index), stats), state)
		}
		children, err := s.logicalChildren(r)
		if err != nil {
			return err
		}
		for _, ch := range children {
			if err := visit(ch); err != nil {
				return err
			}
		}
		return nil
	}
	for _, r := range s.logical.roots {
		if err := visit(r); err != nil {
			return err
		}
	}
	return nil
}

func (s *SeriesTree) reuseNodes(old *SeriesTree) {
	if old == nil || old.logical == nil {
		return
	}
	for level, nodes := range s.logical.levels {
		prior := old.logical.levels[level]
		for i, node := range nodes {
			j := sort.Search(len(prior), func(j int) bool { return prior[j].Start >= node.Start })
			if j < len(prior) && *prior[j] == *node {
				nodes[i] = prior[j]
			}
		}
	}
}
