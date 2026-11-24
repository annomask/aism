package aism

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"sort"
	"sync/atomic"
)

// Source identifies one immutable native TSM and its validated block index.
type Source struct {
	Name  string
	ID    [32]byte
	Index Index
}

func writeString(h hash.Hash, s string) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(s)))
	h.Write(n[:])
	h.Write([]byte(s))
}

// Identify binds both physical addresses and every persisted aggregate. A catalog
// cannot reuse a parent from a different or partially changed companion.
func digestTree(t *Tree) [32]byte {
	h := sha256.New()
	var word [8]byte
	put := func(v uint64) { binary.BigEndian.PutUint64(word[:], v); h.Write(word[:]) }
	put(uint64(t.Kind))
	put(uint64(t.LevelCount()))
	for l := 0; l < t.LevelCount(); l++ {
		put(uint64(t.RowCount(l)))
		for i := 0; i < t.RowCount(l); i++ {
			n := t.NodeAt(l, i)
			for _, v := range []uint64{uint64(n.Start), uint64(n.End), n.N, n.Total, n.Minimum, n.Maximum, n.FirstValue, n.LastValue, uint64(n.MinTime), uint64(n.MaxTime), uint64(n.Precision.Flags), uint64(uint16(n.Precision.Low)), uint64(uint16(n.Precision.High)), uint64(n.Child), uint64(n.Children)} {
				put(v)
			}
		}
	}
	put(uint64(t.LeafCount()))
	for i := 0; i < t.LeafCount(); i++ {
		b := t.BlockAt(i)
		put(uint64(b.Offset))
		put(uint64(b.Size))
		h.Write(b.Digest[:])
	}
	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id
}
func treeIdentity(t *Tree) [32]byte {
	t.identityOnce.Do(func() {
		if !t.identityValid {
			t.identity = digestTree(t)
			t.identityValid = true
		}
	})
	return t.identity
}
func Identify(index Index) [32]byte {
	keys := make([]string, 0, len(index))
	for key := range index {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	writeString(h, "AISM-source-v3")
	for _, key := range keys {
		writeString(h, key)
		if t := index[key]; t != nil {
			id := treeIdentity(t)
			h.Write(id[:])
		}
	}
	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id
}

type part struct {
	name  string
	tree  *Tree
	first int
}

// A reference either addresses a node of an immutable file tree or owns a newly
// joined branch. Leaves and unchanged file-local subtrees are never copied.
type recordRef struct {
	tree   *Tree
	index  uint32
	level  uint8
	branch *forestNode
}
type forestNode struct {
	state               State
	children            []recordRef
	firstTree, lastTree *Tree
	retained            [MaxLevels]uint64
}
type forest []recordRef

type SeriesTree struct {
	parts   []part
	leaves  int
	forests []forest // Temporary builder state only.
	logical *logicalTree
}
type catalogDiskIdentity struct{ path string }
type Catalog struct {
	durable atomic.Pointer[catalogDiskIdentity]
	series  map[string]*SeriesTree
	members map[string][32]byte
	reused  [MaxLevels]uint64
}

func (c *Catalog) Series(key string) *SeriesTree {
	if c == nil {
		return nil
	}
	return c.series[key]
}
func (r recordRef) state() State {
	if r.branch != nil {
		return r.branch.state
	}
	return r.tree.NodeAt(int(r.level)-1, int(r.index)).State
}
func (r recordRef) endpoints() (*Tree, *Tree) {
	if r.branch != nil {
		return r.branch.firstTree, r.branch.lastTree
	}
	return r.tree, r.tree
}
func (r recordRef) children() ([]recordRef, error) {
	if r.branch != nil {
		return r.branch.children, nil
	}
	if r.level <= 1 || r.tree == nil || int(r.level) > r.tree.LevelCount() {
		return nil, errForest
	}
	if int(r.index) >= r.tree.RowCount(int(r.level)-1) {
		return nil, errForest
	}
	n := r.tree.NodeAt(int(r.level)-1, int(r.index))
	if n.Children < Fanout/2 || n.Children > Fanout || uint64(n.Child)+uint64(n.Children) > uint64(r.tree.RowCount(int(r.level)-2)) {
		return nil, errForest
	}
	out := make([]recordRef, n.Children)
	for i := range out {
		out[i] = recordRef{tree: r.tree, index: n.Child + uint32(i), level: r.level - 1}
	}
	return out, nil
}
func newParent(kind Kind, children []recordRef) recordRef {
	b := &forestNode{children: children}
	b.firstTree, _ = children[0].endpoints()
	_, b.lastTree = children[len(children)-1].endpoints()
	for _, child := range children {
		b.state.Merge(kind, child.state())
		countReused(child, &b.retained)
	}
	b.retained[children[0].level]++
	return recordRef{level: children[0].level + 1, branch: b}
}

var (
	errForest         = errors.New("invalid ordered aggregate forest")
	errForestCapacity = errors.New("aggregate forest capacity exceeded")
)

func checkedForest(roots forest) error {
	if len(roots) == 0 || len(roots) > Fanout || roots[0].level < 1 || roots[0].level > MaxLevels {
		return errForest
	}
	for i, r := range roots {
		if r.level != roots[0].level || r.state().N == 0 || r.state().Start > r.state().End || i > 0 && roots[i-1].state().End >= r.state().Start {
			return errForest
		}
	}
	return nil
}
func capRoots(kind Kind, roots forest) (forest, error) {
	if len(roots) <= Fanout {
		return roots, nil
	}
	if len(roots) > 2*Fanout || roots[0].level >= MaxLevels {
		return nil, errForestCapacity
	}
	n := len(roots) / 2
	return forest{newParent(kind, roots[:n]), newParent(kind, roots[n:])}, nil
}
func replaceBoundary(kind Kind, roots forest, parentLevel uint8) (forest, error) {
	if roots[0].level == parentLevel {
		return roots, nil
	}
	if roots[0].level+1 != parentLevel || len(roots) < Fanout/2 {
		return nil, errForest
	}
	return forest{newParent(kind, roots)}, nil
}

// joinForests follows only the facing boundary of the taller input, matching
// Rust join_ordered. Every untouched subtree retains its existing child range.
func joinForests(kind Kind, left, right forest) (forest, error) {
	if err := checkedForest(left); err != nil {
		return nil, err
	}
	if err := checkedForest(right); err != nil {
		return nil, err
	}
	if left[len(left)-1].state().End >= right[0].state().Start {
		return nil, errForest
	}
	a, b := left[0].level, right[0].level
	if a == b {
		roots := make(forest, 0, len(left)+len(right))
		roots = append(roots, left...)
		roots = append(roots, right...)
		return capRoots(kind, roots)
	}
	if a > b {
		last := left[len(left)-1]
		children, err := last.children()
		if err != nil {
			return nil, err
		}
		joined, err := joinForests(kind, children, right)
		if err != nil {
			return nil, err
		}
		replacement, err := replaceBoundary(kind, joined, a)
		if err != nil {
			return nil, err
		}
		roots := append(append(make(forest, 0, len(left)-1+len(replacement)), left[:len(left)-1]...), replacement...)
		return capRoots(kind, roots)
	}
	first := right[0]
	children, err := first.children()
	if err != nil {
		return nil, err
	}
	joined, err := joinForests(kind, left, children)
	if err != nil {
		return nil, err
	}
	replacement, err := replaceBoundary(kind, joined, b)
	if err != nil {
		return nil, err
	}
	roots := append(append(make(forest, 0, len(replacement)+len(right)-1), replacement...), right[1:]...)
	return capRoots(kind, roots)
}
func (s *SeriesTree) appendForest(roots forest) error {
	if len(roots) == 0 {
		return nil
	}
	if err := checkedForest(roots); err != nil {
		return err
	}
	if len(s.forests) == 0 {
		s.forests = []forest{roots}
		return nil
	}
	i := len(s.forests) - 1
	joined, err := joinForests(s.parts[0].tree.Kind, s.forests[i], roots)
	if err == errForestCapacity {
		s.forests = append(s.forests, roots)
		return nil
	}
	if err != nil {
		return err
	}
	s.forests[i] = joined
	return nil
}

func (s *SeriesTree) partAt(i int) *part {
	p := sort.Search(len(s.parts), func(p int) bool { return s.parts[p].first > i }) - 1
	return &s.parts[p]
}

// Matches validates the pinned native readers selected for this interval.
func (s *SeriesTree) Matches(lo, hi int64, trees []*Tree) bool {
	if s == nil {
		return false
	}
	i := sort.Search(len(s.parts), func(i int) bool { return s.parts[i].tree.Summary.End >= lo })
	for _, t := range trees {
		if i >= len(s.parts) || s.parts[i].tree != t || t.Summary.Start > hi {
			return false
		}
		i++
	}
	return i == len(s.parts) || s.parts[i].tree.Summary.Start > hi
}
func usableTree(t *Tree) bool {
	return t != nil && t.Kind >= Integer && t.Kind <= Unsigned && t.LevelCount() > 0 && t.LevelCount() <= MaxLevels && t.LeafCount() > 0 && t.RowCount(t.LevelCount()-1) > 0 && t.RowCount(t.LevelCount()-1) <= Fanout && t.Summary.Start == t.NodeAt(0, 0).Start && t.Summary.End == t.NodeAt(0, t.LeafCount()-1).End
}
func catalogParts(sources []Source) *Catalog {
	c := &Catalog{series: map[string]*SeriesTree{}, members: map[string][32]byte{}}
	for _, src := range sources {
		if _, exists := c.members[src.Name]; exists {
			return &Catalog{series: map[string]*SeriesTree{}, members: map[string][32]byte{}}
		}
		c.members[src.Name] = src.ID
		for key, t := range src.Index {
			if !usableTree(t) {
				continue
			}
			s := c.series[key]
			if s == nil {
				s = &SeriesTree{}
				c.series[key] = s
			}
			s.parts = append(s.parts, part{name: src.Name, tree: t})
		}
	}
	for key, s := range c.series {
		sort.Slice(s.parts, func(i, j int) bool { return s.parts[i].tree.Summary.Start < s.parts[j].tree.Summary.Start })
		for i := range s.parts {
			p := &s.parts[i]
			if p.tree.Kind != s.parts[0].tree.Kind || i > 0 && s.parts[i-1].tree.Summary.End >= p.tree.Summary.Start {
				delete(c.series, key)
				break
			}
			p.first = s.leaves
			s.leaves += p.tree.LeafCount()
		}
		if s.leaves <= Fanout {
			delete(c.series, key)
		}
	}
	return c
}
func fileRoots(t *Tree) forest {
	level := t.LevelCount()
	out := make(forest, t.RowCount(level-1))
	for i := range out {
		out[i] = recordRef{tree: t, index: uint32(i), level: uint8(level)}
	}
	return out
}
func (s *SeriesTree) appendRange(t *Tree, lo, hi int64) error {
	var visit func(recordRef) error
	visit = func(r recordRef) error {
		n := r.state()
		if n.End < lo || n.Start > hi {
			return nil
		}
		if n.Start >= lo && n.End <= hi {
			return s.appendForest(forest{r})
		}
		if r.level == 1 {
			return errForest
		}
		children, err := r.children()
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
	// Keep complete same-height root lists together, including small native files.
	roots := fileRoots(t)
	if t.Summary.Start >= lo && t.Summary.End <= hi {
		return s.appendForest(roots)
	}
	for _, r := range roots {
		if err := visit(r); err != nil {
			return err
		}
	}
	return nil
}
func (s *SeriesTree) leafAt(start int64) (recordRef, bool) {
	i := sort.Search(len(s.parts), func(i int) bool { return s.parts[i].tree.Summary.End >= start })
	if i == len(s.parts) {
		return recordRef{}, false
	}
	t := s.parts[i].tree
	j := sort.Search(t.LeafCount(), func(j int) bool { return t.NodeAt(0, j).Start >= start })
	if j == t.LeafCount() || t.NodeAt(0, j).Start != start {
		return recordRef{}, false
	}
	return recordRef{tree: t, index: uint32(j), level: 1}, true
}
func (s *SeriesTree) countRange(lo, hi int64) int {
	count := 0
	for _, p := range s.parts {
		if p.tree.Summary.Start > hi {
			break
		}
		leaves := p.tree.LeafCount()
		a := sort.Search(leaves, func(i int) bool { return p.tree.NodeAt(0, i).End >= lo })
		b := sort.Search(leaves, func(i int) bool { return p.tree.NodeAt(0, i).Start > hi })
		count += b - a
	}
	return count
}
func countReused(r recordRef, counts *[MaxLevels]uint64) {
	if r.branch != nil {
		for i, n := range r.branch.retained {
			counts[i] += n
		}
		return
	}
	// A file-local subtree spans contiguous rows at every lower level. Count
	// retained internal nodes from those ranges, without visiting their leaves.
	a, b := r.index, r.index
	for level := int(r.level) - 1; level > 0; level-- {
		counts[level] += uint64(b - a + 1)
		first, last := r.tree.NodeAt(level, int(a)), r.tree.NodeAt(level, int(b))
		a = first.Child
		b = last.Child + last.Children - 1
	}
}

// BuildCatalog joins immutable per-flush trees and extends the previous forest.
// Structural repair is limited to joining boundaries; raw blocks are never read.
func BuildCatalog(sources []Source, prev *Catalog) *Catalog {
	c := catalogParts(sources)
	for key, s := range c.series {
		content := s.contentIdentity()
		old := prev.Series(key)
		if old != nil && old.logical != nil && old.logical.content == content {
			s.logical = old.logical
			for i, nodes := range s.logical.levels {
				c.reused[i+1] += uint64(len(nodes))
			}
			continue
		}
		var reused [MaxLevels]uint64
		if s.restoreShape(old, &reused) {
			if err := s.freeze(content); err == nil {
				s.reuseNodes(old)
				for i, n := range reused {
					c.reused[i] += n
				}
				continue
			}
		}
		s.forests = nil
		ok := true
		for _, p := range s.parts {
			if err := s.appendForest(fileRoots(p.tree)); err != nil {
				ok = false
				break
			}
		}
		if !ok || s.freeze(content) != nil {
			delete(c.series, key)
		}
	}
	same := prev != nil && len(c.series) == len(prev.series)
	for key, s := range c.series {
		same = same && prev.Series(key) != nil && s.logical == prev.Series(key).logical
	}
	if same {
		c.durable.Store(prev.durable.Load())
	}
	return c
}

// Walk visits the persisted forest from its roots. Lower level limits force
// descent through the same nodes; acceptance performs no preliminary leaf scan.
func (s *SeriesTree) Walk(lo, hi int64, limit int, window func(int64) (int64, int64), use func(int, int64, State, bool) error, raw func(*Tree, Block, State) error) error {
	return s.WalkWithStats(lo, hi, limit, window, use, raw, nil)
}
func (s *SeriesTree) WalkWithStats(lo, hi int64, limit int, window func(int64) (int64, int64), use func(int, int64, State, bool) error, raw func(*Tree, Block, State) error, stats *WalkStats) (err error) {
	defer guardReadFailure(&err)
	if s.logical != nil {
		return s.walkLogical(lo, hi, limit, window, use, raw, stats)
	}
	if lo > hi {
		return nil
	}
	accept := func(level int, state State, cross bool) (bool, error) {
		if level > limit || state.Start < lo || state.End > hi {
			return false, nil
		}
		b, e := window(state.Start)
		if state.End >= e {
			return false, nil
		}
		return true, use(level, b, state, cross)
	}
	var local func(*Tree, int, int, int) error
	local = func(t *Tree, level, begin, end int) error {
		first := begin + sort.Search(end-begin, func(i int) bool { return t.node(level-1, begin+i, stats).End >= lo })
		for i := first; i < end; i++ {
			n := t.node(level-1, i, stats)
			if n.Start > hi {
				break
			}
			ok, err := accept(level, n.State, false)
			if err != nil {
				return err
			}
			if ok {
				continue
			}
			if level == 1 {
				if err := raw(t, t.block(int(n.Child), stats), n.State); err != nil {
					return err
				}
			} else {
				if uint64(n.Child)+uint64(n.Children) > uint64(t.RowCount(level-2)) {
					return errForest
				}
				if err := local(t, level-1, int(n.Child), int(n.Child+n.Children)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	state := func(r recordRef) State {
		if r.branch != nil {
			if stats != nil {
				stats.Visited[r.level-1]++
			}
			return r.branch.state
		}
		return r.tree.node(int(r.level)-1, int(r.index), stats).State
	}
	var visit func(forest) error
	visit = func(refs forest) error {
		i := sort.Search(len(refs), func(i int) bool { return state(refs[i]).End >= lo })
		for ; i < len(refs); i++ {
			r := refs[i]
			n := state(r)
			if n.Start > hi {
				break
			}
			if r.tree != nil {
				if err := local(r.tree, int(r.level), int(r.index), int(r.index)+1); err != nil {
					return err
				}
				continue
			}
			ok, err := accept(int(r.level), n, r.branch.firstTree != r.branch.lastTree)
			if err != nil {
				return err
			}
			if ok {
				continue
			}
			if err := visit(r.branch.children); err != nil {
				return err
			}
		}
		return nil
	}
	for _, f := range s.forests {
		if err := visit(f); err != nil {
			return err
		}
	}
	return nil
}
