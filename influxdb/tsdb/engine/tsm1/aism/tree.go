// Package aism indexes ordered physical blocks of one TSM series-field key.
package aism

import (
	"bytes"
	"compress/zlib"
	"errors"
	"math"
	"math/bits"
	"sort"
	"sync"
)

const (
	Fanout    = 64
	MaxLevels = 5
)

type Kind byte

const (
	Integer Kind = iota + 1
	Float
	Boolean
	Unsigned
)

type Op byte

const (
	Count Op = iota + 1
	Sum
	Mean
	Min
	Max
	First
	Last
)

func less(k Kind, a, b uint64) bool {
	if k == Float {
		return math.Float64frombits(a) < math.Float64frombits(b)
	}
	if k == Unsigned {
		return a < b
	}
	return int64(a) < int64(b)
}

// Precision describes a common binary quantum for a set of floating-point inputs.
type Precision struct {
	Flags     uint8
	Low, High int16
}

const (
	positive = 1 << iota
	negative
	nonfinite
	negativeZero
)

func precision(v uint64) Precision {
	f := math.Float64frombits(v)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return Precision{Flags: nonfinite}
	}
	if f == 0 {
		if math.Signbit(f) {
			return Precision{Flags: negativeZero}
		}
		return Precision{}
	}
	exp := int((v >> 52) & 0x7ff)
	mant := v & ((1 << 52) - 1)
	if exp == 0 {
		exp = 1
	} else {
		mant |= 1 << 52
	}
	unit := exp - 1023 - 52
	flags := uint8(positive)
	if f < 0 {
		flags = negative
	}
	return Precision{flags, int16(unit + bits.TrailingZeros64(mant)), int16(unit + bits.Len64(mant) - 1)}
}
func (p *Precision) Include(q Precision) {
	if q.Flags&(positive|negative) != 0 {
		if p.Flags&(positive|negative) == 0 || q.Low < p.Low {
			p.Low = q.Low
		}
		if p.Flags&(positive|negative) == 0 || q.High > p.High {
			p.High = q.High
		}
	}
	p.Flags |= q.Flags
}
func (p Precision) Usable(op Op, count uint64) bool {
	if op == Count || op == First || op == Last {
		return true
	}
	if p.Flags&nonfinite != 0 {
		return false
	}
	if op != Sum && op != Mean {
		return true
	}
	if p.Flags&negativeZero != 0 {
		return false
	}
	if p.Flags&(positive|negative) == 0 || count == 0 {
		return true
	}
	// Every partial sum, including the sequential native sum, must be exactly
	// representable. Bounding the sum of absolute magnitudes also covers cancellation.
	growth := bits.Len64(count - 1)
	return int(p.High)-int(p.Low)+growth < 53 && int(p.High)+growth <= 1023
}

type State struct {
	Start, End                                     int64
	N                                              uint64
	Total, Minimum, Maximum, FirstValue, LastValue uint64
	MinTime, MaxTime                               int64
	Precision                                      Precision
}

func Point(k Kind, t int64, v uint64) State {
	s := State{Start: t, End: t, N: 1, Total: v, Minimum: v, Maximum: v, FirstValue: v, LastValue: v, MinTime: t, MaxTime: t}
	if k == Float {
		s.Precision = precision(v)
	}
	return s
}

// Merge accepts its inputs in chronological order, preserving the earliest extrema on ties.
func (s *State) Merge(k Kind, b State) {
	if b.N == 0 {
		return
	}
	if s.N == 0 {
		*s = b
		return
	}
	s.N += b.N
	if k == Float {
		s.Total = math.Float64bits(math.Float64frombits(s.Total) + math.Float64frombits(b.Total))
		s.Precision.Include(b.Precision)
	} else {
		s.Total += b.Total
	}
	if less(k, b.Minimum, s.Minimum) {
		s.Minimum, s.MinTime = b.Minimum, b.MinTime
	}
	if less(k, s.Maximum, b.Maximum) {
		s.Maximum, s.MaxTime = b.Maximum, b.MaxTime
	}
	s.End, s.LastValue = b.End, b.LastValue
}

// Result contains the aggregate for one nonempty bucket. Value uses the field's native bits.
type Result struct {
	Bucket, Time int64
	Value, N     uint64
}

func (r *Result) Fold(k Kind, op Op, bucket int64, b State) {
	first := r.N == 0
	r.Bucket = bucket
	r.N += b.N
	switch op {
	case Count:
		r.Time, r.Value = bucket, r.N
	case Sum, Mean:
		r.Time = bucket
		if first {
			r.Value = b.Total
		} else if k == Float {
			r.Value = math.Float64bits(math.Float64frombits(r.Value) + math.Float64frombits(b.Total))
		} else {
			r.Value += b.Total
		}
	case Min:
		if first || less(k, b.Minimum, r.Value) {
			r.Time, r.Value = b.MinTime, b.Minimum
		}
	case Max:
		if first || less(k, r.Value, b.Maximum) {
			r.Time, r.Value = b.MaxTime, b.Maximum
		}
	case First:
		if first {
			r.Time, r.Value = b.Start, b.FirstValue
		}
	case Last:
		r.Time, r.Value = b.End, b.LastValue
	}
}

type Block struct {
	Offset int64
	Size   uint32
	Digest [32]byte
}
type Node struct {
	State
	// Child is the first child in the lower level; in a leaf it indexes Blocks.
	Child, Children uint32
}
type Tree struct {
	reusable      map[payloadKey]encodedColumn
	pages         *treePages
	identity      [32]byte
	identityValid bool
	identityOnce  sync.Once
	Kind          Kind
	Levels        [][]Node
	Blocks        []Block
	Summary       State
}

// Builder retains at most two unfinished nodes per level. Completed records
// are appended once to the immutable output, independently of batch boundaries.
type Builder struct {
	rows       [MaxLevels]uint32
	pageNodes  [MaxLevels][]Node
	pageBlocks []Block
	nextBlock  Block
	compressed bytes.Buffer
	encoder    *zlib.Writer
	tree       Tree
	pending    [MaxLevels][]pendingNode
	previous   int64
	failed     bool
}
type pendingNode struct {
	node  Node
	index uint32
}

func NewBuilder(k Kind) *Builder { return &Builder{tree: Tree{Kind: k}} }
func (b *Builder) feed(level int, n Node) error {
	if level >= MaxLevels {
		return errors.New("aggregate tree capacity exceeded")
	}
	index := b.rows[level]
	b.rows[level]++
	if b.tree.pages == nil {
		for len(b.tree.Levels) <= level {
			b.tree.Levels = append(b.tree.Levels, nil)
		}
		b.tree.Levels[level] = append(b.tree.Levels[level], n)
	} else {
		if err := b.streamNode(level, n); err != nil {
			return err
		}
	}
	b.pending[level] = append(b.pending[level], pendingNode{n, index})
	if level == MaxLevels-1 && len(b.pending[level]) > Fanout {
		return errors.New("aggregate tree capacity exceeded")
	}
	if level < MaxLevels-1 && len(b.pending[level]) == 2*Fanout {
		parent := b.parent(b.pending[level][:Fanout])
		copy(b.pending[level], b.pending[level][Fanout:])
		b.pending[level] = b.pending[level][:Fanout]
		return b.feed(level+1, parent)
	}
	return nil
}
func (b *Builder) parent(children []pendingNode) Node {
	p := Node{Child: children[0].index, Children: uint32(len(children))}
	for _, ch := range children {
		p.Merge(b.tree.Kind, ch.node.State)
	}
	return p
}
func (b *Builder) Push(n Node, block Block) (err error) {
	if b.failed {
		return errors.New("aggregate builder is failed")
	}
	defer func() {
		if err != nil {
			b.failed = true
		}
	}()
	if b.tree.Kind < Integer || b.tree.Kind > Unsigned || n.N == 0 || n.Start > n.End || b.rows[0] > 0 && b.previous >= n.Start {
		return errors.New("overlapping or empty TSM block")
	}
	n.Child = b.rows[0]
	n.Children = 0
	if b.tree.pages == nil {
		b.tree.Blocks = append(b.tree.Blocks, block)
	} else {
		b.nextBlock = block
	}
	b.tree.Summary.Merge(b.tree.Kind, n.State)
	b.previous = n.End
	return b.feed(0, n)
}
func (b *Builder) Finish() (*Tree, error) {
	if b.failed || b.rows[0] == 0 {
		return nil, errors.New("empty or failed aggregate builder")
	}
	b.failed = true
	for level := 0; level < MaxLevels; level++ {
		pending := b.pending[level]
		b.pending[level] = nil
		higher := false
		for _, p := range b.pending[level+1:] {
			higher = higher || len(p) > 0
		}
		if !higher && len(pending) <= Fanout {
			return b.complete()
		}
		if len(pending) < Fanout/2 || level+1 == MaxLevels {
			return nil, errors.New("incomplete aggregate level coverage")
		}
		split := len(pending)
		if split > Fanout {
			if split < Fanout+Fanout/2 {
				split /= 2
			} else {
				split = Fanout
			}
		}
		for _, group := range [][]pendingNode{pending[:split], pending[split:]} {
			if len(group) > 0 {
				if err := b.feed(level+1, b.parent(group)); err != nil {
					return nil, err
				}
			}
		}
	}
	return nil, errors.New("aggregate tree capacity exceeded")
}
func Build(k Kind, leaves []Node, blocks []Block) (*Tree, error) {
	if len(leaves) != len(blocks) {
		return nil, errors.New("invalid leaf sequence")
	}
	b := NewBuilder(k)
	for i, n := range leaves {
		if err := b.Push(n, blocks[i]); err != nil {
			return nil, err
		}
	}
	return b.Finish()
}

// Walk covers [lo,hi] in chronological order. Window returns InfluxQL's
// half-open bucket. A zero limit forces the same walker to read raw blocks.
func (t *Tree) Walk(lo, hi int64, limit int, window func(int64) (int64, int64), use func(int, int64, State) error, raw func(Block, State) error) error {
	return t.WalkWithStats(lo, hi, limit, window, use, raw, nil)
}
func (t *Tree) WalkWithStats(lo, hi int64, limit int, window func(int64) (int64, int64), use func(int, int64, State) error, raw func(Block, State) error, stats *WalkStats) (err error) {
	defer guardReadFailure(&err)
	if lo > hi {
		return nil
	}
	level := min(max(1, limit), t.LevelCount()) - 1
	var visit func(int, int, int) error
	visit = func(l, begin, end int) error {
		i := begin + sort.Search(end-begin, func(i int) bool { return t.node(l, begin+i, stats).End >= lo })
		for ; i < end; i++ {
			n := t.node(l, i, stats)
			if n.Start > hi {
				break
			}
			if limit > 0 && n.Start >= lo && n.End <= hi {
				start, end := window(n.Start)
				if n.End < end {
					if err := use(l+1, start, n.State); err != nil {
						return err
					}
					continue
				}
			}
			if l == 0 {
				if err := raw(t.block(int(n.Child), stats), n.State); err != nil {
					return err
				}
			} else if err := visit(l-1, int(n.Child), int(n.Child+n.Children)); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(level, 0, t.RowCount(level))
}
