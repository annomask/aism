package aism

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const shardMagic = "AISMLOG4"

var errCatalog = errors.New("invalid or stale shard aggregate catalog")

// catalogWriter stores logical level/range records separately from current file bindings.
type catalogWriter struct {
	w   io.Writer
	err error
}

func (w *catalogWriter) put(v any) {
	if w.err == nil {
		w.err = binary.Write(w.w, binary.BigEndian, v)
	}
}
func (w *catalogWriter) str(s string) {
	w.put(uint32(len(s)))
	if w.err == nil {
		_, w.err = io.WriteString(w.w, s)
	}
}

// SaveCatalog publishes an optional accelerator after syncing the complete file.
// Native data durability never depends on successful catalog publication.
func SaveCatalog(path string, c *Catalog) (err error) {
	defer guardReadFailure(&err)
	if c == nil {
		return errCatalog
	}
	if saved := c.durable.Load(); saved != nil && saved.path == path {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".aism-shard-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	b := bufio.NewWriterSize(f, 256<<10)
	h := sha256.New()
	out := &catalogWriter{w: io.MultiWriter(b, h)}
	_, out.err = io.WriteString(out.w, shardMagic)
	keys := make([]string, 0, len(c.series))
	for key := range c.series {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out.put(uint32(len(keys)))
	for _, key := range keys {
		s := c.series[key]
		if s.logical == nil {
			return errCatalog
		}
		out.str(key)
		out.put(s.logical.content)
		out.put(uint32(len(s.logical.roots)))
		for _, root := range s.logical.roots {
			out.put(root)
		}
		for _, nodes := range s.logical.levels {
			out.put(uint32(len(nodes)))
			for _, node := range nodes {
				out.put(*node)
			}
		}
	}
	if out.err != nil {
		return out.err
	}
	if _, err = b.Write(h.Sum(nil)); err != nil {
		return err
	}
	if err = b.Flush(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	if err = syncDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	c.durable.Store(&catalogDiskIdentity{path: path})
	return nil
}

type catalogReader struct {
	r   *io.LimitedReader
	err error
}

func (r *catalogReader) get(v any) {
	if r.err == nil {
		r.err = binary.Read(r.r, binary.BigEndian, v)
	}
}
func (r *catalogReader) count(minBytes int64) uint32 {
	var n uint32
	r.get(&n)
	if r.err == nil && (minBytes <= 0 || uint64(n) > uint64(r.r.N/minBytes)) {
		r.err = errCatalog
	}
	return n
}
func (r *catalogReader) str() string {
	n := r.count(1)
	if r.err != nil {
		return ""
	}
	b := make([]byte, n)
	_, r.err = io.ReadFull(r.r, b)
	return string(b)
}

// validateCoverage checks every saved range and aggregate against current native
// leaves. A valid checksum alone is not evidence that a catalog is complete.
func validateCoverage(s *SeriesTree) error {
	if s.logical == nil || len(s.logical.roots) == 0 {
		return errCatalog
	}
	visited := [MaxLevels - 1]int{}
	leafCount := 0
	var previous int64
	var visit func(logicalRoot) (State, error)
	visit = func(root logicalRoot) (State, error) {
		if root.Level == 1 {
			r, ok := s.leafAt(root.Start)
			if !ok || r.state().End != root.End || leafCount > 0 && root.Start <= previous {
				return State{}, errCatalog
			}
			previous = root.End
			leafCount++
			return r.state(), nil
		}
		node, err := s.logicalNode(root)
		if err != nil {
			return State{}, err
		}
		visited[root.Level-2]++
		children, err := s.logicalChildren(root)
		if err != nil {
			return State{}, err
		}
		var state State
		for _, ch := range children {
			v, err := visit(ch)
			if err != nil {
				return State{}, err
			}
			state.Merge(s.parts[0].tree.Kind, v)
		}
		if state != node.State {
			return State{}, errCatalog
		}
		return state, nil
	}
	for _, root := range s.logical.roots {
		if _, err := visit(root); err != nil {
			return err
		}
	}
	if leafCount != s.leaves {
		return errCatalog
	}
	for i, nodes := range s.logical.levels {
		if len(nodes) != visited[i] {
			return errCatalog
		}
		for j, n := range nodes {
			if j > 0 && nodes[j-1].End >= n.Start {
				return errCatalog
			}
		}
	}
	return nil
}

func LoadCatalog(path string, sources []Source) (catalog *Catalog, err error) {
	defer guardReadFailure(&err)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size() - sha256.Size
	if size < int64(len(shardMagic)+4) {
		return nil, errCatalog
	}
	h := sha256.New()
	if _, err = io.Copy(h, io.NewSectionReader(f, 0, size)); err != nil {
		return nil, err
	}
	var check [sha256.Size]byte
	if _, err = f.ReadAt(check[:], size); err != nil {
		return nil, err
	}
	if !bytes.Equal(check[:], h.Sum(nil)) {
		return nil, errCatalog
	}
	input := io.NewSectionReader(f, 0, size)
	var magic [len(shardMagic)]byte
	if _, err = io.ReadFull(input, magic[:]); err != nil || string(magic[:]) != shardMagic {
		return nil, errCatalog
	}
	r := &catalogReader{r: &io.LimitedReader{R: input, N: size - int64(len(magic))}}
	c := catalogParts(sources)
	count := r.count(36)
	if r.err != nil || int(count) != len(c.series) {
		return nil, errCatalog
	}
	prev := ""
	for i := uint32(0); i < count; i++ {
		key := r.str()
		s := c.series[key]
		if r.err != nil || s == nil || key <= prev {
			return nil, errCatalog
		}
		prev = key
		l := &logicalTree{}
		r.get(&l.content)
		if r.err != nil || l.content != s.contentIdentity() {
			return nil, errCatalog
		}
		roots := r.count(17)
		if r.err != nil || roots == 0 || uint64(roots) > uint64(s.leaves) {
			return nil, errCatalog
		}
		l.roots = make([]logicalRoot, roots)
		for j := range l.roots {
			r.get(&l.roots[j])
		}
		for j := range l.levels {
			n := r.count(int64(binary.Size(logicalNode{})))
			if r.err != nil || uint64(n) > uint64(s.leaves) {
				return nil, errCatalog
			}
			l.levels[j] = make([]*logicalNode, n)
			for k := range l.levels[j] {
				node := new(logicalNode)
				r.get(node)
				l.levels[j][k] = node
			}
		}
		if r.err != nil {
			return nil, errCatalog
		}
		s.logical = l
		if err := validateCoverage(s); err != nil {
			return nil, err
		}
	}
	if r.err != nil || r.r.N != 0 {
		return nil, errCatalog
	}
	c.durable.Store(&catalogDiskIdentity{path: path})
	return c, nil
}
