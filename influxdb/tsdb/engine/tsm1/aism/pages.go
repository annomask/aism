package aism

import (
	"bytes"
	"compress/zlib"
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/influxdata/influxdb/v2/pkg/mmap"
)

const pageRows = 256

var columnWidth = [8]int{24, 8, 13, 16, 16, 8, 8, 44}

type indexMapping struct {
	data []byte
	file *os.File
}

func mapIndex(path string) (*indexMapping, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if st.Size() > 1<<30 {
		return nil, errors.New("aggregate index exceeds mapping limit")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	b, err := mmap.Map(path, 0)
	if err != nil {
		file.Close()
		return nil, err
	}
	m := &indexMapping{data: b, file: file}
	runtime.SetFinalizer(m, func(m *indexMapping) { _ = mmap.Unmap(m.data); _ = m.file.Close() })
	return m, nil
}

type encodedColumn struct {
	offset int64
	owner  *indexMapping
	bytes  []byte
	crc    uint32
	rawID  [32]byte
}
type indexPage struct {
	owner   *indexMapping
	rows    int
	leaf    bool
	columns [8]encodedColumn
}
type pageLevel struct {
	rows  int
	pages []*indexPage
}
type treePages struct {
	levels []pageLevel
	cache  *PageCache
}
type columnKey struct {
	page   *indexPage
	column uint8
}
type cacheEntry struct {
	key  columnKey
	data []byte
}

// PageCache accounts decoded payload bytes. Locks protect only lookup and LRU
// installation; decompression always runs outside the cache lock.
type PageCache struct {
	mu           sync.Mutex
	limit, bytes int
	entries      map[columnKey]*list.Element
	lru          list.List
}

func NewPageCache(limit int) *PageCache {
	return &PageCache{limit: max(0, limit), entries: make(map[columnKey]*list.Element)}
}
func (c *PageCache) get(key columnKey) []byte {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		c.lru.MoveToFront(e)
		return e.Value.(cacheEntry).data
	}
	return nil
}
func (c *PageCache) put(key columnKey, data []byte) []byte {
	if c == nil || len(data) > c.limit || c.limit == 0 {
		return data
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		c.lru.MoveToFront(e)
		return e.Value.(cacheEntry).data
	}
	for c.bytes+len(data) > c.limit {
		e := c.lru.Back()
		v := e.Value.(cacheEntry)
		c.bytes -= len(v.data)
		delete(c.entries, v.key)
		c.lru.Remove(e)
	}
	c.entries[key] = c.lru.PushFront(cacheEntry{key, data})
	c.bytes += len(data)
	return data
}
func UseCache(idx Index, cache *PageCache) {
	for _, t := range idx {
		if t.pages != nil {
			t.pages.cache = cache
		}
	}
}

// WalkStats belongs to one series expression. Local columns avoid cache locks
// per record while retaining at most 4 MiB of decoded payload references.
type WalkStats struct {
	Op                                         Op
	Visited                                    [MaxLevels]uint64
	PagesDecoded, ColumnsDecoded, PayloadBytes uint64
	decodedPages                               map[*indexPage]bool
	local                                      map[columnKey][]byte
	localBytes                                 int
}

func (s *WalkStats) remember(key columnKey, data []byte) {
	if s == nil || len(data) > 4<<20 {
		return
	}
	if s.localBytes+len(data) > 4<<20 {
		s.local = nil
		s.localBytes = 0
	}
	if s.local == nil {
		s.local = make(map[columnKey][]byte)
	}
	s.local[key] = data
	s.localBytes += len(data)
}

type pageReadError struct{ err error }

func (e pageReadError) Error() string { return e.err.Error() }
func ReadFailure(value any) error {
	if e, ok := value.(pageReadError); ok {
		return e.err
	}
	return nil
}
func guardReadFailure(err *error) {
	if value := recover(); value != nil {
		if failure := ReadFailure(value); failure != nil {
			*err = failure
		} else {
			panic(value)
		}
	}
}

// Keep a version-pinned index handle, just like the native data reader. Runtime
// reads cannot fault on an externally truncated mapping: ReadAt returns an error.
func (c encodedColumn) payload() ([]byte, error) {
	if c.owner == nil {
		return c.bytes, nil
	}
	b := make([]byte, len(c.bytes))
	_, err := c.owner.file.ReadAt(b, c.offset)
	runtime.KeepAlive(c.owner)
	return b, err
}

func (p *indexPage) decode(column int) ([]byte, error) {
	expected := p.rows * columnWidth[column]
	if column == 7 && !p.leaf {
		expected = 0
	}
	c := p.columns[column]
	if expected == 0 {
		if len(c.bytes) != 0 || c.crc != 0 || c.rawID != ([32]byte{}) {
			return nil, errors.New("unexpected aggregate column")
		}
		return nil, nil
	}
	payload, err := c.payload()
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 || crc32.ChecksumIEEE(payload) != c.crc {
		return nil, errors.New("aggregate column checksum mismatch")
	}
	compressed := bytes.NewReader(payload)
	r, err := zlib.NewReader(compressed)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(r, int64(expected+1)))
	closeErr := r.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(b) != expected || compressed.Len() != 0 || sha256.Sum256(b) != c.rawID {
		return nil, errors.New("invalid aggregate column length")
	}
	runtime.KeepAlive(p.owner)
	return b, nil
}
func (t *Tree) column(p *indexPage, c int, stats *WalkStats) []byte {
	key := columnKey{p, uint8(c)}
	if stats != nil {
		if b, ok := stats.local[key]; ok {
			return b
		}
		stats.PayloadBytes += uint64(len(p.columns[c].bytes))
	}
	data := t.pages.cache.get(key)
	if data == nil {
		var err error
		data, err = p.decode(c)
		// Frames were validated before publication. External damage or a failed
		// runtime read invalidates this optional accelerator, not native data.
		if err != nil {
			panic(pageReadError{err})
		}
		if stats != nil {
			stats.ColumnsDecoded++
			if stats.decodedPages == nil {
				stats.decodedPages = make(map[*indexPage]bool)
			}
			if !stats.decodedPages[p] {
				stats.PagesDecoded++
				stats.decodedPages[p] = true
			}
		}
		data = t.pages.cache.put(key, data)
	}
	stats.remember(key, data)
	return data
}
func (t *Tree) LevelCount() int {
	if t.pages != nil {
		return len(t.pages.levels)
	}
	return len(t.Levels)
}
func (t *Tree) RowCount(level int) int {
	if t.pages != nil {
		return t.pages.levels[level].rows
	}
	return len(t.Levels[level])
}
func (t *Tree) LeafCount() int               { return t.RowCount(0) }
func (t *Tree) NodeAt(level, index int) Node { return t.node(level, index, nil) }
func (t *Tree) BlockAt(index int) Block      { return t.block(index, nil) }
func (t *Tree) node(level, index int, stats *WalkStats) Node {
	if stats != nil {
		stats.Visited[level]++
	}
	if t.pages == nil {
		return t.Levels[level][index]
	}
	p := t.pages.levels[level].pages[index/pageRows]
	i := index % pageRows
	position := t.column(p, 0, stats)[i*24 : (i+1)*24]
	n := Node{State: State{Start: int64(binary.BigEndian.Uint64(position)), End: int64(binary.BigEndian.Uint64(position[8:]))}, Child: binary.BigEndian.Uint32(position[16:]), Children: binary.BigEndian.Uint32(position[20:])}
	n.N = binary.BigEndian.Uint64(t.column(p, 1, stats)[i*8:])
	op := Op(0)
	if stats != nil {
		op = stats.Op
	}
	if op == 0 || op == Sum || op == Mean {
		b := t.column(p, 2, stats)[i*13:]
		n.Total = binary.BigEndian.Uint64(b)
		n.Precision = Precision{b[8], int16(binary.BigEndian.Uint16(b[9:])), int16(binary.BigEndian.Uint16(b[11:]))}
	}
	if op == 0 || op == Min {
		b := t.column(p, 3, stats)[i*16:]
		n.Minimum = binary.BigEndian.Uint64(b)
		n.MinTime = int64(binary.BigEndian.Uint64(b[8:]))
	}
	if op == 0 || op == Max {
		b := t.column(p, 4, stats)[i*16:]
		n.Maximum = binary.BigEndian.Uint64(b)
		n.MaxTime = int64(binary.BigEndian.Uint64(b[8:]))
	}
	if op == 0 || op == First {
		n.FirstValue = binary.BigEndian.Uint64(t.column(p, 5, stats)[i*8:])
	}
	if op == 0 || op == Last {
		n.LastValue = binary.BigEndian.Uint64(t.column(p, 6, stats)[i*8:])
	}
	return n
}
func (t *Tree) block(index int, stats *WalkStats) Block {
	if t.pages == nil {
		return t.Blocks[index]
	}
	p := t.pages.levels[0].pages[index/pageRows]
	i := index % pageRows
	b := t.column(p, 7, stats)[i*44:]
	block := Block{Offset: int64(binary.BigEndian.Uint64(b)), Size: binary.BigEndian.Uint32(b[8:])}
	copy(block.Digest[:], b[12:44])
	return block
}
