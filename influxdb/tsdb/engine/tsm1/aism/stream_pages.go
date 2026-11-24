package aism

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"hash/crc32"
)

// NewStreamingBuilder stores completed groups as compressed columns. Its raw
// workspace consists only of each level's unfinished suffix and one page.
func NewStreamingBuilder(k Kind, sources []*Tree, cache *PageCache) *Builder {
	if cache == nil {
		cache = NewPageCache(4 << 20)
	}
	b := NewBuilder(k)
	b.tree.pages = &treePages{cache: cache}
	var indexes []Index
	for _, old := range sources {
		indexes = append(indexes, Index{"": old})
	}
	ReusePages(Index{"": &b.tree}, indexes)
	return b
}
func (b *Builder) streamNode(level int, n Node) error {
	for len(b.tree.pages.levels) <= level {
		b.tree.pages.levels = append(b.tree.pages.levels, pageLevel{})
	}
	layer := &b.tree.pages.levels[level]
	layer.rows++
	b.pageNodes[level] = append(b.pageNodes[level], n)
	if level == 0 {
		b.pageBlocks = append(b.pageBlocks, b.nextBlock)
	}
	if len(b.pageNodes[level]) == pageRows {
		return b.flushPage(level)
	}
	return nil
}
func (b *Builder) flushPage(level int) error {
	nodes := b.pageNodes[level]
	if len(nodes) == 0 {
		return nil
	}
	p := &indexPage{rows: len(nodes), leaf: level == 0}
	tree := &Tree{Kind: b.tree.Kind, Levels: [][]Node{nodes}, Blocks: b.pageBlocks}
	if b.encoder == nil {
		var err error
		b.encoder, err = zlib.NewWriterLevel(&b.compressed, zlib.BestSpeed)
		if err != nil {
			return err
		}
	}
	for c := 0; c < 8; c++ {
		if c == 7 && level != 0 {
			continue
		}
		raw := encodeColumn(tree, 0, 0, len(nodes), c)
		id := sha256.Sum256(raw)
		if old, ok := b.tree.reusable[payloadKey{uint8(c), len(nodes), id}]; ok {
			p.columns[c] = old
			continue
		}
		b.compressed.Reset()
		b.encoder.Reset(&b.compressed)
		if _, err := b.encoder.Write(raw); err != nil {
			return err
		}
		if err := b.encoder.Close(); err != nil {
			return err
		}
		data := bytes.Clone(b.compressed.Bytes())
		p.columns[c] = encodedColumn{bytes: data, crc: crc32.ChecksumIEEE(data), rawID: id}
	}
	b.tree.pages.levels[level].pages = append(b.tree.pages.levels[level].pages, p)
	b.pageNodes[level] = b.pageNodes[level][:0]
	if level == 0 {
		b.pageBlocks = b.pageBlocks[:0]
	}
	return nil
}
func (b *Builder) complete() (*Tree, error) {
	if b.tree.pages != nil {
		for level := range b.tree.pages.levels {
			if err := b.flushPage(level); err != nil {
				return nil, err
			}
		}
	}
	// Return a separate object: an interior pointer into Builder would retain
	// its encoder and unfinished page buffers for every completed field.
	tree := &Tree{Kind: b.tree.Kind, Levels: b.tree.Levels, Blocks: b.tree.Blocks, Summary: b.tree.Summary, pages: b.tree.pages, reusable: b.tree.reusable}
	if tree.pages != nil {
		tree.reusable = nil
	}
	b.tree = Tree{}
	b.pending = [MaxLevels][]pendingNode{}
	b.pageNodes = [MaxLevels][]Node{}
	b.pageBlocks = nil
	b.encoder = nil
	b.compressed = bytes.Buffer{}
	return tree, nil
}
