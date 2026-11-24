package aism

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
)

// Index contains resident field headers and immutable, independently compressed
// pages. Loaded indexes do not materialize their full leaf/node arrays.
type Index map[string]*Tree

const magic = "TSMAISM3"
const trailer = 2 * sha256.Size

func dataDigest(data *os.File) ([]byte, error) {
	stat, err := data.Stat()
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	_, err = io.Copy(h, io.NewSectionReader(data, 0, stat.Size()))
	return h.Sum(nil), err
}
func Save(path string, data *os.File, index Index) error {
	id, err := dataDigest(data)
	if err != nil {
		return err
	}
	return SaveIdentity(path, id, index)
}

func encodeColumn(t *Tree, level, first, count, column int) []byte {
	if level != 0 && column == 7 {
		return nil
	}
	var b bytes.Buffer
	put := func(v any) { _ = binary.Write(&b, binary.BigEndian, v) }
	for i := first; i < first+count; i++ {
		n := t.NodeAt(level, i)
		switch column {
		case 0:
			put(n.Start)
			put(n.End)
			put(n.Child)
			put(n.Children)
		case 1:
			put(n.N)
		case 2:
			put(n.Total)
			put(n.Precision)
		case 3:
			put(n.Minimum)
			put(n.MinTime)
		case 4:
			put(n.Maximum)
			put(n.MaxTime)
		case 5:
			put(n.FirstValue)
		case 6:
			put(n.LastValue)
		case 7:
			put(t.BlockAt(i))
		}
	}
	return b.Bytes()
}

// SaveIdentity accepts the digest collected by the native streaming writer.
func SaveIdentity(path string, id []byte, index Index) (err error) {
	defer guardReadFailure(&err)
	if len(id) != sha256.Size {
		return errors.New("invalid TSM identity")
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".aism-build-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	buffered := bufio.NewWriterSize(f, 256<<10)
	hash := sha256.New()
	out := &catalogWriter{w: io.MultiWriter(buffered, hash)}
	_, out.err = io.WriteString(out.w, magic)
	keys := make([]string, 0, len(index))
	for key := range index {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out.put(uint32(len(keys)))
	var compressed bytes.Buffer
	encoder, _ := zlib.NewWriterLevel(&compressed, zlib.BestSpeed)
	defer encoder.Close()
	for _, key := range keys {
		t := index[key]
		if err := validateTree(t); err != nil {
			return err
		}
		out.str(key)
		out.put(uint8(t.Kind))
		out.put(t.Summary)
		out.put(treeIdentity(t))
		out.put(uint8(t.LevelCount()))
		for level := 0; level < t.LevelCount(); level++ {
			rows := t.RowCount(level)
			out.put(uint32(rows))
			out.put(uint32((rows + pageRows - 1) / pageRows))
			for first := 0; first < rows; first += pageRows {
				count := min(pageRows, rows-first)
				out.put(uint32(count))
				for column := 0; column < 8; column++ {
					if t.pages != nil {
						col := t.pages.levels[level].pages[first/pageRows].columns[column]
						out.put(uint32(len(col.bytes)))
						out.put(col.crc)
						out.put(col.rawID)
						if out.err == nil {
							payload, readErr := col.payload()
							if readErr != nil {
								return readErr
							}
							_, out.err = out.w.Write(payload)
						}
						runtime.KeepAlive(col.owner)
						continue
					}
					raw := encodeColumn(t, level, first, count, column)
					if len(raw) == 0 {
						out.put(uint32(0))
						out.put(uint32(0))
						out.put([32]byte{})
						continue
					}

					rawID := sha256.Sum256(raw)
					reused, ok := t.reusable[payloadKey{uint8(column), count, rawID}]
					var payload []byte
					if ok {
						var readErr error
						payload, readErr = reused.payload()
						if readErr != nil {
							return readErr
						}
					} else {
						compressed.Reset()
						encoder.Reset(&compressed)
						if _, err := encoder.Write(raw); err != nil {
							return err
						}
						if err := encoder.Close(); err != nil {
							return err
						}
						payload = compressed.Bytes()
					}
					out.put(uint32(len(payload)))
					out.put(crc32.ChecksumIEEE(payload))
					out.put(rawID)

					if out.err == nil {
						_, out.err = out.w.Write(payload)
					}
					runtime.KeepAlive(reused.owner)
				}
			}
		}
	}
	if out.err != nil {
		return out.err
	}
	if _, err = out.w.Write(id); err != nil {
		return err
	}
	if _, err = buffered.Write(hash.Sum(nil)); err != nil {
		return err
	}
	if err = buffered.Flush(); err != nil {
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
	return syncDirectory(filepath.Dir(path))
}

func Load(path string, data *os.File) (index Index, err error) {
	defer guardReadFailure(&err)
	mapped, err := mapIndex(path)
	if err != nil {
		return nil, err
	}
	defer runtime.KeepAlive(mapped)
	b := mapped.data
	if len(b) < len(magic)+4+trailer || string(b[:len(magic)]) != magic {
		return nil, errors.New("unknown or short aggregate index")
	}
	end := len(b) - trailer
	digest := sha256.Sum256(b[:len(b)-sha256.Size])
	if !bytes.Equal(digest[:], b[len(b)-sha256.Size:]) {
		return nil, errors.New("aggregate index checksum mismatch")
	}
	id, err := dataDigest(data)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(id, b[end:end+sha256.Size]) {
		return nil, errors.New("aggregate index belongs to another TSM file")
	}
	body := b[len(magic):end]
	reader := bytes.NewReader(body)
	in := &catalogReader{r: &io.LimitedReader{R: reader, N: int64(len(body))}}
	n := in.count(127)
	if in.err != nil {
		return nil, in.err
	}
	idx := make(Index)
	prev := ""
	cache := NewPageCache(4 << 20)
	for field := uint32(0); field < n; field++ {
		key := in.str()
		var kind, levels uint8
		var summary State
		var identity [32]byte
		in.get(&kind)
		in.get(&summary)
		in.get(&identity)
		in.get(&levels)
		if in.err != nil || key <= prev || Kind(kind) < Integer || Kind(kind) > Unsigned || levels == 0 || levels > MaxLevels {
			return nil, errors.New("invalid aggregate field header")
		}
		prev = key
		t := &Tree{Kind: Kind(kind), Summary: summary, identity: identity, identityValid: true, pages: &treePages{cache: cache}}
		for level := 0; level < int(levels); level++ {
			rows := in.count(1)
			groups := in.count(324)
			if in.err != nil || rows == 0 || uint64(groups) != (uint64(rows)+pageRows-1)/pageRows {
				return nil, errors.New("invalid aggregate level")
			}
			layer := pageLevel{rows: int(rows)}
			for group := uint32(0); group < groups; group++ {
				count := in.count(1)
				if in.err != nil || int(count) != min(pageRows, int(rows)-int(group)*pageRows) {
					return nil, errors.New("invalid aggregate page count")
				}
				p := &indexPage{owner: mapped, rows: int(count), leaf: level == 0}
				for column := 0; column < 8; column++ {
					length := in.count(1)
					var crc uint32
					var rawID [32]byte
					in.get(&crc)
					in.get(&rawID)
					if in.err != nil || int64(length) > in.r.N {
						return nil, errors.New("truncated aggregate column")
					}
					start := int64(len(body)) - in.r.N
					p.columns[column] = encodedColumn{bytes: body[start : start+int64(length)], crc: crc, rawID: rawID, owner: mapped, offset: int64(len(magic)) + start}
					_, _ = reader.Seek(int64(length), io.SeekCurrent)
					in.r.N -= int64(length)
					decoded, err := p.decode(column)
					if err != nil {
						return nil, err
					}
					if len(decoded) > 0 {
						cache.put(columnKey{p, uint8(column)}, decoded)
					}
				}
				layer.pages = append(layer.pages, p)
			}
			t.pages.levels = append(t.pages.levels, layer)
		}
		if err = validateTree(t); err != nil {
			return nil, err
		}
		if digestTree(t) != identity {
			return nil, errors.New("aggregate logical identity mismatch")
		}
		idx[key] = t
	}
	if in.err != nil || in.r.N != 0 {
		return nil, errors.New("trailing aggregate index data")
	}
	return idx, nil
}

// payloadKey is independent of file numbers and offsets. Statistics can reuse
// complete compressed columns when native compaction preserves their records.
type payloadKey struct {
	column uint8
	rows   int
	rawID  [32]byte
}

func ReusePages(index Index, sources []Index) {
	for key, t := range index {
		t.reusable = make(map[payloadKey]encodedColumn)
		for _, src := range sources {
			old := src[key]
			if old == nil || old.pages == nil || old.Kind != t.Kind {
				continue
			}
			for _, l := range old.pages.levels {
				for _, p := range l.pages {
					for c, col := range p.columns {
						if len(col.bytes) > 0 {
							t.reusable[payloadKey{uint8(c), p.rows, col.rawID}] = col
						}
					}
				}
			}
		}
	}
}
