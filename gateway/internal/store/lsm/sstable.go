package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

// SSTable binary format
//
//	┌──────────────────────────────────────────────────────┐
//	│  Data Region                                         │
//	│  ┌─────────────────────────────────────────────┐    │
//	│  │  Block 0  (≤ blockSize bytes)               │    │
//	│  │    entry: keyLen:2 | key | valLen:4 | val   │    │
//	│  │    entry: …                                 │    │
//	│  └─────────────────────────────────────────────┘    │
//	│  … more blocks …                                     │
//	├──────────────────────────────────────────────────────┤
//	│  Index Region                                        │
//	│    blockCount:4                                      │
//	│    [ offset:8 | blockLen:4 | keyLen:2 | firstKey ]*  │
//	├──────────────────────────────────────────────────────┤
//	│  Bloom Filter Region                                 │
//	│    k:4 | m:8 | bits…                                 │
//	├──────────────────────────────────────────────────────┤
//	│  Footer (32 bytes, fixed)                            │
//	│    indexOffset:8 | indexLen:4                        │
//	│    bloomOffset:8 | bloomLen:4                        │
//	│    magic:8  ("INDEXQUB" = 0x42555145584E4449)        │
//	└──────────────────────────────────────────────────────┘
//
// tombstoneLen (0xFFFFFFFF) in the valLen field signals a deleted key;
// no value bytes follow it.
const (
	sstMagic     = uint64(0x42555145584E4449) // "INDEXQUB" little-endian
	footerSize   = 32
	tombstoneLen = ^uint32(0) // 0xFFFFFFFF
)

// indexEntry records the file position and first key of one data block.
type indexEntry struct {
	offset   int64
	blockLen uint32
	firstKey []byte
}

// ─── Builder ──────────────────────────────────────────────────────────────────

// Builder writes a new SSTable to disk from an ordered stream of key-value pairs.
// Keys must be supplied in strictly ascending lexicographic order.
//
// Usage:
//
//	b, _ := newBuilder(path, opts)
//	b.Add(key1, val1)
//	b.Add(key2, val2)  // key2 > key1
//	b.Finish()
type Builder struct {
	path  string
	f     *os.File
	bw    *bufio.Writer

	blockBuf bytes.Buffer // current data block accumulator
	blockOff int64        // file offset where blockBuf started
	index    []indexEntry
	bloom    *Bloom
	opts     Options
}

func newBuilder(path string, opts Options) (*Builder, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Builder{
		path:  path,
		f:     f,
		bw:    bufio.NewWriterSize(f, 64*1024),
		bloom: NewBloom(max(opts.BloomExpected, 1), opts.BloomFPRate),
		opts:  opts,
	}, nil
}

// Add appends a key-value pair. val == nil writes a tombstone.
func (b *Builder) Add(key, val []byte) error {
	if len(key) > 0xFFFF {
		return fmt.Errorf("lsm: key too long (%d bytes)", len(key))
	}

	// Start a new index entry when the current block is empty.
	if b.blockBuf.Len() == 0 {
		b.index = append(b.index, indexEntry{
			offset:   b.blockOff,
			firstKey: append([]byte(nil), key...),
		})
	}

	b.bloom.Add(key)

	// Entry wire format: keyLen:2 | key | valLen:4 | val
	var hdr [6]byte
	binary.LittleEndian.PutUint16(hdr[0:], uint16(len(key)))
	if val == nil {
		binary.LittleEndian.PutUint32(hdr[2:], tombstoneLen)
	} else {
		binary.LittleEndian.PutUint32(hdr[2:], uint32(len(val)))
	}
	b.blockBuf.Write(hdr[:])
	b.blockBuf.Write(key)
	if val != nil {
		b.blockBuf.Write(val)
	}

	if b.blockBuf.Len() >= b.opts.BlockSize {
		return b.flushBlock()
	}
	return nil
}

func (b *Builder) flushBlock() error {
	if b.blockBuf.Len() == 0 {
		return nil
	}
	n := b.blockBuf.Len()
	b.index[len(b.index)-1].blockLen = uint32(n)
	if _, err := b.bw.Write(b.blockBuf.Bytes()); err != nil {
		return err
	}
	b.blockOff += int64(n)
	b.blockBuf.Reset()
	return nil
}

// Finish flushes the final block and writes the index, Bloom filter, and footer.
// The file is closed; the Builder must not be used after Finish returns.
func (b *Builder) Finish() error {
	if err := b.flushBlock(); err != nil {
		return err
	}
	if len(b.index) == 0 {
		// No entries were added — write an empty but valid SSTable.
		b.index = []indexEntry{}
	}

	// ── Index block ───────────────────────────────────────────────────────
	indexStart := b.blockOff
	var ibuf bytes.Buffer
	binary.Write(&ibuf, binary.LittleEndian, uint32(len(b.index))) //nolint:errcheck
	for _, e := range b.index {
		binary.Write(&ibuf, binary.LittleEndian, e.offset)          //nolint:errcheck
		binary.Write(&ibuf, binary.LittleEndian, e.blockLen)        //nolint:errcheck
		binary.Write(&ibuf, binary.LittleEndian, uint16(len(e.firstKey))) //nolint:errcheck
		ibuf.Write(e.firstKey)
	}
	indexData := ibuf.Bytes()
	if _, err := b.bw.Write(indexData); err != nil {
		return err
	}

	// ── Bloom filter ──────────────────────────────────────────────────────
	bloomStart := indexStart + int64(len(indexData))
	bloomData := b.bloom.Encode()
	if _, err := b.bw.Write(bloomData); err != nil {
		return err
	}

	// ── Footer (32 bytes) ─────────────────────────────────────────────────
	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:], uint64(indexStart))
	binary.LittleEndian.PutUint32(footer[8:], uint32(len(indexData)))
	binary.LittleEndian.PutUint64(footer[12:], uint64(bloomStart))
	binary.LittleEndian.PutUint32(footer[20:], uint32(len(bloomData)))
	binary.LittleEndian.PutUint64(footer[24:], sstMagic)
	if _, err := b.bw.Write(footer[:]); err != nil {
		return err
	}

	if err := b.bw.Flush(); err != nil {
		return err
	}
	return b.f.Close()
}

// ─── Reader ───────────────────────────────────────────────────────────────────

// SSTable is an open, immutable, read-only view of a single on-disk SSTable file.
// It is safe for concurrent reads via Get and Iterator.
type SSTable struct {
	f     *os.File
	index []indexEntry
	bloom *Bloom
	path  string
}

// openSSTable opens the SSTable at path for reading.
func openSSTable(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size() < footerSize {
		f.Close()
		return nil, fmt.Errorf("lsm: %s is too small to be a valid SSTable", path)
	}

	var foot [footerSize]byte
	if _, err := f.ReadAt(foot[:], fi.Size()-footerSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("lsm: read footer of %s: %w", path, err)
	}
	if binary.LittleEndian.Uint64(foot[24:]) != sstMagic {
		f.Close()
		return nil, fmt.Errorf("lsm: %s has invalid magic", path)
	}

	indexOff := int64(binary.LittleEndian.Uint64(foot[0:]))
	indexLen := int(binary.LittleEndian.Uint32(foot[8:]))
	bloomOff := int64(binary.LittleEndian.Uint64(foot[12:]))
	bloomLen := int(binary.LittleEndian.Uint32(foot[20:]))

	indexData := make([]byte, indexLen)
	if _, err := f.ReadAt(indexData, indexOff); err != nil {
		f.Close()
		return nil, fmt.Errorf("lsm: read index of %s: %w", path, err)
	}
	idx, err := decodeIndex(indexData)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("lsm: decode index of %s: %w", path, err)
	}

	bloomData := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomData, bloomOff); err != nil {
		f.Close()
		return nil, fmt.Errorf("lsm: read bloom of %s: %w", path, err)
	}
	bloom, err := DecodeBloom(bloomData)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("lsm: decode bloom of %s: %w", path, err)
	}

	return &SSTable{f: f, index: idx, bloom: bloom, path: path}, nil
}

func decodeIndex(data []byte) ([]indexEntry, error) {
	if len(data) < 4 {
		return nil, errors.New("index too short")
	}
	count := int(binary.LittleEndian.Uint32(data))
	data = data[4:]
	entries := make([]indexEntry, 0, count)
	for i := range count {
		const fixedPart = 8 + 4 + 2 // offset + blockLen + keyLen
		if len(data) < fixedPart {
			return nil, fmt.Errorf("index entry %d truncated", i)
		}
		off := int64(binary.LittleEndian.Uint64(data[0:]))
		blen := binary.LittleEndian.Uint32(data[8:])
		klen := int(binary.LittleEndian.Uint16(data[12:]))
		data = data[fixedPart:]
		if len(data) < klen {
			return nil, fmt.Errorf("index entry %d: key truncated", i)
		}
		entries = append(entries, indexEntry{
			offset:   off,
			blockLen: blen,
			firstKey: append([]byte(nil), data[:klen]...),
		})
		data = data[klen:]
	}
	return entries, nil
}

// Get looks up key.
//   - (val, true, nil)  — key found with value val
//   - (nil, true, nil)  — key found but is a tombstone (deleted)
//   - (nil, false, nil) — key is absent
func (s *SSTable) Get(key []byte) (val []byte, found bool, err error) {
	// Fast path: Bloom filter is certain the key is absent.
	if !s.bloom.MayContain(key) {
		return nil, false, nil
	}

	// Binary search index: find the rightmost block whose firstKey ≤ key.
	bi := s.findBlock(key)
	if bi < 0 {
		return nil, false, nil
	}

	block, err := s.readBlock(bi)
	if err != nil {
		return nil, false, err
	}
	return scanBlock(block, key)
}

// findBlock returns the index of the block that could contain key,
// or -1 if key is smaller than every firstKey.
func (s *SSTable) findBlock(key []byte) int {
	lo, hi, result := 0, len(s.index)-1, -1
	for lo <= hi {
		mid := (lo + hi) / 2
		if bytes.Compare(s.index[mid].firstKey, key) <= 0 {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return result
}

// readBlock reads raw block bytes from disk using ReadAt (concurrent-safe).
func (s *SSTable) readBlock(i int) ([]byte, error) {
	e := s.index[i]
	buf := make([]byte, e.blockLen)
	if _, err := s.f.ReadAt(buf, e.offset); err != nil {
		return nil, fmt.Errorf("lsm: read block %d of %s: %w", i, s.path, err)
	}
	return buf, nil
}

// scanBlock linearly scans a raw data block for key.
func scanBlock(block, key []byte) (val []byte, found bool, err error) {
	for len(block) >= 6 {
		klen := int(binary.LittleEndian.Uint16(block[0:]))
		vlen := binary.LittleEndian.Uint32(block[2:])
		block = block[6:]

		if len(block) < klen {
			return nil, false, errors.New("lsm: corrupt block: key truncated")
		}
		k := block[:klen]
		block = block[klen:]

		if vlen == tombstoneLen {
			if bytes.Equal(k, key) {
				return nil, true, nil // deleted
			}
			continue
		}

		if int(vlen) > len(block) {
			return nil, false, errors.New("lsm: corrupt block: val truncated")
		}
		v := block[:vlen]
		block = block[vlen:]

		if bytes.Equal(k, key) {
			return append([]byte(nil), v...), true, nil
		}
	}
	return nil, false, nil
}

// Close releases the file handle.
func (s *SSTable) Close() error { return s.f.Close() }

// Path returns the file path of the SSTable.
func (s *SSTable) Path() string { return s.path }

// ─── Iterator ─────────────────────────────────────────────────────────────────

// SSTIter iterates over every entry in an SSTable in ascending key order.
// It is not safe for concurrent use.
type SSTIter struct {
	sst      *SSTable
	blockIdx int    // next block to load (-1 = not started)
	block    []byte // remaining bytes in the current block
	key      []byte
	val      []byte // nil = tombstone
	valid    bool
	err      error
}

// newSSTIter creates an iterator positioned before the first entry.
// Call Next() to advance to the first entry.
func newSSTIter(sst *SSTable) *SSTIter {
	return &SSTIter{sst: sst, blockIdx: -1}
}

// Valid reports whether the iterator is positioned at a valid entry.
func (it *SSTIter) Valid() bool { return it.valid }

// Key returns the current key. Caller must not modify the returned slice.
func (it *SSTIter) Key() []byte { return it.key }

// Val returns the current value, or nil if the entry is a tombstone.
func (it *SSTIter) Val() []byte { return it.val }

// IsTombstone reports whether the current entry is a deletion marker.
func (it *SSTIter) IsTombstone() bool { return it.valid && it.val == nil }

// Err returns the first error encountered, if any.
func (it *SSTIter) Err() error { return it.err }

// Next advances to the next entry. Returns true if a valid entry was reached.
func (it *SSTIter) Next() bool {
	for {
		// Parse the next entry from the current block.
		if len(it.block) >= 6 {
			if it.parseEntry() {
				return true
			}
			if it.err != nil {
				return false
			}
		}
		// Load the next block.
		it.blockIdx++
		if it.blockIdx >= len(it.sst.index) {
			it.valid = false
			return false
		}
		block, err := it.sst.readBlock(it.blockIdx)
		if err != nil {
			it.err = err
			it.valid = false
			return false
		}
		it.block = block
	}
}

func (it *SSTIter) parseEntry() bool {
	klen := int(binary.LittleEndian.Uint16(it.block[0:]))
	vlen := binary.LittleEndian.Uint32(it.block[2:])
	it.block = it.block[6:]

	if len(it.block) < klen {
		it.err = errors.New("lsm: corrupt block: key truncated")
		it.valid = false
		return false
	}
	it.key = it.block[:klen]
	it.block = it.block[klen:]

	if vlen == tombstoneLen {
		it.val = nil
		it.valid = true
		return true
	}
	if int(vlen) > len(it.block) {
		it.err = errors.New("lsm: corrupt block: val truncated")
		it.valid = false
		return false
	}
	it.val = it.block[:vlen]
	it.block = it.block[vlen:]
	it.valid = true
	return true
}
