package sqlo1b

import (
	"encoding/binary"
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// Cold index chunk (doc 03 section 8.2): a 512-byte linear-hashing
// bucket, eight to a group in index extents (the doc's prose said
// four, but 4096 over 512 is arithmetic, and the doc is corrected).
//
//	u16  count       live entries, 0..42
//	u8   cflags      bit0 overflow-continues
//	u8   window_base first hash bit the entry split windows cover
//	u32  chunk_no_lo low 32 bits of the chunk number
//	entry[42]:
//	  u16 fp         key hash bits 63..48
//	  u16 meta       bits 0..3 type tag, 4..5 expiry class, 6 root
//	                 flag, 7..15 split window
//	  u64 vptr       packed record position
//
// The split window solves the redistribution problem: a split of
// bucket S at level L sends each entry left or right on hash bit L,
// but the entry stores only the fingerprint (bits 63..48) and the
// key sits in a record on disk. Without stored bits every split
// reads its whole bucket, about one record read per insert during
// growth. So meta bits 7..15 carry hash bits window_base..
// window_base+8, written from the full hash whenever the inserter
// holds the key, and a split consumes the window bit at position
// L-window_base. Only when a chunk's window no longer covers L
// (once per nine levels a chunk survives, because window_base
// rebases to the current level on refresh) does the split read the
// bucket's records, batched, cutting split IO about 9x to roughly
// 0.11 record reads per insert during growth.
//
// The chunk carries no checksum of its own: the directory's full
// pointer covers it, and chunk_no_lo catches directory slips. When a
// bucket chains (doc 03 section 8.5) entry slot 41 is repurposed as
// the chain pointer, so a chained chunk holds at most 41 live
// entries. A full pointer is 16 bytes and the slot is 12, so the
// chain pointer stores the packed position plus the low 32 bits of
// the overflow chunk's xxhash64; the overflow chunk backs the
// truncated check with its own strict parse and chunk_no_lo, which
// equals the base chunk's because the chain is part of the bucket.
//
// Verdict constants (results/sqlo1/b2-chunkindex.md): geometry
// unchanged from the doc, probe order is base fingerprints first
// with the chain read only on a base miss, and the split policy the
// linear-hashing slice bakes on top of this codec is lf85.
const (
	ChunkSize     = 512
	ChunkCap      = 42
	ChunkChainCap = ChunkCap - 1
	chunkHdrSize  = 8
	chunkEntSize  = 12

	chunksPerGroup = GroupSize / ChunkSize

	// CFlagChained marks a bucket whose slot 41 points at an
	// overflow chunk.
	CFlagChained = 1 << 0

	cflagsKnown = CFlagChained
)

// Key placement (doc 03 section 8.3): the fingerprint takes hash
// bits 63..48 and linear hashing places bits 47..0, so the two never
// overlap and a fingerprint match says nothing a placement collision
// already said.
const placementMask = 1<<48 - 1

// KeyHash is the cold index's key hash: xxhash64 over the key bytes
// or the 16-byte subkey.
func KeyHash(key []byte) uint64 { return xxhash.Sum64(key) }

// Fingerprint extracts the 16-bit chunk fingerprint, hash bits 63..48.
func Fingerprint(hash uint64) uint16 { return uint16(hash >> 48) }

// PlacementBits extracts hash bits 47..0, the linear-hashing
// placement input.
func PlacementBits(hash uint64) uint64 { return hash & placementMask }

// Entry meta layout: bits 0..3 type tag, 4..5 expiry class, 6 root
// flag, 7..15 split window. The type tag namespace is bound by the
// store slice; the codec only sizes the field.
const (
	ExpClassNone = 0
	ExpClassNear = 1
	ExpClassMid  = 2
	ExpClassFar  = 3

	// WindowBits is the split window width: hash bits window_base
	// through window_base+8 in meta bits 7..15.
	WindowBits = 9

	windowMask = 1<<WindowBits - 1

	// maxWindowBase bounds window_base: placement is 48 bits, so no
	// split ever consumes a bit past 47 and no refresh rebases past
	// it.
	maxWindowBase = 47
)

// MakeEntryMeta packs an entry's meta field.
func MakeEntryMeta(typeTag, expiryClass uint8, root bool) (uint16, error) {
	if typeTag > 15 {
		return 0, fmt.Errorf("sqlo1b: type tag %d exceeds 4 bits", typeTag)
	}
	if expiryClass > ExpClassFar {
		return 0, fmt.Errorf("sqlo1b: expiry class %d exceeds 2 bits", expiryClass)
	}
	m := uint16(typeTag) | uint16(expiryClass)<<4
	if root {
		m |= 1 << 6
	}
	return m, nil
}

// MetaTypeTag unpacks the type tag from an entry meta field.
func MetaTypeTag(meta uint16) uint8 { return uint8(meta & 0xF) }

// MetaExpiryClass unpacks the 2-bit expiry class.
func MetaExpiryClass(meta uint16) uint8 { return uint8(meta>>4) & 3 }

// MetaRoot reports the root flag.
func MetaRoot(meta uint16) bool { return meta&(1<<6) != 0 }

// SplitWindow extracts the 9-bit split window for a chunk base from
// a full key hash: hash bits base..base+8. Only inserters holding
// the key call this; a split that no longer holds the key consumes
// stored windows instead.
func SplitWindow(hash uint64, base uint8) (uint16, error) {
	if base > maxWindowBase {
		return 0, fmt.Errorf("sqlo1b: window base %d past placement bit %d", base, maxWindowBase)
	}
	return uint16(hash>>base) & windowMask, nil
}

// MetaSplitWindow unpacks the stored split window.
func MetaSplitWindow(meta uint16) uint16 { return meta >> 7 }

// MetaWithWindow packs a split window into a meta field.
func MetaWithWindow(meta, window uint16) (uint16, error) {
	if window > windowMask {
		return 0, fmt.Errorf("sqlo1b: split window %#x exceeds %d bits", window, WindowBits)
	}
	return meta&0x7F | window<<7, nil
}

// WindowBit reads hash bit level out of an entry's stored window
// under the chunk's base. ok is false when the window does not
// cover the level and the split must refresh from records.
func WindowBit(meta uint16, base, level uint8) (bit uint8, ok bool) {
	if level < base || level-base >= WindowBits {
		return 0, false
	}
	return uint8(MetaSplitWindow(meta)>>(level-base)) & 1, true
}

// ChunkCheck32 is the truncated check word a chain pointer stores
// for its overflow chunk: the low 32 bits of xxhash64 over the
// chunk's 512 bytes.
func ChunkCheck32(b []byte) uint32 { return uint32(xxhash.Sum64(b)) }

// Chunk is a mutable view over exactly ChunkSize bytes. Updates are
// copy-on-write at the store layer: the owner mutates a private copy
// and rewrites the whole chunk (doc 03 section 8.5).
type Chunk struct {
	b []byte
}

// NewChunk allocates an empty chunk for the given chunk number.
// windowBase is 0 for initial chunks, inherited from the parent on
// an ordinary split, and the current level after a refresh.
func NewChunk(chunkNo uint64, windowBase uint8) (*Chunk, error) {
	if windowBase > maxWindowBase {
		return nil, fmt.Errorf("sqlo1b: window base %d past placement bit %d", windowBase, maxWindowBase)
	}
	c := &Chunk{b: make([]byte, ChunkSize)}
	c.b[3] = windowBase
	binary.LittleEndian.PutUint32(c.b[4:8], uint32(chunkNo))
	return c, nil
}

// ParseChunk validates a chunk image against the chunk number the
// caller resolved it under. The parse is strict: unknown cflags, a
// window base past the placement bits, or any nonzero byte outside
// the live region is a format error, so every legal chunk has
// exactly one encoding.
func ParseChunk(b []byte, chunkNo uint64) (*Chunk, error) {
	if len(b) != ChunkSize {
		return nil, fmt.Errorf("sqlo1b: chunk image is %d bytes, want %d", len(b), ChunkSize)
	}
	count := binary.LittleEndian.Uint16(b[0:2])
	cflags := b[2]
	if cflags&^cflagsKnown != 0 {
		return nil, fmt.Errorf("sqlo1b: unknown cflags bits %#x", cflags&^cflagsKnown)
	}
	if b[3] > maxWindowBase {
		return nil, fmt.Errorf("sqlo1b: window base %d past placement bit %d", b[3], maxWindowBase)
	}
	if got, want := binary.LittleEndian.Uint32(b[4:8]), uint32(chunkNo); got != want {
		return nil, fmt.Errorf("sqlo1b: chunk_no_lo %#x, resolved as chunk %#x", got, want)
	}
	chained := cflags&CFlagChained != 0
	limit := ChunkCap
	if chained {
		limit = ChunkChainCap
	}
	if int(count) > limit {
		return nil, fmt.Errorf("sqlo1b: chunk count %d over capacity %d", count, limit)
	}
	from := chunkHdrSize + int(count)*chunkEntSize
	upto := ChunkSize
	if chained {
		upto = chunkHdrSize + ChunkChainCap*chunkEntSize
		pos := Pos(binary.LittleEndian.Uint64(b[ChunkSize-8:]))
		if pos.Slot() >= chunksPerGroup {
			return nil, fmt.Errorf("sqlo1b: chain pointer slot %d, chunks sit %d to a group", pos.Slot(), chunksPerGroup)
		}
	}
	for i := from; i < upto; i++ {
		if b[i] != 0 {
			return nil, fmt.Errorf("sqlo1b: nonzero byte %#x past the live region at offset %d", b[i], i)
		}
	}
	return &Chunk{b: b}, nil
}

// Bytes returns the chunk's backing image.
func (c *Chunk) Bytes() []byte { return c.b }

// Count reports the number of live entries.
func (c *Chunk) Count() int { return int(binary.LittleEndian.Uint16(c.b[0:2])) }

// Chained reports whether slot 41 holds a chain pointer.
func (c *Chunk) Chained() bool { return c.b[2]&CFlagChained != 0 }

// ChunkNoLo returns the stored low 32 bits of the chunk number.
func (c *Chunk) ChunkNoLo() uint32 { return binary.LittleEndian.Uint32(c.b[4:8]) }

// WindowBase returns the first hash bit the chunk's entry split
// windows cover.
func (c *Chunk) WindowBase() uint8 { return c.b[3] }

func (c *Chunk) capacity() int {
	if c.Chained() {
		return ChunkChainCap
	}
	return ChunkCap
}

func (c *Chunk) entryOff(i int) int { return chunkHdrSize + i*chunkEntSize }

// EntryAt returns live entry i; i must be below Count.
func (c *Chunk) EntryAt(i int) (fp, meta uint16, vptr uint64) {
	if i >= c.Count() {
		panic(fmt.Sprintf("sqlo1b: entry %d of %d", i, c.Count()))
	}
	off := c.entryOff(i)
	return binary.LittleEndian.Uint16(c.b[off : off+2]),
		binary.LittleEndian.Uint16(c.b[off+2 : off+4]),
		binary.LittleEndian.Uint64(c.b[off+4 : off+12])
}

// InsertEntry appends an entry. The caller resolves duplicates
// before inserting and packs the split window into meta; the codec
// only enforces capacity.
func (c *Chunk) InsertEntry(fp, meta uint16, vptr uint64) error {
	n := c.Count()
	if n >= c.capacity() {
		return fmt.Errorf("sqlo1b: chunk full at %d entries (chained %v)", n, c.Chained())
	}
	off := c.entryOff(n)
	binary.LittleEndian.PutUint16(c.b[off:off+2], fp)
	binary.LittleEndian.PutUint16(c.b[off+2:off+4], meta)
	binary.LittleEndian.PutUint64(c.b[off+4:off+12], vptr)
	binary.LittleEndian.PutUint16(c.b[0:2], uint16(n+1))
	return nil
}

// RemoveEntry deletes live entry i by moving the last entry into its
// slot and zeroing the vacated bytes, preserving the strict-parse
// invariant that everything past the live region is zero.
func (c *Chunk) RemoveEntry(i int) error {
	n := c.Count()
	if i >= n {
		return fmt.Errorf("sqlo1b: remove entry %d of %d", i, n)
	}
	last := c.entryOff(n - 1)
	if i != n-1 {
		copy(c.b[c.entryOff(i):], c.b[last:last+chunkEntSize])
	}
	clear(c.b[last : last+chunkEntSize])
	binary.LittleEndian.PutUint16(c.b[0:2], uint16(n-1))
	return nil
}

// SetEntry updates live entry i's meta and vptr in place. The
// fingerprint stays: an entry update is the same key's record moving
// or changing class, never a different key.
func (c *Chunk) SetEntry(i int, meta uint16, vptr uint64) error {
	if i >= c.Count() {
		return fmt.Errorf("sqlo1b: set entry %d of %d", i, c.Count())
	}
	off := c.entryOff(i)
	binary.LittleEndian.PutUint16(c.b[off+2:off+4], meta)
	binary.LittleEndian.PutUint64(c.b[off+4:off+12], vptr)
	return nil
}

// Probe scans the live entries for a fingerprint and yields each
// candidate until f returns false. Several entries may share a
// fingerprint; the caller resolves false hits by reading the record
// and comparing the full key (doc 03 section 8.3).
func (c *Chunk) Probe(fp uint16, f func(i int, meta uint16, vptr uint64) bool) {
	n := c.Count()
	for i := range n {
		off := c.entryOff(i)
		if binary.LittleEndian.Uint16(c.b[off:off+2]) != fp {
			continue
		}
		if !f(i,
			binary.LittleEndian.Uint16(c.b[off+2:off+4]),
			binary.LittleEndian.Uint64(c.b[off+4:off+12])) {
			return
		}
	}
}

// SetChain marks the bucket chained and stores the overflow chunk's
// position and truncated check word in slot 41. The slot must be
// free: at most 41 live entries.
func (c *Chunk) SetChain(pos Pos, check uint32) error {
	if pos.Slot() >= chunksPerGroup {
		return fmt.Errorf("sqlo1b: chain pointer slot %d, chunks sit %d to a group", pos.Slot(), chunksPerGroup)
	}
	if n := c.Count(); n > ChunkChainCap {
		return fmt.Errorf("sqlo1b: %d entries leave no slot for the chain pointer", n)
	}
	off := c.entryOff(ChunkChainCap)
	binary.LittleEndian.PutUint32(c.b[off:off+4], check)
	binary.LittleEndian.PutUint64(c.b[off+4:off+12], uint64(pos))
	c.b[2] |= CFlagChained
	return nil
}

// ClearChain removes the chain pointer, for when a split drains the
// overflow back into the bucket.
func (c *Chunk) ClearChain() {
	off := c.entryOff(ChunkChainCap)
	clear(c.b[off : off+chunkEntSize])
	c.b[2] &^= CFlagChained
}

// ChainPtr returns the overflow chunk's position and truncated check
// word.
func (c *Chunk) ChainPtr() (Pos, uint32, error) {
	if !c.Chained() {
		return 0, 0, fmt.Errorf("sqlo1b: chunk %#x is not chained", c.ChunkNoLo())
	}
	off := c.entryOff(ChunkChainCap)
	return Pos(binary.LittleEndian.Uint64(c.b[off+4 : off+12])),
		binary.LittleEndian.Uint32(c.b[off : off+4]), nil
}
