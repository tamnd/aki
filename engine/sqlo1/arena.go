package sqlo1

import (
	"encoding/binary"
	"math/bits"
)

// Chunked byte arena, doc 04 section 3: key and value bytes live here in
// large flat chunks instead of one Go object per key, addressed by 32-bit
// refs from the packed headers. Allocations carry an 8-byte prefix
// (length, then capacity) and are rounded up to power-of-two size classes
// with a per-class freelist, so the steady state of overwrite-heavy
// traffic recycles slots without touching the Go allocator; the alloczero
// lab gates that. In-place update is allowed up to the allocation's
// capacity, which is the "size class allows" rule the doc 04 write path
// names.
//
// A ref packs a chunk index and an offset in 8-byte units into 16 bits
// each: 65536 chunks of 512 KiB address 32 GiB per arena per shard.
// Allocations too big for a standard chunk get a dedicated chunk of their
// exact footprint; freeing one releases the chunk and recycles its index.
// Chunk 0 starts with 8 pad bytes so ref 0 is never a valid allocation
// and doubles as "no ref" in a zero header.

const (
	arenaChunkSize = 512 << 10
	arenaAlign     = 8
	// arenaMinClass is the smallest allocation footprint, prefix included.
	arenaMinClass = 16
	// arenaClasses counts the power-of-two footprints from arenaMinClass
	// through arenaChunkSize.
	arenaClasses = 16
	arenaMaxRefs = 1 << 16
)

// arenaBudget is a hard byte cap shared by the arenas it is wired to
// (doc 04 section 15 gives keys and values one combined share). reserved
// counts chunk bytes actually held from the Go heap; freelist churn does
// not move it, only chunk acquisition and oversize release do. A nil
// budget means uncapped, which only tests use.
type arenaBudget struct {
	limit    int64
	reserved int64
}

func (b *arenaBudget) reserve(n int64) bool {
	if b == nil {
		return true
	}
	if b.reserved+n > b.limit {
		return false
	}
	b.reserved += n
	return true
}

func (b *arenaBudget) unreserve(n int64) {
	if b != nil {
		b.reserved -= n
	}
}

type arena struct {
	chunks [][]byte
	// freeChunks holds chunk indexes released by oversize frees.
	freeChunks []uint32
	cur        uint32 // chunk currently bump-allocated
	curOff     uint32
	// free holds refs recycled per size class.
	free   [arenaClasses][]uint32
	budget *arenaBudget
}

// classFor returns the size-class footprint and index for a payload of n
// bytes, or ok=false when the allocation is oversize.
func classFor(n int) (footprint uint32, idx int, ok bool) {
	need := n + arenaAlign
	if need > arenaChunkSize {
		return 0, 0, false
	}
	f := uint32(arenaMinClass)
	if need > arenaMinClass {
		f = 1 << bits.Len32(uint32(need-1))
	}
	return f, bits.Len32(f) - 5, true
}

func (a *arena) chunkAt(ref uint32) ([]byte, int) {
	return a.chunks[ref>>16], int(ref&0xFFFF) * arenaAlign
}

// alloc copies v into the arena and returns its ref, or 0 when the
// budget refuses the chunk bytes it would take; a failed alloc changes
// nothing, so the caller can surface the refusal as a full table.
func (a *arena) alloc(v []byte) uint32 {
	if len(a.chunks) == 0 {
		// Chunk 0 is always a standard bump chunk with its first slot
		// reserved, so ref 0 is never a live allocation on any path,
		// including an oversize first alloc.
		if !a.budget.reserve(arenaChunkSize) {
			return 0
		}
		a.chunks = append(a.chunks, make([]byte, arenaChunkSize))
		a.cur = 0
		a.curOff = arenaAlign
	}
	f, ci, std := classFor(len(v))
	var ref uint32
	switch {
	case !std:
		ref = a.allocOversize(len(v))
	case len(a.free[ci]) > 0:
		last := len(a.free[ci]) - 1
		ref = a.free[ci][last]
		a.free[ci] = a.free[ci][:last]
	default:
		ref = a.bump(f)
	}
	if ref == 0 {
		return 0
	}
	c, off := a.chunkAt(ref)
	binary.LittleEndian.PutUint32(c[off:], uint32(len(v)))
	copy(c[off+arenaAlign:], v)
	return ref
}

// bump carves a fresh slot of footprint f from the current chunk,
// opening a new one when f does not fit; the tail waste is part of the
// under-10% slack the doc 04 accounting carries. cur only ever points at
// a standard chunk bump itself opened, so oversize chunks are never
// bumped into.
func (a *arena) bump(f uint32) uint32 {
	if a.curOff+f > arenaChunkSize {
		ci, ok := a.newChunk(arenaChunkSize)
		if !ok {
			return 0
		}
		a.cur = ci
		a.curOff = 0
	}
	ref := a.cur<<16 | a.curOff/arenaAlign
	c := a.chunks[a.cur]
	binary.LittleEndian.PutUint32(c[a.curOff+4:], f-arenaAlign)
	a.curOff += f
	return ref
}

func (a *arena) allocOversize(n int) uint32 {
	ci, ok := a.newChunk(n + arenaAlign)
	if !ok {
		return 0
	}
	c := a.chunks[ci]
	binary.LittleEndian.PutUint32(c[4:], uint32(n))
	return ci << 16
}

// newChunk acquires a chunk of size bytes against the budget; a recycled
// oversize slot reserves again because its release unreserved.
func (a *arena) newChunk(size int) (uint32, bool) {
	if !a.budget.reserve(int64(size)) {
		return 0, false
	}
	if n := len(a.freeChunks); n > 0 {
		ci := a.freeChunks[n-1]
		a.freeChunks = a.freeChunks[:n-1]
		a.chunks[ci] = make([]byte, size)
		return ci, true
	}
	if len(a.chunks) >= arenaMaxRefs {
		// The arena budget bounds chunk count long before this; the panic
		// is the honest backstop for a missing cap, not a code path.
		panic("sqlo1: arena chunk space exhausted")
	}
	a.chunks = append(a.chunks, make([]byte, size))
	return uint32(len(a.chunks) - 1), true
}

// data returns the live payload for ref, aliasing the chunk.
func (a *arena) data(ref uint32) []byte {
	c, off := a.chunkAt(ref)
	n := binary.LittleEndian.Uint32(c[off:])
	return c[off+arenaAlign : off+arenaAlign+int(n)]
}

// update overwrites the payload in place when v fits the allocation's
// capacity and reports whether it did; the caller reallocates otherwise.
func (a *arena) update(ref uint32, v []byte) bool {
	c, off := a.chunkAt(ref)
	if uint32(len(v)) > binary.LittleEndian.Uint32(c[off+4:]) {
		return false
	}
	binary.LittleEndian.PutUint32(c[off:], uint32(len(v)))
	copy(c[off+arenaAlign:], v)
	return true
}

// release returns ref's slot to its class freelist, or frees the whole
// chunk for an oversize allocation.
func (a *arena) release(ref uint32) {
	c, off := a.chunkAt(ref)
	capacity := binary.LittleEndian.Uint32(c[off+4:])
	f, ci, std := classFor(int(capacity))
	if !std || f != capacity+arenaAlign {
		a.chunks[ref>>16] = nil
		a.freeChunks = append(a.freeChunks, ref>>16)
		a.budget.unreserve(int64(capacity) + arenaAlign)
		return
	}
	a.free[ci] = append(a.free[ci], ref)
}
