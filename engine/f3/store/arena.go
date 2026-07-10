package store

// The per-shard arena: one backing byte slice divided into fixed segments,
// bump-allocated by the shard's single owner (spec 2064/f3/04 section 4). The
// bump is a plain add, the segment advance is a plain function call, and the
// free list is a plain slice: no atomic cursor, no mutex, no pending-writer
// protocol, because nothing but the owner allocates or frees. Records are
// addressed by their byte offset into the slice, which stays under 48 bits by
// construction; a record never spans a segment, so segment size is floored at
// the largest legal record and a freed segment can hand its pages back to the
// OS without cutting a live record in half.

// defaultSegBytes is the segment size when the caller passes a non-positive
// one: large enough that per-segment overhead and advance frequency vanish
// into noise, small enough that a drained segment is a meaningful reclamation
// unit. Lab constant (LAB-5).
const defaultSegBytes = 8 << 20

// maxRecordBytes is the largest a single record can be: header, expiry slot,
// max-width key, max-width value, each rounded to 8. A segment is never
// smaller, so any valid allocation fits one segment.
const maxRecordBytes = hdrSize + 8 + ((maxKey + 7) &^ 7) + ((maxVal + 7) &^ 7)

// aseg is one segment's descriptor. alloc is the bump cursor, the next free
// offset; it equals base for an empty or freshly freed segment. live counts
// the bytes handed out minus the bytes of records since unlinked, the figure
// the demotion and compaction passes read to pick a drain target.
type aseg struct {
	base  uint64
	alloc uint64
	live  int64
}

type arena struct {
	buf []byte
	// mapped records whether buf is an anonymous mapping outside the Go heap
	// (arena_map_unix.go) rather than the heap fallback; only a mapped buffer
	// may be unmapped when the store goes away.
	mapped   bool
	segSize  uint64
	segStart uint64
	segs     []aseg
	freeSegs []uint64
	// highWater is the count of segments ever brought into use; the range
	// beyond it is untouched headroom the advance draws from after the free
	// list.
	highWater uint64
	cur       uint64
}

// newArena tiles arenaBytes into segments of segBytes. Offset 0 is reserved so
// an index entry of zero stays unambiguous; segments start at offset 8. It
// panics when the arena cannot hold one segment, the caller's sizing error.
func newArena(arenaBytes, segBytes int) arena {
	if segBytes <= 0 {
		segBytes = defaultSegBytes
	}
	segSize := align8(uint64(segBytes))
	if segSize < maxRecordBytes {
		segSize = align8(maxRecordBytes)
	}
	const segStart = uint64(8)
	if uint64(arenaBytes) < segStart+segSize {
		panic("store: arena too small for one segment")
	}
	nSeg := (uint64(arenaBytes) - segStart) / segSize
	segs := make([]aseg, nSeg)
	for i := range segs {
		segs[i].base = segStart + uint64(i)*segSize
		segs[i].alloc = segs[i].base
	}
	buf, mapped := arenaMap(arenaBytes)
	return arena{
		buf:       buf,
		mapped:    mapped,
		segSize:   segSize,
		segStart:  segStart,
		segs:      segs,
		highWater: 1,
		cur:       0,
	}
}

// allocRecord bump-allocates an 8-byte-aligned block from the current segment.
// The common case is one add on a plain cursor. On overshoot the segment is
// full: advance to a new current segment and retry, abandoning the tail
// exactly as any bump allocator abandons its remainder, capped at one
// segment's worth. ok is false only when no segment has room and none can be
// brought in, which the write path reports as ErrFull.
func (a *arena) allocRecord(nbytes uint64) (uint64, bool) {
	n := align8(nbytes)
	for {
		seg := &a.segs[a.cur]
		if seg.alloc+n <= seg.base+a.segSize {
			off := seg.alloc
			seg.alloc += n
			seg.live += int64(n)
			return off, true
		}
		if !a.advance() {
			return 0, false
		}
	}
}

// advance moves the current-segment pointer to a fresh segment: the free list
// first, then the never-used headroom. It reports false when neither exists,
// the arena's genuine full state.
func (a *arena) advance() bool {
	switch {
	case len(a.freeSegs) > 0:
		a.cur = a.freeSegs[len(a.freeSegs)-1]
		a.freeSegs = a.freeSegs[:len(a.freeSegs)-1]
	case a.highWater < uint64(len(a.segs)):
		a.cur = a.highWater
		a.highWater++
	default:
		return false
	}
	return true
}

// segOf returns the index of the segment owning offset off, false for the
// reserved prefix below segStart. Pure arithmetic over the fixed tiling.
func (a *arena) segOf(off uint64) (uint64, bool) {
	if off < a.segStart {
		return 0, false
	}
	si := (off - a.segStart) / a.segSize
	if si >= uint64(len(a.segs)) {
		return 0, false
	}
	return si, true
}

// unlink charges n bytes back to the segment owning off, called when a record
// leaves the index (delete, or an overwrite that repointed the entry). When a
// segment's live counter reaches zero its bytes are all dead and it is a free
// candidate.
func (a *arena) unlink(off, n uint64) {
	if si, ok := a.segOf(off); ok {
		a.segs[si].live -= int64(n)
	}
}

// freeSegment returns segment si to the free list, rewinding its cursor and
// handing its pages back to the OS. The caller must have unlinked every record
// the segment held from the index; the bytes are not scrubbed, because
// initRecord fully rewrites a reused offset's header before any index entry
// exposes it. Freeing the current segment is a caller error the next
// allocation would corrupt, so it is refused.
func (a *arena) freeSegment(si uint64) {
	if si == a.cur {
		return
	}
	seg := &a.segs[si]
	seg.alloc = seg.base
	seg.live = 0
	a.releasePages(seg.base, a.segSize)
	a.freeSegs = append(a.freeSegs, si)
}

// reset rewinds every segment to empty, the arena half of a flush. The caller
// owns the index and must have dropped it too. The pages behind every touched
// segment go back to the OS, same as freeSegment: a flush that only rewound
// cursors would leave the whole dataset resident, and the point of FLUSHALL
// is that the footprint actually drops. Off Linux releasePages is a no-op and
// the pages linger, which is the same story as freed segments there.
func (a *arena) reset() {
	touched := a.segStart + a.highWater*a.segSize
	if touched > uint64(len(a.buf)) {
		touched = uint64(len(a.buf))
	}
	for i := range a.segs {
		a.segs[i].alloc = a.segs[i].base
		a.segs[i].live = 0
	}
	a.freeSegs = a.freeSegs[:0]
	a.highWater = 1
	a.cur = 0
	a.releasePages(0, touched)
}

// fillOf is segment si's handed-out bytes: the cursor advance, clamped to the
// segment size. Zero for an untouched or freed segment.
func (a *arena) fillOf(si uint64) uint64 {
	seg := &a.segs[si]
	if seg.alloc <= seg.base {
		return 0
	}
	return min(seg.alloc-seg.base, a.segSize)
}

// deadOf is segment si's dead bytes: handed out minus still charged. The
// figure is derived, not counted, because every alloc adds to live and every
// unlink subtracts, so fill minus live is exactly the bytes whose records
// left the index. The compactor reads it to pick drain targets.
func (a *arena) deadOf(si uint64) uint64 {
	f := a.fillOf(si)
	l := a.segs[si].live
	if l < 0 || uint64(l) >= f {
		return 0
	}
	return f - uint64(l)
}

// freeSegCount is how many whole segments the allocator can still bring in:
// the free list plus the never-used headroom. The tightness check reads it,
// so it is O(1) on purpose.
func (a *arena) freeSegCount() uint64 {
	return uint64(len(a.freeSegs)) + uint64(len(a.segs)) - a.highWater
}

// used reports the bytes handed out: each touched segment's cursor advance,
// clamped to segSize so a full segment's abandoned overshoot is not counted
// twice. Introspection only, so the walk over segments is fine.
func (a *arena) used() uint64 {
	var u uint64
	for i := range a.segs {
		seg := &a.segs[i]
		if seg.alloc <= seg.base {
			continue
		}
		u += min(seg.alloc-seg.base, a.segSize)
	}
	return u
}
