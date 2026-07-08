package f1raw

import "sync/atomic"

// This file adds the reclaimable segmented arena, milestone M0 of the collection
// cold-record tiering plan (spec 2064/f1_rewrite_ltm/21). It is inert unless the store
// is built with NewSegmented: New leaves s.segmented false and every allocation still
// runs the original grow-only bump path in f1raw.go, so the resident point path pays
// nothing until the segmented path is proven.
//
// The design, from doc 21:
//
//   - The one arena []byte is divided into fixed segments of segSize bytes (D8). A
//     record never spans a segment (D9), so segSize is floored at the largest possible
//     record. Segments are subdivisions of the single backing slice, not separate
//     allocations, so every unsafe.Add(s.base, off) address in the store stays valid
//     unchanged.
//
//   - Overflow buckets live in their own region at the front of the arena that never
//     reclaims (D10). A reclaimed segment must not hold overflow buckets, since the
//     index still links them; keeping them out of segments keeps a freed segment free
//     of anything the index points at. The region is one bump cursor, like the old
//     arena but bounded.
//
//   - Each segment carries a descriptor with its base, a bump cursor, and a live-byte
//     counter (D11). A free list of segment indices plus a current-segment pointer give
//     the head advance and reuse (D12). Allocation within the current segment is one
//     atomic add; the rare segment advance and the free-list edits take a mutex, since
//     an advance happens once per segSize bytes, far off the per-record hot path.
//
// M0 stops here: it makes the arena reclaimable and proves a freed segment can be
// reused without corrupting live records in other segments. The migrator that actually
// drives frees (D14 to D17), the tier-tagged index (D1), and the cold region (D5) land
// in later milestones. Until the migrator exists, freeSegment is the only way a segment
// is returned, which the M0 test drives directly after clearing a segment's records.

// defaultSegBytes is the segment size when NewSegmented is given a non-positive
// segBytes. Doc 21 D8 sizes a segment near 8 MiB: large enough that the per-segment
// descriptor and the once-per-segment advance are negligible against the records it
// holds, small enough that draining one for reuse copies a bounded amount.
const defaultSegBytes = 8 << 20

// maxRecordBytes is the largest a single record can be: the header plus a max-width key
// and a max-width value, each rounded to 8. A segment is never smaller than this so no
// record spans a segment (D9), and allocRecord can assume any valid record fits one
// segment.
const maxRecordBytes = hdrSize + ((maxKey + 7) &^ 7) + ((maxVal + 7) &^ 7)

// segment is one arena segment's descriptor. base is the segment's 8-aligned start
// offset in the arena, fixed for the store's life. alloc is the bump cursor, the next
// free offset within the segment; it equals base for an empty or freshly freed segment
// and advances past base+segSize once the segment is full (the overshoot is abandoned,
// exactly as the old single-arena bump abandons its tail). live counts the record bytes
// handed out of this segment, the figure the later migrator reads to pick a drain
// target; M0 only increments it on allocation and zeroes it on free, since there is no
// migrator yet to decrement it. retire is the epoch the segment was retired at (M2, doc 21
// section 7), valid only while the segment sits on retSegs awaiting reader quiescence; it is
// touched solely under segMu, so a plain field suffices. stuck records the live-byte total a
// drain left behind when it could not empty the segment (a non-migratable record, such as a
// collection header row, pinned it); pickDrainTarget skips a segment whose live still equals its
// stuck mark so the migrator does not re-pick the same unretireable segment forever and starves
// the field-bearing segments that can actually drain. drainSegment writes it lock-free while
// pickDrainTarget and the reclaimer read and clear it under segMu, so it is atomic; a later delete
// lowers live below stuck and re-enables the segment, and a free zeroes both. pending counts the
// writers that have reserved a record in this segment but not yet finished laying its bytes down
// (doc 23 D23-6): allocRecord claims a slot before it bumps the cursor and the writer releases it in
// commitRecord once initRecord has written the record, so a drain of a just-sealed segment waits for
// pending to reach zero before it walks the records, closing the race where the migrator reads a
// header a concurrent initRecord is still writing. draining marks a segment a migrator worker has
// claimed for the current drain so a parallel worker pool never picks the same segment twice:
// pickDrainTarget sets it under segMu on the segment it hands out and skips any segment already
// draining, and the drain clears it when it finishes, so N workers each drain a distinct segment
// concurrently and multiply the cold-sink queue depth the NVMe rewards. The padding keeps adjacent
// descriptors off one cache line so two segments filling or draining concurrently do not false-share.
type segment struct {
	base     uint64
	alloc    atomic.Uint64
	live     atomic.Int64
	retire   uint64
	stuck    atomic.Int64
	pending  atomic.Int64
	draining atomic.Bool
	_        [64 - 52]byte
}

// NewSegmented builds a store whose arena is divided into reclaimable segments of
// segBytes each, with a separate never-reclaimed overflow-bucket region of ovBytes at
// the front. A non-positive segBytes uses defaultSegBytes; segBytes is rounded up to 8
// and floored at maxRecordBytes so no record spans a segment. A non-positive ovBytes
// reserves an eighth of the arena for overflow buckets. It panics if the arena cannot
// hold the overflow region plus at least one segment, the caller's sizing error.
//
// Everything else is New: the index, the cold-log fields, and the collection sidecars
// are identical, and a store built here serves reads and writes through the same paths.
// Only the allocator changes, dispatched on s.segmented.
func NewSegmented(indexBuckets, arenaBytes, segBytes, ovBytes int) *Store {
	s := New(indexBuckets, arenaBytes)
	s.EnableSegments(segBytes, ovBytes)
	return s
}

// EnableSegments switches an existing store's arena to the reclaimable segmented layout,
// the same setup NewSegmented performs. It must be called on an empty store before it
// serves traffic, since it repartitions the arena the allocator draws from; the server
// calls it at startup on the store it built, so the segmented arena composes with a cold
// value log (NewWithCold) without a separate constructor. Calling it twice, or on a store
// that has already allocated records, is a caller error. The segBytes and ovBytes rules
// match NewSegmented.
func (s *Store) EnableSegments(segBytes, ovBytes int) {
	arenaBytes := int(s.cap)
	if segBytes <= 0 {
		segBytes = defaultSegBytes
	}
	segSize := align8(uint64(segBytes))
	if segSize < maxRecordBytes {
		segSize = align8(maxRecordBytes)
	}
	if ovBytes <= 0 {
		ovBytes = arenaBytes / 8
	}
	ovB := align8(uint64(ovBytes))

	// The overflow region starts at offset 8, preserving the reserved offset 0 so an
	// empty index entry (addr 0) stays unambiguous. Segments tile the arena after it.
	const ovBase = uint64(8)
	ovEnd := ovBase + ovB
	segStart := align8(ovEnd)
	if segStart+segSize > s.cap {
		panic("f1raw: arena too small for overflow region plus one segment")
	}
	nSeg := (s.cap - segStart) / segSize
	segs := make([]segment, nSeg)
	for i := range segs {
		segs[i].base = segStart + uint64(i)*segSize
		segs[i].alloc.Store(segs[i].base)
	}

	s.segmented = true
	s.segSize = segSize
	s.segStart = segStart
	s.segs = segs
	s.ovBase = ovBase
	s.ovEnd = ovEnd
	s.ovTail.Store(ovBase)
	s.highWater = 1 // segment 0 is the first current segment
	s.curSeg.Store(0)
	// Every segment but the current one is immediately available (free list empty, all headroom
	// unused), so seed the lock-free hint with that count. It tracks freeSegs and highWater from here.
	s.availSegs.Store(int64(nSeg) - 1)

	// A segmented store can free and reuse a segment, so it needs the reader epoch framework
	// (epoch.go M2) to gate a free on reader quiescence. A non-segmented store never reaches
	// this and never pays for the slots.
	s.initEpoch()
}

// allocRec is the record allocator the write paths call. It routes to the segmented
// allocator when the store is segmented, and to the original single-arena bump
// otherwise, so a non-segmented store's write path is byte-for-byte unchanged.
func (s *Store) allocRec(nbytes uint64) (uint64, bool) {
	if s.segmented {
		return s.allocRecord(nbytes)
	}
	return s.alloc(nbytes)
}

// allocBkt is the overflow-bucket allocator the index growth path calls. In a segmented
// store it draws from the never-reclaimed overflow region; otherwise it bump-allocates
// from the one arena exactly as before.
func (s *Store) allocBkt() (uint64, bool) {
	if s.segmented {
		return s.allocBucket()
	}
	return s.alloc(bucketSize)
}

// allocRecord bump-allocates an 8-byte-aligned record block from the current segment.
// The common case is one atomic add on the current segment's cursor. When that add
// overshoots the segment, the segment is full: advance to a new current segment (from
// the free list or the next unused segment) and retry. The overshoot is abandoned, the
// same bounded waste the old single-arena bump left at the arena tail, now capped at one
// segment. It returns ok=false only when no segment has room and none can be brought in,
// which the write path reports as ErrFull.
func (s *Store) allocRecord(nbytes uint64) (uint64, bool) {
	n := align8(nbytes)
	for {
		si := s.curSeg.Load()
		seg := &s.segs[si]
		// Claim an in-flight-writer slot on this segment before bumping its cursor, so a drain of a
		// just-sealed segment can wait for every writer that reserved a record here to finish laying
		// its bytes down before it walks them (doc 23 D23-6). The claim is taken ahead of the cursor
		// bump so a drain that observes pending == 0 is guaranteed no writer is between reserving a
		// slot and committing its bytes: a writer that has made the slot visible (bumped alloc) has
		// already raised pending. The caller releases the slot in commitRecord once initRecord has
		// written the record; an overshoot that lands in no segment here releases it below.
		seg.pending.Add(1)
		end := seg.alloc.Add(n)
		if end <= seg.base+s.segSize {
			seg.live.Add(int64(n))
			return end - n, true // pending held until the caller commitRecords the written bytes
		}
		seg.pending.Add(-1) // overshoot: this writer wrote no bytes into this segment
		if s.advanceSeg(si) {
			continue
		}
		// No segment has room and none can be brought in. If the migrator is engaged it frees
		// segments by draining full ones cold, so wait on it before reporting the arena full
		// (D12 write-path backpressure). waitForSegment signals the migrator and polls for a
		// freed segment up to a bounded budget; on success the loop retries the allocation, and
		// only a genuine out-of-space returns false. A store with no migrator returns at once,
		// so the non-segmented and no-migrator paths keep reporting ErrFull unchanged.
		if !s.waitForSegment() {
			return 0, false
		}
	}
}

// advanceSeg moves the current-segment pointer off the full segment observed by the
// caller, returning true when a segment is available to fill. It takes segMu, so the
// concurrent allocators that all overshot the same segment serialize here and only one
// actually advances; the rest see curSeg already moved and return. A new current segment
// comes from the free list first, then from the next never-used segment; when neither
// exists the arena is full and it returns false. A popped free segment already has its
// cursor reset to base and its live counter zeroed by freeSegment, and a never-used
// segment was initialized so at NewSegmented, so the new current segment is always ready
// to allocate from.
func (s *Store) advanceSeg(observed uint64) bool {
	s.segMu.Lock()
	defer s.segMu.Unlock()
	if s.curSeg.Load() != observed {
		return true // another allocator advanced already
	}
	var ni uint64
	switch {
	case len(s.freeSegs) > 0:
		ni = s.freeSegs[len(s.freeSegs)-1]
		s.freeSegs = s.freeSegs[:len(s.freeSegs)-1]
		s.availSegs.Add(-1) // drew one claimable segment out of the free list
	case s.highWater < uint64(len(s.segs)):
		ni = s.highWater
		s.highWater++
		s.availSegs.Add(-1) // consumed one segment of never-used headroom
	default:
		// No free segment and no unused one. A migrator may have retired segments that are now
		// safe to reuse; reclaim inline before giving up so a fill that outpaces the migrator's
		// own reclamation pass still recovers freed space rather than reporting a full arena
		// while retired segments wait.
		s.reclaimLocked() // credits availSegs for each segment it moves onto the free list
		if len(s.freeSegs) == 0 {
			return false
		}
		ni = s.freeSegs[len(s.freeSegs)-1]
		s.freeSegs = s.freeSegs[:len(s.freeSegs)-1]
		s.availSegs.Add(-1) // drew the just-reclaimed segment out of the free list
	}
	s.curSeg.Store(ni)
	// A segment just filled and the current pointer moved to a new one: signal the migrator so it
	// checks the fill level and drains cold if it crossed the high-water mark. This is the once-per-
	// segment wake (D14), off the per-record allocation path, and it is a no-op unless the migrator
	// is engaged. It runs while segMu is held, but signalMigrator only does a non-blocking channel
	// send, so it never blocks the advance.
	s.signalMigrator()
	return true
}

// allocBucket bump-allocates one 64-byte overflow bucket from the never-reclaimed
// overflow region. It is one atomic add bounded by the region end; ok is false when the
// region is exhausted, which the caller treats the same as a full arena. Overflow
// buckets stay here rather than in a segment because the index links them for the store's
// life, so a reclaimed segment must never contain one (D10).
func (s *Store) allocBucket() (uint64, bool) {
	end := s.ovTail.Add(bucketSize)
	if end > s.ovEnd {
		return 0, false
	}
	return end - bucketSize, true
}

// recBytesAt returns the resident arena bytes the segment allocator charged to a segment's
// live counter for the record at off: the header, the 8-aligned key, and the reserved value
// capacity (vcap words times 8). That equals align8(recSize(klen, vlen)) at allocation, because
// recSize is already 8-aligned and vcap is align8(vlen)/8, so charging this figure back when the
// record leaves the index cancels the allocation charge exactly and live returns to zero once the
// segment's last record is gone. It reads only immutable header fields, so it is safe on a record
// the index no longer points at, whose bytes are still valid until its segment is freed as a unit.
func (s *Store) recBytesAt(off uint64) uint64 {
	return hdrSize + align8(s.klen(off)) + s.vcapBytes(off)
}

// segOf returns the index of the segment that owns resident arena offset off, with ok false when
// off is not a segment record: a non-segmented store, a cold address (tier bit set, not an arena
// offset at all), or an offset in the never-reclaimed overflow region below segStart. It is pure
// arithmetic over the fixed segment tiling, a subtract, a divide, and one bound check, no lock, so
// the unlink path pays almost nothing to find the segment whose live counter it must adjust.
func (s *Store) segOf(off uint64) (uint64, bool) {
	if !s.segmented || off&tierBit != 0 || off < s.segStart {
		return 0, false
	}
	si := (off - s.segStart) / s.segSize
	if si >= uint64(len(s.segs)) {
		return 0, false
	}
	return si, true
}

// commitRecord releases the in-flight-writer slot allocRecord claimed on the segment that owns off,
// called once the record's bytes are fully written (initRecord done) so the migrator may safely walk
// them (doc 23 D23-6). It pairs one-to-one with the pending increment allocRecord took for the
// reservation that produced off: segOf recovers the same segment from off, so the decrement lands on
// the segment the increment raised, keeping the counter balanced without threading the segment index
// through the write path. A non-segmented store, a cold address, or an overflow-region offset owns no
// per-segment counter, so it is a no-op there, matching allocRec's own dispatch and the alloc path
// that never raised pending.
func (s *Store) commitRecord(off uint64) {
	if si, ok := s.segOf(off); ok {
		s.segs[si].pending.Add(-1)
	}
}

// unlinkResident charges a just-unlinked resident record's bytes back to its owning segment's
// live counter, the decrement M0 and M1 deferred: until now live only ever rose, on allocation,
// so a segment never reported itself drained. It is called at every site that removes a resident
// record from the index, right after the unlink commits, mirroring markSepDead: a string or
// element delete, an overwrite's entry swap to a fresh record, a list or collection element take,
// and the migrator's cold flip. When the last record a segment handed out has been unlinked its
// live counter reaches zero, which is the signal the migrator's drain-completion check reads to
// retire the segment and the emptiest-segment selection (D15) reads to pick its next drain target.
// A cold entry, an overflow-region offset, or a non-segmented store is a no-op, since there is no
// per-segment live counter to adjust. Reading the header here is safe because the record's bytes
// stay valid until its segment is freed as a unit, and the segment cannot be freed while this
// record's own charge still stands in live, which it does until this very decrement lands.
func (s *Store) unlinkResident(off uint64) {
	si, ok := s.segOf(off)
	if !ok {
		return
	}
	s.segs[si].live.Add(-int64(s.recBytesAt(off)))
}

// freeSegment returns segment si to the free list, resetting its cursor to base and its
// live counter to zero so a later advance can reuse it from scratch. It is the only path
// that frees a segment in M0, and the caller must have already unlinked every record the
// segment holds from the index, so no live record's bytes are reclaimed. The later
// migrator (D14 to D17) will drive this after draining a segment's live records forward;
// until then the M0 test calls it directly. The bytes are not scrubbed: a reused offset's
// header is fully rewritten by initRecord before any index entry exposes it, the same
// contract Reset relies on. segMu serializes this against advanceSeg and other frees.
func (s *Store) freeSegment(si uint64) {
	s.segMu.Lock()
	defer s.segMu.Unlock()
	seg := &s.segs[si]
	seg.alloc.Store(seg.base)
	seg.live.Store(0)
	seg.stuck.Store(0)
	seg.draining.Store(false)
	s.releaseArenaPages(seg.base, s.segSize)
	s.freeSegs = append(s.freeSegs, si)
	s.availSegs.Add(1) // one more segment a blocked writer's advanceSeg can now claim
}

// retireSegment marks segment si dead but not yet free, the epoch-gated replacement for
// freeSegment on the concurrent migration path (M2, doc 21 section 7 D18). Where freeSegment
// returns a segment to the free list at once (safe only when traffic is quiesced), a migrator
// running against live readers cannot: a reader may have loaded a resident address from si
// before the migrator flipped the record away and still be copying those bytes. So this bumps
// the global epoch and records si tagged with the epoch just below the bump, the highest epoch a
// stale reader could hold, and defers the actual free to reclaimLocked once the safe epoch passes
// it. The bump is what lets the safe epoch advance under a steady read load: new readers publish
// an epoch above the retire epoch, so as the in-flight readers finish, the minimum published
// epoch rises past it. The caller (the migrator, or the M2 test) must have already flipped every
// live record's index entry off this segment, so nothing the index points at lives here anymore.
// It opportunistically reclaims, which frees si immediately when no reader is active.
func (s *Store) retireSegment(si uint64) {
	s.segMu.Lock()
	defer s.segMu.Unlock()
	newE := s.ep.global.Add(1)
	s.segs[si].retire = newE - 1
	s.retSegs = append(s.retSegs, si)
	s.reclaimLocked()
}

// reclaimLocked returns every retired segment whose retire epoch the safe epoch has passed to the
// free list, resetting each one's cursor and live counter so a later advance reuses it from
// scratch. It must be called under segMu. The safe-epoch read (epoch.go) is the whole guarantee:
// a segment freed here has a retire epoch strictly below every active reader's published epoch, so
// no reader still holds an address into it. A segment whose retire epoch has not yet been passed
// stays on retSegs for a later pass. The in-place filter reuses the retSegs backing array: it only
// ever writes a kept entry at an index at or below the one just read, so it never clobbers an
// entry it has not consumed.
func (s *Store) reclaimLocked() {
	if len(s.retSegs) == 0 {
		return
	}
	safe := s.safeEpoch()
	kept := s.retSegs[:0]
	for _, si := range s.retSegs {
		if safe > s.segs[si].retire {
			seg := &s.segs[si]
			seg.alloc.Store(seg.base)
			seg.live.Store(0)
			seg.stuck.Store(0)
			seg.draining.Store(false)
			s.releaseArenaPages(seg.base, s.segSize)
			s.freeSegs = append(s.freeSegs, si)
			s.availSegs.Add(1) // reclaimed a retired segment: now claimable, credit the hint
		} else {
			kept = append(kept, si)
		}
	}
	s.retSegs = kept
}

// reclaimSegments runs one reclamation pass under segMu and reports how many segments it returned
// to the free list. The migrator calls it to turn drained-and-retired segments into reusable ones
// as readers quiesce, off the foreground path (doc 21 D17 batches this per drained segment); the
// M2 test calls it to drive the retire-then-free cycle and to observe that reclamation made
// progress once its readers released their epochs. advanceSeg reclaims inline through reclaimLocked
// when it runs out of segments, so a fill that outpaces explicit reclamation still recovers freed
// space before reporting the arena full.
func (s *Store) reclaimSegments() int {
	s.segMu.Lock()
	defer s.segMu.Unlock()
	before := len(s.freeSegs)
	s.reclaimLocked()
	return len(s.freeSegs) - before
}

// resetSegments rewinds every segment to empty, drops the free list and the retire list, and
// rewinds the overflow region, the segmented-arena half of Reset. Like Reset itself it assumes
// traffic is quiesced, so it takes segMu only to stay consistent with the concurrent
// allocation paths' view of these fields. The global epoch is left as is: it is monotonic and a
// flush does not need to rewind it, and dropping retSegs is safe under quiescence because the
// rewind loop already returns every retired segment's bytes to empty.
func (s *Store) resetSegments() {
	s.segMu.Lock()
	defer s.segMu.Unlock()
	for i := range s.segs {
		s.segs[i].alloc.Store(s.segs[i].base)
		s.segs[i].live.Store(0)
		s.segs[i].stuck.Store(0)
		s.segs[i].draining.Store(false)
	}
	s.freeSegs = s.freeSegs[:0]
	s.retSegs = s.retSegs[:0]
	s.highWater = 1
	s.curSeg.Store(0)
	// Back to the initial layout: segment 0 current, every other segment claimable headroom.
	s.availSegs.Store(int64(len(s.segs)) - 1)
	s.ovTail.Store(s.ovBase)
}

// segmentUsed reports the resident bytes the segmented arena has handed out: the
// overflow region's used bytes plus each segment's allocated bytes, clamped per segment
// to segSize so a full segment's abandoned overshoot is not double counted. Freed and
// never-used segments read their base as their cursor and contribute nothing. It backs
// ArenaBytes in segmented mode and is an introspection call, not a hot path, so the walk
// over segments is fine.
func (s *Store) segmentUsed() uint64 {
	used := s.ovTail.Load() - s.ovBase
	for i := range s.segs {
		seg := &s.segs[i]
		a := seg.alloc.Load()
		if a <= seg.base {
			continue
		}
		u := min(a-seg.base, s.segSize)
		used += u
	}
	return used
}
