package f1raw

import "time"

// migWaitStep and migWaitPolls bound the write-path backpressure wait (D12): when the arena is
// full and the migrator is engaged, allocRecord signals the migrator and polls this many times,
// sleeping migWaitStep between polls, for a segment to free before it gives up and reports the
// arena full. The product is the total budget a single allocation blocks, kept short enough that a
// genuine out-of-space (cold region full, disk error keeping a drain from completing) still
// surfaces as ErrFull promptly rather than hanging, and long enough that a burst outrunning the
// migrator's own draining waits it out instead of failing a write the migrator would have served.
const (
	migWaitStep  = 100 * time.Microsecond
	migWaitPolls = 10000 // 100us x 10000 = up to 1s per blocked allocation
)

// defaultMigHiNum and defaultMigLoNum are the high- and low-water numerators over 100 of the
// segment budget the migrator drains between (doc 21 D14). It wakes when the resident live-byte
// total crosses 85% of the budget and drains cold until it falls below 75%, then sleeps. The gap
// is the batch budget and the hysteresis that keeps the migrator off the boundary. Both are lab
// knobs the bench sweeps; EnableMigrator seeds these defaults and a test overrides the fields.
const (
	defaultMigHiNum = 85
	defaultMigLoNum = 75
)

// This file adds the migrator's drain loop, the core of milestone M3 of the collection
// cold-record tiering plan (spec 2064/f1_rewrite_ltm/21 section 6). M0 made the arena
// reclaimable, M1 gave a record a way to leave RAM whole (the tier-tagged index and the
// cold record region), and M2 gated a freed segment on reader quiescence. What was still
// missing is the thing that drives records cold under memory pressure so a segment empties
// and can be reused: that is drainSegment here.
//
// drainSegment walks one full segment record by record and sinks every record still live
// in it to the cold region, flipping each record's one index entry to its cold frame with
// the same append-then-CAS MigrateToCold uses (M1). When the segment's last live record has
// left, its live counter (M3 slice 1) reaches zero and the segment retires through the
// epoch gate (M2), returning to the free list once no reader still holds an address into it.
// That closes the loop the larger-than-memory regime needs: a bounded arena serves a dataset
// larger than itself by continually draining its coldest full segments to the single file.
//
// This slice is the drain mechanism only. The trigger that decides when to drain and which
// segment to pick (D14 fill hysteresis, D15 emptiest-eligible selection), the background
// goroutine that runs it off the foreground path, and the write-path backpressure that waits
// on it rather than returning ErrFull (D12) are the following M3 slices. Here drainSegment is
// driven directly, so the drain-and-retire path is provable before the pressure loop that
// calls it exists, the same staging M1 used for MigrateToCold.
//
// Two properties make the linear walk safe without locking the segment:
//
//   - A drained segment is full and is not the current allocation target, so no new record
//     is laid into it during a drain (D15 restricts draining to full, non-current segments).
//     Its records' immutable header fields (kind, key length, reserved value capacity) never
//     change, so stepping record to record by recBytesAt stays aligned to record boundaries
//     even against a concurrent in-place value update, which touches only the value bytes and
//     the seqlock-guarded length.
//
//   - Every sink is a single CAS on the exact index word the walk observed, conditioned on
//     the entry still pointing at this very offset. A record the walk reaches that a
//     concurrent overwrite or delete already unlinked is dead space: its key now resolves to
//     a different offset or to nothing, so the identity check fails and the walk skips it, its
//     bytes already charged out of the segment's live counter when it was unlinked. Nobody
//     frees or reuses a byte except under the epoch gate, so a reader mid-copy is never cut.

// drainRecord sinks the single resident record at arena offset off to the cold region if it
// is still the live record for its key, returning true when it moved one. It is the per-record
// step of drainSegment and the identity-anchored twin of MigrateToCold: where MigrateToCold
// migrates whatever record currently answers a key, this migrates only the record at off, so a
// walk sinks the bytes it is actually trying to drain and never a fresh record a concurrent
// overwrite published in another segment.
//
// It reads the key and kind from the record's immutable header, probes the index, and acts only
// when the entry still points at off. A miss (the key was deleted) or a hit at a different offset
// (the key was overwritten to a new record) means off is already dead space the arena reclaims
// with the segment, so there is nothing to do and its bytes are already out of the live counter.
// On a match it appends the cold frame and CAS-flips the entry to it; a lost CAS re-probes, and if
// the re-probe no longer lands on off the record was unlinked under us and the fresh cold frame is
// abandoned as dead space, exactly MigrateToCold's rule. On the winning CAS it charges the resident
// bytes back to the segment so the segment drains toward zero.
func (s *Store) drainRecord(off uint64) bool {
	if s.recs == nil || off&tierBit != 0 {
		return false
	}
	kind := s.arena[off+offKind]
	if !migratable(kind) {
		// A collection element record still has resident addresses cached in its type's
		// secondary structures (the ordered index and the set member vectors), which a cold
		// flip would leave dangling until D8/D20 refreshes them on migration. Until that
		// lands, only string records, which have no secondary structure and a fully
		// tier-aware read, write, and delete path, are safe to sink; leave the rest resident.
		// A pure-string larger-than-memory workload drains its segments fully regardless.
		return false
	}
	klen := s.klen(off)
	key := s.arena[off+hdrSize : off+hdrSize+klen]
	h := hash(key)
	for {
		cur, b, slot, word, found := s.find(key, h, kind)
		if !found || cur != off {
			// off is not the live record for this key anymore: deleted, or overwritten to a
			// fresh record elsewhere. Either way it is dead space, already unlinked from the
			// segment's live counter, so the drain leaves it alone.
			return false
		}
		frameOff, err := s.migrateRecordAt(off)
		if err != nil {
			return false
		}
		newWord := (word &^ addrMask) | frameOff | tierBit
		if b.slots[slot].CompareAndSwap(word, newWord) {
			s.unlinkResident(off)
			return true
		}
		// Lost the entry to a concurrent writer; the appended frame is now dead space. Re-probe:
		// if the key moved off this offset the next iteration bails, otherwise the retry sinks it.
	}
}

// migratable reports whether a record of the given kind is safe for the background migrator to
// sink to the cold region. Only string records qualify today: they carry no secondary structure
// and their read, write, and delete paths all cross the tier boundary (doc 21 section 9). A
// collection element record's kind fails this until D8/D20 teaches its type's ordered index and
// member vectors to follow a record cold, so the migrator leaves such records resident. This is
// the one place the safe set is named, so widening it later is a single edit here.
func migratable(kind byte) bool {
	return kind == stringKind
}

// drainSegment sinks every record still live in segment si to the cold region and retires the
// segment once it is empty, the M3 drain the pressure loop (a later slice) will call to reclaim a
// full segment's bytes. It walks the segment from its base to its allocation cursor, stepping
// record to record by the immutable record size, and drains each one through drainRecord. Dead
// records (already overwritten, deleted, or migrated) are stepped over at no cost beyond the probe;
// live ones move cold and leave the segment's live counter. When the last live record has left, the
// counter reads zero and the segment retires through the epoch gate (retireSegment), so it returns
// to the free list as soon as the readers that could hold a pre-flip address into it drain.
//
// The caller must pass a full segment that is not the current allocation target, so no new record
// is being laid into it during the walk (D15). The walk clamps at the segment end rather than the
// raw cursor, since a failed final allocation can leave the cursor overshot past the segment with no
// record written there. It takes no lock: the sinks are index-entry CASes and the segment is not
// freed until this same call retires it at the end.
func (s *Store) drainSegment(si uint64) {
	if s.recs == nil || si >= uint64(len(s.segs)) {
		return
	}
	seg := &s.segs[si]
	limit := seg.base + s.segSize
	if a := seg.alloc.Load(); a < limit {
		limit = a
	}
	for off := seg.base; off+hdrSize <= limit; {
		recBytes := s.recBytesAt(off)
		if recBytes == 0 || off+recBytes > limit {
			break // a torn or zero-width header would desync the walk; stop rather than misread
		}
		s.drainRecord(off)
		off += recBytes
	}
	if seg.live.Load() == 0 {
		s.retireSegment(si)
	}
}

// EnableMigrator starts the background migrator, the goroutine that drives records cold under
// arena fill pressure (doc 21 section 6). It requires a segmented arena and an enabled cold record
// region: the migrator sinks whole record frames into the region, so both must be set up first,
// which the server does at startup and a test does before writing. It seeds the default high- and
// low-water marks, which a caller can override on the store fields before the first fill, and spawns
// one migrator goroutine. Calling it twice, or without segments or a cold region, is a caller error;
// a store that never calls it never allocates the channels and never pays for the migrator. Close
// stops the goroutine before closing the cold region it writes to.
func (s *Store) EnableMigrator() {
	if !s.segmented || s.recs == nil {
		panic("f1raw: EnableMigrator needs a segmented arena and an enabled cold record region")
	}
	if s.migOn.Load() {
		panic("f1raw: migrator already enabled")
	}
	if s.migHiNum == 0 {
		s.migHiNum = defaultMigHiNum
	}
	if s.migLoNum == 0 {
		s.migLoNum = defaultMigLoNum
	}
	s.migStop = make(chan struct{})
	s.migDone = make(chan struct{})
	s.migWake = make(chan struct{}, 1)
	s.migOn.Store(true)
	go s.migrator()
}

// signalMigrator wakes the migrator to reassess the fill level. It is a single non-blocking send on
// a size-one channel, so a wake already pending coalesces with this one and the caller never blocks,
// which is what lets it be called from advanceSeg under segMu. It is a no-op unless the migrator is
// engaged, gated by one atomic load so the non-segmented and no-migrator paths pay nothing.
func (s *Store) signalMigrator() {
	if !s.migOn.Load() {
		return
	}
	select {
	case s.migWake <- struct{}{}:
	default:
	}
}

// waitForSegment is the write-path backpressure (D12): when allocRecord cannot get a segment it
// calls this to wait for the migrator to free one rather than reporting the arena full at once. It
// returns true when a segment has become available to retry the allocation, false when no migrator
// is engaged or the bounded wait elapsed with the arena still full, which surfaces as ErrFull.
//
// The wait is what makes a bounded arena serve a dataset larger than itself transparently: the
// migrator drains full segments cold, which moves their records out of RAM and frees the segment,
// so a write that momentarily finds no room blocks on the drain instead of failing. It signals the
// migrator, then polls, reclaiming retired segments each round (a drained segment is free once its
// pre-flip readers quiesce) and checking for a free or never-used segment. Progress holds as long
// as the cold region can take more; when it genuinely cannot, the poll budget elapses and the
// caller reports the arena full. A store with no migrator returns false immediately and pays
// nothing, so the non-LTM path keeps returning ErrFull exactly as before.
func (s *Store) waitForSegment() bool {
	if !s.migOn.Load() {
		return false // no migrator: the arena is genuinely full, report it at once
	}
	for i := 0; i < migWaitPolls; i++ {
		s.signalMigrator()
		time.Sleep(migWaitStep)
		s.reclaimSegments() // turn any drained-and-retired segments into reusable ones
		s.segMu.Lock()
		free := len(s.freeSegs) > 0 || s.highWater < uint64(len(s.segs))
		s.segMu.Unlock()
		if free {
			return true
		}
	}
	return false
}

// migrator is the background goroutine's loop: sleep until woken by a segment advance or a stop,
// then run one migration pass. It owns no segment between passes, so a stop between passes exits at
// once; a stop during a pass is picked up by the pass's own stop check, then this returns on the
// next select. It closes migDone on exit so Close can wait for it to finish.
func (s *Store) migrator() {
	defer close(s.migDone)
	for {
		select {
		case <-s.migStop:
			return
		case <-s.migWake:
		}
		s.migrateDown()
	}
}

// migrateDown runs one migration pass: if the resident live-byte total is above the high-water mark
// it drains the emptiest eligible full segments to cold, one at a time, until the total falls below
// the low-water mark or no eligible segment remains. It bounds the pass at the segment count so a
// disk error that keeps a segment from draining cannot spin the goroutine; the next wake resumes.
// It checks the stop signal between segments so Close does not wait a whole pass.
func (s *Store) migrateDown() {
	budget := uint64(len(s.segs)) * s.segSize
	hi := budget * s.migHiNum / 100
	lo := budget * s.migLoNum / 100
	if s.liveBytes() < hi {
		return
	}
	for n := 0; n < len(s.segs) && s.liveBytes() >= lo; n++ {
		si, ok := s.pickDrainTarget()
		if !ok {
			return
		}
		s.drainSegment(si)
		select {
		case <-s.migStop:
			return
		default:
		}
	}
}

// liveBytes sums the resident live-byte total across every segment, the figure the migrator's
// trigger reads against the budget (D14). It is an O(segment count) walk of atomic counters, run on
// the migrator goroutine and never on the foreground request path, so the walk is fine. A freed or
// never-used segment reads its counter as zero and contributes nothing.
func (s *Store) liveBytes() uint64 {
	var total int64
	for i := range s.segs {
		if v := s.segs[i].live.Load(); v > 0 {
			total += v
		}
	}
	return uint64(total)
}

// pickDrainTarget returns the emptiest full segment eligible to drain, the D15 unit-of-work
// selection: preferring the one with the fewest live bytes (least work per segment freed) among the
// full segments that are neither the current allocation target nor already retired. A full segment
// is one whose bump cursor reached its seal, so a free or partially filled segment (cursor below the
// seal) is never a target and no new record is landing in the one chosen. It runs under segMu so its
// view of the current pointer and the retire list is consistent with the allocators and the
// reclaimer. ok is false when no full segment is eligible, which sends the migrator back to sleep.
func (s *Store) pickDrainTarget() (uint64, bool) {
	s.segMu.Lock()
	defer s.segMu.Unlock()
	cur := s.curSeg.Load()
	var best uint64
	var bestLive int64 = -1
	for i := range s.segs {
		si := uint64(i)
		if si == cur {
			continue
		}
		seg := &s.segs[i]
		if seg.alloc.Load() < seg.base+s.segSize {
			continue // free or partially filled: not a full drain target
		}
		if s.isRetiredLocked(si) {
			continue // already drained and retired, awaiting reader quiescence
		}
		live := seg.live.Load()
		if bestLive < 0 || live < bestLive {
			bestLive = live
			best = si
		}
	}
	if bestLive < 0 {
		return 0, false
	}
	return best, true
}

// isRetiredLocked reports whether segment si is on the retire list, awaiting the epoch gate before
// it returns to the free list. It must be called under segMu. The list holds only drained segments
// not yet reclaimed, so it stays short, and the linear scan is off the foreground path.
func (s *Store) isRetiredLocked(si uint64) bool {
	for _, r := range s.retSegs {
		if r == si {
			return true
		}
	}
	return false
}
