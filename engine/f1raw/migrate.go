package f1raw

import (
	"runtime"
	"time"
)

// migWaitStep and migStallWindow bound the write-path backpressure wait (D12, doc 23): when the
// arena is full and the migrator is engaged, allocRecord signals the migrator and blocks in
// waitForSegment for a segment to free rather than reporting the arena full at once. The wait gives
// up only after migStallWindow of wall-clock time passes with NO forward progress, where progress is
// either the cold-log tail advancing (bytes leaving RAM) or a drain target still existing (a full,
// migratable segment the migrator could free). Either resets the window, so a slow-but-draining
// migrator, or one merely starved of CPU while drainable work remains, holds the writer indefinitely.
// A write is dropped only on a genuine stall, the absence of any drain target: cold region full,
// disk error, non-migratable residue, or a leaked epoch, each of which leaves nothing the migrator
// could do to free a segment. This is the block-not-drop property: a collection larger than the arena
// loads fully rather than truncating, matching a swapping store, slow under overflow but never lossy.
// It replaces the old fixed poll budget, which gave up on total elapsed time and so dropped writes a
// slow migrator would have served (the SET LTM SADD collapse doc 23 root-causes). The window is
// measured in wall-clock, not poll count, because time.Sleep(migWaitStep) inflates to milliseconds
// under scheduler pressure, so a poll count would stretch the real give-up far past the intended
// window on a loaded box.
const (
	migWaitStep    = 100 * time.Microsecond
	migStallWindow = time.Second // wall-clock of ZERO progress before ErrFull
	// migSpinIters is the adaptive spin the backpressure wait runs before falling to the sleeping
	// poll. When the migrator keeps up, a drained segment retires and becomes reclaimable within a
	// few microseconds of the writer blocking, far under one migWaitStep. Sleeping migWaitStep in
	// that case pins every blocked allocation at ~100us and is the P16 SADD-under-migration ceiling:
	// with many concurrent writers each one wakes only once per 100us quantum even though a segment
	// freed almost immediately. The spin yields to the migrator and reclaims retired segments as
	// readers quiesce, so the common fast-free case returns in microseconds; only a genuinely slow
	// drain (cold-region I/O stall, reader holding an epoch) falls through to the sleeping poll.
	migSpinIters = 256
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

// stagedDrain records one record the batched drain (drainSegment) encoded into the shared frame
// buffer during its walk, so the flip pass can point each record's index entry at its cold frame
// after the single pwrite lands. off is the resident arena offset being drained; frameOff is the
// absolute cold-region offset the record's frame will occupy once the batch is written; kind and
// vec select the flip discipline (set member rows retier their dense vector under a shard mutex,
// every other kind flips the index entry directly); ver is the seqlock version encodeColdFrame
// paired with the framed value, so the resident flip can reject a record an in-place update touched
// after it was encoded rather than publish a stale frame.
type stagedDrain struct {
	off      uint64
	frameOff uint64
	ver      uint32
	kind     byte
	vec      bool
}

// flipResident points the index entry for the non-vector record at off at its already-written cold
// frame frameOff, the flip step of the batched drain for every kind but the set member row. It is
// the identity-and-version-anchored successor to the old per-record drainRecord flip: it probes the
// index and acts only when the entry still points at off (a miss or a hit at another offset means
// off was deleted or overwritten and is dead space already out of the segment's live counter), and
// only when the record's seqlock version still equals the one encodeColdFrame framed the value with.
// That version guard closes a hole the per-record path left open: inPlace ticks the version by two,
// so a value update that landed between the drain encoding this record and flipping it changes the
// version, and the guard leaves the record resident to re-drain with a fresh frame rather than flip
// the index to a frame holding the pre-update value. The residual window between the version load
// and the CAS is a few instructions, far shorter than the whole encode-to-flip span the old path
// left unguarded, so this is strictly safer than before. A lost CAS re-probes with the same
// frameOff; if the re-probe no longer lands on off the record moved and the frame is left as dead
// space. On the winning CAS it charges the resident bytes out of the segment so it drains toward
// retirement.
func (s *Store) flipResident(st stagedDrain) {
	klen := s.klen(st.off)
	key := s.arena[st.off+hdrSize : st.off+hdrSize+klen]
	h := hash(key)
	for {
		cur, b, slot, word, found := s.find(key, h, st.kind)
		if !found || cur != st.off {
			return
		}
		if s.verAt(st.off).Load() != st.ver {
			// An in-place update landed after this record was encoded: its framed value is stale.
			// Leave it resident so the next drain pass re-encodes and re-flips it.
			return
		}
		newWord := (word &^ addrMask) | st.frameOff | tierBit
		if b.slots[slot].CompareAndSwap(word, newWord) {
			s.unlinkResident(st.off)
			return
		}
		// Lost the entry to a concurrent writer; re-probe. The written frame is still valid, so a
		// re-probe that still lands on off retries the flip against the fresh word.
	}
}

// migratable reports whether a record of the given kind is safe for the background migrator to
// sink to the cold region. A string record always qualifies: it carries no secondary structure and
// its read, write, and delete paths all cross the tier boundary (doc 21 section 9). Any other kind
// qualifies only when the server's migratableKind policy admits it, which the server sets once it
// has proven that kind's secondary structures follow a record cold (SetMigratableKindFunc). The
// engine is type-agnostic, so it cannot judge a collection kind's tier-safety itself; it names the
// unconditional string floor here and defers the rest to the policy, the same split topKind uses.
// A nil policy (the default) leaves the migrator string-only, exactly as before any kind is opted
// in, so a store that never registers a policy drains only strings and every collection record
// stays resident.
func (s *Store) migratable(kind byte) bool {
	return kind == stringKind || (s.migratableKind != nil && s.migratableKind(kind))
}

// drainSegment sinks every record still live in segment si to the cold region and retires the
// segment once it is empty, the drain the pressure loop calls to reclaim a full segment's bytes. It
// runs in two phases so a whole segment drains with one cold-region pwrite instead of one per
// record: the per-record append the earlier per-record drain issued was the SET larger-than-memory
// write bound (measured ~1µs per record on the cold sink versus ~40ns per record when a segment's
// frames share one pwrite, a 28x cut).
//
// Phase 1 walks the segment from its base to its allocation cursor, stepping record to record by the
// immutable record size, and encodes every migratable record's cold frame into one shared buffer,
// capturing each record's seqlock version alongside its batch-relative frame offset. Phase 2 reserves
// the whole buffer's span in the cold region with one atomic tail bump and writes it with one pwrite,
// so every frame is durable before any index entry points at it. Phase 3 flips each staged record's
// index entry to its now-absolute cold offset: a set member row retiers its dense vector under the
// vector shard mutex (flipVecMember, Option A), every other kind flips the primary entry directly
// (flipResident), and both act only when the entry still points at the resident record and, for the
// direct flip, only when the version has not moved since the frame was encoded. A record deleted or
// overwritten between the walk and the flip is left as dead space in the batch, exactly as a lost CAS
// was before. When the last live record has left, the segment retires through the epoch gate
// (retireSegment) so it returns to the free list once the readers that could hold a pre-flip address
// into it drain.
//
// The caller must pass a full segment that is not the current allocation target, so no new record is
// laid into it during the walk (D15). The walk clamps at the segment end rather than the raw cursor,
// since a failed final allocation can leave the cursor overshot past the segment with no record
// written there. drainMu serializes only the reused scratch buffers, so a rare direct test call and
// the single migrator goroutine never corrupt the shared batch; production drains do not overlap, so
// it never contends. The segment is not freed until this same call retires it at the end, so the
// walk needs no other lock.
func (s *Store) drainSegment(si uint64) {
	if s.recs == nil || si >= uint64(len(s.segs)) {
		return
	}
	seg := &s.segs[si]
	if seg.live.Load() == 0 {
		// Already empty (every record deleted or drained on an earlier pass): retire it without
		// re-walking. The walk would re-find each now-cold record through the tier-aware index,
		// paying a cold-frame probe per record for nothing, so skip straight to the retire.
		s.retireSegment(si)
		return
	}
	// Wait for every writer that reserved a record in this segment to finish laying its bytes down
	// before walking them (doc 23 D23-6). A writer that reserved one of the segment's last slots while
	// it was still the current allocation target can still be inside initRecord after the seal moved
	// the current pointer off it and this drain was picked; the walk reads each record's immutable
	// header (recBytesAt, the liveness probe's key), so touching those bytes mid-write is a data race.
	// The segment is sealed (pickDrainTarget only returns full, non-current segments), so no new
	// reservation lands here and pending only falls, reaching zero once the last in-flight writer
	// commits. This runs on the single migrator goroutine off the foreground path, and the wait is
	// microseconds (initRecord is a memcpy), so a yielding spin is right.
	for seg.pending.Load() != 0 {
		runtime.Gosched()
	}
	limit := seg.base + s.segSize
	if a := seg.alloc.Load(); a < limit {
		limit = a
	}

	s.drainMu.Lock()
	buf := s.drainBuf[:0]
	stg := s.drainStg[:0]

	// Phase 1: encode every migratable record's frame into one buffer. A record is probed for
	// liveness before it is encoded, so a dead record (deleted or overwritten) is stepped over
	// without adding its bytes to the batch, and the frame offset staged here is batch-relative
	// until the reservation in phase 2 turns it absolute.
	for off := seg.base; off+hdrSize <= limit; {
		recBytes := s.recBytesAt(off)
		if recBytes == 0 || off+recBytes > limit {
			break // a torn or zero-width header would desync the walk; stop rather than misread
		}
		kind := s.arena[off+offKind]
		if s.migratable(kind) && s.liveRecordAt(off, kind) {
			rel := uint64(len(buf))
			var ver uint32
			buf, ver = s.encodeColdFrame(off, buf)
			stg = append(stg, stagedDrain{
				off:      off,
				frameOff: rel,
				ver:      ver,
				kind:     kind,
				vec:      s.isVecMember(kind),
			})
		}
		off += recBytes
	}

	// Phase 2: reserve the batch's span in the cold region with one atomic tail bump and write it
	// with one pwrite, so every frame is durable before phase 3 publishes any offset into it.
	var base uint64
	writeOK := true
	if len(buf) > 0 {
		n := uint64(len(buf))
		base = s.recs.tail.Add(n) - n
		if _, err := s.recs.f.WriteAt(buf, int64(base)); err != nil {
			// The batch write failed: publish nothing and leave every staged record resident. The
			// reserved span is abandoned as a hole in the cold region; the records re-drain on a
			// later pass once the sink recovers.
			writeOK = false
		}
	}

	// Phase 3: flip each staged record's index entry to its absolute cold offset.
	if writeOK {
		for i := range stg {
			st := stg[i]
			st.frameOff += base // batch-relative -> absolute cold-region offset
			if st.vec {
				klen := s.klen(st.off)
				key := s.arena[st.off+hdrSize : st.off+hdrSize+klen]
				s.flipVecMember(key, st.kind, st.off, st.frameOff|tierBit)
			} else {
				s.flipResident(st)
			}
		}
	}

	s.drainBuf = buf
	s.drainStg = stg
	s.drainMu.Unlock()

	if live := seg.live.Load(); live == 0 {
		s.retireSegment(si)
	} else {
		// The segment still holds live bytes the migrator cannot move: records of a kind the policy
		// does not admit (a collection header row stays resident while its fields migrate). Record
		// the residue so pickDrainTarget skips this segment until a delete lowers its live below the
		// mark, rather than re-picking the emptiest-but-unretireable segment every pass and starving
		// the field-bearing segments that can actually drain and free space.
		seg.stuck.Store(live)
	}
}

// liveRecordAt reports whether the record at resident offset off is still the live record its key
// resolves to, the phase-1 liveness probe drainSegment uses to skip dead records (deleted or
// overwritten to a fresh record elsewhere) before encoding them into the batch. It is the
// identity-checked probe (cur == off), stronger than the enumeration filter's liveAt (which only asks
// whether the key still exists as some record of that kind): a key overwritten to a fresh record
// elsewhere leaves off dead even though the key still exists, and encoding that dead record would
// waste cold-region space on a frame no index entry would ever point at. The key is read from the
// record's immutable header, so the probe is a plain index lookup.
func (s *Store) liveRecordAt(off uint64, kind byte) bool {
	klen := s.klen(off)
	key := s.arena[off+hdrSize : off+hdrSize+klen]
	cur, _, _, _, found := s.find(key, hash(key), kind)
	return found && cur == off
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
// is engaged or the migrator has genuinely stalled with the arena still full, which surfaces as
// ErrFull.
//
// The wait is what makes a bounded arena serve a dataset larger than itself transparently: the
// migrator drains full segments cold, which moves their records out of RAM and frees the segment,
// so a write that momentarily finds no room blocks on the drain instead of failing. It signals the
// migrator, then polls, reclaiming retired segments each round (a drained segment is free once its
// pre-flip readers quiesce) and checking for a free or never-used segment.
//
// The stopping rule is forward progress, not total elapsed time (doc 23, D23-1). While the migrator
// keeps draining, the cold-log tail keeps advancing, so the wait keeps blocking however long the
// drain takes; only when the tail sits still for migStallWindow does the migrator count as stalled
// and the caller report the arena full. That is the block-not-drop property: a hot
// set larger than the arena blocks on the drain rather than dropping the write, and ErrFull is
// reserved for a genuine stall (cold region full, disk error, non-migratable residue, leaked epoch),
// each of which keeps the tail fixed. A store with no migrator returns false immediately and pays
// nothing, so the non-LTM path keeps returning ErrFull exactly as before.
func (s *Store) waitForSegment() bool {
	if !s.migOn.Load() {
		return false // no migrator: the arena is genuinely full, report it at once
	}
	// Reaching here means the fast alloc found no segment and had to wait for the migrator, so count
	// the backpressure event now, before either the spin or the sleep serves it. Counting at entry
	// rather than only on the sleeping path keeps the INFO waits counter honest under a migrator fast
	// enough to free a segment within the spin: the arena was still overrun and a writer still blocked,
	// which is exactly what the counter reports (doc 23, D23-4).
	s.backpressureWaits.Add(1)
	// Fast path: wake the migrator and spin, yielding to it and reclaiming retired segments as
	// readers quiesce, so the common case where a drain frees a segment within microseconds returns
	// without paying a migWaitStep sleep. reclaimSegments turns the just-retired segment reusable
	// once the safe epoch passes it, which for a writer not holding an epoch here happens almost
	// immediately. Gosched hands the P to the migrator between checks rather than busy-burning it.
	s.signalMigrator()
	for i := 0; i < migSpinIters; i++ {
		if s.segAvailable() {
			return true
		}
		runtime.Gosched()
	}
	// Slow path: the drain is lagging (a large single drain, or heavy writer contention on the shard
	// mutex the migrator needs). Block while forward progress is still possible, giving up only after
	// migStallWindow of wall-clock with none. Progress is either the cold-log tail advancing (the
	// migrator bumps it on every record it sinks cold, so a moving tail means bytes are leaving RAM) or
	// a drain target still existing (pickDrainTarget finds a full, migratable segment the migrator could
	// yet free). Either keeps the write waiting. Only the absence of both for the whole stall window is
	// a genuine stall (cold region full, disk error, non-migratable residue, leaked epoch), and only
	// then does the write report the arena full. A retired segment that frees on reader quiescence
	// rather than a fresh drain is caught by segAvailable directly, so it does not need to show in the
	// progress signal (doc 23, D23-1..D23-3).
	lastTail := s.coldTail()
	lastProgress := time.Now()
	for {
		s.signalMigrator()
		time.Sleep(migWaitStep)
		if s.segAvailable() {
			return true
		}
		if t := s.coldTail(); t != lastTail {
			lastTail = t
			lastProgress = time.Now() // migrator is draining; keep waiting however long it takes
			continue
		}
		// The tail did not advance this poll, but that alone is not a stall: under CPU oversubscription
		// (many blocked writers plus the race detector on a few cores) the single migrator goroutine can
		// go unscheduled for a while, so no bytes leave RAM even though a segment is still drainable. As
		// long as pickDrainTarget finds a full, migratable segment, room can still be freed and the write
		// must keep blocking, which is the block-not-drop guarantee: a write waits on a possible drain
		// rather than dropping just because the migrator has not run yet. A genuine stall is the absence
		// of any drain target: every full segment is the current one, already retired, or non-migratable
		// residue (a failed cold write leaves records resident and marked stuck too), so nothing the
		// migrator could do would free a segment. Only that, sustained for migStallWindow, reports full.
		if _, ok := s.pickDrainTarget(); ok {
			lastProgress = time.Now()
			continue
		}
		if time.Since(lastProgress) >= migStallWindow {
			s.backpressureStalls.Add(1)
			return false // no drainable segment for the stall window: genuine stall, report full
		}
	}
}

// coldTail reports the cold-log append cursor, the write-path backpressure's forward-progress
// signal: it advances by the drained bytes on every record the migrator sinks cold and sits still
// when a drain cannot complete. It is one atomic load and no lock. A store with the migrator engaged
// always has a cold log, so the nil case is only the non-LTM path that never reaches the wait, and
// it reads zero there, which makes the progress check see a permanent stall and fall back to the old
// bounded give-up rather than blocking forever.
func (s *Store) coldTail() uint64 {
	if s.cold == nil {
		return 0
	}
	return s.cold.tail.Load()
}

// segAvailable reclaims any drained-and-retired segments whose readers have quiesced, then reports
// whether a segment is ready to allocate from: a reclaimed or previously-freed segment on the free
// list, or a never-used segment below the high-water mark. It folds the reclaim and the check into
// one segMu acquisition so a caller polling for room does not take the lock twice per round.
func (s *Store) segAvailable() bool {
	s.segMu.Lock()
	s.reclaimLocked()
	free := len(s.freeSegs) > 0 || s.highWater < uint64(len(s.segs))
	s.segMu.Unlock()
	return free
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
		if live > 0 && live == seg.stuck.Load() {
			// A prior drain already sank every migratable record here and left only a non-migratable
			// residue (a resident collection header row), so re-picking it just re-walks a segment
			// that can never retire, starving the field-bearing segments that can. It re-qualifies
			// once a delete (unlinkResident) lowers live below the stuck mark, so this skips it only
			// while the residue is genuinely unretireable, not permanently.
			continue
		}
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
