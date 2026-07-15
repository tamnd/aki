package store

import "sort"

// Arena reclaim (spec 2064/f3/04 section 4.3, path 3, dead-fraction
// compaction, and path 4, whole-segment drop). A superseded or deleted
// record's bytes stay in their segment; unlink charges them back so the
// segment's live counter drops and fill minus live is the segment's dead
// share. This file turns that accounting into freed pages: a segment past
// the dead-fraction threshold gets its survivors copied to the bump tail
// (plain stores and index repoints, single owner), then goes back to the
// free list with its pages returned to the OS.
//
// The spec's full design segregates record, node, and overflow streams into
// their own segments and bounds the online relocation per batch; M0 has one
// stream (records, runs, and chunk directories share segments) and no native
// nodes, so this is the M0 subset: one relocation pass moves whatever kind
// of tenant it finds, victims are picked whole, and the pass runs only where
// nothing else can hold an arena address (the owner's idle boundary, the
// between-drain boundary, and the fully-dead fast path on the write path
// itself). Epoch-gated freeing for off-owner readers (doc 04 section 5.2)
// lands with the snapshot and migrator milestones; today the owner is the
// only toucher and ChunkStream is the one cross-command reader, so an open
// stream pins the arena instead.

const (
	// arenaSegDeadNum/arenaSegDeadDen is the dead fraction past which a
	// segment is a compaction victim: dead*den >= fill*num. Swept by lab
	// labs/f3/m0/10_arena_reclaim over {1/8, 1/4, 1/2} under band-flip
	// churn with a pinned eighth: 1/4 edges 1/8 on ops/s at the same pause
	// p99 and steady footprint, and 1/2 trades a fifth more throughput for
	// an arena that rides one segment from full with the tight-path
	// widening doing all the reclaim, the regime that returned ErrFull
	// before the widening existed. 1/4 keeps the proactive path in charge.
	arenaSegDeadNum = 1
	arenaSegDeadDen = 4

	// arenaMoveBudget caps the survivor bytes one compaction pass may copy,
	// so the pause a pass adds to its drain boundary stays bounded no matter
	// how many segments crossed the threshold at once. The trigger fires
	// again at the next boundary and the pass resumes where it stopped.
	arenaMoveBudget = 64 << 20
)

// TuneArenaReclaim overrides the per-segment dead-fraction threshold
// (dead*den >= fill*num picks a victim). Labs and tests only; the shipped
// value is the frozen constant pair.
func (s *Store) TuneArenaReclaim(num, den uint64) {
	s.segDeadNum, s.segDeadDen = num, den
}

// ArenaReclaimable reports the dead bytes sitting in victim-eligible
// segments: touched, not the current segment, and past the dead-fraction
// threshold. This is the compaction trigger's figure; a walk over the
// segment descriptors, O(segments), run at the owner's idle boundary.
func (s *Store) ArenaReclaimable() uint64 {
	var n uint64
	for si := range s.arena.segs {
		u := uint64(si)
		if u == s.arena.cur || s.arena.segs[si].retired {
			continue
		}
		fill := s.arena.fillOf(u)
		if fill == 0 {
			continue
		}
		if dead := s.arena.deadOf(u); dead*s.segDeadDen >= fill*s.segDeadNum {
			n += dead
		}
	}
	return n
}

// ArenaTight reports whether the allocator is close to its genuine full
// state: fewer than two whole segments left to bring in. The between-drain
// trigger reads it every pass, so it is O(1); a tight arena under sustained
// writes is exactly the state the M0 gate died in, and the answer is a
// compaction pass at the next batch boundary rather than ErrFull.
func (s *Store) ArenaTight() bool {
	return s.arena.freeSegCount() < 2
}

// RetireSegment parks a fully drained segment for epoch-gated reclamation at
// stamp atEpoch and reports whether it took it (false if the segment is the
// current bump target or already retired). The M7 migration quantum
// (engine/f3/shard worker) calls it in phase 2 after every record in the
// segment has flipped cold and unlinked from the index, so the segment is dead
// but its bytes stay resident for any bracket that captured an address before
// the flip; ReclaimSafe frees it once the safe epoch passes the stamp. The
// fully-dead precondition is the caller's, exactly as for the internal free
// path.
func (s *Store) RetireSegment(si, atEpoch uint64) bool {
	return s.arena.retireSegment(si, atEpoch)
}

// ReclaimSafe hands every epoch-retired segment the safe epoch has cleared
// back to the free list and reports how many. The worker calls it at the batch
// boundary with epoch.safe() (engine/f3/shard), after it has exited the batch's
// bracket, so a segment retired while a still-open bracket could name it waits
// for that bracket to drain. Until the M7 migrator retires its first segment
// the retire list is empty and this is one length check; it is wired from the
// first batch so the reclamation contract is exercised before a real reader
// depends on it. Owner-only, same as CompactArena.
func (s *Store) ReclaimSafe(safe uint64) int {
	return s.arena.reclaimSafe(safe)
}

// CompactArena reclaims dead arena bytes: every fully dead segment is freed
// outright, and every segment past the dead-fraction threshold has its
// survivors relocated to the bump tail and is then freed. It returns the
// number of segments freed. Owner-only, and only at a boundary where no
// caller holds an arena address: the shard worker runs it at the idle
// boundary and between drain passes, never mid-command. An open ChunkStream
// holds chunk addresses across commands, so the pass refuses to run under
// one.
//
// Victim selection is budgeted: survivors land at the bump tail, so a pass
// may only move what the free space can hold, most-dead segments first so
// every byte of tail spent frees the most segment. Unbudgeted selection
// under a tight arena copies live bytes into the last free segments without
// draining any victim, which is how a compactor wedges the arena it was
// saving (the lab 10 sweep died exactly there before the budget). The pass
// loops while segments keep coming free, so each drained victim funds the
// next round; the move cap bounds the total copied per call, and an
// oversized backlog drains across calls.
//
// A tight arena also widens eligibility from the dead-fraction threshold to
// any segment with dead bytes: the threshold schedules proactive work, but
// backpressure's contract is that ErrFull means live bytes genuinely exceed
// capacity, and a store one segment from full does not get to be picky about
// which dead bytes it takes back (the lab 10 1/2 cell died at a third of the
// arena dead, all of it under the threshold).
//
// A relocation that cannot place a survivor (the tail is out of room)
// aborts its round; every move already made is complete (bytes copied, entry
// repointed, old charge dead), so an abort loses nothing and the next round
// or call picks up where it stopped.
func (s *Store) CompactArena() int {
	if s.openStreams > 0 {
		return 0
	}
	freed := 0
	budget := uint64(arenaMoveBudget)
	for {
		n, moved := s.compactPass(budget)
		freed += n
		if n == 0 || moved >= budget {
			return freed
		}
		budget -= moved
	}
}

// compactPass is one selection walk and one relocation walk: free the fully
// dead segments, pick victims most-dead-first within the space and move
// budgets, relocate their survivors, free the drained. It reports the
// segments freed and the survivor bytes it committed to moving.
func (s *Store) compactPass(moveBudget uint64) (freed int, moved uint64) {
	if cap(s.victims) < len(s.arena.segs) {
		s.victims = make([]bool, len(s.arena.segs))
	}
	victims := s.victims[:len(s.arena.segs)]
	tight := s.arena.freeSegCount() < 2
	var cands []segCand
	for si := range s.arena.segs {
		victims[si] = false
		u := uint64(si)
		if u == s.arena.cur || s.arena.segs[si].retired {
			continue
		}
		fill := s.arena.fillOf(u)
		if fill == 0 {
			continue
		}
		dead := s.arena.deadOf(u)
		if dead == fill {
			// Nothing lives here: no relocation, straight to the free list.
			s.arena.freeSegment(u)
			freed++
			continue
		}
		if dead == 0 {
			continue
		}
		if tight || dead*s.segDeadDen >= fill*s.segDeadNum {
			cands = append(cands, segCand{si: u, live: fill - dead})
		}
	}
	if len(cands) == 0 {
		return freed, 0
	}
	// The space budget: what the tail can absorb without the pass eating the
	// arena. Moves that fit the current segment's remaining room are always
	// safe (no advance, nothing abandoned); past that, one whole segment
	// stays back as slack for the tails a spanning placement abandons.
	cur := &s.arena.segs[s.arena.cur]
	room := cur.base + s.arena.segSize - cur.alloc
	budget := room
	if fc := s.arena.freeSegCount(); fc > 0 {
		budget = fc*s.arena.segSize + room - s.arena.segSize
	}
	if budget > moveBudget {
		budget = moveBudget
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].live < cands[j].live })
	nv := 0
	for _, c := range cands {
		if c.live > budget {
			break
		}
		budget -= c.live
		moved += c.live
		victims[c.si] = true
		nv++
	}
	if nv == 0 {
		return freed, 0
	}
	s.relocateLive(victims)
	for si := range victims {
		u := uint64(si)
		if victims[si] && s.arena.segs[si].live == 0 && s.arena.fillOf(u) > 0 {
			s.arena.freeSegment(u)
			freed++
		}
	}
	return freed, moved
}

// segCand is one victim candidate: the segment and its live bytes, which is
// exactly what a relocation would copy to the tail (moves reallocate the
// same aligned sizes), so the budget arithmetic is exact.
type segCand struct {
	si   uint64
	live uint64
}

// reclaimOnFull is the write path's backstop when allocRecord comes up
// empty: free every fully dead segment and report whether anything came
// back. Mid-command the caller may hold record addresses, arena views, and
// half-built records, so nothing is relocated here; a fully dead segment is
// safe by the same argument as freeSegment's contract, because anything the
// running command still references carries a live charge until publish
// drops it, and a charged segment is not fully dead. The relocating pass
// stays at the drain boundaries where no such reference can exist.
func (s *Store) reclaimOnFull() bool {
	if s.openStreams > 0 {
		return false
	}
	freed := false
	for si := range s.arena.segs {
		u := uint64(si)
		if u == s.arena.cur || s.arena.segs[si].retired {
			continue
		}
		if fill := s.arena.fillOf(u); fill > 0 && s.arena.segs[si].live == 0 {
			s.arena.freeSegment(u)
			freed = true
		}
	}
	return freed
}

// arenaAlloc is allocRecord with the full-arena backstop: on a failed
// allocation it reclaims what can be reclaimed mid-command and retries
// once. ErrFull surfaces only past this, when the live bytes genuinely
// exceed what the arena can hold. The compactor's own moves call
// allocRecord directly, never this, so a reclaim pass cannot recurse.
func (s *Store) arenaAlloc(n uint64) (uint64, bool) {
	if off, ok := s.arena.allocRecord(n); ok {
		return off, true
	}
	if !s.reclaimOnFull() {
		return 0, false
	}
	return s.arena.allocRecord(n)
}

// relocateLive walks every live index entry once and moves whatever sits in
// a victim segment to the bump tail: the record itself, a separated value's
// arena run, a chunked value's directory, and each arena-resident chunk
// run. The walk is the CompactLog walk: directory slots aliasing one index
// segment are skipped by the seen marks, and chains live in their segment's
// overflow slab. It stops at the first failed placement.
func (s *Store) relocateLive(victims []bool) {
	if cap(s.seen) < len(s.idx.segs) {
		s.seen = make([]bool, len(s.idx.segs))
	}
	seen := s.seen[:len(s.idx.segs)]
	for i := range seen {
		seen[i] = false
	}
	for _, ord := range s.idx.dir {
		if seen[ord] {
			continue
		}
		seen[ord] = true
		seg := s.idx.segs[ord]
		for bi := range seg.buckets {
			if !s.relocateBucket(&seg.buckets[bi], victims) {
				return
			}
		}
		for bi := range seg.overflow {
			if !s.relocateBucket(&seg.overflow[bi], victims) {
				return
			}
		}
	}
}

// relocateBucket moves one bucket's entries out of the victim segments:
// first the record, repointing the entry word in place (same slot, same
// tag), then the record's outside value bytes. It reports false when a
// placement failed and the pass must stop.
func (s *Store) relocateBucket(b *bucket, victims []bool) bool {
	for i := 0; i < slotsPerBucket; i++ {
		w := b.slots[i]
		if w == 0 {
			continue
		}
		addr := w & addrMask
		naddr, ok := s.relocateRecord(addr, victims)
		if !ok {
			return false
		}
		if naddr != addr {
			b.slots[i] = (w &^ addrMask) | naddr
		}
		if !s.relocateValue(naddr, victims) {
			return false
		}
	}
	return true
}

// relocateRecord copies a record out of a victim segment and returns its new
// address (or the old one when the record is not in a victim). The copy is
// verbatim, header through reserved capacity: kind, flags, the expiry slot,
// and the ver word all ride along, so a moved record is indistinguishable
// from the original at its new address. Epoch gating for off-owner readers
// of the old address is the snapshot milestone's job (doc 04 section 5.2);
// no such reader exists yet.
func (s *Store) relocateRecord(addr uint64, victims []bool) (uint64, bool) {
	si, ok := s.arena.segOf(addr)
	if !ok || !victims[si] {
		return addr, true
	}
	n := s.recBytes(addr)
	noff, ok := s.arena.allocRecord(n)
	if !ok {
		return addr, false
	}
	copy(s.arena.buf[noff:noff+n], s.arena.buf[addr:addr+n])
	s.arena.unlink(addr, n)
	return noff, true
}

// relocateValue moves a record's outside value bytes out of the victim
// segments: a separated record's arena run, or a chunked record's directory
// and arena-resident chunks. Log-resident runs are disk, not arena, and stay
// put; CompactLog owns those.
func (s *Store) relocateValue(addr uint64, victims []bool) bool {
	f := s.recFlags(addr)
	if f&flagChunked != 0 {
		return s.relocateChunks(addr, victims)
	}
	if f&flagSep == 0 {
		return true
	}
	vs := s.valueStart(addr)
	word, vlen, vcap := s.readPtr(vs)
	if word&inLogBit != 0 {
		return true
	}
	run := word & runAddrMask
	si, ok := s.arena.segOf(run)
	if !ok || !victims[si] {
		return true
	}
	noff, ok := s.arena.allocRecord(uint64(vcap))
	if !ok {
		return false
	}
	copy(s.arena.buf[noff:noff+uint64(vlen)], s.arena.buf[run:run+uint64(vlen)])
	s.arena.unlink(run, uint64(vcap))
	s.writePtr(vs, noff, vlen, vcap)
	return true
}

// relocateChunks moves a chunked record's directory and arena chunks out of
// the victim segments, rewriting the record's directory pointer and the
// touched directory entries in place.
func (s *Store) relocateChunks(addr uint64, victims []bool) bool {
	vs := s.valueStart(addr)
	word, n, dcap := s.readPtr(vs)
	dirOff := word & runAddrMask
	if si, ok := s.arena.segOf(dirOff); ok && victims[si] {
		nd, ok := s.arena.allocRecord(uint64(dcap) * ptrSize)
		if !ok {
			return false
		}
		copy(s.arena.buf[nd:nd+uint64(n)*ptrSize], s.arena.buf[dirOff:dirOff+uint64(n)*ptrSize])
		s.arena.unlink(dirOff, uint64(dcap)*ptrSize)
		s.writePtr(vs, nd, n, dcap)
		dirOff = nd
	}
	for k := uint32(0); k < n; k++ {
		es := dirOff + uint64(k)*ptrSize
		cw, clen, cc := s.readPtr(es)
		if cw&inLogBit != 0 {
			continue
		}
		run := cw & runAddrMask
		si, ok := s.arena.segOf(run)
		if !ok || !victims[si] {
			continue
		}
		noff, ok := s.arena.allocRecord(uint64(cc))
		if !ok {
			return false
		}
		copy(s.arena.buf[noff:noff+uint64(clen)], s.arena.buf[run:run+uint64(clen)])
		s.arena.unlink(run, uint64(cc))
		s.writePtr(es, noff, clen, cc)
	}
	return true
}
