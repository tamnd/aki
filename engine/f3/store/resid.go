package store

// Hot-set residency for the larger-than-memory regime (spec 2064/f3/09
// section 8, the M0 subset of the doc 06 tiering milestone). Before this file
// the resident cap was a one-way valve: once the arena fill crossed it, new
// separated values spilled to the value log and nothing ever came back, so
// under churn the resident set decayed toward zero live values and a zipfian
// workload paid one synchronous log read per GET for the same few hot keys
// forever. Residency makes the resident band track the working set from both
// sides:
//
//   - Promotion rides the read path. A log-resident separated run that is
//     read twice (the doorkeeper: a first touch only marks, and only a
//     sampled 1-in-residDoorkeeperDen of them, so a one-hit wonder never
//     promotes and uniform churn stays bounded) has its bytes copied into
//     the arena while the live charge sits under the cap, and every later
//     read of it is a memory read.
//   - Demotion rides the owner boundaries. When the resident live charge
//     crosses the low-water mark (the cap minus a slack fraction), a clock
//     hand walks the live index entries SIEVE-style: a resident
//     separated run whose visited bit is set survives with the bit cleared,
//     one whose bit is clear has its bytes appended to the value log and its
//     arena run charged dead. The hand also clears the doorkeeper mark on
//     log-resident records it passes, which is the ghost-window decay: a
//     cold key must be touched twice within one hand revolution to promote,
//     so uniform access over a dataset far past the cap promotes almost
//     nothing while zipfian heat promotes quickly and stays.
//
// The policy state is one header bit and plain single-owner counters: no
// atomics, no background goroutine, no per-record clock fields. The bit
// serves both roles because a run is in exactly one place at a time.
//
// Lifetime rules (the #566 contract) carry over unchanged: demotion frees
// arena bytes a GetView could name, so it runs only where compaction already
// runs, at the owner's idle boundary and between drain passes, never inside a
// command, and it refuses to run under an open ChunkStream. Promotion inside
// a read only allocates (and at worst triggers the fully-dead-segment
// backstop arenaAlloc already carries), which cannot invalidate a live view
// by the same argument reclaimOnFull's contract makes.
//
// Deferred past this slice, the doc 16 machinery this file is the subset of:
// chunked-band demotion and promotion (giant values already spill at write
// time and never promote whole by the doc 09 rule), whole-record demotion for
// the int and embedded bands, the evict byte with the 5-bit LFU counter, and
// async or batched value-log reads on the GET path (a cold GET stays one
// synchronous pread this slice, the same posture reclaim.go takes on
// epoch-gated freeing).
const (
	// residSlackDen sets the demotion low-water mark: a pass drives the live
	// charge down to cap - cap/residSlackDen, so promotions and fresh writes
	// between boundaries have headroom instead of spilling on arrival.
	// Frozen by labs/f3/m0/13_ltm_residency.
	residSlackDen = 8

	// demotePassBudget caps the value bytes one demotion pass may move to the
	// log, so the pause a pass adds to its boundary stays bounded the same way
	// arenaMoveBudget bounds a compaction pass. Under sustained pressure the
	// trigger fires again at the next boundary and the hand resumes.
	demotePassBudget = 8 << 20

	// demoteScanBudget caps the index slots one pass may examine, the guard
	// for the regime where the fill is over the cap but almost nothing is
	// demotable (records and dead bytes, which are compaction's job): the
	// pass must not degenerate into a full index walk per boundary.
	demoteScanBudget = 64 << 10

	// residDoorkeeperDen samples the doorkeeper: a first touch of a
	// log-resident run sets the mark with probability 1/den, so a key needs
	// about den+1 log reads to promote instead of two. The hand's mark decay
	// is far slower than the uniform re-touch interval, so without sampling
	// the marks saturate and two-touch degenerates to first-touch under
	// uniform access: lab 13 measured 0.16 promotions per GET there, and the
	// f3-ltm-strings bench (uniform by protocol) ran 4.7x slower than
	// no-residency main under the churn. Sampling cuts the uniform promotion
	// rate by about den while a zipfian hot key, read thousands of times,
	// still promotes within its first dozen touches. Frozen by lab 13's
	// doorkeeper rows.
	residDoorkeeperDen = 8
)

// Residency modes, TuneResidency's lab surface. The shipped mode is the
// two-touch doorkeeper; first-touch and off exist so the lab sweep can
// measure the policy against its alternatives in one binary.
const (
	ResidTwoTouch = iota
	ResidFirstTouch
	ResidOff
)

// ResidStats is the residency evidence surface: cumulative promotions,
// demotions, and value-log point reads, the figures the LTM lab and the INFO
// blob read to compute hit ratios and reads per op.
type ResidStats struct {
	Promotes uint64
	Demotes  uint64
	LogReads uint64
}

// Resid reports the residency counters.
func (s *Store) Resid() ResidStats {
	return ResidStats{Promotes: s.promotes, Demotes: s.demotes, LogReads: s.logReads}
}

// TuneResidency overrides the promotion policy. Labs and tests only; the
// shipped mode is the frozen two-touch doorkeeper.
func (s *Store) TuneResidency(mode int) {
	s.residMode = mode
	s.ltmOn = s.vlog != nil && s.residentCap > 0 && mode != ResidOff
}

// TuneDoorkeeper overrides the doorkeeper sampling denominator. Labs and
// tests only; den 1 marks every first touch (the deterministic doorkeeper
// the promotion tests pin), the shipped value is residDoorkeeperDen.
func (s *Store) TuneDoorkeeper(den uint64) {
	if den == 0 {
		den = 1
	}
	s.dkDen = den
}

// TuneMarkAlways switches touchResident to store the visited bit on every
// resident hit instead of check-then-set. Labs only: this is the always-store
// variant lab 15 prices the shipped check against, the "flagVisited turns a
// read-only GET into a cache-line write" suspicion from the run3 report.
// Bit semantics are identical either way; only the store count differs.
func (s *Store) TuneMarkAlways(on bool) {
	s.markAlways = on
}

// touchResident sets the visited bit on a resident separated run's record,
// the SIEVE mark the demotion hand honors. Check-then-set so a hot record's
// header line is not re-dirtied on every read; lab 15 measured the
// alternatives (no mark, this, store-every-hit) within noise of each other
// on the fully resident GET path at a 1M-key footprint, so the check is kept
// as free insurance, not as a proven save.
func (s *Store) touchResident(addr uint64) {
	if f := s.recFlags(addr); f&flagVisited == 0 {
		s.setRecFlags(addr, f|flagVisited)
	} else if s.markAlways {
		s.setRecFlags(addr, f)
	}
}

// maybePromote is the read path's promotion hook for a log-resident separated
// run whose bytes were just read into v. First touch marks (the doorkeeper),
// a marked touch promotes: the run's bytes go to the arena when the fill has
// headroom under the cap, the log bytes are charged dead, and the record's
// pointer swaps in place. No headroom means no promotion; the next boundary's
// demotion makes room and the mark keeps the record first in line.
func (s *Store) maybePromote(addr, vs uint64, vlen uint32, v []byte) {
	f := s.recFlags(addr)
	if s.residMode == ResidTwoTouch && f&flagVisited == 0 {
		// Sampled doorkeeper: mark 1 in dkDen first touches (see
		// residDoorkeeperDen). The xorshift is owner-local state, no atomics.
		r := s.dkRng
		r ^= r << 13
		r ^= r >> 7
		r ^= r << 17
		s.dkRng = r
		if r%s.dkDen == 0 {
			s.setRecFlags(addr, f|flagVisited)
		}
		return
	}
	need := align8(uint64(vlen))
	if s.spillNow(need) {
		return
	}
	off, ok := s.arenaAlloc(need)
	if !ok {
		return
	}
	copy(s.arena.buf[off:off+uint64(vlen)], v)
	s.vlog.dead += uint64(vlen)
	s.logRuns--
	s.writePtr(vs, off, vlen, uint32(need))
	if f&flagVisited == 0 {
		s.setRecFlags(addr, f|flagVisited)
	}
	s.promotes++
}

// ResidentOver reports whether the arena fill sits past the resident cap, the
// boundary trigger for the compaction that turns dead and demoted bytes into
// freed pages. Fill-based on purpose where admission (spillNow) is live-based:
// live past the cap is demotion's job, fill past the cap with live under it is
// dead bytes, and this is what schedules their reclaim. Independent of the
// residency mode: the cap bounds pages whether or not promotion is on.
// O(segments), boundary-rate only.
func (s *Store) ResidentOver() bool {
	return s.vlog != nil && s.residentCap > 0 && s.arena.used() > s.residentCap
}

// MaybeDemote runs one bounded demotion pass when the resident live charge
// sits past the low-water mark, and reports the arena bytes it freed.
// Owner-only, and only at a boundary where no caller holds an arena address,
// the CompactArena rule.
//
// The trigger is live past low, not fill past cap, and the distinction is the
// whole steady state. spillNow stops admitting at the cap, so the fill parks
// just under it and never crosses; a fill-triggered pass would fire once at
// most and the store would freeze with zero promotion headroom, the resident
// set forever the fill-order prefix. Targeting live > low keeps a slack of
// cap/residSlackDen open at every boundary: promotions and fresh writes fill
// the slack between boundaries, the next pass demotes the coldest runs back
// to the mark, and the visited bits decide who stays. Fill past the cap with
// live under the mark is dead bytes, compaction's job, and the pass declines.
func (s *Store) MaybeDemote() uint64 {
	if !s.ltmOn || s.openStreams > 0 {
		return 0
	}
	low := s.residentCap - s.residentCap/residSlackDen
	live := s.arena.live()
	if live <= low {
		return 0
	}
	return s.demotePass(live - low)
}

// demotePass advances the clock hand until it has freed want arena bytes, hit
// a pass budget, or completed one full revolution. The hand walks directory
// positions; a segment whose localDepth trails the global depth owns a
// contiguous alias run (top-bits indexing), so stepping by the alias span
// visits each segment exactly once per revolution.
func (s *Store) demotePass(want uint64) uint64 {
	if s.vlog.werr != nil {
		return 0 // a broken log takes no demotions; the store stays resident
	}
	dir := s.idx.dir
	if s.demoteHand >= uint64(len(dir)) {
		s.demoteHand = 0
	}
	var moved, scanned, steps uint64
	for steps < uint64(len(dir)) && moved < want && moved < demotePassBudget && scanned < demoteScanBudget {
		ord := dir[s.demoteHand]
		seg := s.idx.segs[ord]
		span := uint64(1) << (s.idx.gd - seg.localDepth)
		s.demoteHand = s.demoteHand&^(span-1) + span
		if s.demoteHand >= uint64(len(dir)) {
			s.demoteHand = 0
		}
		steps += span
		for bi := range seg.buckets {
			moved += s.demoteBucket(&seg.buckets[bi], &scanned)
		}
		for bi := range seg.overflow {
			moved += s.demoteBucket(&seg.overflow[bi], &scanned)
		}
	}
	return moved
}

// demoteBucket runs the hand over one bucket: resident separated runs demote
// or lose their visited bit, log-resident records lose their doorkeeper mark
// (the ghost-window decay). A demotion is one buffered log append and the
// pointer swap in place: the appended bytes are readable from the pending
// buffer immediately (vlog.append's contract), so the swap cannot dangle on
// the flush cadence, and the ledger deltas are the writeRun-to-log plus
// dropRun pair the write path makes. It returns the arena bytes freed.
func (s *Store) demoteBucket(b *bucket, scanned *uint64) uint64 {
	var freed uint64
	for i := 0; i < slotsPerBucket; i++ {
		w := b.slots[i]
		if w == 0 || slotCold(w) {
			continue // empty, or already cold: no arena run for the hand to move
		}
		*scanned++
		addr := w & addrMask
		f := s.recFlags(addr)
		if f&flagChunked != 0 || f&flagSep == 0 {
			continue // int, embedded, chunked: not this slice's tenants
		}
		vs := s.valueStart(addr)
		word, vlen, vcap := s.readPtr(vs)
		if word&inLogBit != 0 {
			if f&flagVisited != 0 {
				s.setRecFlags(addr, f&^flagVisited)
			}
			continue
		}
		if f&flagVisited != 0 {
			s.setRecFlags(addr, f&^flagVisited) // second chance
			continue
		}
		run := word & runAddrMask
		off, err := s.vlog.append(s.arena.buf[run : run+uint64(vlen)])
		if err != nil {
			return freed // log broken mid-pass: stop moving, keep the rest resident
		}
		s.arena.unlink(run, uint64(vcap))
		// A log run is immutable, so its capacity is exactly its length.
		s.writePtr(vs, inLogBit|off, vlen, vlen)
		s.logRuns++
		s.demotes++
		freed += uint64(vcap)
	}
	return freed
}

// spillCold reports whether an overwrite of a log-resident separated value
// should place its new bytes straight into the log. The residency policy's
// heat signal is reads (the doorkeeper); a write carries no read evidence, so
// once the live charge sits past the demotion low-water mark the
// arena-admit-then-demote round trip is pure churn: lab 17 measured it at
// 0.83 demotions per uniform SET, with the boundary demotion pwrites and the
// full-index compaction walks they trigger eating over 80% of the cell's CPU.
// Below the mark the arena has genuine headroom and the value comes back
// resident, the same recovery a promotion would buy. The gate is the
// low-water mark, not the cap, so the slack band stays what MaybeDemote's
// comment says it is: headroom for read-heat promotions, not for cold
// overwrite traffic.
func (s *Store) spillCold(n uint64) bool {
	if s.vlog == nil || s.residentCap == 0 || !s.spillLogDirect {
		return false
	}
	low := s.residentCap - s.residentCap/residSlackDen
	return s.arena.live()+n > low
}

// TuneSpillPlacement overrides the cold-overwrite placement policy. Labs and
// tests only: false restores the pre-slice arena-admit-then-demote behavior
// lab 17 prices the shipped log-direct placement against.
func (s *Store) TuneSpillPlacement(logDirect bool) {
	s.spillLogDirect = logDirect
}

// TuneVlogFlush overrides the value log's pending-buffer flush threshold.
// Labs and tests only: 1 flushes every append, the pre-batching posture; the
// shipped value is vlogFlushBytes.
func (s *Store) TuneVlogFlush(n int) {
	if s.vlog == nil || n <= 0 {
		return
	}
	s.vlog.flushAt = n
}
