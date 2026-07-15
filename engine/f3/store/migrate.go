package store

// The whole-record cold migrator (spec 2064/f3/06 section 2.4, the P5 half of
// M7 slice 1): the owner-scheduled pass that moves whole int and embedded
// string records out of the arena into the shard's cold region when the
// resident live charge sits past the low-water mark.
//
// It is the companion to the residency hand (resid.go). That hand bounds the
// separated band by moving a cold value's run to the value log while its record
// stays resident; it cannot touch the int and embedded bands, whose value bytes
// live inside the record with nowhere to spill. A workload of many small keys
// therefore pins the whole int-or-embedded record set in the arena, and once
// admission (spillNow) parks the fill at the cap the store has no way to take a
// new key: the resident set is small records that residency cannot demote. This
// migrator is that missing valve. It demotes the coldest whole records to the
// cold region (cold.go), charging their arena bytes dead, and the compaction
// that follows the pass at the same boundary turns those dead bytes into freed
// segments whose pages go back to the OS. Together the two hands make the cap a
// real resident-footprint bound across every value band, which is the memory
// bar the milestone is measured on.
//
// This form runs the migration synchronously on the owner: demoteAt frames the
// record and appends it to the cold region in the owner's buffer, then flips the
// slot in place. Its two-phase off-owner sibling (coldstage.go: stage in phase 1,
// pwrite on the I/O worker, flip on completion) is what the shard worker engages
// now, so it keeps the cold write off the critical path; MigrateCold stays as the
// synchronous store-level driver the migrator tests drive. Both share this hand
// and this trigger and differ only in when the write and the flip happen, so
// either changes only where records live, never how they answer.
const (
	// migratePassBudget caps the arena bytes one migration pass frees, so the
	// pause it adds to a boundary stays bounded like a demotion or compaction
	// pass. Under sustained pressure the trigger fires again next boundary and
	// the hand resumes from coldHand.
	migratePassBudget = 8 << 20
)

// MigrateCold demotes whole eligible string records to the cold region when the
// resident live charge is past the low-water mark, and reports how many records
// it moved. It shares the residency low-water (cap minus the slack fraction) and
// runs after MaybeDemote at the boundary, so value-run demotion gets first
// refusal on the pressure: a record only leaves the arena for cold once the
// cheaper move that keeps it resident is exhausted.
//
// Owner-only and boundary-rate, behind the same no-stream and cap-configured
// guard as the rest of the drain machinery. A store that never crosses the
// low-water never stages a frame, so the L9 no-pressure path is untouched: the
// call is one relaxed live-charge read and a compare.
func (s *Store) MigrateCold() int {
	if s.cold == nil || !s.ltmOn || s.openStreams > 0 || s.cold.werr != nil {
		return 0
	}
	low := s.residentCap - s.residentCap/residSlackDen
	live := s.arena.live()
	if live <= low {
		return 0
	}
	return s.migratePass(live - low)
}

// migratePass advances the migrator's hand until it has freed want arena bytes,
// hit the pass budget, or completed one directory revolution. The hand walks
// directory positions by alias span, the same stepping demotePass uses, so a
// segment aliased by a trailing localDepth is visited once per revolution.
func (s *Store) migratePass(want uint64) int {
	dir := s.idx.dir
	if s.coldHand >= uint64(len(dir)) {
		s.coldHand = 0
	}
	var moved uint64 // arena bytes freed
	var scanned, steps uint64
	n := 0 // records demoted
	for steps < uint64(len(dir)) && moved < want && moved < migratePassBudget && scanned < demoteScanBudget {
		ord := dir[s.coldHand]
		seg := s.idx.segs[ord]
		span := uint64(1) << (s.idx.gd - seg.localDepth)
		s.coldHand = s.coldHand&^(span-1) + span
		if s.coldHand >= uint64(len(dir)) {
			s.coldHand = 0
		}
		steps += span
		for bi := range seg.buckets {
			n += s.migrateBucket(&seg.buckets[bi], &scanned, &moved)
		}
		for bi := range seg.overflow {
			n += s.migrateBucket(&seg.overflow[bi], &scanned, &moved)
		}
	}
	return n
}

// migrateBucket demotes every eligible resident record in one bucket, adding
// each record's arena bytes to moved, and reports the count. A cold or
// non-demotable entry is skipped. The hand runs the SIEVE second chance the
// demotion-policy lab settled (labs/f3/m7/03, doc 06 section 4.2): a demotable
// record whose index-word visited bit is set survives one pass with the bit
// cleared, so a read-hot write-cold record is not sunk to then pay a pread per
// read; an unvisited record sinks. The bit is set by reads (touchSlot) and
// re-earned after a demote, so a record must be read again to keep its reprieve.
func (s *Store) migrateBucket(b *bucket, scanned, moved *uint64) int {
	n := 0
	for i := 0; i < slotsPerBucket; i++ {
		w := b.slots[i]
		if w == 0 || slotCold(w) {
			continue
		}
		*scanned++
		addr := w & addrMask
		if !s.demotable(addr) {
			continue
		}
		if slotVisited(w) {
			b.slots[i] = clearHeat(w) // second chance, re-earned by the next read
			continue
		}
		rb := s.recBytes(addr)
		if s.demoteAt(&b.slots[i], w) {
			n++
			*moved += rb
		}
	}
	return n
}

// touchSlot sets the index-word visited bit on a resident read, the SIEVE mark
// the whole-record migrator (migrateBucket, stageBucket) honors. It is gated on
// ltmOn so a store with no cold tier engaged never dirties the index word, the L9
// no-pressure contract: with LTM off this is one bool load. Check-then-set so a
// hot slot's cache line is not re-dirtied on every read, mirroring touchResident.
// Owner-only, called from the read path with the slot the probe already resolved.
func (s *Store) touchSlot(slot *uint64) {
	if s.ltmOn && !slotVisited(*slot) {
		*slot |= heatVisited
	}
}
