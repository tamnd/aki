package store

import "encoding/binary"

// The two-phase async migration quantum (spec 2064/f3/06 section 3.1, the
// off-owner half of M7 slice 1): the migrator variant that keeps the cold
// pwrite off the owner's critical path. Its synchronous sibling (migrate.go)
// frames a record and appends it to the cold region inside the owner's buffer,
// then flips the slot, all on the owner: correct, but the owner sits in the
// write. This variant splits the move in two so the disk runs in parallel with
// the owner.
//
//   - Phase 1 (StageColdDrain, owner, CPU only): frame a run of cold-bound
//     records into a detached staging buffer, reserve the cold-region span the
//     buffer will occupy, and record one flip entry per record. The records stay
//     resident and fully readable; nothing in the index changes yet. The staged
//     records are marked flagMigrating so a racing foreground write can cancel
//     them (findResident, str.go).
//   - The buffer and its offset go to the shard's I/O worker (ioworker.go), which
//     pwrites it to the cold region off the owner and posts a completion.
//   - Phase 2 (CompleteColdDrain, owner, on the completion event): for each staged
//     record, flip its slot to the cold offset if it is still the record phase 1
//     framed, else drop the frame. A frame the flip drops is unreferenced and
//     falls to the cold region's eventual compaction, exactly like a bring-up's
//     abandoned frame.
//
// The load-bearing correctness question is the stale flip: between phase 1 and
// phase 2 the owner admits foreground commands, and one may write the very key a
// staged frame holds. f3's in-place overwrite mutates the value at the same
// arena address, so an address-equality check (doc 06 section 3.1) would flip a
// frame that no longer matches the record and resurrect the old value. The fix
// is a version compare plus the flagMigrating mark: findResident cancels a
// staged record's migration in place (cancelMigrate bumps its version and clears
// the mark) before any write touches it, and phase 2 flips only a slot that
// still names the staged address at the staged version with the mark intact. An
// overwrite, a republish, a delete, or an arena-address reuse each fails at least
// one of those, so the flip drops rather than corrupts.

// stageFillNum and stageFillDen bound how much of a staging buffer one drain
// fills before it stops framing: past this fraction of the buffer's capacity the
// pass leaves the rest, so a single demotable record wider than the remaining
// slack cannot force the buffer to grow past the pool's fixed capacity and lose
// the memory bound the cap/4 pool exists to hold. The slack below the fraction
// covers one whole-record frame (a 64 KiB key plus a 64 KiB value plus the
// header), which a buffer sized in the megabytes clears with room to spare.
const (
	stageFillNum = 3
	stageFillDen = 4
)

// coldFlip is one staged record's phase-2 instruction: where its key sits in the
// drain buffer (so phase 2 can re-find the record by key without a copy), the
// identity it must still match to flip (arena address and version), and the cold
// offset the frame will live at once the pwrite lands. hash is the record's full
// hash, captured in phase 1 so phase 2 re-finds with a plain findEntry.
type coldFlip struct {
	keyOff  int
	keyLen  int
	hash    uint64
	addr    uint64
	ver     uint32
	coldOff uint64
}

// coldDrain is one in-flight two-phase migration: the staging buffer the owner
// framed and the I/O worker pwrites, the cold-region base offset the whole
// buffer lands at, the per-record flip list, and the fill ceiling phase 1 framed
// against. It crosses the owner-to-worker boundary by reference; the worker reads
// buf and off only, and phase 2 back on the owner reads flips.
type coldDrain struct {
	buf   []byte
	off   uint64
	flips []coldFlip
	soft  int
}

// Buf is the staged bytes the I/O worker pwrites. Off is the cold-region offset
// it pwrites them to. Both are set before the drain leaves the owner.
func (d *coldDrain) Buf() []byte { return d.buf }
func (d *coldDrain) Off() int64  { return int64(d.off) }

// recVer reads the record's version word. Even means committed; the migrator's
// stale-flip guard compares it, and cancelMigrate bumps it.
func (s *Store) recVer(off uint64) uint32 {
	return binary.LittleEndian.Uint32(s.arena.buf[off+offVer:])
}

// bumpVer advances the record's version by two, keeping it even. A foreground
// write to a staged record calls it through cancelMigrate so the migrator's
// phase-2 compare misses the record it staged.
func (s *Store) bumpVer(off uint64) {
	binary.LittleEndian.PutUint32(s.arena.buf[off+offVer:], s.recVer(off)+2)
}

// cancelMigrate takes a resident record back out of an in-flight cold drain: it
// clears the migrating mark and bumps the version, so phase 2's flip drops the
// frame it already wrote rather than pointing the index at a stale value. It is
// the single interlock findResident runs when a foreground write reaches a record
// while a drain is in flight (str.go), and it no-ops on any record that is not
// actually staged, so the guard costs one flag test off the hot path. It does not
// touch the migrating counter: phase 2 still walks this record's flip entry and
// accounts it there, whether it flips or drops.
func (s *Store) cancelMigrate(addr uint64) {
	f := s.recFlags(addr)
	if f&flagMigrating == 0 {
		return
	}
	s.setRecFlags(addr, f&^flagMigrating)
	s.bumpVer(addr)
}

// NeedsColdDrain reports whether the resident live charge sits past the migrator's
// low-water mark with a reachable cold region, the cheap gate the shard worker
// checks before it draws a staging buffer. It shares the synchronous migrator's
// trigger exactly: a store under its cap answers false on one relaxed live-charge
// read and a compare, so the L9 no-pressure boundary pays that and no pool churn.
func (s *Store) NeedsColdDrain() bool {
	if s.cold == nil || !s.ltmOn || s.openStreams > 0 || s.cold.werr != nil {
		return false
	}
	low := s.residentCap - s.residentCap/residSlackDen
	return s.arena.live() > low
}

// StageColdDrain is phase 1: it frames a run of cold-bound records into buf,
// reserves the cold-region span they will occupy, and returns the drain for the
// I/O worker to pwrite. It fires under the same low-water pressure and behind the
// same guard as the synchronous migrator, so a store under its resident cap
// stages nothing and the L9 no-pressure path is untouched. It returns nil when
// there is no pressure or no reachable cold region, and a drain with an empty
// flip list when the pass framed nothing (the caller returns the buffer to the
// pool). The staged records stay resident until phase 2; buf is the caller's
// pool buffer and may be returned grown.
func (s *Store) StageColdDrain(buf []byte) *coldDrain {
	if !s.NeedsColdDrain() {
		return nil
	}
	low := s.residentCap - s.residentCap/residSlackDen
	d := &coldDrain{buf: buf[:0], soft: cap(buf) * stageFillNum / stageFillDen}
	s.stagePass(d, s.arena.live()-low)
	if len(d.flips) == 0 {
		return d
	}
	// Reserve the whole buffer's span in the cold region and fix each frame's
	// absolute offset. The reservation flushes any pending cold bytes and routes
	// the range to the file, but publishes no index entry, so the unwritten range
	// is a hole no read reaches until phase 2 flips a slot onto it. A refused
	// reservation (a broken region) unstages every record and reports no drain.
	base, err := s.cold.reserve(len(d.buf))
	if err != nil {
		s.CompleteColdDrain(d, false)
		return nil
	}
	d.off = base
	for i := range d.flips {
		d.flips[i].coldOff += base
	}
	return d
}

// stagePass advances the migrator's clock hand, framing every eligible record it
// passes into the drain until it has staged want arena bytes' worth, filled the
// buffer to its soft ceiling, hit the pass or scan budget, or completed one
// directory revolution. It is migratePass's walk (the same coldHand, the same
// alias-span stepping) with framing in place of the synchronous demote, so the
// two migrator forms share a hand and a trigger and differ only in when the write
// and the flip happen.
func (s *Store) stagePass(d *coldDrain, want uint64) {
	dir := s.idx.dir
	if s.coldHand >= uint64(len(dir)) {
		s.coldHand = 0
	}
	var moved uint64
	var scanned, steps uint64
	for steps < uint64(len(dir)) && moved < want && moved < migratePassBudget && scanned < demoteScanBudget && len(d.buf) < d.soft {
		ord := dir[s.coldHand]
		seg := s.idx.segs[ord]
		span := uint64(1) << (s.idx.gd - seg.localDepth)
		s.coldHand = s.coldHand&^(span-1) + span
		if s.coldHand >= uint64(len(dir)) {
			s.coldHand = 0
		}
		steps += span
		for bi := range seg.buckets {
			s.stageBucket(&seg.buckets[bi], d, &scanned, &moved)
		}
		for bi := range seg.overflow {
			s.stageBucket(&seg.overflow[bi], d, &scanned, &moved)
		}
	}
}

// stageBucket frames every eligible resident record in one bucket into the drain,
// adding each record's arena bytes to moved. It stops early once the buffer
// reaches its soft ceiling so the bucket cannot push the buffer past the pool's
// capacity in one pass. A cold or non-demotable entry is skipped, exactly as the
// synchronous migrateBucket skips it.
func (s *Store) stageBucket(b *bucket, d *coldDrain, scanned, moved *uint64) {
	for i := 0; i < slotsPerBucket; i++ {
		if len(d.buf) >= d.soft {
			return
		}
		w := b.slots[i]
		if w == 0 || slotCold(w) {
			continue
		}
		*scanned++
		addr := w & addrMask
		if !s.demotable(addr) {
			continue
		}
		*moved += s.recBytes(addr)
		s.stageRecord(d, addr)
	}
}

// stageRecord frames the record at addr into the drain and records its flip
// entry. The frame is written before the migrating mark is set, so it carries the
// record's clean flags (the mark is an owner-side interlock, never part of the
// durable frame). The flip captures the record's identity now (address and
// version) so phase 2 can tell the staged record from anything a racing write put
// in its place. The migrating counter rises here and falls in phase 2.
func (s *Store) stageRecord(d *coldDrain, addr uint64) {
	frameStart := len(d.buf)
	d.buf = s.frameRecord(addr, d.buf)
	d.flips = append(d.flips, coldFlip{
		keyOff:  frameStart + coldHdr,
		keyLen:  int(s.klen(addr)),
		hash:    Hash(s.keyAt(addr)),
		addr:    addr,
		ver:     s.recVer(addr),
		coldOff: uint64(frameStart),
	})
	s.setRecFlags(addr, s.recFlags(addr)|flagMigrating)
	s.migrating++
}

// CompleteColdDrain is phase 2: it resolves every staged record now that the
// pwrite has returned, and reports how many records it flipped cold. ok is the
// pwrite's success. On a failed write no frame is durable, so it keeps every
// still-staged record resident and only clears its mark. On success it flips each
// slot that still names the staged address at the staged version with the mark
// intact, dropping any frame a racing write, delete, or address reuse orphaned.
// It runs on the owner off the completion queue, in program order with the
// foreground commands that may have raced it, and drops the migrating count by
// one per staged record however that record resolved.
func (s *Store) CompleteColdDrain(d *coldDrain, ok bool) int {
	n := 0
	for i := range d.flips {
		f := &d.flips[i]
		s.migrating--
		key := d.buf[f.keyOff : f.keyOff+f.keyLen]
		slot, addr, _ := s.findEntry(f.hash, key)
		staged := slot != nil && !slotCold(*slot) && addr == f.addr &&
			s.recVer(addr) == f.ver && s.recFlags(addr)&flagMigrating != 0
		if !staged {
			// A racing write already cancelled this record (mark cleared, version
			// bumped), or it was deleted, republished, or its arena address reused:
			// the frame is stale. Nothing to do on failure; on success the frame is
			// simply left unreferenced for the cold region's later compaction.
			continue
		}
		if !ok {
			// The write failed: no frame is durable, so the record stays resident.
			// Clear the mark so a later drain can stage it again.
			s.setRecFlags(addr, s.recFlags(addr)&^flagMigrating)
			continue
		}
		// Still the staged record, unchanged, and its frame is on disk: flip the
		// slot to the cold offset in place, keeping tag and heat, and charge the
		// arena bytes dead. The mark rode along in the record's flags, not the
		// frame, so the band census reads the clean flags.
		w := *slot
		*slot = (w &^ (addrMask | tierMask<<tierShift)) | tierCold<<tierShift | f.coldOff
		s.noteDrop(s.recFlags(addr) &^ flagMigrating)
		s.coldRecs++
		s.arena.unlink(addr, s.recBytes(addr))
		// A flip that unlinked the last live record of its segment leaves the
		// segment fully dead. Note it so the worker retires it through the epoch
		// path at the boundary: a segment the migrator drained is exactly the
		// segment a future off-owner reader (a parked cold read, a cross-shard
		// hop) could still name, so its bytes must outlive the bracket in flight,
		// not free the instant the compactor next runs.
		if si, ok := s.arena.segOf(addr); ok && s.arena.fullyDead(si) {
			s.markDrained(si)
		}
		n++
	}
	return n
}

// ColdWriteAt is the pwrite seam the shard's I/O worker calls off the owner: it
// writes a staged drain buffer at its reserved cold-region offset. It touches
// only the cold file handle (a positioned pwrite, safe alongside the owner's
// positioned preads), never the owner-local tail the reservation already
// advanced, so it honors the I/O worker's four-op contract. A store with no cold
// region cannot have staged a drain, so the nil guard only ever sees a wired
// seam.
func (s *Store) ColdWriteAt(off int64, b []byte) (int, error) {
	if s.cold == nil {
		return 0, errLogBroken
	}
	return s.cold.writeAt(off, b)
}
