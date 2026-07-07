package f1raw

import "encoding/binary"

// This file adds the tier-tagged index and the cold record region, milestone M1 of the
// collection cold-record tiering plan (spec 2064/f1_rewrite_ltm/21). It builds on M0's
// reclaimable segmented arena and it is what lets a whole record, not just its value,
// leave RAM: a migrated record's header, key, and value move to an append-only region of
// the single file, and the record's one index entry flips to point at the cold frame.
//
// The design, from doc 21 sections 3, 4, and 9:
//
//   - There is one index, and bit 47 of an entry's 48-bit address is the tier bit (D1).
//     Clear means the low 47 bits are a resident arena offset, exactly as today; set means
//     they are an offset into the cold record region. A 47-bit offset addresses 128 TiB,
//     past any real arena or dataset, so nothing is lost by spending the top bit. The tag
//     (bits 48 to 63) is untouched, so the fast-reject probe is unchanged, and a cold read
//     stays one index probe plus one pread rather than the two probes a second cold index
//     would cost (the departure from F2 that keeps the resident 2x intact).
//
//   - The cold region holds whole framed records (D2), not bare values, so a collection
//     element whose identity is its key can spill. A frame is
//     [u32 total | u8 kind | u8 flags | u16 klen | u32 vlen | key | value]; total makes a
//     scan step record to record without the index, which recovery and compaction need.
//     When flagSep is set in the frame, the "value" bytes are the 12-byte cold value
//     pointer, for a record whose value had already separated to the value log before the
//     record migrated (D6), so a cold read of it is two preads. The common collection case
//     (small member, empty or small value) is one pread of the frame.
//
//   - The record region reuses the coldLog append-and-pread machinery (D5): it is a second
//     append-only log under the store's directory, with the same random-read advise and the
//     same per-read DONTNEED that hold the resident footprint at index-plus-keys. The
//     single-file merge of the value and record regions is a later, format-only change.
//
// M1 wires the read path only: find matches a cold entry by reading its frame, and Get and
// GetKind resolve a cold entry's value through the region. The background migrator that
// drives records cold under memory pressure (D14 to D17) is M3; here MigrateToCold is the
// test and introspection hook that moves one named record, so the read path can be proven
// before the pressure-driven mover exists. Nothing migrates on its own, so a store that
// never calls MigrateToCold has no cold entries and every path runs exactly as before.

// tierBit is bit 47 of an index entry's 48-bit address, the D1 tier tag. Set means the low
// 47 bits address the cold record region; clear means they are a resident arena offset. It
// is carved out of the address field, below the tag, so tagOf and the tag reject are
// unchanged.
const tierBit = uint64(1) << 47

// frameHdrSize is the fixed prefix of a cold record frame: total u32, kind u8, flags u8,
// klen u16, vlen u32. The key and value bytes follow it. klen fits u16 and vlen fits u32
// because maxKey and maxVal are both 0xffff, and a separated frame's vlen is ptrSize.
const frameHdrSize = 12

const (
	frameOffTotal = 0
	frameOffKind  = 4
	frameOffFlags = 5
	frameOffKlen  = 6
	frameOffVlen  = 8
)

// EnableColdRecords opens the cold record region at path and engages the tiering read
// path. It is separate from the cold value log (openColdLog / s.cold): the value log holds
// separated values, the record region holds whole migrated frames, and a store can run
// either, both, or neither. Like the value log it truncates any prior file, since durable
// reopen is a later milestone (D27). It must be called on a store before any record is
// migrated; the server calls it at startup, and the M1 test calls it directly.
func (s *Store) EnableColdRecords(path string) error {
	rl, err := openColdLog(path)
	if err != nil {
		return err
	}
	s.recs = rl
	return nil
}

// ColdRecords reports the cold record region's total and dead bytes, mirroring ColdBytes
// for the value log. A store with no record region reports zero for both. It is the
// accounting the record-region compactor (a later milestone) reads and the introspection
// path surfaces.
func (s *Store) ColdRecords() (total, dead uint64) {
	if s.recs == nil {
		return 0, 0
	}
	return s.recs.tail.Load(), s.recs.dead.Load()
}

// migrateRecordAt copies the resident record at arena offset off into the cold record
// region as one frame and returns the frame's region offset. It reads only immutable
// header fields plus the value: an inline value is copied under the seqlock so a concurrent
// in-place writer cannot tear it; a separated value's 12-byte cold pointer is immutable and
// copied verbatim, with flagSep carried into the frame so a later read knows to chase the
// pointer into the value log (D6). The caller flips the index entry to the returned offset
// with tierBit set; until it does, the frame is unreferenced dead space, so a failure here
// leaves the resident record authoritative.
func (s *Store) migrateRecordAt(off uint64) (uint64, error) {
	kind := s.arena[off+offKind]
	flags := s.arena[off+offFlags]
	klen := s.klen(off)
	kstart := off + hdrSize

	var valBuf []byte
	if flags&flagSep != 0 {
		// The value cell is the immutable 12-byte cold value pointer; carry it verbatim.
		vbase := off + hdrSize + align8(klen)
		valBuf = append(valBuf, s.arena[vbase:vbase+ptrSize]...)
	} else {
		valBuf = s.readValue(off, nil)
	}

	frame := make([]byte, frameHdrSize+int(klen)+len(valBuf))
	binary.LittleEndian.PutUint32(frame[frameOffTotal:], uint32(len(frame)))
	frame[frameOffKind] = kind
	frame[frameOffFlags] = flags
	binary.LittleEndian.PutUint16(frame[frameOffKlen:], uint16(klen))
	binary.LittleEndian.PutUint32(frame[frameOffVlen:], uint32(len(valBuf)))
	copy(frame[frameHdrSize:], s.arena[kstart:kstart+klen])
	copy(frame[frameHdrSize+int(klen):], valBuf)

	return s.recs.append(frame)
}

// MigrateToCold moves the record for key in the given kind namespace to the cold record
// region and repoints its index entry, returning true if a record was moved. It is the M1
// mover: the pressure-driven migrator (M3) will drive the same append-then-flip, but M1
// exposes it directly so the tiered read path is provable without the mover. It is a no-op
// (returns true) on a record already cold, and false on a missing key or a store with no
// record region. The entry flip is a CAS conditional on the observed word, so a same-key
// overwrite that raced the migration wins and the fresh cold frame is left as dead space.
// The old resident record's bytes become dead space the arena reclaims in a later
// milestone; M1 does not decrement the segment live counter, matching M0's deferral.
func (s *Store) MigrateToCold(key []byte, kind byte) bool {
	if s.recs == nil {
		return false
	}
	h := hash(key)
	for {
		off, b, slot, word, found := s.find(key, h, kind)
		if !found {
			return false
		}
		if off&tierBit != 0 {
			return true // already cold
		}
		frameOff, err := s.migrateRecordAt(off)
		if err != nil {
			return false
		}
		newWord := (word &^ addrMask) | frameOff | tierBit
		if b.slots[slot].CompareAndSwap(word, newWord) {
			// The record now lives cold and its resident bytes are dead space. Return them to
			// the source segment's live counter so the segment drains toward zero, the signal
			// the migrator retires it on (doc 21 section 6). off is a resident arena offset here
			// (the tier-bit branch above returned already), so unlinkResident charges the right
			// segment. This is the decrement M1 deferred; the pressure-driven migrator (M3)
			// relies on it to know when a drained segment is empty and safe to retire.
			s.unlinkResident(off)
			return true
		}
		// Lost the entry to a concurrent writer; the appended frame is now dead space.
		// Retry the probe against the new state.
	}
}

// coldFrameMatches reports whether the cold frame at region offset coldOff carries key in
// the given kind namespace. It reads the fixed frame header, rejects on kind or key length
// before touching the key bytes, then compares the key read from the frame (D5: the frame
// stores the full key so a tag collision is resolved against the real bytes). Both reads go
// through the region's pread path, so this is only reached on a tag hit, never on the
// fast-reject majority of probe slots.
func (s *Store) coldFrameMatches(coldOff uint64, key []byte, kind byte) bool {
	var hdr [frameHdrSize]byte
	if _, err := s.recs.readInto(coldOff, frameHdrSize, hdr[:]); err != nil {
		return false
	}
	if hdr[frameOffKind] != kind {
		return false
	}
	klen := binary.LittleEndian.Uint16(hdr[frameOffKlen:])
	if int(klen) != len(key) {
		return false
	}
	kbuf := make([]byte, klen)
	if _, err := s.recs.readInto(coldOff+frameHdrSize, int(klen), kbuf); err != nil {
		return false
	}
	return string(kbuf) == string(key)
}

// readColdValue resolves the value of the cold frame at region offset coldOff into dst. It
// reads the frame header for the value length and offset, then the value bytes. When the
// frame's flagSep is set the value bytes are the 12-byte cold value pointer, so a second
// pread against the value log serves the real value (D6); otherwise the value is inline in
// the frame and one pread of the region serves it. A cold frame is immutable, so no seqlock
// is needed on either read.
func (s *Store) readColdValue(coldOff uint64, dst []byte) ([]byte, bool) {
	var hdr [frameHdrSize]byte
	if _, err := s.recs.readInto(coldOff, frameHdrSize, hdr[:]); err != nil {
		return dst[:0], false
	}
	flags := hdr[frameOffFlags]
	klen := uint64(binary.LittleEndian.Uint16(hdr[frameOffKlen:]))
	vlen := int(binary.LittleEndian.Uint32(hdr[frameOffVlen:]))
	voff := coldOff + frameHdrSize + klen

	val, err := s.recs.readInto(voff, vlen, dst)
	if err != nil {
		return dst[:0], false
	}
	if flags&flagSep == 0 {
		return val, true
	}
	// Doubly-cold: the frame's value bytes are a 12-byte pointer into the value log.
	ptrOff := binary.LittleEndian.Uint64(val[0:])
	n := int(binary.LittleEndian.Uint32(val[8:]))
	v, err := s.cold.readInto(ptrOff, n, dst)
	if err != nil {
		return dst[:0], false
	}
	return v, true
}

// readValueByAddr resolves a value from a logical index address into dst, branching on the
// tier bit (D1, doc 21 section 9). The resident branch is exactly today's read: a separated
// record reads through the value log, an inline record reads under the seqlock. The cold
// branch preads the frame. Get and GetKind funnel through here so the tier check lands in
// one place. The resident branch is the predicted, almost-always-taken path, so the fast
// path pays one not-taken branch on tierBit and nothing else.
func (s *Store) readValueByAddr(off uint64, dst []byte) ([]byte, bool) {
	if off&tierBit != 0 {
		return s.readColdValue(off&^tierBit, dst)
	}
	if s.cold != nil && s.isSep(off) {
		return s.readSeparated(off, dst)
	}
	return s.readValue(off, dst), true
}
