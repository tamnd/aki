package store

import (
	"encoding/binary"
	"strconv"
)

// The cold data plane (spec 2064/f3/06 sections 2 and 7): the store-side read,
// write, and demotion path for keys the migrator moves out of the arena into
// the shard's cold region. The region itself is an append log identical in
// mechanism to the value log (append, pread, random-advise); what differs is
// its tenant. The value log holds value runs a resident record still points at,
// and CompactLog rewrites those runs forward. The cold region holds whole cold
// frames (coldframe.go) the index names directly through a cold-tier entry, so
// its liveness is Bitcask-style (the index names a frame or it is dead) and its
// compaction is the migrator's, not CompactLog's. Mixing the two would drop
// cold frames on the first value-log rewrite, which is why they are separate
// files.
//
// The resolver is the tier bit (hash.go): a resident entry reads the arena
// byte-for-byte on the hot path, a cold entry reads one frame from the cold
// region. This slice engages the string plane only, and only the two
// self-contained value bands: the V_INT cell and the embedded bytes. A demoted
// frame carries its whole value, so a cold read is one pread with no second hop
// and a cold key needs no arena bytes at all. The separated and chunked bands
// (whose value bytes live outside the record, in an arena run or the value log)
// keep their arena residency this slice; their cold forms and the promotion
// doorkeeper on cold reads are later M7 slices, the per-type engagement the
// milestone is named for. Until the shard migrator wires DemoteCold into a
// drain trigger (slice-1 PR 4b), cold entries are produced only by the
// store-level DemoteCold below, so the resident path pays nothing for the tier
// check beyond one already-loaded word.

// reserveColdNull burns the cold region's offset 0 so no live frame ever sits
// there. The store uses a zero arena address as its null sentinel (the arena
// hands nothing out at 0), and a cold-tier slot carries a region offset in the
// same address field, so a frame at offset 0 would read back as absent through
// every findLive caller. The reservation is one empty frame (kind 0, no key, no
// value): it is self-delimiting like any other, so a future linear recovery
// walk (doc 07) advances past it by its total and skips it as a zero-kind
// placeholder. Called on a fresh region and after Reset rewinds one.
func (s *Store) reserveColdNull() {
	if s.cold == nil || s.cold.tail != 0 {
		return
	}
	_, _ = s.cold.append(appendColdFrame(nil, 0, 0, 0, nil, nil))
}

// coldHeader preads a cold frame's fixed header at off and returns the frame's
// total byte length, its key length, its record flags, and its logical value
// length. One small pread; the caller reads the key or value with a second
// pread only when it needs the bytes.
func (s *Store) coldHeader(off uint64) (total, klen int, flags byte, vlen uint32, err error) {
	var h [coldHdr]byte
	if _, err = s.cold.readInto(off, coldHdr, h[:]); err != nil {
		return 0, 0, 0, 0, err
	}
	total = int(binary.LittleEndian.Uint32(h[0:]))
	flags = h[5]
	klen = int(binary.LittleEndian.Uint16(h[6:]))
	vlen = binary.LittleEndian.Uint32(h[8:])
	return total, klen, flags, vlen, nil
}

// coldKeyMatches reports whether the cold frame at off carries key, the
// tier-cold arm of findEntry's tag-collision check. A tag match already
// narrowed the field, so this runs only on the rare collision; it reads the
// header and the key in one pread into the compare scratch and compares the
// bytes. A read error reads as no match, the only safe answer for a frame the
// store cannot reach.
func (s *Store) coldKeyMatches(off uint64, key []byte) bool {
	n := coldHdr + len(key)
	buf, err := s.cold.readInto(off, n, s.coldBuf)
	s.coldBuf = buf[:cap(buf)][:0]
	if err != nil {
		return false
	}
	if int(binary.LittleEndian.Uint16(buf[6:])) != len(key) {
		return false
	}
	return string(buf[coldHdr:n]) == string(key)
}

// coldVlen reports a cold record's logical value length, the STRLEN answer for
// a cold key. One header pread, no value bytes.
func (s *Store) coldVlen(off uint64) uint32 {
	_, _, _, vlen, err := s.coldHeader(off)
	if err != nil {
		return 0
	}
	return vlen
}

// coldValue reads a cold frame's value into the frame scratch and returns it by
// band: the int cell rendered to decimal text, or the embedded bytes verbatim.
// The returned slice aliases the scratch and is valid until the next cold read.
// This slice never demotes a separated or chunked record, so no pointer band
// reaches here.
func (s *Store) coldValue(off uint64) ([]byte, bool) {
	total, klen, flags, _, err := s.coldHeader(off)
	if err != nil {
		return nil, false
	}
	vstart := off + coldHdr + uint64(klen)
	vlenBytes := total - coldHdr - klen
	buf, err := s.cold.readInto(vstart, vlenBytes, s.coldBuf)
	s.coldBuf = buf[:cap(buf)][:0]
	if err != nil {
		return nil, false
	}
	if flags&flagInt != 0 {
		n := int64(binary.LittleEndian.Uint64(buf))
		return strconv.AppendInt(s.coldBuf[:0], n, 10), true
	}
	return buf, true
}

// coldRead copies a cold record's value into dst, the GetString answer for a
// cold key. It goes through coldValue's scratch so the int render and the
// embedded bytes share one path, then copies into the caller's buffer.
func (s *Store) coldRead(off uint64, dst []byte) ([]byte, bool) {
	v, ok := s.coldValue(off)
	if !ok {
		return dst[:0], false
	}
	return append(dst[:0], v...), true
}

// demotable reports whether the resident record at addr is one this slice's
// migrator may move to the cold region: a plain string in the int or embedded
// band, with no deadline and no outside value bytes. A separated or chunked
// record's run would dangle once its segment frees, so those keep their arena
// residency until their cold forms land; a record with a TTL has no expiry
// field in the frame; a dead record is already leaving the index.
func (s *Store) demotable(addr uint64) bool {
	if s.arena.buf[addr+offKind] != kindString {
		return false
	}
	f := s.recFlags(addr)
	return f&(flagSep|flagChunked|flagHasTTL|flagDead) == 0
}

// demoteAt moves the resident record the entry word w names into the cold
// region and rewrites the slot to a cold-tier entry in place. The frame carries
// the whole record, so the arena bytes charge dead the moment the slot flips:
// the record's own value is self-contained (int or embedded), there are no
// outside bytes to release, and the tag and heat fields ride through untouched
// so no rehash is needed. It reports false when the cold append fails (a broken
// region), leaving the record resident. The caller guarantees demotable(addr).
func (s *Store) demoteAt(slot *uint64, w uint64) bool {
	addr := w & addrMask
	s.frameBuf = s.frameRecord(addr, s.frameBuf[:0])
	off, err := s.cold.append(s.frameBuf)
	if err != nil {
		return false
	}
	// Keep the tag, clear the address, tier, and heat fields, set tier cold and
	// the cold-region offset. Heat clears because a cold entry carries doorkeeper
	// state, not a visited bit, and a later bring-up re-earns its heat.
	*slot = (w &^ (addrMask | tierMask<<tierShift | heatMask<<heatShift)) | tierCold<<tierShift | off
	s.noteDrop(s.recFlags(addr))
	s.coldRecs++
	s.arena.unlink(addr, s.recBytes(addr))
	return true
}

// bringUp promotes the cold frame at off back to a resident record and rewrites
// the slot to a resident entry in place, returning the new arena address. It is
// the unconditional bring-up doc 06 section 7.3 mandates for a write to a cold
// key, and the safety fallback for any read path that dereferences the arena
// directly rather than serving the frame. The old frame is left unreferenced in
// the cold region for its eventual cold compaction (a later slice); nothing
// charges it dead here because cold liveness is decided by the index, not a
// counter. The reconstructed record keeps its band and its raw-sticky bit and
// drops the residency and dead marks.
func (s *Store) bringUp(h uint64, slot *uint64, off uint64) uint64 {
	total, klen, flags, vlen, err := s.coldHeader(off)
	if err != nil {
		return off
	}
	frame, err := s.cold.readInto(off, total, s.coldBuf)
	s.coldBuf = frame[:cap(frame)][:0]
	if err != nil {
		return off
	}
	key := frame[coldHdr : coldHdr+klen]
	value := frame[coldHdr+klen : total]
	nf := flags & (flagInt | flagRawSticky)

	var vcapB uint64 = 8
	if nf&flagInt == 0 {
		vcapB = align8(uint64(len(value)))
	}
	noff, ok := s.arenaAlloc(uint64(hdrSize) + align8(uint64(klen)) + vcapB)
	if !ok {
		// The arena cannot take it back; leave the key cold. The read fallback
		// still serves from the frame, and a write retries the bring-up at the
		// next call after a boundary reclaims space.
		return off
	}
	buf := s.arena.buf
	binary.LittleEndian.PutUint32(buf[noff+offVer:], 0)
	binary.LittleEndian.PutUint32(buf[noff+offVlen:], vlen)
	binary.LittleEndian.PutUint16(buf[noff+offKlen:], uint16(klen))
	binary.LittleEndian.PutUint16(buf[noff+offVcap:], uint16(vcapB/8))
	buf[noff+offKind] = kindString
	buf[noff+offFlags] = nf
	binary.LittleEndian.PutUint16(buf[noff+offKindBits:], 0)
	copy(buf[s.keyStart(noff):], key)
	copy(buf[s.valueStart(noff):], value)

	*slot = tagOf(h)<<tagShift | noff
	s.coldRecs--
	s.noteNew(nf)
	return noff
}

// promoteOnColdRead runs the cold-read doorkeeper (colddoor.go) for a value read
// that resolved to the cold frame at off. A first sighting marks the key and
// returns false, so the caller serves the frame and the key stays cold; a second
// sighting within the window promotes the frame back to the arena and returns
// true, so the caller reads the now-resident record. Promotion is skipped when
// the fill sits at the cap: a read never pushes past it, the boundary demotion
// owns crossing (the maybePromote rule). Writes never reach here; a write's
// bring-up is unconditional (findResident). The caller guarantees ltmOn.
func (s *Store) promoteOnColdRead(h uint64, slot *uint64, off uint64) bool {
	if s.door == nil {
		return false
	}
	if !s.door.test(h) {
		s.door.mark(h)
		return false
	}
	if s.spillNow(0) {
		return false
	}
	s.bringUp(h, slot, off)
	return !slotCold(*slot)
}

// dropColdEntry retires a cold key: the slot is already cleared by the caller,
// so the frame is unreferenced and its bytes fall to the cold region's eventual
// compaction. No pread and no dead charge, matching doc 06 section 7.3: cold
// liveness is the index's to decide, and a frame the index no longer names is
// dead by definition. Only the census counter moves.
func (s *Store) dropColdEntry() {
	s.coldRecs--
}

// DemoteCold moves every eligible resident string record into the cold region
// and reports how many it demoted. It is the store-level driver the slice-1
// tests use to force the cold plane before the shard migrator (PR 4b) wires
// demotion into a drain trigger; the walk is the CompactLog walk, each index
// segment visited once through the seen marks, buckets then overflow. A cold
// region that is nil or broken demotes nothing.
func (s *Store) DemoteCold() int {
	if s.cold == nil || s.cold.werr != nil {
		return 0
	}
	if cap(s.seen) < len(s.idx.segs) {
		s.seen = make([]bool, len(s.idx.segs))
	}
	seen := s.seen[:len(s.idx.segs)]
	for i := range seen {
		seen[i] = false
	}
	n := 0
	for _, ord := range s.idx.dir {
		if seen[ord] {
			continue
		}
		seen[ord] = true
		seg := s.idx.segs[ord]
		for bi := range seg.buckets {
			n += s.demoteBucketCold(&seg.buckets[bi])
		}
		for bi := range seg.overflow {
			n += s.demoteBucketCold(&seg.overflow[bi])
		}
	}
	return n
}

// demoteBucketCold demotes every eligible resident entry in one bucket and
// reports the count. A cold or non-demotable entry is left as it is.
func (s *Store) demoteBucketCold(b *bucket) int {
	n := 0
	for i := 0; i < slotsPerBucket; i++ {
		w := b.slots[i]
		if w == 0 || slotCold(w) {
			continue
		}
		if !s.demotable(w & addrMask) {
			continue
		}
		if s.demoteAt(&b.slots[i], w) {
			n++
		}
	}
	return n
}

// DemoteKey demotes one resident string key to the cold region and reports
// whether it moved, the targeted form the race and round-trip tests drive. It
// returns false when the key is absent, already cold, or not demotable, or when
// the cold append fails.
func (s *Store) DemoteKey(key []byte) bool {
	if s.cold == nil {
		return false
	}
	h := Hash(key)
	slot, addr, _ := s.findEntry(h, key)
	if slot == nil || slotCold(*slot) || !s.demotable(addr) {
		return false
	}
	return s.demoteAt(slot, *slot)
}

// ColdStats is the cold tier's evidence surface: the live cold-record count and
// the cold region's appended bytes. The live figure is the census the arena
// band counts exclude; the byte figure is what the region has written, the
// memory the cold plane keeps off the resident arena.
type ColdStats struct {
	Records    uint64
	RegionSize uint64
}

// Cold reports the cold tier counters. A store with no cold region reports
// zero.
func (s *Store) Cold() ColdStats {
	if s.cold == nil {
		return ColdStats{}
	}
	return ColdStats{Records: s.coldRecs, RegionSize: s.cold.tail}
}
