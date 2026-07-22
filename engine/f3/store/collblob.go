package store

import "encoding/binary"

// Inline collection records (spec 2064/f3, keyspace-unification arc, slice 1).
//
// A tiny collection today costs three separate Go-heap objects plus map
// overhead: the per-type header struct, its packed data slice, and the
// registry map entry with its copied key. That is the memory-bar wall on the
// 2M-tiny-collection gate cell, where every one of those objects is GC-scanned
// and pointer-chased. The fix is to store a tiny collection's whole packed blob
// where a small string value already lives: inline in an arena record's value
// area, discriminated by a collection kind byte. The three heap objects and the
// map entry collapse to one off-heap record with zero GC-scanned objects.
//
// This slice adds the storage primitives only, behind no caller: put, get, and
// the inline-cap bound. The types keep their Go-heap registries until the
// per-type routing slices (set first) wire tiny collections onto this path.
// Durability logging is deliberately not wired here; it lands with the routing
// slice that gives a collection blob a recoverable frame, so the primitives
// stay a pure in-arena memory lever this slice can measure in isolation.

// collInlineMax is the largest collection blob the arena embeds inline. It is
// the value-capacity field's own ceiling: offVcap counts 8-byte words in
// sixteen bits, so a record's value area reaches almost 512KiB, orders past any
// tiny collection the inline form targets. A blob larger than this is the
// caller's signal to keep the collection in its promoted, large form; the
// per-type count caps (a 512-entry intset, a 128-entry listpack) trip long
// before the byte ceiling does.
const collInlineMax = int(maxVal) * 8

// isCollKind reports whether kind is one of the inline collection record kinds.
// The kinds occupy a contiguous range, so this is a bounds test.
func isCollKind(k byte) bool { return k >= kindSet && k <= kindStream }

// PutCollBlob stores blob under key as an inline collection record of the given
// kind, with blind-upsert semantics: an existing record at key of any kind is
// superseded. bits is the caller's per-collection small field, stashed in the
// header's otherwise-free offKindBits word (a set's encoding-and-count, a hash's
// entry count): sixteen bits the key's header line already pays for. at is the
// absolute unix-ms deadline (0 for none); now stamps the fresh record's arena
// residency clock.
//
// The blob is embedded: this path owns no separated or chunked band, so the
// whole collection stays in one arena record with no outside bytes and no
// GC-scanned object. In place when the record at key already embeds its bytes,
// the blob fits the reserved capacity, and the TTL layout is compatible (a
// slotless record cannot take a deadline); otherwise one fresh record is
// published with a capacity rounded to the blob. A blob past collInlineMax is
// refused with ErrTooBig, the caller's cue to hold the collection in its
// promoted form.
func (s *Store) PutCollBlob(key []byte, kind byte, bits uint16, blob []byte, at, now int64) error {
	if len(key) == 0 {
		return errEmptyKey
	}
	if len(key) > maxKey {
		return ErrTooBig
	}
	if len(blob) > collInlineMax {
		return ErrTooBig
	}
	h := Hash(key)
	slot, addr, _ := s.findResident(h, key, now)

	if addr != 0 {
		f := s.recFlags(addr)
		hasSlot := f&flagHasTTL != 0
		if f&(flagSep|flagChunked) == 0 && uint64(len(blob)) <= s.vcapBytes(addr) && (at == 0 || hasSlot) {
			nf := f &^ (flagInt | flagRawSticky)
			vs := s.valueStart(addr)
			// The in-place path rewrites the record without touching the index, so it
			// bypasses publish and dropRecord and owns its own coll accounting. A
			// string record flipped to a collection joins the coll subset; a
			// collection rewritten in place is already counted. kind is always a coll
			// kind here, so no leave case exists.
			if !isCollKind(s.arena.buf[addr+offKind]) {
				s.collCount++
			}
			copy(s.arena.buf[vs:vs+uint64(len(blob))], blob)
			s.arena.buf[addr+offKind] = kind
			binary.LittleEndian.PutUint16(s.arena.buf[addr+offKindBits:], bits)
			s.noteFlip(f, nf)
			s.setRecFlags(addr, nf)
			s.setVlen(addr, uint32(len(blob)))
			if hasSlot {
				s.setExpireAt(addr, at)
			}
			return nil
		}
	}

	var flags byte
	if at != 0 {
		flags |= flagHasTTL
	}
	off, err := s.allocString(key, align8(uint64(len(blob))), flags, at, now)
	if err != nil {
		return err
	}
	s.arena.buf[off+offKind] = kind
	binary.LittleEndian.PutUint16(s.arena.buf[off+offKindBits:], bits)
	copy(s.arena.buf[s.valueStart(off):], blob)
	s.setVlen(off, uint32(len(blob)))
	s.publish(h, slot, addr, off)
	return nil
}

// DropCollBlob removes the inline collection record at key and reports whether
// one was there to remove. It is the collection type's explicit delete, distinct
// from store.Del in two ways the unified keyspace needs. First it cuts no string
// tombstone: a collection's durability rides its own type effect log (the type
// logs the delete there), so a string tombstone here would be a stray entry the
// string recovery path replays for a key that never held a string. Second it
// drops only a collection record, never a string: a string at key reads as no
// collection to remove, so a mis-routed call cannot delete a string value out
// from under its own keyspace. It reaps unconditionally (the caller resolved the
// record live at this command boundary), so it takes no clock; the record's
// coll-count charge is cleared by the shared dropRecord exit. A cold record
// cannot hold an inline collection, so a cold slot at key reads as absent here.
func (s *Store) DropCollBlob(key []byte) bool {
	h := Hash(key)
	slot, addr, inOverflow := s.findEntry(h, key)
	if addr == 0 || slotCold(*slot) {
		return false
	}
	if !isCollKind(s.arena.buf[addr+offKind]) {
		return false
	}
	s.deleteAt(h, slot, inOverflow)
	s.dropRecord(addr)
	s.count--
	return true
}

// GetCollBlob returns the packed blob, kind, and per-collection bits for key's
// inline collection record, and whether one is present. The blob is a view into
// the arena, stable until the next store write that could republish the record,
// so a caller that must keep the bytes copies them. A key that holds a
// non-collection record (a string) reads as absent here, the same answer a
// wrong-type collection read gives; the lazy-expiry rule applies as it does to
// any touch, so an expired key reads as absent. The read stamps the record's
// heat bit, the access the tier clock counts.
func (s *Store) GetCollBlob(key []byte, now int64) (blob []byte, kind byte, bits uint16, ok bool) {
	slot, addr, _ := s.findLive(Hash(key), key, now)
	if addr == 0 {
		return nil, 0, 0, false
	}
	if slotCold(*slot) {
		// A collection never demotes to the cold tier in the inline form, so a
		// cold slot at this key is a string record sharing the keyspace, not a
		// collection: absent for a collection read.
		return nil, 0, 0, false
	}
	k := s.arena.buf[addr+offKind]
	if !isCollKind(k) {
		return nil, 0, 0, false
	}
	s.touchSlot(slot)
	vs := s.valueStart(addr)
	blob = s.arena.buf[vs : vs+s.vlen(addr)]
	bits = binary.LittleEndian.Uint16(s.arena.buf[addr+offKindBits:])
	return blob, k, bits, true
}

// CollKind reports the collection kind stored at key and whether key holds an
// inline collection record at all, the type discriminant the unified keyspace
// answers TYPE and WRONGTYPE from without materializing the blob.
func (s *Store) CollKind(key []byte, now int64) (byte, bool) {
	slot, addr, _ := s.findLive(Hash(key), key, now)
	if addr == 0 || slotCold(*slot) {
		return 0, false
	}
	k := s.arena.buf[addr+offKind]
	if !isCollKind(k) {
		return 0, false
	}
	return k, true
}

// PeekCollBlob returns the packed blob, kind, per-collection bits, and deadline
// for key's inline collection record without reaping an expired one and without
// stamping the access clock: the NOTOUCH, no-reap resolve the routing layer's
// probe path needs. GetCollBlob reaps a past-deadline record silently inside the
// index touch, which would rob the collection type of the chance to publish its
// own ordered expired keyspace event; this returns the deadline instead and
// leaves the record in place, so the caller decides expiry, fires the event, and
// deletes (store.Delete, which skips the reap at now==0). A key holding a string
// or a cold record reads as absent, the same answer a wrong-type read gives. The
// blob is a view into the arena, valid until the next write that could republish
// the record.
func (s *Store) PeekCollBlob(key []byte) (blob []byte, kind byte, bits uint16, at int64, ok bool) {
	slot, addr, _ := s.findEntry(Hash(key), key)
	if addr == 0 || slotCold(*slot) {
		return nil, 0, 0, 0, false
	}
	k := s.arena.buf[addr+offKind]
	if !isCollKind(k) {
		return nil, 0, 0, 0, false
	}
	vs := s.valueStart(addr)
	blob = s.arena.buf[vs : vs+s.vlen(addr)]
	bits = binary.LittleEndian.Uint16(s.arena.buf[addr+offKindBits:])
	at = s.expireAt(addr)
	return blob, k, bits, at, true
}

// SetCollBits overwrites the per-collection bits word of key's inline collection
// record in place, without rewriting the blob or moving the record, and reports
// whether an inline collection record was there to write. It is the read-touch
// clock stamp mechanism: a collection's idle clock rides the high bits of this
// word (the low bit is the encoding discriminant), so a read that must stamp the
// clock reads the record, composes the new bits, and writes them back here with
// no value copy and no reallocation, the same two-byte header poke stampClock
// makes for a string. The caller supplies the whole word; the collection type
// owns the bit layout. A string or cold record, or a missing key, writes nothing
// and reports false. It does not reap an expired record, so a stamp on a
// past-deadline record is a harmless no-op the caller has already ruled out.
func (s *Store) SetCollBits(key []byte, bits uint16) bool {
	slot, addr, _ := s.findEntry(Hash(key), key)
	if addr == 0 || slotCold(*slot) {
		return false
	}
	if !isCollKind(s.arena.buf[addr+offKind]) {
		return false
	}
	binary.LittleEndian.PutUint16(s.arena.buf[addr+offKindBits:], bits)
	return true
}

// RangeCollKind calls fn with every live key holding an inline collection record
// of the given kind, the per-type arm of the unified KEYS and SCAN walk once a
// collection lives in the arena rather than a Go-heap registry. It walks the
// index directly the way RangeKeys does, skipping empty and cold slots and any
// record of another kind, and skips a record whose deadline has passed when now
// is non-zero, matching the lazy-expiry rule every read follows. It mutates
// nothing: no visited bit, no reap, so it is perf-neutral and leaves the
// residency clock untouched. fn returns false to stop the walk early. The key
// slice fn receives aliases the arena and is valid only for that call.
func (s *Store) RangeCollKind(kind byte, now int64, fn func(key []byte) bool) {
	for _, seg := range s.idx.segs {
		if seg == nil {
			continue
		}
		for i := range seg.buckets {
			if !s.rangeCollBucket(&seg.buckets[i], kind, now, fn) {
				return
			}
		}
		for i := range seg.overflow {
			if !s.rangeCollBucket(&seg.overflow[i], kind, now, fn) {
				return
			}
		}
	}
}

// rangeCollBucket hands fn every live key in one bucket whose record is an inline
// collection of kind. A cold slot cannot hold an inline collection (a tiny
// collection never demotes), so it is skipped without a frame read.
func (s *Store) rangeCollBucket(b *bucket, kind byte, now int64, fn func(key []byte) bool) bool {
	for i := 0; i < slotsPerBucket; i++ {
		w := b.slots[i]
		if w == 0 || slotCold(w) {
			continue
		}
		addr := w & addrMask
		if s.arena.buf[addr+offKind] != kind {
			continue
		}
		if now != 0 {
			if at := s.expireAt(addr); at != 0 && at <= now {
				continue
			}
		}
		if !fn(s.keyAt(addr)) {
			return false
		}
	}
	return true
}

// CountCollKind returns how many inline collection records of the given kind the
// store holds, and how many of those carry a deadline: the per-type arms of
// DBSIZE (total) and INFO's Keyspace expires field (withTTL). One index walk
// serves both. It counts a record whether or not its deadline has passed,
// matching the map-size basis of the count the Go-heap registry kept (a
// lazily-expired-but-unreaped key still shows until a keyed read drops it), so it
// takes no clock. It mutates nothing and runs on the owner goroutine at a command
// boundary, off every command's critical path.
func (s *Store) CountCollKind(kind byte) (total, withTTL uint64) {
	for _, seg := range s.idx.segs {
		if seg == nil {
			continue
		}
		count := func(b *bucket) {
			for i := 0; i < slotsPerBucket; i++ {
				w := b.slots[i]
				if w == 0 || slotCold(w) {
					continue
				}
				addr := w & addrMask
				if s.arena.buf[addr+offKind] != kind {
					continue
				}
				total++
				if s.expireAt(addr) != 0 {
					withTTL++
				}
			}
		}
		for i := range seg.buckets {
			count(&seg.buckets[i])
		}
		for i := range seg.overflow {
			count(&seg.overflow[i])
		}
	}
	return total, withTTL
}
