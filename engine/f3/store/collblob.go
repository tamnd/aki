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
