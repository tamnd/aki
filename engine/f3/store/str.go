package store

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
)

// The string surface (spec 2064/f3/09): the point commands' storage half.
// Two bands live here, the V_INT cell and the embedded bytes; the separated
// and chunked bands land with the value-bands slice, so the 64KiB embed cap
// is the hard value bound until then. TTL is the inline expiry slot with lazy
// delete-on-touch; every read path funnels through findLive so an expired
// record is reaped by the first touch that sees it.

// ErrNotInt is returned when arithmetic hits a value that is not a canonical
// integer.
var ErrNotInt = errors.New("store: value is not an integer")

// ErrOverflow is returned when an increment would leave int64.
var ErrOverflow = errors.New("store: increment overflow")

// ParseInt parses b as a canonical base-10 int64 under the string2ll
// discipline doc 09 section 7 pins: "0" alone is zero, otherwise an optional
// '-' then a first digit of 1 to 9; no '+', no leading zeros, no "-0", no
// surrounding space. Anything else is not int-shaped and stays text.
func ParseInt(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 20 {
		return 0, false
	}
	i := 0
	neg := false
	if b[0] == '-' {
		neg = true
		i = 1
		if len(b) == 1 {
			return 0, false
		}
	}
	if b[i] == '0' {
		if !neg && len(b) == 1 {
			return 0, true
		}
		return 0, false
	}
	var u uint64
	lim := uint64(math.MaxInt64)
	if neg {
		lim++
	}
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if u > (lim-d)/10 {
			return 0, false
		}
		u = u*10 + d
	}
	if neg {
		if u == lim {
			return math.MinInt64, true
		}
		return -int64(u), true
	}
	return int64(u), true
}

// decLen is the decimal rendering length of n, sign included: the vlen an int
// cell carries.
func decLen(n int64) uint32 {
	var u uint64
	var l uint32
	if n < 0 {
		l = 1
		u = uint64(^n) + 1
	} else {
		u = uint64(n)
	}
	for {
		l++
		u /= 10
		if u == 0 {
			return l
		}
	}
}

// Header field access, single-owner plain loads and stores throughout.

func (s *Store) recFlags(off uint64) byte { return s.arena.buf[off+offFlags] }

func (s *Store) setRecFlags(off uint64, f byte) { s.arena.buf[off+offFlags] = f }

func (s *Store) setVlen(off uint64, n uint32) {
	binary.LittleEndian.PutUint32(s.arena.buf[off+offVlen:], n)
}

// expireAt reads the record's absolute unix-ms expiry, 0 when the record has
// no slot or the slot holds no deadline.
func (s *Store) expireAt(off uint64) int64 {
	if s.recFlags(off)&flagHasTTL == 0 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(s.arena.buf[off+hdrSize:]))
}

// setExpireAt writes the expiry slot; the record must carry one.
func (s *Store) setExpireAt(off uint64, at int64) {
	binary.LittleEndian.PutUint64(s.arena.buf[off+hdrSize:], uint64(at))
}

// findLive is findEntry plus the doc 09 lazy-expiry rule: every read checks
// the deadline before serving, and the owner deletes an expired record on
// touch. now is the batch's cached clock; zero means no clock (the wrapper
// paths), which skips the check.
func (s *Store) findLive(h uint64, key []byte, now int64) (slot *uint64, addr uint64, inOverflow bool) {
	slot, addr, inOverflow = s.findEntry(h, key)
	if addr != 0 && now != 0 {
		if at := s.expireAt(addr); at != 0 && at <= now {
			s.deleteAt(h, slot, inOverflow)
			s.arena.unlink(addr, s.recBytes(addr))
			s.count--
			return nil, 0, false
		}
	}
	return slot, addr, inOverflow
}

// allocString lays down a fresh string record: header, expiry slot when the
// flags carry one, key. The value area is the caller's to write, along with
// vlen, before the record is published. Kind and flags are stored explicitly
// because arena bytes are reused unscrubbed.
func (s *Store) allocString(key []byte, vcapB uint64, flags byte, at int64) (uint64, error) {
	need := uint64(hdrSize) + align8(uint64(len(key))) + vcapB
	if flags&flagHasTTL != 0 {
		need += 8
	}
	off, ok := s.arena.allocRecord(need)
	if !ok {
		return 0, ErrFull
	}
	buf := s.arena.buf
	binary.LittleEndian.PutUint32(buf[off+offVer:], 0)
	binary.LittleEndian.PutUint32(buf[off+offVlen:], 0)
	binary.LittleEndian.PutUint16(buf[off+offKlen:], uint16(len(key)))
	binary.LittleEndian.PutUint16(buf[off+offVcap:], uint16(vcapB/8))
	buf[off+offKind] = kindString
	buf[off+offFlags] = flags
	binary.LittleEndian.PutUint16(buf[off+offKindBits:], 0)
	if flags&flagHasTTL != 0 {
		s.setExpireAt(off, at)
	}
	copy(buf[s.keyStart(off):], key)
	return off, nil
}

// publish points the index at a fresh record: repoint the existing slot and
// charge back the old bytes, or insert a new entry.
func (s *Store) publish(h uint64, slot *uint64, oldAddr, off uint64) {
	word := tagOf(h)<<tagShift | off
	if oldAddr != 0 {
		*slot = word
		s.arena.unlink(oldAddr, s.recBytes(oldAddr))
		return
	}
	s.insertEntry(h, word)
	s.count++
}

// GetString copies the value for key into dst (reusing its capacity) and
// reports presence, rendering an int cell back to its decimal text.
func (s *Store) GetString(key []byte, now int64, dst []byte) ([]byte, bool) {
	h := Hash(key)
	_, addr, _ := s.findLive(h, key, now)
	if addr == 0 {
		return dst[:0], false
	}
	vs := s.valueStart(addr)
	if s.recFlags(addr)&flagInt != 0 {
		n := int64(binary.LittleEndian.Uint64(s.arena.buf[vs:]))
		return strconv.AppendInt(dst[:0], n, 10), true
	}
	return append(dst[:0], s.arena.buf[vs:vs+s.vlen(addr)]...), true
}

// Exists reports whether key holds a live record.
func (s *Store) Exists(key []byte, now int64) bool {
	_, addr, _ := s.findLive(Hash(key), key, now)
	return addr != 0
}

// StrLen reports the value's byte length (an int cell's digit count) and
// presence.
func (s *Store) StrLen(key []byte, now int64) (int64, bool) {
	_, addr, _ := s.findLive(Hash(key), key, now)
	if addr == 0 {
		return 0, false
	}
	return int64(s.vlen(addr)), true
}

// Del removes key under the lazy-expiry rule: deleting an already expired
// record reports false, the same answer the client would get from any read.
func (s *Store) Del(key []byte, now int64) bool {
	h := Hash(key)
	slot, addr, inOverflow := s.findLive(h, key, now)
	if addr == 0 {
		return false
	}
	s.deleteAt(h, slot, inOverflow)
	s.arena.unlink(addr, s.recBytes(addr))
	s.count--
	return true
}

// SetString stores val under key with blind-upsert semantics and the doc 09
// band selection: canonical integer text takes the V_INT cell, anything else
// embeds. expireAt is the absolute unix-ms deadline (0: none); keepTTL
// carries an existing deadline through the write instead. A fresh record gets
// zero headroom (vcap = the value rounded to 8), the section 2 capacity
// policy: write-once keys never pay growth slack.
func (s *Store) SetString(key, val []byte, now, expireAt int64, keepTTL bool) error {
	if len(key) == 0 {
		return errEmptyKey
	}
	if len(key) > maxKey || len(val) > maxVal {
		return ErrTooBig
	}
	h := Hash(key)
	slot, addr, _ := s.findLive(h, key, now)

	at := expireAt
	if keepTTL && addr != 0 {
		at = s.expireAt(addr)
	}
	iv, isInt := ParseInt(val)

	if addr != 0 {
		// In place when the value fits the reserved capacity and the TTL
		// layout is compatible: a record without a slot cannot take a
		// deadline, and a record with one keeps it for life (clearing writes
		// zero into the slot).
		f := s.recFlags(addr)
		hasSlot := f&flagHasTTL != 0
		need := uint64(len(val))
		if isInt {
			need = 8
		}
		if need <= s.vcapBytes(addr) && (at == 0 || hasSlot) {
			vs := s.valueStart(addr)
			nf := f &^ (flagInt | flagRawSticky)
			if isInt {
				binary.LittleEndian.PutUint64(s.arena.buf[vs:], uint64(iv))
				nf |= flagInt
			} else {
				copy(s.arena.buf[vs:vs+uint64(len(val))], val)
			}
			s.setRecFlags(addr, nf)
			s.setVlen(addr, uint32(len(val)))
			if hasSlot {
				s.setExpireAt(addr, at)
			}
			return nil
		}
	}

	var flags byte
	vcapB := align8(uint64(len(val)))
	if isInt {
		flags |= flagInt
		vcapB = 8
	}
	if at != 0 {
		flags |= flagHasTTL
	}
	off, err := s.allocString(key, vcapB, flags, at)
	if err != nil {
		return err
	}
	vs := s.valueStart(off)
	if isInt {
		binary.LittleEndian.PutUint64(s.arena.buf[vs:], uint64(iv))
	} else {
		copy(s.arena.buf[vs:], val)
	}
	s.setVlen(off, uint32(len(val)))
	s.publish(h, slot, addr, off)
	return nil
}

// IncrBy adds delta to the integer at key, creating the key at delta when
// absent. The arithmetic runs on the raw int64 cell: a text record that
// parses canonically converts to the cell in place (any non-empty text has
// vcap of at least 8), and there is no string round-trip anywhere on the
// path (doc 09 section 7).
func (s *Store) IncrBy(key []byte, delta, now int64) (int64, error) {
	if len(key) == 0 {
		return 0, errEmptyKey
	}
	if len(key) > maxKey {
		return 0, ErrTooBig
	}
	h := Hash(key)
	slot, addr, _ := s.findLive(h, key, now)
	if addr == 0 {
		off, err := s.allocString(key, 8, flagInt, 0)
		if err != nil {
			return 0, err
		}
		binary.LittleEndian.PutUint64(s.arena.buf[s.valueStart(off):], uint64(delta))
		s.setVlen(off, decLen(delta))
		s.publish(h, slot, 0, off)
		return delta, nil
	}
	f := s.recFlags(addr)
	vs := s.valueStart(addr)
	var cur int64
	if f&flagInt != 0 {
		cur = int64(binary.LittleEndian.Uint64(s.arena.buf[vs:]))
	} else {
		var ok bool
		cur, ok = ParseInt(s.arena.buf[vs : vs+s.vlen(addr)])
		if !ok {
			return 0, ErrNotInt
		}
	}
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	n := cur + delta
	binary.LittleEndian.PutUint64(s.arena.buf[vs:], uint64(n))
	s.setRecFlags(addr, (f&^flagRawSticky)|flagInt)
	s.setVlen(addr, decLen(n))
	return n, nil
}

// growCap is the doc 09 section 2 capacity policy for a growing embedded
// value: republish with the larger of the exact need and double the old
// reservation. The multiplier is the settled APPEND growth lab (doubling
// inside the embedded band); the doc stops doubling at str_inline_max and
// moves the value to the separated band past it, but that band is a later
// slice, so until it lands doubling runs to the 64KiB embed cap.
func growCap(newLen, oldCap uint64) uint64 {
	const capMax = (maxVal + 7) &^ 7
	c := align8(newLen)
	d := 2 * oldCap
	if d > capMax {
		d = capMax
	}
	if d > c {
		c = d
	}
	return c
}

// materialize returns the record's value as text: the embedded bytes
// directly, or an int cell rendered into scratch.
func (s *Store) materialize(addr uint64, scratch []byte) []byte {
	vs := s.valueStart(addr)
	if s.recFlags(addr)&flagInt != 0 {
		n := int64(binary.LittleEndian.Uint64(s.arena.buf[vs:]))
		return strconv.AppendInt(scratch[:0], n, 10)
	}
	return s.arena.buf[vs : vs+s.vlen(addr)]
}

// Append concatenates add onto key's value, creating the key when absent,
// and returns the new length. In place when the result fits the reserved
// capacity; otherwise one republish under growCap. Any existing deadline
// rides along: APPEND modifies the value, it does not replace the key.
func (s *Store) Append(key, add []byte, now int64) (int64, error) {
	if len(key) == 0 {
		return 0, errEmptyKey
	}
	if len(key) > maxKey {
		return 0, ErrTooBig
	}
	h := Hash(key)
	slot, addr, _ := s.findLive(h, key, now)
	if addr == 0 {
		if len(add) > maxVal {
			return 0, ErrTooBig
		}
		// Create-on-miss is SET with the raw-sticky bit: zero headroom, the
		// first growth buys the slack.
		off, err := s.allocString(key, align8(uint64(len(add))), flagRawSticky, 0)
		if err != nil {
			return 0, err
		}
		copy(s.arena.buf[s.valueStart(off):], add)
		s.setVlen(off, uint32(len(add)))
		s.publish(h, slot, 0, off)
		return int64(len(add)), nil
	}
	f := s.recFlags(addr)
	var scratch [20]byte
	old := s.materialize(addr, scratch[:])
	newLen := len(old) + len(add)
	if newLen > maxVal {
		return 0, ErrTooBig
	}
	vs := s.valueStart(addr)
	if uint64(newLen) <= s.vcapBytes(addr) {
		if f&flagInt != 0 {
			copy(s.arena.buf[vs:], old)
		}
		copy(s.arena.buf[vs+uint64(len(old)):], add)
		s.setRecFlags(addr, (f&^flagInt)|flagRawSticky)
		s.setVlen(addr, uint32(newLen))
		return int64(newLen), nil
	}
	at := s.expireAt(addr)
	flags := byte(flagRawSticky)
	if at != 0 {
		flags |= flagHasTTL
	}
	off, err := s.allocString(key, growCap(uint64(newLen), s.vcapBytes(addr)), flags, at)
	if err != nil {
		return 0, err
	}
	nvs := s.valueStart(off)
	copy(s.arena.buf[nvs:], old)
	copy(s.arena.buf[nvs+uint64(len(old)):], add)
	s.setVlen(off, uint32(newLen))
	s.publish(h, slot, addr, off)
	return int64(newLen), nil
}

// SetRange overwrites the value at offset, zero-filling any gap between the
// old length and the offset, creating the key when absent, and returns the
// new length. The caller guarantees val is non-empty and offset non-negative
// (an empty SETRANGE is a pure length read that never writes). The gap fill
// is explicit in every path because arena bytes are reused unscrubbed. Like
// Append it keeps any existing deadline.
func (s *Store) SetRange(key []byte, offset int, val []byte, now int64) (int64, error) {
	if len(key) == 0 {
		return 0, errEmptyKey
	}
	if len(key) > maxKey {
		return 0, ErrTooBig
	}
	end := offset + len(val)
	if end > maxVal {
		return 0, ErrTooBig
	}
	h := Hash(key)
	slot, addr, _ := s.findLive(h, key, now)
	buf := s.arena.buf
	if addr == 0 {
		off, err := s.allocString(key, align8(uint64(end)), flagRawSticky, 0)
		if err != nil {
			return 0, err
		}
		vs := s.valueStart(off)
		clear(buf[vs : vs+uint64(offset)])
		copy(buf[vs+uint64(offset):], val)
		s.setVlen(off, uint32(end))
		s.publish(h, slot, 0, off)
		return int64(end), nil
	}
	f := s.recFlags(addr)
	var scratch [20]byte
	old := s.materialize(addr, scratch[:])
	newLen := len(old)
	if end > newLen {
		newLen = end
	}
	vs := s.valueStart(addr)
	if uint64(newLen) <= s.vcapBytes(addr) {
		if f&flagInt != 0 {
			copy(buf[vs:], old)
		}
		if offset > len(old) {
			clear(buf[vs+uint64(len(old)) : vs+uint64(offset)])
		}
		copy(buf[vs+uint64(offset):], val)
		s.setRecFlags(addr, (f&^flagInt)|flagRawSticky)
		s.setVlen(addr, uint32(newLen))
		return int64(newLen), nil
	}
	at := s.expireAt(addr)
	flags := byte(flagRawSticky)
	if at != 0 {
		flags |= flagHasTTL
	}
	off, err := s.allocString(key, growCap(uint64(newLen), s.vcapBytes(addr)), flags, at)
	if err != nil {
		return 0, err
	}
	nvs := s.valueStart(off)
	copy(buf[nvs:], old)
	if offset > len(old) {
		clear(buf[nvs+uint64(len(old)) : nvs+uint64(offset)])
	}
	copy(buf[nvs+uint64(offset):], val)
	s.setVlen(off, uint32(newLen))
	s.publish(h, slot, addr, off)
	return int64(newLen), nil
}
