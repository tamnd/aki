package store

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
)

// The string surface (spec 2064/f3/09): the point commands' storage half,
// over the value bands bands.go defines. TTL is the inline expiry slot with
// lazy delete-on-touch; every read path funnels through findLive so an
// expired record is reaped by the first touch that sees it.

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
	// A cold entry carries no expiry (the migrator demotes only TTL-free
	// records this slice) and its address is a cold-frame offset, so the arena
	// expiry read is both unnecessary and wrong for it. addr is the cold offset;
	// the caller serves it through the frame or brings it up.
	if addr != 0 && now != 0 && !slotCold(*slot) {
		if at := s.expireAt(addr); at != 0 && at <= now {
			s.deleteAt(h, slot, inOverflow)
			s.dropRecord(addr)
			s.count--
			return nil, 0, false
		}
	}
	return slot, addr, inOverflow
}

// findResident is findLive with cold bring-up: a cold hit is promoted back to a
// resident record so the returned address is always an arena address. Every
// path that dereferences the arena directly (the write commands, and the read
// commands this slice does not yet serve from the frame) goes through it, so a
// cold key is never handed to a caller as a raw arena offset. A resident hit
// costs one extra tier-bit test.
func (s *Store) findResident(h uint64, key []byte, now int64) (slot *uint64, addr uint64, inOverflow bool) {
	slot, addr, inOverflow = s.findLive(h, key, now)
	if addr != 0 && slotCold(*slot) {
		addr = s.bringUp(h, slot, addr)
	} else if addr != 0 && s.migrating != 0 {
		// A resident record found for a write that is also staged in an in-flight
		// cold drain: cancel its migration in place (coldstage.go). The write is
		// about to change the record (in place at the same address, or by
		// republishing it), and phase 2 must not flip the stale frame it already
		// wrote. cancelMigrate bumps the record's version so phase 2's compare
		// misses and drops the frame. Guarded by the migrating counter, so a
		// store with no drain in flight never runs it.
		s.cancelMigrate(addr)
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
	off, ok := s.arenaAlloc(need)
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
// release the superseded record (its band count, its outside value bytes, its
// arena charge), or insert a new entry.
func (s *Store) publish(h uint64, slot *uint64, oldAddr, off uint64) {
	word := tagOf(h)<<tagShift | off
	s.noteNew(s.recFlags(off))
	if oldAddr != 0 {
		*slot = word
		s.dropRecord(oldAddr)
		return
	}
	s.insertEntry(h, word)
	s.count++
}

// GetString copies the value for key into dst (reusing its capacity) and
// reports presence, rendering an int cell back to its decimal text. A chunked
// value materializes whole here; the streaming read path is GetStream.
func (s *Store) GetString(key []byte, now int64, dst []byte) ([]byte, bool) {
	h := Hash(key)
	slot, addr, _ := s.findLive(h, key, now)
	if addr == 0 {
		return dst[:0], false
	}
	if slotCold(*slot) {
		// Served straight from the cold frame, one pread, no promotion: a read
		// keeps a cold key cold so the migrator's work is not undone by traffic
		// (the promotion doorkeeper on cold reads is a later slice).
		return s.coldRead(addr, dst)
	}
	s.touchSlot(slot)
	return s.readValue(addr, dst)
}

// readValue copies a live record's value into dst, whatever its band.
func (s *Store) readValue(addr uint64, dst []byte) ([]byte, bool) {
	vs := s.valueStart(addr)
	f := s.recFlags(addr)
	if f&flagInt != 0 {
		n := int64(binary.LittleEndian.Uint64(s.arena.buf[vs:]))
		return strconv.AppendInt(dst[:0], n, 10), true
	}
	if f&flagChunked != 0 {
		return s.readChunked(addr, dst)
	}
	if f&flagSep != 0 {
		return s.readSep(addr, dst)
	}
	return append(dst[:0], s.arena.buf[vs:vs+s.vlen(addr)]...), true
}

// readSep copies a separated record's run into dst: a view copy from the
// arena, or one pread from the log. A log read error reads as absent, the
// only answer a point read can give for bytes it cannot reach. Under a
// resident cap the read is also the residency policy's input: an arena run
// gets its visited bit, a log run goes through the promotion doorkeeper.
func (s *Store) readSep(addr uint64, dst []byte) ([]byte, bool) {
	vs := s.valueStart(addr)
	word, vlen, _ := s.readPtr(vs)
	if word&inLogBit != 0 {
		v, err := s.vlog.readInto(word&runAddrMask, int(vlen), dst)
		if err != nil {
			return dst[:0], false
		}
		s.logReads++ // a fact, not a policy: counted with residency off too
		if s.ltmOn {
			s.maybePromote(addr, vs, vlen, v)
		}
		return v, true
	}
	if s.ltmOn {
		s.touchResident(addr)
	}
	run := word & runAddrMask
	return append(dst[:0], s.arena.buf[run:run+uint64(vlen)]...), true
}

// Exists reports whether key holds a live record.
func (s *Store) Exists(key []byte, now int64) bool {
	_, addr, _ := s.findLive(Hash(key), key, now)
	return addr != 0
}

// StrLen reports the value's byte length (an int cell's digit count) and
// presence.
func (s *Store) StrLen(key []byte, now int64) (int64, bool) {
	slot, addr, _ := s.findLive(Hash(key), key, now)
	if addr == 0 {
		return 0, false
	}
	if slotCold(*slot) {
		return int64(s.coldVlen(addr)), true
	}
	s.touchSlot(slot)
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
	if slotCold(*slot) {
		// The frame is unreferenced once the slot clears; no pread, no arena
		// drop, its bytes fall to the cold region's later compaction.
		s.deleteAt(h, slot, inOverflow)
		s.dropColdEntry()
		s.count--
		return true
	}
	s.deleteAt(h, slot, inOverflow)
	s.dropRecord(addr)
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
	if len(key) > maxKey || len(val) > maxValueLen {
		return ErrTooBig
	}
	h := Hash(key)
	slot, addr, _ := s.findResident(h, key, now)

	at := expireAt
	if keepTTL && addr != 0 {
		at = s.expireAt(addr)
	}
	iv, isInt := ParseInt(val)

	if addr != 0 {
		// In place when the record holds its bytes itself (int or embedded; a
		// chunked record's full replace always republishes and re-selects the
		// band from scratch), the value fits the reserved capacity, and the
		// TTL layout is compatible: a record without a slot cannot take a
		// deadline, and a record with one keeps it for life (clearing writes
		// zero into the slot).
		f := s.recFlags(addr)
		hasSlot := f&flagHasTTL != 0
		need := uint64(len(val))
		if isInt {
			need = 8
		}
		if f&flagChunked == 0 && f&flagSep != 0 && !isInt &&
			len(val) > strInlineMax && len(val) < strChunkMin && (at == 0 || hasSlot) {
			// Separated over separated: the record's value area is the run
			// pointer either way, so the record itself is reused and only the
			// run is touched. In place when the new value fits the old run's
			// arena capacity; otherwise a fresh run replaces it and the old
			// bytes are charged dead where they sit (their segment, or the
			// value log). Without this path a sustained same-size overwrite at
			// separated sizes republished record and run on every SET and bled
			// the arena dry.
			vs := s.valueStart(addr)
			word, vlen, vcap := s.readPtr(vs)
			if word&inLogBit == 0 && uint64(len(val)) <= uint64(vcap) {
				run := word & runAddrMask
				copy(s.arena.buf[run:run+uint64(len(val))], val)
			} else {
				nw, nc, err := s.replaceSepRun(word, vlen, vcap, val)
				if err != nil {
					return err
				}
				word, vcap = nw, nc
			}
			s.writePtr(vs, word, uint32(len(val)), vcap)
			s.setRecFlags(addr, f&^flagRawSticky)
			s.setVlen(addr, uint32(len(val)))
			if hasSlot {
				s.setExpireAt(addr, at)
			}
			return nil
		}
		if f&(flagSep|flagChunked) == 0 && need <= s.vcapBytes(addr) && (at == 0 || hasSlot) {
			vs := s.valueStart(addr)
			nf := f &^ (flagInt | flagRawSticky)
			if isInt {
				binary.LittleEndian.PutUint64(s.arena.buf[vs:], uint64(iv))
				nf |= flagInt
			} else {
				copy(s.arena.buf[vs:vs+uint64(len(val))], val)
			}
			s.noteFlip(f, nf)
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
	switch {
	case isInt:
		flags |= flagInt
		vcapB = 8
	case len(val) >= strChunkMin:
		flags |= flagChunked
		vcapB = ptrSize
	case len(val) > strInlineMax:
		flags |= flagSep
		vcapB = ptrSize
	}
	if at != 0 {
		flags |= flagHasTTL
	}
	off, err := s.allocString(key, vcapB, flags, at)
	if err != nil {
		return err
	}
	vs := s.valueStart(off)
	switch {
	case isInt:
		binary.LittleEndian.PutUint64(s.arena.buf[vs:], uint64(iv))
	case flags&flagChunked != 0:
		dirOff, n, err := s.writeChunked(nil, 0, val, len(val))
		if err != nil {
			s.arena.unlink(off, s.recBytes(off))
			return err
		}
		s.writePtr(s.valueStart(off), dirOff, n, n)
		s.chunkBytes += uint64(len(val))
	case flags&flagSep != 0:
		word, vcap, err := s.writeRun(val, nil, 0)
		if err != nil {
			s.arena.unlink(off, s.recBytes(off))
			return err
		}
		s.writePtr(s.valueStart(off), word, uint32(len(val)), vcap)
	default:
		copy(s.arena.buf[vs:], val)
	}
	s.setVlen(off, uint32(len(val)))
	s.publish(h, slot, addr, off)
	return nil
}

// replaceSepRun places val as a separated record's new run and releases the
// old one, the full-replace half of the sep-over-sep SET path. A log-resident
// value being overwritten while the store sits past the demotion low-water
// mark goes straight back to the log (spillCold, one buffered append): the
// doc 09 section 8 placement rule, touched bytes hot or fresh-cold, bulk
// stays cold, and lab 17's verdict against the arena round trip. Everything
// else takes writeRun's usual placement. A broken log falls through to the
// arena path, which is the only placement left that can take the bytes.
func (s *Store) replaceSepRun(word uint64, vlen, vcap uint32, val []byte) (uint64, uint32, error) {
	if word&inLogBit != 0 && s.spillCold(align8(uint64(len(val)))) {
		if off, err := s.vlog.append(val); err == nil {
			s.dropRun(word, vlen, vcap)
			s.logRuns++
			// A log run is immutable, so its capacity is exactly its length.
			return inLogBit | off, uint32(len(val)), nil
		}
	}
	nw, nc, err := s.writeRun(val, nil, 0)
	if err != nil {
		return 0, 0, err
	}
	s.dropRun(word, vlen, vcap)
	return nw, nc, nil
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
	slot, addr, _ := s.findResident(h, key, now)
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
		// A separated or chunked value is over 1KiB by construction, far past
		// any integer's text, so it is not int-shaped without reading it.
		if f&(flagSep|flagChunked) != 0 {
			return 0, ErrNotInt
		}
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
	nf := (f &^ flagRawSticky) | flagInt
	s.noteFlip(f, nf)
	s.setRecFlags(addr, nf)
	s.setVlen(addr, decLen(n))
	return n, nil
}

// growCap is the doc 09 section 2 capacity policy for a growing embedded
// value: republish with the larger of the exact need and double the old
// reservation, and stop doubling at str_inline_max, past which the value
// moves to the separated band. The multiplier is the settled APPEND growth
// lab (doubling inside the embedded band, 1.5x in the separated band).
func growCap(newLen, oldCap uint64) uint64 {
	c := align8(newLen)
	d := 2 * oldCap
	if d > strInlineMax {
		d = strInlineMax
	}
	if d > c {
		c = d
	}
	return c
}

// growSepCap is the separated band's run growth: 1.5x the old run, floored at
// the exact need.
func growSepCap(newLen, oldCap uint64) uint64 {
	c := align8(newLen)
	if d := align8(oldCap + oldCap/2); d > c {
		c = d
	}
	return c
}

// materialize returns the record's value as text: the embedded bytes or an
// arena run directly, an int cell rendered into scratch, or a log run read
// into the store scratch. The view is valid until the next store write.
func (s *Store) materialize(addr uint64, scratch []byte) ([]byte, error) {
	vs := s.valueStart(addr)
	f := s.recFlags(addr)
	if f&flagInt != 0 {
		n := int64(binary.LittleEndian.Uint64(s.arena.buf[vs:]))
		return strconv.AppendInt(scratch[:0], n, 10), nil
	}
	if f&flagChunked != 0 {
		v, ok := s.readChunked(addr, s.vbuf)
		s.vbuf = v[:cap(v)][:0]
		if !ok {
			return nil, errChunkRead
		}
		return v, nil
	}
	if f&flagSep != 0 {
		word, vlen, _ := s.readPtr(vs)
		if word&inLogBit != 0 {
			v, err := s.vlog.readInto(word&runAddrMask, int(vlen), s.vbuf)
			s.vbuf = v[:cap(v)][:0]
			if err != nil {
				return nil, err
			}
			// Counted but never promoted: materialize serves a rewrite, and
			// the rewrite's own placement decides where the value lands.
			s.logReads++
			return v, nil
		}
		run := word & runAddrMask
		return s.arena.buf[run : run+uint64(vlen)], nil
	}
	return s.arena.buf[vs : vs+s.vlen(addr)], nil
}

// allocSep lays down a fresh separated record: header and key through
// allocString, the run of a then b (b may be nil) through writeRun, then the
// pointer in the value area. The record is unlinked again if the run fails,
// so a caller sees either a complete record or nothing.
func (s *Store) allocSep(key, a, b []byte, flags byte, at int64) (uint64, error) {
	off, err := s.allocString(key, ptrSize, flags|flagSep, at)
	if err != nil {
		return 0, err
	}
	word, vcap, err := s.writeRun(a, b, 0)
	if err != nil {
		s.arena.unlink(off, s.recBytes(off))
		return 0, err
	}
	n := uint32(len(a) + len(b))
	s.writePtr(s.valueStart(off), word, n, vcap)
	s.setVlen(off, n)
	return off, nil
}

// appendSep grows a separated record's run to old followed by add: in place
// when the run sits in the arena with capacity to spare, otherwise a fresh
// run under growSepCap and a pointer swap inside the unchanged record. The
// record never republishes because its value area is the pointer either way.
func (s *Store) appendSep(addr uint64, old, add []byte, newLen int) (int64, error) {
	vs := s.valueStart(addr)
	word, _, vcap := s.readPtr(vs)
	f := s.recFlags(addr)
	if word&inLogBit == 0 && uint64(newLen) <= uint64(vcap) {
		run := word & runAddrMask
		copy(s.arena.buf[run+uint64(len(old)):], add)
	} else {
		nw, nc, err := s.writeRun(old, add, growSepCap(uint64(newLen), uint64(vcap)))
		if err != nil {
			return 0, err
		}
		// Release the old run while the pointer still names it, then swap.
		s.dropValue(addr)
		word, vcap = nw, nc
	}
	s.writePtr(vs, word, uint32(newLen), vcap)
	s.setRecFlags(addr, f|flagRawSticky)
	s.setVlen(addr, uint32(newLen))
	return int64(newLen), nil
}

// Append concatenates add onto key's value, creating the key when absent,
// and returns the new length. In place when the result fits the reserved
// capacity; otherwise one republish under growCap, or a run swap under
// growSepCap once the value sits in the separated band. Any existing deadline
// rides along: APPEND modifies the value, it does not replace the key.
func (s *Store) Append(key, add []byte, now int64) (int64, error) {
	if len(key) == 0 {
		return 0, errEmptyKey
	}
	if len(key) > maxKey {
		return 0, ErrTooBig
	}
	h := Hash(key)
	slot, addr, _ := s.findResident(h, key, now)
	if addr == 0 {
		if len(add) > maxValueLen {
			return 0, ErrTooBig
		}
		// Create-on-miss is SET with the raw-sticky bit: zero headroom, the
		// first growth buys the slack.
		if len(add) >= strChunkMin {
			off, err := s.allocChunked(key, nil, 0, add, len(add), flagRawSticky, 0)
			if err != nil {
				return 0, err
			}
			s.publish(h, slot, 0, off)
			return int64(len(add)), nil
		}
		if len(add) > strInlineMax {
			off, err := s.allocSep(key, add, nil, flagRawSticky, 0)
			if err != nil {
				return 0, err
			}
			s.publish(h, slot, 0, off)
			return int64(len(add)), nil
		}
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
	if f&flagChunked != 0 {
		// Chunk-bounded: no materialize, the patch composes only the final
		// and the fresh chunks.
		oldLen := int(s.vlen(addr))
		newLen := oldLen + len(add)
		if newLen > maxValueLen {
			return 0, ErrTooBig
		}
		if err := s.updateChunked(addr, oldLen, add, oldLen, newLen); err != nil {
			return 0, err
		}
		return int64(newLen), nil
	}
	var scratch [20]byte
	old, err := s.materialize(addr, scratch[:])
	if err != nil {
		return 0, err
	}
	newLen := len(old) + len(add)
	if newLen > maxValueLen {
		return 0, ErrTooBig
	}
	if newLen >= strChunkMin {
		// The growth crosses the chunk threshold: the record republishes in
		// chunked form over old then add. old is under the threshold by
		// construction, so the materialized copy is bounded.
		at := s.expireAt(addr)
		flags := byte(flagRawSticky)
		if at != 0 {
			flags |= flagHasTTL
		}
		off, err := s.allocChunked(key, old, len(old), add, newLen, flags, at)
		if err != nil {
			return 0, err
		}
		s.publish(h, slot, addr, off)
		return int64(newLen), nil
	}
	if f&flagSep != 0 {
		return s.appendSep(addr, old, add, newLen)
	}
	vs := s.valueStart(addr)
	if uint64(newLen) <= s.vcapBytes(addr) {
		if f&flagInt != 0 {
			copy(s.arena.buf[vs:], old)
		}
		copy(s.arena.buf[vs+uint64(len(old)):], add)
		nf := (f &^ flagInt) | flagRawSticky
		s.noteFlip(f, nf)
		s.setRecFlags(addr, nf)
		s.setVlen(addr, uint32(newLen))
		return int64(newLen), nil
	}
	at := s.expireAt(addr)
	flags := byte(flagRawSticky)
	if at != 0 {
		flags |= flagHasTTL
	}
	if newLen > strInlineMax {
		// The growth crosses the embedded cap: the record leaves the band and
		// republishes in separated form.
		off, err := s.allocSep(key, old, add, flags, at)
		if err != nil {
			return 0, err
		}
		s.publish(h, slot, addr, off)
		return int64(newLen), nil
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
	if end > maxValueLen {
		return 0, ErrTooBig
	}
	h := Hash(key)
	slot, addr, _ := s.findResident(h, key, now)
	buf := s.arena.buf
	if addr == 0 {
		if end >= strChunkMin {
			off, err := s.allocChunked(key, nil, offset, val, end, flagRawSticky, 0)
			if err != nil {
				return 0, err
			}
			s.publish(h, slot, 0, off)
			return int64(end), nil
		}
		if end > strInlineMax {
			nv := s.patchValue(nil, offset, val)
			off, err := s.allocSep(key, nv, nil, flagRawSticky, 0)
			if err != nil {
				return 0, err
			}
			s.publish(h, slot, 0, off)
			return int64(end), nil
		}
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
	if f&flagChunked != 0 {
		// Chunk-bounded: no materialize, only the chunks the patch range,
		// the gap fill, and any extension touch are rewritten.
		oldLen := int(s.vlen(addr))
		newLen := oldLen
		if end > newLen {
			newLen = end
		}
		if err := s.updateChunked(addr, offset, val, oldLen, newLen); err != nil {
			return 0, err
		}
		return int64(newLen), nil
	}
	var scratch [20]byte
	old, err := s.materialize(addr, scratch[:])
	if err != nil {
		return 0, err
	}
	newLen := len(old)
	if end > newLen {
		newLen = end
	}
	if newLen >= strChunkMin {
		// The write crosses the chunk threshold: the record republishes in
		// chunked form over the patched bytes. old is under the threshold by
		// construction, so the materialized copy is bounded.
		at := s.expireAt(addr)
		flags := byte(flagRawSticky)
		if at != 0 {
			flags |= flagHasTTL
		}
		off, err := s.allocChunked(key, old, offset, val, newLen, flags, at)
		if err != nil {
			return 0, err
		}
		s.publish(h, slot, addr, off)
		return int64(newLen), nil
	}
	if f&flagSep != 0 {
		return s.setRangeSep(addr, old, offset, val, newLen)
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
		nf := (f &^ flagInt) | flagRawSticky
		s.noteFlip(f, nf)
		s.setRecFlags(addr, nf)
		s.setVlen(addr, uint32(newLen))
		return int64(newLen), nil
	}
	at := s.expireAt(addr)
	flags := byte(flagRawSticky)
	if at != 0 {
		flags |= flagHasTTL
	}
	if newLen > strInlineMax {
		// The write crosses the embedded cap: the record leaves the band and
		// republishes in separated form over the patched bytes.
		nv := s.patchValue(old, offset, val)
		off, err := s.allocSep(key, nv, nil, flags, at)
		if err != nil {
			return 0, err
		}
		s.publish(h, slot, addr, off)
		return int64(newLen), nil
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

// patchValue builds old with val overwritten at offset, zero-filling any gap
// past old's end, in the store scratch. old may itself be the scratch (a
// log-resident materialize): the prefix copy is then the identity and the
// patch lands over it either way. The view is valid until the next store
// write.
func (s *Store) patchValue(old []byte, offset int, val []byte) []byte {
	newLen := offset + len(val)
	if len(old) > newLen {
		newLen = len(old)
	}
	nv := s.vbuf
	if cap(nv) < newLen {
		nv = make([]byte, newLen)
	}
	nv = nv[:newLen]
	n := copy(nv, old)
	if offset > n {
		clear(nv[n:offset])
	}
	copy(nv[offset:], val)
	s.vbuf = nv[:0]
	return nv
}

// setRangeSep patches a separated record's run: in place when the run sits in
// the arena (an arena run is mutable and the write fits under its capacity
// whenever end does not grow past it), otherwise a fresh run over the patched
// bytes and a pointer swap, since a log run is immutable.
func (s *Store) setRangeSep(addr uint64, old []byte, offset int, val []byte, newLen int) (int64, error) {
	vs := s.valueStart(addr)
	word, _, vcap := s.readPtr(vs)
	f := s.recFlags(addr)
	if word&inLogBit == 0 && uint64(newLen) <= uint64(vcap) {
		run := word & runAddrMask
		if offset > len(old) {
			clear(s.arena.buf[run+uint64(len(old)) : run+uint64(offset)])
		}
		copy(s.arena.buf[run+uint64(offset):], val)
	} else {
		nv := s.patchValue(old, offset, val)
		nw, nc, err := s.writeRun(nv, nil, growSepCap(uint64(newLen), uint64(vcap)))
		if err != nil {
			return 0, err
		}
		s.dropValue(addr)
		word, vcap = nw, nc
	}
	s.writePtr(vs, word, uint32(newLen), vcap)
	s.setRecFlags(addr, f|flagRawSticky)
	s.setVlen(addr, uint32(newLen))
	return int64(newLen), nil
}
