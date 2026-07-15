package store

import (
	"encoding/binary"
	"math/bits"
	"strconv"
)

// The bit kernel (spec 2064/f3/15 section 3): BITCOUNT and BITPOS run over the
// same chunk-streamed byte range, word-at-a-time. Go has no stable SIMD, so the
// strategy is math/bits.OnesCount64 (one POPCNT) with an eight-way unroll and
// four accumulators to keep the superscalar core fed, and a word scan with
// LeadingZeros for BITPOS. The range walk reads exactly the chunks a range
// overlaps and holds one chunk resident at a time, never a whole-value copy.

// popcountBytes counts the set bits in b, eight independent OnesCount64 chains
// over four accumulators so POPCNT's single-port throughput is not the ceiling,
// then a word tail and a byte tail.
func popcountBytes(b []byte) int {
	var n0, n1, n2, n3 int
	i := 0
	for ; i+64 <= len(b); i += 64 {
		n0 += bits.OnesCount64(binary.LittleEndian.Uint64(b[i:])) + bits.OnesCount64(binary.LittleEndian.Uint64(b[i+8:]))
		n1 += bits.OnesCount64(binary.LittleEndian.Uint64(b[i+16:])) + bits.OnesCount64(binary.LittleEndian.Uint64(b[i+24:]))
		n2 += bits.OnesCount64(binary.LittleEndian.Uint64(b[i+32:])) + bits.OnesCount64(binary.LittleEndian.Uint64(b[i+40:]))
		n3 += bits.OnesCount64(binary.LittleEndian.Uint64(b[i+48:])) + bits.OnesCount64(binary.LittleEndian.Uint64(b[i+56:]))
	}
	for ; i+8 <= len(b); i += 8 {
		n0 += bits.OnesCount64(binary.LittleEndian.Uint64(b[i:]))
	}
	for ; i < len(b); i++ {
		n0 += bits.OnesCount8(b[i])
	}
	return n0 + n1 + n2 + n3
}

// firstBitInByte returns the MSB-first position (0..7) of the first bit equal to
// want in v, or -1 if none. Bit 0 is the MSB, matching the 2.1 wire contract.
func firstBitInByte(v byte, want int) int {
	if want == 1 {
		if v == 0 {
			return -1
		}
		return bits.LeadingZeros8(v)
	}
	if v == 0xFF {
		return -1
	}
	return bits.LeadingZeros8(^v)
}

// zeros returns a read-only zeroed slice of length n (n <= strChunkSize), the
// shared hole chunk the range walk yields for an all-zero chunk.
func (s *Store) zeros(n int64) []byte {
	if s.zbuf == nil {
		s.zbuf = make([]byte, strChunkSize)
	}
	return s.zbuf[:n]
}

// eachByteRange calls fn with read-only byte segments covering the value's byte
// range [lo, hi] (inclusive, both in range) in ascending order: one segment per
// overlapping chunk for a chunked value, one segment otherwise. A hole yields a
// zeroed segment. fn returns true to stop early. Segments alias store memory or
// a scratch buffer and must not be retained past the callback.
func (s *Store) eachByteRange(addr uint64, lo, hi int64, fn func(base int64, seg []byte) bool) {
	f := s.recFlags(addr)
	vs := s.valueStart(addr)
	if f&flagChunked != 0 {
		word, n, _ := s.readPtr(vs)
		dirOff := word & runAddrMask
		for k := lo / strChunkSize; k < int64(n); k++ {
			cb := k * strChunkSize
			if cb > hi {
				return
			}
			w, l, _ := s.readPtr(dirOff + uint64(k)*ptrSize)
			a, b := lo, hi
			if cb > a {
				a = cb
			}
			if last := cb + int64(l) - 1; last < b {
				b = last
			}
			if a > b {
				continue
			}
			off := a - cb
			ln := b - a + 1
			var seg []byte
			switch {
			case w == 0:
				seg = s.zeros(ln)
			case w&inLogBit != 0:
				buf := s.stage()[:ln]
				if err := s.vlog.readFill((w&runAddrMask)+uint64(off), buf); err != nil {
					return
				}
				seg = buf
			default:
				run := w & runAddrMask
				seg = s.arena.buf[run+uint64(off) : run+uint64(off)+uint64(ln)]
			}
			if fn(a, seg) {
				return
			}
		}
		return
	}
	if f&flagInt != 0 {
		var scratch [20]byte
		v := strconv.AppendInt(scratch[:0], int64(binary.LittleEndian.Uint64(s.arena.buf[vs:])), 10)
		fn(lo, v[lo:hi+1])
		return
	}
	if f&flagSep != 0 {
		word, _, _ := s.readPtr(vs)
		if word&inLogBit != 0 {
			buf := s.stage()[:hi-lo+1]
			if err := s.vlog.readFill((word&runAddrMask)+uint64(lo), buf); err != nil {
				return
			}
			fn(lo, buf)
			return
		}
		run := word & runAddrMask
		fn(lo, s.arena.buf[run+uint64(lo):run+uint64(hi)+1])
		return
	}
	fn(lo, s.arena.buf[vs+uint64(lo):vs+uint64(hi)+1])
}

// BitCount answers BITCOUNT over the byte range [lo, hi], which the caller has
// resolved and clamped to 0 <= lo <= hi < length. firstMask is ANDed against
// byte lo and lastMask against byte hi so a BIT-unit range counts only its
// in-range boundary bits (both 0xFF for a BYTE range). It streams the interior
// through the word kernel and corrects the two boundary bytes from their single
// reads, so nothing outside the range is counted and the value is never
// materialized whole.
func (s *Store) BitCount(key []byte, lo, hi int64, firstMask, lastMask byte, now int64) int64 {
	_, addr, _ := s.findResident(Hash(key), key, now)
	if addr == 0 {
		return 0
	}
	if lo == hi {
		return int64(bits.OnesCount8(s.byteAt(addr, lo) & firstMask & lastMask))
	}
	var sum int64
	s.eachByteRange(addr, lo, hi, func(_ int64, seg []byte) bool {
		sum += int64(popcountBytes(seg))
		return false
	})
	// The interior was counted whole; drop the out-of-range bits in the two
	// boundary bytes a BIT-unit range excludes.
	sum -= int64(bits.OnesCount8(s.byteAt(addr, lo) &^ firstMask))
	sum -= int64(bits.OnesCount8(s.byteAt(addr, hi) &^ lastMask))
	return sum
}

// BitPos answers BITPOS: the absolute offset of the first bit equal to bit in
// the byte range [lo, hi] (resolved and clamped by the caller), or -1 when the
// range holds no such bit. firstMask and lastMask mark the in-range bits of the
// two boundary bytes for a BIT-unit range; out-of-range boundary bits are forced
// to the opposite value so they never match. The past-end semantics of a 0
// search are the caller's, not the range's.
func (s *Store) BitPos(key []byte, bit int, lo, hi int64, firstMask, lastMask byte, now int64) int64 {
	_, addr, _ := s.findResident(Hash(key), key, now)
	if addr == 0 {
		return -1
	}
	masked := firstMask != 0xFF || lastMask != 0xFF
	// skipWord is the word value that holds no match: all-zero when searching 1,
	// all-one when searching 0, so a whole interior word can be skipped at once.
	var skipWord uint64
	if bit == 0 {
		skipWord = ^uint64(0)
	}
	found := int64(-1)
	s.eachByteRange(addr, lo, hi, func(base int64, seg []byte) bool {
		i := 0
		for i < len(seg) {
			gi := base + int64(i)
			// Word fast-path over an interior window that touches neither masked
			// boundary byte: skip eight bytes when they hold no match.
			if i+8 <= len(seg) && (!masked || (gi > lo && gi+7 < hi)) {
				if binary.LittleEndian.Uint64(seg[i:]) == skipWord {
					i += 8
					continue
				}
			}
			v := seg[i]
			if masked {
				if gi == lo {
					if bit == 1 {
						v &= firstMask
					} else {
						v |= ^firstMask
					}
				}
				if gi == hi {
					if bit == 1 {
						v &= lastMask
					} else {
						v |= ^lastMask
					}
				}
			}
			if p := firstBitInByte(v, bit); p >= 0 {
				found = gi*8 + int64(p)
				return true
			}
			i++
		}
		return false
	})
	return found
}
