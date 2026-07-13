package store

import (
	"encoding/binary"
	"strconv"
)

// The bit surface (spec 2064/f3/15 section 2): a bitmap is a string addressed
// at the bit level, no distinct value type. GETBIT reads one covering byte and
// SETBIT does a read-modify-write of one byte, both bounded to the chunk (or
// record) the bit lands in, never a whole-value materialize. The bit-to-byte
// contract is fixed by the wire: bit offset i lives in byte i>>3 at position
// 7-(i&7), MSB first, so bit 0 is the MSB of byte 0.

// GetBit answers GETBIT: the bit at bitOffset, 0 when the key is absent or the
// offset is at or past the value length. Only the covering byte is read; a
// chunked value touches one chunk, an int cell is read from its inline word.
func (s *Store) GetBit(key []byte, bitOffset int64, now int64) int {
	byteIdx := bitOffset >> 3
	_, addr, _ := s.findLive(Hash(key), key, now)
	if addr == 0 || byteIdx >= int64(s.vlen(addr)) {
		return 0
	}
	b := s.byteAt(addr, byteIdx)
	return int(b>>(7-uint(bitOffset&7))) & 1
}

// SetBit answers SETBIT: it sets the bit at bitOffset to bit (0 or 1),
// zero-extending the value when the offset is past the end, and returns the
// previous bit. A write that changes no live byte is skipped, but a write that
// grows the value always lands so the value reaches the offset even when the
// bit is 0, matching Redis. The one-byte patch rides SetRange, which is
// chunk-bounded, so a SETBIT into a giant value rewrites one chunk.
func (s *Store) SetBit(key []byte, bitOffset int64, bit int, now int64) (int, error) {
	byteIdx := bitOffset >> 3
	mask := byte(1) << (7 - uint(bitOffset&7))
	_, addr, _ := s.findLive(Hash(key), key, now)
	inRange := addr != 0 && byteIdx < int64(s.vlen(addr))
	var cur byte
	if inRange {
		cur = s.byteAt(addr, byteIdx)
	}
	old := 0
	if cur&mask != 0 {
		old = 1
	}
	nb := cur &^ mask
	if bit == 1 {
		nb = cur | mask
	}
	// A change to a byte already inside the value is the only case a no-op
	// write can skip; a write past the end must still extend the value.
	if inRange && nb == cur {
		return old, nil
	}
	if _, err := s.SetRange(key, int(byteIdx), []byte{nb}, now); err != nil {
		return 0, err
	}
	return old, nil
}

// byteAt reads the single logical byte at i from a live record, bounded to the
// band the byte lands in. The caller guarantees i is inside the value length.
// A chunked value reads only the covering chunk's byte (arena direct, or one
// log byte); an int cell renders its decimal text; every other band indexes
// the value bytes in place.
func (s *Store) byteAt(addr uint64, i int64) byte {
	f := s.recFlags(addr)
	vs := s.valueStart(addr)
	if f&flagChunked != 0 {
		return s.chunkByteAt(vs, i)
	}
	if f&flagInt != 0 {
		var scratch [20]byte
		n := int64(binary.LittleEndian.Uint64(s.arena.buf[vs:]))
		v := strconv.AppendInt(scratch[:0], n, 10)
		return v[i]
	}
	if f&flagSep != 0 {
		word, _, _ := s.readPtr(vs)
		if word&inLogBit != 0 {
			var one [1]byte
			if err := s.vlog.readFill((word&runAddrMask)+uint64(i), one[:]); err != nil {
				return 0
			}
			return one[0]
		}
		return s.arena.buf[(word&runAddrMask)+uint64(i)]
	}
	return s.arena.buf[vs+uint64(i)]
}

// chunkByteAt reads byte i of a chunked value from its covering chunk. vs is
// the record's value start, holding the directory pointer. An arena chunk is
// indexed directly; a log chunk yields one byte through a single-byte read.
func (s *Store) chunkByteAt(vs uint64, i int64) byte {
	word, n, _ := s.readPtr(vs)
	dirOff := word & runAddrMask
	k := uint64(i) / strChunkSize
	if k >= uint64(n) {
		return 0
	}
	w, l, _ := s.readPtr(dirOff + k*ptrSize)
	j := uint64(i) - k*strChunkSize
	if w == 0 || j >= uint64(l) {
		return 0
	}
	if w&inLogBit != 0 {
		var one [1]byte
		if err := s.vlog.readFill((w&runAddrMask)+j, one[:]); err != nil {
			return 0
		}
		return one[0]
	}
	return s.arena.buf[(w&runAddrMask)+j]
}
