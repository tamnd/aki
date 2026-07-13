package store

import (
	"encoding/binary"
	"strconv"
)

// BITOP on the string substrate (spec 2064/f3/15 section 5), the co-located
// case: destination and every source hash to one owner, so the whole algebra
// runs here under F1 with no hop. The command streams the sources chunk by
// chunk, applies the word-op kernel (section 3.2, plain 64-bit ALU ops the Go
// compiler already emits), and writes the destination one chunk at a time, so
// at most (sources + 1) chunks are resident regardless of bitmap length. That
// is the L11 discharge: no source and no result is ever whole in memory.
//
// The result length is the longest source's length, shorter sources zero-pad
// (2.1). Past a source's end every byte reads zero, which the fill below
// synthesizes without allocating, so AND past the shortest source is all zero
// (the short-circuit), OR/XOR carry the longer source through, and NOT of a
// missing byte is 0xFF (its output is dense by definition, 5). All-missing
// sources give a zero result length, which deletes the destination and returns
// 0 per the string rules.

// BITOP operation codes. NOT takes exactly one source; the others take one or
// more. The derived layer validates the arity and the NOT-single-source rule
// before calling here.
const (
	BitAnd = iota
	BitOr
	BitXor
	BitNot
)

// BitOp computes op over the source values and stores the result at dest,
// returning the result length (the BITOP reply) or 0 when the result is empty
// and dest is deleted. All keys are co-located on this store by the dispatch
// check. The destination may alias a source: the result at chunk k depends only
// on the sources at chunk k, so reading every source's chunk k before writing
// dest's chunk k keeps an aliased destination correct without a defensive clone
// (the swap-on-completion intent from 5, made structural by chunk locality).
func (s *Store) BitOp(op int, dest []byte, srcs [][]byte, now int64) (int64, error) {
	addrs := make([]uint64, len(srcs))
	lens := make([]int64, len(srcs))
	var maxlen int64
	minlen := int64(-1)
	for i, k := range srcs {
		_, addr, _ := s.findLive(Hash(k), k, now)
		addrs[i] = addr
		if addr != 0 {
			lens[i] = int64(s.vlen(addr))
		}
		if lens[i] > maxlen {
			maxlen = lens[i]
		}
		if minlen < 0 || lens[i] < minlen {
			minlen = lens[i]
		}
	}
	if maxlen == 0 {
		s.Del(dest, now)
		return 0, nil
	}

	// An aliased destination cannot be cleared up front, since it is still a
	// source; every chunk (including all-zero ones) is written in order so the
	// old bytes underneath are fully overwritten. maxlen covers the aliased
	// source, so the old destination is never longer than the result and needs
	// no truncation. A non-aliased destination is built fresh: it is dropped
	// first, then only live chunks are written so all-zero interiors fall
	// through to directory holes (2.3).
	aliased := false
	for _, k := range srcs {
		if string(k) == string(dest) {
			aliased = true
			break
		}
	}
	if !aliased {
		s.Del(dest, now)
	}

	// Per-source chunk buffers plus the result buffer: the (sources + 1) chunk
	// residency the memory bound promises.
	bufs := make([][]byte, len(srcs))
	for i := range bufs {
		bufs[i] = make([]byte, strChunkSize)
	}
	res := make([]byte, strChunkSize)

	nChunks := (maxlen + strChunkSize - 1) / strChunkSize
	for k := int64(0); k < nChunks; k++ {
		cs := k * strChunkSize
		cl := int64(strChunkSize)
		if maxlen-cs < cl {
			cl = maxlen - cs
		}
		out := res[:cl]
		switch op {
		case BitNot:
			s.fillRange(addrs[0], lens[0], cs, out)
			notWords(out)
		case BitAnd:
			if cs >= minlen {
				// Past the shortest source every AND byte is zero: no source is
				// read here, the chunk is a hole (or, when aliased, zeros).
				clear(out)
			} else {
				s.fillRange(addrs[0], lens[0], cs, out)
				for i := 1; i < len(srcs); i++ {
					b := bufs[i][:cl]
					s.fillRange(addrs[i], lens[i], cs, b)
					andWords(out, b)
				}
			}
		case BitOr:
			s.fillRange(addrs[0], lens[0], cs, out)
			for i := 1; i < len(srcs); i++ {
				b := bufs[i][:cl]
				s.fillRange(addrs[i], lens[i], cs, b)
				orWords(out, b)
			}
		case BitXor:
			s.fillRange(addrs[0], lens[0], cs, out)
			for i := 1; i < len(srcs); i++ {
				b := bufs[i][:cl]
				s.fillRange(addrs[i], lens[i], cs, b)
				xorWords(out, b)
			}
		}
		last := k == nChunks-1
		// A non-aliased build skips all-zero interior chunks so they stay holes;
		// the final chunk is always written so the destination reaches maxlen
		// exactly even when its bytes are zero. An aliased build writes every
		// chunk to overwrite the old value beneath it.
		if aliased || last || !allZero(out) {
			if _, err := s.SetRange(dest, int(cs), out, now); err != nil {
				return 0, err
			}
		}
	}
	return maxlen, nil
}

// ReadInto fills dst with the value at key over [off, off+len(dst)), zero-filled
// past the value end or when the key is absent. off must be chunk-aligned, the
// contract the coordinator reads sources under. It is the by-key entry to
// fillRange for a cross-shard read hop, which holds a key rather than the resolved
// address a co-located call already has. Any band is handled; a non-string key
// reads as absent, the same no-foreign-guard rule the co-located path takes.
func (s *Store) ReadInto(key []byte, off int64, dst []byte, now int64) {
	_, addr, _ := s.findLive(Hash(key), key, now)
	var length int64
	if addr != 0 {
		length = int64(s.vlen(addr))
	}
	s.fillRange(addr, length, off, dst)
}

// CombineChunk applies op over the source chunk views into dst and reports whether
// the result is all zero, so a caller building a fresh destination can leave an
// all-zero interior chunk as a directory hole. dst and every view are the same
// length. NOT complements the single source; AND/OR/XOR fold the first source with
// the rest. No views at all yields an all-zero dst, the AND short-circuit past the
// shortest source the coordinator uses to skip a chunk's read hops. This keeps the
// word kernels private to the store while the cross-shard coordinator drives the
// hops.
func CombineChunk(op int, dst []byte, srcs [][]byte) bool {
	switch {
	case len(srcs) == 0:
		clear(dst)
	case op == BitNot:
		copy(dst, srcs[0])
		notWords(dst)
	default:
		copy(dst, srcs[0])
		for i := 1; i < len(srcs); i++ {
			switch op {
			case BitAnd:
				andWords(dst, srcs[i])
			case BitOr:
				orWords(dst, srcs[i])
			case BitXor:
				xorWords(dst, srcs[i])
			}
		}
	}
	return allZero(dst)
}

// fillRange fills dst with the value's bytes at [off, off+len(dst)), zero-filled
// where the range runs past the value length or the key is absent. off is
// chunk-aligned by the BitOp loop, so a chunked value's range lands inside one
// chunk; the other bands hold the whole value below one chunk. Nothing is
// materialized beyond the one chunk dst spans.
func (s *Store) fillRange(addr uint64, length, off int64, dst []byte) {
	n := int64(len(dst))
	if addr == 0 || off >= length {
		clear(dst)
		return
	}
	m := n
	if length-off < m {
		m = length - off
	}
	f := s.recFlags(addr)
	vs := s.valueStart(addr)
	switch {
	case f&flagChunked != 0:
		s.copyChunkAligned(vs, off, dst[:m])
	case f&flagInt != 0:
		var scratch [20]byte
		v := strconv.AppendInt(scratch[:0], int64(binary.LittleEndian.Uint64(s.arena.buf[vs:])), 10)
		copy(dst[:m], v[off:off+m])
	case f&flagSep != 0:
		word, _, _ := s.readPtr(vs)
		base := word & runAddrMask
		if word&inLogBit != 0 {
			_ = s.vlog.readFill(base+uint64(off), dst[:m])
		} else {
			copy(dst[:m], s.arena.buf[base+uint64(off):])
		}
	default:
		copy(dst[:m], s.arena.buf[vs+uint64(off):])
	}
	if m < n {
		clear(dst[m:])
	}
}

// copyChunkAligned copies a chunk-aligned range of a chunked value into dst: off
// is a multiple of strChunkSize, so the range is chunk k = off/strChunkSize at
// intra-chunk offset zero. A hole (word 0) or a range past the stored chunk
// length yields zeros.
func (s *Store) copyChunkAligned(vs uint64, off int64, dst []byte) {
	word, n, _ := s.readPtr(vs)
	dirOff := word & runAddrMask
	k := uint64(off) / strChunkSize
	if k >= uint64(n) {
		clear(dst)
		return
	}
	w, l, _ := s.readPtr(dirOff + k*ptrSize)
	if w == 0 {
		clear(dst)
		return
	}
	m := len(dst)
	if uint64(m) > uint64(l) {
		m = int(l)
	}
	if w&inLogBit != 0 {
		_ = s.vlog.readFill(w&runAddrMask, dst[:m])
	} else {
		run := w & runAddrMask
		copy(dst[:m], s.arena.buf[run:run+uint64(m)])
	}
	if m < len(dst) {
		clear(dst[m:])
	}
}

// The word-op kernels (spec 2064/f3/15 section 3.2): eight bytes at a time
// through plain 64-bit ALU ops, which the compiler emits at two to three per
// cycle, with a byte tail. Bitwise ops are endian-agnostic, so reading the
// bytes as little-endian words and writing them back preserves the byte layout.

func andWords(dst, src []byte) {
	i := 0
	for ; i+8 <= len(dst); i += 8 {
		a := binary.LittleEndian.Uint64(dst[i:])
		b := binary.LittleEndian.Uint64(src[i:])
		binary.LittleEndian.PutUint64(dst[i:], a&b)
	}
	for ; i < len(dst); i++ {
		dst[i] &= src[i]
	}
}

func orWords(dst, src []byte) {
	i := 0
	for ; i+8 <= len(dst); i += 8 {
		a := binary.LittleEndian.Uint64(dst[i:])
		b := binary.LittleEndian.Uint64(src[i:])
		binary.LittleEndian.PutUint64(dst[i:], a|b)
	}
	for ; i < len(dst); i++ {
		dst[i] |= src[i]
	}
}

func xorWords(dst, src []byte) {
	i := 0
	for ; i+8 <= len(dst); i += 8 {
		a := binary.LittleEndian.Uint64(dst[i:])
		b := binary.LittleEndian.Uint64(src[i:])
		binary.LittleEndian.PutUint64(dst[i:], a^b)
	}
	for ; i < len(dst); i++ {
		dst[i] ^= src[i]
	}
}

func notWords(dst []byte) {
	i := 0
	for ; i+8 <= len(dst); i += 8 {
		binary.LittleEndian.PutUint64(dst[i:], ^binary.LittleEndian.Uint64(dst[i:]))
	}
	for ; i < len(dst); i++ {
		dst[i] = ^dst[i]
	}
}

// allZero reports whether b is all zero bytes, word-at-a-time, so a non-aliased
// build can drop an all-zero interior chunk to a directory hole.
func allZero(b []byte) bool {
	i := 0
	for ; i+8 <= len(b); i += 8 {
		if binary.LittleEndian.Uint64(b[i:]) != 0 {
			return false
		}
	}
	for ; i < len(b); i++ {
		if b[i] != 0 {
			return false
		}
	}
	return true
}
