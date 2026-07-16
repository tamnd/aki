package derived

// The HLL register-merge fold (spec 2064/f3/15 section 8): for each of 16384
// registers, acc[i] = max(acc[i], src[i]), the dominant term in PFMERGE and
// multi-key PFCOUNT. The fold runs on a one-byte-per-register scratch: each
// source is unpacked into the scratch and folded with a SWAR byte-max, so
// PFMERGE repacks the scratch once at the end and multi-key PFCOUNT reads its
// histogram straight from the scratch with no repack. The labs/f3/m6/06_hll_merge
// lab prices this kernel and pins it byte-identical to the packed per-register
// merge; the strategy and the branchless arithmetic are recorded there.

// swarHigh is the per-lane high-bit mask the byte-max leans on: every register
// is at most 63 so the high bit of every lane is clear, which is what makes the
// borrow stay inside its lane.
const swarHigh = 0x8080808080808080

// unpackDenseInto expands a packed 6-bit dense register array into dst, one byte
// per register, reading 12 bytes and peeling 16 registers per step so the loop
// has no tail and no per-register branch. dst must be hllRegisters long.
func unpackDenseInto(regs, dst []byte) {
	r := regs
	o := dst
	for j := 0; j < hllRegisters/16; j++ {
		b0 := uint(r[0])
		b1 := uint(r[1])
		b2 := uint(r[2])
		b3 := uint(r[3])
		b4 := uint(r[4])
		b5 := uint(r[5])
		b6 := uint(r[6])
		b7 := uint(r[7])
		b8 := uint(r[8])
		b9 := uint(r[9])
		b10 := uint(r[10])
		b11 := uint(r[11])
		o[0] = byte(b0 & 63)
		o[1] = byte((b0>>6 | b1<<2) & 63)
		o[2] = byte((b1>>4 | b2<<4) & 63)
		o[3] = byte((b2 >> 2) & 63)
		o[4] = byte(b3 & 63)
		o[5] = byte((b3>>6 | b4<<2) & 63)
		o[6] = byte((b4>>4 | b5<<4) & 63)
		o[7] = byte((b5 >> 2) & 63)
		o[8] = byte(b6 & 63)
		o[9] = byte((b6>>6 | b7<<2) & 63)
		o[10] = byte((b7>>4 | b8<<4) & 63)
		o[11] = byte((b8 >> 2) & 63)
		o[12] = byte(b9 & 63)
		o[13] = byte((b9>>6 | b10<<2) & 63)
		o[14] = byte((b10>>4 | b11<<4) & 63)
		o[15] = byte((b11 >> 2) & 63)
		r = r[12:]
		o = o[16:]
	}
}

// swarMaxInto folds src into dst on the unpacked one-byte-per-register form,
// eight lanes per 8-byte word with no branch. (a|H)-b keeps each lane's high bit
// set exactly when a >= b and never borrows across a lane, so h isolates a
// per-lane a-ge-b flag; h|(h-(h>>7)) expands it to a full-byte select mask.
func swarMaxInto(dst, src []byte) {
	n := len(dst) &^ 7
	for i := 0; i < n; i += 8 {
		a := leU64(dst[i:])
		b := leU64(src[i:])
		h := ((a | swarHigh) - b) & swarHigh
		m := h | (h - (h >> 7))
		putU64(dst[i:], (a&m)|(b&^m))
	}
}

func leU64(b []byte) uint64 {
	_ = b[7]
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

func putU64(b []byte, v uint64) {
	_ = b[7]
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
}

// foldInto folds a validated sketch into the unpacked scratch acc, returning
// false on a corrupt sparse stream. A dense source is unpacked into tmp (a
// reusable hllRegisters scratch) and folded with the word kernel; a sparse
// source is walked run by run, ZERO/XZERO runs skipped because max with zero is
// identity, so only its VAL runs touch acc and a fresh HLL costs its opcode
// count, not 16384.
func foldInto(acc, tmp, blob []byte) bool {
	switch blob[4] {
	case hllDense:
		unpackDenseInto(blob[hllHdrSize:], tmp)
		swarMaxInto(acc, tmp)
		return true
	case hllSparse:
		return foldSparseInto(acc, blob[hllHdrSize:])
	default:
		return false
	}
}

// foldSparseInto folds a sparse opcode stream into acc, touching only VAL runs.
// It reports false unless the stream covers exactly 16384 registers, the
// coverage check the estimator refuses to guess through.
func foldSparseInto(acc, opcodes []byte) bool {
	idx := 0
	p := 0
	end := len(opcodes)
	for p < end {
		op := opcodes[p]
		switch {
		case sparseIsZero(op):
			idx += sparseZeroLen(op)
			p++
		case sparseIsXZero(op):
			if p+1 >= end {
				return false
			}
			idx += sparseXZeroLen(opcodes[p], opcodes[p+1])
			p += 2
		default: // VAL
			runLen := sparseValLen(op)
			val := byte(sparseValValue(op))
			if idx+runLen > hllRegisters {
				return false
			}
			for k := 0; k < runLen; k++ {
				if val > acc[idx+k] {
					acc[idx+k] = val
				}
			}
			idx += runLen
			p++
		}
	}
	return idx == hllRegisters
}

// scratchHisto folds the unpacked scratch into a value histogram, the multi-key
// PFCOUNT read path that never repacks.
func scratchHisto(acc []byte, histo *[64]int) {
	for _, v := range acc {
		histo[v&63]++
	}
}

// denseFromScratch repacks the unpacked scratch into a fresh dense sketch,
// word-at-a-time (16 registers into 12 bytes per step, the inverse of the
// unpack), with the cache marked stale. PFMERGE always stores a dense
// destination, matching Redis, so the next PFCOUNT recomputes from the merged
// registers.
func denseFromScratch(acc []byte) []byte {
	blob := make([]byte, hllDenseSize)
	blob[0], blob[1], blob[2], blob[3] = 'H', 'Y', 'L', 'L'
	blob[4] = hllDense
	r := blob[hllHdrSize:]
	s := acc
	for j := 0; j < hllRegisters/16; j++ {
		v0 := uint(s[0])
		v1 := uint(s[1])
		v2 := uint(s[2])
		v3 := uint(s[3])
		v4 := uint(s[4])
		v5 := uint(s[5])
		v6 := uint(s[6])
		v7 := uint(s[7])
		v8 := uint(s[8])
		v9 := uint(s[9])
		v10 := uint(s[10])
		v11 := uint(s[11])
		v12 := uint(s[12])
		v13 := uint(s[13])
		v14 := uint(s[14])
		v15 := uint(s[15])
		r[0] = byte(v0 | v1<<6)
		r[1] = byte(v1>>2 | v2<<4)
		r[2] = byte(v2>>4 | v3<<2)
		r[3] = byte(v4 | v5<<6)
		r[4] = byte(v5>>2 | v6<<4)
		r[5] = byte(v6>>4 | v7<<2)
		r[6] = byte(v8 | v9<<6)
		r[7] = byte(v9>>2 | v10<<4)
		r[8] = byte(v10>>4 | v11<<2)
		r[9] = byte(v12 | v13<<6)
		r[10] = byte(v13>>2 | v14<<4)
		r[11] = byte(v14>>4 | v15<<2)
		r = r[12:]
		s = s[16:]
	}
	invalidateCache(blob)
	return blob
}
