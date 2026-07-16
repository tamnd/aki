package sqlo1

// HyperLogLog, doc 05 section 4: the value under the key is the
// Redis HYLL envelope byte for byte, sparse or dense, so RDB-style
// interchange and byte-parity fixtures against a real redis-server
// both hold. The codec below is a transcription of Redis's
// hyperloglog.c (BSD): same murmur64a seed, same sparse opcode
// rewrite rules, same promotion thresholds, same Ertl estimator, so
// the same PFADD sequence produces the same bytes and the same
// counts. The layer ops are whole-value read-modify-write through
// Get and Set: a valid HLL is at most 12304 bytes, well under every
// ladder boundary, and Set already preserves expiry, which is what
// PFADD and PFMERGE need.

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

// The HYLL geometry, fixed by the format.
const (
	hllP         = 14
	hllQ         = 64 - hllP
	hllRegisters = 1 << hllP
	hllPMask     = hllRegisters - 1
	hllBits      = 6
	hllRegMax    = 1<<hllBits - 1
	hllHdrSize   = 16
	hllDenseSize = hllHdrSize + (hllRegisters*hllBits+7)/8

	hllEncDense  = 0
	hllEncSparse = 1

	// Sparse opcode limits and the promotion ceiling
	// (server.hll_sparse_max_bytes at its Redis default; the config
	// knob changes fixture bytes, so it stays a constant here).
	hllSparseValMaxValue = 32
	hllSparseValMaxLen   = 4
	hllSparseZeroMaxLen  = 64
	hllSparseXZeroMaxLen = 16384
	hllSparseMaxBytes    = 3000

	// 0.5/ln(2), the Ertl estimator's alpha at infinity.
	hllAlphaInf = 0.721347520444481703680
)

// The layer's sentinel errors; the server maps them onto Redis's
// exact wire texts.
var (
	errNotHLL     = errors.New("not a valid HyperLogLog string value")
	errCorruptHLL = errors.New("corrupted HLL object detected")
)

// murmur64a is MurmurHash2 64-bit, the endian-neutral variant Redis
// uses, with its fixed seed applied by hllPatLen.
func murmur64a(data []byte, seed uint64) uint64 {
	const m = 0xc6a4a7935bd1e995
	const r = 47
	h := seed ^ uint64(len(data))*m
	for len(data) >= 8 {
		k := binary.LittleEndian.Uint64(data)
		k *= m
		k ^= k >> r
		k *= m
		h ^= k
		h *= m
		data = data[8:]
	}
	switch len(data) {
	case 7:
		h ^= uint64(data[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(data[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(data[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(data[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(data[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(data[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(data[0])
		h *= m
	}
	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}

// hllPatLen hashes the element and returns its register index and
// the length of the trailing 000..1 pattern, 1 to Q+1.
func hllPatLen(ele []byte) (index int, count uint8) {
	hash := murmur64a(ele, 0xadc83b19)
	index = int(hash & hllPMask)
	hash >>= hllP
	hash |= 1 << hllQ
	return index, uint8(bits.TrailingZeros64(hash) + 1)
}

// hllDenseGet reads the 6-bit register at index from the 12288-byte
// dense register array. Registers pack little-endian across byte
// boundaries; the last register's high bits vanish (fb is 2 there),
// so no byte past the array is ever touched.
func hllDenseGet(regs []byte, index int) uint8 {
	b := index * hllBits >> 3
	fb := uint(index*hllBits) & 7
	v := uint(regs[b]) >> fb
	if fb > 2 {
		v |= uint(regs[b+1]) << (8 - fb)
	}
	return uint8(v & hllRegMax)
}

// hllDenseSetReg unconditionally writes the 6-bit register.
func hllDenseSetReg(regs []byte, index int, val uint8) {
	b := index * hllBits >> 3
	fb := uint(index*hllBits) & 7
	regs[b] &^= hllRegMax << fb
	regs[b] |= val << fb
	if fb > 2 {
		regs[b+1] &^= hllRegMax >> (8 - fb)
		regs[b+1] |= val >> (8 - fb)
	}
}

// hllDenseSet writes the register if val beats the current value,
// reporting whether it did.
func hllDenseSet(regs []byte, index int, val uint8) int {
	if val > hllDenseGet(regs, index) {
		hllDenseSetReg(regs, index, val)
		return 1
	}
	return 0
}

// Sparse opcode predicates and fields, uint8-pointer macros in C.
func hllSparseIsZero(b byte) bool  { return b&0xc0 == 0 }
func hllSparseIsXZero(b byte) bool { return b&0xc0 == 0x40 }
func hllSparseIsVal(b byte) bool   { return b&0x80 != 0 }
func hllSparseZeroLen(b byte) int  { return int(b&0x3f) + 1 }
func hllSparseValValue(b byte) int { return int(b>>2&0x1f) + 1 }
func hllSparseValLen(b byte) int   { return int(b&3) + 1 }

func hllSparseXZeroLen(b0, b1 byte) int { return (int(b0&0x3f)<<8 | int(b1)) + 1 }

func hllSparseValByte(val, length int) byte {
	return byte((val-1)<<2|(length-1)) | 0x80
}

// createHLL builds the empty sparse value: header plus one XZERO
// opcode covering all 16384 registers. The zero card bytes are a
// valid cached cardinality of zero, exactly like Redis.
func createHLL() []byte {
	v := make([]byte, hllHdrSize+2)
	copy(v, "HYLL")
	v[4] = hllEncSparse
	l := hllSparseXZeroMaxLen - 1
	v[hllHdrSize] = byte(l>>8) | 0x40
	v[hllHdrSize+1] = byte(l)
	return v
}

// isHLL is isHLLObjectOrReply's byte-level check: magic, a known
// encoding, and the exact dense length.
func isHLL(v []byte) bool {
	if len(v) < hllHdrSize {
		return false
	}
	if v[0] != 'H' || v[1] != 'Y' || v[2] != 'L' || v[3] != 'L' {
		return false
	}
	if v[4] > hllEncSparse {
		return false
	}
	if v[4] == hllEncDense && len(v) != hllDenseSize {
		return false
	}
	return true
}

// hllInvalidateCache marks the cached cardinality stale (card MSB).
func hllInvalidateCache(v []byte) { v[15] |= 1 << 7 }

// hllValidCache reports whether the cached cardinality is usable.
func hllValidCache(v []byte) bool { return v[15]&(1<<7) == 0 }

// hllSparseToDense converts a sparse value to a fresh dense one,
// carrying the header (including the cached cardinality) over. It
// reports ok=false when the opcodes do not cover exactly the
// register space.
func hllSparseToDense(v []byte) ([]byte, bool) {
	if v[4] == hllEncDense {
		return v, true
	}
	d := make([]byte, hllDenseSize)
	copy(d, v[:hllHdrSize])
	d[4] = hllEncDense
	regs := d[hllHdrSize:]
	idx := 0
	p := hllHdrSize
	for p < len(v) {
		switch {
		case hllSparseIsZero(v[p]):
			idx += hllSparseZeroLen(v[p])
			p++
		case hllSparseIsXZero(v[p]):
			if p+1 >= len(v) {
				return v, false
			}
			idx += hllSparseXZeroLen(v[p], v[p+1])
			p += 2
		default:
			runlen := hllSparseValLen(v[p])
			regval := uint8(hllSparseValValue(v[p]))
			if idx+runlen > hllRegisters {
				return v, false
			}
			for range runlen {
				hllDenseSetReg(regs, idx, regval)
				idx++
			}
			p++
		}
		if idx > hllRegisters {
			return v, false
		}
	}
	if idx != hllRegisters {
		return v, false
	}
	return d, true
}

// hllSparsePromote is hllSparseSet's promote label: convert to dense
// and apply the pending register write there.
func hllSparsePromote(v []byte, index int, count uint8) ([]byte, int) {
	d, ok := hllSparseToDense(v)
	if !ok {
		return v, -1
	}
	hllDenseSet(d[hllHdrSize:], index, count)
	return d, 1
}

// hllSparseSet sets the sparse register at index to count if the
// current value is smaller, returning the possibly reallocated or
// promoted value and 1 when the register changed, 0 when not, -1 on
// a corrupt representation. It is a line-for-line transcription of
// Redis's hllSparseSet: the same case analysis (in-place VAL
// upgrade, single-slot ZERO replacement, the up-to-5-byte splice),
// the same promotion conditions, and the same post-update merge scan
// over five opcodes from the previous one, so the bytes stay
// identical to Redis's for identical add sequences.
func hllSparseSet(v []byte, index int, count uint8) ([]byte, int) {
	if count > hllSparseValMaxValue {
		return hllSparsePromote(v, index, count)
	}

	// Step 1: locate the opcode covering index.
	end := len(v)
	p := hllHdrSize
	first := 0
	prev := -1
	span := 0
	for p < end {
		oplen := 1
		switch {
		case hllSparseIsZero(v[p]):
			span = hllSparseZeroLen(v[p])
		case hllSparseIsVal(v[p]):
			span = hllSparseValLen(v[p])
		default:
			if p+1 >= end {
				return v, -1
			}
			span = hllSparseXZeroLen(v[p], v[p+1])
			oplen = 2
		}
		if index <= first+span-1 {
			break
		}
		prev = p
		p += oplen
		first += span
	}
	if span == 0 || p >= end {
		return v, -1
	}

	var isZero, isXZero, isVal bool
	runlen := 0
	switch {
	case hllSparseIsZero(v[p]):
		isZero = true
		runlen = hllSparseZeroLen(v[p])
	case hllSparseIsXZero(v[p]):
		isXZero = true
		runlen = hllSparseXZeroLen(v[p], v[p+1])
	default:
		isVal = true
		runlen = hllSparseValLen(v[p])
	}

	// Steps 2 and 3: the trivial in-place cases, then the splice.
	if isVal {
		oldcount := hllSparseValValue(v[p])
		if oldcount >= int(count) {
			return v, 0
		}
		if runlen == 1 {
			v[p] = hllSparseValByte(int(count), 1)
			return hllSparseUpdated(v, prev), 1
		}
	}
	if isZero && runlen == 1 {
		v[p] = hllSparseValByte(int(count), 1)
		return hllSparseUpdated(v, prev), 1
	}

	var seq [5]byte
	n := 0
	last := first + span - 1
	appendGap := func(length int) {
		if length > hllSparseZeroMaxLen {
			seq[n] = byte((length-1)>>8) | 0x40
			seq[n+1] = byte(length - 1)
			n += 2
		} else {
			seq[n] = byte(length - 1)
			n++
		}
	}
	if isZero || isXZero {
		if index != first {
			appendGap(index - first)
		}
		seq[n] = hllSparseValByte(int(count), 1)
		n++
		if index != last {
			appendGap(last - index)
		}
	} else {
		curval := hllSparseValValue(v[p])
		if index != first {
			seq[n] = hllSparseValByte(curval, index-first)
			n++
		}
		seq[n] = hllSparseValByte(int(count), 1)
		n++
		if index != last {
			seq[n] = hllSparseValByte(curval, last-index)
			n++
		}
	}

	seqlen := n
	oldlen := 1
	if isXZero {
		oldlen = 2
	}
	deltalen := seqlen - oldlen
	if deltalen > 0 && len(v)+deltalen > hllSparseMaxBytes {
		return hllSparsePromote(v, index, count)
	}
	if deltalen > 0 {
		v = append(v, seq[:deltalen]...)
	}
	if deltalen != 0 {
		copy(v[p+seqlen:], v[p+oldlen:end])
		if deltalen < 0 {
			v = v[:end+deltalen]
		}
	}
	copy(v[p:], seq[:seqlen])
	return hllSparseUpdated(v, prev), 1
}

// hllSparseUpdated is the shared tail of every successful sparse
// update: merge up to five opcodes starting from the previous one
// (adjacent VALs with the same value fold while the combined run
// fits), then invalidate the cached cardinality.
func hllSparseUpdated(v []byte, prev int) []byte {
	p := prev
	if p < 0 {
		p = hllHdrSize
	}
	scanlen := 5
	for p < len(v) && scanlen > 0 {
		scanlen--
		if hllSparseIsXZero(v[p]) {
			p += 2
			continue
		}
		if hllSparseIsZero(v[p]) {
			p++
			continue
		}
		if p+1 < len(v) && hllSparseIsVal(v[p+1]) {
			v1 := hllSparseValValue(v[p])
			v2 := hllSparseValValue(v[p+1])
			if v1 == v2 {
				l := hllSparseValLen(v[p]) + hllSparseValLen(v[p+1])
				if l <= hllSparseValMaxLen {
					v[p+1] = hllSparseValByte(v1, l)
					copy(v[p:], v[p+1:])
					v = v[:len(v)-1]
					continue
				}
			}
		}
		p++
	}
	hllInvalidateCache(v)
	return v
}

// hllAdd routes one element through the encoding at hand, returning
// 1 on a register change, 0 on none, -1 on corruption, with the
// possibly changed value.
func hllAdd(v []byte, ele []byte) ([]byte, int) {
	index, count := hllPatLen(ele)
	switch v[4] {
	case hllEncDense:
		return v, hllDenseSet(v[hllHdrSize:], index, count)
	case hllEncSparse:
		return hllSparseSet(v, index, count)
	}
	return v, -1
}

// hllMergeMax folds a value's registers into max, one byte per
// register, keeping the larger of the pair. It reports ok=false on a
// corrupt sparse representation.
func hllMergeMax(max []byte, v []byte) bool {
	if v[4] == hllEncDense {
		regs := v[hllHdrSize:]
		for i := range hllRegisters {
			if val := hllDenseGet(regs, i); val > max[i] {
				max[i] = val
			}
		}
		return true
	}
	i := 0
	p := hllHdrSize
	for p < len(v) {
		switch {
		case hllSparseIsZero(v[p]):
			i += hllSparseZeroLen(v[p])
			p++
		case hllSparseIsXZero(v[p]):
			if p+1 >= len(v) {
				return false
			}
			i += hllSparseXZeroLen(v[p], v[p+1])
			p += 2
		default:
			runlen := hllSparseValLen(v[p])
			regval := uint8(hllSparseValValue(v[p]))
			if i+runlen > hllRegisters {
				return false
			}
			for range runlen {
				if regval > max[i] {
					max[i] = regval
				}
				i++
			}
			p++
		}
		if i > hllRegisters {
			return false
		}
	}
	return i == hllRegisters
}

// hllSigma from Ertl, "New cardinality estimation algorithms for
// HyperLogLog sketches", arXiv:1702.01284.
func hllSigma(x float64) float64 {
	if x == 1 {
		return math.Inf(1)
	}
	y := 1.0
	z := x
	for {
		x *= x
		zPrime := z
		z += x * y
		y += y
		if zPrime == z {
			return z
		}
	}
}

// hllTau from the same paper.
func hllTau(x float64) float64 {
	if x == 0 || x == 1 {
		return 0
	}
	y := 1.0
	z := 1 - x
	for {
		x = math.Sqrt(x)
		zPrime := z
		y *= 0.5
		z -= (1 - x) * (1 - x) * y
		if zPrime == z {
			return z / 3
		}
	}
}

// hllCountHisto turns a register histogram into the Ertl estimate.
func hllCountHisto(hist *[64]int) uint64 {
	m := float64(hllRegisters)
	z := m * hllTau((m-float64(hist[hllQ+1]))/m)
	for j := hllQ; j >= 1; j-- {
		z += float64(hist[j])
		z *= 0.5
	}
	z += m * hllSigma(float64(hist[0])/m)
	return uint64(math.Round(hllAlphaInf * m * m / z))
}

// hllCount estimates the cardinality of one HLL value. It reports
// ok=false on a corrupt sparse representation.
func hllCount(v []byte) (uint64, bool) {
	var hist [64]int
	if v[4] == hllEncDense {
		regs := v[hllHdrSize:]
		for i := range hllRegisters {
			hist[hllDenseGet(regs, i)]++
		}
	} else {
		i := 0
		p := hllHdrSize
		for p < len(v) {
			switch {
			case hllSparseIsZero(v[p]):
				runlen := hllSparseZeroLen(v[p])
				i += runlen
				hist[0] += runlen
				p++
			case hllSparseIsXZero(v[p]):
				if p+1 >= len(v) {
					return 0, false
				}
				runlen := hllSparseXZeroLen(v[p], v[p+1])
				i += runlen
				hist[0] += runlen
				p += 2
			default:
				runlen := hllSparseValLen(v[p])
				hist[hllSparseValValue(v[p])] += runlen
				i += runlen
				p++
			}
			if i > hllRegisters {
				return 0, false
			}
		}
		if i != hllRegisters {
			return 0, false
		}
	}
	return hllCountHisto(&hist), true
}

// hllCountRaw estimates from a raw one-byte-per-register array, the
// internal-only merge target multi-key PFCOUNT uses.
func hllCountRaw(regs []byte) uint64 {
	var hist [64]int
	for _, r := range regs {
		hist[r&hllRegMax]++
	}
	return hllCountHisto(&hist)
}

// hllRead pulls key's whole value into the HLL scratch and validates
// the envelope. The copy is what makes read-modify-write safe: Get's
// bytes die on the next store call.
func (s *Str) hllRead(ctx context.Context, key []byte) ([]byte, bool, error) {
	v, ok, err := s.Get(ctx, key)
	if err != nil || !ok {
		return nil, false, err
	}
	if !isHLL(v) {
		return nil, false, errNotHLL
	}
	s.hllBuf = append(s.hllBuf[:0], v...)
	return s.hllBuf, true, nil
}

// PfAdd folds the elements into key's HLL, creating it when absent,
// and returns 1 when any register changed (creation counts), else 0.
// A live expiry on the key survives, per Redis.
func (s *Str) PfAdd(ctx context.Context, key []byte, elems [][]byte) (int64, error) {
	h, ok, err := s.hllRead(ctx, key)
	if err != nil {
		return 0, err
	}
	updated := 0
	if !ok {
		h = createHLL()
		updated++
	}
	for _, e := range elems {
		var ret int
		h, ret = hllAdd(h, e)
		if ret < 0 {
			return 0, errCorruptHLL
		}
		updated += ret
	}
	s.hllBuf = h[:0]
	if updated == 0 {
		return 0, nil
	}
	hllInvalidateCache(h)
	if err := s.Set(ctx, key, h); err != nil {
		return 0, err
	}
	return 1, nil
}

// PfCount estimates the cardinality of one key, or of the union when
// given several. The single-key path serves the cached cardinality
// when it is valid and writes it back when it had to recompute,
// exactly like Redis, which is what makes the cache bytes part of
// the parity surface.
func (s *Str) PfCount(ctx context.Context, keys [][]byte) (int64, error) {
	if len(keys) > 1 {
		var max [hllRegisters]byte
		for _, k := range keys {
			v, ok, err := s.Get(ctx, k)
			if err != nil {
				return 0, err
			}
			if !ok {
				continue
			}
			if !isHLL(v) {
				return 0, errNotHLL
			}
			if !hllMergeMax(max[:], v) {
				return 0, errCorruptHLL
			}
		}
		return int64(hllCountRaw(max[:])), nil
	}
	h, ok, err := s.hllRead(ctx, keys[0])
	if err != nil || !ok {
		return 0, err
	}
	if hllValidCache(h) {
		return int64(binary.LittleEndian.Uint64(h[8:16])), nil
	}
	card, valid := hllCount(h)
	if !valid {
		return 0, errCorruptHLL
	}
	binary.LittleEndian.PutUint64(h[8:16], card)
	if err := s.Set(ctx, keys[0], h); err != nil {
		return 0, err
	}
	return int64(card), nil
}

// PfMerge folds the sources and the current destination into dest.
// The destination stays sparse when every input was sparse (the
// Redis 7 behavior) and goes dense the moment any input is dense.
func (s *Str) PfMerge(ctx context.Context, dest []byte, srcs [][]byte) error {
	var max [hllRegisters]byte
	useDense := false
	destExists := false

	fold := func(k []byte, keep bool) error {
		v, ok, err := s.Get(ctx, k)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if !isHLL(v) {
			return errNotHLL
		}
		if v[4] == hllEncDense {
			useDense = true
		}
		if !hllMergeMax(max[:], v) {
			return errCorruptHLL
		}
		if keep {
			destExists = true
			s.hllBuf = append(s.hllBuf[:0], v...)
		}
		return nil
	}
	if err := fold(dest, true); err != nil {
		return err
	}
	for _, k := range srcs {
		if err := fold(k, false); err != nil {
			return err
		}
	}

	h := s.hllBuf
	if !destExists {
		h = createHLL()
	}
	if useDense {
		var ok bool
		if h, ok = hllSparseToDense(h); !ok {
			return errCorruptHLL
		}
		regs := h[hllHdrSize:]
		for j := range hllRegisters {
			hllDenseSetReg(regs, j, max[j]&hllRegMax)
		}
	} else {
		for j := range hllRegisters {
			if max[j] == 0 {
				continue
			}
			var ret int
			switch h[4] {
			case hllEncDense:
				ret = hllDenseSet(h[hllHdrSize:], j, max[j])
			case hllEncSparse:
				h, ret = hllSparseSet(h, j, max[j])
			}
			if ret < 0 {
				return errCorruptHLL
			}
		}
	}
	hllInvalidateCache(h)
	s.hllBuf = h[:0]
	return s.Set(ctx, dest, h)
}

// PfGet reads and validates key's HLL for the PFDEBUG read paths.
// The bytes alias the HLL scratch and die on the next call.
func (s *Str) PfGet(ctx context.Context, key []byte) ([]byte, bool, error) {
	return s.hllRead(ctx, key)
}

// PfToDense converts key's HLL to the dense representation in place,
// persisting the conversion, and reports whether a conversion
// happened and whether the key exists.
func (s *Str) PfToDense(ctx context.Context, key []byte) (converted, exists bool, err error) {
	h, ok, err := s.hllRead(ctx, key)
	if err != nil || !ok {
		return false, ok, err
	}
	if h[4] == hllEncDense {
		return false, true, nil
	}
	d, valid := hllSparseToDense(h)
	if !valid {
		return false, true, errCorruptHLL
	}
	return true, true, s.Set(ctx, key, d)
}

// hllSelfTest is PFSELFTEST's body: a deterministic port of Redis's
// register access test plus a sparse/dense agreement pass, using a
// splitmix64 stream instead of rand() so the command cannot flake.
func hllSelfTest() error {
	seed := uint64(0x9e3779b97f4a7c15)
	next := func() uint64 {
		seed += 0x9e3779b97f4a7c15
		z := seed
		z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
		z = (z ^ z>>27) * 0x94d049bb133111eb
		return z ^ z>>31
	}

	// Test 1: dense register access retains values without touching
	// neighbors, against a plain byte-per-register mirror.
	regs := make([]byte, hllDenseSize-hllHdrSize)
	var mirror [hllRegisters]byte
	for range 16 {
		for i := range hllRegisters {
			r := uint8(next() & hllRegMax)
			mirror[i] = r
			hllDenseSetReg(regs, i, r)
		}
		for i := range hllRegisters {
			if got := hllDenseGet(regs, i); got != mirror[i] {
				return errors.New("PFSELFTEST failed: dense register mismatch")
			}
		}
	}

	// Test 2: the sparse path (with its natural promotion) and a
	// pure dense path agree register for register over the same
	// element stream, and the estimate lands near the truth.
	sparse := createHLL()
	dense := make([]byte, hllDenseSize)
	copy(dense, "HYLL")
	dense[4] = hllEncDense
	var ele [8]byte
	const n = 5000
	for range n {
		binary.LittleEndian.PutUint64(ele[:], next())
		var ret int
		if sparse, ret = hllAdd(sparse, ele[:]); ret < 0 {
			return errors.New("PFSELFTEST failed: sparse add errored")
		}
		index, count := hllPatLen(ele[:])
		hllDenseSet(dense[hllHdrSize:], index, count)
	}
	full, ok := hllSparseToDense(sparse)
	if !ok {
		return errors.New("PFSELFTEST failed: sparse representation corrupt")
	}
	for i := range hllRegisters {
		if hllDenseGet(full[hllHdrSize:], i) != hllDenseGet(dense[hllHdrSize:], i) {
			return errors.New("PFSELFTEST failed: sparse/dense register mismatch")
		}
	}
	est, _ := hllCount(dense)
	if est < n-n/5 || est > n+n/5 {
		return errors.New("PFSELFTEST failed: estimate outside 20% of truth")
	}
	return nil
}
