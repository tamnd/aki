package f1srv

import (
	"encoding/binary"
	"math"
)

// HyperLogLog lives on the string model, exactly as Redis stores it: a HYLL-tagged blob that a
// PFADD/PFCOUNT/PFMERGE reads and writes through the same f1raw string record any GET/SET uses. The
// blob starts with a 16-byte header (magic "HYLL", one encoding byte, three reserved, an 8-byte
// little-endian cached cardinality whose top bit is the cache-invalid flag) followed by either the
// dense register array (16384 registers packed 6 bits each) or a sparse run-length opcode stream.
// TYPE reports "string" and a non-string collection under the key is a WRONGTYPE, since an HLL is a
// string at the keyspace layer. Every layout and estimator constant mirrors Redis so PFCOUNT and the
// stored blob are byte-identical across tools: a PFMERGE or a raw GET of an HLL round-trips with a
// same-version Redis or Valkey, and PFCOUNT returns the same integer they return.

const (
	hllP        = 14
	hllQ        = 64 - hllP           // 50: bits left after removing the register index.
	hllRegisters = 1 << hllP          // 16384 registers.
	hllPMask    = hllRegisters - 1    // mask to extract the register index from a hash.
	hllBits     = 6                   // bits per dense register.
	hllRegisterMax = (1 << hllBits) - 1 // 63: largest value a register holds.
	hllHdrSize  = 16
	hllDenseSize = hllHdrSize + (hllRegisters*hllBits+7)/8 // 12304 bytes for a dense HLL.

	hllDense       = 0
	hllSparse      = 1
	hllRaw         = 255 // internal-only: one byte per register, used to union for PFCOUNT.
	hllMaxEncoding = 1

	hllSparseXzeroBit = 0x40 // 01xxxxxx marks an XZERO opcode.
	hllSparseValBit   = 0x80 // 1vvvvvxx marks a VAL opcode.

	hllSparseValMaxValue = 32
	hllSparseValMaxLen   = 4
	hllSparseZeroMaxLen  = 64
	hllSparseXzeroMaxLen = 16384
	// hllSparseMaxBytes is Redis's default hll-sparse-max-bytes: past this the sparse blob is
	// promoted to dense. It must match Redis's default so the promotion point is identical.
	hllSparseMaxBytes = 3000

	// hllAlphaInf is the alpha constant for m -> infinity used by the estimator (1/(2 ln 2)),
	// matching Redis's HLL_ALPHA_INF.
	hllAlphaInf = 0.7213475204444817
)

// Error strings byte-identical to Redis. The key-holds-a-non-HLL-string case and the corrupted-blob
// case are distinct messages; a WRONGTYPE against a non-string collection uses the shared wrongType.
const (
	hllNotValidErr = "WRONGTYPE Key is not a valid HyperLogLog string value."
	hllCorruptErr  = "INVALIDOBJ Corrupted HLL object detected"
)

// hllEncoding returns the encoding byte (dense/sparse/raw) of a blob.
func hllEncoding(blob []byte) byte { return blob[4] }

// hllInvalidateCache clears the cached-cardinality valid bit (the top bit of the last header byte),
// so the next PFCOUNT recomputes.
func hllInvalidateCache(blob []byte) { blob[15] |= 1 << 7 }

// hllValidCache reports whether the header's cached cardinality is still valid.
func hllValidCache(blob []byte) bool { return blob[15]&(1<<7) == 0 }

// hllReadCache reads the cached cardinality. Only meaningful when hllValidCache is true, in which
// case the top bit is zero and the eight header bytes are the exact cardinality little-endian.
func hllReadCache(blob []byte) uint64 { return binary.LittleEndian.Uint64(blob[8:16]) }

// hllWriteCache stores the cardinality little-endian and, since a real cardinality never reaches
// 2^63, leaves the top bit clear which marks the cache valid.
func hllWriteCache(blob []byte, card uint64) { binary.LittleEndian.PutUint64(blob[8:16], card) }

// hllValid reports whether blob is a structurally valid HLL: it has a header, the "HYLL" magic, a
// known encoding, and, for a dense blob, exactly the dense length. This is the check Redis makes
// before every HLL operation on an existing string, and a failure is the hllNotValidErr reply.
func hllValid(blob []byte) bool {
	if len(blob) < hllHdrSize {
		return false
	}
	if blob[0] != 'H' || blob[1] != 'Y' || blob[2] != 'L' || blob[3] != 'L' {
		return false
	}
	if blob[4] > hllMaxEncoding {
		return false
	}
	if blob[4] == hllDense && len(blob) != hllDenseSize {
		return false
	}
	return true
}

// hllCreate builds a fresh sparse HLL: the header plus a run of XZERO opcodes covering all 16384
// registers as zero. With HLL_SPARSE_XZERO_MAX_LEN == HLL_REGISTERS this is a single XZERO, so a new
// HLL is 18 bytes, not 12 KiB.
func hllCreate() []byte {
	sparselen := hllHdrSize + ((hllRegisters+hllSparseXzeroMaxLen-1)/hllSparseXzeroMaxLen)*2
	blob := make([]byte, sparselen)
	copy(blob[0:4], "HYLL")
	blob[4] = hllSparse
	p := hllHdrSize
	aux := hllRegisters
	for aux > 0 {
		x := hllSparseXzeroMaxLen
		if x > aux {
			x = aux
		}
		sparseXzeroSet(blob, p, x)
		p += 2
		aux -= x
	}
	return blob
}

// --- sparse opcode helpers, mirroring Redis's HLL_SPARSE_* macros exactly ---

func sparseIsZero(b byte) bool  { return b&0xc0 == 0 }
func sparseIsXzero(b byte) bool { return b&0xc0 == hllSparseXzeroBit }
func sparseIsVal(b byte) bool   { return b&hllSparseValBit != 0 }

func sparseZeroLen(b byte) int          { return int(b&0x3f) + 1 }
func sparseXzeroLen(b0, b1 byte) int    { return (int(b0&0x3f)<<8 | int(b1)) + 1 }
func sparseValValue(b byte) uint8       { return (b>>2)&0x1f + 1 }
func sparseValLen(b byte) int           { return int(b&0x3) + 1 }

func sparseValByte(val uint8, l int) byte {
	return byte(((int(val)-1)<<2 | (l - 1)) | hllSparseValBit)
}
func sparseValSet(blob []byte, p int, val uint8, l int) { blob[p] = sparseValByte(val, l) }
func sparseZeroSet(blob []byte, p int, l int)           { blob[p] = byte(l - 1) }
func sparseXzeroSet(blob []byte, p int, l int) {
	l--
	blob[p] = byte(l>>8) | hllSparseXzeroBit
	blob[p+1] = byte(l & 0xff)
}

// --- dense 6-bit register access, mirroring HLL_DENSE_GET/SET_REGISTER exactly ---

// hllDenseGetRegister reads register regnum out of the packed 6-bit array. The last register's high
// byte would read one past the array; since its contribution is masked away we treat that byte as
// zero rather than allocate a guard byte, which keeps the stored blob exactly the dense length.
func hllDenseGetRegister(reg []byte, regnum int) uint8 {
	b := regnum * hllBits / 8
	fb := uint(regnum*hllBits) & 7
	fb8 := 8 - fb
	b0 := uint(reg[b])
	b1 := uint(0)
	if b+1 < len(reg) {
		b1 = uint(reg[b+1])
	}
	return uint8(((b0 >> fb) | (b1 << fb8)) & hllRegisterMax)
}

// hllDenseSetRegister writes val into register regnum. The high-byte update for the last register is
// a no-op (its bits do not spill past the array), so guarding the second byte matches Redis's bytes.
func hllDenseSetRegister(reg []byte, regnum int, val uint8) {
	b := regnum * hllBits / 8
	fb := uint(regnum*hllBits) & 7
	fb8 := 8 - fb
	v := uint(val)
	reg[b] &= ^byte(hllRegisterMax << fb)
	reg[b] |= byte(v << fb)
	if b+1 < len(reg) {
		reg[b+1] &= ^byte(hllRegisterMax >> fb8)
		reg[b+1] |= byte(v >> fb8)
	}
}

// hllDenseSet updates register index to count if count is larger, returning 1 on change else 0.
func hllDenseSet(reg []byte, index int, count uint8) int {
	old := hllDenseGetRegister(reg, index)
	if count > old {
		hllDenseSetRegister(reg, index, count)
		return 1
	}
	return 0
}

// --- hashing and pattern length ---

// murmurHash64A is Redis's 64-bit MurmurHash, byte-for-byte, so an element maps to the same register
// and value under our implementation and Redis's. The seed 0xadc83b19 is the one Redis uses.
func murmurHash64A(data []byte, seed uint64) uint64 {
	const m = 0xc6a4a7935bd1e995
	const r = 47
	l := len(data)
	h := seed ^ (uint64(l) * m)
	n := l - (l & 7)
	for i := 0; i < n; i += 8 {
		k := binary.LittleEndian.Uint64(data[i:])
		k *= m
		k ^= k >> r
		k *= m
		h ^= k
		h *= m
	}
	tail := data[n:]
	switch l & 7 {
	case 7:
		h ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(tail[0])
		h *= m
	}
	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}

// hllPatLen hashes an element and returns its register index and the count (leading-zero run plus
// one) that register would take. The high bit is forced so the loop terminates with count <= Q+1.
func hllPatLen(ele []byte) (int, uint8) {
	hash := murmurHash64A(ele, 0xadc83b19)
	index := int(hash & hllPMask)
	hash >>= hllP
	hash |= uint64(1) << hllQ
	var bit uint64 = 1
	count := uint8(1)
	for hash&bit == 0 {
		count++
		bit <<= 1
	}
	return index, count
}

// --- sparse update path, mirroring hllSparseSet ---

// hllSparseSet sets register index to count on a sparse blob if count is larger, splicing the opcode
// stream in place and merging adjacent VAL opcodes afterward. It returns the possibly reallocated
// blob and a status: 1 if a register changed, 0 if not, -1 if the blob is corrupt. When the value or
// the size exceeds the sparse limits the blob is promoted to dense and the dense set is applied.
func hllSparseSet(blob []byte, index int, count uint8) ([]byte, int) {
	if int(count) > hllSparseValMaxValue {
		return hllSparsePromote(blob, index, count)
	}

	// Step 1: locate the opcode covering register index.
	p := hllHdrSize
	end := len(blob)
	first := 0
	prev := -1
	span := 0
	for p < end {
		oplen := 1
		var s int
		switch {
		case sparseIsZero(blob[p]):
			s = sparseZeroLen(blob[p])
		case sparseIsVal(blob[p]):
			s = sparseValLen(blob[p])
		default: // XZERO
			s = sparseXzeroLen(blob[p], blob[p+1])
			oplen = 2
		}
		span = s
		if index <= first+span-1 {
			break
		}
		prev = p
		p += oplen
		first += span
	}
	if span == 0 || p >= end {
		return blob, -1
	}

	next := p + 1
	if sparseIsXzero(blob[p]) {
		next = p + 2
	}
	if next >= end {
		next = -1
	}

	isZero, isXzero, isVal := false, false, false
	runlen := 0
	switch {
	case sparseIsZero(blob[p]):
		isZero = true
		runlen = sparseZeroLen(blob[p])
	case sparseIsXzero(blob[p]):
		isXzero = true
		runlen = sparseXzeroLen(blob[p], blob[p+1])
	default:
		isVal = true
		runlen = sparseValLen(blob[p])
	}

	// Step 2: trivial in-place cases.
	if isVal {
		old := sparseValValue(blob[p])
		if int(old) >= int(count) { // Case A: already at least this value.
			return blob, 0
		}
		if runlen == 1 { // Case B: a length-1 VAL, just bump it.
			sparseValSet(blob, p, count, 1)
			return hllSparseUpdated(blob, prev)
		}
	}
	if isZero && runlen == 1 { // Case C: a length-1 ZERO becomes a VAL.
		sparseValSet(blob, p, count, 1)
		return hllSparseUpdated(blob, prev)
	}

	// Step D: general split into up to five opcodes (worst case XZERO-VAL-XZERO).
	var seq [5]byte
	n := 0
	last := first + span - 1
	if isZero || isXzero {
		if index != first {
			l := index - first
			if l > hllSparseZeroMaxLen {
				sparseXzeroSet(seq[:], n, l)
				n += 2
			} else {
				sparseZeroSet(seq[:], n, l)
				n++
			}
		}
		sparseValSet(seq[:], n, count, 1)
		n++
		if index != last {
			l := last - index
			if l > hllSparseZeroMaxLen {
				sparseXzeroSet(seq[:], n, l)
				n += 2
			} else {
				sparseZeroSet(seq[:], n, l)
				n++
			}
		}
	} else {
		curval := sparseValValue(blob[p])
		if index != first {
			l := index - first
			sparseValSet(seq[:], n, curval, l)
			n++
		}
		sparseValSet(seq[:], n, count, 1)
		n++
		if index != last {
			l := last - index
			sparseValSet(seq[:], n, curval, l)
			n++
		}
	}

	// Step 3: substitute the new sequence for the old opcode.
	oldlen := 1
	if isXzero {
		oldlen = 2
	}
	deltalen := n - oldlen
	if deltalen > 0 && len(blob)+deltalen > hllSparseMaxBytes {
		return hllSparsePromote(blob, index, count)
	}
	newblob := make([]byte, 0, len(blob)+deltalen)
	newblob = append(newblob, blob[:p]...)
	newblob = append(newblob, seq[:n]...)
	newblob = append(newblob, blob[p+oldlen:]...)
	return hllSparseUpdated(newblob, prev)
}

// hllSparseUpdated runs Step 4: it merges adjacent VAL opcodes of equal value where the combined run
// still fits a VAL length, scanning up to five opcodes from prev, then invalidates the cache. It
// returns the possibly shortened blob and status 1.
func hllSparseUpdated(blob []byte, prev int) ([]byte, int) {
	p := prev
	if p < 0 {
		p = hllHdrSize
	}
	end := len(blob)
	scanlen := 5
	for p < end && scanlen > 0 {
		scanlen--
		if sparseIsXzero(blob[p]) {
			p += 2
			continue
		}
		if sparseIsZero(blob[p]) {
			p++
			continue
		}
		if p+1 < end && sparseIsVal(blob[p+1]) {
			v1 := sparseValValue(blob[p])
			v2 := sparseValValue(blob[p+1])
			if v1 == v2 {
				l := sparseValLen(blob[p]) + sparseValLen(blob[p+1])
				if l <= hllSparseValMaxLen {
					sparseValSet(blob, p+1, v1, l)
					copy(blob[p:end], blob[p+1:end])
					end--
					blob = blob[:end]
					continue
				}
			}
		}
		p++
	}
	hllInvalidateCache(blob)
	return blob, 1
}

// hllSparsePromote converts a sparse blob to dense then applies the set, returning the dense blob.
func hllSparsePromote(blob []byte, index int, count uint8) ([]byte, int) {
	dense, ok := hllSparseToDense(blob)
	if !ok {
		return blob, -1
	}
	hllDenseSet(dense[hllHdrSize:], index, count)
	return dense, 1
}

// hllSparseToDense expands a sparse blob into the fixed-size dense form, returning ok=false if the
// opcode stream does not cover exactly 16384 registers (a corrupt blob).
func hllSparseToDense(blob []byte) ([]byte, bool) {
	if hllEncoding(blob) == hllDense {
		return blob, true
	}
	dense := make([]byte, hllDenseSize)
	copy(dense[:hllHdrSize], blob[:hllHdrSize])
	dense[4] = hllDense
	reg := dense[hllHdrSize:]
	p := hllHdrSize
	end := len(blob)
	idx := 0
	for p < end {
		switch {
		case sparseIsZero(blob[p]):
			idx += sparseZeroLen(blob[p])
			p++
		case sparseIsXzero(blob[p]):
			idx += sparseXzeroLen(blob[p], blob[p+1])
			p += 2
		default:
			rl := sparseValLen(blob[p])
			rv := sparseValValue(blob[p])
			if rl+idx > hllRegisters {
				return nil, false
			}
			for rl > 0 {
				hllDenseSetRegister(reg, idx, rv)
				idx++
				rl--
			}
			p++
		}
	}
	if idx != hllRegisters {
		return nil, false
	}
	return dense, true
}

// hllAdd adds one element to the blob, dispatching on the encoding, returning the possibly grown blob
// and the set status (1 changed, 0 unchanged, -1 corrupt).
func hllAdd(blob []byte, ele []byte) ([]byte, int) {
	index, count := hllPatLen(ele)
	switch hllEncoding(blob) {
	case hllDense:
		return blob, hllDenseSet(blob[hllHdrSize:], index, count)
	case hllSparse:
		return hllSparseSet(blob, index, count)
	default:
		return blob, -1
	}
}

// hllMerge folds blob's registers into max (a 16384-byte raw register array) by register-wise
// maximum. It returns false on a corrupt sparse stream. This is the union kernel shared by PFCOUNT's
// multi-key path and PFMERGE.
func hllMerge(max []byte, blob []byte) bool {
	if hllEncoding(blob) == hllDense {
		reg := blob[hllHdrSize:]
		for i := 0; i < hllRegisters; i++ {
			v := hllDenseGetRegister(reg, i)
			if v > max[i] {
				max[i] = v
			}
		}
		return true
	}
	p := hllHdrSize
	end := len(blob)
	i := 0
	for p < end {
		switch {
		case sparseIsZero(blob[p]):
			i += sparseZeroLen(blob[p])
			p++
		case sparseIsXzero(blob[p]):
			i += sparseXzeroLen(blob[p], blob[p+1])
			p += 2
		default:
			rl := sparseValLen(blob[p])
			rv := sparseValValue(blob[p])
			if rl+i > hllRegisters {
				return false
			}
			for rl > 0 {
				if rv > max[i] {
					max[i] = rv
				}
				i++
				rl--
			}
			p++
		}
	}
	return i == hllRegisters
}

// --- estimator, mirroring hllCount / hllTau / hllSigma from Redis ---

// hllSigma and hllTau are the correction functions from Ertl's estimator, computed to convergence in
// double precision. The temporaries keep each multiply a separate statement so the arithmetic tracks
// Redis's double math rather than fusing into an FMA.
func hllSigma(x float64) float64 {
	if x == 1.0 {
		return math.Inf(1)
	}
	y := 1.0
	z := x
	for {
		x = x * x
		zPrime := z
		prod := x * y
		z += prod
		y += y
		if zPrime == z {
			break
		}
	}
	return z
}

func hllTau(x float64) float64 {
	if x == 0.0 || x == 1.0 {
		return 0.0
	}
	y := 1.0
	z := 1 - x
	for {
		x = math.Sqrt(x)
		zPrime := z
		y *= 0.5
		t := 1 - x
		prod := math.Pow(t, 2) * y
		z -= prod
		if zPrime == z {
			break
		}
	}
	return z / 3
}

// hllRegHisto builds the 64-bucket histogram of register values for the blob, dispatching on the
// encoding. It returns false if a sparse stream is corrupt.
func hllRegHisto(blob []byte, histo *[64]int) bool {
	switch hllEncoding(blob) {
	case hllDense:
		reg := blob[hllHdrSize:]
		for j := 0; j < hllRegisters; j++ {
			histo[hllDenseGetRegister(reg, j)]++
		}
		return true
	case hllRaw:
		reg := blob[hllHdrSize:]
		for j := 0; j < hllRegisters; j++ {
			histo[reg[j]]++
		}
		return true
	case hllSparse:
		idx := 0
		p := hllHdrSize
		end := len(blob)
		for p < end {
			if sparseIsZero(blob[p]) {
				rl := sparseZeroLen(blob[p])
				idx += rl
				histo[0] += rl
				p++
			} else if sparseIsXzero(blob[p]) {
				rl := sparseXzeroLen(blob[p], blob[p+1])
				idx += rl
				histo[0] += rl
				p += 2
			} else {
				rl := sparseValLen(blob[p])
				rv := sparseValValue(blob[p])
				if rl+idx > hllRegisters {
					break
				}
				idx += rl
				histo[rv] += rl
				p++
			}
		}
		return idx == hllRegisters
	default:
		return false
	}
}

// hllCount estimates the cardinality of the blob using the register histogram and Ertl's bias
// correction. It returns the estimate and false if the blob is corrupt. The arithmetic order and the
// llround-equivalent final rounding mirror Redis so the integer result matches byte-for-byte.
func hllCount(blob []byte) (uint64, bool) {
	var histo [64]int
	if !hllRegHisto(blob, &histo) {
		return 0, false
	}
	m := float64(hllRegisters)
	z := m * hllTau((m-float64(histo[hllQ+1]))/m)
	for j := hllQ; j >= 1; j-- {
		z += float64(histo[j])
		z *= 0.5
	}
	z += m * hllSigma(float64(histo[0])/m)
	e := math.Round(hllAlphaInf * m * m / z)
	return uint64(e), true
}

// --- commands ---

func (c *connState) cmdPfAdd(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'pfadd' command")
		return
	}
	key := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()

	v, hit := c.srv.store.Get(key, nil)
	updated := 0
	var blob []byte
	if !hit {
		if c.collConflict(key) {
			c.writeErr(wrongType)
			return
		}
		blob = hllCreate()
		updated++
	} else {
		if !hllValid(v) {
			c.writeErr(hllNotValidErr)
			return
		}
		blob = v
	}
	for j := 2; j < len(argv); j++ {
		nb, ret := hllAdd(blob, argv[j])
		blob = nb
		switch ret {
		case 1:
			updated++
		case -1:
			c.writeErr(hllCorruptErr)
			return
		}
	}
	if updated > 0 {
		hllInvalidateCache(blob)
		if err := c.srv.store.Set(key, blob); err != nil {
			c.writeErr("ERR " + err.Error())
			return
		}
		c.writeInt(1)
		return
	}
	c.writeInt(0)
}

func (c *connState) cmdPfCount(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'pfcount' command")
		return
	}
	if len(argv) > 2 {
		c.pfCountUnion(argv)
		return
	}

	key := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()

	v, hit := c.srv.store.Get(key, nil)
	if !hit {
		if c.collConflict(key) {
			c.writeErr(wrongType)
			return
		}
		c.writeInt(0)
		return
	}
	if !hllValid(v) {
		c.writeErr(hllNotValidErr)
		return
	}
	if hllValidCache(v) {
		c.writeInt(int64(hllReadCache(v)))
		return
	}
	card, ok := hllCount(v)
	if !ok {
		c.writeErr(hllCorruptErr)
		return
	}
	hllWriteCache(v, card)
	if err := c.srv.store.Set(key, v); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	c.writeInt(int64(card))
}

// pfCountUnion answers a multi-key PFCOUNT: the cardinality of the union, computed by folding every
// input into a raw register array and estimating over it. No input key is modified.
func (c *connState) pfCountUnion(argv [][]byte) {
	keys := argv[1:]
	unlock := c.lockStripes(keys)
	defer unlock()

	max := make([]byte, hllRegisters)
	for _, key := range keys {
		v, hit := c.srv.store.Get(key, nil)
		if !hit {
			if c.collConflict(key) {
				c.writeErr(wrongType)
				return
			}
			continue
		}
		if !hllValid(v) {
			c.writeErr(hllNotValidErr)
			return
		}
		if !hllMerge(max, v) {
			c.writeErr(hllCorruptErr)
			return
		}
	}
	raw := make([]byte, hllHdrSize+hllRegisters)
	raw[4] = hllRaw
	copy(raw[hllHdrSize:], max)
	card, _ := hllCount(raw)
	c.writeInt(int64(card))
}

func (c *connState) cmdPfMerge(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'pfmerge' command")
		return
	}
	keys := argv[1:]
	unlock := c.lockStripes(keys)
	defer unlock()

	// Fold every input (including the destination if it exists) into the raw max array.
	max := make([]byte, hllRegisters)
	useDense := false
	for _, key := range keys {
		v, hit := c.srv.store.Get(key, nil)
		if !hit {
			if c.collConflict(key) {
				c.writeErr(wrongType)
				return
			}
			continue
		}
		if !hllValid(v) {
			c.writeErr(hllNotValidErr)
			return
		}
		if hllEncoding(v) == hllDense {
			useDense = true
		}
		if !hllMerge(max, v) {
			c.writeErr(hllCorruptErr)
			return
		}
	}

	dest := argv[1]
	dv, hit := c.srv.store.Get(dest, nil)
	var blob []byte
	if !hit {
		blob = hllCreate()
	} else {
		blob = dv // already validated in the fold loop.
	}
	if useDense {
		d, ok := hllSparseToDense(blob)
		if !ok {
			c.writeErr(hllCorruptErr)
			return
		}
		blob = d
	}
	for j := 0; j < hllRegisters; j++ {
		if max[j] == 0 {
			continue
		}
		switch hllEncoding(blob) {
		case hllDense:
			hllDenseSet(blob[hllHdrSize:], j, max[j])
		case hllSparse:
			nb, _ := hllSparseSet(blob, j, max[j])
			blob = nb
		}
	}
	hllInvalidateCache(blob)
	if err := c.srv.store.Set(dest, blob); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	c.writeSimple("OK")
}

func (c *connState) cmdPfDebug(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'pfdebug' command")
		return
	}
	sub := argv[1]
	key := argv[2]
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()

	v, hit := c.srv.store.Get(key, nil)
	if !hit {
		c.writeErr("ERR The specified key does not exist")
		return
	}
	if !hllValid(v) {
		c.writeErr(hllNotValidErr)
		return
	}
	switch {
	case eqFold(sub, "GETREG"):
		if hllEncoding(v) == hllSparse {
			d, ok := hllSparseToDense(v)
			if !ok {
				c.writeErr(hllCorruptErr)
				return
			}
			v = d
			if err := c.srv.store.Set(key, v); err != nil {
				c.writeErr("ERR " + err.Error())
				return
			}
		}
		reg := v[hllHdrSize:]
		c.writeArrayHeader(hllRegisters)
		for j := 0; j < hllRegisters; j++ {
			c.writeInt(int64(hllDenseGetRegister(reg, j)))
		}
	case eqFold(sub, "ENCODING"):
		if hllEncoding(v) == hllDense {
			c.writeSimple("dense")
		} else {
			c.writeSimple("sparse")
		}
	case eqFold(sub, "TODENSE"):
		changed := int64(0)
		if hllEncoding(v) == hllSparse {
			d, ok := hllSparseToDense(v)
			if !ok {
				c.writeErr(hllCorruptErr)
				return
			}
			if err := c.srv.store.Set(key, d); err != nil {
				c.writeErr("ERR " + err.Error())
				return
			}
			changed = 1
		}
		c.writeInt(changed)
	default:
		c.writeErr("ERR unknown PFDEBUG subcommand or wrong number of arguments")
	}
}

// cmdPfSelfTest runs a bounded self-consistency check of the HLL implementation: it adds a spread of
// synthetic elements, verifies each estimate is within a generous multiple of the standard error,
// and checks the dense expansion of a sparse sketch reproduces the same registers. It replies OK on
// success. It is a correctness gate, not a data command, so it holds no key.
func (c *connState) cmdPfSelfTest(argv [][]byte) {
	blob := hllCreate()
	// Standard error of HLL at p=14 is ~1.04/sqrt(m); allow a wide band for the probabilistic tail.
	relErr := 1.04 / math.Sqrt(float64(hllRegisters))
	checks := []int{100, 1000, 10000, 50000}
	next := 0
	var buf [16]byte
	for target := 1; target <= 50000; target++ {
		binary.LittleEndian.PutUint64(buf[:8], uint64(target))
		binary.LittleEndian.PutUint64(buf[8:], uint64(target)*0x9e3779b1)
		nb, ret := hllAdd(blob, buf[:])
		if ret == -1 {
			c.writeErr(hllCorruptErr)
			return
		}
		blob = nb
		if next < len(checks) && target == checks[next] {
			next++
			card, ok := hllCount(blob)
			if !ok {
				c.writeErr(hllCorruptErr)
				return
			}
			tol := float64(target) * relErr * 5.0
			if math.Abs(float64(card)-float64(target)) > tol+10 {
				c.writeErr("ERR TESTFAILED estimate out of range")
				return
			}
		}
	}
	// A sparse-then-dense round trip must reproduce identical registers.
	sparse := hllCreate()
	for i := 0; i < 200; i++ {
		binary.LittleEndian.PutUint64(buf[:8], uint64(i)*0xff51afd7)
		sparse, _ = hllAdd(sparse, buf[:8])
	}
	dense, ok := hllSparseToDense(sparse)
	if !ok {
		c.writeErr(hllCorruptErr)
		return
	}
	var hSparse, hDense [64]int
	hllRegHisto(sparse, &hSparse)
	hllRegHisto(dense, &hDense)
	if hSparse != hDense {
		c.writeErr("ERR TESTFAILED sparse/dense register mismatch")
		return
	}
	c.writeSimple("OK")
}
