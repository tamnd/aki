package derived

// The HYLL codec (spec 2064/f3/15 section 7.1): the packed 6-bit dense register
// access, the sparse opcode splice that PFADD runs on a low-cardinality sketch,
// and the one-way sparse-to-dense promotion. Every routine here is ported from
// Redis hyperloglog.c so the sketch bytes stay byte-for-byte compatible: the
// sparse opcode stream a sequence of PFADDs produces in aki must match the stream
// the same PFADDs produce in Redis, because GET/SET expose those bytes.

// denseGet reads register index from a packed little-endian 6-bit array. A
// register straddling the final byte reads one byte past its start; that byte's
// contribution is always masked away, so when it falls past the array (the last
// register) it reads as zero, matching Redis's reliance on the sds terminator.
func denseGet(regs []byte, index int) byte {
	b := (index * hllBits) / 8
	fb := uint(index*hllBits) & 7
	b0 := uint(regs[b])
	var b1 uint
	if b+1 < len(regs) {
		b1 = uint(regs[b+1])
	}
	return byte(((b0 >> fb) | (b1 << (8 - fb))) & hllRegMax)
}

// denseSetReg writes val into register index, the inverse packing of denseGet.
// The second byte is only touched when the register straddles a byte boundary and
// stays in range; for the final register the straddle write is a no-op, so the
// range guard changes nothing but the panic.
func denseSetReg(regs []byte, index int, val byte) {
	b := (index * hllBits) / 8
	fb := uint(index*hllBits) & 7
	v := uint(val)
	regs[b] &= byte(^(uint(hllRegMax) << fb))
	regs[b] |= byte(v << fb)
	if b+1 < len(regs) {
		regs[b+1] &= byte(^(uint(hllRegMax) >> (8 - fb)))
		regs[b+1] |= byte(v >> (8 - fb))
	}
}

// denseSet keeps the register at the max of its value and count, returning 1 if
// it grew and 0 otherwise.
func denseSet(regs []byte, index int, count byte) int {
	if count > denseGet(regs, index) {
		denseSetReg(regs, index, count)
		return 1
	}
	return 0
}

// Sparse opcode readers and writers, the Redis HLL_SPARSE_* macros. ZERO is a
// 1-byte run of up to 64 zeros, XZERO a 2-byte run of up to 16384 zeros, VAL a
// 1-byte run of up to 4 registers holding a value 1..32.
func sparseIsZero(op byte) bool  { return op&0xc0 == 0 }
func sparseIsXZero(op byte) bool { return op&0xc0 == 0x40 }
func sparseIsVal(op byte) bool   { return op&0x80 != 0 }

func sparseZeroLen(op byte) int          { return int(op&0x3f) + 1 }
func sparseXZeroLen(op0, op1 byte) int   { return int(op0&0x3f)<<8 | int(op1) + 1 }
func sparseValValue(op byte) int         { return int((op>>2)&0x1f) + 1 }
func sparseValLen(op byte) int           { return int(op&0x3) + 1 }
func sparseValByte(val, runLen int) byte { return byte((val-1)<<2|(runLen-1)) | 0x80 }
func sparseZeroByte(runLen int) byte     { return byte(runLen - 1) }
func sparseXZeroBytes(runLen int) (byte, byte) {
	l := runLen - 1
	return byte(l>>8) | 0x40, byte(l & 0xff)
}

// appendXZero appends an XZERO opcode covering run zero registers.
func appendXZero(blob []byte, run int) []byte {
	b0, b1 := sparseXZeroBytes(run)
	return append(blob, b0, b1)
}

// spliceBytes replaces blob[at:at+oldLen] with ins, returning the new slice. It
// allocates rather than shifting in place because a sparse sketch is at most a
// few thousand bytes and byte-identical output, not the allocation, is the
// contract this slice defends.
func spliceBytes(blob []byte, at, oldLen int, ins []byte) []byte {
	out := make([]byte, 0, len(blob)-oldLen+len(ins))
	out = append(out, blob[:at]...)
	out = append(out, ins...)
	out = append(out, blob[at+oldLen:]...)
	return out
}

// sparseSet applies register index = count to a sparse sketch, returning the
// possibly reallocated sketch and 1/0/-1 for grew/unchanged/corrupt. It ports
// Redis hllSparseSet: locate the covering opcode, handle the trivial in-place
// cases, else split the run into up to five opcodes and splice, then merge
// adjacent equal VAL runs. A value past the VAL ceiling or a splice past the byte
// budget promotes to dense.
func sparseSet(blob []byte, index int, count byte) ([]byte, int) {
	if int(count) > hllSparseValMaxValue {
		return promoteAndSet(blob, index, count)
	}

	// Step 1: locate the opcode covering register index.
	p := hllHdrSize
	end := len(blob)
	first := 0
	prev := -1
	span := 0
	for p < end {
		oplen := 1
		op := blob[p]
		switch {
		case sparseIsZero(op):
			span = sparseZeroLen(op)
		case sparseIsVal(op):
			span = sparseValLen(op)
		default: // XZERO
			if p+1 >= end {
				return blob, -1
			}
			span = sparseXZeroLen(blob[p], blob[p+1])
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
		return blob, -1
	}

	op := blob[p]
	isZero := sparseIsZero(op)
	isXZero := sparseIsXZero(op)
	isVal := sparseIsVal(op)
	var runLen int
	switch {
	case isVal:
		runLen = sparseValLen(op)
	case isZero:
		runLen = sparseZeroLen(op)
	default:
		if p+1 >= end {
			return blob, -1
		}
		runLen = sparseXZeroLen(blob[p], blob[p+1])
	}

	// Step 2: the trivial cases that update in place.
	if isVal {
		oldcount := byte(sparseValValue(op))
		if oldcount >= count { // Case A: already at least as high.
			return blob, 0
		}
		if runLen == 1 { // Case B: a length-1 VAL is just overwritten.
			blob[p] = sparseValByte(int(count), 1)
			return sparseMerge(blob, prev), 1
		}
	}
	if isZero && runLen == 1 { // Case C: a length-1 ZERO becomes a VAL.
		blob[p] = sparseValByte(int(count), 1)
		return sparseMerge(blob, prev), 1
	}

	// Step 3 (Case D): split the run. The worst case is XZERO to
	// XZERO-VAL-XZERO, at most five bytes.
	var seq [5]byte
	n := 0
	last := first + span - 1
	if isZero || isXZero {
		if index != first {
			n += writeZeroRun(seq[n:], index-first)
		}
		seq[n] = sparseValByte(int(count), 1)
		n++
		if index != last {
			n += writeZeroRun(seq[n:], last-index)
		}
	} else { // splitting a VAL run: the flanks keep the old value.
		curval := sparseValValue(op)
		if index != first {
			seq[n] = sparseValByte(curval, index-first)
			n++
		}
		seq[n] = sparseValByte(int(count), 1)
		n++
		if index != last {
			seq[n] = sparseValByte(curval, last-index)
			n++
		}
	}

	oldLen := 1
	if isXZero {
		oldLen = 2
	}
	if n-oldLen > 0 && len(blob)+(n-oldLen) > hllSparseMaxBytes {
		return promoteAndSet(blob, index, count)
	}
	blob = spliceBytes(blob, p, oldLen, seq[:n])
	return sparseMerge(blob, prev), 1
}

// writeZeroRun writes a ZERO or XZERO opcode for a run of runLen zeros into dst
// and returns the bytes written, the ZERO-versus-XZERO choice Redis makes by the
// 64-register ZERO ceiling.
func writeZeroRun(dst []byte, runLen int) int {
	if runLen > hllSparseZeroMaxLen {
		dst[0], dst[1] = sparseXZeroBytes(runLen)
		return 2
	}
	dst[0] = sparseZeroByte(runLen)
	return 1
}

// sparseMerge coalesces adjacent equal-value VAL opcodes whose combined length
// fits the VAL ceiling, scanning up to five opcodes from prev, then invalidates
// the cache. This is Redis's step 4; it keeps the sparse form minimal so the
// bytes match Redis's after the same operation.
func sparseMerge(blob []byte, prev int) []byte {
	p := prev
	if p < 0 {
		p = hllHdrSize
	}
	end := len(blob)
	scan := 5
	for p < end && scan > 0 {
		scan--
		op := blob[p]
		if sparseIsXZero(op) {
			p += 2
			continue
		}
		if sparseIsZero(op) {
			p++
			continue
		}
		if p+1 < end && sparseIsVal(blob[p+1]) {
			v1 := sparseValValue(blob[p])
			v2 := sparseValValue(blob[p+1])
			if v1 == v2 {
				l := sparseValLen(blob[p]) + sparseValLen(blob[p+1])
				if l <= hllSparseValMaxLen {
					blob[p+1] = sparseValByte(v1, l)
					blob = append(blob[:p], blob[p+1:]...)
					end--
					continue // retry the merged VAL against its new neighbour
				}
			}
		}
		p++
	}
	invalidateCache(blob)
	return blob
}

// sparseToDense decodes a sparse sketch into a fresh dense record, preserving the
// header verbatim including the cache and stale bit. It returns false on a
// malformed opcode stream (one that does not cover exactly 16384 registers), the
// corruption Redis reports rather than trusting.
func sparseToDense(blob []byte) ([]byte, bool) {
	dense := make([]byte, hllDenseSize)
	copy(dense[:hllHdrSize], blob[:hllHdrSize])
	dense[4] = hllDense
	regs := dense[hllHdrSize:]

	idx := 0
	p := hllHdrSize
	end := len(blob)
	for p < end {
		op := blob[p]
		switch {
		case sparseIsZero(op):
			idx += sparseZeroLen(op)
			p++
		case sparseIsXZero(op):
			if p+1 >= end {
				return nil, false
			}
			idx += sparseXZeroLen(blob[p], blob[p+1])
			p += 2
		default: // VAL
			runLen := sparseValLen(op)
			regval := byte(sparseValValue(op))
			if runLen+idx > hllRegisters {
				return nil, false
			}
			for k := 0; k < runLen; k++ {
				denseSetReg(regs, idx, regval)
				idx++
			}
			p++
		}
	}
	if idx != hllRegisters {
		return nil, false
	}
	return dense, true
}

// promoteAndSet densifies the sketch and applies the register set on the dense
// form, the one-way sparse-to-dense transition of F4. A promotion only fires when
// the register must grow past what sparse can hold, so the dense set always
// returns 1; a corrupt source returns -1.
func promoteAndSet(blob []byte, index int, count byte) ([]byte, int) {
	dense, ok := sparseToDense(blob)
	if !ok {
		return blob, -1
	}
	denseSet(dense[hllHdrSize:], index, count)
	return dense, 1
}
