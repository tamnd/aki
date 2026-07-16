package derived

import "math"

// The PFCOUNT recompute (spec 2064/f3/15 section 7.3): fold the 16384 registers
// into a 64-entry value histogram, then run the closed-form Ertl estimator over
// it. The estimator is ported constant-for-constant from Redis hyperloglog.c so
// PFCOUNT on identical bytes returns the number a same-version Redis returns. The
// labs/f3/m6/05_hll_recompute lab prices this path and pins the estimator against
// known cardinalities.

// hllAlphaInf is Redis's HLL_ALPHA_INF, 0.5/ln(2), the Ertl estimator's
// normalizer. It is the one constant the port cannot get approximately right: the
// Euler-Mascheroni constant sits nearby and yields a flat 0.8x undercount.
const hllAlphaInf = 0.7213475204444817

// hllSigma is the Ertl correction for the zero-register tail.
func hllSigma(x float64) float64 {
	if x == 1.0 {
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

// hllTau is the Ertl correction for the saturated-register end.
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
		z -= (1 - x) * (1 - x) * y
		if zPrime == z {
			return z / 3
		}
	}
}

// estimateHisto turns a register-value histogram into a cardinality with the
// Ertl closed form: the tau term for the saturated end, a halving sweep over the
// middle, the sigma term for the zero registers.
func estimateHisto(histo *[64]int) uint64 {
	m := float64(hllRegisters)
	z := m * hllTau((m-float64(histo[hllQ+1]))/m)
	for j := hllQ; j >= 1; j-- {
		z += float64(histo[j])
		z *= 0.5
	}
	z += m * hllSigma(float64(histo[0])/m)
	return uint64(math.Round(hllAlphaInf * m * m / z))
}

// denseRegHisto folds a packed dense register array into the histogram 12 bytes
// at a time, peeling 16 registers per step with fixed shifts. HLL_REGISTERS is a
// multiple of 16, so the loop never has a tail and never reads past the array.
func denseRegHisto(regs []byte, histo *[64]int) {
	r := regs
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
		histo[b0&63]++
		histo[((b0>>6)|(b1<<2))&63]++
		histo[((b1>>4)|(b2<<4))&63]++
		histo[(b2>>2)&63]++
		histo[b3&63]++
		histo[((b3>>6)|(b4<<2))&63]++
		histo[((b4>>4)|(b5<<4))&63]++
		histo[(b5>>2)&63]++
		histo[b6&63]++
		histo[((b6>>6)|(b7<<2))&63]++
		histo[((b7>>4)|(b8<<4))&63]++
		histo[(b8>>2)&63]++
		histo[b9&63]++
		histo[((b9>>6)|(b10<<2))&63]++
		histo[((b10>>4)|(b11<<4))&63]++
		histo[(b11>>2)&63]++
		r = r[12:]
	}
}

// sparseRegHisto folds a sparse opcode stream into the histogram: ZERO and XZERO
// runs land in bucket 0, VAL runs in their value bucket. It reports false on an
// opcode stream that does not cover exactly 16384 registers, the corruption the
// estimator refuses to guess through.
func sparseRegHisto(opcodes []byte, histo *[64]int) bool {
	idx := 0
	p := 0
	end := len(opcodes)
	for p < end {
		op := opcodes[p]
		switch {
		case sparseIsZero(op):
			runLen := sparseZeroLen(op)
			idx += runLen
			histo[0] += runLen
			p++
		case sparseIsXZero(op):
			if p+1 >= end {
				return false
			}
			runLen := sparseXZeroLen(opcodes[p], opcodes[p+1])
			idx += runLen
			histo[0] += runLen
			p += 2
		default: // VAL
			runLen := sparseValLen(op)
			regval := sparseValValue(op)
			if runLen+idx > hllRegisters {
				return false
			}
			histo[regval] += runLen
			idx += runLen
			p++
		}
	}
	return idx == hllRegisters
}

// hllCount estimates the cardinality of a validated sketch, dense or sparse,
// returning false on a corrupt sparse stream. It is the cache-miss body of
// PFCOUNT and the reader half of multi-key PFCOUNT.
func hllCount(blob []byte) (uint64, bool) {
	var histo [64]int
	if blob[4] == hllDense {
		denseRegHisto(blob[hllHdrSize:], &histo)
	} else {
		if !sparseRegHisto(blob[hllHdrSize:], &histo) {
			return 0, false
		}
	}
	return estimateHisto(&histo), true
}
