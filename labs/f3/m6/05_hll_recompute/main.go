// Command 05_hll_recompute prices the PFCOUNT cache-miss recompute kernel.
//
// A PFCOUNT with the cache-stale bit set cannot return the cached 8-byte count.
// It has to walk all 16384 six-bit registers, fold them into a 64-entry value
// histogram, and run the closed-form Ertl estimator over that histogram. This is
// the whole cost of a cold PFCOUNT, so the slice needs to know two things before
// it bakes the kernel in: how the histogram build should read the packed array,
// and how big the bounded scan actually is.
//
// The build has two honest shapes. The naive shape extracts one register at a
// time with the straddling shift-or macro, 16384 times. The word shape reads the
// array 12 bytes at a time and peels 16 registers per step with fixed shifts,
// which is the same layout Redis's dense reghisto fast path uses. Both feed the
// same histogram and the same estimator, so the estimate is identical and only
// the build cost moves. The estimator itself is a fixed 52-entry fold, dwarfed by
// the 16384-register walk, so the build shape is the whole lever.
package main

import (
	"fmt"
	"math"
	"time"
)

const (
	hllP         = 14
	hllRegisters = 1 << hllP // 16384
	hllBits      = 6
	hllRegMax    = 63
	hllQ         = 64 - hllP                  // 50
	regBytes     = hllRegisters * hllBits / 8 // 12288
	hllDenseSize = 16 + regBytes
	hllAlphaInf  = 0.7213475204444817 // 0.5/ln(2), the Ertl estimator normalizer
)

// newRegs allocates a register array plus one guard byte. getRegister reads the
// byte past the last register's start when a register straddles the final byte;
// Redis leans on the sds null terminator for that read, so the lab mirrors it
// with an explicit trailing byte the packing never uses.
func newRegs() []byte {
	return make([]byte, regBytes+1)
}

// getRegister reads register regnum from a packed little-endian 6-bit array, the
// straddling shift-or Redis's HLL_DENSE_GET_REGISTER macro uses.
func getRegister(p []byte, regnum int) byte {
	b := regnum * hllBits / 8
	fb := uint(regnum*hllBits) & 7
	b0 := uint(p[b])
	b1 := uint(p[b+1])
	return byte(((b0 >> fb) | (b1 << (8 - fb))) & hllRegMax)
}

// setRegister writes val into register regnum of a packed little-endian 6-bit
// array, the inverse of getRegister.
func setRegister(p []byte, regnum int, val byte) {
	b := regnum * hllBits / 8
	fb := uint(regnum*hllBits) & 7
	v := uint(val)
	p[b] &= byte(^(uint(hllRegMax) << fb))
	p[b] |= byte(v << fb)
	p[b+1] &= byte(^(uint(hllRegMax) >> (8 - fb)))
	p[b+1] |= byte(v >> (8 - fb))
}

// regHistoNaive folds the register array into a 64-entry value histogram one
// register at a time through the straddling macro.
func regHistoNaive(regs []byte) [64]int {
	var h [64]int
	for i := 0; i < hllRegisters; i++ {
		h[getRegister(regs, i)]++
	}
	return h
}

// regHistoWord folds the register array into the same histogram 12 bytes at a
// time, peeling 16 registers per step with fixed shifts. This is Redis's dense
// reghisto fast path: HLL_REGISTERS is a multiple of 16, so the loop never has a
// tail. The extra locals keep every read register-local instead of chasing the
// straddling byte index.
func regHistoWord(regs []byte) [64]int {
	var h [64]int
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
		h[b0&63]++
		h[((b0>>6)|(b1<<2))&63]++
		h[((b1>>4)|(b2<<4))&63]++
		h[(b2>>2)&63]++
		h[b3&63]++
		h[((b3>>6)|(b4<<2))&63]++
		h[((b4>>4)|(b5<<4))&63]++
		h[(b5>>2)&63]++
		h[b6&63]++
		h[((b6>>6)|(b7<<2))&63]++
		h[((b7>>4)|(b8<<4))&63]++
		h[(b8>>2)&63]++
		h[b9&63]++
		h[((b9>>6)|(b10<<2))&63]++
		h[((b10>>4)|(b11<<4))&63]++
		h[(b11>>2)&63]++
		r = r[12:]
	}
	return h
}

// hllSigma and hllTau are the Ertl refinement terms, ported constant-for-constant
// from Redis's hyperloglog.c so this lab doubles as the estimator reference the
// slice will port.
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
		z -= math.Pow(1-x, 2) * y
		if zPrime == z {
			return z / 3
		}
	}
}

// estimate runs the Ertl closed form over a value histogram, the same fold Redis
// hllCount does: tau term for the saturated end, halving sweep over the middle,
// sigma term for the zero registers.
func estimate(h [64]int) int64 {
	m := float64(hllRegisters)
	z := m * hllTau((m-float64(h[hllQ+1]))/m)
	for j := hllQ; j >= 1; j-- {
		z += float64(h[j])
		z *= 0.5
	}
	z += m * hllSigma(float64(h[0])/m)
	return int64(math.Round(hllAlphaInf * m * m / z))
}

// splitmix64 is a fast deterministic hash for populating a register array to a
// target cardinality. The recompute kernel is hash-independent, so the lab does
// not need Redis's MurmurHash64A here; it needs a realistic register fill.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// fill populates a fresh dense register array as if n distinct elements had been
// added: low 14 bits pick the register, the first set bit in the remaining 50
// bits is the candidate value, keep the max per register.
func fill(n int) []byte {
	regs := newRegs()
	for i := 0; i < n; i++ {
		hash := splitmix64(uint64(i) * 0x2545f4914f6cdd1d)
		idx := int(hash & (hllRegisters - 1))
		rest := hash >> hllP
		val := byte(1)
		for rest&1 == 0 && val <= hllQ {
			val++
			rest >>= 1
		}
		if getRegister(regs, idx) < val {
			setRegister(regs, idx, val)
		}
	}
	return regs
}

func timeHisto(f func([]byte) [64]int, regs []byte, reps int) (time.Duration, int64) {
	// Warm up and pin the estimate so the histogram build cannot be dropped.
	var est int64
	f(regs)
	start := time.Now()
	for i := 0; i < reps; i++ {
		est = estimate(f(regs))
	}
	return time.Since(start) / time.Duration(reps), est
}

func main() {
	fmt.Printf("dense HYLL record is %d bytes, register array %d bytes\n\n",
		hllDenseSize, hllDenseSize-16)
	fmt.Printf("%-12s %10s %10s %8s %12s %12s\n",
		"trueCard", "naive", "word", "speedup", "estNaive", "estWord")

	const reps = 2000
	for _, n := range []int{100, 1000, 10000, 100000, 1000000} {
		regs := fill(n)
		naiveNs, estN := timeHisto(regHistoNaive, regs, reps)
		wordNs, estW := timeHisto(regHistoWord, regs, reps)
		fmt.Printf("%-12d %10s %10s %7.2fx %12d %12d\n",
			n, naiveNs, wordNs, float64(naiveNs)/float64(wordNs), estN, estW)
	}

	fmt.Printf("\nboth builds feed the same histogram, so estNaive == estWord on every\n")
	fmt.Printf("row; only the build moves. the whole cold PFCOUNT is this bounded\n")
	fmt.Printf("%d-byte walk plus a fixed 52-entry fold, no allocation.\n", hllDenseSize-16)
}
