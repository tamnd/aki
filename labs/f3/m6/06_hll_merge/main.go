// Command 06_hll_merge prices the HLL register-merge kernel, the fold under
// PFMERGE and multi-key PFCOUNT.
//
// The merge is, for each of 16384 registers, dst[i] = max(dst[i], src[i]). The
// naive shape does that on the packed 6-bit array: unpack a register, compare,
// repack, 16384 times per source, and the branch on the comparison is the cost
// the vendors' SIMD rewrites deleted. The branchless shape the slice will ship
// unpacks each input once into a one-byte-per-register scratch (word-at-a-time,
// the reghisto layout), folds with a SWAR byte-max on 8-byte words (no branch,
// no lane crossing because every register is at most 63 so the high bit of every
// lane is clear), and repacks once at the end, or feeds the histogram straight
// from the scratch when the caller is PFCOUNT and never repacks at all.
//
// This lab pins three things before the kernel lands: that the two shapes
// produce the byte-identical merged sketch and the identical estimate, that the
// SWAR fold beats the per-register loop, and how the gap moves as the fold takes
// more sources, since a multi-source PFMERGE pays one fold per extra source.
package main

import (
	"fmt"
	"time"
)

const (
	hllP         = 14
	hllRegisters = 1 << hllP // 16384
	hllBits      = 6
	hllRegMax    = 63
	hllQ         = 64 - hllP                  // 50
	regBytes     = hllRegisters * hllBits / 8 // 12288
	hllAlphaInf  = 0.7213475204444817         // 0.5/ln(2), the Ertl normalizer
)

// newRegs allocates a packed register array plus one guard byte, the way the
// engine mirrors Redis's reliance on the sds terminator for the final register's
// straddle read.
func newRegs() []byte { return make([]byte, regBytes+1) }

// getRegister and setRegister are the packed 6-bit accessors, little-endian, the
// naive merge's per-register path.
func getRegister(regs []byte, i int) byte {
	b := i * hllBits / 8
	fb := uint(i*hllBits) & 7
	b0 := uint(regs[b])
	var b1 uint
	if b+1 < len(regs) {
		b1 = uint(regs[b+1])
	}
	return byte(((b0 >> fb) | (b1 << (8 - fb))) & hllRegMax)
}

func setRegister(regs []byte, i int, val byte) {
	b := i * hllBits / 8
	fb := uint(i*hllBits) & 7
	v := uint(val)
	regs[b] &= byte(^(uint(hllRegMax) << fb))
	regs[b] |= byte(v << fb)
	if b+1 < len(regs) {
		regs[b+1] &= byte(^(uint(hllRegMax) >> (8 - fb)))
		regs[b+1] |= byte(v >> (8 - fb))
	}
}

// naiveMerge folds src into dst on the packed form, one register at a time.
func naiveMerge(dst, src []byte) {
	for i := 0; i < hllRegisters; i++ {
		s := getRegister(src, i)
		if s > getRegister(dst, i) {
			setRegister(dst, i, s)
		}
	}
}

// unpack expands a packed 6-bit array into one byte per register, reading 12
// bytes and peeling 16 registers per step so the loop has no tail and never
// branches, the same word layout the reghisto build uses.
func unpack(regs []byte) []byte {
	out := make([]byte, hllRegisters)
	r := regs
	o := out
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
	return out
}

// repack writes a one-byte-per-register scratch back to the packed 6-bit form,
// word-at-a-time: 16 six-bit values contiguously packed into 12 bytes per step,
// the exact inverse of unpack, so the loop has no tail and no per-register
// branch.
func repack(scratch []byte) []byte {
	regs := newRegs()
	r := regs
	s := scratch
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
	return regs
}

// histoFromScratch folds an unpacked one-byte-per-register scratch straight into
// a value histogram, the multi-key PFCOUNT read path that never repacks.
func histoFromScratch(scratch []byte) [64]int {
	var histo [64]int
	for _, v := range scratch {
		histo[v&63]++
	}
	return histo
}

const swarHigh = 0x8080808080808080

// swarByteMaxInto folds src into dst on the unpacked one-byte-per-register form,
// eight lanes per 8-byte word with no branch. Every lane is at most 63, so the
// high bit of every lane is clear: (a|H)-b keeps the high bit set exactly when
// a >= b and never borrows across a lane, so h isolates a per-lane a-ge-b mask,
// expanded to a full-byte select mask with h|(h-(h>>7)).
func swarByteMaxInto(dst, src []byte) {
	n := len(dst) &^ 7
	for i := 0; i < n; i += 8 {
		a := le64(dst[i:])
		b := le64(src[i:])
		h := ((a | swarHigh) - b) & swarHigh
		m := h | (h - (h >> 7)) // 0xff per lane where a >= b, else 0x00
		max := (a & m) | (b &^ m)
		putLE64(dst[i:], max)
	}
}

func le64(b []byte) uint64 {
	_ = b[7]
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

func putLE64(b []byte, v uint64) {
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

// swarMergeAll folds every source into a fresh scratch and repacks once, the
// PFMERGE shape: one unpack per source, one SWAR fold per source, one repack.
func swarMergeAll(srcs [][]byte) []byte {
	acc := unpack(srcs[0])
	for _, s := range srcs[1:] {
		swarByteMaxInto(acc, unpack(s))
	}
	return repack(acc)
}

// naiveMergeAll folds every source into a copy of the first on the packed form,
// the per-register baseline.
func naiveMergeAll(srcs [][]byte) []byte {
	acc := newRegs()
	copy(acc, srcs[0])
	for _, s := range srcs[1:] {
		naiveMerge(acc, s)
	}
	return acc
}

// swarCountUnion is the multi-key PFCOUNT read path: fold every source into the
// scratch and estimate straight from it, no repack and no destination write.
func swarCountUnion(srcs [][]byte) uint64 {
	acc := unpack(srcs[0])
	for _, s := range srcs[1:] {
		swarByteMaxInto(acc, unpack(s))
	}
	return estimateHisto(histoFromScratch(acc))
}

// naiveCountUnion is the same read as the per-register merge would do it: fold on
// the packed form, then estimate.
func naiveCountUnion(srcs [][]byte) uint64 {
	return estimate(naiveMergeAll(srcs))
}

// splitmix64 gives deterministic distinct 64-bit values per element index.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4d7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// fill builds a dense sketch of roughly card distinct elements by hashing an
// index stream and taking the register pattern, the same hash HLL uses.
func fill(seed, card int) []byte {
	regs := newRegs()
	for i := 0; i < card; i++ {
		h := splitmix64(uint64(seed)<<40 ^ uint64(i))
		idx := int(h & (hllRegisters - 1))
		rest := h>>hllP | 1<<hllQ
		var count byte = 1
		for rest&1 == 0 {
			count++
			rest >>= 1
		}
		if count > getRegister(regs, idx) {
			setRegister(regs, idx, count)
		}
	}
	return regs
}

// estimate runs the Ertl closed form over the packed register array so the two
// merge shapes can be checked for the identical answer.
func estimate(regs []byte) uint64 {
	var histo [64]int
	for i := 0; i < hllRegisters; i++ {
		histo[getRegister(regs, i)]++
	}
	return estimateHisto(histo)
}

// estimateHisto runs the Ertl closed form over a value histogram.
func estimateHisto(histo [64]int) uint64 {
	m := float64(hllRegisters)
	z := m * tau((m-float64(histo[hllQ+1]))/m)
	for j := hllQ; j >= 1; j-- {
		z += float64(histo[j])
		z *= 0.5
	}
	z += m * sigma(float64(histo[0])/m)
	return uint64(roundf(hllAlphaInf * m * m / z))
}

func sigma(x float64) float64 {
	if x == 1.0 {
		return 1e308
	}
	y, z := 1.0, x
	for {
		x *= x
		zp := z
		z += x * y
		y += y
		if zp == z {
			return z
		}
	}
}

func tau(x float64) float64 {
	if x == 0.0 || x == 1.0 {
		return 0.0
	}
	y, z := 1.0, 1-x
	for {
		x = sqrtf(x)
		zp := z
		y *= 0.5
		z -= (1 - x) * (1 - x) * y
		if zp == z {
			return z / 3
		}
	}
}

func sqrtf(x float64) float64 {
	if x <= 0 {
		return 0
	}
	g := x
	for i := 0; i < 60; i++ {
		g = 0.5 * (g + x/g)
	}
	return g
}

func roundf(x float64) float64 {
	if x < 0 {
		return float64(int64(x - 0.5))
	}
	return float64(int64(x + 0.5))
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// timeMerge runs f enough times to get a stable per-merge number.
func timeMerge(f func() []byte) time.Duration {
	const iters = 2000
	start := time.Now()
	for i := 0; i < iters; i++ {
		f()
	}
	return time.Since(start) / iters
}

// timeCount is timeMerge for the read path, whose result is a scalar count.
func timeCount(f func() uint64) time.Duration {
	const iters = 2000
	start := time.Now()
	for i := 0; i < iters; i++ {
		f()
	}
	return time.Since(start) / iters
}

func main() {
	fmt.Println("two-source fold, per cardinality (naive packed vs SWAR unpack-fold-repack):")
	fmt.Printf("%-10s %12s %12s %9s %12s %12s\n", "card", "naive", "swar", "speedup", "estNaive", "estSwar")
	for _, card := range []int{100, 1000, 10000, 100000, 1000000} {
		a := fill(1, card)
		b := fill(2, card)
		srcs := [][]byte{a, b}

		gotNaive := naiveMergeAll(srcs)
		gotSwar := swarMergeAll(srcs)
		if !bytesEqual(gotNaive, gotSwar) {
			fmt.Printf("MISMATCH at card=%d: merged sketches differ\n", card)
			return
		}
		en := estimate(gotNaive)
		es := estimate(gotSwar)

		tn := timeMerge(func() []byte { return naiveMergeAll(srcs) })
		ts := timeMerge(func() []byte { return swarMergeAll(srcs) })
		fmt.Printf("%-10d %12v %12v %8.2fx %12d %12d\n", card, tn, ts, float64(tn)/float64(ts), en, es)
	}

	fmt.Println("\nN-source fold at card=100000 each (PFMERGE fan-in):")
	fmt.Printf("%-10s %12s %12s %9s\n", "sources", "naive", "swar", "speedup")
	for _, n := range []int{2, 4, 8, 16} {
		srcs := make([][]byte, n)
		for i := range srcs {
			srcs[i] = fill(i+1, 100000)
		}
		if !bytesEqual(naiveMergeAll(srcs), swarMergeAll(srcs)) {
			fmt.Printf("MISMATCH at sources=%d\n", n)
			return
		}
		tn := timeMerge(func() []byte { return naiveMergeAll(srcs) })
		ts := timeMerge(func() []byte { return swarMergeAll(srcs) })
		fmt.Printf("%-10d %12v %12v %8.2fx\n", n, tn, ts, float64(tn)/float64(ts))
	}

	fmt.Println("\nmulti-key PFCOUNT read union, no repack, card=100000 each:")
	fmt.Printf("%-10s %12s %12s %9s %10s\n", "keys", "naive", "swar", "speedup", "estAgree")
	for _, n := range []int{2, 4, 8, 16} {
		srcs := make([][]byte, n)
		for i := range srcs {
			srcs[i] = fill(i+1, 100000)
		}
		agree := naiveCountUnion(srcs) == swarCountUnion(srcs)
		tn := timeCount(func() uint64 { return naiveCountUnion(srcs) })
		ts := timeCount(func() uint64 { return swarCountUnion(srcs) })
		fmt.Printf("%-10d %12v %12v %8.2fx %10v\n", n, tn, ts, float64(tn)/float64(ts), agree)
	}
}
