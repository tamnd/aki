// Lab 06: ZRANK count-prefix accumulation (spec 2064/f3 doc 12, M2 lab 06,
// issue #544).
//
// The question: a counted B+ tree ZRANK descends root-to-leaf, and at every
// interior level it sums the subtree counts of the children strictly left of the
// routed child, acc += sum(bCount(node, 0..c)). The per-element reader bCount
// recomputes the block slice and the count-array base offset and runs a
// count-width switch on every child; a level touches up to arity-1 = 15 of them,
// a 1M-entry tree is about 5 levels deep, so a single ZRANK pays that per-element
// arithmetic on the order of tens of times. On a uniform-random ZRANK the descent
// blocks are cold and the op is memory-latency-bound, so that arithmetic hides
// under the cache misses. On a zipf ZRANK the hot members reuse the same few
// descent blocks, they stay L1-resident, and the op turns compute-bound, so the
// per-element overhead becomes the whole delta. That is the zrank_zipf_c1m gate
// cell, measured at 1.08x, a compute-bound cell where a constant factor moves the
// number.
//
// This lab prices the accumulation kernel in isolation, the exact code the slice
// changes: it models one interior branch block byte-identically to
// engine/f3/struct (a 256-byte block, arity 16, u32 counts at the frozen
// countOff), and sums the first c child counts three ways.
//
//   - perElem: the old form, recomputing the block base and running the
//     count-width switch once per child, the bCount-in-a-loop shape.
//   - hoisted: the new form, block base and count-width switch lifted above the
//     loop, a bare strided read per child (Tree.bCountPrefix).
//   - swar: countW==4 only, two u32 counts per u64 load summed in split lanes
//     (counts are well under 2^24 so 15 of them never carry across the 32-bit
//     lane boundary), then the lanes folded, half the loop trips.
//
// Two residency arms bracket the gate reality:
//
//   - hot: one branch block reused every op, the block stays in L1, the
//     compute-bound zipf-hot descent. This is where the kernel win must show.
//   - cold: a large ring of branch blocks, a random block per op, every read a
//     cache miss, the memory-latency-bound uniform-random descent. The kernel
//     win is expected to wash out here, which is the honest bound on what this
//     slice can move on the uniform cells.
//
// Swept over prefix length c in {1,2,4,8,15}: c is the routed child index, so a
// ZRANK near the low end sums few counts and one near the high end sums many, and
// the win scales with c.
//
// Read: ns per prefix-sum per arm and c, the hoisted-over-perElem speedup, and
// the hot-versus-cold gap that says how much of it survives into a real descent.
// The end-to-end gate delta is a box A/B of the zrank_zipf_c1m aki-bench cell on
// the old versus new f3srv binary; this lab isolates the mechanism and bounds it.
// See README.md for the tables and the frozen verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"time"
)

// Frozen geometry, mirrored from engine/f3/struct tree.go so the modeled block is
// byte-identical to a production interior node: a 256-byte branch at arity 16 with
// u32 subtree counts, the counts packed at countOff.
const (
	branchSize = 256
	branchHdr  = 8
	ordSz      = 4
	countW     = 4
	arity      = 16 // ArityFor(256, 4)
	sepMax     = arity - 1
	childOff   = branchHdr + sepMax*8   // 8 + 120 = 128
	countOff   = childOff + arity*ordSz // 128 + 64 = 192, the last cache line
)

// sink defeats dead-code elimination: every kernel folds into it.
var sink uint64

// makeBlock builds one interior branch block with plausible subtree counts. The
// counts are the sizes of arity children of a subtree of about total entries,
// spread so no lane is degenerate; only the count array (bytes countOff..) is
// filled, the rest of the block is the routing state this lab does not touch.
func makeBlock(total uint64) []byte {
	b := make([]byte, branchSize)
	// Split total across arity children, biased a little so counts vary but every
	// child is non-empty, the shape a healthy tree keeps.
	base := total / arity
	for i := 0; i < arity; i++ {
		c := base + uint64(i%5)*(base/16+1)
		binary.LittleEndian.PutUint32(b[countOff+i*countW:], uint32(c))
	}
	return b
}

// perElem is the old accumulation: bCount(node, i) in a loop, so the block base
// and the count-width switch are recomputed on every child. Kept structurally
// identical to tree.go's bCount so the delta is only the hoist.
func perElem(block []byte, c int) uint64 {
	var acc uint64
	for i := 0; i < c; i++ {
		acc += bCount(block, i)
	}
	return acc
}

// bCount mirrors Tree.bCount exactly: recompute the count-array slice and switch
// the width on every call.
func bCount(block []byte, i int) uint64 {
	p := block[countOff+i*countW:]
	switch countW {
	case 2:
		return uint64(binary.LittleEndian.Uint16(p))
	case 8:
		return binary.LittleEndian.Uint64(p)
	default:
		return uint64(binary.LittleEndian.Uint32(p))
	}
}

// hoisted mirrors Tree.bCountPrefix: base out of the loop, width switch above it,
// a bare strided read per child.
func hoisted(block []byte, c int) uint64 {
	p := block[countOff:]
	var acc uint64
	switch countW {
	case 2:
		for i := 0; i < c; i++ {
			acc += uint64(binary.LittleEndian.Uint16(p[i*2:]))
		}
	case 8:
		for i := 0; i < c; i++ {
			acc += binary.LittleEndian.Uint64(p[i*8:])
		}
	default:
		for i := 0; i < c; i++ {
			acc += uint64(binary.LittleEndian.Uint32(p[i*4:]))
		}
	}
	return acc
}

// swar sums u32 counts two per u64 load, countW==4 only. Two u32 packed in a u64
// as [hi|lo] add lane-wise as long as the low lane never carries into the high;
// counts here are well under 2^24 and c<=15, so the low-lane running sum stays
// under 2^28, safe. The tail handles an odd c, then the lanes fold.
func swar(block []byte, c int) uint64 {
	p := block[countOff:]
	var packed uint64
	i := 0
	for ; i+2 <= c; i += 2 {
		packed += binary.LittleEndian.Uint64(p[i*4:])
	}
	acc := (packed & 0xFFFFFFFF) + (packed >> 32)
	if i < c {
		acc += uint64(binary.LittleEndian.Uint32(p[i*4:]))
	}
	return acc
}

type kernel struct {
	name string
	fn   func([]byte, int) uint64
}

// bench times one kernel over reps ops against a block ring. ringMask==0 is the
// hot arm (one block, L1-resident); a non-zero mask is the cold arm (a random
// block per op through a large ring). idx walks the ring by a large odd stride so
// consecutive ops miss to unrelated lines without a PRNG in the timed loop.
func bench(k kernel, ring [][]byte, ringMask uint32, c int, reps int) float64 {
	var idx uint32
	start := time.Now()
	for r := 0; r < reps; r++ {
		block := ring[idx&ringMask]
		sink += k.fn(block, c)
		idx += 2654435761 // Knuth multiplicative stride, hits the ring pseudo-randomly
	}
	el := time.Since(start)
	return float64(el.Nanoseconds()) / float64(reps)
}

func main() {
	quick := flag.Bool("quick", false, "smaller sweep")
	flag.Parse()

	kernels := []kernel{
		{"perElem", perElem},
		{"hoisted", hoisted},
		{"swar", swar},
	}
	cs := []int{1, 2, 4, 8, 15}
	// A 1M-entry tree spread across arity children per interior node; the exact
	// total only shapes the count magnitudes (kept under 2^24 for the SWAR lane
	// bound), the descent arithmetic is total-independent.
	const total = 1 << 20
	reps := 40_000_000
	// coldRing must dwarf the LLC so a random block is a miss; each block is 256B,
	// 1<<16 blocks is 16MiB, past a typical L2 and into a cold-ish L3/DRAM mix.
	coldPow := 16
	if *quick {
		reps = 2_000_000
		coldPow = 12
	}
	coldN := 1 << coldPow
	coldMask := uint32(coldN - 1)

	hotRing := [][]byte{makeBlock(total)}
	coldRing := make([][]byte, coldN)
	for i := range coldRing {
		coldRing[i] = makeBlock(total)
	}

	fmt.Printf("M2 lab 06: ZRANK count-prefix accumulation\n")
	fmt.Printf("branch %dB arity %d countW %d countOff %d, total %d, reps %d, coldRing %dB\n\n",
		branchSize, arity, countW, countOff, total, reps, coldN*branchSize)

	for _, arm := range []struct {
		name string
		ring [][]byte
		mask uint32
	}{
		{"hot (L1, zipf-hot descent)", hotRing, 0},
		{"cold (random block, uniform descent)", coldRing, coldMask},
	} {
		fmt.Printf("== %s ==\n", arm.name)
		fmt.Printf("%3s  %10s %10s %10s   %10s %10s\n",
			"c", "perElem", "hoisted", "swar", "hoist_spd", "swar_spd")
		for _, c := range cs {
			var ns [3]float64
			for i, k := range kernels {
				ns[i] = bench(k, arm.ring, arm.mask, c, reps)
			}
			fmt.Printf("%3d  %10.3f %10.3f %10.3f   %9.2fx %9.2fx\n",
				c, ns[0], ns[1], ns[2], ns[0]/ns[1], ns[0]/ns[2])
		}
		fmt.Println()
	}

	// Full-descent arm: a 1M tree is about 5 interior levels; a ZRANK sums a
	// prefix at each. Model one op as five prefix sums at representative routed
	// indices, the per-ZRANK count-accumulation cost the gate cell actually pays.
	descentCs := []int{7, 9, 11, 8, 6} // ~5 levels, mid-to-high routed children
	fmt.Printf("== full descent (5 levels, per-ZRANK accumulation) ==\n")
	fmt.Printf("%-28s %10s %10s %10s\n", "arm", "perElem", "hoisted", "swar")
	for _, arm := range []struct {
		name string
		ring [][]byte
		mask uint32
	}{
		{"hot (L1, zipf-hot)", hotRing, 0},
		{"cold (random, uniform)", coldRing, coldMask},
	} {
		var ns [3]float64
		for i, k := range kernels {
			start := time.Now()
			var idx uint32
			opreps := reps / len(descentCs)
			for r := 0; r < opreps; r++ {
				for _, c := range descentCs {
					sink += k.fn(arm.ring[idx&arm.mask], c)
					idx += 2654435761
				}
			}
			ns[i] = float64(time.Since(start).Nanoseconds()) / float64(opreps)
		}
		fmt.Printf("%-28s %10.3f %10.3f %10.3f\n", arm.name, ns[0], ns[1], ns[2])
	}
	fmt.Printf("\nsink=%d\n", sink)
}
