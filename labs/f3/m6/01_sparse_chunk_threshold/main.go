// Lab: sparse-bitmap resident cost, chunk-directory holes versus a full extent
// (spec 2064/f3/15 section 2.3, M6 lab 01).
//
// The question: a bitmap in aki rides the chunked string band (doc 09). A value at
// or past the chunk threshold splits into 64 KiB chunks located by a directory of
// 16-byte run pointers, one per chunk. Section 2.3 lets an all-zero chunk stay a
// hole: the directory entry carries the chunk length with a nil run word and the
// chunk consumes no run bytes. So a SETBIT at a high offset stores only the chunks
// that hold a set bit plus the directory that spans the extent, while Redis and
// Valkey store the whole extent as one contiguous SDS string: SETBIT k 4e9 1
// allocates the full 512 MiB. The claim the slice bakes in is that aki's resident
// cost tracks the live chunks, not the logical extent, so a sparse bitmap uses far
// less memory, and that at full density the directory overhead is negligible so the
// hole scheme never meaningfully costs.
//
// This lab prices that. It models both layouts' resident bytes from a set of set-bit
// offsets: aki as live-chunk runs plus the full directory plus the record, a rival
// as the extent SDS. It sweeps the single-high-bit pathology, the density crossover
// (where enough chunks fill that holes stop helping, the "sparse-chunk threshold" the
// lab is named for), and the full-density directory overhead. The memory bar is that
// aki stays under the rival across the sparse regime and ideally at or below half.
//
// Method: in-process, no server, no wire, no engine import, the lab-local model the
// other f3 labs use. The chunk geometry (64 KiB chunk, 16-byte directory pointer)
// and the run and record overheads match the store's chunked band so the resident
// figures are the store's, not a stand-in. The rival model is a jemalloc-rounded SDS
// string of the extent, the whole-extent allocation Redis makes for a bitmap.
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"sort"
	"time"
)

const (
	chunkSize = 64 * 1024 // strChunkSize, the chunked band's chunk width
	ptrSize   = 16        // one directory run pointer per chunk
	runHdr    = 16        // arena run header carried per live chunk run
	recBytes  = 48        // the chunked record: key header plus the directory pointer
	sdsHdr    = 16        // a rival SDS string header (len, alloc, flags)
)

// extentBytes is the byte length a bitmap addressed up to bit maxBit spans: the
// covering byte of the highest set bit plus one, the value length both layouts
// agree on.
func extentBytes(maxBit int64) int64 {
	if maxBit < 0 {
		return 0
	}
	return maxBit/8 + 1
}

// chunkOf is the chunk index a bit offset lands in, the bit's covering byte divided
// by the chunk width.
func chunkOf(bit int64) int64 { return (bit / 8) / chunkSize }

// layout is one bitmap's resident cost and the shape that drove it: the extent it
// spans, how many chunks that extent needs, and how many of them hold a set bit.
type layout struct {
	extent     int64
	nChunks    int64
	liveChunks int64
}

// fromBits builds the layout a set of set-bit offsets produces: the extent is the
// covering byte of the max offset, the live chunks are the distinct chunk indices
// the offsets touch.
func fromBits(bits []int64) layout {
	var maxBit int64 = -1
	live := map[int64]bool{}
	for _, b := range bits {
		if b > maxBit {
			maxBit = b
		}
		live[chunkOf(b)] = true
	}
	ext := extentBytes(maxBit)
	return layout{extent: ext, nChunks: chunkCount(ext), liveChunks: int64(len(live))}
}

// chunkCount is how many chunks an extent of n bytes needs.
func chunkCount(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return (n + chunkSize - 1) / chunkSize
}

// akiBytes is aki's resident cost for the layout: the record, the full directory
// (one pointer per chunk in the extent, holes included), and one run per live chunk
// (the chunk bytes plus its run header). A hole costs only its directory pointer.
func akiBytes(l layout) int64 {
	dir := l.nChunks * ptrSize
	runs := l.liveChunks * (chunkSize + runHdr)
	return recBytes + dir + runs
}

// rivalBytes is the whole-extent SDS a rival allocates: the extent bytes plus the
// string header. This is the conservative rival footprint, the live bytes Redis
// holds without leaning on any allocator size-class rounding, which only ever adds
// to the rival's side.
func rivalBytes(l layout) int64 {
	return l.extent + sdsHdr
}

// setBits lays down count set bits spread over [0,maxBit] with a fixed seed so a run
// is reproducible, the sparse fill a scattered-user bitmap makes.
func setBits(count int, maxBit int64, seed uint64) []int64 {
	r := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	bits := make([]int64, count)
	for i := range bits {
		bits[i] = r.Int64N(maxBit + 1)
	}
	return bits
}

func main() {
	quick := flag.Bool("quick", false, "smaller counts for a fast check")
	flag.Parse()

	fmt.Printf("sparse bitmap resident cost, chunk holes vs full extent, %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("chunk %d KiB, directory pointer %d B, run header %d B\n", chunkSize/1024, ptrSize, runHdr)

	// Sweep A: the single-high-bit pathology. One set bit at a rising offset. aki
	// holds one chunk plus the directory that spans the extent; the rival holds the
	// whole extent. This is the SETBIT k <big> 1 case.
	fmt.Println()
	fmt.Println("Sweep A: one set bit at a high offset (the SETBIT k <big> 1 case)")
	fmt.Printf("%-14s %12s %14s %14s %10s\n", "maxBitOffset", "extent", "akiBytes", "rivalBytes", "aki/rival")
	for _, mb := range []int64{1 << 20, 1 << 24, 1 << 28, 1<<32 - 1} {
		l := fromBits([]int64{mb})
		a, rv := akiBytes(l), rivalBytes(l)
		fmt.Printf("%-14d %12s %14s %14s %10.4f\n", mb, human(l.extent), human(a), human(rv), ratio(a, rv))
	}

	// Sweep B: the density crossover. A fixed 512 MiB extent (bit offset cap), rising
	// set-bit counts. While the live chunks cover under half the extent aki wins; once
	// enough distinct chunks fill, holes stop helping. The crossover is the threshold.
	fmt.Println()
	fmt.Println("Sweep B: density crossover at a 512 MiB extent (8192 chunks)")
	fmt.Printf("%-12s %10s %12s %14s %10s %8s\n", "setBits", "liveChk", "coverage", "akiBytes", "aki/rival", "verdict")
	maxBit := int64(1<<32 - 1)
	counts := []int{1_000, 10_000, 100_000, 1_000_000, 4_000_000, 16_000_000}
	if *quick {
		counts = []int{1_000, 100_000, 4_000_000}
	}
	for _, c := range counts {
		l := fromBits(setBits(c, maxBit, uint64(c)))
		a, rv := akiBytes(l), rivalBytes(l)
		cov := float64(l.liveChunks) / float64(l.nChunks)
		v := "aki<"
		if a >= rv {
			v = "tie/over"
		}
		fmt.Printf("%-12d %10d %11.1f%% %14s %10.4f %8s\n", c, l.liveChunks, cov*100, human(a), ratio(a, rv), v)
	}

	// Sweep C: full-density directory overhead. Every chunk in the extent is live, so
	// aki pays the whole directory on top of the same bytes the rival holds. The
	// overhead is one pointer per 64 KiB chunk, the tax the hole scheme costs when
	// there is nothing to save.
	fmt.Println()
	fmt.Println("Sweep C: full-density directory overhead (every chunk live)")
	fmt.Printf("%-12s %10s %14s %14s %10s\n", "extent", "chunks", "akiBytes", "rivalBytes", "overhead")
	for _, ext := range []int64{1 << 20, 1 << 24, 1 << 28, 512 << 20} {
		nc := chunkCount(ext)
		l := layout{extent: ext, nChunks: nc, liveChunks: nc}
		a, rv := akiBytes(l), rivalBytes(l)
		over := float64(a-rv) / float64(rv)
		fmt.Printf("%-12s %10d %14s %14s %9.3f%%\n", human(ext), nc, human(a), human(rv), over*100)
	}

	// The threshold, stated: the coverage fraction at which aki crosses the 0.5x
	// memory bar and the 1.0x break-even, computed from the per-chunk costs alone.
	fmt.Println()
	half := crossover(0.5)
	one := crossover(1.0)
	fmt.Printf("Threshold: aki holds under half the rival below %.1f%% chunk coverage, ", half*100)
	fmt.Printf("crosses break-even at %.1f%%.\n", one*100)
}

// crossover returns the chunk-coverage fraction at which aki's bytes equal target
// times the rival's, from the dominating per-chunk terms: aki ~ coverage*nChunks*
// (chunkSize+runHdr) + nChunks*ptrSize, rival ~ nChunks*chunkSize. Solving
// aki = target*rival for coverage.
func crossover(target float64) float64 {
	per := float64(chunkSize + runHdr)
	dir := float64(ptrSize)
	f := (target*float64(chunkSize) - dir) / per
	if f < 0 {
		return 0
	}
	if f > 1 {
		return 1
	}
	return f
}

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

// human renders a byte count in binary units for the tables.
func human(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2fGiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2fMiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2fKiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// sortedDistinct is a small helper the tests use to count distinct chunks the way
// fromBits does, kept here so the model and its check share one definition.
func distinctChunks(bits []int64) int {
	seen := map[int64]bool{}
	for _, b := range bits {
		seen[chunkOf(b)] = true
	}
	ks := make([]int64, 0, len(seen))
	for k := range seen {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
	return len(ks)
}
