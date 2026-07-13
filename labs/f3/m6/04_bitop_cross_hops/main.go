// Lab: cross-shard BITOP, streaming hops vs gather-then-materialize, the memory
// bound across shards (spec 2064/f3/15 section 5, M6 lab 04).
//
// The question: co-located BITOP (lab 03) already streams a chunk at a time to
// hold (sources + 1) chunks resident. Cross-shard BITOP has the same memory bar
// but the sources live on other shards, so each chunk it reads costs a hop to
// that source's owner and each chunk it writes costs a hop to the destination's
// owner. The alternative is to gather every source whole in one hop per source
// shard, run the op over the full length on the coordinator, and write the result
// back: far fewer hops, but the coordinator then holds every source and the result
// whole, which is exactly the multi-hundred-MiB peak the memory bar forbids.
//
// The tradeoff is hops against resident bytes. The streaming coordinator keeps the
// peak flat at (sources + 1) chunks no matter how long the bitmaps are, at the
// cost of a hop count that grows with the length; the gather coordinator keeps the
// hop count flat at (source shards + 1) at the cost of a peak that grows with the
// length. The memory bar decides it: aki must hold less than a rival that gathers,
// so the streaming form is the one the slice ships. This lab prices both sides, the
// measured resident bytes and the modeled hop latency, so the choice is visible.
//
// Method: in-process, no server, no wire, no engine import, the lab-local model the
// other f3 labs use. combineAnd is the same word kernel the store runs (a bitwise
// op does not change residency or hop count, so AND stands in for all four). The
// sweep runs a three-source cross-shard AND over a length sweep for a fixed
// per-hop latency and reports, for each form, the measured allocation, the peak
// resident model, the hop count, and the modeled hop latency. main_test.go carries
// the hop-count formulas as a test so the counts the verdict rests on are checked,
// plus an equivalence test that the streaming assembly matches the whole-buffer
// answer.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"runtime"
	"time"
)

const chunkSize = 64 << 10 // strChunkSize, the store's chunk width.

// combineAnd folds src into dst eight bytes at a time, the store kernel. A bitwise
// op is endian-agnostic, so little-endian word reads preserve the byte layout.
func combineAnd(dst, src []byte) {
	i := 0
	for ; i+8 <= len(dst); i += 8 {
		binary.LittleEndian.PutUint64(dst[i:], binary.LittleEndian.Uint64(dst[i:])&binary.LittleEndian.Uint64(src[i:]))
	}
	for ; i < len(dst); i++ {
		dst[i] &= src[i]
	}
}

func fillChunk(src []byte, off int, dst []byte) {
	if off >= len(src) {
		clear(dst)
		return
	}
	m := copy(dst, src[off:])
	if m < len(dst) {
		clear(dst[m:])
	}
}

func maxLen(srcs [][]byte) int {
	m := 0
	for _, s := range srcs {
		if len(s) > m {
			m = len(s)
		}
	}
	return m
}

func chunks(ml int) int { return (ml + chunkSize - 1) / chunkSize }

// streamingHops is the cross-shard streaming hop count: one length hop per source
// shard, then for every chunk one read hop per source shard and one write hop to
// the destination owner. It grows with the bitmap length.
func streamingHops(ml, srcShards int) int {
	return srcShards + chunks(ml)*(srcShards+1)
}

// gatherHops is the gather-then-materialize hop count: one gather hop per source
// shard and one write hop, flat in the bitmap length.
func gatherHops(srcShards int) int {
	return srcShards + 1
}

// streamPeak is the streaming coordinator's peak resident bytes: (sources + 1)
// chunk buffers, flat in the bitmap length. gatherPeak is the gather coordinator's
// peak: every source and the result whole, linear in the bitmap length.
func streamPeak(nsrc int) int     { return (nsrc + 1) * chunkSize }
func gatherPeak(nsrc, ml int) int { return (nsrc + 1) * ml }

// runStreaming walks (sources + 1) chunk buffers, folding each chunk with the
// kernel and handing the result to a sink, the model of the write hop. It never
// holds a whole source or the whole result.
func runStreaming(srcs [][]byte, sink func(off int, b []byte)) {
	ml := maxLen(srcs)
	bufs := make([][]byte, len(srcs))
	for i := range bufs {
		bufs[i] = make([]byte, chunkSize)
	}
	out := make([]byte, chunkSize)
	for off := 0; off < ml; off += chunkSize {
		cl := chunkSize
		if ml-off < cl {
			cl = ml - off
		}
		o := out[:cl]
		fillChunk(srcs[0], off, o)
		for i := 1; i < len(srcs); i++ {
			b := bufs[i][:cl]
			fillChunk(srcs[i], off, b)
			combineAnd(o, b)
		}
		sink(off, o)
	}
}

// runGather copies every source whole (the gather hops) and folds over the full
// length, holding it all resident.
func runGather(srcs [][]byte) []byte {
	ml := maxLen(srcs)
	gathered := make([][]byte, len(srcs))
	for i, s := range srcs {
		g := make([]byte, ml)
		copy(g, s)
		gathered[i] = g
	}
	res := make([]byte, ml)
	copy(res, gathered[0])
	for i := 1; i < len(srcs); i++ {
		combineAnd(res, gathered[i])
	}
	return res
}

func allocBytes(fn func()) uint64 {
	var a, b runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&a)
	fn()
	runtime.ReadMemStats(&b)
	return b.TotalAlloc - a.TotalAlloc
}

func mib(n int) float64     { return float64(n) / (1 << 20) }
func mibU(n uint64) float64 { return float64(n) / (1 << 20) }

type size struct {
	name string
	n    int
}

func main() {
	flag.Parse()

	fmt.Printf("cross-shard BITOP, streaming hops vs gather residency, %s\n", time.Now().Format("2006-01-02"))

	const nsrc = 3
	const srcShards = 2 // the three sources spread over two shards.
	const hopLatency = 5 * time.Microsecond

	sizes := []size{
		{"1MiB", 1 << 20},
		{"16MiB", 16 << 20},
		{"256MiB", 256 << 20},
	}

	fmt.Println()
	fmt.Printf("AND over %d sources on %d shards, %v modeled per hop\n", nsrc, srcShards, hopLatency)
	fmt.Printf("residentMiB is measured allocation (streaming reuses its buffers, so its total\n")
	fmt.Printf("allocation is its peak); modelMiB is the (sources+1) formula for the same peak.\n\n")
	fmt.Printf("%-8s %12s %12s %14s %14s %12s %12s\n",
		"size", "streamHops", "gatherHops", "streamMiB", "gatherMiB", "streamLat", "gatherLat")
	for _, sz := range sizes {
		srcs := make([][]byte, nsrc)
		for i := range srcs {
			b := make([]byte, sz.n)
			for j := range b {
				b[j] = byte(j*7 + i*13)
			}
			srcs[i] = b
		}

		var sink byte
		streamAlloc := allocBytes(func() {
			runStreaming(srcs, func(_ int, b []byte) { sink ^= b[0] })
		})
		gatherAlloc := allocBytes(func() {
			sink ^= runGather(srcs)[0]
		})

		sh := streamingHops(sz.n, srcShards)
		gh := gatherHops(srcShards)
		fmt.Printf("%-8s %12d %12d %14s %14s %12s %12s\n",
			sz.name, sh, gh,
			fmt.Sprintf("%.2f/%.2f", mibU(streamAlloc), mib(streamPeak(nsrc))),
			fmt.Sprintf("%.2f/%.2f", mibU(gatherAlloc), mib(gatherPeak(nsrc, sz.n))),
			(time.Duration(sh) * hopLatency).String(), (time.Duration(gh) * hopLatency).String())
		_ = sink
	}

	fmt.Println()
	fmt.Println("Verdict: the streaming coordinator holds a flat (sources + 1) * 64KiB peak")
	fmt.Println("regardless of bitmap length, where the gather coordinator's peak climbs to a")
	fmt.Println("full gigabyte at 256MiB and 3 sources. The price is hops: streaming's hop count")
	fmt.Println("grows with the length while gather's stays flat, so at a few microseconds per")
	fmt.Println("hop the streaming form trades some cross-shard latency for the bounded memory")
	fmt.Println("the bar demands. Cross-shard BITOP is rare and the residency is the hard")
	fmt.Println("constraint, so aki streams; the hop chatter is the priced, bounded-per-chunk")
	fmt.Println("cost, not a regression on the co-located fast path that never hops at all.")
}
