// Lab: LSET spurious directory rebuild versus a guarded stale flag (spec
// 2064/f3 doc 13 section 5.6, M3 lab 08, issue #545).
//
// The question: the M2/M3 gate measured deep LSET on a one-million-element list
// at 0.028x of the rival (lset_c1m), 35x slower, while LINDEX on the identical
// list ran 4.06x, a fast indexed read. So the locate is not the problem; the
// write path is. LSET resolves the index, and when the new value differs in
// length from the old it repacks the one chunk that holds it (bounded in-chunk
// surgery, section 5.6). That repack replaces one element with another, so the
// chunk's element count is unchanged and the Fenwick chunk directory (which maps
// a dense index to a (chunk, ordinal) pair off the per-chunk counts) is still
// exactly correct. But the shipped setAt marked the directory stale after every
// length-changing repack, so the NEXT locate rebuilt the whole O(chunks)
// directory (chunkDir.sync: zero the tree, refill from every chunk's count,
// re-link the Fenwick blocks). On a 1M list that is ~17K chunks rebuilt on every
// LSET, an O(n/CAP) tax bolted onto an O(CAP) surgery.
//
// The fix marks the directory stale only when the repack actually splits the
// chunk (an overflow that inserts a chunk and so renumbers the ring); a
// no-split length change leaves the flag clear and the next locate is a plain
// O(log chunks) descent over the still-valid tree. This lab prices that tax: the
// per-LSET cost with the spurious rebuild versus with the guarded flag, both
// paying the same O(CAP) in-chunk repack and the same O(log chunks) seek, so the
// measured delta is exactly the deleted rebuild.
//
// The model imports no engine: a chunkDir byte-identical in shape to
// engine/f3/list/native.go (1-indexed Fenwick BIT over per-chunk counts, the
// same sync rebuild and the same power-of-two rank descent) over a ring of
// `chunks` chunks. Each simulated LSET does one rank descent (the locate seek),
// a fixed O(CAP) repack cost (a memcpy over one chunk's worth of frame bytes,
// the surgery both variants share), and then, for the rebuild variant, a
// chunkDir.sync. main_test.go proves the descent after a no-split LSET returns
// the same (chunk, ordinal) the flat scan does, so skipping the rebuild is
// correct.
//
// Swept over chunk count {128, 1024, 4096, 17408} (17408 is the 1M-at-64B gate
// case). See README.md for the table and the frozen verdict.
package main

import (
	"flag"
	"fmt"
	"time"
)

// sink defeats dead-code elimination.
var sink uint64

// chunkDir mirrors engine/f3/list/native.go's Fenwick chunk directory: a
// 1-indexed BIT over per-chunk live counts with a stale flag, sync rebuilding the
// tree O(chunks) when stale, rank a power-of-two descent resolving a dense index
// to (chunk, ordinal).
type chunkDir struct {
	tree  []uint64
	n     int
	pw    int
	stale bool
}

// counts is the ring's per-chunk live element counts, the source of truth the
// directory caches.
type ring struct {
	counts []uint64
}

// sync rebuilds the tree from the ring counts when stale or a length mismatch,
// the exact O(chunks) refill native.go runs.
func (d *chunkDir) sync(r *ring) {
	if !d.stale && d.n == len(r.counts) {
		return
	}
	n := len(r.counts)
	if cap(d.tree) >= n+1 {
		d.tree = d.tree[:n+1]
		for i := range d.tree {
			d.tree[i] = 0
		}
	} else {
		d.tree = make([]uint64, n+1)
	}
	for i := 0; i < n; i++ {
		d.tree[i+1] = r.counts[i]
	}
	for i := 1; i <= n; i++ {
		if j := i + (i & -i); j <= n {
			d.tree[j] += d.tree[i]
		}
	}
	pw := 1
	for pw<<1 <= n {
		pw <<= 1
	}
	d.n = n
	d.pw = pw
	d.stale = false
}

// rank resolves dense index k to (chunk, ordinal), the same descent native.go
// runs. The tree must be current (sync first).
func (d *chunkDir) rank(k int) (ci, ord int) {
	pos, rem := 0, k
	for pw := d.pw; pw > 0; pw >>= 1 {
		next := pos + pw
		if next <= d.n && d.tree[next] <= uint64(rem) {
			pos = next
			rem -= int(d.tree[next])
		}
	}
	return pos, rem
}

// repackCost stands in for the O(CAP) in-chunk surgery both variants pay: a
// memcpy over one chunk's frame bytes. It is deterministic and identical across
// the two kernels, so it cancels out of the measured delta and only keeps the
// per-op shape honest (a real LSET is not a bare tree op).
func repackCost(blob []byte) {
	var acc uint64
	for i := range blob {
		acc += uint64(blob[i])
	}
	sink += acc
}

// lsetRebuild is the shipped path: locate (rebuild if stale), repack the chunk,
// mark stale. The stale mark forces the next locate to rebuild the whole
// directory even though no count changed.
func lsetRebuild(d *chunkDir, r *ring, blob []byte, idx int) {
	d.sync(r)
	ci, _ := d.rank(idx)
	_ = ci
	repackCost(blob)
	d.stale = true // spurious: the repack changed no count
}

// lsetGuarded is the fix: locate (no rebuild, the tree is still valid), repack.
// The directory is left clear because a no-split length change renumbers
// nothing, so the next locate is a plain descent.
func lsetGuarded(d *chunkDir, r *ring, blob []byte, idx int) {
	d.sync(r) // no-op: not stale, lengths match
	ci, _ := d.rank(idx)
	_ = ci
	repackCost(blob)
	// no stale mark: ring.n unchanged, counts unchanged
}

func main() {
	quick := flag.Bool("quick", false, "smaller sweep")
	flag.Parse()

	const fill = 128 // chunkElemCap: a small-element chunk fills by count
	const cap = 4096 // chunkBlobCap: the repack memcpy width
	chunkCounts := []int{128, 1024, 4096, 17408}
	reps := 200_000
	if *quick {
		chunkCounts = []int{128, 4096}
		reps = 20_000
	}

	blob := make([]byte, cap)
	for i := range blob {
		blob[i] = byte(i*3 + 1)
	}

	fmt.Printf("M3 lab 08: LSET spurious directory rebuild vs guarded stale flag\n")
	fmt.Printf("fill %d elems/chunk, repack %dB, reps %d\n\n", fill, cap, reps)
	fmt.Printf("%8s %9s   %12s %12s   %9s\n",
		"chunks", "elems", "rebuild_ns", "guarded_ns", "speedup")

	for _, chunks := range chunkCounts {
		counts := make([]uint64, chunks)
		for i := range counts {
			counts[i] = fill
		}
		total := chunks * fill

		// A deep, mid-list index so the descent is a full seek and the flat
		// rebuild would touch every chunk. Stride the index so it is not a single
		// hot slot, matching a keyspace-spread LSET workload.
		idxs := make([]int, 0, 1024)
		for s := fill / 2; s < total; s += total / 1024 {
			idxs = append(idxs, s)
		}

		// The directory persists across the LSET stream (one build up front), so
		// the rebuild variant pays a fresh sync on every op after the first (each
		// LSET left it stale) while the guarded variant's per-op sync is a no-op.
		rRB := &ring{counts: counts}
		dRB := &chunkDir{stale: true}
		dRB.sync(rRB)
		rb := timeLset(reps, idxs, func(idx int) { lsetRebuild(dRB, rRB, blob, idx) })

		rGD := &ring{counts: counts}
		dGD := &chunkDir{stale: true}
		dGD.sync(rGD)
		gd := timeLset(reps, idxs, func(idx int) { lsetGuarded(dGD, rGD, blob, idx) })
		fmt.Printf("%8d %9d   %10.1f   %10.1f   %8.2fx\n",
			chunks, total, rb, gd, rb/gd)
	}
	fmt.Printf("\nsink=%d\n", sink)
}

// timeLset times fn (one simulated LSET) over reps iterations, cycling the deep
// index so no single slot stays hot, returning ns per LSET.
func timeLset(reps int, idxs []int, fn func(idx int)) float64 {
	fn(idxs[0]) // warm
	si := 0
	start := time.Now()
	for r := 0; r < reps; r++ {
		fn(idxs[si])
		si++
		if si == len(idxs) {
			si = 0
		}
	}
	return float64(time.Since(start).Nanoseconds()) / float64(reps)
}
