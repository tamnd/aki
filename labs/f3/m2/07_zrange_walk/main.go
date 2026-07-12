// Lab: ZRANGE window walk, closure-chain and cache-scatter decomposition
// (spec 2064/f3 doc 12 section 6.4, M2 lab 07, issue #544).
//
// The question: unlike LRANGE (M3 lab 07), ZRANGE is NOT algorithmically wrong.
// The zset native band streams a rank window with one counted seek then a leaf
// chain walk (zset/skiplist.go walkRange -> struct/tree.go WalkFromRank:
// descendToRank once, then follow the singly-linked leaves), so it is already
// O(w), not O(w log n). Yet the M2/M3 gate measured ZRANGE / ZRANGEBYSCORE at
// 0.50 to 0.72x of Valkey on a 100-element window over a one-million-element
// zset, aki slower than the rival. So the loss is a CONSTANT FACTOR, and this
// lab prices where that constant lives so the fix is aimed, not guessed.
//
// The shipped per-element cost has three parts the lab separates:
//
//  1. Closure hops. walkRange hands the tree a callback; that callback reads the
//     record and calls a SECOND callback (rangeByRankWindow's emit) to append
//     the bytes. So every element pays two indirect calls the compiler cannot
//     inline across the tree boundary. A fused walk that appends RESP directly
//     in one loop deletes both.
//
//  2. Record scatter. The leaf stores a record ref; the walk reads recs[ref] to
//     get (slab offset, member length, score bits). refs are assigned in
//     INSERTION order (newRecord appends), but the walk visits them in SCORE
//     (rank) order, so on a zset built in arbitrary score order the rank-order
//     ref stream is a random permutation: recs[ref] is a scattered 16-byte load,
//     a likely cache miss per element on a large zset.
//
//  3. Member scatter + RESP encode. slab[loc:loc+mlen] is likewise a scattered
//     read (loc is insertion-ordered), then the RESP bulk header + memcpy. This
//     is the irreducible floor a range read must pay.
//
// The fix for (1) is cheap and mechanical (a fused tree-level append). The fix
// for (2)/(3) is a LAYOUT change (a rank-ordered co-located member cache) and is
// a milestone of its own. So the whole point of this lab is the ratio between
// them: if the closure hops are most of the gap, a fused walk closes it and is
// worth a PR; if the scatter dominates, a fused walk barely moves ZRANGE and the
// real lever is layout. Two axes answer it:
//
//   - closure vs fused, both scattered: the closure-removal win in isolation.
//   - scattered vs sequential, both fused: the cache-scatter penalty, the layout
//     ceiling a fused walk cannot touch.
//
// The model imports no engine: a recs vector byte-identical in shape to
// zset/skiplist.go's natRecord (loc uint32, mlen uint32, bits uint64), a member
// slab, and an `order` array standing in for the leaf-chain rank order (a fixed
// permutation for the scattered case, identity for the co-located case). Both
// kernels emit byte-identical RESP into a reused scratch, so the measured delta
// is only the addressing the fused walk changes. main_test.go proves the two
// kernels return identical bytes.
//
// Swept over cardinality {10k, 100k, 1M} and window {10, 100, 1000}; the 1M,
// window-100 row is the gate cell. See README.md for the table and verdict.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

// sink defeats dead-code elimination: every kernel folds its output length here.
var sink uint64

// rec mirrors zset/skiplist.go's natRecord: the slab offset, member length, and
// raw IEEE-754 score bits, 16 bytes, indexed by the tree's record ref.
type rec struct {
	loc  uint32
	mlen uint32
	bits uint64
}

// model is the lab's zset native band: the record vector, the member slab, and
// the rank-order visit sequence the leaf chain walk produces.
type model struct {
	recs  []rec
	slab  []byte
	order []uint32 // order[rank] = record ref visited at that rank
}

// build lays out n members of mlen bytes each in insertion order (so recs and
// slab locations are dense and monotonic in ref), then sets the rank-order visit
// sequence: a fixed-seed permutation when scatter is true (a zset built in
// arbitrary score order, the realistic case), identity when false (members that
// happened to arrive already score-sorted, the co-located best case). The build
// is deterministic; no PRNG runs in the timed loop.
func build(n, mlen int, scatter bool) *model {
	m := &model{
		recs: make([]rec, n),
		slab: make([]byte, n*mlen),
	}
	for i := 0; i < n; i++ {
		loc := i * mlen
		for j := 0; j < mlen; j++ {
			m.slab[loc+j] = byte('a' + (i+j)%26)
		}
		m.recs[i] = rec{loc: uint32(loc), mlen: uint32(mlen), bits: uint64(i)}
	}
	m.order = make([]uint32, n)
	for i := range m.order {
		m.order[i] = uint32(i)
	}
	if scatter {
		rng := rand.New(rand.NewSource(1)) // fixed seed: deterministic scatter
		rng.Shuffle(n, func(i, j int) { m.order[i], m.order[j] = m.order[j], m.order[i] })
	}
	return m
}

// appendBulk mirrors resp.AppendBulk: $<len>\r\n<bytes>\r\n, the exact per-member
// wire encoding the shipped streamer writes.
func appendBulk(out, b []byte) []byte {
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(b)), 10)
	out = append(out, '\r', '\n')
	out = append(out, b...)
	return append(out, '\r', '\n')
}

// walkRefs is go:noinline and takes a callback by value so the per-element fn
// call is a genuine indirect call the compiler cannot elide, faithful to (in
// fact pessimistic against) the real tree.WalkFromRank which takes the walk
// callback across the struct->zset package boundary. Without this the local
// closures inline away and understate the shipped closure cost.
//
//go:noinline
func walkRefs(order []uint32, lo, hi int, fn func(ref uint32)) {
	for i := lo; i < hi; i++ {
		fn(order[i])
	}
}

// closureWalk mirrors the shipped path: an outer walk (WalkFromRank) hands each
// ref to a callback that reads the record and calls a second callback (emit) to
// append the member. Two indirect calls per element across the tree boundary,
// plus the scattered recs[ref] and slab[loc] reads.
func (m *model) closureWalk(out []byte, lo, w int) []byte {
	emit := func(b []byte) { out = appendBulk(out, b) }
	walkRefs(m.order, lo, lo+w, func(ref uint32) {
		r := &m.recs[ref]
		emit(m.slab[r.loc : r.loc+r.mlen])
	})
	return out
}

// fusedWalk is the proposed fix: one loop, no per-element closures, appending
// RESP directly. Same scattered recs[ref] and slab[loc] reads as closureWalk, so
// the delta between them is exactly the two closure hops.
func (m *model) fusedWalk(out []byte, lo, w int) []byte {
	hi := lo + w
	for i := lo; i < hi; i++ {
		r := &m.recs[m.order[i]]
		out = appendBulk(out, m.slab[r.loc:r.loc+r.mlen])
	}
	return out
}

func main() {
	quick := flag.Bool("quick", false, "smaller sweep")
	flag.Parse()

	const mlen = 16 // "member:00012345"-sized, the gate's member width
	cards := []int{10_000, 100_000, 1_000_000}
	windows := []int{10, 100, 1000}
	reps := 200_000
	if *quick {
		cards = []int{10_000, 1_000_000}
		windows = []int{100}
		reps = 20_000
	}

	fmt.Printf("M2 lab 07: ZRANGE window walk, closure-chain and scatter decomposition\n")
	fmt.Printf("member %dB, reps %d\n\n", mlen, reps)
	fmt.Printf("%9s %7s   %12s %12s %12s   %10s %10s\n",
		"card", "window", "clos_scat", "fused_scat", "fused_seq",
		"clos_win%", "scat_pen%")

	for _, card := range cards {
		scat := build(card, mlen, true)
		seq := build(card, mlen, false)
		buf := make([]byte, 0, 1024*256)
		for _, w := range windows {
			if w > card {
				continue
			}
			// Sweep the window start across the WHOLE zset, not a fixed
			// mid-list window: a fixed window keeps the same w records hot in
			// cache for every rep, so recs[ref] never misses and the scatter
			// penalty reads as zero. The gate hits windows all over the
			// keyspace, so its scatter is cold. starts strides by w so one pass
			// touches every record once; the timed loop cycles through them.
			starts := make([]int, 0, card/w)
			for s := 0; s+w <= card; s += w {
				starts = append(starts, s)
			}

			cs := timeWalk(reps, w, starts, func(lo int) { buf = scat.closureWalk(buf[:0], lo, w) })
			fs := timeWalk(reps, w, starts, func(lo int) { buf = scat.fusedWalk(buf[:0], lo, w) })
			qs := timeWalk(reps, w, starts, func(lo int) { buf = seq.fusedWalk(buf[:0], lo, w) })

			// clos_win%: how much of the closure-path per-element cost the fused
			// walk removes. scat_pen%: how much of the fused per-element cost is
			// the cache scatter (the layout ceiling), i.e. the sequential floor
			// deleted.
			closWin := 100 * (cs - fs) / cs
			scatPen := 100 * (fs - qs) / fs
			fmt.Printf("%9d %7d   %10.3f/e %10.3f/e %10.3f/e   %9.1f%% %9.1f%%\n",
				card, w, cs, fs, qs, closWin, scatPen)
		}
	}
	fmt.Printf("\nsink=%d\n", sink)
}

// timeWalk times fn over reps iterations, cycling the window start through
// starts so every rep reads a different window and the working set is the whole
// zset (cold scatter), returning ns per emitted element. The cycle is a counter
// increment and one array read, negligible against the walk. fn takes the window
// start and emits one window.
func timeWalk(reps, w int, starts []int, fn func(lo int)) float64 {
	fn(starts[0]) // warm: size the buffer
	si := 0
	start := time.Now()
	for r := 0; r < reps; r++ {
		fn(starts[si])
		si++
		if si == len(starts) {
			si = 0
		}
	}
	perWin := float64(time.Since(start).Nanoseconds()) / float64(reps)
	sink += uint64(perWin)
	return perWin / float64(w)
}
