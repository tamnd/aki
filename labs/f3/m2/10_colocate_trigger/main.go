// Lab: the slab co-location trigger, when an ordered read should reorder
// (spec 2064/f3 doc 12, M2 lab 10, issue #544).
//
// Lab 09 froze the layout: architecture A (reorder the member slab into rank
// order, leave the records alone) captures 76..85 percent of the ZRANGE scatter
// penalty, ZSCAN-safe and zero added memory. This lab settles the OTHER open
// decision of the co-location plan (milestones/M2-zrange-colocation-plan.md,
// open decision 3): WHEN to run that reorder. The reorder is O(card), so a
// trigger that fires it too eagerly turns a read-write mix into O(card)-per-read
// thrash, and one that fires too late leaves the scatter in place.
//
// The engine predicate under test (zset/skiplist.go maybeColocate) is two gates:
//
//	divergence gate  reorder only once >= card/D members were inserted or
//	                 rescored since the last reorder (D=8), so a nearly-sorted
//	                 slab is left alone.
//	read gate        reorder only once ordered reads have streamed >= card/R
//	                 elements since the last reorder (R=1), so the O(card)
//	                 reorder is amortized over the reads that benefit and a
//	                 write-heavy, rarely-read store never pays for it.
//
// The lab times three real kernels at the 1M gate cell, a scattered walk
// (today), an architecture-A walk (slab rank-ordered), and a full slab reorder,
// then drives a deterministic read/write op stream through the predicate using
// those measured costs. It compares the two-gate predicate against a naive
// divergence-only predicate (no read gate) and against the never-reorder
// baseline and the always-sorted ideal, to show the read gate is what stops the
// thrash. No engine import: the kernels mirror zset/skiplist.go's record and
// slab shapes, and main_test.go proves the walks emit identical bytes.
//
// See README.md for the tables and the frozen predicate.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

var sink uint64

// rec mirrors zset/skiplist.go's natRecord: 16 bytes, slab offset, member
// length, raw score bits, indexed by the tree's record ref.
type rec struct {
	loc  uint32
	mlen uint32
	bits uint64
}

// model holds the scattered layout (insertion order) and the rank-ordered slab
// architecture A produces, over one member set, so the two walks are directly
// comparable and the reorder can be timed copying into the rank slab.
type model struct {
	perm     []uint32 // perm[rank] = insertion ordinal at that rank
	recsScat []rec    // insertion order, loc into slabScat (today's layout)
	slabScat []byte   // members in insertion order
	recsA    []rec    // insertion order, loc into slabRank (architecture A)
	slabRank []byte   // members in rank order
	mlen     int
}

func build(n, mlen int) *model {
	m := &model{
		perm:     make([]uint32, n),
		recsScat: make([]rec, n),
		slabScat: make([]byte, n*mlen),
		recsA:    make([]rec, n),
		slabRank: make([]byte, n*mlen),
		mlen:     mlen,
	}
	for i := range m.perm {
		m.perm[i] = uint32(i)
	}
	rng := rand.New(rand.NewSource(1)) // fixed seed: deterministic scatter
	rng.Shuffle(n, func(i, j int) { m.perm[i], m.perm[j] = m.perm[j], m.perm[i] })

	writeMember := func(dst []byte, off, ord int) {
		for j := 0; j < mlen; j++ {
			dst[off+j] = byte('a' + (ord+j)%26)
		}
	}
	for ord := 0; ord < n; ord++ {
		loc := ord * mlen
		writeMember(m.slabScat, loc, ord)
		m.recsScat[ord] = rec{loc: uint32(loc), mlen: uint32(mlen), bits: uint64(ord)}
	}
	for p := 0; p < n; p++ {
		ord := int(m.perm[p])
		loc := p * mlen
		writeMember(m.slabRank, loc, ord)
		m.recsA[ord] = rec{loc: uint32(loc), mlen: uint32(mlen), bits: uint64(ord)}
	}
	return m
}

func appendBulk(out, b []byte) []byte {
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(b)), 10)
	out = append(out, '\r', '\n')
	out = append(out, b...)
	return append(out, '\r', '\n')
}

// scatWalk is today's layout: record scattered, slab scattered.
func (m *model) scatWalk(out []byte, lo, w int) []byte {
	for p := lo; p < lo+w; p++ {
		r := &m.recsScat[m.perm[p]]
		out = appendBulk(out, m.slabScat[r.loc:r.loc+r.mlen])
	}
	return out
}

// archAWalk is what a co-located store reads: record scattered, slab rank-ordered
// (the engine leaves records in insertion order and reorders only the slab).
func (m *model) archAWalk(out []byte, lo, w int) []byte {
	for p := lo; p < lo+w; p++ {
		r := &m.recsA[m.perm[p]]
		out = appendBulk(out, m.slabRank[r.loc:r.loc+r.mlen])
	}
	return out
}

// reorder copies every member into a fresh slab in rank order, rewriting each
// record's loc, the exact work zset/skiplist.go's colocateSlab does. Timed to
// price the trigger's cost per element.
func (m *model) reorder() {
	fresh := make([]byte, 0, len(m.slabScat))
	for p := 0; p < len(m.perm); p++ {
		ord := m.perm[p]
		r := &m.recsA[ord]
		loc := uint32(len(fresh))
		fresh = append(fresh, m.slabRank[r.loc:r.loc+r.mlen]...)
		r.loc = loc
	}
	sink += uint64(len(fresh))
}

// predicate is a trigger under test: reorder when divergence*divD >= card AND
// readSince*readR >= card. readR = 0 means no read gate (the naive predicate).
type predicate struct {
	name  string
	divD  int
	readR int // 0 disables the read gate
}

// simResult reports what a predicate did over an op stream: how many reorders it
// ran, how many read elements were served scattered versus sequential, and the
// modeled total nanoseconds, given the measured per-element costs.
type simResult struct {
	reorders  int
	scatElems float64
	seqElems  float64
	reorderEl int
	reads     int
}

func (s simResult) ns(scatNs, seqNs, reorderNs float64) float64 {
	return s.scatElems*scatNs + s.seqElems*seqNs + float64(s.reorderEl)*reorderNs
}

func (s simResult) nsPerRead(scatNs, seqNs, reorderNs float64) float64 {
	if s.reads == 0 {
		return 0
	}
	return s.ns(scatNs, seqNs, reorderNs) / float64(s.reads)
}

// simulate drives a deterministic op stream (a fraction writeFrac of writes, the
// rest ordered reads of `window` elements) through a predicate, starting from a
// fully scattered slab (divergence == card, the incremental-ZADD build). It is
// read-centric, one iteration per read with the writeFrac/(1-writeFrac) writes
// that precede it folded in, so it stays O(reads) even at write fractions near 1
// where a per-op loop would need billions of iterations. A read serves each
// element scattered with probability divergence/card and sequential otherwise
// (writes spread uniformly over ranks), the honest expected split for a partially
// co-located slab. p==nil is the never-reorder baseline.
func simulate(card, window int, writeFrac float64, p *predicate, targetReads int) simResult {
	var s simResult
	divergence := card
	readSince := 0
	writesPerRead := writeFrac / (1 - writeFrac) // writeFrac < 1 for every sweep point
	wacc := 0.0
	fCard := float64(card)
	for s.reads < targetReads {
		// The writes that precede this read, folded in as one divergence bump.
		wacc += writesPerRead
		if w := int(wacc); w > 0 {
			wacc -= float64(w)
			if divergence += w; divergence > card {
				divergence = card
			}
		}
		// Apply the trigger first (the engine reorders at the head of the walk so
		// the walk itself reads the co-located slab).
		readSince += window
		if p != nil && divergence*p.divD >= card &&
			(p.readR == 0 || readSince*p.readR >= card) {
			s.reorders++
			s.reorderEl += card
			divergence = 0
			readSince = 0
		}
		frac := float64(divergence) / fCard
		s.scatElems += float64(window) * frac
		s.seqElems += float64(window) * (1 - frac)
		s.reads++
	}
	return s
}

func main() {
	quick := flag.Bool("quick", false, "smaller card and fewer reads")
	flag.Parse()

	card := 1_000_000
	mlen := 8
	reads := 100_000
	if *quick {
		card = 200_000
		reads = 20_000
	}
	const window = 100

	m := build(card, mlen)

	// Time the three kernels at the gate cell. The walk starts sweep the whole
	// zset so the working set is the full arrays and the scatter is cold (lab
	// 07/09 methodology); the reorder is timed as a whole-slab pass.
	starts := make([]int, 0, card/window)
	for s := 0; s+window <= card; s += window {
		starts = append(starts, s)
	}
	buf := make([]byte, 0, 64*1024)
	scatNs := timeWalk(200_000, window, starts, func(lo int) { buf = m.scatWalk(buf[:0], lo, window) })
	seqNs := timeWalk(200_000, window, starts, func(lo int) { buf = m.archAWalk(buf[:0], lo, window) })
	reorderNs := timeReorder(m, card)

	fmt.Printf("M2 lab 10: slab co-location trigger, card %d, member %dB, window %d\n", card, mlen, window)
	fmt.Printf("measured: scattered read %.3f ns/e, arch-A read %.3f ns/e, reorder %.3f ns/e\n\n",
		scatNs, seqNs, reorderNs)

	// Table 1: the thrash test. At the chosen predicate (D=8, R=1) versus the
	// naive divergence-only predicate (no read gate) versus baseline, sweep the
	// write fraction and read the per-read modeled cost. The naive predicate
	// should blow up as writes rise; the chosen one should track baseline from
	// above only where it cannot win and beat it heavily when reads dominate.
	chosen := &predicate{name: "two-gate D8 R1", divD: 8, readR: 1}
	naive := &predicate{name: "naive D8 R0", divD: 8, readR: 0}

	fmt.Printf("Table 1: net ns per read op by write fraction (%d reads)\n", reads)
	fmt.Printf("%12s  %12s %12s %12s %12s   %10s %10s\n",
		"writeFrac", "baseline", "naive", "two-gate", "ideal", "naive_reord", "gate_reord")
	for _, wf := range []float64{0, 0.001, 0.01, 0.1, 0.5, 0.99, 0.9999, 0.999995} {
		base := simulate(card, window, wf, nil, reads)
		nv := simulate(card, window, wf, naive, reads)
		gt := simulate(card, window, wf, chosen, reads)
		fmt.Printf("%12.6f  %11.2f %11.2f %11.2f %11.2f   %10d %10d\n",
			wf,
			base.nsPerRead(scatNs, seqNs, reorderNs),
			nv.nsPerRead(scatNs, seqNs, reorderNs),
			gt.nsPerRead(scatNs, seqNs, reorderNs),
			seqNs*float64(window),
			nv.reorders, gt.reorders)
	}

	// Table 2: read-heavy convergence. At writeFrac 0 (the gate shape, build then
	// read), vary the read-gate divisor R to show how fast the win turns on. A
	// bigger R reorders sooner (fewer scattered reads up front) but the steady
	// state is identical, so R trades warm-up length for one extra early reorder
	// risk under writes; R=1 is the safe amortized choice.
	fmt.Printf("\nTable 2: read-heavy (writeFrac 0), effect of the read-gate divisor R\n")
	fmt.Printf("%6s  %12s %12s %14s\n", "R", "ns/read", "reorders", "reads_to_win")
	for _, r := range []int{1, 2, 4, 8} {
		p := &predicate{name: "R", divD: 8, readR: r}
		s := simulate(card, window, 0, p, reads)
		// reads_to_win: the read index at which the first reorder fires.
		firstReorderReads := card / (r * window)
		fmt.Printf("%6d  %11.2f %12d %14d\n",
			r, s.nsPerRead(scatNs, seqNs, reorderNs), s.reorders, firstReorderReads)
	}

	fmt.Printf("\nsink=%d\n", sink)
}

func timeWalk(reps, w int, starts []int, fn func(lo int)) float64 {
	fn(starts[0])
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

// timeReorder times a whole-slab reorder and returns ns per element moved.
func timeReorder(m *model, card int) float64 {
	const reps = 20
	start := time.Now()
	for r := 0; r < reps; r++ {
		m.reorder()
	}
	return float64(time.Since(start).Nanoseconds()) / float64(reps) / float64(card)
}
