// Lab: ZRANGE scatter decomposition, the recs half versus the slab half
// (spec 2064/f3 doc 12 section 6.4, M2 lab 09, issue #544).
//
// Lab 07 pinned the ZRANGE 0.5x at the 1M gate cell: 80 percent is cache scatter
// on the per-element member read, and the fix is a layout change, not a fused
// walk. But "the member read" is TWO scattered loads, not one, and the plan
// (milestones/M2-zrange-colocation-plan.md) branches on their ratio:
//
//   - recs[ref]: a 16-byte natRecord (slab offset, member length, score bits),
//     indexed by the tree's ref. refs are assigned in INSERTION order but the
//     walk visits them in RANK order, so recs[ref] is a scattered 16B load.
//   - slab[loc]: the member bytes, also at an insertion-order offset, so a
//     second scattered load per element.
//
// A rank-ordered slab that leaves recs alone (architecture A, ZSCAN-safe, zero
// added memory) kills only the slab half. Killing the recs half too needs either
// a recs reorder (which breaks ZSCAN's record-order cursor and must decouple it)
// or moving the member location into the leaf so the walk never reads recs at
// all (architecture C, ZSCAN-safe). Which is worth the extra work depends on how
// the 80 percent splits between the two loads, and that split MOVES with member
// size: for small members the 16B recs is the bigger scattered structure, for
// large members the slab dominates. This lab measures the split so the engine PR
// scope is chosen on a number, not a guess.
//
// The walk style is held constant (fused, no closures) across every arm so the
// only variable is layout. Four arms, all emitting byte-identical RESP:
//
//	baseline   recs scattered, slab scattered  (today)
//	slabRank   recs scattered, slab sequential  (architecture A: slab-only reorder)
//	bothRank   recs sequential, slab sequential (architecture B: reorder recs too)
//	leafLoc    recs never read, slab sequential (architecture C: loc carried in the leaf)
//
// baseline-bothRank is the whole scatter penalty; baseline-slabRank is the part
// architecture A captures; the ratio is the answer. leafLoc against bothRank
// prices the 16B recs load itself, i.e. what architecture C saves over B by not
// touching recs at all.
//
// No engine import: a recs vector byte-identical in shape to zset/skiplist.go's
// natRecord, a member slab, and a rank-order permutation standing in for the leaf
// chain. main_test.go proves the four arms emit identical bytes.
//
// Swept over cardinality {10k, 100k, 1M} and member size {8, 32} at the gate
// window (100). See README.md for the table and verdict.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

// sink defeats dead-code elimination.
var sink uint64

// rec mirrors zset/skiplist.go's natRecord: 16 bytes, slab offset, member
// length, raw score bits, indexed by the tree's record ref.
type rec struct {
	loc  uint32
	mlen uint32
	bits uint64
}

// leafEnt models architecture C's per-entry inline location: the member's slab
// offset and length carried in the leaf itself (the reserved leaf word plus a
// length), laid out in rank order so the walk reads it sequentially and never
// touches the scattered recs vector. 8 bytes, walked in rank order.
type leafEnt struct {
	loc  uint32
	mlen uint32
}

// model holds the four layouts over one set of members. The member bytes are
// fixed per insertion ordinal, so every arm emits the same bytes for a given
// rank and the arms are directly comparable.
type model struct {
	perm     []uint32 // perm[rank] = insertion ordinal visited at that rank
	recsIns  []rec    // insertion order, loc into slabIns (baseline)
	slabIns  []byte   // members in insertion order
	recsA    []rec    // insertion order, loc into slabRank (architecture A)
	recsRank []rec    // rank order, loc into slabRank (architecture B)
	slabRank []byte   // members in rank order
	leaf     []leafEnt
	mlen     int
}

// build lays out n members of mlen bytes. Member bytes are a deterministic
// function of the insertion ordinal, so the rank-order copies carry the same
// bytes and the arms stay byte-identical. The rank order is a fixed-seed
// permutation of insertion order (a zset built in arbitrary score order, the
// realistic case). No PRNG runs in the timed loop.
func build(n, mlen int) *model {
	m := &model{
		perm:     make([]uint32, n),
		recsIns:  make([]rec, n),
		slabIns:  make([]byte, n*mlen),
		recsA:    make([]rec, n),
		recsRank: make([]rec, n),
		slabRank: make([]byte, n*mlen),
		leaf:     make([]leafEnt, n),
		mlen:     mlen,
	}
	for i := range m.perm {
		m.perm[i] = uint32(i)
	}
	rng := rand.New(rand.NewSource(1)) // fixed seed: deterministic scatter
	rng.Shuffle(n, func(i, j int) { m.perm[i], m.perm[j] = m.perm[j], m.perm[i] })

	// Member bytes in insertion order, and each insertion record's location in
	// the insertion-order slab.
	writeMember := func(dst []byte, off, ord int) {
		for j := 0; j < mlen; j++ {
			dst[off+j] = byte('a' + (ord+j)%26)
		}
	}
	for ord := 0; ord < n; ord++ {
		loc := ord * mlen
		writeMember(m.slabIns, loc, ord)
		m.recsIns[ord] = rec{loc: uint32(loc), mlen: uint32(mlen), bits: uint64(ord)}
	}
	// Rank-order layouts: at rank p the member is the one whose insertion
	// ordinal is perm[p]. slabRank co-locates it at p*mlen; recsRank and leaf
	// describe rank p at index p. recsA keeps the insertion-order (scattered)
	// record cells but points each at the rank slab, so architecture A reads a
	// scattered record and a sequential member.
	for p := 0; p < n; p++ {
		ord := int(m.perm[p])
		loc := p * mlen
		writeMember(m.slabRank, loc, ord)
		m.recsRank[p] = rec{loc: uint32(loc), mlen: uint32(mlen), bits: uint64(ord)}
		m.leaf[p] = leafEnt{loc: uint32(loc), mlen: uint32(mlen)}
		m.recsA[ord] = rec{loc: uint32(loc), mlen: uint32(mlen), bits: uint64(ord)}
	}
	return m
}

// appendBulk mirrors resp.AppendBulk, the exact per-member wire encoding.
func appendBulk(out, b []byte) []byte {
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(b)), 10)
	out = append(out, '\r', '\n')
	out = append(out, b...)
	return append(out, '\r', '\n')
}

// baseline: recs scattered, slab scattered (today's layout).
func (m *model) baseline(out []byte, lo, w int) []byte {
	hi := lo + w
	for p := lo; p < hi; p++ {
		r := &m.recsIns[m.perm[p]]
		out = appendBulk(out, m.slabIns[r.loc:r.loc+r.mlen])
	}
	return out
}

// slabRankWalk: recs scattered, slab sequential (architecture A). The record is
// still read at a scattered insertion ordinal, but its loc points into the
// rank-ordered slab so the member bytes come out sequentially.
func (m *model) slabRankWalk(out []byte, lo, w int) []byte {
	hi := lo + w
	for p := lo; p < hi; p++ {
		r := &m.recsA[m.perm[p]]
		out = appendBulk(out, m.slabRank[r.loc:r.loc+r.mlen])
	}
	return out
}

// bothRankWalk: recs sequential, slab sequential (architecture B). recsRank is
// visited at rank p, so both loads stride forward.
func (m *model) bothRankWalk(out []byte, lo, w int) []byte {
	hi := lo + w
	for p := lo; p < hi; p++ {
		r := &m.recsRank[p]
		out = appendBulk(out, m.slabRank[r.loc:r.loc+r.mlen])
	}
	return out
}

// leafLocWalk: recs never read, slab sequential (architecture C). The member
// location comes from the rank-ordered leaf array (8B/entry, sequential), so the
// 16B scattered recs load is gone entirely and ZSCAN's insertion-order recs is
// left untouched.
func (m *model) leafLocWalk(out []byte, lo, w int) []byte {
	hi := lo + w
	for p := lo; p < hi; p++ {
		e := &m.leaf[p]
		out = appendBulk(out, m.slabRank[e.loc:e.loc+e.mlen])
	}
	return out
}

func main() {
	quick := flag.Bool("quick", false, "smaller sweep")
	flag.Parse()

	cards := []int{10_000, 100_000, 1_000_000}
	mlens := []int{8, 32}
	const w = 100 // the gate window
	reps := 200_000
	if *quick {
		cards = []int{10_000, 1_000_000}
		mlens = []int{8}
		reps = 20_000
	}

	fmt.Printf("M2 lab 09: ZRANGE scatter split, recs half versus slab half\n")
	fmt.Printf("window %d, reps %d\n\n", w, reps)
	fmt.Printf("%9s %6s   %10s %10s %10s %10s   %10s %10s\n",
		"card", "member", "base_ns", "slabR_ns", "bothR_ns", "leaf_ns",
		"A_kill%", "recs_pen%")

	for _, mlen := range mlens {
		for _, card := range cards {
			m := build(card, mlen)
			buf := make([]byte, 0, 1024*64)
			// Sweep the window start across the whole zset so the working set is
			// the full arrays and the scatter is cold (lab 07's methodology).
			starts := make([]int, 0, card/w)
			for s := 0; s+w <= card; s += w {
				starts = append(starts, s)
			}

			base := timeWalk(reps, w, starts, func(lo int) { buf = m.baseline(buf[:0], lo, w) })
			slabR := timeWalk(reps, w, starts, func(lo int) { buf = m.slabRankWalk(buf[:0], lo, w) })
			bothR := timeWalk(reps, w, starts, func(lo int) { buf = m.bothRankWalk(buf[:0], lo, w) })
			leaf := timeWalk(reps, w, starts, func(lo int) { buf = m.leafLocWalk(buf[:0], lo, w) })

			// A_kill%: fraction of the total scatter penalty (base-bothR) that
			// the slab-only reorder (architecture A) captures. recs_pen%: how much
			// of the fully-sequential-recs cost the leaf path saves by not reading
			// recs at all (bothR-leaf over bothR), i.e. the 16B recs load's share.
			var aKill, recsPen float64
			if base > bothR {
				aKill = 100 * (base - slabR) / (base - bothR)
			}
			if bothR > 0 {
				recsPen = 100 * (bothR - leaf) / bothR
			}
			fmt.Printf("%9d %6d   %8.3f/e %8.3f/e %8.3f/e %8.3f/e   %9.1f%% %9.1f%%\n",
				card, mlen, base, slabR, bothR, leaf, aKill, recsPen)
		}
	}
	fmt.Printf("\nsink=%d\n", sink)
}

// timeWalk times fn over reps iterations, cycling the window start through
// starts so every rep reads a different window and the working set is the whole
// zset (cold scatter), returning ns per emitted element.
func timeWalk(reps, w int, starts []int, fn func(lo int)) float64 {
	fn(starts[0]) // warm
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
