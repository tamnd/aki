// Lab: ZRANGE cursor score-skip, the increment on top of the fused walk
// (spec 2064/f3 doc 12 section 6.4, M2 lab 08, issue #544).
//
// Lab 07 priced the fused walk: dropping the two closure hops per element wins
// ~18 to 20% on a cache-resident ZRANGE. This lab prices the SECOND half of the
// same change, which lab 07 did not model: the shipped walk reads the leaf's
// score for every element and throws it away on a plain ZRANGE.
//
// The reason is in the callback signature. struct/tree.go WalkFromRank calls
// fn(t.lScore(ord, off), t.lRef(ord, off)) for each entry, so it always loads
// the 8-byte score from the leaf's interleaved entry array. But zset's walk
// callback is func(_ uint64, ref uint32): it DISCARDS that score and reads the
// authoritative bits from the record instead. So on a ZRANGE without WITHSCORES,
// every element pays a leaf score load that is never used. A box CPU profile of
// the 10k gate cell showed lScore at ~5% of the on-CPU time under zrangeByIndex.
//
// The fused rank cursor (struct/tree.go RankCursor) fixes this for free: the
// caller pulls Ref() only, and reads the score from the record's bits when, and
// only when, WITHSCORES asks. So a plain ZRANGE never touches the leaf score
// array at all.
//
// This lab models one leaf's storage faithfully: entries are laid out as the
// tree lays them, an interleaved [score uint64, ref uint32, reserved uint32] at
// a 16-byte stride (struct/tree.go leafHdr + i*entrySz, entrySz 16), in rank
// order (a leaf holds a contiguous rank run). Two fused kernels, identical
// member output:
//
//   - withScore: reads lScore(i) every element and folds it into a sink, exactly
//     the discarded load the shipped WalkFromRank path pays.
//   - skipScore: never reads the score, the cursor path a plain ZRANGE takes.
//
// The delta is the wasted-score-load cost in isolation, on top of lab 07's
// closure win. Both append byte-identical member RESP; main_test.go proves it.
// Swept over window {10, 100, 1000}; entries stay rank-contiguous so the score
// load streams the leaf, the realistic (and conservative) shape for the cost.
package main

import (
	"flag"
	"fmt"
	"strconv"
	"time"
)

// sink defeats dead-code elimination for both the member output length and the
// discarded score load, so the score read cannot be optimized away.
var sink uint64

// entrySz mirrors struct/tree.go's leaf entry stride: score uint64 then ref
// uint32 then a reserved uint32 (member length and hash back-ordinal), 16 bytes.
const entrySz = 16

// leaf models one tree leaf's entry array plus the member slab a walk reads
// through. entries are in rank order (a leaf holds a contiguous rank run), so the
// score load streams; the member bytes are read via the entry's slab offset.
type leaf struct {
	ent  []byte // interleaved [score u64, ref u32, resv u32] per entry, rank order
	slab []byte // member bytes, indexed by the per-entry offset
	mlen int
}

// build lays out n entries of mlen-byte members. The score is the rank (a stand
// in for any monotone score; the load cost is the same), the slab offset is
// rank*mlen so the member read is sequential too, isolating the score load as
// the only difference between the kernels.
func build(n, mlen int) *leaf {
	l := &leaf{
		ent:  make([]byte, n*entrySz),
		slab: make([]byte, n*mlen),
		mlen: mlen,
	}
	for i := 0; i < n; i++ {
		putU64(l.ent[i*entrySz:], uint64(i))   // score bits
		putU32(l.ent[i*entrySz+8:], uint32(i)) // ref (unused here)
		loc := i * mlen
		for j := 0; j < mlen; j++ {
			l.slab[loc+j] = byte('a' + (i+j)%26)
		}
	}
	return l
}

func putU64(b []byte, v uint64) {
	b[0], b[1], b[2], b[3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
	b[4], b[5], b[6], b[7] = byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56)
}
func putU32(b []byte, v uint32) {
	b[0], b[1], b[2], b[3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
}
func getU64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

// lScore mirrors struct/tree.go lScore: the 8-byte score at the entry's stride.
func (l *leaf) lScore(i int) uint64 { return getU64(l.ent[i*entrySz:]) }

func (l *leaf) member(i int) []byte {
	loc := i * l.mlen
	return l.slab[loc : loc+l.mlen]
}

// appendBulk mirrors resp.AppendBulk: $<len>\r\n<bytes>\r\n.
func appendBulk(out, b []byte) []byte {
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(b)), 10)
	out = append(out, '\r', '\n')
	out = append(out, b...)
	return append(out, '\r', '\n')
}

// withScore is the shipped WalkFromRank shape: it reads the leaf score every
// element (folded into the sink to stand for the callback's discarded score arg)
// and appends the member. The score read is pure waste on a plain ZRANGE.
func (l *leaf) withScore(out []byte, lo, w int) []byte {
	var acc uint64
	for i := lo; i < lo+w; i++ {
		acc ^= l.lScore(i) // the discarded load the shipped path pays
		out = appendBulk(out, l.member(i))
	}
	sink += acc
	return out
}

// skipScore is the fused cursor's plain-ZRANGE path: never reads the score.
func (l *leaf) skipScore(out []byte, lo, w int) []byte {
	for i := lo; i < lo+w; i++ {
		out = appendBulk(out, l.member(i))
	}
	return out
}

func main() {
	quick := flag.Bool("quick", false, "smaller sweep")
	flag.Parse()

	const mlen = 16
	const card = 1_000_000
	windows := []int{10, 100, 1000}
	reps := 200_000
	if *quick {
		windows = []int{100}
		reps = 20_000
	}

	l := build(card, mlen)
	buf := make([]byte, 0, 1024*256)

	fmt.Printf("M2 lab 08: ZRANGE cursor score-skip (increment on the fused walk)\n")
	fmt.Printf("member %dB, card %d, reps %d\n\n", mlen, card, reps)
	fmt.Printf("%7s   %12s %12s   %10s\n", "window", "withScore", "skipScore", "skip_win%")

	for _, w := range windows {
		starts := make([]int, 0, card/w)
		for s := 0; s+w <= card; s += w {
			starts = append(starts, s)
		}
		ws := timeWalk(reps, w, starts, func(lo int) { buf = l.withScore(buf[:0], lo, w) })
		ss := timeWalk(reps, w, starts, func(lo int) { buf = l.skipScore(buf[:0], lo, w) })
		win := 100 * (ws - ss) / ws
		fmt.Printf("%7d   %10.3f/e %10.3f/e   %9.1f%%\n", w, ws, ss, win)
	}
	fmt.Printf("\nsink=%d\n", sink)
}

// timeWalk times fn over reps iterations, cycling the window start through starts
// so the leaf is read cold across its whole span, returning ns per element.
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
	per := float64(time.Since(start).Nanoseconds()) / float64(reps)
	sink += uint64(per)
	return per / float64(w)
}
