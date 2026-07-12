// Lab: LRANGE per-element locate versus cursor walk (spec 2064/f3 doc 13
// section 2.4, M3 lab 07, issue #545).
//
// The question: f3's native list is a chunked resident byte deque, a ring of
// fixed-capacity chunks each holding a contiguous run of consecutive positions
// (lab 02 froze the flat-versus-Fenwick directory that resolves a dense index to
// a (chunk, ordinal) pair). LRANGE key start stop streams a window of the list.
// The shipped M3 handler resolved every element in the window with its own index
// lookup, calling get(i) once per position, and get resolves i through the
// directory. Above the flat crossover that directory read is an O(log chunks)
// Fenwick descent, so a window of w elements paid w descents: the range read was
// O(w log n) when it should be O(w). The M2/M3 gate measured LRANGE at 0.51x of
// Valkey on a 100-element window over a one-million-element list, aki slower than
// the rival, a clean regression against a range read that should walk the window
// at contiguous speed. The fix seeks to the first element once and then advances
// the (chunk, ordinal) cursor by layout across chunk boundaries.
//
// This lab prices that difference on a lab-local model of the same geometry: a
// list of `chunks` chunks with realistic per-chunk live counts, a Fenwick
// directory byte-identical in shape to engine/f3/list's chunkDir (1-indexed BIT,
// power-of-two rank descent), and a backing frame value per position so the emit
// does real per-element work the two walks share. Two range kernels:
//
//   - perElem: the old shape, locate(i) for every i in the window, each locate a
//     fresh Fenwick descent from the root, then read the frame. This is get(i) in
//     a loop, the LRANGE the gate measured.
//   - cursor: the new shape, one locate(lo) to seek the window start, then walk
//     (chunk, ordinal) forward, emitting the rest of a chunk and stepping to the
//     next, no directory read per element. This is native.rangeInto.
//
// Both emit identical work per position (a checksum over the modeled frame), so
// the measured delta is purely the index resolution the cursor deletes.
//
// Swept over chunk count in {16, 128, 256, 1024, 4096, 17408} (17408 is the
// spec's 17K-chunk, one-million-element-at-64B case) and window length in
// {10, 100, 1000}. The window sits mid-list so the seek is a full descent and
// the walk crosses chunk boundaries, the ZRANGE/LRANGE gate shape.
//
// Read: for each (chunks, window) the perElem and cursor ns per window, the ns
// per emitted element, and the speedup. cursor's per-element cost is flat in the
// chunk count (one seek amortizes over the window, then contiguous walk);
// perElem's grows with log(chunks). The window-100 row at 17408 chunks is the
// gate cell. See README.md for the table and the frozen verdict.
package main

import (
	"flag"
	"fmt"
	"time"
)

// sink defeats dead-code elimination: every kernel folds its emitted bytes here.
var sink uint64

// chunkDir mirrors engine/f3/list's Fenwick chunk directory (native.go
// chunkDir): a 1-indexed BIT over per-chunk live counts, rank(k) a power-of-two
// descent returning the (chunk, in-chunk ordinal) pair for dense index k. It is
// the lab's model of the locate path get(i) rides above the flat crossover.
type chunkDir struct {
	tree []uint64
	n    int
	pw   int
}

func newChunkDir(counts []uint64) *chunkDir {
	n := len(counts)
	d := &chunkDir{tree: make([]uint64, n+1), n: n}
	for i := 0; i < n; i++ {
		d.tree[i+1] = counts[i]
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
	d.pw = pw
	return d
}

// rank resolves dense index k to (chunk, ordinal), the same descent native.go's
// chunkDir.rank runs. k must be in [0, total).
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

// list models the deque as the directory plus a per-chunk backing of frame
// values, so a resolved (chunk, ordinal) reads a real byte the way frameAt does.
type list struct {
	dir    *chunkDir
	counts []uint64
	frames [][]uint64 // frames[ci][ord] is the modeled element value at that slot
	total  int
}

// makeList builds a list of `chunks` chunks with counts biased around fill so no
// chunk is degenerate, mirroring a healthy quicklist, and fills each slot with a
// deterministic value.
func makeList(chunks, fill int) *list {
	counts := make([]uint64, chunks)
	frames := make([][]uint64, chunks)
	total := 0
	for i := 0; i < chunks; i++ {
		c := fill - fill/4 + (i*7)%(fill/2+1) // vary the fill without a PRNG in the build
		if c < 1 {
			c = 1
		}
		counts[i] = uint64(c)
		fr := make([]uint64, c)
		for j := 0; j < c; j++ {
			fr[j] = uint64((i*1315423911 + j*2654435761) & 0xffff)
		}
		frames[i] = fr
		total += c
	}
	return &list{dir: newChunkDir(counts), counts: counts, frames: frames, total: total}
}

// emit is the shared per-element work: fold the modeled frame value into sink,
// the same in both kernels so only the index resolution differs.
func (l *list) emit(ci, ord int) {
	sink += l.frames[ci][ord]
}

// perElem is the old LRANGE: locate every index in [lo, hi] from the root.
func (l *list) perElem(lo, hi int) {
	for i := lo; i <= hi; i++ {
		ci, ord := l.dir.rank(i)
		l.emit(ci, ord)
	}
}

// cursor is the new LRANGE: seek lo once, then advance (chunk, ordinal) by
// layout across chunk boundaries, no directory read per element.
func (l *list) cursor(lo, hi int) {
	ci, ord := l.dir.rank(lo)
	remaining := hi - lo + 1
	for remaining > 0 {
		n := int(l.counts[ci])
		for ord < n && remaining > 0 {
			l.emit(ci, ord)
			ord++
			remaining--
		}
		ci++
		ord = 0
	}
}

func main() {
	quick := flag.Bool("quick", false, "smaller sweep")
	flag.Parse()

	const fill = 60 // a 64B-band chunk holds about sixty positions (lab 01/02)
	chunkCounts := []int{16, 128, 256, 1024, 4096, 17408}
	windows := []int{10, 100, 1000}
	reps := 200_000
	if *quick {
		chunkCounts = []int{16, 256, 4096}
		windows = []int{100}
		reps = 20_000
	}

	fmt.Printf("M3 lab 07: LRANGE per-element locate vs cursor walk\n")
	fmt.Printf("fill %d positions/chunk, reps %d\n\n", fill, reps)
	fmt.Printf("%8s %7s %8s %12s %12s %10s %10s   %9s\n",
		"chunks", "elems", "window", "perElem_ns", "cursor_ns", "perE_ns/e", "curs_ns/e", "speedup")

	for _, chunks := range chunkCounts {
		l := makeList(chunks, fill)
		for _, w := range windows {
			if w > l.total {
				continue
			}
			lo := l.total/2 - w/2 // mid-list window: full seek, crosses chunk edges
			if lo < 0 {
				lo = 0
			}
			hi := lo + w - 1

			pe := timeRange(reps, w, func() { l.perElem(lo, hi) })
			cu := timeRange(reps, w, func() { l.cursor(lo, hi) })
			fmt.Printf("%8d %7d %8d %12.1f %12.1f %10.3f %10.3f   %8.2fx\n",
				chunks, l.total, w, pe.win, cu.win, pe.perE, cu.perE, pe.win/cu.win)
		}
	}
	fmt.Printf("\nsink=%d\n", sink)
}

type timing struct{ win, perE float64 }

// timeRange times fn over reps iterations, returning ns per window call and ns
// per emitted element.
func timeRange(reps, window int, fn func()) timing {
	start := time.Now()
	for r := 0; r < reps; r++ {
		fn()
	}
	el := float64(time.Since(start).Nanoseconds()) / float64(reps)
	return timing{win: el, perE: el / float64(window)}
}
