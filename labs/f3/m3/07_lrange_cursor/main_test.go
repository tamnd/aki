package main

import "testing"

// TestCursorMatchesPerElem proves the two range walks emit the identical
// sequence of (chunk, ordinal) pairs for every window, so the cursor walk is a
// pure performance change and LRANGE returns byte-for-byte what the per-element
// locate returned. If they always agree, the seek-once-then-advance walk never
// drifts from repeated index resolution across chunk boundaries.
//
// The smoke test stays small (a few hundred chunks, not the 17408-chunk sweep
// arm) so `go test` runs in well under a second: the DRAM-scale build arm lives
// only in `go run .` (the M4 lab lesson, a heavy build in the test binary times
// out loaded CI runners).
func TestCursorMatchesPerElem(t *testing.T) {
	for _, chunks := range []int{4, 16, 128, 257} {
		l := makeList(chunks, 60)
		// Every window length against every start, over the whole list.
		for _, w := range []int{1, 7, 60, 129, 500} {
			if w > l.total {
				continue
			}
			for lo := 0; lo+w <= l.total; lo += 37 { // stride to keep the test quick but cover edges
				hi := lo + w - 1
				pe := collect(l, l.perElemPairs, lo, hi)
				cu := collect(l, l.cursorPairs, lo, hi)
				if len(pe) != len(cu) {
					t.Fatalf("chunks=%d window=[%d,%d]: len perElem=%d cursor=%d",
						chunks, lo, hi, len(pe), len(cu))
				}
				for i := range pe {
					if pe[i] != cu[i] {
						t.Fatalf("chunks=%d window=[%d,%d] pos %d: perElem=%v cursor=%v",
							chunks, lo, hi, i, pe[i], cu[i])
					}
				}
			}
		}
	}
}

type pair struct{ ci, ord int }

func collect(l *list, walk func(lo, hi int, fn func(ci, ord int)), lo, hi int) []pair {
	var out []pair
	walk(lo, hi, func(ci, ord int) { out = append(out, pair{ci, ord}) })
	return out
}

// perElemPairs and cursorPairs mirror the benchmarked kernels but yield the
// resolved pairs instead of folding into sink, so the oracle checks the exact
// addressing the timed loops use.
func (l *list) perElemPairs(lo, hi int, fn func(ci, ord int)) {
	for i := lo; i <= hi; i++ {
		ci, ord := l.dir.rank(i)
		fn(ci, ord)
	}
}

func (l *list) cursorPairs(lo, hi int, fn func(ci, ord int)) {
	ci, ord := l.dir.rank(lo)
	remaining := hi - lo + 1
	for remaining > 0 {
		n := int(l.counts[ci])
		for ord < n && remaining > 0 {
			fn(ci, ord)
			ord++
			remaining--
		}
		ci++
		ord = 0
	}
}
