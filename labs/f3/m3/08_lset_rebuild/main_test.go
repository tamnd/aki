package main

import "testing"

// flatRank resolves k the way native.go's flat scan does, reading the ring
// counts directly, an independent oracle for the Fenwick descent.
func flatRank(r *ring, k int) (ci, ord int) {
	for i := range r.counts {
		n := int(r.counts[i])
		if k < n {
			return i, k
		}
		k -= n
	}
	panic("out of range")
}

// TestGuardedDescentMatchesFlat is the correctness bar the guarded stale flag
// rests on: a length-changing LSET that does not split leaves every chunk count
// unchanged, so the still-valid (never rebuilt) Fenwick tree must resolve every
// index exactly as the flat scan does. If skipping the rebuild ever drifted from
// the flat answer, the guard would be wrong; it never does, because a no-split
// repack changes no count.
//
// The test stays small (a few hundred chunks, not the 17408 sweep arm) so
// `go test` runs well under a second: the DRAM-scale sweep lives only in
// `go run .` (the M4 lab lesson).
func TestGuardedDescentMatchesFlat(t *testing.T) {
	for _, chunks := range []int{4, 128, 130, 257} {
		counts := make([]uint64, chunks)
		for i := range counts {
			counts[i] = uint64(60 + (i*7)%40) // varied fills, no degenerate chunk
		}
		r := &ring{counts: counts}
		d := &chunkDir{stale: true}
		d.sync(r)

		total := 0
		for _, c := range counts {
			total += int(c)
		}
		// Simulate a stream of no-split LSETs: the guarded path never marks the
		// tree stale and never rebuilds, so d stays the original build. Every
		// index must still resolve against the flat oracle.
		blob := make([]byte, 512)
		for step := 0; step < 5; step++ {
			idx := (step*step*911 + 7) % total
			lsetGuarded(d, r, blob, idx)
			if d.stale {
				t.Fatalf("chunks=%d step=%d: guarded LSET marked the tree stale", chunks, step)
			}
			for k := 0; k < total; k++ {
				gc, go_ := d.rank(k)
				fc, fo := flatRank(r, k)
				if gc != fc || go_ != fo {
					t.Fatalf("chunks=%d step=%d k=%d: fenwick=(%d,%d) flat=(%d,%d) without rebuild",
						chunks, step, k, gc, go_, fc, fo)
				}
			}
		}
	}
}
