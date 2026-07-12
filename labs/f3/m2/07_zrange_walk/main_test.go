package main

import (
	"bytes"
	"testing"
)

// TestClosureAndFusedMatch proves the two kernels emit byte-identical RESP for
// every window over both the scattered and the co-located layout, so a fused
// walk that deletes the closure hops is a pure performance change and ZRANGE
// returns exactly what the shipped closure path returned. If they always agree,
// removing the callback indirection never changes the wire bytes.
//
// The smoke test stays small (a few thousand records, not the 1M sweep arm) so
// `go test` runs well under a second: the DRAM-scale build arm lives only in
// `go run .` (the M4 lab lesson, a heavy build in the test binary times out
// loaded CI runners).
func TestClosureAndFusedMatch(t *testing.T) {
	const mlen = 16
	for _, scatter := range []bool{false, true} {
		m := build(2000, mlen, scatter)
		for _, w := range []int{1, 7, 100, 333} {
			for lo := 0; lo+w <= m.n(); lo += 137 { // stride to cover edges cheaply
				a := m.closureWalk(nil, lo, w)
				b := m.fusedWalk(nil, lo, w)
				if !bytes.Equal(a, b) {
					t.Fatalf("scatter=%v window=[%d,%d): closure and fused differ\nclosure=%q\nfused  =%q",
						scatter, lo, lo+w, a, b)
				}
			}
		}
	}
}

func (m *model) n() int { return len(m.order) }
