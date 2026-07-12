package main

import "testing"

// TestKernelsAgree pins the property the whole lab rests on: the three
// accumulation kernels return the identical prefix sum for every prefix length,
// so the hoisted and SWAR forms are byte-identical to the per-element bCount loop
// the slice replaces. It sweeps c across the full [0,arity] range against a
// realistic block, including c==0 (empty prefix) and the odd lengths the SWAR
// tail handles.
func TestKernelsAgree(t *testing.T) {
	block := makeBlock(1 << 20)
	for c := 0; c <= arity; c++ {
		want := perElem(block, c)
		if got := hoisted(block, c); got != want {
			t.Fatalf("c=%d: hoisted=%d, want %d", c, got, want)
		}
		if got := swar(block, c); got != want {
			t.Fatalf("c=%d: swar=%d, want %d", c, got, want)
		}
	}
}

// TestQuickSweep drives the sweep body on a tiny op budget so the smoke run stays
// under a second on a loaded CI runner. It proves the harness produces finite,
// positive per-op timings for every kernel and prefix length without building the
// DRAM-scale cold ring the reported run uses (main() sizes that; the CI-timeout
// rule keeps it out of the test).
func TestQuickSweep(t *testing.T) {
	block := makeBlock(1 << 20)
	ring := [][]byte{block}
	for _, c := range []int{1, 4, 15} {
		for _, k := range []kernel{{"perElem", perElem}, {"hoisted", hoisted}, {"swar", swar}} {
			if ns := bench(k, ring, 0, c, 50_000); ns <= 0 {
				t.Fatalf("%s c=%d: non-positive ns %.3f", k.name, c, ns)
			}
		}
	}
}
