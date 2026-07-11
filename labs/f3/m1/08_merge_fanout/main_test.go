package main

import (
	"fmt"
	"testing"
)

// oracleCount is the map intersection over the raw member strings, the
// ground truth the kernels are checked against.
func oracleCount(a, b *op) int {
	seen := make(map[string]bool, len(a.slab)/memW)
	for i := 0; i+memW <= len(a.slab); i += memW {
		seen[string(a.slab[i:i+memW])] = true
	}
	count := 0
	for i := 0; i+memW <= len(b.slab); i += memW {
		if seen[string(b.slab[i:i+memW])] {
			count++
		}
	}
	return count
}

// TestRunsPartitionDomain proves the build's split is a partition: every
// entry lands in the run its top hash bits select, runs are sorted, and the
// entry total is the cardinality.
func TestRunsPartitionDomain(t *testing.T) {
	for _, p := range []int{4, 16, 64} {
		o := build(p, 10_000, 5_000, 'a')
		lg := 0
		for 1<<lg < p {
			lg++
		}
		total := 0
		for i, r := range o.runs {
			for j, e := range r {
				if int(e.h>>(64-lg)) != i {
					t.Fatalf("P=%d run %d entry %d: hash %#x routed wrong", p, i, j, e.h)
				}
				if j > 0 && r[j-1].h > e.h {
					t.Fatalf("P=%d run %d not sorted at %d", p, i, j)
				}
			}
			total += len(r)
		}
		if total != 10_000 {
			t.Fatalf("P=%d holds %d entries, want 10000", p, total)
		}
	}
}

// TestSeqMatchesOracle checks the sequential per-partition merge against the
// map oracle across sizes, partition counts, and overlaps.
func TestSeqMatchesOracle(t *testing.T) {
	for _, p := range []int{4, 16, 64} {
		for _, n := range []int{300, 5_000, 40_000} {
			for _, ov := range []float64{0, 0.1, 0.5, 0.9, 1} {
				a, b := buildPair(p, n, ov)
				want := oracleCount(a, b)
				if want != int(float64(n)*ov) {
					t.Fatalf("oracle %d, expected shared %d", want, int(float64(n)*ov))
				}
				if got := seq(a, b); got != want {
					t.Fatalf("P=%d n=%d ov=%.1f: seq %d, want %d", p, n, ov, got, want)
				}
			}
		}
	}
}

// TestExecutorsAgree checks the spawn and pool executors return exactly the
// sequential count for every k, so donating the group tasks changes nothing
// but the wall clock.
func TestExecutorsAgree(t *testing.T) {
	a, b := buildPair(16, 30_000, 0.5)
	want := seq(a, b)
	for _, k := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			if got := spawn(a, b, k); got != want {
				t.Fatalf("spawn k=%d: %d, want %d", k, got, want)
			}
			pl := newPool(k)
			defer pl.stop()
			for r := 0; r < 3; r++ {
				if got := pl.run(a, b); got != want {
					t.Fatalf("pool k=%d rep %d: %d, want %d", k, r, got, want)
				}
			}
		})
	}
}

// TestSkewGallops checks the kernel on a heavily skewed pair, the shape the
// galloping advance exists for.
func TestSkewGallops(t *testing.T) {
	a := build(16, 500, 250, 'a')
	b := build(16, 100_000, 250, 'b')
	if got, want := seq(a, b), oracleCount(a, b); got != want {
		t.Fatalf("skewed: %d, want %d", got, want)
	}
}
