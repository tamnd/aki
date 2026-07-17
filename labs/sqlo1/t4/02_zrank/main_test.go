package main

import (
	"math/rand"
	"testing"
)

// The oracle: the walk must land every absolute rank on exactly the
// run a brute prefix sum over the leaf counts picks, with the same
// residual offset, for every shape, before and after a churn of
// moves; and every index level's totals must stay the sum of their
// children throughout.

func checkLevels(t *testing.T, f *fence) {
	t.Helper()
	prev := make([]uint64, len(f.counts))
	for i, c := range f.counts {
		prev[i] = uint64(c)
	}
	for k, fan := range f.fan {
		want := make([]uint64, (len(prev)+fan-1)/fan)
		for i, v := range prev {
			want[i/fan] += v
		}
		if len(want) != len(f.levels[k]) {
			t.Fatalf("level %d: %d nodes, want %d", k, len(f.levels[k]), len(want))
		}
		for i := range want {
			if f.levels[k][i] != want[i] {
				t.Fatalf("level %d node %d: total %d, want %d", k, i, f.levels[k][i], want[i])
			}
		}
		prev = want
	}
}

func bruteRank(f *fence, r uint64) (int, uint64) {
	for i, c := range f.counts {
		if r < uint64(c) {
			return i, r
		}
		r -= uint64(c)
	}
	return len(f.counts) - 1, uint64(f.counts[len(f.counts)-1]) - 1
}

func TestRankOracle(t *testing.T) {
	shapes := []shape{
		{"flat", nil},
		{"p4", []int{4}},
		{"p4x4", []int{4, 4}},
		{"p3x5", []int{3, 5}},
		{"p7x2x3", []int{7, 2, 3}},
	}
	for _, sh := range shapes {
		rng := rand.New(rand.NewSource(7))
		f := buildFence(20_000, sh, rng)
		checkLevels(t, f)
		probe := func(tag string) {
			// Every boundary rank on both sides plus a random spray.
			var edge uint64
			for i := 0; i < len(f.counts) && i < 400; i++ {
				for _, r := range []uint64{edge, edge + uint64(f.counts[i]) - 1} {
					gr, goff := f.rank(r)
					br, boff := bruteRank(f, r)
					if gr != br || goff != boff {
						t.Fatalf("%s %s: rank(%d) = (%d,%d), brute (%d,%d)", sh.name, tag, r, gr, goff, br, boff)
					}
				}
				edge += uint64(f.counts[i])
			}
			for range 2000 {
				r := rng.Uint64() % f.total
				gr, goff := f.rank(r)
				br, boff := bruteRank(f, r)
				if gr != br || goff != boff {
					t.Fatalf("%s %s: rank(%d) = (%d,%d), brute (%d,%d)", sh.name, tag, r, gr, goff, br, boff)
				}
			}
		}
		probe("built")
		for range 5000 {
			a := rng.Intn(len(f.counts))
			for f.counts[a] <= 1 {
				a = rng.Intn(len(f.counts))
			}
			b := rng.Intn(len(f.counts))
			nodes := f.move(a, b)
			for k, ns := range nodes {
				want := 2
				ia, ib := a, b
				for j := 0; j <= k; j++ {
					ia, ib = ia/f.fan[j], ib/f.fan[j]
				}
				if ia == ib {
					want = 1
				}
				if len(ns) != want {
					t.Fatalf("%s: move(%d,%d) level %d touched %d nodes, want %d", sh.name, a, b, k, len(ns), want)
				}
			}
		}
		checkLevels(t, f)
		probe("churned")
	}
}

func TestScanRunWalksToOffset(t *testing.T) {
	img := buildRunImg()
	if got := scanRun(img, 0); got != 0 {
		t.Fatalf("offset 0 accumulated %d entries' scores, want the first (0)", got)
	}
	// Entry i carries sortable i<<32; the scan to offset k sums 0..k.
	var want uint64
	for i := uint64(0); i <= 10; i++ {
		want += i << 32
	}
	if got := scanRun(img, 10); got != want {
		t.Fatalf("offset 10 accumulated %#x, want %#x", scanRun(img, 10), want)
	}
}
