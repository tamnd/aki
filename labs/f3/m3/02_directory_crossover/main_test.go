package main

import (
	"testing"
)

// TestFlatFenwickAgree proves the two directory representations are
// interchangeable: for every dense index k in [0,total), selectFlat and
// fenwick.rank return the identical (chunk, rem) pair, across several chunk-count
// configs with random per-chunk counts, seeded deterministically. If they always
// agree, the crossover is a pure performance choice and nothing observable
// changes when the implementation slice swaps one for the other.
func TestFlatFenwickAgree(t *testing.T) {
	// Include a config with zero-count chunks: a chunk holding no live positions
	// is never selected by any k, and both directories must skip it the same way.
	configs := []struct {
		chunks int
		b      band
	}{
		{4, band{"64B", 56, 64}},
		{8, band{"small", 3, 4}},
		{17, band{"mixed", 0, 9}},
		{64, band{"64B", 56, 64}},
		{257, band{"small", 1, 5}},
	}
	for _, cfg := range configs {
		counts := makeCounts(cfg.chunks, cfg.b, 0xabcdef)
		f := newFenwick(counts)
		tot := total(counts)
		for k := 0; k < tot; k++ {
			fc, fr := selectFlat(counts, k)
			bc, br := f.rank(k)
			if fc != bc || fr != br {
				t.Fatalf("chunks=%d band=%s k=%d: flat=(%d,%d) fenwick=(%d,%d)",
					cfg.chunks, cfg.b.name, k, fc, fr, bc, br)
			}
		}
	}
}

// TestFenwickAddMatchesFlat checks the update paths stay in step: after the same
// stream of count bumps, the Fenwick tree still resolves every k the way a flat
// scan over the updated counts does. This guards the O(log chunks) mirror walk,
// so the update column in the sweep measures a correct operation, not a broken
// one that happens to be fast.
func TestFenwickAddMatchesFlat(t *testing.T) {
	counts := makeCounts(32, band{"64B", 56, 64}, 0x1234)
	f := newFenwick(counts)

	rng := xorshift(0x77)
	for i := 0; i < 500; i++ {
		idx := int(rng.next() % uint64(len(counts)))
		counts[idx]++
		f.add(idx, 1)
	}
	// A few decrements too, so the mirror walk is exercised both ways.
	for i := 0; i < 100; i++ {
		idx := int(rng.next() % uint64(len(counts)))
		if counts[idx] == 0 {
			continue
		}
		counts[idx]--
		f.add(idx, -1)
	}

	tot := total(counts)
	for k := 0; k < tot; k++ {
		fc, fr := selectFlat(counts, k)
		bc, br := f.rank(k)
		if fc != bc || fr != br {
			t.Fatalf("after updates k=%d: flat=(%d,%d) fenwick=(%d,%d)", k, fc, fr, bc, br)
		}
	}
}

// TestFenwickBuildMatchesIncremental checks the O(n) tree build agrees with an
// empty tree filled by adds, so newFenwick is not the odd path out.
func TestFenwickBuildMatchesIncremental(t *testing.T) {
	counts := makeCounts(48, band{"small", 1, 7}, 0x99)
	built := newFenwick(counts)

	zero := make([]uint64, len(counts))
	incr := newFenwick(zero)
	for i, c := range counts {
		incr.add(i, int64(c))
	}
	for i := 1; i <= built.n; i++ {
		if built.tree[i] != incr.tree[i] {
			t.Fatalf("tree[%d]: build=%d incremental=%d", i, built.tree[i], incr.tree[i])
		}
	}
}
