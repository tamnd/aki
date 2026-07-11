package set

import "testing"

// The algebra-driver benchmarks (the PRED-F3-M1-SINTER shapes, doc 11 section
// 12.2): SINTER over equal-overlap and skewed operand pairs, timed on the probe
// path (flag off) against the merge path (flag on), and SINTERCARD with a small
// LIMIT to show the early exit. These are darwin microbenchmarks; the merge win
// is a gate-box (Linux DRAM) effect (lab 03, lab 05), so on this box the two arms
// mostly track each other and the numbers are here to prove the dispatcher wires
// both paths, not to read the 2x.

// benchSetPair builds two native sets: aN members and bN members overlapping by
// half of the smaller. maintain engages the arrays so the driver takes the merge
// path when the pair is eligible.
func benchSetPair(aN, bN, w int, maintain bool) (*set, *set) {
	defer SetAlgebraMaintain(SetAlgebraMaintain(maintain))
	a := newSet([]byte("seed-a"))
	for _, m := range gen("a", 0, aN, w) {
		a.add([]byte(m))
	}
	b := newSet([]byte("seed-b"))
	overlap := aN / 2
	for _, m := range gen("a", 0, overlap, w) { // shared prefix range
		b.add([]byte(m))
	}
	for _, m := range gen("b", 0, bN-overlap, w) {
		b.add([]byte(m))
	}
	return a, b
}

func benchSinter(b *testing.B, aN, bN int, maintain bool) {
	sa, sb := benchSetPair(aN, bN, 8, maintain)
	sets := []*set{sa, sb}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = false
		sinter(sets, func(m []byte) { sink = true })
	}
}

// Equal overlap: 1M and 1M, probe against merge.
func BenchmarkSinterEqualProbe(b *testing.B) { benchSinter(b, 1_000_000, 1_000_000, false) }
func BenchmarkSinterEqualMerge(b *testing.B) { benchSinter(b, 1_000_000, 1_000_000, true) }

// Skewed pair: 1k into 1M (ratio 1000, past the k=7 crossover, so the driver
// probes on both arms; the merge arm still has the arrays built).
func BenchmarkSinterSkewProbe(b *testing.B) { benchSinter(b, 1_000, 1_000_000, false) }
func BenchmarkSinterSkewMerge(b *testing.B) { benchSinter(b, 1_000, 1_000_000, true) }

// Merge-eligible pair: 200k and 1M (ratio 5, below k=7), the shape the flag-on
// arm actually routes to the merge kernel.
func BenchmarkSinterMergeEligibleProbe(b *testing.B) { benchSinter(b, 200_000, 1_000_000, false) }
func BenchmarkSinterMergeEligibleMerge(b *testing.B) { benchSinter(b, 200_000, 1_000_000, true) }

func benchSintercard(b *testing.B, aN, bN, limit int, maintain bool) {
	sa, sb := benchSetPair(aN, bN, 8, maintain)
	sets := []*set{sa, sb}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkN = sintercard(sets, limit)
	}
}

// SINTERCARD with a small LIMIT: the early exit should make the count independent
// of the operand sizes.
func BenchmarkSintercardLimit10Probe(b *testing.B) {
	benchSintercard(b, 1_000_000, 1_000_000, 10, false)
}
func BenchmarkSintercardLimit10Merge(b *testing.B) {
	benchSintercard(b, 1_000_000, 1_000_000, 10, true)
}
func BenchmarkSintercardUnlimited(b *testing.B) {
	benchSintercard(b, 1_000_000, 1_000_000, 0, false)
}

var sinkN int
