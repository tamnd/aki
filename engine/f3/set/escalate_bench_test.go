package set

import "testing"

// The F13 escalation microbenchmarks (doc 11 section 5.4, lab 07). They put the
// two-level draw on the clock against the flat single-owner weighted draw at 4M
// members (P16), the shape the hot-key SPOP gate row exercises. The escalated
// draw must not regress single-thread: it wraps the same in-partition weighted
// walk in a k-group scatter scan, and the fan-out (which the execution model
// wires) is the aggregate lever, not a single-thread speedup. buildBand and
// benchThreshold come from partition_bench_test.go; these are darwin numbers, the
// binding read is the GamingPC gate box.

// escalatedBand builds a 4M-member partitioned set and escalates it into k
// draw-groups, so the draw benchmarks below run the two-level path.
func escalatedBand(b *testing.B, k int) *set {
	benchThreshold(b, partTarget)
	s := buildBand(4_000_000)
	if s.enc != encPartitioned {
		b.Fatalf("enc = %s, want partitioned", s.enc)
	}
	if !s.escalateDraws(k) {
		b.Fatalf("escalateDraws(%d) did not engage at P=%d", k, len(s.part.parts))
	}
	return s
}

// BenchmarkLocateEscalated4M isolates the two-level escalated locate at 4M
// members (P16 split into 4 groups): the k-group scatter scan plus the
// group-local weighted walk. It is the direct counterpart to
// BenchmarkPartLocate4M's flat scan, so the two rows read as the single-thread
// escalation tax.
func BenchmarkLocateEscalated4M(b *testing.B) {
	s := escalatedBand(b, 4)
	pt := s.part
	total := pt.total
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, _ := pt.locateEscalated(uint64(i) * 2654435761 % total)
		sinkInt = p
	}
}

// BenchmarkDrawOneEscalated4M is the end-to-end escalated draw at 4M members: the
// scatter pick, the group-local weighted walk, and the vector read. It reads
// against BenchmarkDrawOnePart4M, the flat draw at the same cardinality.
func BenchmarkDrawOneEscalated4M(b *testing.B) {
	s := escalatedBand(b, 4)
	g := benchReg()
	var sc [64]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkBytes = s.drawOne(g, sc[:])
	}
}
