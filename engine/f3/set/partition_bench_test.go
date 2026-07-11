package set

import (
	"math"
	"testing"
)

// The partitioned-band microbenchmarks (doc 11 section 4, lab 04). They put the
// band's three costs on the clock against the unpartitioned native table at the
// same cardinality: the point-op probe (SISMEMBER) and insert (SADD) must not
// regress from splitting the table, the weighted draw's branchless prefix scan
// must hold its ~10ns bound at 4M members, and the maintained write tax must stay
// near the 154.5ns unpartitioned baseline from #598. Partitioned and
// unpartitioned are the same corpus built under two thresholds: the production
// 262144 engages the band, and a threshold above the corpus keeps one flat table.
// These are darwin microbenchmarks; the gate rows key on the GamingPC numbers.

// benchThreshold sets the engagement threshold for one benchmark and restores it
// on cleanup, so a run can force the partitioned band on or off at a fixed
// cardinality.
func benchThreshold(b *testing.B, n int) {
	old := partitionThreshold
	partitionThreshold = n
	b.Cleanup(func() { partitionThreshold = old })
}

// buildBand builds an n-member set under the current threshold, so the caller's
// benchThreshold decides partitioned versus one flat native table.
func buildBand(n int) *set {
	s := newSet([]byte("seed-not-an-int-so-listpack-then-table"))
	keys := members16(n)
	for _, k := range keys {
		s.add(k)
	}
	return s
}

func benchPartIsMember(b *testing.B, n int, partition bool) {
	if partition {
		benchThreshold(b, partTarget) // production threshold engages the band
	} else {
		benchThreshold(b, math.MaxInt) // above the corpus: one flat native table
	}
	s := buildBand(n)
	wantPart := encHashtable
	if partition {
		wantPart = encPartitioned
	}
	if s.enc != wantPart {
		b.Fatalf("enc = %s, want %s", s.enc, wantPart)
	}
	probes := members16(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = s.has(probes[i%n])
	}
}

func BenchmarkSIsMemberPart1M(b *testing.B)   { benchPartIsMember(b, 1_000_000, true) }
func BenchmarkSIsMemberUnpart1M(b *testing.B) { benchPartIsMember(b, 1_000_000, false) }
func BenchmarkSIsMemberPart4M(b *testing.B)   { benchPartIsMember(b, 4_000_000, true) }
func BenchmarkSIsMemberUnpart4M(b *testing.B) { benchPartIsMember(b, 4_000_000, false) }

func benchPartAdd(b *testing.B, n int, partition bool) {
	if partition {
		benchThreshold(b, partTarget)
	} else {
		benchThreshold(b, math.MaxInt)
	}
	keys := members16(n)
	seed := []byte("seed-not-an-int-so-listpack-then-table")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := newSet(seed)
		for _, k := range keys {
			s.add(k)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/member")
}

func BenchmarkSAddPart1M(b *testing.B)   { benchPartAdd(b, 1_000_000, true) }
func BenchmarkSAddUnpart1M(b *testing.B) { benchPartAdd(b, 1_000_000, false) }
func BenchmarkSAddPart4M(b *testing.B)   { benchPartAdd(b, 4_000_000, true) }
func BenchmarkSAddUnpart4M(b *testing.B) { benchPartAdd(b, 4_000_000, false) }

// BenchmarkDrawOnePart4M times the whole SRANDMEMBER draw over a 4M partitioned
// set: the weighted partition pick plus the in-partition vector read. It is the
// end-to-end draw the command runs.
func BenchmarkDrawOnePart4M(b *testing.B) {
	benchThreshold(b, partTarget)
	s := buildBand(4_000_000)
	if s.enc != encPartitioned {
		b.Fatalf("enc = %s, want partitioned", s.enc)
	}
	g := benchReg()
	var sc [64]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkBytes = s.drawOne(g, sc[:])
	}
}

// BenchmarkPartLocate4M isolates the branchless weighted prefix scan at 4M
// members (P16): it times locate alone, the ~10ns bound lab 04 sets for the scan
// that must not drift to the 25-28ns a scalar early-exit walk hit at this scale.
// The flat position sweeps the whole range so every partition boundary is
// exercised.
func BenchmarkPartLocate4M(b *testing.B) {
	benchThreshold(b, partTarget)
	s := buildBand(4_000_000)
	if s.enc != encPartitioned {
		b.Fatalf("enc = %s, want partitioned", s.enc)
	}
	pt := s.part
	total := pt.total
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, _ := pt.locate(uint64(i) * 2654435761 % total)
		sinkInt = p
	}
}

// benchPartMaintainTax builds an n-member set under the maintained write path,
// partitioned against one flat table, and reports ns per member. The
// unpartitioned arm is the #598 154.5ns baseline; the partitioned arm should sit
// near it, since maintenance now runs per partition at n/P scale rather than over
// the whole set.
func benchPartMaintainTax(b *testing.B, n int, partition bool) {
	defer SetAlgebraMaintain(SetAlgebraMaintain(true))
	if partition {
		benchThreshold(b, partTarget)
	} else {
		benchThreshold(b, math.MaxInt)
	}
	keys := members16(n)
	seed := []byte("seed-not-an-int-so-listpack-then-table")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := newSet(seed)
		for _, k := range keys {
			s.add(k)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/member")
}

func BenchmarkSAddMaintainPart1M(b *testing.B)   { benchPartMaintainTax(b, 1_000_000, true) }
func BenchmarkSAddMaintainUnpart1M(b *testing.B) { benchPartMaintainTax(b, 1_000_000, false) }
