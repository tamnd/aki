package set

import "testing"

// The algebra microbenchmarks (doc 11 sections 6.3 and 6.6). The write tax is
// SADD's per-member cost with maintenance on against off, which proves the
// point-op path is unchanged when the flag is off and quotes the enabled tax
// (lab 05 predicts ~55-68ns amortized with the scaled tail on this box). The
// kernel benchmark is the merge's ns per member streamed, the sequential-stream
// cost that is the design's 2x lever. These are darwin microbenchmarks; the gate
// rows key on the GamingPC server numbers.

// benchAddTax builds an n-member table through htable.add, the maintained write
// path, and reports ns per member so growth, rehash, and any tail flush amortize
// across the build.
func benchAddTax(b *testing.B, n int, maintain bool) {
	defer SetAlgebraMaintain(SetAlgebraMaintain(maintain))
	keys := members16(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := newHashtable(n)
		for _, k := range keys {
			h.add(k)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/member")
}

func BenchmarkSAddMaintainOff10k(b *testing.B) { benchAddTax(b, 10_000, false) }
func BenchmarkSAddMaintainOn10k(b *testing.B)  { benchAddTax(b, 10_000, true) }
func BenchmarkSAddMaintainOff1M(b *testing.B)  { benchAddTax(b, 1_000_000, false) }
func BenchmarkSAddMaintainOn1M(b *testing.B)   { benchAddTax(b, 1_000_000, true) }

// benchMerge builds two indexed n-member tables at overlap 0.5 and times one
// full intersection, reporting ns per member streamed (2n members walked).
func benchMerge(b *testing.B, n int) {
	aIDs := make([]int, n)
	bIDs := make([]int, n)
	for i := range aIDs {
		aIDs[i] = i
		bIDs[i] = i + n/2 // half overlap
	}
	ha := indexedFrom(aIDs, 0)
	hb := indexedFrom(bIDs, 0)
	confirm := confirmOf(ha, hb)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sa, _, _ := ha.mergeStream(nil)
		sb, _, _ := hb.mergeStream(nil)
		sink = false
		mergeIntersect(&sa, &sb, func(x, y uint32) bool {
			if confirm(x, y) {
				sink = true
				return true
			}
			return false
		})
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*2*n), "ns/member")
}

func BenchmarkMergeIntersect10k(b *testing.B)  { benchMerge(b, 10_000) }
func BenchmarkMergeIntersect100k(b *testing.B) { benchMerge(b, 100_000) }
func BenchmarkMergeIntersect1M(b *testing.B)   { benchMerge(b, 1_000_000) }
