package zset

import (
	"testing"
)

// The algebra kernel benchmarks at the lab-05 shapes: two sources of N members
// with 50 percent overlap, at 10k and 1M members per source. Each reports the
// per-output-element cost (ns/elem) alongside ns/op, the figure the M2 gate reads
// to confirm the sort-at-end plan holds on the Linux box.

// buildOverlap builds two native-band zsets of n members each sharing half their
// members, the equal-overlap shape the lab priced.
func buildOverlap(n int) (*zset, *zset) {
	a := newZset()
	b := newZset()
	for i := 0; i < n; i++ {
		ma := "m" + itoa(i)
		a.update([]byte(ma), float64(i), flags{})
		// b shares the lower half of a's members and adds n/2 fresh ones.
		if i < n/2 {
			b.update([]byte(ma), float64(i)+0.5, flags{})
		} else {
			b.update([]byte("n"+itoa(i)), float64(i)+0.5, flags{})
		}
	}
	return a, b
}

func benchAlgebra(b *testing.B, n int, run func(ops []operand) int) {
	za, zb := buildOverlap(n)
	ops := []operand{{z: za, weight: 1}, {z: zb, weight: 1}}
	elems := run(ops) // warm once to size the per-element metric
	if elems == 0 {
		b.Fatal("empty result")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run(ops)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(elems), "ns/elem")
}

func BenchmarkZUnion10k(b *testing.B) {
	benchAlgebra(b, 10_000, func(ops []operand) int { return len(union(ops, aggSum)) })
}
func BenchmarkZUnion1M(b *testing.B) {
	benchAlgebra(b, 1_000_000, func(ops []operand) int { return len(union(ops, aggSum)) })
}
func BenchmarkZInter10k(b *testing.B) {
	benchAlgebra(b, 10_000, func(ops []operand) int { return len(intersect(ops, aggSum)) })
}
func BenchmarkZInter1M(b *testing.B) {
	benchAlgebra(b, 1_000_000, func(ops []operand) int { return len(intersect(ops, aggSum)) })
}
func BenchmarkZDiff10k(b *testing.B) {
	benchAlgebra(b, 10_000, func(ops []operand) int { return len(diff(ops)) })
}
func BenchmarkZDiff1M(b *testing.B) {
	benchAlgebra(b, 1_000_000, func(ops []operand) int { return len(diff(ops)) })
}
