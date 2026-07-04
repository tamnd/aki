package f1raw

import (
	"fmt"
	"testing"
)

// These benchmarks are the number slice 1 of spec 2064/f1_rewrite_ltm/18 rests on: the
// dense vector must answer a uniform random draw and a destructive draw in constant time
// where the order-statistic skip list answers them in an O(log n) descent that grows with
// cardinality. They build a set of N members and compare the two structures head to head at
// 100k and a million members, so a run either confirms the O(1) win on paper or sends the
// design back before any command-path work is spent on it.

// buildBenchSet fills a store with n members under prefix "s" through the real add triple and
// returns the store and the prefix. The vector is built eagerly so the draw benchmarks
// measure the steady-state draw, not the one-time lazy build.
func buildBenchSet(b *testing.B, n int) (*Store, []byte) {
	b.Helper()
	s := New(1<<20, 1<<30)
	s.SetTopKindFunc(func(byte) bool { return false })
	prefix := setPrefixBytes("s")
	s.CollRandEnsure(prefix)
	for i := 0; i < n; i++ {
		mk := memberKeyBytes("s", fmt.Sprintf("m%d", i))
		if _, err := s.PutKind(mk, nil, tKindSetMember); err != nil {
			b.Fatal(err)
		}
		s.CollInsert(mk, tKindSetMember)
		s.CollRandInsert(prefix, mk, tKindSetMember)
	}
	return s, prefix
}

// BenchmarkRandSelectVec is the non-destructive draw over the dense vector, an array index
// plus a key resolve. It should be flat across cardinality.
func BenchmarkRandSelectVec(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			s, prefix := buildBenchSet(b, n)
			var rng uint64 = 0x9e3779b97f4a7c15
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rng += 0x9e3779b97f4a7c15
				if _, ok := s.CollRandSelect(prefix, rng); !ok {
					b.Fatal("empty draw")
				}
			}
		})
	}
}

// BenchmarkRandSelectVecParallel is the slice 1 measurement: many goroutines draw from one
// hot key at once, the shape SRANDMEMBER runs under a P16 pipeline against a single giant set.
// The read draw takes the shard RLock and a caller-supplied random word, so concurrent draws
// on one key run in parallel instead of serializing on a single mutex. Compare its ns/op to
// the serial BenchmarkRandSelectVec above: a flat or near-flat number here is the read-side
// lock split doing its job, a number that climbs with -cpu is the serialization slice 1 is
// meant to remove. This is the pre-partitioning ceiling; the write path still needs doc 19.
func BenchmarkRandSelectVecParallel(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			s, prefix := buildBenchSet(b, n)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				// Each goroutine carries its own splitmix64 stream, matching the per-connection
				// rng the server feeds in; no shared draw state means no false contention.
				var rng uint64 = 0x9e3779b97f4a7c15
				for pb.Next() {
					rng += 0x9e3779b97f4a7c15
					z := rng
					z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
					z = (z ^ (z >> 27)) * 0x94d049bb133111eb
					if _, ok := s.CollRandSelect(prefix, z^(z>>31)); !ok {
						b.Fatal("empty draw")
					}
				}
			})
		})
	}
}

// BenchmarkRandSelectSkip is the same non-destructive draw over the order-statistic skip
// list (CollSelectAt at a random index), the path SRANDMEMBER runs today. It should grow
// with log(cardinality).
func BenchmarkRandSelectSkip(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			s, prefix := buildBenchSet(b, n)
			sh := s.rvec.shardFor(prefix)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sh.mu.Lock()
				idx := sh.drawIndex(n)
				sh.mu.Unlock()
				if _, ok := s.CollSelectAt(prefix, idx); !ok {
					b.Fatal("empty select")
				}
			}
		})
	}
}

// BenchmarkRandSelectRemoveVec is the destructive draw over the dense vector: pick, swap-
// remove, then re-add the drawn member so the cardinality stays fixed across iterations and
// the measured cost is the pick-plus-swap-remove, not a shrinking set. The re-add is the
// same O(1) append, so the pair is two O(1) vector ops plus the hash-index churn both paths
// share, which is why the two select-remove benchmarks are compared to each other, not read
// in absolute terms.
func BenchmarkRandSelectRemoveVec(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			s, prefix := buildBenchSet(b, n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k, ok := s.CollRandSelectRemove(prefix)
				if !ok {
					b.Fatal("empty select-remove")
				}
				// Re-add the same member so the set size is stable. The record still exists
				// (SPOP would DeleteKind it; here we keep it), so CollRandInsert re-appends
				// its offset, restoring density.
				s.CollRandInsert(prefix, k, tKindSetMember)
			}
		})
	}
}

// BenchmarkRandSelectRemoveSkip is the destructive draw over the skip list: the fused
// select-and-remove CollSelectRemoveAt SPOP runs today, then a CollInsert re-add to hold the
// cardinality fixed. This is the ~1.4us (100k) / ~2.2us (1M) path the probe measured; the
// vector benchmark above is the O(1) replacement.
func BenchmarkRandSelectRemoveSkip(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			s, prefix := buildBenchSet(b, n)
			sh := s.rvec.shardFor(prefix)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sh.mu.Lock()
				idx := sh.drawIndex(n)
				sh.mu.Unlock()
				k, ok := s.CollSelectRemoveAt(prefix, idx)
				if !ok {
					b.Fatal("empty select-remove")
				}
				// Re-insert to keep the skip list at n nodes, matching the vector bench.
				s.CollInsert(k, tKindSetMember)
			}
		})
	}
}
