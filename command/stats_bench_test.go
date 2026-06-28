package command

import (
	"sync/atomic"
	"testing"
)

// BenchmarkStatCallParallel measures the per-command call-counter bump under the
// kind of cross-core contention a saturating single-command load (GET-P64 against
// many clients) puts on it. Every goroutine bumps the same cmdStat.calls counter,
// the exact pattern the integrated fast path runs once statCallFast trimmed the
// bump down to that one counter. The striped counter spreads the bump across one
// cell per P, so this should stay roughly flat as -cpu rises instead of degrading
// the way a single shared atomic does.
func BenchmarkStatCallParallel(b *testing.B) {
	var cs cmdStat
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cs.calls.Add(1)
		}
	})
	if cs.calls.Load() == 0 {
		b.Fatal("no calls recorded")
	}
}

// BenchmarkStatCallParallelSingleAtomic is the same load on a plain shared
// atomic.Uint64, the counter shape calls had before striping. It is the baseline
// the striped benchmark above is read against: the gap between the two at high
// -cpu is the cross-core contention striping removes.
func BenchmarkStatCallParallelSingleAtomic(b *testing.B) {
	var n atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n.Add(1)
		}
	})
	if n.Load() == 0 {
		b.Fatal("no calls recorded")
	}
}
