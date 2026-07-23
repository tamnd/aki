package shard

import "testing"

// BenchmarkBatchNodeChurn measures hop-transport node allocation under a
// pipelined burst where the reader outruns the writer's recycle, so more nodes
// are outstanding at once than the per-connection L1 free list can hold. That is
// the regime the GamingPC tiny-set memory gate runs in: 512 connections at P16,
// where shard.newBatch was 98% of all allocation and the ~6 KB nodes drove peak
// VmHWM above the rivals (labs/f3/m0/31).
//
// The two arms differ only in what happens to a node the L1 cannot absorb:
//
//	l1only  drops it to the collector (the pre-change behavior), so the next
//	        take past a dry L1 allocates a fresh node.
//	shared  returns it to the runtime-wide sync.Pool, so the next take reuses it.
//
// allocs/op is the figure that matters. l1only allocates (depth-cap) nodes per
// round; shared drives it to about zero once the pool is warm, which is the peak
// RSS win the arena-embed flip's memory column needed.
func BenchmarkBatchNodeChurn(b *testing.B) {
	const (
		freeCap = 8  // L1 per-connection free-list capacity
		depth   = 64 // outstanding nodes per round: 8x the L1, a deep pipeline
	)

	b.Run("l1only", func(b *testing.B) {
		l1 := newL1Only(freeCap)
		out := make([]*hopBatch, depth)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range out {
				out[j] = l1.take()
			}
			for _, n := range out {
				l1.recycle(n)
			}
		}
	})

	b.Run("shared", func(b *testing.B) {
		r := New(2, 64<<20, 0)
		r.resolveConnCaps(Config{FreeListCap: freeCap})
		c := r.NewConn()
		out := make([]*hopBatch, depth)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range out {
				out[j] = c.take()
			}
			for _, n := range out {
				c.recycle(n)
			}
		}
	})
}

// l1Only reproduces the pre-change node cache: a per-connection free channel with
// no shared backing, so an overflow on recycle drops to the collector and an
// empty take allocates fresh. It exists only to give the benchmark the baseline
// the shared pool replaced.
type l1Only struct {
	free    chan *hopBatch
	dataCap int
	repCap  int
}

func newL1Only(cap int) *l1Only {
	return &l1Only{
		free:    make(chan *hopBatch, cap),
		dataCap: batchDataCap,
		repCap:  batchDataCap + 64*batchCap,
	}
}

func (l *l1Only) take() *hopBatch {
	select {
	case b := <-l.free:
		return b
	default:
		return newBatch(l.dataCap, l.repCap)
	}
}

func (l *l1Only) recycle(b *hopBatch) {
	b.reset()
	select {
	case l.free <- b:
	default: // L1 full: drop to the collector, the churn this benchmark exposes
	}
}
