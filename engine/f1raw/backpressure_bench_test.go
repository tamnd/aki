package f1raw

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkBackpressureConcurrent measures write throughput when concurrent writers outrun the
// arena and block on the migrator, the engine-level stand-in for SADD-under-migration at P16. It
// drives many arenas' worth of distinct records through a small segmented arena with the migrator
// engaged, from writers goroutines at once, so nearly every Set fills the arena and waits in
// waitForSegment for a drain to free a segment. It reports ns per write, which is dominated by that
// backpressure wait: the adaptive spin (migSpinIters) should return a just-freed segment in
// microseconds where the bare sleeping poll pinned every blocked write near one migWaitStep.
//
// Run the A/B by flipping migSpinIters (256 vs 0) and comparing ns/op; the workload is otherwise
// identical. It is a benchmark, so it never runs under go test without -bench.
func BenchmarkBackpressureConcurrent(b *testing.B) {
	const (
		valLen  = 256
		nSeg    = 6
		writers = 16
	)
	// Each segment is floored at maxRecordBytes so no record spans one, so size the arena
	// from that floor: nSeg+1 segments plus the overflow region. Distinct keys never delete,
	// so the resident set fills the small nSeg-segment record region within a few thousand
	// writes and every steady-state write then blocks in waitForSegment until the migrator
	// drains a segment cold. That drain-wait is the quantity this benchmark measures.
	//
	// The record region stays small (nSeg segments) so backpressure engages, but the resident
	// index must hold an entry for every distinct key the benchmark ever writes (cold ones too:
	// a migrated record keeps its index entry). So size the index generously and give it a large
	// overflow region: at 1<<22 primary buckets (~29M slots) the driven key count stays at load
	// far under one per bucket for any reasonable benchtime, so almost no overflow bucket is
	// needed and the benchmark measures record-arena backpressure rather than exhausting the
	// index. An absurd benchtime that still outruns this sizing surfaces the legible ErrIndexFull
	// (raise IndexBuckets), distinct from the ErrFull the record arena would report.
	segSize := int(align8(maxRecordBytes))
	ov := 1 << 20 // 1 MiB of overflow buckets, ample headroom past the ~zero the load needs
	arena := 8 + ov + (nSeg+1)*segSize
	s := NewSegmented(1<<22, arena, segSize, ov)
	if !s.segmented {
		b.Fatal("NewSegmented did not enable the segmented arena")
	}
	if err := s.EnableColdRecords(filepath.Join(b.TempDir(), "recs.log")); err != nil {
		b.Fatalf("EnableColdRecords: %v", err)
	}
	defer s.Close()
	s.EnableMigrator()

	val := make([]byte, valLen)
	for i := range val {
		val[i] = byte('a' + i%26)
	}

	b.ResetTimer()
	var next int64
	var wg sync.WaitGroup
	per := b.N / writers
	for range writers {
		wg.Go(func() {
			var kb [16]byte
			for {
				i := atomic.AddInt64(&next, 1) - 1
				if int(i) >= writers*per {
					return
				}
				// distinct key per op so every write is a fresh record that forces the arena to fill
				for j := range 16 {
					kb[j] = byte(i>>(uint(j)*4)) | 0x40
				}
				if err := s.Set(kb[:], val); err != nil {
					// arena genuinely full past the wait budget: report and stop this writer
					b.Errorf("Set blocked past the backpressure budget at i=%d: %v", i, err)
					return
				}
			}
		})
	}
	wg.Wait()
	b.StopTimer()
}
