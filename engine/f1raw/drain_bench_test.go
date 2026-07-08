package f1raw

import (
	"fmt"
	"path/filepath"
	"testing"
)

// BenchmarkDrainSegment measures the migrator's per-record drain cost on a full multi-record segment,
// the throughput the batched two-phase drain targets. Each iteration fills one segment with distinct
// large records, then times drainSegment sinking the whole segment cold. The batched drain encodes
// every record's frame into one buffer and issues a single pwrite for the segment before flipping any
// index entry, so the cold-region sink cost that dominated the SET larger-than-memory write bound
// falls from one pwrite per record to one per segment. The reported recs/drain metric divides the
// per-op time back to a per-record figure so the A/B against the per-record append is direct.
//
// The fill runs under a stopped timer, so ns/op is the drain alone. The migrator is not enabled: the
// benchmark drives drainSegment by hand on the just-filled, now-non-current segment, exactly the call
// the background goroutine makes, and each drain retires its segment back to the free list so the fill
// never starves.
func BenchmarkDrainSegment(b *testing.B) {
	segSize := int(align8(maxRecordBytes))
	ov := 1 << 16
	const nSeg = 64
	arena := 8 + ov + (nSeg+1)*segSize
	s := NewSegmented(1<<14, arena, segSize, ov)
	if !s.segmented {
		b.Fatal("NewSegmented did not enable the segmented arena")
	}
	if err := s.EnableColdRecords(filepath.Join(b.TempDir(), "recs.log")); err != nil {
		b.Fatalf("EnableColdRecords: %v", err)
	}
	defer s.Close()

	val := make([]byte, churnValLen)
	for i := range val {
		val[i] = byte('a' + i%26)
	}

	// fillOneSegment writes distinct records into the current segment until it advances, returning the
	// full, now-non-current segment and how many records it holds. uniq keeps every key distinct across
	// iterations so a fill never overwrites a prior record and every filled record drains live.
	uniq := 0
	fillOneSegment := func() (uint64, int) {
		seg := s.curSeg.Load()
		n := 0
		for s.curSeg.Load() == seg {
			k := fmt.Appendf(nil, "k%09d", uniq)
			uniq++
			if err := s.Set(k, val); err != nil {
				b.Fatalf("Set: %v", err)
			}
			if s.curSeg.Load() != seg {
				s.Delete(k) // spilled into the next segment; keep the count exact for seg
				break
			}
			n++
		}
		if n == 0 {
			b.Fatal("filled no records before the segment advanced")
		}
		return seg, n
	}

	recs := 0
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		seg, n := fillOneSegment()
		recs += n
		b.StartTimer()
		s.drainSegment(seg)
	}
	b.StopTimer()
	b.ReportMetric(float64(recs)/float64(b.N), "recs/drain")
}
