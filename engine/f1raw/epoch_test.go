package f1raw

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// churnSegColdStore builds a segmented store with a cold record region for the reclaim-under-churn
// test. It is sized differently from arena_test.go's segStore: that helper reserves only a few
// overflow buckets, which is fine for a fill-once test but not for one that keeps a segment's keys
// live in the index (migrated, not deleted) while writing thousands more. So this gives a roomy
// index and overflow region, and the records it writes are large (churnValLen) so a segment fills
// in tens of writes rather than thousands, keeping the live key count well under the index size.
func churnSegColdStore(t *testing.T, nSegWanted int) *Store {
	t.Helper()
	segSize := int(align8(maxRecordBytes))
	ov := 1 << 16
	arena := 8 + ov + (nSegWanted+1)*segSize
	s := NewSegmented(1<<14, arena, segSize, ov)
	if !s.segmented {
		t.Fatal("NewSegmented did not enable the segmented arena")
	}
	if len(s.segs) < nSegWanted {
		t.Fatalf("arena holds %d segments, wanted at least %d", len(s.segs), nSegWanted)
	}
	if err := s.EnableColdRecords(filepath.Join(t.TempDir(), "recs.log")); err != nil {
		t.Fatalf("EnableColdRecords: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// churnValLen makes each record big enough that a segment holds only tens of them, so the churn
// test fills segments with a tractable number of writes and never floods the index.
const churnValLen = 2600

// churnVal builds a distinct verifiable value of churnValLen bytes for key prefix+index, so a read
// after all the migration and reuse proves the exact bytes survived rather than just the length.
func churnVal(prefix string, i int) []byte {
	v := make([]byte, churnValLen)
	head := fmt.Sprintf("val-%s-%06d:", prefix, i)
	copy(v, head)
	for j := len(head); j < len(v); j++ {
		v[j] = byte('a' + (i+j)%26)
	}
	return v
}

// fillSegBig writes churnVal records under prefix until the current segment advances, returning the
// segment they landed in and their keys, the large-record analogue of arena_test.go's fillSeg.
func fillSegBig(t *testing.T, s *Store, prefix string) (startSeg uint64, keys [][]byte) {
	t.Helper()
	startSeg = s.curSeg.Load()
	for i := 0; s.curSeg.Load() == startSeg; i++ {
		k := []byte(fmt.Sprintf("%s%06d", prefix, i))
		if err := s.Set(k, churnVal(prefix, i)); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
		if s.curSeg.Load() != startSeg {
			s.Delete(k) // spilled into the next segment; keep the returned set exactly startSeg's records
			break
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		t.Fatal("filled no keys before the segment advanced")
	}
	return startSeg, keys
}

// TestSegmentLiveDrainsToZero is the M3 slice-1 gate for segment live-byte accounting (doc 21
// section 5.1, section 6). M0 and M1 only ever incremented a segment's live counter, on
// allocation, so a segment never reported itself drained; the drain loop's completion check
// (seg.live == 0) and D15's emptiest-segment selection both need live to fall as records leave
// the index. This fills one segment, checks its live counter equals the resident bytes of the
// records it holds, then removes every one of those records by both routes a real drain uses, a
// delete and a cold migration, and asserts the counter lands exactly on zero. A drained segment
// then retires and frees at once because no reader is active, closing the accounting loop.
func TestSegmentLiveDrainsToZero(t *testing.T) {
	s := churnSegColdStore(t, 5)

	seg0, keysA := fillSegBig(t, s, "a")

	// The live counter equals the summed resident bytes of the records the fill left in seg0.
	var want int64
	for _, k := range keysA {
		off, _, _, _, ok := s.find(k, hash(k), stringKind)
		if !ok {
			t.Fatalf("filled key %q is missing from the index", k)
		}
		want += int64(s.recBytesAt(off))
	}
	if got := s.segs[seg0].live.Load(); got != want {
		t.Fatalf("seg %d live = %d, want %d (summed resident record bytes)", seg0, got, want)
	}

	// Remove every record: half by delete, half by cold migration. Each route must charge the
	// record's bytes back to seg0's live counter, so both together drain it to exactly zero.
	for i, k := range keysA {
		if i%2 == 0 {
			if !s.Delete(k) {
				t.Fatalf("Delete %q returned false", k)
			}
		} else {
			if !s.MigrateToCold(k, stringKind) {
				t.Fatalf("MigrateToCold %q returned false", k)
			}
		}
	}
	if got := s.segs[seg0].live.Load(); got != 0 {
		t.Fatalf("after draining every record, seg %d live = %d, want 0", seg0, got)
	}

	// Drained to zero and no reader active, so retiring frees the segment at once.
	before := len(s.freeSegs)
	s.retireSegment(seg0)
	if len(s.freeSegs) != before+1 {
		t.Fatalf("drained segment did not free: freeSegs %d -> %d, want +1", before, len(s.freeSegs))
	}

	// The deleted keys are gone; the migrated keys still read their exact value from cold.
	for i, k := range keysA {
		v, ok := s.Get(k, nil)
		if i%2 == 0 {
			if ok {
				t.Fatalf("deleted key %q still present as %q", k, v)
			}
			continue
		}
		if !ok || string(v) != string(churnVal("a", i)) {
			t.Fatalf("migrated key %q = %q,%v; want its cold value", k, v, ok)
		}
	}
}

// TestEpochReclaimGate is the deterministic M2 gate for the retire-then-free machinery (doc 21
// section 7, D18 to D20). It checks three things without concurrency, so a failure points at the
// epoch math rather than a scheduling accident: a segment with no active reader frees the moment
// it is retired; a segment retired while a reader holds the retire epoch stays pinned; and that
// same reader republishing its epoch (the D20 between-batch refresh) lets the segment free even
// though the reader is still active, so a long cursor never starves reclamation.
func TestEpochReclaimGate(t *testing.T) {
	s := segStore(t, 6)
	last := uint64(len(s.segs) - 1)
	prev := last - 1

	// No active reader: retiring frees at once, because the safe epoch (no published slots) is
	// the current global epoch, which the bump just pushed past the retire epoch.
	freeBefore := len(s.freeSegs)
	s.retireSegment(last)
	if len(s.freeSegs) != freeBefore+1 {
		t.Fatalf("retire with no reader left %d free segments, want %d", len(s.freeSegs), freeBefore+1)
	}
	if len(s.retSegs) != 0 {
		t.Fatalf("retire with no reader left %d segments pending, want 0", len(s.retSegs))
	}

	// A reader pins the live epoch, then a segment is retired. The reader could still hold a
	// stale address from it, so it must not free while the reader publishes an epoch at or below
	// the retire epoch.
	g := s.pin()
	s.retireSegment(prev)
	if n := s.reclaimSegments(); n != 0 {
		t.Fatalf("reclaimed %d segments while a reader held the retire epoch, want 0", n)
	}
	if len(s.retSegs) != 1 {
		t.Fatalf("held retire left %d segments pending, want 1", len(s.retSegs))
	}

	// The reader republishes to the current epoch (D20). It is still active, but its new epoch is
	// above the retire epoch, so the safe epoch passes it and the segment frees. This is the
	// property that keeps a long enumeration from starving the migrator.
	g.refresh()
	if n := s.reclaimSegments(); n != 1 {
		t.Fatalf("reclaimed %d after the reader refreshed past the retire epoch, want 1", n)
	}
	g.unpin()
}

// TestSegmentEpochNoUseAfterFree is the M2 -race gate: readers hammer a set of keys while the
// records those keys point at are migrated to the cold region, their segment retired, and the
// freed segment reused by fresh writes. The epoch scheme must keep a segment's bytes alive until
// every reader that loaded a resident address from it has finished, so no Get ever reads the
// reused bytes. A broken gate shows up two ways: a read returns a wrong value, or the race
// detector flags the reused-byte write against a reader's in-flight copy.
func TestSegmentEpochNoUseAfterFree(t *testing.T) {
	s := churnSegColdStore(t, 5)

	seg0, keysA := fillSegBig(t, s, "a")
	if seg0 != 0 {
		t.Fatalf("first fill landed in segment %d, want 0", seg0)
	}
	// Advance off seg0 so it is not the current segment when we retire it.
	fillSegBig(t, s, "b")

	want := make([]string, len(keysA))
	for i := range keysA {
		want[i] = string(churnVal("a", i))
	}

	var stop atomic.Bool
	var firstErr atomic.Pointer[string]
	var wg sync.WaitGroup
	const readers = 6
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for j := 0; !stop.Load(); j++ {
				i := (j + r) % len(keysA)
				v, ok := s.Get(keysA[i], nil)
				if !ok || string(v) != want[i] {
					msg := fmt.Sprintf("Get %q = %q,%v; want %q,true", keysA[i], v, ok, want[i])
					firstErr.CompareAndSwap(nil, &msg)
					return
				}
			}
		}(r)
	}

	// Drain seg0: migrate every one of its records to the cold region so the index no longer
	// points into it, then retire it. Under the concurrent readers the retire cannot free seg0
	// until the readers holding pre-flip resident addresses drain (the epoch gate).
	for _, k := range keysA {
		if !s.MigrateToCold(k, stringKind) {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("MigrateToCold(%q) returned false", k)
		}
	}
	s.retireSegment(seg0)

	// Fill so the reclaimed seg0 is reused while readers still run, overwriting the arena bytes
	// keysA used to live at. reclaimSegments turns seg0 free as the readers cycle and release
	// their epochs; the reuse then lands the current-segment pointer back on seg0.
	reused := false
	for iter := 0; iter < len(keysA)*40 && !reused; iter++ {
		s.reclaimSegments()
		k := []byte(fmt.Sprintf("c%07d", iter))
		if err := s.Set(k, churnVal("c", iter)); err != nil {
			// Arena momentarily full: a reclaim pass may free seg0 now that readers have moved on.
			s.reclaimSegments()
			if err2 := s.Set(k, churnVal("c", iter)); err2 != nil {
				break
			}
		}
		// Delete the filler so the index stays bounded across the long churn; the point is to
		// keep advancing the current segment through seg0, not to accumulate keys.
		s.Delete(k)
		if s.curSeg.Load() == seg0 {
			reused = true
		}
	}

	stop.Store(true)
	wg.Wait()
	if msg := firstErr.Load(); msg != nil {
		t.Fatal(*msg)
	}
	if !reused {
		t.Fatal("the freed segment was never reused, so the use-after-free window was not exercised")
	}

	// Every key still reads its exact value from the cold region after all the churn.
	for i, k := range keysA {
		if v, ok := s.Get(k, nil); !ok || string(v) != want[i] {
			t.Fatalf("post-churn Get %q = %q,%v; want %q,true", k, v, ok, want[i])
		}
	}
}
