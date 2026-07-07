package f1raw

import (
	"fmt"
	"sync"
	"testing"
)

// segStore builds a segmented store sized to hold a handful of small segments, so a
// test can fill one segment, spill into the next, and reclaim the first. segBytes is
// floored at maxRecordBytes inside NewSegmented, so the segment size the store actually
// uses is that floor; the record count per segment follows from it, not from segBytes.
func segStore(t *testing.T, nSegWanted int) *Store {
	t.Helper()
	segSize := align8(maxRecordBytes)
	ov := uint64(4 * bucketSize)
	arena := int(8 + ov + uint64(nSegWanted)*segSize + segSize) // headroom for one spare segment
	s := NewSegmented(1<<12, arena, int(segSize), int(ov))
	if !s.segmented {
		t.Fatal("NewSegmented did not enable the segmented arena")
	}
	if len(s.segs) < nSegWanted {
		t.Fatalf("arena holds %d segments, wanted at least %d", len(s.segs), nSegWanted)
	}
	return s
}

// fillSeg writes records under keys prefix+"0".. until the current segment advances,
// returning the keys it wrote (all landed in the segment index that was current when it
// started). Each value is distinct so a later read proves the exact bytes survived.
func fillSeg(t *testing.T, s *Store, prefix string) (startSeg uint64, keys [][]byte) {
	t.Helper()
	startSeg = s.curSeg.Load()
	for i := 0; s.curSeg.Load() == startSeg; i++ {
		k := []byte(fmt.Sprintf("%s%06d", prefix, i))
		v := []byte(fmt.Sprintf("val-%s-%06d", prefix, i))
		if err := s.Set(k, v); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
		if s.curSeg.Load() != startSeg {
			// This Set spilled into the next segment; drop the key so the returned set
			// is exactly the records that live in startSeg.
			s.Delete(k)
			break
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		t.Fatal("filled no keys before the segment advanced")
	}
	return startSeg, keys
}

func mustGet(t *testing.T, s *Store, key []byte, want string) {
	t.Helper()
	got, ok := s.Get(key, nil)
	if !ok {
		t.Fatalf("Get %q: missing", key)
	}
	if string(got) != want {
		t.Fatalf("Get %q = %q, want %q", key, got, want)
	}
}

// TestSegmentFillFreeReuse is the M0 gate: fill segment 0, spill into segment 1, clear
// segment 0's records and free it, then fill more and confirm the freed segment is
// reused without corrupting the records still live in segment 1 or the records freshly
// written into the reused segment.
func TestSegmentFillFreeReuse(t *testing.T) {
	s := segStore(t, 3)

	seg0, keysA := fillSeg(t, s, "a")
	if seg0 != 0 {
		t.Fatalf("first fill landed in segment %d, want 0", seg0)
	}
	seg1, keysB := fillSeg(t, s, "b")
	if seg1 == seg0 {
		t.Fatal("second fill did not advance to a new segment")
	}

	// Every group-A record is still readable before we reclaim its segment.
	for i, k := range keysA {
		mustGet(t, s, k, fmt.Sprintf("val-a-%06d", i))
	}

	// Clear segment 0's records from the index, then reclaim the segment. Deleting first
	// is the invariant freeSegment's contract requires: no live record may remain in a
	// freed segment. The migrator will do this drain in a later milestone; M0 does it by
	// hand.
	for _, k := range keysA {
		if !s.Delete(k) {
			t.Fatalf("Delete %q: missing", k)
		}
	}
	s.freeSegment(seg0)

	// Freeing segment 0 must not have touched segment 1: group B reads intact.
	for i, k := range keysB {
		mustGet(t, s, k, fmt.Sprintf("val-b-%06d", i))
	}

	// Fill again. The current segment eventually fills and the allocator pops segment 0
	// off the free list, so group C reuses the reclaimed bytes.
	sawReuse := false
	var keysC [][]byte
	for i := 0; i < len(keysA)*4 && !sawReuse; i++ {
		k := []byte(fmt.Sprintf("c%06d", i))
		v := []byte(fmt.Sprintf("val-c-%06d", i))
		if err := s.Set(k, v); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
		keysC = append(keysC, k)
		if s.curSeg.Load() == seg0 {
			sawReuse = true
		}
	}
	if !sawReuse {
		t.Fatal("allocator never reused the freed segment")
	}

	// Both the reused-segment writes and the still-live segment-1 writes read correctly.
	for i, k := range keysC {
		mustGet(t, s, k, fmt.Sprintf("val-c-%06d", i))
	}
	for i, k := range keysB {
		mustGet(t, s, k, fmt.Sprintf("val-b-%06d", i))
	}
}

// TestSegmentedConcurrentFill drives allocRecord and advanceSeg under the race detector:
// many goroutines write disjoint keys into a segmented store at once, then every key
// must read back with its exact value. It exercises the concurrent bump-and-advance the
// single-threaded test does not.
func TestSegmentedConcurrentFill(t *testing.T) {
	s := segStore(t, 8)

	const workers = 8
	const perWorker = 200
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				k := []byte(fmt.Sprintf("w%02d-%06d", w, i))
				v := []byte(fmt.Sprintf("v%02d-%06d", w, i))
				if err := s.Set(k, v); err != nil {
					errs[w] = fmt.Errorf("Set %q: %w", k, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for w := 0; w < workers; w++ {
		for i := 0; i < perWorker; i++ {
			mustGet(t, s, []byte(fmt.Sprintf("w%02d-%06d", w, i)), fmt.Sprintf("v%02d-%06d", w, i))
		}
	}
}

// TestSegmentedResetRewinds confirms Reset returns a segmented store to empty and reuses
// segment 0 as the current segment, so a flushed store serves fresh writes from the
// front of the arena again.
func TestSegmentedResetRewinds(t *testing.T) {
	s := segStore(t, 3)
	fillSeg(t, s, "a")
	fillSeg(t, s, "b")

	s.Reset()
	if s.curSeg.Load() != 0 {
		t.Fatalf("Reset left current segment at %d, want 0", s.curSeg.Load())
	}
	if s.Len() != 0 {
		t.Fatalf("Reset left %d records, want 0", s.Len())
	}
	used, _ := s.ArenaBytes()
	if used != 0 {
		t.Fatalf("Reset left %d used bytes, want 0", used)
	}
	if err := s.Set([]byte("post"), []byte("reset")); err != nil {
		t.Fatalf("Set after Reset: %v", err)
	}
	mustGet(t, s, []byte("post"), "reset")
}
