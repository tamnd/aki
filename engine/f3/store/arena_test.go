package store

import (
	"fmt"
	"testing"
)

// fillSeg writes records under prefix-numbered keys until the current segment
// advances, returning the keys that landed in the segment that was current
// when it started. Each value is distinct so a later read proves the exact
// bytes survived.
func fillSeg(t *testing.T, s *Store, prefix string) (startSeg uint64, keys [][]byte) {
	t.Helper()
	startSeg = s.arena.cur
	for i := 0; s.arena.cur == startSeg; i++ {
		k := []byte(fmt.Sprintf("%s%06d", prefix, i))
		v := []byte(fmt.Sprintf("val-%s-%06d", prefix, i))
		if err := s.Set(k, v); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
		if s.arena.cur != startSeg {
			// This Set spilled into the next segment; drop the key so the
			// returned set is exactly the records living in startSeg.
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

// TestSegmentFillFreeReuse fills segment 0, spills into segment 1, clears
// segment 0's records and frees it, then fills more and confirms the freed
// segment is reused without corrupting the records still live in segment 1 or
// the records freshly written into the reused segment.
func TestSegmentFillFreeReuse(t *testing.T) {
	s := testStore(t, 4)

	seg0, keysA := fillSeg(t, s, "a")
	if seg0 != 0 {
		t.Fatalf("first fill landed in segment %d, want 0", seg0)
	}
	seg1, keysB := fillSeg(t, s, "b")
	if seg1 == seg0 {
		t.Fatal("second fill did not advance to a new segment")
	}

	// Every group-A record is still readable before its segment reclaims.
	for i, k := range keysA {
		mustGet(t, s, k, fmt.Sprintf("val-a-%06d", i))
	}

	// Clear segment 0's records from the index, then reclaim the segment.
	// Deleting first is freeSegment's contract: no live record may remain in
	// a freed segment. The demotion machinery will do this drain in a later
	// slice; this test does it by hand, and the live counter must agree that
	// the segment is fully dead.
	for _, k := range keysA {
		if !s.Delete(k) {
			t.Fatalf("Delete %q: missing", k)
		}
	}
	if live := s.arena.segs[seg0].live; live != 0 {
		t.Fatalf("segment 0 reports %d live bytes after every record was deleted", live)
	}
	s.arena.freeSegment(seg0)

	// Freeing segment 0 must not have touched segment 1: group B reads intact.
	for i, k := range keysB {
		mustGet(t, s, k, fmt.Sprintf("val-b-%06d", i))
	}

	// Fill again. The current segment eventually fills and the allocator pops
	// segment 0 off the free list, so group C reuses the reclaimed bytes.
	sawReuse := false
	var keysC [][]byte
	for i := 0; i < len(keysA)*6 && !sawReuse; i++ {
		k := []byte(fmt.Sprintf("c%06d", i))
		v := []byte(fmt.Sprintf("val-c-%06d", i))
		if err := s.Set(k, v); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
		keysC = append(keysC, k)
		if s.arena.cur == seg0 {
			sawReuse = true
		}
	}
	if !sawReuse {
		t.Fatal("allocator never reused the freed segment")
	}

	// Both the reused-segment writes and the still-live segment-1 writes read
	// correctly.
	for i, k := range keysC {
		mustGet(t, s, k, fmt.Sprintf("val-c-%06d", i))
	}
	for i, k := range keysB {
		mustGet(t, s, k, fmt.Sprintf("val-b-%06d", i))
	}
}

// TestRecordNeverSpansSegment writes records of varied sizes across several
// segments and checks the invariant every reclamation path depends on: a
// record's bytes, header through reserved capacity, sit inside exactly one
// segment, so freeing a segment can never cut a live record in half.
func TestRecordNeverSpansSegment(t *testing.T) {
	s := testStore(t, 4)
	var keys [][]byte
	for i := 0; s.arena.highWater < 3; i++ {
		k := []byte(fmt.Sprintf("span-%06d", i))
		// Vary the value size so record ends land at irregular offsets and a
		// span bug cannot hide behind a lucky uniform stride.
		v := make([]byte, (i*137)%4096+1)
		if err := s.Set(k, v); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		t.Fatal("wrote nothing")
	}
	for _, k := range keys {
		_, addr, _ := s.findEntry(Hash(k), k)
		if addr == 0 {
			t.Fatalf("%q missing", k)
		}
		n := s.recBytes(addr)
		first, ok1 := s.arena.segOf(addr)
		last, ok2 := s.arena.segOf(addr + n - 1)
		if !ok1 || !ok2 {
			t.Fatalf("record %q at %d(+%d) is outside the segment tiling", k, addr, n)
		}
		if first != last {
			t.Fatalf("record %q spans segments %d and %d", k, first, last)
		}
		seg := &s.arena.segs[first]
		if addr < seg.base || addr+n > seg.base+s.arena.segSize {
			t.Fatalf("record %q [%d,%d) outside segment %d [%d,%d)", k, addr, addr+n, first, seg.base, seg.base+s.arena.segSize)
		}
	}
}

// TestFreeCurrentSegmentRefused pins freeSegment's guard: rewinding the
// segment the bump cursor is filling would hand out offsets twice.
func TestFreeCurrentSegmentRefused(t *testing.T) {
	s := testStore(t, 2)
	if err := s.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	cur := s.arena.cur
	before := s.arena.segs[cur].alloc
	s.arena.freeSegment(cur)
	if s.arena.segs[cur].alloc != before {
		t.Fatal("freeSegment rewound the current segment")
	}
	if len(s.arena.freeSegs) != 0 {
		t.Fatal("freeSegment put the current segment on the free list")
	}
	mustGet(t, s, []byte("k"), "v")
}
