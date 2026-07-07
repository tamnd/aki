package f1raw

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestDrainSegmentSinksLiveRecords is the M3 slice-2 gate for the migrator drain loop (doc 21
// section 6). It fills one segment, advances off it so it is full and no longer the allocation
// target, then makes the segment a realistic drain target by leaving some of its records live,
// deleting some, and overwriting one to a fresh record in another segment so its copy here is dead
// space. drainSegment must sink every live record to the cold region, step over the dead ones, land
// the segment's live counter on exactly zero, and retire the now-empty segment (freed at once with
// no reader active). Afterward the survivors read their exact value from cold, the deleted keys are
// gone, and the overwritten key reads its new value.
func TestDrainSegmentSinksLiveRecords(t *testing.T) {
	s := churnSegColdStore(t, 5)

	seg0, keysA := fillSegBig(t, s, "a")
	// Advance past seg0 so it is a full, non-current segment, the only kind drainSegment drains.
	fillSegBig(t, s, "b")
	if seg0 == s.curSeg.Load() {
		t.Fatal("seg0 is still the current segment; the fill did not advance off it")
	}

	// Delete every fourth record: these become dead space the drain must step over, not migrate.
	deleted := make(map[string]bool)
	for i, k := range keysA {
		if i%4 == 0 {
			if !s.Delete(k) {
				t.Fatalf("Delete %q returned false", k)
			}
			deleted[string(k)] = true
		}
	}

	// Overwrite one surviving record with a value too large to fit its reserved capacity, so the
	// write publishes a fresh resident record in the current segment and the copy in seg0 goes dead.
	// The drain must skip that stale copy (its key resolves elsewhere) yet still empty seg0.
	var overwritten []byte
	bigVal := make([]byte, churnValLen*2)
	for i := range bigVal {
		bigVal[i] = byte('Z')
	}
	for i, k := range keysA {
		if !deleted[string(k)] && i%4 == 1 {
			if err := s.Set(k, bigVal); err != nil {
				t.Fatalf("overwrite Set %q: %v", k, err)
			}
			overwritten = k
			break
		}
	}
	if overwritten == nil {
		t.Fatal("no key was available to overwrite")
	}

	s.drainSegment(seg0)

	if got := s.segs[seg0].live.Load(); got != 0 {
		t.Fatalf("after draining, seg %d live = %d, want 0", seg0, got)
	}
	// No reader is active, so retiring the drained segment freed it in the same pass.
	freed := false
	for _, si := range s.freeSegs {
		if si == seg0 {
			freed = true
			break
		}
	}
	if !freed {
		t.Fatalf("drained segment %d did not reach the free list; freeSegs=%v", seg0, s.freeSegs)
	}

	for i, k := range keysA {
		v, ok := s.Get(k, nil)
		switch {
		case deleted[string(k)]:
			if ok {
				t.Fatalf("deleted key %q still present as %q", k, v)
			}
		case string(k) == string(overwritten):
			if !ok || string(v) != string(bigVal) {
				t.Fatalf("overwritten key %q = %q,%v; want its new big value", k, v, ok)
			}
		default:
			if !ok || string(v) != string(churnVal("a", i)) {
				t.Fatalf("survived key %q = %q,%v; want its cold value", k, v, ok)
			}
			// A survivor now lives cold: its index entry carries the tier bit.
			off, _, _, _, found := s.find(k, hash(k), stringKind)
			if !found || off&tierBit == 0 {
				t.Fatalf("survived key %q not tier-tagged cold after drain (off=%#x found=%v)", k, off, found)
			}
		}
	}
}

// TestDrainSegmentFreesForReuse checks the whole point of the drain: a segment drained to empty
// returns to the free list and a later fill reuses it, so a bounded arena keeps serving writes by
// recycling drained segments. It fills two segments, drains the first, and confirms the reclaimed
// segment index is handed back out by subsequent allocation.
func TestDrainSegmentFreesForReuse(t *testing.T) {
	s := churnSegColdStore(t, 3)

	seg0, keysA := fillSegBig(t, s, "a")
	fillSegBig(t, s, "b")

	s.drainSegment(seg0)
	if got := s.segs[seg0].live.Load(); got != 0 {
		t.Fatalf("drained seg %d live = %d, want 0", seg0, got)
	}

	// Keep writing until the allocator hands seg0 back out as the current segment, which proves the
	// drained segment was reclaimed and reused rather than the arena simply having spare segments.
	reused := false
	for i := 0; i < len(keysA)*40 && !reused; i++ {
		k := []byte(fmt.Sprintf("c%07d", i))
		if err := s.Set(k, churnVal("c", i)); err != nil {
			s.reclaimSegments()
			if err2 := s.Set(k, churnVal("c", i)); err2 != nil {
				break
			}
		}
		s.Delete(k) // keep the index bounded; the point is to advance the current segment, not to fill
		if s.curSeg.Load() == seg0 {
			reused = true
		}
	}
	if !reused {
		t.Fatalf("drained segment %d was never reused as the current segment", seg0)
	}

	// The migrated records still read their exact value from cold after the reuse churn.
	for i, k := range keysA {
		if v, ok := s.Get(k, nil); !ok || string(v) != string(churnVal("a", i)) {
			t.Fatalf("post-reuse Get %q = %q,%v; want its cold value", k, v, ok)
		}
	}
}

// TestDrainSegmentNoUseAfterFree is the M3 slice-2 -race gate: readers hammer a segment's keys while
// drainSegment sinks every one of that segment's records to cold and retires the segment, and the
// freed segment is then reused by fresh writes. The epoch gate (M2) must hold the segment's bytes
// alive until every reader that loaded a pre-flip resident address has finished, so no Get ever reads
// the reused bytes and the race detector never flags the reuse write against a reader's in-flight
// copy. It is drainSegment driving the same use-after-free window the M2 test drove by hand.
func TestDrainSegmentNoUseAfterFree(t *testing.T) {
	s := churnSegColdStore(t, 5)

	seg0, keysA := fillSegBig(t, s, "a")
	if seg0 != 0 {
		t.Fatalf("first fill landed in segment %d, want 0", seg0)
	}
	fillSegBig(t, s, "b") // advance off seg0 so the drain targets a full, non-current segment

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

	// Drain seg0 under the concurrent readers: every record moves cold and the segment retires, but
	// it cannot free until the readers holding pre-flip resident addresses drain (the epoch gate).
	s.drainSegment(seg0)

	// Reuse the reclaimed seg0 while readers still run, overwriting the arena bytes keysA lived at.
	reused := false
	for iter := 0; iter < len(keysA)*40 && !reused; iter++ {
		s.reclaimSegments()
		k := []byte(fmt.Sprintf("c%07d", iter))
		setReusingFreedSeg(t, s, k, churnVal("c", iter))
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

	for i, k := range keysA {
		if v, ok := s.Get(k, nil); !ok || string(v) != want[i] {
			t.Fatalf("post-churn Get %q = %q,%v; want %q,true", k, v, ok, want[i])
		}
	}
}

// setReusingFreedSeg writes one filler record during the reuse-under-readers churn, treating a
// momentary full arena as backpressure rather than a failure. The full error means the drained
// segment is retired but not yet safe to reclaim, because an in-flight reader still holds its
// epoch; yield so those readers advance and release it, reclaim, and retry. The reclaimed segment
// then frees and the next allocation lands on it. An earlier form broke out of the churn on the
// second full error, which flaked on slower boxes where the readers held their epoch across the
// whole fill so the segment never freed inside the two attempts. It fails only if the arena stays
// full across a long yield budget, which flags a genuine reclamation stall (the epoch gate never
// releasing the segment) rather than scheduling timing.
func setReusingFreedSeg(t *testing.T, s *Store, k, v []byte) {
	t.Helper()
	for attempt := 0; attempt < 200000; attempt++ {
		err := s.Set(k, v)
		if err == nil {
			return
		}
		if err != ErrFull {
			t.Fatalf("reuse Set %q: %v", k, err)
		}
		runtime.Gosched()
		s.reclaimSegments()
	}
	t.Fatalf("arena stayed full reusing a freed segment for %q; reclamation stalled", k)
}
