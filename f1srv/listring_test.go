package f1srv

import (
	"bytes"
	"fmt"
	"testing"
)

// TestListRingRoundtrip stores and reads back bytes at positive and negative positions, since a list
// grows its head below zero, and confirms the mask index never lands out of range for either sign.
func TestListRingRoundtrip(t *testing.T) {
	r := newListRing(16)
	for _, pos := range []int64{0, 1, 15, 16, -1, -16, -17, 1 << 40, -(1 << 40)} {
		want := []byte(fmt.Sprintf("elem-%d", pos))
		r.put(pos, want)
		if got := r.get(pos); !bytes.Equal(got, want) {
			t.Fatalf("pos %d: got %q want %q", pos, got, want)
		}
	}
}

// TestListRingSlotAliasing checks that positions cap apart share a slot, the collision the resident
// cap invariant is responsible for keeping from ever holding two live positions at once. The test
// documents the invariant rather than defends against its violation: a later put wins the slot.
func TestListRingSlotAliasing(t *testing.T) {
	r := newListRing(8)
	r.put(3, []byte("first"))
	r.put(3+8, []byte("second"))
	if got := r.get(3); !bytes.Equal(got, []byte("second")) {
		t.Fatalf("colliding slot: got %q want %q", got, "second")
	}
}

// TestListRingReuseNoRegrow puts a longer then shorter value at one position and confirms the slot
// reuses its backing array (no reallocation on the shrink) while reporting the right length.
func TestListRingReuseNoRegrow(t *testing.T) {
	r := newListRing(4)
	r.put(0, []byte("a-longer-value"))
	first := &r.slots[0][0]
	r.put(0, []byte("short"))
	if got := r.get(0); string(got) != "short" {
		t.Fatalf("reuse: got %q want short", got)
	}
	if second := &r.slots[0][0]; first != second {
		t.Fatalf("expected slot backing array reuse, array moved")
	}
}

// TestListRingReset clears a slot's content but keeps it usable for the next push.
func TestListRingReset(t *testing.T) {
	r := newListRing(4)
	r.put(2, []byte("x"))
	r.reset(2)
	if got := r.get(2); len(got) != 0 {
		t.Fatalf("after reset len=%d want 0", len(got))
	}
	r.put(2, []byte("y"))
	if got := r.get(2); string(got) != "y" {
		t.Fatalf("reuse after reset: got %q want y", got)
	}
}

// TestListRingQueueSpan simulates the queue access pattern the model targets: push at the tail, pop
// from the head, with a live span held below cap the whole time. Every get during the walk must see
// the exact bytes pushed for that position, which is the collision-free guarantee under the intended
// use. cap is deliberately smaller than the total positions touched, so the slots wrap many times.
func TestListRingQueueSpan(t *testing.T) {
	const capp = 64
	r := newListRing(capp)
	const span = 40 // committedTail - committedHead, always < cap
	var head, tail int64
	// Prime the resident span.
	for tail-head < span {
		r.put(tail, []byte(fmt.Sprintf("v%d", tail)))
		tail++
	}
	// Walk far past cap: pop head, push tail, keep the span fixed, verify the head each step.
	for step := 0; step < 10*capp; step++ {
		want := []byte(fmt.Sprintf("v%d", head))
		if got := r.get(head); !bytes.Equal(got, want) {
			t.Fatalf("step %d head %d: got %q want %q", step, head, got, want)
		}
		r.reset(head)
		head++
		r.put(tail, []byte(fmt.Sprintf("v%d", tail)))
		tail++
	}
}

// TestListRingGrowPreservesLive fills a ring to just under capacity, grows it, and confirms every live
// position still reads back its exact bytes at the new mask. Positions include a negative head so the
// head-below-zero case (LPUSH growth) is covered across the doubling.
func TestListRingGrowPreservesLive(t *testing.T) {
	r := newListRing(8)
	const head, tail = int64(-3), int64(4) // span 7 < cap 8
	for p := head; p < tail; p++ {
		r.put(p, []byte(fmt.Sprintf("v%d", p)))
	}
	r.grow(head, tail)
	if r.capacity() != 16 {
		t.Fatalf("after grow cap=%d want 16", r.capacity())
	}
	for p := head; p < tail; p++ {
		want := []byte(fmt.Sprintf("v%d", p))
		if got := r.get(p); !bytes.Equal(got, want) {
			t.Fatalf("after grow pos %d: got %q want %q", p, got, want)
		}
	}
}

// TestListRingGrowMovesBackingByReference proves the rehash relocates the slice header without copying
// the element bytes: the backing array a live position points at before the grow is the same array it
// points at after. This is what keeps growth O(span) pointer moves, not O(bytes).
func TestListRingGrowMovesBackingByReference(t *testing.T) {
	r := newListRing(4)
	const head, tail = int64(0), int64(3)
	for p := head; p < tail; p++ {
		r.put(p, []byte(fmt.Sprintf("value-%d", p)))
	}
	before := &r.get(1)[0]
	r.grow(head, tail)
	if after := &r.get(1)[0]; before != after {
		t.Fatalf("grow copied element bytes, backing array moved")
	}
}

// TestListRingGrowThenQueueSpan grows mid-walk and keeps running the queue pattern, so a grow that
// lands in the middle of steady push/pop churn does not lose the head element. The span stays under
// the post-grow cap, so the collision-free guarantee holds through and past the doubling.
func TestListRingGrowThenQueueSpan(t *testing.T) {
	r := newListRing(8)
	var head, tail int64
	const span = 6 // < initial cap 8
	for tail-head < span {
		r.put(tail, []byte(fmt.Sprintf("v%d", tail)))
		tail++
	}
	// Grow while live, then keep walking the queue far past the new cap.
	r.grow(head, tail)
	for step := 0; step < 10*16; step++ {
		want := []byte(fmt.Sprintf("v%d", head))
		if got := r.get(head); !bytes.Equal(got, want) {
			t.Fatalf("step %d head %d: got %q want %q", step, head, got, want)
		}
		r.reset(head)
		head++
		r.put(tail, []byte(fmt.Sprintf("v%d", tail)))
		tail++
	}
}

func TestListRingBadCapacity(t *testing.T) {
	for _, bad := range []int64{0, -8, 3, 6, 100} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("cap %d: expected panic", bad)
				}
			}()
			newListRing(bad)
		}()
	}
}
