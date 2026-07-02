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
