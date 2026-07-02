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

// TestListRingScanSig checks that the signature scan visits exactly the live positions whose element
// signature equals the target, in ascending order forward and descending order backward, over a live
// range that wraps the ring end and dips below zero (LPUSH growth). The reference is a plain loop over
// the same range computing listSig per element, so the test pins the position math the wrapped
// bytes.IndexByte walk does, independent of any full compare the caller layers on top.
func TestListRingScanSig(t *testing.T) {
	const capp = 32
	r := newListRing(capp)
	// A live span that wraps: start below zero, run past the ring end, stay under cap.
	const head, tail = int64(-5), int64(20) // span 25 < cap 32
	for p := head; p < tail; p++ {
		// Vary the bytes so signatures spread, with deliberate repeats so several positions collide.
		r.put(p, []byte(fmt.Sprintf("e%d", p%7)))
	}
	for want := 0; want < 256; want++ {
		wb := byte(want)
		var refFwd []int64
		for p := head; p < tail; p++ {
			if listSig(r.get(p)) == wb {
				refFwd = append(refFwd, p)
			}
		}
		var gotFwd []int64
		r.scanSigForward(head, tail, wb, func(pos int64) bool { gotFwd = append(gotFwd, pos); return false })
		if !eqI64(gotFwd, refFwd) {
			t.Fatalf("sig %d forward: got %v want %v", want, gotFwd, refFwd)
		}
		refBwd := make([]int64, len(refFwd))
		for i, p := range refFwd {
			refBwd[len(refFwd)-1-i] = p
		}
		var gotBwd []int64
		r.scanSigBackward(head, tail, wb, func(pos int64) bool { gotBwd = append(gotBwd, pos); return false })
		if !eqI64(gotBwd, refBwd) {
			t.Fatalf("sig %d backward: got %v want %v", want, gotBwd, refBwd)
		}
	}
}

// TestListRingScanSigEarlyStop confirms visit returning true halts the walk at the first match, the
// single-match LPOS path, in both directions.
func TestListRingScanSigEarlyStop(t *testing.T) {
	const capp = 16
	r := newListRing(capp)
	const head, tail = int64(0), int64(12)
	for p := head; p < tail; p++ {
		r.put(p, []byte("same"))
	}
	want := listSig([]byte("same"))
	var fwd []int64
	r.scanSigForward(head, tail, want, func(pos int64) bool { fwd = append(fwd, pos); return true })
	if len(fwd) != 1 || fwd[0] != head {
		t.Fatalf("forward early stop: got %v want [%d]", fwd, head)
	}
	var bwd []int64
	r.scanSigBackward(head, tail, want, func(pos int64) bool { bwd = append(bwd, pos); return true })
	if len(bwd) != 1 || bwd[0] != tail-1 {
		t.Fatalf("backward early stop: got %v want [%d]", bwd, tail-1)
	}
}

// TestListRingMoveDetaches confirms move relocates the element and leaves the source slot nil, so
// dst and src never end up aliasing one backing array. A put at src afterward must allocate fresh
// and must not disturb the element now at dst.
func TestListRingMoveDetaches(t *testing.T) {
	r := newListRing(8)
	r.put(2, []byte("keep"))
	r.move(3, 2)
	if got := r.get(3); string(got) != "keep" {
		t.Fatalf("move dst: got %q want keep", got)
	}
	if r.slots[2&r.mask] != nil {
		t.Fatalf("move left source non-nil, slots share a backing array")
	}
	if r.sig[3&r.mask] != listSig([]byte("keep")) {
		t.Fatalf("move did not carry the signature")
	}
	// A put reusing the source slot allocates fresh and must not touch the moved element.
	r.put(2, []byte("new"))
	if got := r.get(3); string(got) != "keep" {
		t.Fatalf("put at source corrupted dst: got %q want keep", got)
	}
	if got := r.get(2); string(got) != "new" {
		t.Fatalf("put at source: got %q want new", got)
	}
}

// TestListRingShiftDownFillTop shifts the left run down by one and puts a new element into the top
// slot the shift frees, the exact sequence a BEFORE-pivot LINSERT runs. Every survivor must read
// back its own bytes and the new element must not alias any of them, the aliasing trap that a
// pointer move without a detach would spring when put reuses the freed slot's backing.
func TestListRingShiftDownFillTop(t *testing.T) {
	r := newListRing(16)
	const head, tail = int64(0), int64(6)
	for p := head; p < tail; p++ {
		r.put(p, []byte(fmt.Sprintf("v%d", p)))
	}
	// Insert before index 3: shift [head, head+3) down, fill the freed slot at head+2.
	const i = int64(3)
	r.shiftDown(head, head+i)
	r.put(head+i-1, []byte("NEW"))
	want := []string{"v0", "v1", "v2", "NEW", "v3", "v4", "v5"}
	for j, w := range want {
		p := head - 1 + int64(j)
		if got := r.get(p); string(got) != w {
			t.Fatalf("pos %d: got %q want %q", p, got, w)
		}
	}
}

// TestListRingShiftUpFillBottom mirrors the above for an AFTER-pivot LINSERT that shifts the right
// run up by one and fills the freed bottom slot.
func TestListRingShiftUpFillBottom(t *testing.T) {
	r := newListRing(16)
	const head, tail = int64(0), int64(6)
	for p := head; p < tail; p++ {
		r.put(p, []byte(fmt.Sprintf("v%d", p)))
	}
	// Insert after index 2 (before index 3): shift [head+3, tail) up, fill the freed slot at head+3.
	const i = int64(3)
	r.shiftUp(head+i, tail)
	r.put(head+i, []byte("NEW"))
	want := []string{"v0", "v1", "v2", "NEW", "v3", "v4", "v5"}
	for j, w := range want {
		p := head + int64(j)
		if got := r.get(p); string(got) != w {
			t.Fatalf("pos %d: got %q want %q", p, got, w)
		}
	}
}

// TestListRingCompact runs the LREM compaction pattern: drop a set of positions and slide survivors
// down with a write cursor, moving only across a gap. The survivors must end contiguous from head in
// their original order, and a put reusing any vacated tail slot must not alias a live survivor.
func TestListRingCompact(t *testing.T) {
	r := newListRing(16)
	const head, tail = int64(0), int64(8)
	for p := head; p < tail; p++ {
		r.put(p, []byte(fmt.Sprintf("v%d", p)))
	}
	drop := map[int64]bool{1: true, 2: true, 5: true}
	wpos := head
	for p := head; p < tail; p++ {
		if drop[p] {
			continue
		}
		if wpos != p {
			r.move(wpos, p)
		}
		wpos++
	}
	want := []string{"v0", "v3", "v4", "v6", "v7"}
	for j, w := range want {
		p := head + int64(j)
		if got := r.get(p); string(got) != w {
			t.Fatalf("survivor pos %d: got %q want %q", p, got, w)
		}
	}
	// A push reusing a now-dead tail slot must not disturb any survivor.
	r.put(wpos, []byte("PUSH"))
	for j, w := range want {
		p := head + int64(j)
		if got := r.get(p); string(got) != w {
			t.Fatalf("after tail push, survivor pos %d: got %q want %q", p, got, w)
		}
	}
}

func eqI64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
