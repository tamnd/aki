package main

import (
	"testing"
)

// TestFIFOOrder drives interleaved park and wake against the intrusive list and
// checks the served order is strict enqueue order (longest waiting first),
// including after a middle waiter times out: the timed-out one is skipped and
// the rest keep their order.
func TestFIFOOrder(t *testing.T) {
	ar := newArena(16)
	l := newIlist(ar)

	// park 1..6, then wake two: must serve 1 then 2.
	idx := make(map[uint64]uint32)
	for c := uint64(1); c <= 6; c++ {
		idx[c] = l.park(c)
	}
	for _, want := range []uint64{1, 2} {
		got, ok := l.wake()
		if !ok || got != want {
			t.Fatalf("wake got %d ok=%v, want %d", got, ok, want)
		}
	}

	// waiter 4 times out and is unlinked from the middle.
	l.unlinkNode(idx[4])

	// park two more, 7 and 8, at the tail while 3,5,6 remain in the middle.
	idx[7] = l.park(7)
	idx[8] = l.park(8)

	// the remaining serve order must be 3,5,6,7,8: 4 skipped, order otherwise
	// preserved, newcomers at the back.
	want := []uint64{3, 5, 6, 7, 8}
	for _, w := range want {
		got, ok := l.wake()
		if !ok || got != w {
			t.Fatalf("wake got %d ok=%v, want %d", got, ok, w)
		}
	}
	if _, ok := l.wake(); ok {
		t.Fatalf("list should be drained")
	}
	if l.n != 0 {
		t.Fatalf("live count %d, want 0", l.n)
	}

	// unlinking an already-woken node is a no-op and must not corrupt counts.
	l.unlinkNode(idx[3])
	if l.n != 0 {
		t.Fatalf("live count %d after redundant unlink, want 0", l.n)
	}
}

// TestMultiElementServe checks that one push carrying k elements wakes exactly
// the k oldest waiters in FIFO order and parks nothing, and that a push with
// more elements than waiters serves all of them and reports the shortfall so
// the caller pushes the leftover as plain values.
func TestMultiElementServe(t *testing.T) {
	ar := newArena(16)
	l := newIlist(ar)
	for c := uint64(1); c <= 5; c++ {
		l.park(c)
	}

	// RPUSH with 3 elements against 5 waiters: serve 1,2,3, leave 4,5 parked.
	out := make([]uint64, 3)
	served := l.serveUpTo(3, out)
	if served != 3 {
		t.Fatalf("served %d, want 3", served)
	}
	for i, want := range []uint64{1, 2, 3} {
		if out[i] != want {
			t.Fatalf("served[%d]=%d, want %d", i, out[i], want)
		}
	}
	if l.n != 2 {
		t.Fatalf("live count %d, want 2 still parked", l.n)
	}

	// a push carrying 4 elements against the 2 that remain serves both and
	// reports 2, so the caller knows to push the other 2 elements as values.
	out2 := make([]uint64, 4)
	served2 := l.serveUpTo(4, out2)
	if served2 != 2 {
		t.Fatalf("served %d, want 2", served2)
	}
	for i, want := range []uint64{4, 5} {
		if out2[i] != want {
			t.Fatalf("served[%d]=%d, want %d", i, out2[i], want)
		}
	}
	if l.n != 0 {
		t.Fatalf("live count %d, want 0", l.n)
	}
}

// TestMultiKeySiblingUnlink checks that a client blocked on m keys registers one
// node per key, the first wake on any key unlinks all m siblings, and none of
// the siblings can be woken again on any other key.
func TestMultiKeySiblingUnlink(t *testing.T) {
	const m = 4
	ar := newArena(64)
	s := newMkset(ar, m)

	keys := []int{0, 1, 2, 3}
	// the multi-key client (conn 1) parks first, so it heads every key.
	s.parkMulti(1, keys)
	// a second single-key waiter parks behind it on each key.
	for k := 0; k < m; k++ {
		s.lists[k].park(uint64(100 + k))
	}

	// every key list holds 2 waiters now.
	for k := 0; k < m; k++ {
		if s.lists[k].n != 2 {
			t.Fatalf("key %d has %d waiters, want 2", k, s.lists[k].n)
		}
	}

	// wake key 2: it must serve conn 1 and unlink all m siblings.
	conn, unl, ok := s.wakeKey(2)
	if !ok || conn != 1 {
		t.Fatalf("wakeKey served %d ok=%v, want conn 1", conn, ok)
	}
	if unl != m {
		t.Fatalf("unlinked %d, want %d siblings", unl, m)
	}

	// conn 1 must be gone from every key, and the single-key waiter is now the
	// head, so a wake on any key serves the background conn, never conn 1 again.
	for k := 0; k < m; k++ {
		if s.lists[k].n != 1 {
			t.Fatalf("key %d has %d waiters after sibling unlink, want 1", k, s.lists[k].n)
		}
		got, ok := s.lists[k].wake()
		if !ok || got != uint64(100+k) {
			t.Fatalf("key %d woke %d ok=%v, want %d", k, got, ok, 100+k)
		}
	}
}
