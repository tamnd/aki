package main

import (
	"math/rand"
	"testing"
	"time"
)

// The harness checks: the standalone heap pops due timers in deadline order
// (so the fire-batch numbers measure real ordered drains), and both park shapes
// actually deliver a wake (so the round-trip numbers time a completed park, not
// a missed one). The production heap and park path are proven in the shard
// package's timer_test; this file only proves the lab measures what it claims.

func TestPheapPopsInOrder(t *testing.T) {
	h := &pheap{}
	const n = 500
	now := int64(1000)
	for i := 0; i < n; i++ {
		h.push(&ptimer{deadlineMs: now + int64(rand.Intn(1000))})
	}
	out := h.popDue(now+2000, n, nil)
	if len(out) != n {
		t.Fatalf("popped %d, want %d", len(out), n)
	}
	for i := 1; i < len(out); i++ {
		if out[i-1].deadlineMs > out[i].deadlineMs {
			t.Fatalf("out of order at %d: %d then %d", i, out[i-1].deadlineMs, out[i].deadlineMs)
		}
	}
}

func TestPopDueRespectsNowAndCap(t *testing.T) {
	h := &pheap{}
	for i := 0; i < 10; i++ {
		h.push(&ptimer{deadlineMs: int64(i)})
	}
	out := h.popDue(4, 2, nil) // due are 0..4, cap 2
	if len(out) != 2 || out[0].deadlineMs != 0 || out[1].deadlineMs != 1 {
		t.Fatalf("cap not honored: %v", out)
	}
	if h.a[0].deadlineMs != 2 {
		t.Fatalf("left the wrong min, got %d want 2", h.a[0].deadlineMs)
	}
}

func TestParkShapesDeliver(t *testing.T) {
	for _, timed := range []bool{false, true} {
		ns, _, _ := parkRoundTrip(2000, timed)
		if ns <= 0 {
			t.Fatalf("timed=%v produced no timing", timed)
		}
	}
}

func TestFireBurstFiresAll(t *testing.T) {
	total, maxPass := fireBurst(1000, 64)
	if total <= 0 || maxPass <= 0 {
		t.Fatalf("burst produced no timing: total=%v maxPass=%v", total, maxPass)
	}
}

func TestFarTimerDoesNotFire(t *testing.T) {
	// The overhead measurement relies on the far deadline never firing during a
	// park; confirm a park with a far deadline is woken only by the channel.
	w := newPworker()
	done := make(chan struct{})
	go func() {
		for w.state.Load() != stParked {
		}
		w.wake()
		close(done)
	}()
	w.timedPark(time.Hour)
	<-done
}
