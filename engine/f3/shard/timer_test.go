package shard

import (
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"
)

// The per-worker timer heap (timer.go) and its owner-loop wiring (worker.go
// fireTimers and the idle timed park). No registered handler arms a timer in
// production yet, so these tests drive the mechanism through a shard-package
// test hook, the same way block_test drives the deferred-reply seam. The heap
// unit tests check the structure against a naive reference model, and the driven
// tests check that a timer fires on the owner from a parked worker and that a
// producer wake during a timed park still delivers.

// checkHeap walks the heap array and asserts the two invariants every operation
// must preserve: the min-heap order and the heapPos back-pointer at every slot.
func checkHeap(t *testing.T, h *timerHeap) {
	t.Helper()
	for i := range h.a {
		if h.a[i].heapPos != i {
			t.Fatalf("heapPos drift at %d: stored %d", i, h.a[i].heapPos)
		}
		l, r := 2*i+1, 2*i+2
		if l < len(h.a) && h.a[l].deadlineMs < h.a[i].deadlineMs {
			t.Fatalf("heap order broken at %d/%d: %d < %d", i, l, h.a[l].deadlineMs, h.a[i].deadlineMs)
		}
		if r < len(h.a) && h.a[r].deadlineMs < h.a[i].deadlineMs {
			t.Fatalf("heap order broken at %d/%d: %d < %d", i, r, h.a[r].deadlineMs, h.a[i].deadlineMs)
		}
	}
}

func TestTimerHeapPopDueOrder(t *testing.T) {
	h := &timerHeap{}
	fire := func(cx *Ctx) {}
	const n = 1000
	want := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		d := int64(rand.Intn(100000))
		h.push(d, fire)
		want = append(want, d)
		checkHeap(t, h)
	}
	sort.Slice(want, func(a, b int) bool { return want[a] < want[b] })

	// Pop everything due at a far horizon; deadline order must match the sort.
	out := h.popDue(1<<62, n, nil)
	if len(out) != n {
		t.Fatalf("popped %d, want %d", len(out), n)
	}
	for i := range out {
		if out[i].deadlineMs != want[i] {
			t.Fatalf("pop %d = %d, want %d", i, out[i].deadlineMs, want[i])
		}
		if out[i].heapPos != -1 {
			t.Fatalf("popped timer %d kept heapPos %d, want -1", i, out[i].heapPos)
		}
	}
	if h.len() != 0 {
		t.Fatalf("heap not drained, len %d", h.len())
	}
}

func TestTimerHeapPopDueNowAndCap(t *testing.T) {
	h := &timerHeap{}
	fire := func(cx *Ctx) {}
	for i := 0; i < 20; i++ {
		h.push(int64(i), fire) // deadlines 0..19
	}
	// nowMs 9 makes 0..9 due; cap 4 takes only the four earliest.
	out := h.popDue(9, 4, nil)
	if len(out) != 4 {
		t.Fatalf("cap not honored, popped %d want 4", len(out))
	}
	for i := range out {
		if out[i].deadlineMs != int64(i) {
			t.Fatalf("pop %d = %d, want %d", i, out[i].deadlineMs, int64(i))
		}
	}
	checkHeap(t, h)
	if d, _ := h.peekDeadline(); d != 4 {
		t.Fatalf("next min = %d, want 4", d)
	}
	// The rest of the due window (4..9) pops next; 10..19 stay.
	out = h.popDue(9, 100, nil)
	if len(out) != 6 {
		t.Fatalf("second popDue got %d, want 6", len(out))
	}
	if d, _ := h.peekDeadline(); d != 10 {
		t.Fatalf("after the due window min = %d, want 10", d)
	}
}

func TestTimerHeapRemove(t *testing.T) {
	h := &timerHeap{}
	fire := func(cx *Ctx) {}
	ts := make([]*timer, 0, 32)
	for i := 0; i < 32; i++ {
		ts = append(ts, h.push(int64(i*7%32), fire))
	}
	checkHeap(t, h)

	// Remove a middle element, the min, and the last, each O(1) via heapPos.
	mid := ts[15]
	h.remove(mid)
	checkHeap(t, h)
	if mid.heapPos != -1 {
		t.Fatal("removed middle kept a live heapPos")
	}
	// Idempotent: removing twice is a no-op.
	h.remove(mid)
	checkHeap(t, h)

	// Remove the current min.
	minT := h.a[0]
	h.remove(minT)
	checkHeap(t, h)
	// Remove the current last slot's timer.
	last := h.a[len(h.a)-1]
	h.remove(last)
	checkHeap(t, h)

	if h.len() != 29 {
		t.Fatalf("len after three removes = %d, want 29", h.len())
	}
}

// TestTimerHeapModel runs interleaved random push/remove against a naive sorted
// reference and checks the heap pops the same order and keeps heapPos honest
// throughout.
func TestTimerHeapModel(t *testing.T) {
	h := &timerHeap{}
	fire := func(cx *Ctx) {}
	rng := rand.New(rand.NewSource(1))
	live := map[*timer]int64{}
	for step := 0; step < 4000; step++ {
		if len(live) == 0 || rng.Intn(2) == 0 {
			d := int64(rng.Intn(10000))
			tm := h.push(d, fire)
			live[tm] = d
		} else {
			// Remove a random live timer.
			var pick *timer
			for tm := range live {
				pick = tm
				break
			}
			h.remove(pick)
			delete(live, pick)
		}
		checkHeap(t, h)
		if h.len() != len(live) {
			t.Fatalf("len %d, model %d", h.len(), len(live))
		}
	}
	// Drain and compare to the sorted reference.
	ref := make([]int64, 0, len(live))
	for _, d := range live {
		ref = append(ref, d)
	}
	sort.Slice(ref, func(a, b int) bool { return ref[a] < ref[b] })
	out := h.popDue(1<<62, len(ref), nil)
	if len(out) != len(ref) {
		t.Fatalf("drained %d, model %d", len(out), len(ref))
	}
	for i := range out {
		if out[i].deadlineMs != ref[i] {
			t.Fatalf("drain %d = %d, model %d", i, out[i].deadlineMs, ref[i])
		}
	}
}

// TestFireTimersOrderAndCap arms more than one fire pass worth of already-due
// timers on a real worker's Ctx and checks fireTimers runs the earliest
// timerFireCap of them in deadline order on the owner, leaving the rest for the
// next pass.
func TestFireTimersOrderAndCap(t *testing.T) {
	rt := testRuntime(1)
	w := rt.workers[0]

	n := timerFireCap + 6
	var fired []int64
	base := time.Now().UnixMilli() - int64(n) - 10
	// Arm in shuffled order so the fire order proves the heap, not the arm order.
	order := rand.Perm(n)
	for _, k := range order {
		dl := base + int64(k)
		w.cx.ArmTimer(dl, func(cx *Ctx) { fired = append(fired, dl) })
	}
	if w.timers.len() != n {
		t.Fatalf("armed %d, heap holds %d", n, w.timers.len())
	}

	got := w.fireTimers()
	if got != timerFireCap {
		t.Fatalf("first pass fired %d, want the cap %d", got, timerFireCap)
	}
	if len(fired) != timerFireCap {
		t.Fatalf("first pass ran %d fire funcs, want %d", len(fired), timerFireCap)
	}
	for i := 1; i < len(fired); i++ {
		if fired[i] < fired[i-1] {
			t.Fatalf("fired out of deadline order at %d: %d then %d", i, fired[i-1], fired[i])
		}
	}
	if w.timers.len() != 6 {
		t.Fatalf("after the capped pass %d remain, want 6", w.timers.len())
	}
	if got := w.fireTimers(); got != 6 {
		t.Fatalf("second pass fired %d, want 6", got)
	}
	if w.timers.len() != 0 {
		t.Fatalf("heap not drained, %d remain", w.timers.len())
	}
	// An empty heap fires nothing, the hot-path gate.
	if got := w.fireTimers(); got != 0 {
		t.Fatalf("empty heap fired %d, want 0", got)
	}
}

// newTimerRuntime builds a single-shard runtime with a keyed test hook that arms
// a timeout timer: args are (key, offsetMs), and the timer's fire sends its
// deadline on fireCh when it runs on the owner. A second op cancels the most
// recent timer to exercise CancelTimer. One shard keeps the owner single so the
// assertions are exact.
func newTimerRuntime(t *testing.T) (*Runtime, byte, chan int64) {
	t.Helper()
	fireCh := make(chan int64, 256)
	handlers := testHandlers()
	armOp := byte(len(handlers))
	handlers = append(handlers, func(cx *Ctx, a [][]byte, r Reply) {
		off, err := strconv.Atoi(string(a[1]))
		if err != nil {
			r.Err("ERR bad offset")
			return
		}
		dl := time.Now().UnixMilli() + int64(off)
		cx.ArmTimer(dl, func(cx *Ctx) { fireCh <- dl })
		r.Status("OK")
	})
	rt := New(1, testArena, testSeg)
	rt.Use(handlers)
	return rt, armOp, fireCh
}

// TestTimedParkFiresFromPark starts the runtime, arms a short deadline on an
// otherwise idle worker, and asserts the timer fires within a tolerance: the
// worker had parked and the timed park woke it on the deadline.
func TestTimedParkFiresFromPark(t *testing.T) {
	rt, armOp, fireCh := newTimerRuntime(t)
	rt.Start()
	defer rt.Stop()
	c := rt.NewConn()

	if err := c.Do(armOp, true, args("k", "40")); err != nil { // fire in 40ms
		t.Fatal(err)
	}
	c.Flush()
	got := collect(t, c, 1)
	if string(got[0]) != "+OK\r\n" {
		t.Fatalf("arm reply = %q", got[0])
	}

	select {
	case <-fireCh:
		// fired on the owner from a parked worker
	case <-time.After(5 * time.Second):
		t.Fatal("timer never fired from a parked worker")
	}
}

// TestProducerWakeDuringTimedPark arms a far deadline so the worker is timed
// parked on it, then enqueues a normal command and asserts it runs promptly,
// well before the far deadline: the channel branch of the timed-park select must
// deliver a producer wake exactly like the plain park. Run under -race this
// exercises the token handoff on the timed park.
func TestProducerWakeDuringTimedPark(t *testing.T) {
	rt, armOp, fireCh := newTimerRuntime(t)
	rt.Start()
	defer rt.Stop()
	c := rt.NewConn()

	if err := c.Do(armOp, true, args("k", "600000")); err != nil { // 10 minutes out
		t.Fatal(err)
	}
	c.Flush()
	_ = collect(t, c, 1) // arm ack; worker now timed-parks on the far deadline

	time.Sleep(20 * time.Millisecond) // let it reach the timed park

	start := time.Now()
	if err := c.Do(opEcho, false, args("live")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	got := collect(t, c, 1)
	if string(got[0]) != "$4\r\nlive\r\n" {
		t.Fatalf("wake-through reply = %q", got[0])
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("producer wake took %v, the far timer must not have gated it", el)
	}
	select {
	case dl := <-fireCh:
		t.Fatalf("far timer fired early at %d", dl)
	default:
	}
}

// TestTimerHeapArmAllocFree pins the steady-path discipline: once the recycle
// pool and slices are warm, arming a timer and removing it allocates nothing.
// The fire func is a single shared non-capturing closure so the measurement is
// the heap's, not a per-iteration closure allocation.
func TestTimerHeapArmAllocFree(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation accounting is not meaningful under the race detector")
	}
	h := &timerHeap{}
	fire := func(cx *Ctx) {}
	// Warm the free pool and the backing slice.
	for i := 0; i < 256; i++ {
		tm := h.push(int64(i), fire)
		h.remove(tm)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		tm := h.push(1, fire)
		h.remove(tm)
	}); allocs != 0 {
		t.Fatalf("arm+cancel allocates %.1f allocs/op, want 0", allocs)
	}
	// popDue reuse: fire and release path is alloc-free once warm too.
	scratch := make([]*timer, 0, 8)
	for i := 0; i < 256; i++ {
		h.push(int64(i), fire)
		scratch = h.popDue(1<<62, 8, scratch[:0])
		for _, tm := range scratch {
			h.release(tm)
		}
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		h.push(1, fire)
		scratch = h.popDue(1<<62, 8, scratch[:0])
		for _, tm := range scratch {
			h.release(tm)
		}
	}); allocs != 0 {
		t.Fatalf("arm+fire+release allocates %.1f allocs/op, want 0", allocs)
	}
}

// TestFireTimersEmptyGateFree pins the no-timer hot path: fireTimers on an empty
// heap is a single length check and a return, allocating nothing and firing
// nothing, so a saturated worker with no armed timer pays exactly the gate.
func TestFireTimersEmptyGateFree(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation accounting is not meaningful under the race detector")
	}
	rt := testRuntime(1)
	w := rt.workers[0]
	if allocs := testing.AllocsPerRun(1000, func() {
		if n := w.fireTimers(); n != 0 {
			t.Fatalf("empty heap fired %d", n)
		}
	}); allocs != 0 {
		t.Fatalf("empty fireTimers allocates %.1f allocs/op, want 0", allocs)
	}
}

// TestTimerCompletesBlockedReply is the light integration with the deferred-
// reply seam: a hook handler parks its reply and arms a timeout timer whose fire
// calls CompleteBlocked with a timeout reply. No serving push arrives, so the
// timer fires on the owner and the parked slot emits at its pipeline position.
// This proves the timer and the #638 deferred reply compose, without a real
// BLPOP.
func TestTimerCompletesBlockedReply(t *testing.T) {
	handlers := testHandlers()
	blockOp := byte(len(handlers))
	handlers = append(handlers, func(cx *Ctx, a [][]byte, r Reply) {
		conn, seq := cx.CurConn(), cx.CurSeq()
		dl := time.Now().UnixMilli() + 30 // 30ms timeout
		cx.ArmTimer(dl, func(cx *Ctx) {
			conn.CompleteBlocked(seq, []byte("*-1\r\n")) // BLPOP timeout null array
		})
		r.Park()
	})
	rt := New(1, testArena, testSeg)
	rt.Use(handlers)
	rt.Start()
	defer rt.Stop()
	c := rt.NewConn()

	if err := c.Do(opSet, true, args("a", "1")); err != nil { // seq 0
		t.Fatal(err)
	}
	if err := c.Do(blockOp, true, args("blk")); err != nil { // seq 1, parks
		t.Fatal(err)
	}
	c.ArmBlock()
	if err := c.Do(opGet, true, args("a")); err != nil { // seq 2, held behind the park
		t.Fatal(err)
	}
	c.Flush()

	got := collect(t, c, 3) // waits for the timer to fire and release the ring
	want := []string{"+OK\r\n", "*-1\r\n", "$1\r\n1\r\n"}
	for i, exp := range want {
		if string(got[i]) != exp {
			t.Fatalf("reply %d = %q, want %q", i, got[i], exp)
		}
	}
	if c.Blocked() {
		t.Fatal("barrier still armed after the timeout reply emitted")
	}
}
