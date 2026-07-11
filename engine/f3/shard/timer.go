package shard

// timer is one armed deadline on a worker's timer heap. fire runs on the shard
// owner when the deadline passes, unless the command that armed it cancels
// first through the worker's CancelTimer. heapPos is the timer's live index in
// the heap slice, kept current on every sift so remove is O(1); it is -1 once
// the timer has left the heap (fired or cancelled), which is what makes remove
// idempotent. A blocking command with a finite timeout is the only thing that
// arms one, so the heap is empty everywhere on the throughput path.
type timer struct {
	deadlineMs int64
	fire       func(cx *Ctx)
	heapPos    int
}

// timerHeap is a min-heap of armed timers keyed by absolute deadline in unix-ms.
// It is owner-only like keyQ: exactly one goroutine (the shard worker) ever
// touches it, so it holds no lock and every field below is a plain load. It is
// empty on the throughput hot path, since a worker arms a timer only while a
// blocking command with a finite timeout is parked on it, so len() is the one
// relaxed gate every timer touchpoint checks before it does anything else.
//
// The heap is hand-written, not built on container/heap: that interface boxes
// every element through interface{} and its Pop cannot give back the O(1)
// remove this needs, which keys on the position back-pointer heapPos that a
// container/heap Less/Swap pair would have to maintain out of band anyway. The
// sift is a dozen lines and matches the other hand-written structures in this
// package (keyQ, the mpsc, the reorder ring).
//
// free is the recycle pool: a timer that leaves the heap goes back on free and
// the next push reuses it, so arming is allocation-free on the steady path once
// the pool and the slices have grown to their working size (asserted in
// timer_test). The fire func is generic (func(*Ctx)) so the doc 16 expiry ticks
// can share the same heap later with no rework (plan O4).
type timerHeap struct {
	a    []*timer
	free []*timer
}

// len is the relaxed gate. Owner-only, so a plain slice length, no atomic.
func (h *timerHeap) len() int { return len(h.a) }

// alloc pulls a timer struct off the recycle pool, or makes one when the pool
// is empty (the warmup allocations, paid once).
func (h *timerHeap) alloc() *timer {
	if n := len(h.free); n > 0 {
		t := h.free[n-1]
		h.free[n-1] = nil
		h.free = h.free[:n-1]
		return t
	}
	return &timer{}
}

// release returns a detached timer to the recycle pool, dropping its fire
// closure so the captured reply target can be collected and cannot be run
// twice. Only a timer already off the heap (heapPos -1) is released.
func (h *timerHeap) release(t *timer) {
	t.fire = nil
	h.free = append(h.free, t)
}

// push arms a timer at deadlineMs and returns the handle the caller cancels
// with remove. It sifts the new timer up to its slot and records its position.
func (h *timerHeap) push(deadlineMs int64, fire func(cx *Ctx)) *timer {
	t := h.alloc()
	t.deadlineMs = deadlineMs
	t.fire = fire
	t.heapPos = len(h.a)
	h.a = append(h.a, t)
	h.siftUp(t.heapPos)
	return t
}

// peekDeadline returns the nearest deadline without popping, the value the idle
// timed park computes its sleep from. The bool is false on an empty heap.
func (h *timerHeap) peekDeadline() (int64, bool) {
	if len(h.a) == 0 {
		return 0, false
	}
	return h.a[0].deadlineMs, true
}

// popDue pops every timer whose deadline is at or before nowMs, up to limit,
// appending them to out and returning it. The popped timers keep their fire
// funcs so the caller can run them; the caller releases each back to the pool
// after firing. A run that hits the limit leaves the rest for the next pass,
// which is why fireTimers returns a positive count and the owner loop comes
// straight back.
func (h *timerHeap) popDue(nowMs int64, limit int, out []*timer) []*timer {
	for len(h.a) > 0 && h.a[0].deadlineMs <= nowMs && len(out) < limit {
		out = append(out, h.popMin())
	}
	return out
}

// popMin detaches the earliest timer, moves the last element into its slot, and
// sifts it down. It does not release the timer; the fire func stays live for the
// caller. heapPos is set to -1 so a stale handle passed to remove is a no-op.
func (h *timerHeap) popMin() *timer {
	a := h.a
	n := len(a)
	t := a[0]
	last := a[n-1]
	a[0] = last
	last.heapPos = 0
	a[n-1] = nil
	h.a = a[:n-1]
	t.heapPos = -1
	if len(h.a) > 0 {
		h.siftDown(0)
	}
	return t
}

// remove takes a timer out of the heap in O(1) via its stored position and
// releases it to the pool. It is idempotent: a timer already off the heap
// (heapPos -1, or a slot that no longer holds it) is left alone, so a command
// that both got served and later tries to cancel does no harm. The moved
// tail element is re-sifted the container/heap way: sift it down, and only sift
// it up if the down pass did not move it.
func (h *timerHeap) remove(t *timer) {
	i := t.heapPos
	if i < 0 || i >= len(h.a) || h.a[i] != t {
		return
	}
	n := len(h.a)
	last := h.a[n-1]
	h.a[i] = last
	last.heapPos = i
	h.a[n-1] = nil
	h.a = h.a[:n-1]
	t.heapPos = -1
	if i < len(h.a) {
		if !h.siftDown(i) {
			h.siftUp(i)
		}
	}
	h.release(t)
}

// siftUp walks the timer at i toward the root while it precedes its parent,
// keeping heapPos current on every swap.
func (h *timerHeap) siftUp(i int) {
	a := h.a
	for i > 0 {
		parent := (i - 1) / 2
		if a[parent].deadlineMs <= a[i].deadlineMs {
			break
		}
		a[parent], a[i] = a[i], a[parent]
		a[parent].heapPos = parent
		a[i].heapPos = i
		i = parent
	}
}

// siftDown walks the timer at i toward the leaves while a child precedes it, and
// reports whether it moved (the signal remove uses to decide whether an up pass
// is still needed).
func (h *timerHeap) siftDown(i int) bool {
	a := h.a
	n := len(a)
	start := i
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		m := l
		if r := l + 1; r < n && a[r].deadlineMs < a[l].deadlineMs {
			m = r
		}
		if a[i].deadlineMs <= a[m].deadlineMs {
			break
		}
		a[i], a[m] = a[m], a[i]
		a[i].heapPos = i
		a[m].heapPos = m
		i = m
	}
	return i > start
}
