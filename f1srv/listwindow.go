package f1srv

import (
	"sync"
	"sync/atomic"
)

// listWindow is the resident reserved/committed window that lets many connections append to one
// hot list without serializing on the key's stripe lock (spec 2064/f1_rewrite_ltm/impl/26). A
// list grows at both ends, so the window tracks a reserved and a committed bound at each end. A
// push reserves its positions with one atomic bump of the reserved bound, fills its element rows
// off the lock through f1raw's lock-free point publish, then commits, and every reader sees only
// the committed bound. A slot between committed and reserved is claimed but not yet visible, so a
// threaded reader that stops at the committed bound never lands in the reserve-then-fill gap a
// single-threaded listpack never has.
//
// The bounds are an int64 index into an ever-growing window: RPUSH reserves at reservedTail and
// advances it up, LPUSH reserves at reservedHead and advances it down, and count is
// committedTail - committedHead. The reserved bumps are lock-free atomic adds, the hot path. The
// commit bookkeeping takes a tiny per-list mutex, but it guards only pointer-free map and counter
// work, never an element write or any I/O, so concurrent runs from different connections serialize
// only over that bookkeeping and fill their element rows in parallel. That is the whole lever:
// today's stripe lock is held across N element PutKind writes, and the window holds its lock across
// a couple of map operations instead.
type listWindow struct {
	reservedHead  atomic.Int64 // next LPUSH position, decremented to reserve
	committedHead atomic.Int64 // lowest visible position, catches up after the element lands
	reservedTail  atomic.Int64 // next RPUSH position, incremented to reserve
	committedTail atomic.Int64 // one past the highest visible position

	mu sync.Mutex // guards the two pending sets and the ordered commit-bound advance
	// pendTail maps a finished tail run's start position to its end, for runs that committed out of
	// reservation order. The in-order predecessor drains the chain forward when it reaches that
	// start. It is empty in the common case, where one connection's pipelined run reserves a
	// contiguous block and commits it in one step.
	pendTail map[int64]int64
	// pendHead mirrors pendTail for the head end, mapping a finished head run's low end (the more
	// negative bound, which is where the next-lower run continues from) to its high end.
	pendHead map[int64]int64

	// gate separates the lock-free push fast path from the rare command that must retire the
	// window. A push takes gate.RLock across its reserve, element writes, and commit, so many
	// pushes run in parallel; drainEvict takes gate.Lock, which waits out every in-flight push
	// (so no reserved slot is left unpublished) and blocks new pushes from entering, then flushes
	// the committed bounds to the persistent header row and marks the window evicted. This is the
	// only coordination a push pays beyond its own reserve and commit, and it never contends
	// unless a non-push command lands on the same hot key.
	gate sync.RWMutex
	// evicted latches true once drainEvict has retired this window. A push that took gate.RLock
	// after a racing drainEvict set the flag but before it removed the map entry sees the flag and
	// falls back to the stripe-lock path, which re-reads the freshly flushed header and re-admits.
	evicted atomic.Bool

	// lpBytes is the running listpack byte size, seeded from the header at admission and grown by
	// each push run's element bytes. It is a single atomic line updated once per run (not per
	// element), so it is off the per-element path; the everLarge sticky bit latches when it first
	// crosses the byte budget. Reads that need the size or the encoding take these from the window
	// when it is resident, since the header row is stale until the window flushes.
	lpBytes   atomic.Int64
	everLarge atomic.Bool
}

// newListWindow starts an empty window seeded from a list's persisted header bounds. A cold key
// starts at head == tail == 0; a key loaded from its header row starts at the stored head and tail,
// both reserved and committed, so the first push extends from the persisted end.
func newListWindow(head, tail int64) *listWindow {
	w := &listWindow{}
	w.reservedHead.Store(head)
	w.committedHead.Store(head)
	w.reservedTail.Store(tail)
	w.committedTail.Store(tail)
	w.pendTail = make(map[int64]int64)
	w.pendHead = make(map[int64]int64)
	return w
}

// count is the visible length, two atomic loads with no lock, so LLEN never contends the push
// path. It reads committedTail before committedHead; a concurrent push only widens the window, so a
// torn read can undercount by an in-flight push but never reports a position that is not committed.
func (w *listWindow) count() int64 {
	t := w.committedTail.Load()
	h := w.committedHead.Load()
	return t - h
}

// seedBytes sets the running listpack byte size and the sticky large flag at admission, from the
// header the cold-key push just wrote. After this the window owns the size; the header row is
// stale until drainEvict flushes it back.
func (w *listWindow) seedBytes(lpBytes uint64, everLarge bool) {
	w.lpBytes.Store(int64(lpBytes))
	w.everLarge.Store(everLarge)
}

// addBytes grows the running size by one push run's element bytes in a single atomic add and
// latches everLarge the first time the total crosses the listpack byte budget. It is called once
// per run, not per element, so the size line is off the per-element path.
func (w *listWindow) addBytes(delta int64) {
	total := w.lpBytes.Add(delta)
	if total > listListpackMaxBytes && !w.everLarge.Load() {
		w.everLarge.Store(true)
	}
}

// sizeState reads the running size and the large flag for a flush or an encoding read.
func (w *listWindow) sizeState() (lpBytes uint64, everLarge bool) {
	return uint64(w.lpBytes.Load()), w.everLarge.Load()
}

// bounds returns the visible window, the committed head and tail, for a read that consults the
// window instead of the stale header row.
func (w *listWindow) bounds() (head, tail int64) {
	return w.committedHead.Load(), w.committedTail.Load()
}

// reserveTail claims n contiguous positions at the tail for an RPUSH run and returns the first, so
// the run writes elements at [start, start+n). It is a single lock-free atomic add, the hot path
// that replaces taking the stripe lock.
func (w *listWindow) reserveTail(n int64) (start int64) {
	return w.reservedTail.Add(n) - n
}

// commitTail makes an RPUSH run's positions [start, start+n) visible, advancing committedTail only
// in reservation order so a reader never sees a gap. If this run is next (committedTail == start) it
// advances the bound past itself and drains any later runs that were waiting on it; otherwise it
// records its end for the in-order predecessor to pick up. All of it is under the per-list mutex,
// but the mutex covers only these map and counter operations, never an element write.
func (w *listWindow) commitTail(start, n int64) {
	end := start + n
	w.mu.Lock()
	if w.committedTail.Load() != start {
		w.pendTail[start] = end
		w.mu.Unlock()
		return
	}
	next := end
	for {
		e, ok := w.pendTail[next]
		if !ok {
			break
		}
		delete(w.pendTail, next)
		next = e
	}
	w.committedTail.Store(next)
	w.mu.Unlock()
}

// reserveHead claims n contiguous positions at the head for an LPUSH run and returns the lowest, so
// the run writes elements at [start, start+n) with start below the old head. The reserved bound
// moves down by n, mirroring reserveTail.
func (w *listWindow) reserveHead(n int64) (start int64) {
	return w.reservedHead.Add(-n)
}

// commitHead makes an LPUSH run's positions [start, start+n) visible, advancing committedHead down
// only in reservation order. The head chains on the high end (start+n): the run whose high end
// equals the current committedHead is next, and it drains lower runs waiting on its own low end.
func (w *listWindow) commitHead(start, n int64) {
	high := start + n
	w.mu.Lock()
	if w.committedHead.Load() != high {
		w.pendHead[high] = start
		w.mu.Unlock()
		return
	}
	next := start
	for {
		s, ok := w.pendHead[next]
		if !ok {
			break
		}
		delete(w.pendHead, next)
		next = s
	}
	w.committedHead.Store(next)
	w.mu.Unlock()
}
