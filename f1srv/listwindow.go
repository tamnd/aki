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

	mu sync.Mutex // guards the two pending sets, the commit-bound advance, and every ring mutation
	// pendTail maps a finished tail run's start position to its far bound (end) and element bytes,
	// for runs that committed out of reservation order. The in-order predecessor drains the chain
	// forward when it reaches that start, filling each drained run's bytes into the ring as it goes,
	// so the ring is always filled in commit order and its live range stays exactly the committed
	// span. It is empty in the common case, where one connection's pipelined run reserves a
	// contiguous block and commits it in one step.
	pendTail map[int64]pendRun
	// pendHead mirrors pendTail for the head end, keyed by a finished head run's high end and
	// carrying its low bound (where the next-lower run continues from) and element bytes.
	pendHead map[int64]pendRun

	// ring holds the resident element bytes for every committed position, so a read or pop can take
	// bytes straight from a slot instead of paying an f1raw hash probe per element (spec 2064/impl/34).
	// Every ring mutation (put and grow) happens under mu, and the ring's live range is kept equal to
	// [committedHead, committedTail), so a single grow rehashing that range preserves exactly the live
	// bytes. Slice 2 fills the ring but does not yet read from it (reads and pops still go through
	// f1raw); the differential test asserts the ring tracks f1raw for every committed position.
	ring *listRing

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

	// preLo, preHi bound the pre-admission block: the positions [preLo, preHi) that were already
	// written to f1raw rows by the stripe-lock push that admitted this window (the seed span). Every
	// position outside that block was appended lock-free after admission and lives only in the ring,
	// never in an f1raw row (slice 3: push stops writing rows for resident positions). resident(pos)
	// tells pop and read which store to use: the ring for a resident position, the f1raw row for a
	// pre-block one.
	//
	// The block only ever shrinks over the window's life, never grows: a pop that drains a bound into
	// the seed span deletes those rows for good, so contractPreblock walks the matching end inward as
	// each pop lands. That monotonic contraction is what keeps residency correct across a pop-then-push
	// that reuses a position number: a tail pop past preHi frees a seed position, and a later push
	// re-fills that same position into the ring, so preHi must have already stepped below it or the
	// re-pushed value would be misread from its deleted row. The two bounds are atomic because a
	// reader (LPOS, LRANGE) scans them under the shared gate while a concurrent pop contracts them
	// under the same shared gate; every write is monotonic, so a racing reader sees either the old or
	// the new bound and both are consistent with the committed span it also snapshots. drainEvict
	// flushes the surviving resident positions to rows on retire so the persisted image is whole.
	preLo, preHi atomic.Int64
}

// resident reports whether position pos lives in the ring rather than an f1raw row. A position is
// resident when it was appended after admission, which is every position outside the pre-admission
// block [preLo, preHi). Pop reads a resident position straight from the ring with no hash probe,
// the whole point of the model, and takes a pre-block position from its f1raw row.
func (w *listWindow) resident(pos int64) bool {
	return pos < w.preLo.Load() || pos >= w.preHi.Load()
}

// contractPreblock walks the pre-block band inward after a pop advanced a committed bound into it, so
// the seed rows a pop deleted stop being classified as pre-block and a position re-pushed into that
// freed number reads from the ring where the push put it. A head pop raises the low bound to the new
// committed head; a tail pop lowers the high bound to the new committed tail. When the two bounds
// cross, the seed is fully drained and the band collapses to empty. The caller holds w.mu, and the
// writes are monotonic (preLo only rises, preHi only falls), so a reader scanning the band under the
// shared gate never sees it widen. It is a cheap pair of compares on the pop path, off the ring work.
func (w *listWindow) contractPreblock(atHead bool, newBound int64) {
	lo, hi := w.preLo.Load(), w.preHi.Load()
	if lo >= hi {
		return // already empty, nothing seeded to protect
	}
	if atHead {
		if newBound <= lo {
			return // the pop stayed below the seed, band unchanged
		}
		if newBound >= hi {
			w.preLo.Store(0)
			w.preHi.Store(0)
			return
		}
		w.preLo.Store(newBound)
		return
	}
	if newBound >= hi {
		return // the pop stayed above the seed, band unchanged
	}
	if newBound <= lo {
		w.preLo.Store(0)
		w.preHi.Store(0)
		return
	}
	w.preHi.Store(newBound)
}

// pendRun is a committed-but-not-yet-visible push run held for its in-order predecessor to drain.
// far is the run's opposite bound from its map key (the end for a tail run keyed by start, the low
// bound for a head run keyed by high), and elems carries the run's element bytes in position order,
// deep-copied at stash time because the originals alias the connection read buffer.
type pendRun struct {
	far   int64
	elems [][]byte
}

// listRingMinCap is the floor capacity a fresh window's ring starts at, a power of two generous
// enough that a short-lived list never grows the ring while staying small.
const listRingMinCap = 1 << 10

// nextPow2 returns the smallest power of two greater than or equal to n, and at least 1.
func nextPow2(n int64) int64 {
	p := int64(1)
	for p < n {
		p <<= 1
	}
	return p
}

// newListWindow starts an empty window seeded from a list's persisted header bounds. A cold key
// starts at head == tail == 0; a key loaded from its header row starts at the stored head and tail,
// both reserved and committed, so the first push extends from the persisted end. The ring is sized
// to hold the seeded span with headroom, so a window admitted for an already-large list does not
// grow the ring on its first push.
func newListWindow(head, tail int64) *listWindow {
	w := &listWindow{}
	w.reservedHead.Store(head)
	w.committedHead.Store(head)
	w.reservedTail.Store(tail)
	w.committedTail.Store(tail)
	// The seed span was written to f1raw rows by the admitting stripe-lock push; mark it the pre-block
	// so pop takes those positions from their rows and every later lock-free push lands resident.
	w.preLo.Store(head)
	w.preHi.Store(tail)
	w.pendTail = make(map[int64]pendRun)
	w.pendHead = make(map[int64]pendRun)
	span := tail - head
	if span < listRingMinCap {
		span = listRingMinCap
	}
	w.ring = newListRing(nextPow2(span + 1))
	return w
}

// copyRun deep-copies a run's element bytes for stashing in a pending map, since the originals alias
// the connection read buffer that the next command overwrites.
func copyRun(posElems [][]byte) [][]byte {
	out := make([][]byte, len(posElems))
	for i, e := range posElems {
		b := make([]byte, len(e))
		copy(b, e)
		out[i] = b
	}
	return out
}

// ringPutRun fills the ring slots for a run whose position start+j holds posElems[j]. The caller
// holds mu and has already grown the ring so the run's positions are collision-free.
func (w *listWindow) ringPutRun(start int64, posElems [][]byte) {
	for j, e := range posElems {
		w.ring.put(start+int64(j), e)
	}
}

// ringEnsure grows the ring until it can hold the live positions [lo, hi) collision-free, rehashing
// the current committed live range [committedHead, committedTail) on each doubling. The caller holds
// mu and passes the live range the ring will hold once the run being committed is added; the current
// committed bounds still describe the bytes already in the ring, so the rehash preserves exactly
// those and the run's new positions are put fresh after the grow.
func (w *listWindow) ringEnsure(lo, hi int64) {
	for w.ring.capacity() <= hi-lo {
		w.ring.grow(w.committedHead.Load(), w.committedTail.Load())
	}
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
// in reservation order so a reader never sees a gap. posElems carries the run's element bytes in
// position order (posElems[j] is the element at start+j). If this run is next (committedTail ==
// start) it fills its bytes into the ring, advances the bound past itself, and drains any later runs
// that were waiting on it, filling each as it goes; otherwise it stashes its bytes for the in-order
// predecessor to pick up. Every ring fill and grow happens here under the per-list mutex, so the ring
// is mutated in commit order and its live range stays exactly the committed span.
func (w *listWindow) commitTail(start, n int64, posElems [][]byte) {
	end := start + n
	w.mu.Lock()
	if w.committedTail.Load() != start {
		w.pendTail[start] = pendRun{far: end, elems: copyRun(posElems)}
		w.mu.Unlock()
		return
	}
	w.ringEnsure(w.committedHead.Load(), end)
	w.ringPutRun(start, posElems)
	w.committedTail.Store(end)
	next := end
	for {
		pr, ok := w.pendTail[next]
		if !ok {
			break
		}
		delete(w.pendTail, next)
		w.ringEnsure(w.committedHead.Load(), pr.far)
		w.ringPutRun(next, pr.elems)
		w.committedTail.Store(pr.far)
		next = pr.far
	}
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
// posElems carries the run's element bytes in position order (posElems[j] is the element at start+j),
// filled into the ring as each run becomes visible, under the per-list mutex like commitTail.
func (w *listWindow) commitHead(start, n int64, posElems [][]byte) {
	high := start + n
	w.mu.Lock()
	if w.committedHead.Load() != high {
		w.pendHead[high] = pendRun{far: start, elems: copyRun(posElems)}
		w.mu.Unlock()
		return
	}
	w.ringEnsure(start, w.committedTail.Load())
	w.ringPutRun(start, posElems)
	w.committedHead.Store(start)
	next := start
	for {
		pr, ok := w.pendHead[next]
		if !ok {
			break
		}
		delete(w.pendHead, next)
		w.ringEnsure(pr.far, w.committedTail.Load())
		w.ringPutRun(pr.far, pr.elems)
		w.committedHead.Store(pr.far)
		next = pr.far
	}
	w.mu.Unlock()
}
