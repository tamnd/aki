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

	// mu guards the committed-bound advance and every ring mutation. A push claims its positions and
	// fills its ring slots in one step under this mutex (appendTail/appendHead), so positions are
	// assigned in commit order by construction and there is never an out-of-order run to stash. Pushes
	// to one hot key serialize here only over the bound move and the ring fill, never an element write
	// or any I/O, which is the whole lever: today's stripe lock is held across N element PutKind writes,
	// and the window holds this mutex across a bound bump and one ring copy instead.
	mu sync.Mutex

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
	span := tail - head
	if span < listRingMinCap {
		span = listRingMinCap
	}
	w.ring = newListRing(nextPow2(span + 1))
	return w
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

// appendTail claims n tail positions and fills them into the ring in one mu-guarded step, returning
// the visible length before the run so an RPUSH reply is baseLen+n. It assigns the run's positions at
// commit time, start == committedTail, so the ring is filled in commit order by construction and there
// is never an out-of-order run to stash. The earlier lock-free reserve-then-commit split existed to
// move an off-lock element write out of the lock, but a resident push writes only the ring, which is
// itself mu-guarded, so fusing the reserve and the commit removes the out-of-order stash (its
// allocation and map churn) with no loss of parallelism: pushes to one hot key already serialized on
// this mutex to fill the ring in order. posElems is the run in position order (posElems[j] is the
// element at start+j). reservedTail is kept equal to committedTail so the pop fast path, which falls
// back when the two bounds differ, never sees a phantom mid-flight push.
func (w *listWindow) appendTail(n int64, posElems [][]byte) (baseLen int64) {
	w.mu.Lock()
	start := w.committedTail.Load()
	baseLen = start - w.committedHead.Load()
	end := start + n
	w.ringEnsure(w.committedHead.Load(), end)
	w.ringPutRun(start, posElems)
	w.reservedTail.Store(end)
	w.committedTail.Store(end)
	w.mu.Unlock()
	return baseLen
}

// appendHead mirrors appendTail at the low end: it claims n positions below the current head, fills
// them into the ring in one mu-guarded step, and returns the visible length before the run so an LPUSH
// reply is baseLen+n. posElems is the run in position order (posElems[j] is the element at start+j),
// the reverse of the LPUSH push order, matching how the stripe-lock body lands elements by
// decrementing the head. reservedHead is kept equal to committedHead for the same pop-fast-path reason.
func (w *listWindow) appendHead(n int64, posElems [][]byte) (baseLen int64) {
	w.mu.Lock()
	high := w.committedHead.Load()
	start := high - n
	baseLen = w.committedTail.Load() - high
	w.ringEnsure(start, w.committedTail.Load())
	w.ringPutRun(start, posElems)
	w.reservedHead.Store(start)
	w.committedHead.Store(start)
	w.mu.Unlock()
	return baseLen
}

