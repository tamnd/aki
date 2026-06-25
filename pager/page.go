// Package pager is aki's single-file pager and buffer pool (spec 2064 doc 03).
// It turns a flat VFS file into an array of fixed-size pages, caches hot pages
// in memory, tracks dirty pages, allocates and frees pages through a freelist,
// and commits a transaction atomically by swapping the double-buffered meta
// page (doc 02 §9). Durability via the write-ahead log is layered on top in the
// wal package (doc 04); on its own the pager is already crash-atomic because a
// commit becomes visible only when the higher-sequence meta page is fsynced.
package pager

import (
	"sync"
	"sync/atomic"

	"github.com/tamnd/aki/format"
)

// Page is a single in-memory page frame. Data is exactly pageSize bytes and is
// the authoritative copy while the page is cached; readers and writers operate
// on Data directly and mark the page dirty through the pager.
//
// pins, dirty, and ref are atomics so the buffer-pool hot path (a cache hit and
// its matching Unpin) can pin and release under the frame table's read lock
// instead of its write lock. The map structure itself still moves only under the
// write lock; these three per-frame flags are the only state those shared-lock
// paths touch, and an eviction sweep, which reads all three, runs under the write
// lock so it never overlaps a concurrent pin.
type Page struct {
	No   uint32
	Data []byte

	dirty atomic.Bool
	pins  atomic.Int32
	// ref is the clock-eviction reference bit: set on access, cleared by the
	// sweep, evicted when found clear (doc 03 §7).
	ref atomic.Bool
}

// Header parses the common page header from the front of the page.
func (p *Page) Header() (format.PageHeader, error) {
	return format.ParsePageHeader(p.Data)
}

// PutHeader writes h into the front of the page and marks it dirty-pending; the
// caller still unpins with dirty=true to schedule write-back.
func (p *Page) PutHeader(h format.PageHeader) error {
	return h.MarshalTo(p.Data)
}

// poolStripes is the number of independent frame tables the buffer pool is split
// into. A page lives in stripe pgno&poolStripeMask, so the reader-counter atomic
// of each stripe's lock is a separate cache line and the shard workers stop
// fighting over one shared counter on every page pin and unpin (note 204). The
// count is a power of two so the routing is a mask, and sixteen against the eight
// shard workers keeps the odds of two workers colliding in one stripe low.
const (
	poolStripes    = 16
	poolStripeMask = poolStripes - 1
)

// bufferPool is the buffer pool: an array of independent frame-table stripes
// routed by page number. Per-page work (lookup, pin, eviction) touches one
// stripe; whole-pool work (counts, flush, close, the pinned-page walk) visits
// every stripe in turn, each under its own lock, since none of those needs a
// snapshot consistent across stripes.
type bufferPool struct {
	stripes [poolStripes]frameTable
}

func newBufferPool(capacity int) *bufferPool {
	if capacity < 8 {
		capacity = 8
	}
	per := capacity / poolStripes
	if per < 8 {
		per = 8
	}
	bp := &bufferPool{}
	for i := range bp.stripes {
		bp.stripes[i].init(per)
	}
	return bp
}

// stripe returns the frame table that owns pgno.
func (bp *bufferPool) stripe(pgno uint32) *frameTable {
	return &bp.stripes[pgno&poolStripeMask]
}

// counts sums resident and dirty frames across every stripe. The pager calls it
// for the buffer-pool growth fields.
func (bp *bufferPool) counts() (resident, dirty int) {
	for i := range bp.stripes {
		r, d := bp.stripes[i].counts()
		resident += r
		dirty += d
	}
	return resident, dirty
}

// frameTable is one buffer-pool stripe, guarded by its own mutex so the pager's
// page-fault path is the only critical section on the hot read path.
type frameTable struct {
	mu     sync.RWMutex
	frames map[uint32]*Page
	// clock is the round-robin sweep order for eviction.
	clock []uint32
	hand  int
	cap   int
}

func (ft *frameTable) init(capacity int) {
	ft.frames = make(map[uint32]*Page, capacity)
	ft.cap = capacity
}

// counts returns the number of resident frames and how many of them are dirty.
func (ft *frameTable) counts() (resident, dirty int) {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	resident = len(ft.frames)
	for _, p := range ft.frames {
		if p.dirty.Load() {
			dirty++
		}
	}
	return resident, dirty
}

// maybeEvict drops one clean victim if the stripe is at capacity. Caller holds
// ft.mu for writing.
func (ft *frameTable) maybeEvict() {
	if len(ft.frames) < ft.cap {
		return
	}
	if victim, ok := ft.evictable(); ok {
		ft.drop(victim)
	}
}

// get returns the cached frame for pgno, or nil. It is called both under the read
// lock (the hit path) and the write lock (the miss recheck); it only reads the
// map and sets the atomic ref bit, so the read lock is enough.
func (ft *frameTable) get(pgno uint32) *Page {
	p := ft.frames[pgno]
	if p != nil {
		p.ref.Store(true)
	}
	return p
}

// put inserts a freshly loaded frame, evicting a clean unpinned victim if the
// pool is over capacity. It returns the victim's page number list to drop, if
// any (always clean, so the caller need not write them back).
func (ft *frameTable) put(p *Page) {
	ft.frames[p.No] = p
	ft.clock = append(ft.clock, p.No)
}

// evictable scans clockwise for a clean, unpinned frame with its ref bit clear,
// clearing ref bits as it sweeps. It returns the victim page number and true,
// or 0 and false if nothing can be evicted right now.
func (ft *frameTable) evictable() (uint32, bool) {
	if len(ft.clock) == 0 {
		return 0, false
	}
	for range 2 * len(ft.clock) {
		pgno := ft.clock[ft.hand]
		ft.hand = (ft.hand + 1) % len(ft.clock)
		p := ft.frames[pgno]
		if p == nil {
			continue
		}
		if p.pins.Load() > 0 || p.dirty.Load() {
			continue
		}
		if p.ref.Load() {
			p.ref.Store(false)
			continue
		}
		return pgno, true
	}
	return 0, false
}

// drop removes a frame from the table and the clock ring.
func (ft *frameTable) drop(pgno uint32) {
	delete(ft.frames, pgno)
	for i, n := range ft.clock {
		if n == pgno {
			ft.clock = append(ft.clock[:i], ft.clock[i+1:]...)
			if ft.hand > i {
				ft.hand--
			}
			break
		}
	}
}
