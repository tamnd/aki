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

	"github.com/tamnd/aki/format"
)

// Page is a single in-memory page frame. Data is exactly pageSize bytes and is
// the authoritative copy while the page is cached; readers and writers operate
// on Data directly and mark the page dirty through the pager.
type Page struct {
	No   uint32
	Data []byte

	dirty bool
	pins  int32
	// ref is the clock-eviction reference bit: set on access, cleared by the
	// sweep, evicted when found clear (doc 03 §7).
	ref bool
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

// frameTable is the buffer pool's frame index, guarded by its own mutex so the
// pager's page-fault path is the only critical section on the hot read path.
type frameTable struct {
	mu     sync.Mutex
	frames map[uint32]*Page
	// clock is the round-robin sweep order for eviction.
	clock []uint32
	hand  int
	cap   int
}

func newFrameTable(capacity int) *frameTable {
	if capacity < 8 {
		capacity = 8
	}
	return &frameTable{
		frames: make(map[uint32]*Page, capacity),
		cap:    capacity,
	}
}

// get returns the cached frame for pgno, or nil.
func (ft *frameTable) get(pgno uint32) *Page {
	p := ft.frames[pgno]
	if p != nil {
		p.ref = true
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
		if p.pins > 0 || p.dirty {
			continue
		}
		if p.ref {
			p.ref = false
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
