package f1raw

import (
	"runtime"
	"sync/atomic"
)

// This file adds the reader epoch framework, milestone M2 of the collection cold-record
// tiering plan (spec 2064/f1_rewrite_ltm/21, section 7). It is the one piece of FASTER's
// epoch machinery this design needs, and it needs it only now, because M0 made the arena
// reclaimable and M1 made records migrate: once a segment can be freed and its bytes reused,
// a reader that loaded a resident address before a migrator flipped the record away must not
// have those bytes pulled out from under it. The epoch scheme gates a segment's free on
// reader quiescence without putting any per-record atomic on the read path.
//
// The design, from doc 21 section 7 (D18 to D20):
//
//   - A reader publishes the current global epoch into a per-worker slot before it touches
//     the index, and clears it when the operation ends (D18, D19). Publishing is a single
//     relaxed store to a slot the reader owns for the operation, no CAS on the common path
//     once the slot is held, and it is the only cost the read path pays for safe reclamation.
//
//   - The global safe epoch is the minimum published epoch across active slots; anything
//     retired at an epoch strictly below the safe epoch is unreachable by any reader, so the
//     bytes are safe to hand back to the allocator (arena.go retireSegment / reclaimLocked).
//
//   - A long enumerating cursor holds the epoch only for one bounded batch and republishes
//     between batches (D20), so it never starves the migrator: refresh advances the reader's
//     slot to the current global epoch, letting the safe epoch move past a retired segment
//     even while the cursor keeps running.
//
// Correctness rests on Go's sync/atomic being sequentially consistent: a reader's slot store
// and its index load are one total order with the migrator's entry flip and its safe-epoch
// scan. If a reader loads the pre-flip resident address, its slot store precedes the flip in
// that order, so the migrator's later safe-epoch scan observes the reader's published epoch
// (at or below the retire epoch) and does not free the segment. So no fence beyond the atomic
// accesses is required, and the read path stays free of any per-record atomic. The whole
// scheme is inert on a non-segmented store, which never reclaims: pin is only ever called on
// the segmented path, so the default in-memory-fit point path publishes no epoch at all.

// numEpochSlots is the fixed count of reader epoch slots. Doc 21 D19 bounds the slot count by
// the worker count, so the safe-epoch scan is over a small fixed array rather than every
// goroutine. It is a power of two so pin can mask a rotating hint into the slot range, and it
// is sized well past any realistic concurrent-reader count on the segmented path so pin claims
// a free slot on its first probe in the common case.
const numEpochSlots = 512

// paddedEpoch is one reader's published-epoch slot on its own cache line, so two readers
// publishing concurrently never false-share. A value of 0 means the slot is free (no reader
// holds it); a non-zero value is the epoch its holder published. The global epoch starts at 1
// and only rises, so a published epoch is always non-zero and never collides with the free
// sentinel.
type paddedEpoch struct {
	e atomic.Uint64
	_ [64 - 8]byte
}

// epoch is the store's reader-quiescence framework. global is the monotonically rising global
// epoch, bumped by the migrator when it retires a segment. slots are the per-worker published
// epochs the safe-epoch scan reads. hint rotates pin's starting slot so concurrent pins spread
// across the array instead of contending on slot 0. It is allocated lazily by EnableSegments,
// so a non-segmented store carries an empty epoch and never touches these fields.
type epoch struct {
	global atomic.Uint64
	slots  []paddedEpoch
	hint   atomic.Uint64
}

// initEpoch allocates the epoch slots and seeds the global epoch at 1. EnableSegments calls it
// when it switches a store to the reclaimable segmented arena, since only a store that can free
// a segment needs reader quiescence. A store built without segments never calls this and its
// epoch stays zero-valued, which is fine because pin is never reached on the non-segmented path.
func (s *Store) initEpoch() {
	s.ep.slots = make([]paddedEpoch, numEpochSlots)
	s.ep.global.Store(1)
}

// epochGuard is a reader's hold on an epoch slot for the duration of one operation or one
// bounded cursor batch. The zero value (s == nil) is the no-op guard the non-segmented path
// gets from protect, so a caller can defer g.unpin() unconditionally. Do not copy a live guard
// across goroutines: the slot it names is owned by exactly one operation until unpin frees it.
type epochGuard struct {
	s   *Store
	idx int
}

// protect returns an epoch guard for the current operation. On a segmented store it pins a slot
// and publishes the live epoch (pin); on a non-segmented store, which never reclaims, it returns
// the zero guard whose unpin is a no-op, so the in-memory-fit point path pays nothing. A caller
// that already branches on s.segmented for the hot path calls pin directly instead.
func (s *Store) protect() epochGuard {
	if !s.segmented {
		return epochGuard{}
	}
	return s.pin()
}

// pin claims a free epoch slot and publishes the current global epoch into it, returning a guard
// that holds the slot until unpin. Claiming scans the slot array from a rotating hint for a free
// (zero) slot and CAS-claims it with the live epoch in one word, so the claim and the first
// publish are the same store. The published epoch is read just before the claim; a slightly stale
// value is safe because it can only be at or below the current global epoch, which makes the safe
// epoch more conservative (holding a segment longer), never freeing bytes a reader can still
// reach. It spins only if every slot is momentarily busy, which does not happen when the array is
// sized past the concurrent-reader count; the spin exists so a reader never proceeds unprotected.
func (s *Store) pin() epochGuard {
	slots := s.ep.slots
	n := uint64(len(slots))
	start := s.ep.hint.Add(1)
	for {
		for i := uint64(0); i < n; i++ {
			idx := (start + i) & (n - 1)
			if slots[idx].e.Load() != 0 {
				continue
			}
			e := s.ep.global.Load()
			if slots[idx].e.CompareAndSwap(0, e) {
				return epochGuard{s: s, idx: int(idx)}
			}
		}
		// Every slot is busy right now. This is not expected when numEpochSlots exceeds the
		// concurrent-reader count, but a reader must never run unprotected, so yield and retry
		// rather than proceed without a published epoch.
		runtime.Gosched()
	}
}

// refresh republishes the current global epoch into the guard's slot, the D20 between-batch
// step: a cursor that holds one epoch across a bounded batch calls this before the next batch so
// the safe epoch can advance past a segment the migrator retired mid-scan, keeping a long
// enumeration from starving reclamation. It is a single relaxed store, the same cost as the
// initial publish. On the zero guard it is a no-op.
func (g epochGuard) refresh() {
	if g.s == nil {
		return
	}
	g.s.ep.slots[g.idx].e.Store(g.s.ep.global.Load())
}

// unpin releases the guard's slot by clearing it to the free sentinel, ending this operation's
// epoch protection so the safe epoch can advance past it. On the zero guard (the non-segmented
// no-op path) it returns immediately. After unpin the guard must not be used again.
func (g epochGuard) unpin() {
	if g.s == nil {
		return
	}
	g.s.ep.slots[g.idx].e.Store(0)
}

// safeEpoch is the minimum epoch any active reader has published, or the current global epoch
// when no reader is active. A segment retired at an epoch strictly below this value is
// unreachable by every reader and safe to free. The scan is over the fixed slot array and runs
// on the migrator (off the read path), so its cost is bounded and never taxes a reader. Reading
// global first and then the slots is deliberate: a reader that claims a slot after this scan
// passes it published an epoch at least as new as the global value read here, so missing it
// cannot make the result unsafe, only more current.
func (s *Store) safeEpoch() uint64 {
	safe := s.ep.global.Load()
	for i := range s.ep.slots {
		if v := s.ep.slots[i].e.Load(); v != 0 && v < safe {
			safe = v
		}
	}
	return safe
}
