package shard

import "sync/atomic"

// The F6 epoch bracket, single-owner form (doc 03 section 3.5). The worker
// enters the epoch once at the top of a batch and exits at the bottom, so the
// amortized cost is one enter and one exit per batch. Under single ownership
// the epoch guards nothing on the data path itself; it exists for the
// boundary with the shard's future off-worker consumers (the M7 reclaimer,
// snapshot cuts, parked cold reads), which must never free a segment a batch
// in flight can still reference.
//
// The multi-reader slot array and its CAS claim are gone: the shard has
// exactly one data-path publisher, so the published epoch is one owned word
// and enter is a plain store into it, no claim, no scan for a free slot. The
// stores stay atomic because the M7 reclaimer reads the word from its own
// goroutine when it computes the safe epoch; that read is the only concurrent
// access the scheme ever has.
type epoch struct {
	// global is the current epoch, bumped by the reclaimer when it retires a
	// segment. Until M7 lands nothing bumps it, so the bracket runs at epoch 1
	// and safe never moves; the bracket discipline is in place from the first
	// batch so reclamation arrives under an already-exercised contract.
	global atomic.Uint64

	// owner is the worker's published epoch: non-zero for exactly the span of
	// one batch, zero when the worker is quiescent between batches.
	owner atomic.Uint64
}

func (e *epoch) init() {
	e.global.Store(1)
}

// enter publishes the current epoch for the batch about to execute. One plain
// owned store; the f1 pin path's slot scan and CAS claim existed only because
// many readers shared the slot array.
func (e *epoch) enter() {
	e.owner.Store(e.global.Load())
}

// exit ends the batch's bracket, letting the safe epoch advance past it.
func (e *epoch) exit() {
	e.owner.Store(0)
}

// bump advances the global epoch and returns the stamp that was current before
// the advance: the epoch a segment retired "now" carries. The reclaimer's half
// of the F6 contract is retire-at-stamp-then-advance, so this one call does the
// advance and hands back the stamp to retire under (store.ReclaimSafe frees a
// retired segment once safe passes strictly beyond its stamp). Every future
// bracket now publishes an epoch past the stamp, so once the brackets live at
// retirement drain, safe clears it. Owner-only, like enter and exit; the
// reclaimer runs on the owner between batches.
func (e *epoch) bump() uint64 {
	return e.global.Add(1) - 1
}

// safe is the newest epoch every in-flight bracket has moved past: anything
// retired strictly below it is unreachable and free to reclaim. This is the
// M7 reclamation hook; segment retire, the global bump, and the actual free
// land there, and until then safe is read only by tests pinning the bracket
// semantics.
func (e *epoch) safe() uint64 {
	s := e.global.Load()
	if v := e.owner.Load(); v != 0 && v < s {
		s = v
	}
	return s
}
