package shard

import (
	"time"

	"github.com/tamnd/aki/engine/obs1/store"
)

// Block-not-drop backpressure on the owner (spec 2064/f3/06 section 8, plan
// milestones/M7-slice2-block-not-drop-plan.md, slice 5a: the core park and
// retry over the single-key write path).
//
// The async migrator leaves a staged record resident until its phase-2 flip
// lands on a later completion boundary. Under a tight resident cap that window
// is exactly when a foreground write can fail to allocate: the arena is full of
// records the drain is about to free but has not freed yet. Rather than map that
// ErrFull to an OOM reply, which would be a write acknowledged as failed while
// the store is one drain-completion away from having room (the L17 silent-drop
// class F9 forbids), the worker parks the write at the owner and retries it when
// a drain frees space. The wait is progress-gated on the cold append cursor, not
// a wall clock, so a write blocks indefinitely while the drain advances however
// slowly and only surfaces ErrFull when no progress is possible.
//
// This slice reuses the batch-defer rail intent deferral already runs
// (txnroute.go): a parked write holds its batch through the batch's defer count,
// so the node stays with the owner and its replies wait in place until the retry
// re-runs the handler and the count falls to zero, at which point the whole node
// pushes in order. The one tradeoff is that the parked write's batch-mates wait
// with it; slice 5b's out-of-order completion narrows the hold to the single
// parked slot. With no write parked the path is one bool load after each write
// handler (executeCmd) and one length check at each boundary, so the L9
// no-pressure contract holds: a store that never crosses its resident cap never
// parks a write and the M0-M6 matrices re-run at zero delta.

// bpStallWindow is the coarse stall bound for slice 5a: the number of retry
// passes without a cold-cursor advance (and with no drain in flight or pending)
// after which a parked write surfaces the OOM reply. It is a fixed poll count,
// never a wall-clock budget, so a wait that is making progress resets it and can
// legally run for a long time on a saturated disk. Slice 5b replaces it with the
// four-case taxonomy and a calibrated poll constant.
const bpStallWindow = 64

// fullWaiter is one write parked on a full arena: its batch node and the command
// slot within it. The pair is enough to re-run the command against a reclaimed
// arena (executeCmd rebuilds the argument views from the node) and to write the
// reply into the command's own slot, so no reply bytes are copied at park time.
type fullWaiter struct {
	b *hopBatch
	i int
	// resume is the argument index a re-run of this write restarts at: 0 for a
	// single-key write and for a multi-key write that parked before committing any
	// pair, or the first unwritten pair's index for one that parked part-way
	// (handler.go ParkFullAt). The worker seeds Ctx.resume with it before each
	// retry. Because it is here the retryFull loop cannot use a struct conversion
	// to Reply anymore; it builds the Reply from b and i by name.
	resume int
	// reason names why the write parked (park.go, doc 04 section 6): resident
	// for the arena-full park below, flushlag and lease when their slices raise
	// them. A stall-out counts the waiter under its reason.
	reason ParkReason
}

// parkOnFull registers a write that could not allocate on the shard's full-waiter
// FIFO and holds its batch by raising the batch's defer count, exactly as an
// intent-deferred command does. The node will not push until the count falls back
// to zero, so the parked slot (which produced no reply) and its batch-mates wait
// together until retryFull re-runs the command and it either allocates or stalls
// out. Owner goroutine only.
func (w *worker) parkOnFull(b *hopBatch, i int) {
	w.fullWaiters = append(w.fullWaiters, fullWaiter{b: b, i: i, resume: w.cx.resume, reason: w.cx.parkReason})
	b.deferN++
	w.bpWaits++
	w.bpReasonWaits[w.cx.parkReason]++
}

// retryFull re-runs every parked write against the arena a boundary just
// reclaimed, in FIFO order, completing the ones that now allocate and holding the
// rest. A write that succeeds writes its reply into its own batch slot, drops the
// batch's defer count, and pushes the node when it was the last hold; a write
// that parks again stays in the FIFO. When writes remain parked the stall counter
// advances only on a pass with no cold-cursor progress and no drain in flight,
// and crossing the window surfaces the OOM reply to every remaining waiter, the
// honest answer when the cold tail cannot move. The fast path is one length check
// (L9). Owner goroutine only.
func (w *worker) retryFull() {
	if len(w.fullWaiters) == 0 {
		return
	}
	w.ep.enter()
	w.cx.NowMs = time.Now().UnixMilli()
	w.cx.retrying = true
	kept := w.fullWaiters[:0]
	for _, fw := range w.fullWaiters {
		w.cx.parkFull = false
		w.cx.resume = fw.resume
		w.executeCmd(fw.b, fw.i)
		if w.cx.parkFull {
			// Still full: keep the waiter, its batch stays held. A multi-key write
			// that committed more pairs before re-parking advanced Ctx.resume, so
			// carry the new cursor forward and the next retry resumes past the pairs
			// this pass just wrote.
			fw.resume = w.cx.resume
			kept = append(kept, fw)
			continue
		}
		// Allocated: the reply (or partial, for a fan sub-command) is in the slot
		// now, release the batch's hold and push the node if this was the last
		// command holding it.
		w.releaseHold(fw.b)
	}
	for i := len(kept); i < len(w.fullWaiters); i++ {
		w.fullWaiters[i] = fullWaiter{}
	}
	w.fullWaiters = kept
	w.cx.parkFull = false
	w.cx.retrying = false
	w.cx.resume = 0
	w.ep.exit()
	if len(w.fullWaiters) == 0 {
		w.bpStall = 0
		return
	}
	w.stallCheck()
}

// stallCheck advances the coarse stall bound after a retry pass left writes
// parked. Any of three signals is progress and resets the counter: the cold
// cursor advanced since the last pass, a drain is in flight or pending
// (ColdDraining), or the migrator has cold space queued to return to the arena
// (ReclaimPending, segments a flip emptied but the epoch has not freed yet). The
// last one closes the window slice 5a's cold-cursor-only check left open: after a
// drain moves records cold and stops, the arena still needs a few boundaries to
// hand the emptied segments back through the epoch, and during those the cold tail
// is static and no drain is in flight, so a retry loop would wrongly count them as
// stalls and OOM a write whose room was one reclaim pass away. Only when none of
// the three holds does the counter climb, and crossing the window means the arena
// truly cannot free room (disk full, an I/O error, no migratable residue, or a
// leaked epoch that never releases a retired segment, the section 8.3 taxonomy
// names), so every remaining waiter takes the OOM reply. Owner goroutine only.
func (w *worker) stallCheck() {
	prog := w.st.ColdProgress()
	if prog != w.bpProg || w.st.ColdDraining() || w.st.ReclaimPending() {
		w.bpProg = prog
		w.bpStall = 0
		return
	}
	w.bpStall++
	if w.bpStall >= bpStallWindow {
		w.stallOut()
	}
}

// stallOut surfaces the OOM reply to every remaining parked write and releases
// their batches, the terminal answer when no drain progress is possible. The
// message is store.ErrFull's own text with the stall taxonomy's cause appended in
// parentheses (store.StallReason, doc 06 section 8.3): the same out-of-memory
// class a client already handles as a refusal, now carrying why the migrator
// could not free room (a full cold device, a cold I/O error, a stream pinning
// migration, no tier, or an exhausted arena). It never acknowledges a write and
// then drops it: a parked write ends in exactly one of a real reply after a drain
// or this OOM reply after a genuine stall. Every waiter today parked as
// ParkResident, whose doc 04 section 6 stall reply is exactly this f3 string;
// the flushlag reply ("ERR store: flush stalled") and the lease resolution (the
// doc 07 MOVED redirect, not an error) land with the slices that first raise
// those reasons. Owner goroutine only.
func (w *worker) stallOut() {
	msg := "ERR " + store.ErrFull.Error() + " (" + w.st.StallReason() + ")"
	for _, fw := range w.fullWaiters {
		r := Reply{b: fw.b, i: fw.i}
		if fw.b.fan(fw.i) != nil {
			// A fan sub-command returns its outcome as a partial the coordinator
			// folds (fan.go mergeFan), not a framed reply: write the stall text as a
			// FanOK error partial so the gather frames it once into the MSET's
			// reply. A framed Err here would be double-framed by AppendErrorBytes.
			r.FanErrString(msg)
		} else {
			r.Err(msg)
		}
		w.releaseHold(fw.b)
		w.bpStalls++
		w.bpReasonStalls[fw.reason]++
	}
	for i := range w.fullWaiters {
		w.fullWaiters[i] = fullWaiter{}
	}
	w.fullWaiters = w.fullWaiters[:0]
	w.bpStall = 0
}

// releaseHold drops one hold a parked write placed on its batch and pushes the
// node when the count reaches zero, the same defer-count release runDeferred
// runs. A retry that resolved a waiter (real reply or OOM) calls it once; the
// node goes back on its connection's outbound queue with the waiter's reply and
// its batch-mates in sequence order. Owner goroutine only.
func (w *worker) releaseHold(b *hopBatch) {
	b.deferN--
	if b.deferN == 0 {
		if b.conn.out.push(b) {
			b.conn.wk.wake()
		}
	}
}
