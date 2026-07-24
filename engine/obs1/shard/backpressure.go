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

// bpFlushPollMs paces the flushlag half of the stall window on the batch
// clock: a fruitless flush-progress check only advances the counter when at
// least this many milliseconds passed since the last one. The resident check
// needs no pacing because its retry passes are driven by drain completions,
// but an idle boundary with a parked write spins retry passes at Gosched
// speed while a WAL PUT takes milliseconds, so counting raw passes would
// cross the window before a single flush could possibly land. 64 checks at
// this pace give a flush pipeline over three seconds of genuine silence
// before a parked write takes the stall reply, and any completed flush
// inside that window resets it.
const bpFlushPollMs = 50

// bpLeasePollMs paces the lease half the same way and for the same reason: a
// renewal rides a chain append, which takes milliseconds, so counting raw
// retry passes would cross the window before a single append could land.
// The lease window has its own length because doc 02 section 4.3 names its
// bound: bpLeaseStallWindow checks at this pace are the handoff-park cap,
// 5000ms of genuine renewal silence before a lease-parked write takes the
// CLUSTERDOWN reply; a renewal inside the window resets it.
const bpLeasePollMs = 50

// bpLeaseStallWindow is the doc 02 section 4.3 handoff-park cap in poll
// units: 100 checks at bpLeasePollMs is the 5000ms default.
const bpLeaseStallWindow = 100

// bpFoldKickPollMs paces the fold pressure trigger (doc 06 section 1.4): while
// resident writes are parked the worker kicks the folder to cut what it holds,
// but retry passes run at boundary speed while a cut is a mutex hop into the
// folder, so the kick fires at most once per this many milliseconds per shard.
const bpFoldKickPollMs = 50

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
	// marks holds the WAL marks the write emitted before it parked, the
	// committed prefix of a multi-key write on a strict connection. Each retry
	// is seeded with them so the marks the completing pass hands to the gather
	// cover every pair the command ever committed, not just the last pass's
	// (strict fan acks, doc 04 section 3.2). Nil on a relaxed connection.
	marks []WALMark
}

// parkOnFull registers a write that could not allocate on the shard's full-waiter
// FIFO and holds its batch by raising the batch's defer count, exactly as an
// intent-deferred command does. The node will not push until the count falls back
// to zero, so the parked slot (which produced no reply) and its batch-mates wait
// together until retryFull re-runs the command and it either allocates or stalls
// out. Owner goroutine only.
func (w *worker) parkOnFull(b *hopBatch, i int) {
	fw := fullWaiter{b: b, i: i, resume: w.cx.resume, reason: w.cx.parkReason}
	if len(w.cx.marks) != 0 {
		fw.marks = append([]WALMark(nil), w.cx.marks...)
	}
	w.fullWaiters = append(w.fullWaiters, fw)
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
		// Seed the marks the earlier passes captured: noteMark coalesces the
		// pairs this pass commits on top of them, so a completing pass hands
		// the union to the hold, never just its own suffix.
		w.cx.marks = append(w.cx.marks[:0], fw.marks...)
		w.executeCmd(fw.b, fw.i)
		if w.cx.parkFull {
			// Still full: keep the waiter, its batch stays held. A multi-key write
			// that committed more pairs before re-parking advanced Ctx.resume and
			// grew the mark set, so carry both forward and the next retry resumes
			// past the pairs this pass just wrote. The reason travels too: a write
			// that parked on flushlag can re-park on resident once the lag clears
			// and its handler finally runs into a full arena (or the other way
			// around), and the stall accounting must count it where it waits now.
			fw.resume = w.cx.resume
			fw.reason = w.cx.parkReason
			fw.marks = append(fw.marks[:0], w.cx.marks...)
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
	w.cx.marks = w.cx.marks[:0]
	w.ep.exit()
	if len(w.fullWaiters) == 0 {
		w.bpStall = 0
		w.bpFlushStall = 0
		w.bpFlushCheckMs = 0
		w.bpLeaseStall = 0
		w.bpLeaseCheckMs = 0
		return
	}
	w.stallCheck()
}

// stallCheck advances the stall bounds after a retry pass left writes parked,
// one window per park reason (doc 04 section 6), each with its own progress
// signal, so a flushlag storm can never stall out a resident waiter or the
// other way around.
//
// For resident waiters any of four signals is progress and resets the
// counter: the cold cursor advanced since the last pass, a drain is in flight
// or pending (ColdDraining), the migrator has cold space queued to return
// to the arena (ReclaimPending, segments a flip emptied but the epoch has not
// freed yet), or the fold cursor advanced (SetFoldProgress, the doc 04
// section 6 signal for the obs1 tier where only folded records are
// evictable). While the resident window runs, the fold pressure trigger
// (SetFoldKick) fires at most once per bpFoldKickPollMs so the folder cuts
// what it holds instead of waiting for size or age, which is doc 06 section
// 1.4's pressure promotion. The last one closes the window slice 5a's cold-cursor-only
// check left open: after a drain moves records cold and stops, the arena
// still needs a few boundaries to hand the emptied segments back through the
// epoch, and during those the cold tail is static and no drain is in flight,
// so a retry loop would wrongly count them as stalls and OOM a write whose
// room was one reclaim pass away. Only when none of the three holds does the
// counter climb, and crossing the window means the arena truly cannot free
// room (disk full, an I/O error, no migratable residue, or a leaked epoch
// that never releases a retired segment, the section 8.3 taxonomy names), so
// every resident waiter takes the OOM reply.
//
// For flushlag waiters progress is a completed WAL flush (FlushCount moved),
// and a flushlag waiter still parked after a retry pass implies the lag flag
// was up during the pass, because a clear flag would have run its handler and
// resolved it. The counter is paced by bpFlushPollMs (see the constant) and
// crossing the window means flushing has genuinely stopped, the bucket
// refusing PUTs past the retry backoff, so every flushlag waiter takes the
// flush-stalled reply.
//
// For lease waiters progress is a renewal (LeaseView.Renewals moved), which
// is a successful chain append of the holder's own, and demotion resolves the
// waiter through the retry itself: the gate re-runs each pass and a demoted
// group writes the MOVED redirect instead of re-parking, so the window here
// only covers the genuinely unleased case. The counter is paced by
// bpLeasePollMs and crossing the window is the doc 07 handoff-park cap: the
// slot's group is not served and no renewal is landing, so every lease waiter
// takes the CLUSTERDOWN reply. A reason with no waiters gets its counter
// reset so a later park starts a fresh window. Owner goroutine only.
func (w *worker) stallCheck() {
	var resident, flushlag, lease bool
	for i := range w.fullWaiters {
		switch w.fullWaiters[i].reason {
		case ParkFlushlag:
			flushlag = true
		case ParkLease:
			lease = true
		default:
			resident = true
		}
	}
	if !resident {
		w.bpStall = 0
	} else {
		if w.foldKick != nil && w.cx.NowMs-w.bpKickMs >= bpFoldKickPollMs {
			w.bpKickMs = w.cx.NowMs
			w.foldKick()
		}
		prog := w.st.ColdProgress()
		var fold uint64
		if w.foldProg != nil {
			fold = w.foldProg()
		}
		if prog != w.bpProg || fold != w.bpFoldSeen || w.st.ColdDraining() || w.st.ReclaimPending() {
			w.bpProg = prog
			w.bpFoldSeen = fold
			w.bpStall = 0
		} else if w.bpStall++; w.bpStall >= bpStallWindow {
			w.stallOutReason(ParkResident, "ERR "+store.ErrFull.Error()+" ("+w.st.StallReason()+")")
			w.bpStall = 0
		}
	}
	w.stallCheckFlush(flushlag)
	w.stallCheckLease(lease)
}

// stallCheckFlush advances the flushlag window, the paced half stallCheck
// split out when the lease window joined it. Owner goroutine only.
func (w *worker) stallCheckFlush(flushlag bool) {
	if !flushlag {
		w.bpFlushStall = 0
		w.bpFlushCheckMs = 0
		return
	}
	if fc := w.cx.Log.FlushCount(); fc != w.bpFlushSeen {
		w.bpFlushSeen = fc
		w.bpFlushStall = 0
		w.bpFlushCheckMs = w.cx.NowMs
		return
	}
	if w.cx.NowMs-w.bpFlushCheckMs < bpFlushPollMs {
		return
	}
	w.bpFlushCheckMs = w.cx.NowMs
	w.bpFlushStall++
	if w.bpFlushStall >= bpStallWindow {
		w.stallOutReason(ParkFlushlag, "ERR store: flush stalled")
		w.bpFlushStall = 0
	}
}

// stallCheckLease advances the lease window, same shape as the flushlag one
// with the renewal count as its progress signal. The stall reply is the
// doc 07 line for a slot whose group is unleased past the handoff-park cap;
// the Reply.Err framing adds the leading dash, so the string carries none.
// Owner goroutine only.
func (w *worker) stallCheckLease(lease bool) {
	if !lease {
		w.bpLeaseStall = 0
		w.bpLeaseCheckMs = 0
		return
	}
	if rn := w.leases.Renewals(); rn != w.bpLeaseSeen {
		w.bpLeaseSeen = rn
		w.bpLeaseStall = 0
		w.bpLeaseCheckMs = w.cx.NowMs
		return
	}
	if w.cx.NowMs-w.bpLeaseCheckMs < bpLeasePollMs {
		return
	}
	w.bpLeaseCheckMs = w.cx.NowMs
	w.bpLeaseStall++
	if w.bpLeaseStall >= bpLeaseStallWindow {
		w.stallOutReason(ParkLease, "CLUSTERDOWN Hash slot not served")
		w.bpLeaseStall = 0
	}
}

// stallOutReason surfaces msg to every parked write waiting under reason and
// releases their batches, the terminal answer when that reason's progress has
// genuinely stopped; waiters parked under any other reason stay in the FIFO
// with their own window still running. For resident the message is
// store.ErrFull's own text with the stall taxonomy's cause appended in
// parentheses (store.StallReason, doc 06 section 8.3): the same out-of-memory
// class a client already handles as a refusal, now carrying why the migrator
// could not free room (a full cold device, a cold I/O error, a stream pinning
// migration, no tier, or an exhausted arena). For flushlag it is the doc 04
// section 6 flush-stalled string; for lease it is the doc 07 CLUSTERDOWN
// line, and only for the unleased case: a demotion never reaches here because
// the retrying gate resolves the waiter with the MOVED redirect instead of
// re-parking it. It never acknowledges a write and then drops it: a parked
// write ends in exactly one of a real reply after its reason's progress
// resumes, the redirect after a demotion, or this reply after a genuine
// stall. Owner goroutine only.
func (w *worker) stallOutReason(reason ParkReason, msg string) {
	kept := w.fullWaiters[:0]
	for _, fw := range w.fullWaiters {
		if fw.reason != reason {
			kept = append(kept, fw)
			continue
		}
		r := Reply{b: fw.b, i: fw.i}
		if fw.b.fan(fw.i) != nil {
			// A fan sub-command returns its outcome as a partial the coordinator
			// folds (fan.go mergeFan), not a framed reply: write the stall text as a
			// FanOK error partial so the gather frames it once into the MSET's
			// reply. A framed Err here would be double-framed by AppendErrorBytes.
			// The waiter's marks still ride: the pairs behind them committed and
			// framed, so even this error reply holds until they are covered.
			r.FanErrString(msg)
			if len(fw.marks) != 0 {
				fw.b.setFanMarks(fw.i, fw.marks)
			}
		} else {
			// A point write parks before it emits anything (a single pair either
			// allocates or parks whole), so a stalled point waiter never carries
			// marks and the error takes the slot at once.
			r.Err(msg)
		}
		w.releaseHold(fw.b)
		w.bpStalls++
		w.bpReasonStalls[fw.reason]++
	}
	for i := len(kept); i < len(w.fullWaiters); i++ {
		w.fullWaiters[i] = fullWaiter{}
	}
	w.fullWaiters = kept
}

// residentParked reports whether any parked write waits under the resident
// reason. drainCold keys its deeper backpressure trigger on it rather than on
// the FIFO being non-empty: a flushlag waiter is waiting on the flusher, not
// the arena, and draining resident records toward an empty arena would evict
// hot data for nothing. Owner goroutine only; the FIFO is small (bounded by
// parked commands), so the scan is cheap and only runs while something is
// parked.
func (w *worker) residentParked() bool {
	for i := range w.fullWaiters {
		if w.fullWaiters[i].reason == ParkResident {
			return true
		}
	}
	return false
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
