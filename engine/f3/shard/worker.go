package shard

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

// worker owns one shard: its store, its inbound queue, its epoch. Exactly one
// goroutine runs the loop, locked to an OS thread, and nothing else ever
// touches the store; every plain load and store below leans on that.
type worker struct {
	id      int
	st      *store.Store
	inbound mpsc
	wk      waker
	ep      epoch
	stop    atomic.Bool
	done    chan struct{}

	// The F17 tier-two intent path (intent.go): intentInbox carries the
	// coordinator's control ops, intentPending gates the drain so a shard with
	// no tier-two traffic pays one relaxed load per pass, and keyQ holds the
	// owner-only per-key intent queues. All three are untouched by the
	// single-key fast path.
	intentInbox   opQueue
	intentPending atomic.Int64
	keyQ          map[string]*keyList

	// timers is the owner-only deadline heap (timer.go): a blocking command with
	// a finite timeout arms one so its timeout reply fires on this shard even if
	// no serving push arrives. It is empty on the throughput hot path, so
	// fireTimers and the idle park both gate on w.timers.len(), one relaxed load
	// per pass exactly like intentPending and donated. timerDue is the reused
	// scratch fireTimers pops into so a firing pass allocates nothing, and
	// parkTimer is the single reusable *time.Timer the idle timed park selects
	// over; both stay nil until the first blocking command actually uses them.
	timers    timerHeap
	timerDue  []*timer
	parkTimer *time.Timer

	// rt is the runtime this worker belongs to, fixed before Start: the intent
	// coordinator and the donation offer walk the pool through it.
	rt *Runtime

	// donated is the worker-donation slot (donate.go): a coordinator fanning
	// out read-only tasks CASes a job in here and the worker helps between its
	// own batches. One relaxed load per pass when empty, like intentPending.
	donated atomic.Pointer[donateJob]

	// pin locks the worker goroutine to an OS thread for its whole life.
	// Correctness never needs it: the single-owner invariant is goroutine
	// affinity, one goroutine owning the shard, and that holds wherever the
	// scheduler runs it. The lock only ever bought cache residency, and it
	// charges every park and unpark the handoffp/startlockedm double
	// cond_signal (issue #542's wake tax); the labs/f3/m0/11_transport sweep
	// measured unpinned as winning or tying every cell. Default off; Config
	// exposes it for boxes where thread residency measurably pays.
	pin bool

	// handlers is the op-indexed table Runtime.Use registered, fixed before
	// Start. The worker looks handlers up by op byte and never interprets one.
	handlers []Handler

	// cx is the worker's handler context, one per shard for its whole life:
	// the store, the per-batch clock, and the value scratch whose grown
	// capacity carries across commands so the steady path allocates nothing.
	cx Ctx

	// argv is the reused argument-view slice handed to handlers.
	argv [][]byte

	// sink absorbs the prefetch touch loads so the compiler cannot treat the
	// stage-one loop as dead.
	sink uint64

	// streams are the in-flight streamed replies this shard is pumping. The
	// worker keeps servicing them between batches and never parks while any
	// are live, so a consumer waiting on the ring always has a live producer.
	streams []*stream

	// wakes collects the connections whose reply queues went empty to
	// non-empty during the current drain pass, each at most once; the pass
	// delivers them at its end so a run of batches for one connection costs
	// one wake, not one per batch. Owner goroutine only.
	wakes []*Conn

	// loops collects the distinct LoopWakers behind the batch-notified
	// connections in wakes whose claim this pass won; wakeConns delivers one
	// WakeLoop to each at the end of the pass, which is the slice 4 batching
	// (one eventfd write per touched loop, not per dirty connection). Owner
	// goroutine only, like wakes.
	loops []LoopWaker

	// connWakes counts the connection writer wake tokens this worker sent
	// (wakeConns, only the sends that claimed a parked writer), and parks
	// counts the worker's real parks. Owner-goroutine single-writer counters;
	// see Runtime.NetWakes.
	connWakes eventCounter
	parks     eventCounter

	// io is the shard's off-owner I/O worker (ioworker.go): the goroutine that
	// pwrites a staged cold drain and posts the completion back to this owner.
	// It starts on the first submit and stays dark until the M7 migrator drains
	// a segment, so a store under its resident cap never pays for it.
	io ioworker

	// The block-not-drop backpressure state (backpressure.go, spec 2064/f3/06
	// section 8). fullWaiters is the per-shard FIFO of writes parked on a full
	// arena, each holding its batch through the batch's defer count until a drain
	// frees room; it is nil and untouched until the first write parks, so a store
	// under its resident cap never allocates it. bpProg snapshots the cold append
	// cursor between retry passes and bpStall counts consecutive passes without an
	// advance, the coarse stall bound that surfaces the OOM reply when no progress
	// is possible. bpWaits and bpStalls are the cumulative park and stall-out
	// counts INFO surfaces in slice 5b.
	fullWaiters []fullWaiter
	bpProg      uint64
	bpStall     int
	bpWaits     uint64
	bpStalls    uint64
}

func newWorker(id int, st *store.Store) *worker {
	w := &worker{id: id, st: st, done: make(chan struct{})}
	w.cx.St = st
	w.cx.w = w
	w.argv = make([][]byte, 0, 16)
	w.wakes = make([]*Conn, 0, drainPassCap)
	w.keyQ = make(map[string]*keyList)
	w.wk.init()
	w.ep.init()
	w.inbound.init()
	w.intentInbox.init()
	w.io.init(w)
	return w
}

// run is the owner loop (doc 03 section 3.1, the M0 subset: batches and idle;
// intents, timers, parked resumptions, and the background slice land with
// their milestones). When pin is set the thread lock is for keeps: the worker
// owns its thread so cache residency and core pinning mean something.
func (w *worker) run() {
	if w.pin {
		runtime.LockOSThread()
	}
	defer close(w.done)
	for {
		n := w.drainPass()
		// Fire any deadlines that came due (timer.go): near-free when no timer is
		// armed, one relaxed length check per pass exactly like the intent gate
		// below. A pass that fired timers counts them so the loop makes another
		// pass in case more are due past the batch cap.
		n += w.fireTimers()
		// Advance the tier-two intent queues (doc 03 section 6): near-free when
		// no tier-two traffic is in flight, one relaxed load per pass. A pass
		// that applied ops counts them so the loop stays awake to service the
		// coordinator that posted them.
		n += w.advanceIntents()
		// Help a donated fan-out (donate.go): one relaxed load when no job is
		// offered, and between-batches-only execution when one is, which is the
		// section-6.5 fairness bound.
		n += w.helpDonated()
		if len(w.streams) > 0 {
			w.pumpStreams()
		}
		if n > 0 && len(w.streams) == 0 {
			// Sustained writes can walk the arena to its full state without
			// the queue ever draining, which is how the M0 gate died at 4KiB
			// (issue #542): the idle trigger below never fired. The tight
			// check is O(1), and the boundary between two drain passes holds
			// no arena address (views die with their command, streams are
			// checked), so a compaction here is as safe as one at idle.
			// Residency demotion shares the boundary and the safety argument:
			// past the resident cap, cold value runs move to the log here,
			// and the compaction that follows turns the bytes they and the
			// tight state left dead into freed segments whose pages go back
			// to the OS, which is what keeps the cap an RSS bound and not
			// just a placement rule.
			// MaybeDemote moves cold separated runs to the log; drainCold is the
			// whole-record valve that follows it, staging int and embedded records
			// the run demotion cannot touch for the off-owner cold write when the
			// resident set is still over the mark. The demotion has side effects and
			// must run every boundary, so it lands in a local before the compaction
			// decision rather than short-circuiting inside it. The cold drain frees
			// no arena bytes here (its flip lands on a later completion boundary), so
			// the compaction decision leans on the arena-pressure checks, which hold
			// while the staged records still charge the arena.
			demoted := w.st.MaybeDemote() > 0
			w.drainCold()
			// Park the segments this boundary's drains emptied on the epoch
			// retire list before the compactor runs, so it leaves them alone
			// (a retired segment is skipped) and their bytes outlive any bracket
			// in flight rather than freeing outright.
			w.retireDrained()
			if demoted || w.st.ArenaTight() || w.st.ResidentOver() {
				w.st.CompactArena()
			}
			// Return any epoch-retired segments this boundary's exited bracket
			// has cleared (M7 reclamation): the pass above exited its batch
			// bracket, so safe reflects it. A length check until the migrator
			// retires its first segment; wired from the first batch so the
			// contract runs before a real reader depends on it.
			w.st.ReclaimSafe(w.ep.safe())
			// Retry any writes parked on a full arena against the space this
			// boundary just reclaimed (backpressure.go). No-op with no waiter, one
			// length check on the no-pressure path (L9).
			w.retryFull()
		}
		if n == 0 {
			if w.stop.Load() {
				w.abortStreams()
				return
			}
			if len(w.streams) > 0 {
				// Streams in flight: stay live for the pump, yield instead of
				// parking so the consumers cannot wait on a parked producer.
				runtime.Gosched()
				continue
			}
			w.maybeCompact()
			w.runMaintainer()
			if len(w.fullWaiters) > 0 && !w.st.ColdDraining() {
				// Writes are parked but no drain is in flight or pending, so no
				// completion will wake the worker to retry them: spin the boundary
				// toward the stall window instead of parking (maybeCompact above
				// advanced the stall counter, and it fires the OOM reply once the
				// window is crossed). A drain in flight (ColdDraining) posts a
				// completion that wakes a parked worker, so that case parks below.
				runtime.Gosched()
				continue
			}
			w.idle()
		}
	}
}

// maybeCompact is the owner-scheduled compaction trigger for both dead-byte
// pools: it runs only at the idle boundary (the queue is drained and no
// streams are in flight, so no ChunkStream snapshot can name the bytes it
// moves). The value log rewrites when at least compactMinDead bytes are dead
// and they are at least half the log; a failed compaction leaves the store
// on its original log by CompactLog's contract, and the same trigger simply
// fires again once more bytes die. The arena compacts when its
// victim-eligible segments hold at least arenaCompactMinDead dead bytes, the
// same worth-the-pass floor.
func (w *worker) maybeCompact() {
	total, dead := w.st.LogBytes()
	if dead >= compactMinDead && dead*2 >= total {
		_ = w.st.CompactLog()
	}
	// Demote before the arena trigger reads the dead figures: a demotion pass
	// creates exactly the dead bytes the compaction that follows reclaims.
	demoted := w.st.MaybeDemote()
	// Retire any segments a prior boundary's drains emptied before the compactor
	// runs, same as the active-boundary path: the compactor skips a retired
	// segment, so this hands the drained ones to the epoch path instead.
	w.retireDrained()
	if demoted > 0 || w.st.ArenaReclaimable() >= arenaCompactMinDead {
		w.st.CompactArena()
	}
	// The idle-boundary half of M7 reclamation: the queue is drained and the
	// bracket long exited, so safe has advanced past any bracket that could
	// have named a retired segment. Empty-list cheap until the migrator lands.
	w.st.ReclaimSafe(w.ep.safe())
	// Retry any writes parked on a full arena at the idle boundary too, so a
	// shard that quiesced with a completion still in flight serves its waiters
	// once the drain lands. No-op with no waiter (backpressure.go).
	w.retryFull()
}

// drainCold stages a two-phase cold migration and hands it to the I/O worker
// (ioworker.go), the off-owner half of the M7 migrator. Phase 1 runs here on the
// owner: it checks a staging buffer out of the cap/4 pool and asks the store to
// frame a run of cold-bound records into it and reserve their cold-region span.
// The buffer and its offset go to the I/O worker, which pwrites them off the
// owner and posts a completion where phase 2 flips the slots. A store with no
// cold region or no pressure stages nothing and returns the buffer at once, so a
// shard under its resident cap never starts the goroutine. When the pool is at
// its in-flight bound the drain defers to a later boundary, the admission control
// on per-shard cold I/O concurrency.
func (w *worker) drainCold() {
	if !w.st.NeedsColdDrain() {
		return
	}
	// Wire the pwrite seam to the store's cold region on first use only. The
	// assignment must precede the first submit (which starts the goroutine), and
	// never repeat once the goroutine is running and reading the seam, or the two
	// race; the nil check keeps it to exactly one owner-side store before the
	// goroutine exists, which the go statement in submit publishes.
	if w.io.write == nil {
		w.io.write = w.st.ColdWriteAt
	}
	buf := w.io.pool.get()
	if buf == nil {
		return
	}
	d := w.st.StageColdDrain(buf)
	if d == nil || len(d.Buf()) == 0 {
		w.io.pool.put(buf)
		return
	}
	w.io.submit(ioJob{
		buf: d.Buf(),
		off: d.Off(),
		onDone: func(cx *Ctx, res ioResult) {
			w.st.CompleteColdDrain(d, res.err == nil)
		},
	})
}

// retireDrained parks the segments the migrator's phase 2 emptied since the last
// boundary on the epoch retire list, the first caller of the F6 reclamation
// machinery (epoch.go, store reclaim.go). It is the "segment fully drained ->
// retire at the current batch boundary" half of the migration quantum (doc 06
// section 3.1): the store detected each empty segment on the flip that unlinked
// its last record, and this stamps them with the epoch current now and hands them
// to RetireSegment, so the compactor leaves them and ReclaimSafe frees them only
// once every bracket in flight at retirement has exited. bump both advances the
// global epoch and returns the stamp to retire under, so a segment retired here
// frees at a later boundary, never the one that emptied it while a batch that read
// its bytes could still be in flight. Empty list (no drain emptied a segment) is
// one length check and no bump, so the epoch clock only moves when a segment
// actually retires.
//
// This covers only the segments the migrator itself evacuates whole. The
// compactor's relocation empties segments too and still frees them outright; a
// follow-on slice routes that path through the epoch to close reader-safety for
// it. Until then the store is reader-safe for the migrator-emptied case, which is
// the one this slice's own machinery produces.
func (w *worker) retireDrained() {
	dead := w.st.TakeColdDrained()
	if len(dead) == 0 {
		return
	}
	stamp := w.ep.bump()
	for _, si := range dead {
		w.st.RetireSegment(si, stamp)
	}
}

// pumpStreams runs one producer pass over the in-flight streams, dropping the
// ones that finished or failed. Compaction is in place; order among streams
// does not matter, each has its own ring.
//
// A pass that published chunks (or a failure) owes the connection a writer
// wake: a step-mode consumer (the reactor loop) parks between chunks instead
// of spinning on the ring, so the pump pays the same publish-then-wake the
// reply push pays, with the chunk ring's prod store as the publication and
// ParkWriter's StreamReady re-check as the other half of the proof. The claim
// CAS folds redundant deliveries, and a goroutine-driver consumer is never
// parked mid-stream (emitStream spins), so the wake there is the one
// uncontended load wake always costs.
func (w *worker) pumpStreams() {
	live := w.streams[:0]
	for _, st := range w.streams {
		before := st.prod.Load()
		done := st.pump()
		if st.prod.Load() != before || st.failed.Load() {
			st.conn.wk.wake()
		}
		if !done {
			live = append(live, st)
		}
	}
	for i := len(live); i < len(w.streams); i++ {
		w.streams[i] = nil
	}
	w.streams = live
}

// abortStreams fails every in-flight stream on shutdown so a consumer blocked
// on an empty ring unwinds instead of waiting on a producer that exited. The
// wake goes through wk.wake, which for a loop-owned connection is the
// SetWriterBatchNotify seam: the claim marks the connection dirty on its loop
// and delivers one WakeLoop, so a reactor loop parked in epoll_wait comes
// back, observes the failure through StreamReady/StreamAborted, and drops the
// connection instead of waiting forever on chunks that will never come.
func (w *worker) abortStreams() {
	for i, st := range w.streams {
		st.failed.Store(true)
		st.finish()
		st.conn.wk.wake()
		w.streams[i] = nil
	}
	w.streams = w.streams[:0]
}

// drainPass runs up to drainPassCap batches back to back, then delivers the
// deferred writer wakes, one per connection touched. The cap bounds how long
// a wake can sit deferred and keeps the loop returning to its stream pump at
// bounded intervals.
func (w *worker) drainPass() int {
	total := 0
	for i := 0; i < drainPassCap; i++ {
		n := w.executeOne()
		if n == 0 {
			break
		}
		total += n
	}
	w.wakeConns()
	return total
}

// drainAndExecute is one complete owner step, pop-execute-wake, for callers
// outside the run loop (the owner-path tests drive the shard with it). The
// run loop itself uses drainPass so wakes coalesce across batches.
func (w *worker) drainAndExecute() int {
	n := w.executeOne()
	w.wakeConns()
	return n
}

// fireTimers runs the deadlines that came due on this owner. The empty-heap
// gate is the whole hot-path cost: with no timer armed it is one plain length
// load and a return, byte-identical to a pass that never had the call. When
// timers exist it reads the wall clock here (there is no batch clock at this
// boundary, unlike executeOne's cached NowMs), pops every timer at or before
// now up to timerFireCap into the reused w.timerDue scratch, and runs each fire
// on the owner. The cap bounds how long a burst of simultaneous timeouts can
// hold the loop off command processing; a pass that hit the cap leaves the rest
// and returns a positive count, so the owner loop comes right back for them.
func (w *worker) fireTimers() int {
	if w.timers.len() == 0 {
		return 0
	}
	nowMs := time.Now().UnixMilli()
	w.timerDue = w.timers.popDue(nowMs, timerFireCap, w.timerDue[:0])
	n := len(w.timerDue)
	for _, t := range w.timerDue {
		t.fire(&w.cx)
	}
	for i := range w.timerDue {
		w.timers.release(w.timerDue[i])
		w.timerDue[i] = nil
	}
	w.timerDue = w.timerDue[:0]
	return n
}

// executeOne pops one batch and runs it as a unit: prefetch, execute, flush,
// all inside one epoch bracket (doc 03 sections 3.3 to 3.5). The writer wake
// it may owe is recorded in w.wakes, not sent; the enclosing pass delivers.
func (w *worker) executeOne() int {
	b := w.inbound.pop()
	if b == nil {
		return 0
	}
	w.ep.enter()
	n := int(b.n)

	// The batch's clock: read once, shared by every expiry comparison in the
	// batch (doc 09 section 2's cached now_ms).
	w.cx.NowMs = time.Now().UnixMilli()

	// Stage one: hash every keyed command and touch its home bucket, so the
	// probes in the execute loop run against warm lines instead of paying a
	// serialized miss each. Go has no prefetch intrinsic, so the touch is a
	// plain load folded into a sink the compiler must keep.
	touched := uint64(0)
	depth := n
	if depth > prefetchDepth {
		depth = prefetchDepth
	}
	for i := 0; i < depth; i++ {
		if b.cmds[i].keyed {
			touched += w.st.TouchBucket(store.Hash(b.arg(i, 0)))
		}
	}
	w.sink += touched

	// Stage two: execute in command order, replies into the node's reply
	// arena. The record-line prefetch stage (doc 03 stage 2) joins when the
	// store exposes probe-then-execute; buckets are the first dependent miss
	// and the one this slice hides.
	for i := 0; i < n; i++ {
		w.execute(b, i)
	}
	w.ep.exit()

	// Adopt any streamed replies before the push: after it the writer may
	// recycle the node at any moment, so the stream handles must be off it.
	if b.hasStream {
		for i := 0; i < n; i++ {
			if st := b.stream(i); st != nil {
				w.streams = append(w.streams, st)
			}
		}
	}

	// A node holding commands deferred behind queued intents (txnroute.go)
	// stays with the owner: its executed replies are written but the node
	// cannot go back until every parked command has run, and runDeferred
	// pushes it when the last one does. Streams of the executed commands were
	// adopted above; deferred slots are still nil there.
	if b.deferN > 0 {
		return n
	}

	// Flush: the whole node goes back on the connection's outbound queue with
	// one atomic push, and the writer is woken by the section 9.1 rule, with
	// two coalescing refinements. The wake-skip invariant: a wake may be
	// skipped only when the observed queue state guarantees the consumer will
	// re-check after its current pass. A push whose tail swap returned a real
	// node proves the queue held a reply node the writer has not returned;
	// the writer parks only after storing parked and re-checking out.ready(),
	// and ready() stays true from that earlier node until ours is popped, so
	// the writer cannot park past our node, and the earlier node's producer
	// already sent (or rightly skipped) the token. An empty-to-non-empty push
	// does owe a wake, but it is deferred into w.wakes so a pass that drains
	// several batches for one connection wakes it once at the end.
	c := b.conn
	if c.out.push(b) {
		w.noteWake(c)
	}
	return n
}

// noteWake records that c is owed a writer wake at the end of the current
// drain pass, deduplicated so each connection is woken at most once per pass.
// The scan is linear over at most drainPassCap entries.
func (w *worker) noteWake(c *Conn) {
	for _, x := range w.wakes {
		if x == c {
			return
		}
	}
	w.wakes = append(w.wakes, c)
}

// wakeConns delivers the wakes the pass deferred and clears the list. The
// wake happens after every push of the pass, so the section 9.1 publish-then-
// load order holds for each of them.
//
// A batch-notified connection (SetWriterBatchNotify) splits its wake in two:
// the per-connection claim happens here (the same waker CAS, so the counters
// and the exactly-once rule are unchanged), but the delivery is deferred once
// more, to the loop level: each claim marks its connection dirty on its loop,
// and the pass ends with one WakeLoop per distinct loop touched. Between the
// claim and that WakeLoop the pass runs only the straight-line loop below, so
// the delivery every claim owes arrives within a bounded number of plain
// instructions; nothing here can block or park with a claim outstanding.
func (w *worker) wakeConns() {
	sent := uint64(0)
	for i, c := range w.wakes {
		if c.batchMark != nil {
			if c.wk.claim() {
				sent++
				if c.batchMark() {
					w.noteLoop(c.batchLoop)
				}
			}
		} else if c.wk.wake() {
			sent++
		}
		w.wakes[i] = nil
	}
	w.wakes = w.wakes[:0]
	for i, l := range w.loops {
		l.WakeLoop()
		w.loops[i] = nil
	}
	w.loops = w.loops[:0]
	if sent > 0 {
		w.connWakes.add(sent)
	}
}

// noteLoop records that l is owed a pass-end WakeLoop, deduplicated like
// noteWake: the scan is linear over the loops the pass has touched, at most
// one entry per event loop behind the connections in wakes.
func (w *worker) noteLoop(l LoopWaker) {
	for _, x := range w.loops {
		if x == l {
			return
		}
	}
	w.loops = append(w.loops, l)
}

// execute runs one command, after the two tier-two gates in front of the
// handler table: OpTxnArm enqueues its intent in inbound order (txnroute.go),
// and a command touching a key with queued intents parks until they release.
// Every command checks, keyed or not: DoAt-routed verbs (OBJECT, SINTERCARD)
// and multi-key reads carry keys past argument zero, and a transaction's
// exclusion has to cover them all (doc 03 section 6.7). A shard with no
// queued intents pays one map length check per command here and nothing else.
func (w *worker) execute(b *hopBatch, i int) {
	c := &b.cmds[i]
	if c.op == OpTxnArm {
		w.armIntent(b, i)
		return
	}
	if len(w.keyQ) != 0 && w.deferForIntent(b, i) {
		return
	}
	w.executeCmd(b, i)
}

// executeCmd runs one command through the registered handler table, with no
// tier-two gates: execute calls it on the drain path and runDeferred calls it
// when a parked command's awaited intents have released (re-checking would
// re-park it behind transactions that armed after it arrived). OpError is the
// one builtin here: it echoes the message the dispatcher routed, keeping
// parse-side errors in pipeline order.
func (w *worker) executeCmd(b *hopBatch, i int) {
	c := &b.cmds[i]
	r := Reply{b: b, i: i}
	if c.op == OpError {
		r.errBytes(b.arg(i, 0))
		return
	}
	var h Handler
	if int(c.op) < len(w.handlers) {
		h = w.handlers[c.op]
	}
	if h == nil {
		r.Err("ERR unknown op")
		return
	}
	argv := w.argv[:0]
	for k := 0; k < int(c.argn); k++ {
		argv = append(argv, b.arg(i, k))
	}
	w.argv = argv
	w.cx.curConn = b.conn // completion target for a Park; owner-local, per command
	w.cx.curSeq = c.seq
	h(&w.cx, argv, r)
	// Block-not-drop: a write handler that could not allocate set parkFull through
	// ParkFull instead of writing a reply. One bool load, zero cost when unset; on
	// the normal path it registers the command on the full-waiter FIFO, and the
	// retry driver clears retrying so a re-park during a retry is read there rather
	// than double-registered here (backpressure.go).
	if w.cx.parkFull && !w.cx.retrying {
		w.cx.parkFull = false
		w.parkOnFull(b, i)
	}
}

// idle is the spin-then-park protocol (doc 03 section 9.1): store spinning,
// burn the window on plain queue checks, store parked, re-check, block. The
// re-check after the parked store is the lost-wake guard: a producer's push
// happens before its state load, so either this check sees the node or the
// producer sees parked and sends the token.
func (w *worker) idle() {
	w.wk.state.Store(stateSpinning)
	// The window is counted in calibrated iterations, not clock reads: a
	// time.Now per turn was measurable CPU and the window only needs to be
	// roughly workerSpinWindow (see workerSpinIters). The worker's window is
	// its own, swept apart from the connection writers' in lab 11; at the
	// frozen value of zero the loop is skipped and the worker parks at once.
	for i := 0; i < workerSpinIters; i++ {
		if w.inbound.ready() || w.intentReady() || w.donateReady() || w.stop.Load() {
			w.wk.state.Store(stateRunning)
			return
		}
	}
	w.wk.state.Store(stateParked)
	// The re-check after the parked store is the lost-wake guard, and it covers
	// the intent queue and the donation slot too: an intent op or a donated job
	// posted before its wake either shows in this check or the posting
	// producer's wake claims the park (postIntent and FanOut wake exactly like
	// a hop producer).
	if w.inbound.ready() || w.intentReady() || w.donateReady() || w.stop.Load() {
		w.wk.unparkSelf()
		return
	}
	// The no-timer path is byte-identical to before: with an empty heap this is
	// one plain length load and then the same plain park. A worker only has a
	// timer armed while a blocking command with a finite timeout is parked on
	// it, which never happens on the throughput path, so the timed branch below
	// is off everywhere that matters for P1.
	if w.timers.len() > 0 {
		w.timedPark()
		return
	}
	w.parks.bump()
	w.wk.park()
}

// timedPark blocks until the nearest armed deadline or a producer wake,
// whichever comes first, entered only from idle with state already stateParked.
// It is the subtle half of the timer wiring, so the waker-state contract is
// spelled out here in full.
//
// If the nearest deadline is already at or past now, it does not park at all:
// it puts the worker back to running and returns, and the next loop pass runs
// fireTimers on the due timer. Otherwise it selects over the waker channel and
// a single reusable *time.Timer (never a fresh timer per park), reset with the
// drain-before-reset idiom so a stale fire from a prior park cannot leak in.
//
// The two branches restore the waker state differently, because wk.park's proof
// (a producer CAS'd stateParked->stateRunning before it sent the token) holds on
// only one of them:
//   - The channel branch is exactly wk.park: a producer already claimed the wake
//     and moved the state to running before the token we just received, so there
//     is nothing to restore, only the pending timer to stop and drain.
//   - The timer branch had no producer touch the state, so it is still
//     stateParked and this worker must move it back itself. unparkSelf does
//     exactly that and also closes the race where a producer's claim landed
//     between the timer firing and here: its CAS fails, and because a claim is
//     always followed by a delivery (waker.go), the token is on its way and
//     unparkSelf consumes it, so no wake is lost and no stale token survives to
//     satisfy a later park.
func (w *worker) timedPark() {
	deadlineMs, ok := w.timers.peekDeadline()
	if !ok {
		w.parks.bump()
		w.wk.park()
		return
	}
	d := time.Duration(deadlineMs-time.Now().UnixMilli()) * time.Millisecond
	if d <= 0 {
		// Already due: do not park, hand the state back and let fireTimers take it
		// on the next pass. unparkSelf restores running and drains a racing token.
		w.wk.unparkSelf()
		return
	}
	if w.parkTimer == nil {
		w.parkTimer = time.NewTimer(d)
	} else {
		if !w.parkTimer.Stop() {
			select {
			case <-w.parkTimer.C:
			default:
			}
		}
		w.parkTimer.Reset(d)
	}
	w.parks.bump()
	select {
	case <-w.wk.ch:
		// A producer claimed the park and sent the token, the wk.park case: the
		// state is already running. Cancel the timer and drain it if it fired in
		// the meantime so the next reset starts clean.
		if !w.parkTimer.Stop() {
			select {
			case <-w.parkTimer.C:
			default:
			}
		}
	case <-w.parkTimer.C:
		// The deadline fired and no producer necessarily touched the state, so
		// take the worker out of parked ourselves, consuming a racing producer's
		// token if one is in flight.
		w.wk.unparkSelf()
	}
}
