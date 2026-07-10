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
}

func newWorker(id int, st *store.Store) *worker {
	w := &worker{id: id, st: st, done: make(chan struct{})}
	w.cx.St = st
	w.argv = make([][]byte, 0, 16)
	w.wakes = make([]*Conn, 0, drainPassCap)
	w.wk.init()
	w.ep.init()
	w.inbound.init()
	return w
}

// run is the owner loop (doc 03 section 3.1, the M0 subset: batches and idle;
// intents, timers, parked resumptions, and the background slice land with
// their milestones). The thread lock is for keeps: the worker owns its thread
// so cache residency and the future core pinning mean something.
func (w *worker) run() {
	runtime.LockOSThread()
	defer close(w.done)
	for {
		n := w.drainPass()
		if len(w.streams) > 0 {
			w.pumpStreams()
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
			w.idle()
		}
	}
}

// maybeCompact is the owner-scheduled value-log compaction trigger: it runs
// only at the idle boundary (the queue is drained and no streams are in
// flight, so no ChunkStream snapshot can name the bytes it moves), and only
// when the dead share of the log is worth the rewrite: at least
// compactMinDead bytes dead and at least half the log. A failed compaction
// leaves the store on its original log by CompactLog's contract, and the
// same trigger simply fires again once more bytes die.
func (w *worker) maybeCompact() {
	total, dead := w.st.LogBytes()
	if dead >= compactMinDead && dead*2 >= total {
		_ = w.st.CompactLog()
	}
}

// pumpStreams runs one producer pass over the in-flight streams, dropping the
// ones that finished or failed. Compaction is in place; order among streams
// does not matter, each has its own ring.
func (w *worker) pumpStreams() {
	live := w.streams[:0]
	for _, st := range w.streams {
		if !st.pump() {
			live = append(live, st)
		}
	}
	for i := len(live); i < len(w.streams); i++ {
		w.streams[i] = nil
	}
	w.streams = live
}

// abortStreams fails every in-flight stream on shutdown so a consumer blocked
// on an empty ring unwinds instead of waiting on a producer that exited.
func (w *worker) abortStreams() {
	for i, st := range w.streams {
		st.failed.Store(true)
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
func (w *worker) wakeConns() {
	for i, c := range w.wakes {
		c.wk.wake()
		w.wakes[i] = nil
	}
	w.wakes = w.wakes[:0]
}

// execute runs one command through the registered handler table. OpError is
// the one shard builtin: it echoes the message the dispatcher routed, keeping
// parse-side errors in pipeline order.
func (w *worker) execute(b *hopBatch, i int) {
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
	h(&w.cx, argv, r)
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
	// roughly spinWindow (see spinIters).
	for i := 0; i < spinIters; i++ {
		if w.inbound.ready() || w.stop.Load() {
			w.wk.state.Store(stateRunning)
			return
		}
	}
	w.wk.state.Store(stateParked)
	if w.inbound.ready() || w.stop.Load() {
		w.wk.unparkSelf()
		return
	}
	w.wk.park()
}
