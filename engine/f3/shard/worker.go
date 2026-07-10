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
}

func newWorker(id int, st *store.Store) *worker {
	w := &worker{id: id, st: st, done: make(chan struct{})}
	w.cx.St = st
	w.argv = make([][]byte, 0, 16)
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
		n := w.drainAndExecute()
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
			w.idle()
		}
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

// drainAndExecute pops one batch and runs it as a unit: prefetch, execute,
// flush, all inside one epoch bracket (doc 03 sections 3.3 to 3.5). Bounded
// per call so the loop returns to its queue at bounded intervals.
func (w *worker) drainAndExecute() int {
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
	// one atomic push, and the writer is woken by the section 9.1 rule.
	c := b.conn
	c.out.push(b)
	c.wk.wake()
	return n
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
	deadline := time.Now().Add(spinWindow)
	for {
		if w.inbound.ready() || w.stop.Load() {
			w.wk.state.Store(stateRunning)
			return
		}
		if !time.Now().Before(deadline) {
			break
		}
	}
	w.wk.state.Store(stateParked)
	if w.inbound.ready() || w.stop.Load() {
		w.wk.unparkSelf()
		return
	}
	w.wk.park()
}
