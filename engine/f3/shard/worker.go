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

	// val is the worker's value scratch for GET: the store copies into it and
	// the reply builder copies out, so the buffer's grown capacity is reused
	// and the steady path allocates nothing.
	val []byte

	// sink absorbs the prefetch touch loads so the compiler cannot treat the
	// stage-one loop as dead.
	sink uint64
}

func newWorker(id int, st *store.Store) *worker {
	w := &worker{id: id, st: st, done: make(chan struct{})}
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
		if w.drainAndExecute() == 0 {
			if w.stop.Load() {
				return
			}
			w.idle()
		}
	}
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
		if b.cmds[i].klen != 0 {
			touched += w.st.TouchBucket(store.Hash(b.key(i)))
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

	// Flush: the whole node goes back on the connection's outbound queue with
	// one atomic push, and the writer is woken by the section 9.1 rule.
	c := b.conn
	c.out.push(b)
	c.wk.wake()
	return n
}

func (w *worker) execute(b *hopBatch, i int) {
	switch b.cmds[i].op {
	case OpPing:
		if b.cmds[i].alen == 0 {
			b.replyStatic(i, "+PONG\r\n")
		} else {
			b.replyBulk(i, b.arg(i))
		}
	case OpEcho:
		b.replyBulk(i, b.arg(i))
	case OpGet:
		v, ok := w.st.Get(b.key(i), w.val)
		w.val = v[:0]
		if ok {
			b.replyBulk(i, v)
		} else {
			b.replyStatic(i, "$-1\r\n")
		}
	case OpSet:
		if err := w.st.Set(b.key(i), b.arg(i)); err != nil {
			b.replyError(i, []byte(err.Error()))
		} else {
			b.replyStatic(i, "+OK\r\n")
		}
	case OpError:
		b.replyError(i, b.arg(i))
	default:
		b.replyError(i, []byte("unknown op"))
	}
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
