package f1srv

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// Per-shard worker pool for key-affinity execution (spec 2064/f1_rewrite_ltm/17, section 6).
//
// The affinity model routes every command for a key to the single worker that owns the
// key's shard, so the serial data op runs lock-free on one core while parse and network run
// off-core on the connection's home loop. This file is the transport that carries a
// connection's per-shard work to the owning worker and signals it back done. It is the piece
// doc 17 calls the batched MPSC hop: for one pipeline drain a home loop collects the commands
// bound for each shard into one hop, hands each hop to its shard's worker with a single atomic
// push, and waits on a per-drain completion group until every worker has run its hop. The
// worker runs each hop's function to completion in the order the pushes arrived from any one
// producer, which is all a single connection needs because a connection's commands for a
// given shard are carried in one hop and run in that hop in pipeline order.
//
// The transport is deliberately independent of what the hop function does. Here it only moves
// work and counts completions; the routing that decides which commands go in which hop, the
// per-shard store the worker owns, and the reply reordering the home loop does after the
// barrier all land in later slices on top of this. Until those land the pool is constructed
// only when the affinity model is selected and nothing submits to it, so the default shared
// path is untouched.

// hopGroup is the completion barrier for one pipeline drain. A home loop that fans work out to
// several shard workers creates one group, adds a count for each hop it submits, and waits
// until every worker has reported its hop done. It is single-waiter (the home loop) and
// multi-signaler (the workers), so the wait side owns a mutex and condition and each worker
// only decrements the atomic and, when it drops the count to zero, takes the mutex to wake the
// waiter. The atomic fast path means a worker that is not the last to finish never touches the
// lock, so the common case of several workers finishing in parallel stays contention-free
// until the final one.
type hopGroup struct {
	remaining atomic.Int64

	mu   sync.Mutex
	cond *sync.Cond
	done bool
}

func newHopGroup() *hopGroup {
	g := &hopGroup{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// add registers n more hops the group must wait for. The home loop calls it before it pushes
// the hops, so the count can never reach zero between a push and its matching complete.
func (g *hopGroup) add(n int) {
	g.remaining.Add(int64(n))
}

// complete reports one hop finished. The worker calls it after it has run the hop function.
// The last completion (the one that drops the count to zero) wakes the waiting home loop; the
// others return on the atomic alone without touching the lock.
func (g *hopGroup) complete() {
	if g.remaining.Add(-1) != 0 {
		return
	}
	g.mu.Lock()
	g.done = true
	g.cond.Signal()
	g.mu.Unlock()
}

// wait blocks the home loop until every hop in the group has completed. A group with nothing
// added returns at once. It is safe to call after the last complete has already run: done is
// latched, so a wait that arrives late still returns rather than sleeping forever.
func (g *hopGroup) wait() {
	if g.remaining.Load() == 0 {
		return
	}
	g.mu.Lock()
	for !g.done {
		g.cond.Wait()
	}
	g.mu.Unlock()
}

// hop is one unit of shard-local work: a function the owning worker runs to completion and the
// group it reports back to. fn carries a connection's commands for this worker's shard and
// runs them in pipeline order; the worker calls group.complete after fn returns. next links
// hops in the worker's lock-free intake stack and is touched only by the push and drain of
// that stack.
type hop struct {
	fn    func()
	group *hopGroup
	next  *hop
}

// shardWorker owns one shard and runs the hops routed to it, one at a time, on its own
// goroutine, so the shard's data is touched by exactly one core and needs no lock. Producers
// (the connection home loops) hand it work through head, a Treiber stack drained by the worker;
// the stack is multi-producer, single-consumer, so a push is one CAS and a drain is one swap.
// state coordinates parking: the worker spins a bounded number of times checking the stack,
// then parks on the condition, and a producer that pushes onto an empty stack wakes it.
type shardWorker struct {
	shard int

	// head is the top of the intake stack. Producers push with a CAS; the worker swaps the
	// whole stack out to drain. A nil head means no pending work.
	head atomic.Pointer[hop]

	// parkMu/parkCond/parked coordinate the spin-then-park handoff. parked is set by the worker
	// just before it waits and cleared when it wakes; a producer reads it after its push to
	// decide whether it must signal. wake latches a signal that arrives between the worker's
	// last stack check and its wait, so the worker never sleeps through a pending wake.
	parkMu   sync.Mutex
	parkCond *sync.Cond
	parked   bool
	wake     bool

	stop atomic.Bool
	done chan struct{}
}

// spinBudget is how many times a worker rechecks its intake stack after finding it empty
// before it parks. A short spin keeps a worker that is being fed a steady pipeline off the
// park path entirely, which is the hot case, while still yielding the core promptly when the
// shard genuinely goes idle. It mirrors the spin-before-park handoff the write-behind worker
// already uses so a busy shard never pays a futex round trip per hop.
const spinBudget = 256

func newShardWorker(shard int) *shardWorker {
	w := &shardWorker{shard: shard, done: make(chan struct{})}
	w.parkCond = sync.NewCond(&w.parkMu)
	return w
}

// submit pushes a hop onto the worker's intake and wakes the worker if it has parked. It is
// the producer side, safe to call from any home loop concurrently: the push is a single CAS
// loop onto the Treiber stack, and the wake takes the park lock only when the worker is
// actually parked, so a worker that is spinning or running is fed without any lock at all.
func (w *shardWorker) submit(h *hop) {
	for {
		old := w.head.Load()
		h.next = old
		if w.head.CompareAndSwap(old, h) {
			break
		}
	}
	// Wake the worker if it is parked. Set wake under the lock unconditionally so a signal that
	// races the worker's decision to park is not lost: if the worker is between its last stack
	// check and its wait, it will see wake set and skip the sleep.
	w.parkMu.Lock()
	w.wake = true
	if w.parked {
		w.parkCond.Signal()
	}
	w.parkMu.Unlock()
}

// drain swaps the whole intake stack out and returns it in arrival order. The stack is built
// LIFO by the pushers, so the raw chain is newest-first; drain reverses it so hops run in the
// order they were submitted. Order across producers does not matter, but reversing keeps a
// single producer's successive submits in submit order, which is the least surprising behavior
// and costs nothing on top of the swap.
func (w *shardWorker) drain() *hop {
	top := w.head.Swap(nil)
	if top == nil || top.next == nil {
		return top
	}
	var rev *hop
	for top != nil {
		next := top.next
		top.next = rev
		rev = top
		top = next
	}
	return rev
}

// run is the worker goroutine. It drains and runs hops until stopped, spinning a bounded
// number of times when it finds no work before it parks on the condition. Each hop's function
// runs to completion and then the hop reports back to its group, so a home loop's wait returns
// only once every command it routed here has been applied.
func (w *shardWorker) run() {
	defer close(w.done)
	spins := 0
	for {
		h := w.drain()
		if h == nil {
			if w.stop.Load() {
				return
			}
			if spins < spinBudget {
				spins++
				runtime.Gosched()
				continue
			}
			w.park()
			spins = 0
			continue
		}
		spins = 0
		for h != nil {
			next := h.next
			h.fn()
			h.group.complete()
			h = next
		}
	}
}

// park sleeps until a producer signals work or the worker is stopped. It rechecks the intake
// and the latched wake flag under the lock before sleeping so a wake that arrived during the
// spin is never missed: submit sets wake under the same lock, so either the worker sees it here
// and skips the sleep, or submit's Signal lands after the worker is genuinely waiting.
func (w *shardWorker) park() {
	w.parkMu.Lock()
	for !w.wake && !w.stop.Load() && w.head.Load() == nil {
		w.parked = true
		w.parkCond.Wait()
	}
	w.parked = false
	w.wake = false
	w.parkMu.Unlock()
}

// shutdown stops the worker and waits for its goroutine to exit. It is called on server close.
func (w *shardWorker) shutdown() {
	w.stop.Store(true)
	w.parkMu.Lock()
	w.wake = true
	w.parkCond.Signal()
	w.parkMu.Unlock()
	<-w.done
}

// shardPool is the set of workers, one per shard, that the affinity model routes over. It is
// built with as many workers as the server's shard count so shardFor's result indexes it
// directly. The pool is created only when the affinity model is selected; the shared path
// never constructs it.
type shardPool struct {
	workers []*shardWorker
}

func newShardPool(nShards int) *shardPool {
	if nShards < 1 {
		nShards = 1
	}
	p := &shardPool{workers: make([]*shardWorker, nShards)}
	for i := range p.workers {
		p.workers[i] = newShardWorker(i)
	}
	return p
}

// start launches every worker goroutine.
func (p *shardPool) start() {
	for _, w := range p.workers {
		go w.run()
	}
}

// shutdown stops every worker and waits for all of them to exit.
func (p *shardPool) shutdown() {
	for _, w := range p.workers {
		w.shutdown()
	}
}

// submit routes a hop to the worker that owns the given shard. The caller has already computed
// the shard with shardFor, so this is a direct index.
func (p *shardPool) submit(shard int, h *hop) {
	p.workers[shard].submit(h)
}
