package command

import (
	"sync/atomic"
	"time"
)

// shardQueue is the lock-free hand-off from the connection goroutines to one
// shard's write worker. It replaces the buffered Go channel that used to sit
// between them, whose hchan.lock all the connections serialized on: under the
// lockstep benchmark fifty connections write the same key, the key hashes to one
// shard, and all fifty producers contended on that one channel's mutex. The CPU
// profile attributed roughly an eighth of all time to that hand-off
// (runtime.lock2, the cond_wait/cond_signal pair, and chansend). This structure
// removes the shared mutex from the producer path.
//
// It is a multi-producer single-consumer queue. Producers (any connection
// goroutine) push; the one shard worker is the only consumer. The node is the
// writeReq itself via its next field, so the queue allocates nothing: the req
// the hand-off already pools carries its own link.
//
// The queue is a Treiber stack that the consumer drains whole and reverses to
// restore first-in-first-out order. Producers publish with a single
// compare-and-swap on top; the consumer swaps the entire stack out in one atomic
// operation and walks it. This was chosen over a Vyukov-style linked queue for
// two concrete reasons. First, the consumer wants a whole batch at once
// (drainShardList coalesces same-key SETs across the batch), and a single swap
// hands it exactly that with no per-node consumer synchronization and no
// in-flight gap to spin on. Second, the popped nodes are returned to the caller
// as-is and recycled to the pool immediately, with no stub node that has to be
// held back until the following pop. The cost is a compare-and-swap loop on the
// producer instead of a wait-free swap, but that loop spins only on a single
// cache line and never enters the kernel, which is the whole point: the mutex
// and its futex are gone.
//
// First-in-first-out order is not relied on for correctness any more (the write
// sinks are version-guarded, see slice 201), but the reverse preserves it for
// free, so coalesceSets sees the same batch shape it always has and the consumer
// side did not change.
type shardQueue struct {
	// top is the Treiber stack head. Producers compare-and-swap a new node in;
	// the consumer swaps the whole stack out.
	top atomic.Pointer[writeReq]
	// state is owned by the consumer. It is stateRunning while the worker is
	// draining and stateParked once it has blocked on the doorbell. A producer
	// reads it after pushing and rings the doorbell only when it finds the worker
	// parked, so under load (worker always running) the producer never touches the
	// channel at all.
	state atomic.Int32
	// length is an approximate depth used only for backpressure: a producer pushes
	// only while it is under shardQueueCap, otherwise it sheds to the synchronous
	// path so an unbounded queue can never grow without limit when the worker
	// falls behind. It is incremented per push and decremented per drained batch.
	length atomic.Int64
	// wake is the one-slot doorbell. A producer rings it only on the transition
	// that parked the worker, so it carries at most one token per park cycle.
	wake chan struct{}
}

const (
	stateRunning int32 = 0
	stateParked  int32 = 1
)

// shardQueueCap bounds the queue depth before producers shed to the synchronous
// path. It matches the buffer the replaced channel carried, so the backpressure
// point is unchanged.
const shardQueueCap = 4096

// shardParkBackstop bounds how long the worker blocks on the doorbell before it
// re-checks the queue on its own. A hand-rolled wakeup has a subtle lost-wakeup
// failure mode; the backstop turns that from a hang into at most this much extra
// latency on the next write, so correctness rests on the queue structure and the
// re-check, never on the doorbell being perfect. Under load the queue is never
// empty long enough to park, so the timer almost never fires.
const shardParkBackstop = 200 * time.Microsecond

// init prepares the queue for a run. The Engine reuses its shardQueue array
// across StartWorker/StopWorker cycles (tests do this), so init resets the stack
// and state and drains any stale doorbell token left by a prior run.
func (q *shardQueue) init() {
	q.top.Store(nil)
	q.state.Store(stateRunning)
	q.length.Store(0)
	if q.wake == nil {
		q.wake = make(chan struct{}, 1)
		return
	}
	select {
	case <-q.wake:
	default:
	}
}

// push enqueues one request. It is safe for any number of concurrent producers.
// It links the node onto the Treiber stack with a single compare-and-swap, then
// rings the doorbell only if the worker has parked, winning the parked-to-running
// transition first so exactly one producer per park cycle does the wakeup.
func (q *shardQueue) push(n *writeReq) {
	for {
		old := q.top.Load()
		n.next.Store(old)
		if q.top.CompareAndSwap(old, n) {
			break
		}
	}
	q.length.Add(1)
	if q.state.Load() == stateParked && q.state.CompareAndSwap(stateParked, stateRunning) {
		select {
		case q.wake <- struct{}{}:
		default:
		}
	}
}

// popAll detaches the entire pending stack in one atomic swap and returns it as
// a first-in-first-out linked list (via next), or nil when the queue is empty.
// Only the single consumer calls it. After the swap the nodes are owned solely
// by the consumer, so it walks and relinks them without further synchronization.
func (q *shardQueue) popAll() *writeReq {
	top := q.top.Swap(nil)
	if top == nil {
		return nil
	}
	var head *writeReq
	var n int64
	for top != nil {
		next := top.next.Load()
		top.next.Store(head)
		head = top
		top = next
		n++
	}
	q.length.Add(-n)
	return head
}

// stopTimer stops t and drains its channel if it had already fired, so the timer
// can be safely reset on the next park without a stale token firing the select
// early.
func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
