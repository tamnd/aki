package shard

import (
	"errors"
	"runtime"
	"sync/atomic"
	"time"
)

// ErrTooBig is returned by Do when one command's key plus argument cannot fit
// an empty batch node. The chunked giant-value path lifts this bound in the
// value-bands slice.
var ErrTooBig = errors.New("shard: command exceeds the batch data cap")

// Conn is one client connection's two halves of the hop transport.
//
// The reader side batches commands per shard: each command gets the next
// per-connection sequence at enqueue time, and Flush publishes each pending
// node with one atomic push per shard, which is the one-atomic-per-batch rule
// of doc 03 section 4.1.
//
// The writer side pops completed nodes off the outbound queue and emits their
// replies. Emission is in arrival order for now, which is request order only
// while a pipeline stays on one shard; the reorder ring that makes it request
// order across shards is the hop-transport slice on top of this.
//
// Exactly one goroutine may drive the reader side and one the writer side;
// they may be the same goroutine.
type Conn struct {
	rt *Runtime

	// Reader-side state.
	seq     uint32
	pending []*hopBatch // one open node per shard, nil when none
	free    chan *hopBatch

	// Writer-side state.
	out     mpsc
	wk      waker
	next    uint32
	emitted atomic.Uint32 // writer's progress, read by the reader's throttle
	closed  atomic.Bool
}

// NewConn builds a connection against the runtime.
func (r *Runtime) NewConn() *Conn {
	c := &Conn{
		rt:      r,
		pending: make([]*hopBatch, len(r.workers)),
		free:    make(chan *hopBatch, freeListCap),
	}
	c.out.init()
	c.wk.init()
	return c
}

// Do enqueues one command. Keyed ops route by wyhash of the key; keyless ops
// (PING, ECHO, parse errors) round-robin by sequence so the smoke surface
// exercises the hop on every shard. Nothing is published until Flush or the
// node fills.
func (c *Conn) Do(op byte, key, arg []byte) error {
	// The pipeline-window throttle: a reader more than a window ahead of the
	// writer blocks on the writer's progress. The doc 03 section 4.5
	// watermarks replace this with per-shard backpressure in the RESP2 slice.
	for c.seq-c.emitted.Load() >= replyRing {
		c.Flush()
		runtime.Gosched()
	}
	var sh int
	if len(key) > 0 {
		sh = c.rt.ShardOf(key)
	} else {
		sh = int(c.seq % uint32(len(c.rt.workers)))
	}
	b := c.pending[sh]
	if b == nil {
		b = c.take()
		c.pending[sh] = b
	}
	if !b.add(op, c.seq, key, arg) {
		c.flushShard(sh)
		b = c.take()
		c.pending[sh] = b
		if !b.add(op, c.seq, key, arg) {
			return ErrTooBig
		}
	}
	c.seq++
	return nil
}

// Flush publishes every pending node, one atomic push per shard, and wakes
// each touched worker.
func (c *Conn) Flush() {
	for sh, b := range c.pending {
		if b != nil {
			c.flushShard(sh)
		}
	}
}

func (c *Conn) flushShard(sh int) {
	b := c.pending[sh]
	c.pending[sh] = nil
	w := c.rt.workers[sh]
	w.inbound.push(b)
	w.wk.wake()
}

// take pulls a node from the free list, or allocates when the list is dry;
// the steady path recycles.
func (c *Conn) take() *hopBatch {
	select {
	case b := <-c.free:
		return b
	default:
		b := newBatch()
		b.conn = c
		return b
	}
}

func (c *Conn) recycle(b *hopBatch) {
	b.reset()
	select {
	case c.free <- b:
	default:
	}
}

// DrainReplies pops every completed node currently queued, emits its replies
// through emit, recycles the nodes, and reports how many replies were
// emitted. Writer side only. Emitted bytes are valid only for the duration of
// the emit call.
func (c *Conn) DrainReplies(emit func([]byte)) int {
	n := 0
	for {
		b := c.out.pop()
		if b == nil {
			if n > 0 {
				c.emitted.Store(c.next)
			}
			return n
		}
		for i := 0; i < int(b.n); i++ {
			emit(b.reply(i))
			c.next++
			n++
		}
		c.recycle(b)
	}
}

// Wait blocks the writer until the outbound queue has work, spinning briefly
// then parking on the connection waker under the same rules as a worker. It
// returns false when the connection is closed and the queue is drained.
func (c *Conn) Wait() bool {
	for {
		if c.out.ready() {
			return true
		}
		if c.closed.Load() {
			return false
		}
		c.idleOnce()
	}
}

// idleOnce is one spin-then-park turn of the writer's wait, the section 9.1
// protocol with the same lost-wake guard the worker's idle carries.
func (c *Conn) idleOnce() {
	c.wk.state.Store(stateSpinning)
	deadline := time.Now().Add(spinWindow)
	for time.Now().Before(deadline) {
		if c.out.ready() || c.closed.Load() {
			c.wk.state.Store(stateRunning)
			return
		}
	}
	c.wk.state.Store(stateParked)
	if c.out.ready() || c.closed.Load() {
		c.wk.unparkSelf()
		return
	}
	c.wk.park()
}

// Close marks the connection done and wakes a parked writer so it can drain
// and exit. Reader side; in-flight replies for a gone client are dropped by
// the caller not draining them.
func (c *Conn) Close() {
	c.closed.Store(true)
	c.wk.wake()
}
