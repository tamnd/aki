package shard

import (
	"errors"
	"runtime"
	"sync/atomic"
)

// ErrTooBig is returned by Do when one command cannot fit even an empty batch
// node: more than spanCap arguments, or more than maxCmdBytes of argument
// data. The dispatcher answers it with an in-order error reply.
var ErrTooBig = errors.New("shard: command exceeds the batch node caps")

// ErrConnFailed is returned by Do and DoFan on a single-goroutine connection
// (SetInlineDrain) whose inline throttle drain hit an unrecoverable wire: a
// streamed reply died mid-emit, or the connection closed under the enqueue.
// Fatal to the connection, like any non-ErrTooBig Do error.
var ErrConnFailed = errors.New("shard: connection failed")

// parked is one out-of-order reply held in the reorder ring until every lower
// sequence has been emitted. The buffer is owned by the slot and reused, so
// parking costs a copy but no allocation once the slot has been warm. A
// streamed reply parks as its stream handle instead of bytes: the chunks stay
// in the stream's ring (where the worker keeps pumping into the bounded
// window) until the sequence comes due.
type parked struct {
	seq  uint32
	live bool
	buf  []byte
	st   *stream
}

// Conn is one client connection's two halves of the hop transport.
//
// The reader side batches commands per shard: each command gets the next
// per-connection sequence at enqueue time, and Flush publishes each pending
// node with one atomic push per shard, which is the one-atomic-per-batch rule
// of doc 03 section 4.1.
//
// The writer side pops completed nodes off the outbound queue and emits
// replies in exact request order: a reply whose sequence is next goes straight
// out and drains its contiguous parked successors; anything else parks in the
// ring. A same-shard pipeline arrives already ordered and pays one compare per
// reply; only a pipeline that genuinely interleaves shards pays the parking
// copy (doc 03 section 4.3).
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
	ring    []parked
	emitted atomic.Uint32 // writer's progress, read by the reader's throttle
	closed  atomic.Bool

	// published is the reader's issued-sequence watermark at its last publish:
	// the value of seq the last time a node went onto a shard queue. Some
	// commands below it may still sit in other shards' pending nodes at that
	// moment, but the reader publishes everything pending before it blocks (the
	// boundary Flush in the transport read loop, or the throttle above), so a
	// reply for every sequence below the watermark arrives without further
	// input from the client. The writer compares it against its own emit
	// progress in Owes, the transport's flush gate.
	published atomic.Uint32

	// failed flips when a streamed reply dies mid-emit: the bulk header is
	// already on the wire, so nothing coherent can follow and the transport
	// must drop the connection. Writer side only, plain field.
	failed bool

	// wokeWorkers counts the worker wake tokens the reader side sent (the
	// flushShard wake path, only when the CAS claimed a parked worker).
	// Reader-side single-writer counter; see NetWakes.
	wokeWorkers eventCounter

	// parks counts the writer goroutine's real parks: blocks taken on the
	// waker channel after the spin window, not spin turns. Writer-side
	// single-writer counter; see NetWakes.
	parks eventCounter

	// inlineEmit, when set, declares the connection single-goroutine driven:
	// the same goroutine runs the reader and writer sides, so the pipeline
	// throttle in Do cannot yield to a writer goroutine that does not exist
	// and instead drains replies itself through this emit. Set once before
	// traffic; read by the reader side only.
	inlineEmit func([]byte)
}

// SetWriterNotify makes the writer-side wake target pluggable: a claimed wake
// calls fn instead of sending the waker channel token. It exists for drivers
// whose consumer cannot block on a channel, the reactor loop being the one
// today: the loop parks in epoll_wait, so the owner's wake must reach it
// through an eventfd, and fn is where the driver hangs that write. The
// waker's publish-then-check proof is untouched; only the token's transport
// changes. fn runs on whichever owner goroutine claims the wake, so it must
// be safe from any goroutine and must never block. Call before any traffic on
// the connection.
func (c *Conn) SetWriterNotify(fn func()) { c.wk.notify = fn }

// CanEnqueue reports whether one more command can enqueue without the Do
// throttle engaging: the reader is less than a full reorder ring ahead of the
// writer's emit cursor. A notifier-driven transport (the reactor loop) checks
// it before every dispatch and stops parsing when it goes false, because the
// throttle's wait paths would block or spin the loop thread; the loop resumes
// its parse backlog once DrainReplies has advanced the cursor. Reader side
// only.
func (c *Conn) CanEnqueue() bool {
	return c.seq-c.emitted.Load() < uint32(len(c.ring))
}

// ParkWriter arms the writer-side wake for a notifier-driven consumer and
// reports whether it parked clean: it stores parked, then re-checks the
// outbound queue, the same publish-then-check shape as idleOnce, so an owner
// push landing between the drain and the park cannot be lost (either this
// re-check sees the work, or the owner's state load sees parked and delivers
// the notify). A false return means work is already queued, or the connection
// is closed, and the caller must service it again instead of sleeping. It
// must be the last touch on the connection before the consumer blocks in its
// own wait (epoll_wait for the loop). Writer side only.
func (c *Conn) ParkWriter() bool {
	c.wk.state.Store(stateParked)
	if c.out.ready() || c.closed.Load() {
		c.wk.unparkSelf()
		return false
	}
	c.parks.bump()
	return true
}

// SetInlineDrain declares this connection single-goroutine driven and gives
// the reader-side throttle the emit to drain replies through when the reader
// runs a full reorder ring ahead of its own emit cursor. Without it a
// single-goroutine transport would deadlock in Do the moment one read pass
// parses more than a ring's worth of commands: the throttle would wait on a
// drain only this goroutine can run. Call before any traffic on the
// connection.
func (c *Conn) SetInlineDrain(emit func([]byte)) { c.inlineEmit = emit }

// Failed reports whether a streamed reply died mid-emit, leaving the wire in
// an unrecoverable state. The transport checks it after each drain and tears
// the connection down when set. Writer side only.
func (c *Conn) Failed() bool { return c.failed }

// Owes reports whether replies are still due for commands at or below the
// reader's publish watermark: the writer has not yet emitted every sequence
// the reader had issued at its last publish. The transport writer uses it as
// its flush gate, deferring the socket flush until a pipelined round has fully
// drained; a lone command hits zero the moment its reply emits, so nothing
// idles in the buffer. A false from a stale or overtaken watermark read only
// means one early flush, never a stall: every sequence under the watermark has
// a reply on the way with no further input from the client (see published), so
// the writer that skips a flush is always woken again. The comparison is the
// wrap-safe signed difference, not equality, because a fan-out's sub-commands
// carry a sequence the reader has not advanced past yet: a node fill mid-fan
// publishes a watermark of exactly that sequence, and once the gathered reply
// emits the cursor sits one past it. Equality would read that as owed and
// defer the flush with nothing left in flight. Writer side only (it reads the
// writer's plain emit cursor).
func (c *Conn) Owes() bool { return int32(c.published.Load()-c.next) > 0 }

// NewConn builds a connection against the runtime.
func (r *Runtime) NewConn() *Conn {
	c := &Conn{
		rt:      r,
		pending: make([]*hopBatch, len(r.workers)),
		free:    make(chan *hopBatch, freeListCap),
		ring:    make([]parked, replyRing),
	}
	c.out.init()
	c.wk.init()
	return c
}

// Do enqueues one command with its full argument vector; a keyed command's
// args[0] is its key. Keyed ops route by wyhash of the key; keyless ops
// (PING, ECHO, parse errors) round-robin by sequence so the smoke surface
// exercises the hop on every shard. The arguments are copied into the node
// before Do returns, so the caller may reuse its views immediately. Nothing
// is published until Flush or the node fills.
func (c *Conn) Do(op byte, keyed bool, args [][]byte) error {
	if c.seq-c.emitted.Load() >= uint32(len(c.ring)) {
		if err := c.throttle(); err != nil {
			return err
		}
	}
	var sh int
	if keyed {
		sh = c.rt.ShardOf(args[0])
	} else {
		sh = int(c.seq % uint32(len(c.rt.workers)))
	}
	b := c.pending[sh]
	if b == nil {
		b = c.take()
		c.pending[sh] = b
	}
	if !b.add(op, c.seq, keyed, args) {
		c.flushShard(sh)
		b = c.take()
		c.pending[sh] = b
		if !b.add(op, c.seq, keyed, args) {
			return ErrTooBig
		}
	}
	c.seq++
	return nil
}

// throttle is the pipeline-window backstop, entered only when the reader is a
// full reorder ring ahead of the emit cursor, so a parked reply always has a
// distinct ring slot. On a paired connection it yields to the writer
// goroutine and rechecks. A single-goroutine connection (SetInlineDrain) is
// its own writer, so yielding could never open the window: it publishes, waits
// on the conn waker like the transport's boundary drain does, and drains the
// replies right here. The doc 03 section 4.5 watermarks replace this with
// per-shard backpressure in a later slice.
func (c *Conn) throttle() error {
	for c.seq-c.emitted.Load() >= uint32(len(c.ring)) {
		c.Flush()
		if c.inlineEmit == nil {
			runtime.Gosched()
			continue
		}
		if !c.Wait() {
			// Closed with the window still full. A single-goroutine
			// connection closes itself, so this is unreachable in the
			// transport; it exists so a misuse unwinds instead of spinning.
			return ErrConnFailed
		}
		c.DrainReplies(c.inlineEmit)
		if c.failed {
			// A streamed reply died mid-emit during the inline drain; the
			// wire is unrecoverable and the enqueue must unwind so the
			// transport can tear the connection down.
			return ErrConnFailed
		}
	}
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
	// Advance the watermark before the push so that by the time this node's
	// replies drain, Owes already accounts for every sequence issued so far.
	c.published.Store(c.seq)
	w := c.rt.workers[sh]
	if w.inbound.push(b) {
		if w.wk.wake() {
			c.wokeWorkers.bump()
		}
		return
	}
	// Wake skipped. The invariant: a wake may be skipped only when the
	// observed queue state guarantees the consumer will re-check after its
	// current pass. The push's tail swap returned a real node, so at publish
	// time the queue held a batch the worker has not returned; the worker
	// parks only after storing parked and re-checking ready(), and ready()
	// stays true from that earlier batch through our push until our node is
	// popped, so the worker cannot park without seeing it. Whoever published
	// that earlier node already paid the wake (or skipped under this same
	// rule against a still-earlier one).
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
// in request order through emit, recycles the nodes, and reports how many
// replies were emitted. Writer side only. Emitted bytes are valid only for
// the duration of the emit call.
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
			if fc := b.fan(i); fc != nil {
				n += c.mergeFan(fc, b.cmds[i].seq, b, i, emit)
				continue
			}
			if st := b.stream(i); st != nil {
				n += c.deliverStream(b.cmds[i].seq, st, emit)
				continue
			}
			n += c.deliver(b.cmds[i].seq, b.reply(i), emit)
		}
		c.recycle(b)
	}
}

// deliver reorders one reply: emit now when it is the next sequence, then
// drain contiguous parked successors; park otherwise.
func (c *Conn) deliver(seq uint32, rep []byte, emit func([]byte)) int {
	if seq != c.next {
		s := &c.ring[seq%uint32(len(c.ring))]
		s.seq = seq
		s.live = true
		s.st = nil
		s.buf = append(s.buf[:0], rep...)
		return 0
	}
	emit(rep)
	c.next++
	return 1 + c.drainParked(emit)
}

// deliverStream reorders one streamed reply under the same rule: serve it now
// when it is the next sequence, park its handle otherwise. Serving blocks
// until the stream completes, because everything behind it in the pipeline
// must wait anyway.
func (c *Conn) deliverStream(seq uint32, st *stream, emit func([]byte)) int {
	if seq != c.next {
		s := &c.ring[seq%uint32(len(c.ring))]
		s.seq = seq
		s.live = true
		s.st = st
		s.buf = s.buf[:0]
		return 0
	}
	c.emitStream(st, emit)
	c.next++
	return 1 + c.drainParked(emit)
}

// drainParked emits the contiguous parked run starting at c.next, bytes and
// streams alike.
func (c *Conn) drainParked(emit func([]byte)) int {
	n := 0
	for {
		s := &c.ring[c.next%uint32(len(c.ring))]
		if !s.live || s.seq != c.next {
			return n
		}
		if s.st != nil {
			st := s.st
			s.st = nil
			s.live = false
			c.emitStream(st, emit)
		} else {
			emit(s.buf)
			s.live = false
		}
		c.next++
		n++
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
	// Calibrated iteration count instead of a time.Now per turn; see
	// spinIters in tuning.go.
	for i := 0; i < spinIters; i++ {
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
	c.parks.bump()
	c.wk.park()
}

// Close marks the connection done and wakes a parked writer so it can drain
// and exit. Reader side; in-flight replies for a gone client are dropped by
// the caller not draining them.
func (c *Conn) Close() {
	c.closed.Store(true)
	c.wk.wake()
}
