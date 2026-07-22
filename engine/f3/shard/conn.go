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

	// Reader-side fan-out scatter scratch, reused across DoFan calls so a
	// multi-key command (MGET/MSET) does not allocate its routing and
	// sub-command buffers per command. fanOrder holds each key's owning shard,
	// fanArgv and fanPos are the per-sub-command argument and MGET position
	// buffers b.add copies out of on enqueue, so they are safe to reuse the
	// moment enqueueFan returns. Reader goroutine only, like the rest of this
	// block.
	fanOrder []int
	fanArgv  [][]byte
	fanPos   []byte

	// silentNext is the reader's CLIENT REPLY suppression flag for the next
	// command it enqueues: the driver sets it through SetReplySilent before each
	// dispatch, and enqueue/enqueueFan/InlineReply stamp it onto the command's
	// hopCmd.silent so the writer drops the reply. Reader side only, a plain field
	// restamped every command, so a stale value never leaks past one enqueue.
	silentNext bool

	// Writer-side state.
	out     mpsc
	wk      waker
	next    uint32
	ring    []parked
	emitted atomic.Uint32 // writer's progress, read by the reader's throttle
	closed  atomic.Bool

	// silentRing records, per reorder slot, whether the reply at that sequence is
	// suppressed (CLIENT REPLY OFF/SKIP). The writer sets slot seq%len when it
	// processes the command's node in DrainReplies, and the emit wrapper reads
	// slot c.next%len when a reply is about to go out, so a point, streamed, or
	// fan reply all drop through the one check. Sized to the reorder ring and
	// writer-owned. everSilent gates the whole path: it stays false until the
	// connection first suppresses a reply, so a connection that never runs CLIENT
	// REPLY pays nothing (no slot writes, no emit wrapper) on the reply hot path.
	silentRing []bool
	everSilent atomic.Bool

	// blockAt arms the reader-side connection barrier: it holds one past the
	// sequence of a blocking command the reader dispatched, or zero when none is
	// outstanding. Blocked() compares it against the writer's emitted watermark, so
	// the barrier disarms itself the moment that command's reply goes out, whether
	// the command served immediately or its Park was completed later. Written by the
	// reader (ArmBlock), read by the reader, which also reads emitted, the same
	// cross-goroutine load CanEnqueue makes. Reader side.
	blockAt atomic.Uint32

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

	// The batched writer-notify seam (SetWriterBatchNotify): batchMark marks
	// this connection dirty on its consumer loop, batchLoop is the loop whose
	// one pass-end WakeLoop covers every mark of the pass. Both are set once
	// before traffic and read by owner goroutines through the inbound queue's
	// ordering, like wk.notify.
	batchMark func() bool
	batchLoop LoopWaker

	// The step-mode stream cursor (SetStreamStep): cur is the streamed reply
	// mid-emit, whose bulk header is already on the wire and whose sequence
	// still holds the reorder cursor; curSent is its body progress. A
	// non-blocking consumer (the reactor loop) adopts a due stream here
	// instead of blocking in emitStream, and advances it with StreamStep at
	// its own pace. Writer side only.
	stepStream bool
	cur        *stream
	curSent    int64

	// inlineEmit, when set, declares the connection single-goroutine driven:
	// the same goroutine runs the reader and writer sides, so the pipeline
	// throttle in Do cannot yield to a writer goroutine that does not exist
	// and instead drains replies itself through this emit. Set once before
	// traffic; read by the reader side only.
	inlineEmit func([]byte)

	// resp3 is the connection's negotiated protocol version: false for RESP2 (the
	// default every connection opens on), true once HELLO 3 switched it. The reply
	// writer reads it through Reply.Resp3 to pick the RESP3 frame types (map, set,
	// double, boolean, null, push) over their RESP2 shapes. Written by the network
	// layer's HELLO handler on the reader side and read by an owner goroutine
	// building a reply, so it is an atomic; the HELLO reply is published before any
	// later command's batch, so an owner that reads it sees the negotiated version.
	resp3 atomic.Bool

	// tx is the connection's MULTI/EXEC transaction state, an opaque handle the
	// dispatch layer owns (its concrete type is dispatch's txnState). It is nil
	// until the connection issues MULTI or WATCH. It is read and written only on
	// the reader goroutine, where the queue-then-EXEC lifecycle lives, so it is a
	// plain field, not an atomic; the shard layer never inspects it and only
	// carries the pointer so the per-connection queue survives across commands.
	tx any
}

// TxState returns the connection's MULTI/EXEC transaction handle, or nil when the
// connection is not queuing a transaction. The dispatch layer stashes its per-conn
// queue here through SetTxState. Reader goroutine only.
func (c *Conn) TxState() any { return c.tx }

// SetTxState records the connection's MULTI/EXEC transaction handle, or clears it
// with nil when EXEC/DISCARD/RESET finishes the transaction. Reader goroutine only.
func (c *Conn) SetTxState(v any) { c.tx = v }

// ShardOf reports the owner shard of key, the dispatch hash the EXEC route uses to
// place a queued command's point op on the owner that holds its intent lock. It is
// the connection-side twin of the runtime hash Do routes on. Reader goroutine only.
func (c *Conn) ShardOf(key []byte) int { return c.rt.ShardOf(key) }

// SetReplySilent arms (or clears) reply suppression for the next command the
// reader enqueues: the driver's CLIENT REPLY handling calls it before each
// dispatch with whether that command's reply must be dropped (CLIENT REPLY OFF,
// or the one command after CLIENT REPLY SKIP). The flag rides the command's
// hopCmd.silent to the writer, which drops the bytes at emit time while the
// command still executes and the reorder cursor still advances. The first true
// latches everSilent, which turns the writer's drop path on for the rest of the
// connection's life; a connection that never suppresses keeps everSilent false
// and the reply hot path unchanged. Reader side only.
func (c *Conn) SetReplySilent(v bool) {
	c.silentNext = v
	if v {
		c.everSilent.Store(true)
	}
}

// SetResp3 records the connection's negotiated protocol version, the switch
// HELLO 2 and HELLO 3 throw. The reply writer reads it through Resp3.
func (c *Conn) SetResp3(v bool) { c.resp3.Store(v) }

// Resp3 reports whether the connection negotiated RESP3. The reply writer calls
// it to choose a frame type; a connection that never sent HELLO 3 reads false and
// gets RESP2 shapes, the default.
func (c *Conn) Resp3() bool { return c.resp3.Load() }

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

// LoopWaker is the delivery half of the batched writer notify: one WakeLoop
// covers every connection marked dirty on that loop since its last drain.
// Connections sharing a consumer loop must be registered with the identical
// LoopWaker value, because the worker dedups its pass-end deliveries on it.
type LoopWaker interface {
	// WakeLoop delivers one wake to the loop (the reactor's eventfd write).
	// It must behave like a level trigger: a delivery after the loop's
	// dirty-list swap must bring the loop back for another swap, never fold
	// into a wake the loop has already consumed. It runs on whichever owner
	// goroutine ends a drain pass, so it must be safe from any goroutine and
	// must never block.
	WakeLoop()
}

// SetWriterBatchNotify is SetWriterNotify's batched form, for consumers that
// own many connections behind one wake fd (the reactor loop). mark records
// this connection dirty on the loop and reports whether this call queued it;
// a false means an earlier mark is still queued and undrained, so the wake
// delivery that mark owes covers this one too. mark may elide ONLY on that
// evidence: the loop clears the queued state before it services the
// connection, so any mark the loop could miss is a queuing one and forces a
// fresh WakeLoop.
//
// The worker's drain pass claims the writer wake per connection (the waker
// CAS, counted as a conn wake), calls mark for each claim, and ends the pass
// with one WakeLoop per distinct loop marked, which is the slice 4 batching:
// eventfd writes per pass drop from O(dirty connections) to O(touched loops).
// Wakes claimed outside a drain pass (Close, a stream abort) deliver
// immediately through the same mark-then-WakeLoop pair, so the loop never
// learns about a dirty connection without a delivery behind it. The waker's
// publish-then-check proof is unchanged; only the token's transport is.
// Call before any traffic on the connection.
func (c *Conn) SetWriterBatchNotify(loop LoopWaker, mark func() bool) {
	c.batchMark = mark
	c.batchLoop = loop
	c.wk.notify = func() {
		if mark() {
			loop.WakeLoop()
		}
	}
}

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

// ArmBlock arms the reader-side barrier after a blocking verb has enqueued: the
// dispatcher calls it once the command's Do has advanced c.seq, so the stored
// value is one past the command's own sequence. Blocked() then reads that
// barrier against the writer's emit watermark until the command's reply goes
// out. Reader side only.
func (c *Conn) ArmBlock() { c.blockAt.Store(c.seq) }

// Blocked reports whether a blocking command the reader dispatched is still
// waiting for its reply: the emit watermark has not yet reached the armed
// barrier. The reader consults it where it consults CanEnqueue, and the barrier
// disarms itself implicitly, because emitted crossing blockAt flips the compare
// whether the command served immediately or its Park was completed later. The
// comparison is the wrap-safe signed difference, the same idiom Owes uses.
// Reader side only.
func (c *Conn) Blocked() bool { return int32(c.blockAt.Load()-c.emitted.Load()) > 0 }

// ParkWriter arms the writer-side wake for a notifier-driven consumer and
// reports whether it parked clean: it stores parked, then re-checks the
// outbound queue and the in-progress stream, the same publish-then-check
// shape as idleOnce, so an owner push or a pump's chunk publish landing
// between the drain and the park cannot be lost (either this re-check sees
// the work, or the producer's state load sees parked and delivers the
// notify). A false return means work is already queued, a stream can make
// progress, or the connection is closed, and the caller must service it
// again instead of sleeping. It
// must be the last touch on the connection before the consumer blocks in its
// own wait (epoll_wait for the loop). Writer side only.
func (c *Conn) ParkWriter() bool {
	c.wk.state.Store(stateParked)
	if c.out.ready() || c.closed.Load() || c.StreamReady() {
		c.wk.unparkSelf()
		return false
	}
	c.parks.bump()
	return true
}

// SetStreamStep declares the writer side a non-blocking consumer: a streamed
// reply whose turn comes in the reorder order is adopted as an in-progress
// cursor (its header emitted, its sequence holding the reorder cursor)
// instead of being drained to completion inside DrainReplies, and the caller
// advances it with StreamStep as its socket and buffers allow. It exists for
// the reactor loop, which owns every connection on its fd shard and must
// never block on one of them (doc 08 sections 3.3 and 9.1: a streaming reply
// on the loop yields between chunks or it convoys the shard). Call before any
// traffic on the connection.
func (c *Conn) SetStreamStep() { c.stepStream = true }

// StreamReady reports whether the in-progress streamed reply can make
// progress right now: chunks sit ready in its ring, the body is complete and
// only the trailer (and any parked successors) remains, or it has failed and
// the failure is waiting to be observed. ParkWriter re-checks it, so a
// producer's chunk publish landing against a parking consumer is never lost
// (the pump's publish-then-wake pairs with the park's store-then-re-check,
// the waker proof shape). Writer side only.
func (c *Conn) StreamReady() bool {
	st := c.cur
	if st == nil {
		return false
	}
	return st.prod.Load() != st.cons.Load() || st.failed.Load() || c.curSent == st.total
}

// StreamAborted reports whether the in-progress streamed reply has failed
// (source error, client gone, shard shutdown). The header is already on the
// wire, so the connection is unrecoverable and the transport must drop it,
// mid-cycle, without waiting for write-side room to observe the failure
// through StreamStep. Writer side only.
func (c *Conn) StreamAborted() bool {
	st := c.cur
	return st != nil && st.failed.Load()
}

// StreamStep advances the in-progress streamed reply (step mode): it emits
// ready chunks until the ring runs dry, roughly budget bytes have gone out,
// or the stream completes. On completion it emits the trailer, advances the
// reorder cursor, and drains parked successors, which may adopt the next
// parked stream as the new cursor and continue under the same budget. A dry
// ring returns without spinning; the producer's pump delivers the writer wake
// that resumes the step. A failed stream flips Failed and clears the cursor;
// the caller tears the connection down. Writer side only.
func (c *Conn) StreamStep(emit func([]byte), budget int) {
	if c.everSilent.Load() {
		// The reorder cursor holds the stream's sequence for the whole reply, so
		// the silent bit is constant across its chunks and trailer; the wrapper
		// drops them all while consumeOne still drains the ring so the producer
		// never stalls behind a suppressed reply.
		emit = c.silentEmit(emit)
	}
	advanced := false
	for st := c.cur; st != nil; st = c.cur {
		if st.failed.Load() {
			c.failed = true
			c.cur = nil
			break
		}
		if c.curSent == st.total {
			if !st.raw {
				emit(crlf)
			}
			c.cur = nil
			c.next++
			c.drainParked(emit)
			advanced = true
			continue
		}
		if budget <= 0 {
			break
		}
		n, ok := st.consumeOne(emit)
		if !ok {
			break
		}
		c.curSent += n
		budget -= int(n)
	}
	if advanced {
		c.emitted.Store(c.next)
	}
}

// beginStream adopts st as the connection's in-progress streamed reply (step
// mode): the bulk header goes out now, the chunks follow through StreamStep.
// The reorder cursor stays on the stream's sequence until the last chunk and
// the trailer have been emitted, so everything behind it in the pipeline
// parks in the ring, bounded by the reader-side window. Writer side only.
func (c *Conn) beginStream(st *stream, emit func([]byte)) {
	st.header(emit)
	c.cur = st
	c.curSent = 0
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
		rt:         r,
		pending:    make([]*hopBatch, len(r.workers)),
		free:       make(chan *hopBatch, r.freeListCap),
		ring:       make([]parked, r.replyRing),
		silentRing: make([]bool, r.replyRing),
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
	return c.enqueue(op, keyed, sh, args)
}

// DoAt is Do for a keyed command whose key is not its first argument: it routes
// by args[keyIdx] instead of args[0]. OBJECT ENCODING is the one verb in this
// shape, its key sitting after the subcommand token; every other keyed command
// keys on args[0] and goes through Do. The command is marked keyed on the wire
// so replies keep their reorder slot.
func (c *Conn) DoAt(op byte, keyIdx int, args [][]byte) error {
	if c.seq-c.emitted.Load() >= uint32(len(c.ring)) {
		if err := c.throttle(); err != nil {
			return err
		}
	}
	// The node is marked keyless: routing is already fixed by the shard picked
	// here, and the only other reader of the flag is the prefetch stage, which
	// touches the store bucket for args[0]. OBJECT's args[0] is its subcommand,
	// not a key, so a prefetch there would warm the wrong line; skipping it
	// costs nothing on a command this rare.
	return c.enqueue(op, false, c.rt.ShardOf(args[keyIdx]), args)
}

// enqueue adds one command to the pending node for shard sh, flushing and
// retrying once if the node is full, and advances the sequence. Do and DoAt
// share it; they differ only in how they pick sh.
func (c *Conn) enqueue(op byte, keyed bool, sh int, args [][]byte) error {
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
	b.cmds[b.n-1].silent = c.silentNext
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
		b := newBatch(c.rt.batchDataCap, c.rt.repCap)
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
	// Reply suppression (CLIENT REPLY OFF/SKIP) is gated on everSilent: until the
	// connection first suppresses a reply the writer never wraps the emit or
	// touches the silent ring, so the common connection pays nothing on the reply
	// hot path. Once armed, wemit drops the reply at any sequence the silent ring
	// marks, while the command still ran on its owner and the cursor still
	// advances past it; the silentRing writes below record each sequence's bit.
	wemit := emit
	silent := c.everSilent.Load()
	if silent {
		wemit = c.silentEmit(emit)
	}
	n := 0
	for {
		b := c.out.pop()
		if b == nil {
			if n > 0 {
				c.emitted.Store(c.next)
			}
			return n
		}
		if b.oob {
			// An out-of-band node (DeliverOOB): its replies belong to no pipeline
			// sequence, so they go straight to the wire in write order without
			// touching c.next or the reorder ring. One flag load per drained node,
			// off the point-op path, keeps this out of the reorder machinery. An
			// unsolicited push is never suppressed: a pub/sub message reaches a
			// subscriber regardless of its CLIENT REPLY mode, so it uses raw emit.
			for i := 0; i < int(b.n); i++ {
				emit(b.reply(i))
				n++
			}
			c.recycle(b)
			continue
		}
		for i := 0; i < int(b.n); i++ {
			if !silent && b.cmds[i].silent {
				// everSilent latched after the top-of-drain load: the pair shape
				// runs the writer on its own goroutine, so a node can carry a
				// suppression bit this drain snapshotted as off. Any silent node
				// forces the wrapper on for the rest of this drain; everSilent is
				// monotonic so no earlier node in this drain was silent, and later
				// drains see the latched flag directly.
				silent = true
				wemit = c.silentEmit(emit)
			}
			if silent && b.cmds[i].op != opBlockDone {
				// Record this sequence's suppression before the reply is served,
				// so the emit wrapper sees it whether the reply goes out in order
				// now or parks and drains later. A CompleteBlocked loopback
				// (opBlockDone) is skipped so it preserves the bit the original
				// blocked command already recorded for the slot; every other node,
				// including an opInlineDone reply, takes the ordinary set path.
				c.silentRing[b.cmds[i].seq%uint32(len(c.silentRing))] = b.cmds[i].silent
			}
			if b.blocked(i) {
				// Parked (Reply.Park): no reply now, and c.next must NOT advance past this
				// sequence. Its reply arrives later on a CompleteBlocked loopback node;
				// until then every later reply parks in the ring behind it.
				continue
			}
			if fc := b.fan(i); fc != nil {
				n += c.mergeFan(fc, b.cmds[i].seq, b, i, wemit)
				continue
			}
			if st := b.stream(i); st != nil {
				n += c.deliverStream(b.cmds[i].seq, st, wemit)
				continue
			}
			n += c.deliver(b.cmds[i].seq, b.reply(i), wemit)
		}
		c.recycle(b)
	}
}

// silentEmit wraps an emit so a reply whose sequence is marked in the silent
// ring is dropped instead of written. It keys on c.next, the sequence the writer
// is emitting at the moment of the call: a point reply, a streamed reply's every
// chunk (the cursor holds the stream's sequence until its trailer), and a fan
// reply all pass through it, so one check covers every reply shape. Writer side
// only; used only once everSilent has latched.
func (c *Conn) silentEmit(emit func([]byte)) func([]byte) {
	return func(p []byte) {
		if !c.silentRing[c.next%uint32(len(c.silentRing))] {
			emit(p)
		}
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
// must wait anyway; a step-mode connection adopts it as the in-progress
// cursor instead and never blocks here.
func (c *Conn) deliverStream(seq uint32, st *stream, emit func([]byte)) int {
	if seq != c.next {
		s := &c.ring[seq%uint32(len(c.ring))]
		s.seq = seq
		s.live = true
		s.st = st
		s.buf = s.buf[:0]
		return 0
	}
	if c.stepStream {
		c.beginStream(st, emit)
		return 0
	}
	c.emitStream(st, emit)
	c.next++
	return 1 + c.drainParked(emit)
}

// drainParked emits the contiguous parked run starting at c.next, bytes and
// streams alike. A parked stream stops the run on a step-mode connection: it
// becomes the in-progress cursor without advancing the reorder cursor, and
// the run resumes through StreamStep once its bytes are out.
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
			if c.stepStream {
				c.beginStream(st, emit)
				return n
			}
			c.emitStream(st, emit)
		} else {
			emit(s.buf)
			s.live = false
		}
		c.next++
		n++
	}
}

// CompleteBlocked delivers a blocking command's deferred reply: a loopback node
// carrying rep at the parked command's sequence goes onto the outbound queue,
// where the writer reorders it like any owner-produced reply, so the reply that
// Reply.Park held open lands at its original pipeline slot. It is the general
// form of finishTxn (txnroute.go): the later owner step that serves the block (a
// push that satisfies a BLPOP, a firing timeout) calls it with the seq and conn
// the handler captured through CurSeq and CurConn. The free-list channel and the
// outbound MPSC are both safe from that goroutine, and the wake-skip rule is the
// same one every producer follows (see flushShard).
func (c *Conn) CompleteBlocked(seq uint32, rep []byte) {
	b := c.take()
	b.add(opBlockDone, seq, false, nil)
	r := Reply{b: b, i: 0}
	r.Raw(rep)
	if c.out.push(b) {
		c.wk.wake()
	}
}

// InlineReply enqueues a reply the network layer produced on the reader
// goroutine at the next pipeline sequence, so a command answered without a shard
// hop (the pub/sub family, doc 17 section 13) keeps its place in reply order: a
// PING pipelined ahead of a SUBSCRIBE still answers PONG before the subscribe
// confirmation. The node carries the finished bytes and rides the outbound queue
// and reorder ring exactly like an owner reply, which is also what makes it safe
// under the pair shape, where emitting straight from the reader would race the
// writer goroutine. It advances published itself so a pure pub/sub pass, which
// flushes no shard node, still trips Owes and drains on the single shape. Reader
// side only, like Do.
func (c *Conn) InlineReply(rep []byte) error {
	if c.seq-c.emitted.Load() >= uint32(len(c.ring)) {
		if err := c.throttle(); err != nil {
			return err
		}
	}
	seq := c.seq
	c.seq++
	b := c.take()
	b.add(opInlineDone, seq, false, nil)
	// An inline reply is suppressed the same way a shard-hop reply is: CLIENT
	// REPLY OFF drops a CLIENT ID or a pub/sub confirmation just like a GET. The
	// stamp rides the node to the writer, which records it into the silent ring
	// when it drains the node (opInlineDone takes the ordinary set path, unlike a
	// CompleteBlocked loopback).
	b.cmds[0].silent = c.silentNext
	r := Reply{b: b, i: 0}
	r.Raw(rep)
	c.published.Store(c.seq)
	if c.out.push(b) {
		c.wk.wake()
	}
	return nil
}

// DeliverOOB pushes an unsolicited reply onto this connection's wire from any
// goroutine: a pub/sub message a publisher's network thread fans out to a
// subscriber. It reuses the CompleteBlocked seam (outbound MPSC push plus a wake,
// both foreign-goroutine safe) but marks the node out-of-band, since a delivered
// message has no reserved pipeline sequence here; DrainReplies emits it at once.
// Delivery to an idle subscriber needs a writer that is not the reader (the pair
// shape or the reactor), because the single shape's one goroutine sits in Read
// with nobody on the waker; that is the push-mode driver requirement doc 08
// carries.
func (c *Conn) DeliverOOB(rep []byte) {
	b := c.take()
	b.oob = true
	b.add(opBlockDone, 0, false, nil)
	r := Reply{b: b, i: 0}
	r.Raw(rep)
	if c.out.push(b) {
		c.wk.wake()
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

// spinBudget is how many idle re-check iterations the writer runs before it
// parks. Below the live-connection high-water it is the full calibrated
// spinIters, which catches the next reply without a futex wake while cores sit
// idle. At or above the high-water the box is saturated and the spin only steals
// cores the shard workers need, so it collapses to 0 and the writer parks
// immediately (see connSpinHighWater and labs/f3/m0/22_conn_spin).
func (c *Conn) spinBudget() int {
	if int(c.rt.live.Load()) >= connSpinHighWater {
		return 0
	}
	return spinIters
}

// idleOnce is one spin-then-park turn of the writer's wait, the section 9.1
// protocol with the same lost-wake guard the worker's idle carries.
func (c *Conn) idleOnce() {
	c.wk.state.Store(stateSpinning)
	// Calibrated iteration count instead of a time.Now per turn; see
	// spinIters in tuning.go. spinBudget collapses to 0 once the box is
	// saturated so the writer parks at once (the loop is skipped).
	for i := 0; i < c.spinBudget(); i++ {
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
