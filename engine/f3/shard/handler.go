package shard

import (
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// Ctx is what a handler sees of its shard: the owner's store, the batch's
// cached clock, and a value scratch buffer whose grown capacity carries across
// commands. A handler runs on the owner goroutine; everything here is plain
// single-owner state, which is the whole F1 point.
type Ctx struct {
	// St is the shard's store. The handler is running on the owner, so every
	// call is a plain single-threaded call.
	St *store.Store

	// NowMs is the batch's cached unix-ms clock (doc 09 section 2): read once
	// per batch, shared by every expiry comparison in it, never mid-batch.
	NowMs int64

	// Val is the shard's value scratch: a handler that needs a copy of a value
	// reads into Val[:0] and stores the result back, so the capacity is reused
	// and the steady path allocates nothing.
	Val []byte

	// Aux is a second scratch for handlers that need one while Val is busy:
	// the fan-out partial builders assemble their encoding here while reading
	// values through Val. Same reuse contract as Val.
	Aux []byte

	// Coll is the shard's owner-local collection state: the per-key native
	// structures a type keeps outside the string store. It lives for the
	// worker's life like the rest of Ctx and is only ever touched by the owner
	// goroutine, so a type stashes a plain map or struct here with no lock. The
	// set type (spec 2064/f3/11 M1) is the first user; it holds its per-key
	// registry here. A second collection type sharing the slot lands with the
	// shared holder the keyspace-unification slice introduces.
	Coll any

	// ZColl is the same owner-local slot for the zset type (spec 2064/f3/12
	// M2): the zset registry hangs here so a zset command and a set command on
	// the same shard do not fight over Coll. It is the temporary parallel field
	// the Coll comment anticipates; the keyspace-unification slice folds both
	// into one shared holder. Owner-only, so no lock.
	ZColl any

	// w is the owning worker, the seam FanOut (donate.go) reaches the pool
	// through. It is nil on a bare Ctx built outside a runtime (tests), where
	// FanOut runs its tasks inline.
	w *worker

	// curConn and curSeq name the command the worker is running right now: its
	// originating connection and its per-connection reply sequence. The worker
	// sets them just before each handler call, so a command that decides to
	// block reads its own completion target through CurConn and CurSeq and hands
	// them to whatever later owner step (a serving push, a firing timer) finishes
	// the reply. They are valid only for the duration of the handler call and
	// only on the owner goroutine; nothing off the owner may read them.
	curConn *Conn
	curSeq  uint32

	// parkFull and retrying carry the block-not-drop backpressure decision across
	// the handler boundary (backpressure.go). A write handler that cannot allocate
	// calls ParkFull, which sets parkFull; the worker reads it once after every
	// handler returns (one bool load, zero cost when unset) and registers the
	// command on the shard's full-waiter FIFO instead of pushing a reply. retrying
	// is set while the FIFO is being re-run so a re-park is reported to the retry
	// driver rather than self-registering a second time.
	parkFull bool
	retrying bool

	// resume is the argument index a retried multi-key write picks up from
	// (backpressure.go). A fan sub-command that parks part-way through its pairs
	// records how far it committed through ParkFullAt; the worker seeds this before
	// the retry re-runs the handler, so MSetShard restarts at the first unwritten
	// pair instead of re-applying the committed prefix. Re-applying a committed pair
	// would write a fresh record and strand the old one as dead bytes, re-consuming
	// the very space the drain just freed and starving the failing pair forever, so
	// resume-from-first-unwritten is a liveness requirement, not just an
	// optimization. A fresh call runs at 0; single-key writes never set it. Owner
	// goroutine only.
	resume int
}

// ParkFull declares a write cannot allocate right now and parks it for retry
// when the cold migrator frees room (spec 2064/f3/06 section 8): the command
// produces no reply now, and the worker holds its batch until a later boundary
// re-runs the handler against a reclaimed arena or a genuine stall surfaces the
// OOM reply. It parks only on store.ErrFull; any other error (or nil) returns
// false so the handler writes its own reply as before. On a bare Ctx built
// outside a runtime (cx.w == nil, the test-built Ctx) it returns false too, so a
// handler with no owner to park at falls back to writing the error, keeping the
// contract total. Owner goroutine only.
func (cx *Ctx) ParkFull(err error) bool {
	return cx.ParkFullAt(err, 0)
}

// ParkFullAt is ParkFull for a multi-key write that has already committed some of
// its pairs before it ran out of room: resume is the argument index the retry
// must restart at, so the committed prefix is not re-applied. It parks only on
// store.ErrFull; any other error (or nil), or a bare Ctx with no owner, returns
// false so the handler writes its own reply. The single-key ParkFull is this with
// resume 0. Owner goroutine only.
func (cx *Ctx) ParkFullAt(err error, resume int) bool {
	if err != store.ErrFull || cx.w == nil {
		return false
	}
	cx.parkFull = true
	cx.resume = resume
	return true
}

// ResumeIndex is the argument index a retried multi-key write resumes from, 0 on
// a fresh call (the resume field). A fan sub-command reads it to skip the pairs it
// committed before it parked. Owner goroutine only.
func (cx *Ctx) ResumeIndex() int { return cx.resume }

// BackpressureWaits is the cumulative number of writes this shard has parked on a
// full arena (backpressure.go), the count INFO surfaces as backpressure_waits.
// Zero on a bare Ctx with no worker. Owner goroutine only.
func (cx *Ctx) BackpressureWaits() uint64 {
	if cx.w == nil {
		return 0
	}
	return cx.w.bpWaits
}

// BackpressureStalls is the cumulative number of parked writes this shard has
// failed with the OOM reply after a genuine stall (backpressure.go), the count
// INFO surfaces as backpressure_stalls. Zero on a bare Ctx with no worker. Owner
// goroutine only.
func (cx *Ctx) BackpressureStalls() uint64 {
	if cx.w == nil {
		return 0
	}
	return cx.w.bpStalls
}

// CurConn is the connection the running command belongs to, the completion
// target a blocking command captures for its later CompleteBlocked. Owner
// goroutine only, valid only during the handler call.
func (cx *Ctx) CurConn() *Conn { return cx.curConn }

// CurSeq is the running command's per-connection reply sequence, the slot its
// deferred reply must land at when a later owner step completes it. Owner
// goroutine only, valid only during the handler call.
func (cx *Ctx) CurSeq() uint32 { return cx.curSeq }

// ArmTimer schedules fire to run on this shard's owner at deadlineMs (unix-ms),
// the deadline a blocking command with a finite timeout sets so its timeout
// reply is delivered even if no serving push arrives. It returns a handle the
// command cancels with CancelTimer when it is served first. Owner goroutine
// only. On a bare Ctx built outside a runtime (cx.w == nil, the test-built Ctx)
// it arms nothing and returns nil; the real driver always has cx.w set, and
// CancelTimer is a no-op on a nil handle, so the contract stays total.
func (cx *Ctx) ArmTimer(deadlineMs int64, fire func(cx *Ctx)) *timer {
	if cx.w == nil {
		return nil
	}
	return cx.w.timers.push(deadlineMs, fire)
}

// CancelTimer removes a timer the command armed, when the command is served
// before its deadline. It is idempotent and nil-safe: a handle already fired or
// already cancelled, or the nil ArmTimer returns on a bare Ctx, is a no-op.
// Owner goroutine only.
func (cx *Ctx) CancelTimer(t *timer) {
	if cx.w == nil || t == nil {
		return
	}
	cx.w.timers.remove(t)
}

// Runtime is this shard's runtime, the seam a cross-shard blocking serve reaches
// PostOwner and ShardOf through. It is nil on a bare Ctx built outside a runtime
// (tests), the same total contract ArmTimer follows; a caller that may run on a
// bare Ctx checks for nil (or routes through ShardOf, which reports the
// single-shard degenerate answer). Owner goroutine only.
func (cx *Ctx) Runtime() *Runtime {
	if cx.w == nil {
		return nil
	}
	return cx.w.rt
}

// ShardID is this owner's shard index, the source shard a cross-shard blocking
// waiter records so a serving owner can skip its own cancel hop. It is -1 on a
// bare Ctx (cx.w == nil), which no real shard index equals, so a nil-runtime
// remote check reads false and the co-located path is taken. Owner goroutine
// only.
func (cx *Ctx) ShardID() int {
	if cx.w == nil {
		return -1
	}
	return cx.w.id
}

// ShardOf reports the owner shard of key, the dispatch hash a serving owner uses
// to tell a co-located destination from a remote one. It is -1 on a bare Ctx, so
// a remote check against ShardID (also -1 there) reads equal and stays on the
// co-located path. Owner goroutine only.
func (cx *Ctx) ShardOf(key []byte) int {
	if cx.w == nil {
		return -1
	}
	return cx.w.rt.ShardOf(key)
}

// Handler executes one command against its shard. args are views into the hop
// node, valid for the duration of the call; a keyed command's args[0] is its
// key. Exactly one reply must be written through r.
type Handler func(cx *Ctx, args [][]byte, r Reply)

// Reply writes one command's reply into its batch node's reply arena, in RESP
// wire form through the resp emitters, and records the span. Value type on
// purpose: it is two words and never escapes.
type Reply struct {
	b *hopBatch
	i int
}

func (r Reply) span(off int) {
	r.b.reps[r.i] = repSpan{off: uint32(off), len: uint32(len(r.b.rep) - off)}
}

// Status writes a simple string reply: +s.
func (r Reply) Status(s string) {
	off := len(r.b.rep)
	r.b.rep = resp.AppendStatus(r.b.rep, s)
	r.span(off)
}

// Err writes an error reply; msg carries its own code prefix ("ERR ...").
func (r Reply) Err(msg string) {
	off := len(r.b.rep)
	r.b.rep = resp.AppendError(r.b.rep, msg)
	r.span(off)
}

// errBytes is Err for a message already in byte form, the OpError path.
func (r Reply) errBytes(msg []byte) {
	off := len(r.b.rep)
	r.b.rep = resp.AppendErrorBytes(r.b.rep, msg)
	r.span(off)
}

// Int writes an integer reply: :n.
func (r Reply) Int(n int64) {
	off := len(r.b.rep)
	r.b.rep = resp.AppendInt(r.b.rep, n)
	r.span(off)
}

// Bulk writes a bulk string reply.
func (r Reply) Bulk(v []byte) {
	off := len(r.b.rep)
	r.b.rep = resp.AppendBulk(r.b.rep, v)
	r.span(off)
}

// Resp3 reports whether the originating connection negotiated RESP3, so a handler
// (or a type-package reply builder it calls) can pick the RESP3 frame shape over
// its RESP2 sibling. A batch with no connection (an internal replay path) reads
// false, the RESP2 default.
func (r Reply) Resp3() bool { return r.b.conn != nil && r.b.conn.Resp3() }

// Null writes the null reply in the connection's protocol: the RESP2 null bulk
// $-1, or the RESP3 null _ when the connection negotiated RESP3.
func (r Reply) Null() {
	off := len(r.b.rep)
	if r.b.conn != nil && r.b.conn.Resp3() {
		r.b.rep = resp.AppendNull3(r.b.rep)
	} else {
		r.b.rep = resp.AppendNull(r.b.rep)
	}
	r.span(off)
}

// Double writes a numeric reply in the connection's protocol: a RESP2 bulk string
// of the redis-formatted score digits, or a RESP3 double (,digits) when the
// connection negotiated RESP3. The digits are FormatScore's, so ZSCORE and the
// withscores pairs carry the same value bytes under both protocols. This is the
// addReplyDouble path (ZSCORE, ZINCRBY, ZADD INCR); INCRBYFLOAT keeps its own
// long-double digits and reframes them through DoubleBytes instead.
func (r Reply) Double(v float64) {
	off := len(r.b.rep)
	if r.b.conn != nil && r.b.conn.Resp3() {
		r.b.rep = resp.AppendDouble(r.b.rep, v)
	} else {
		var sc [40]byte
		r.b.rep = resp.AppendBulk(r.b.rep, resp.FormatScore(sc[:0], v))
	}
	r.span(off)
}

// DoubleBytes writes an already-formatted numeric reply, reusing the caller's
// digits rather than reformatting: a RESP2 bulk string, or a RESP3 double
// (,digits) under RESP3. INCRBYFLOAT and HINCRBYFLOAT compute their own digits and
// reframe them here so the RESP3 form carries the identical bytes the RESP2 bulk
// would, only the frame differing.
func (r Reply) DoubleBytes(digits []byte) {
	off := len(r.b.rep)
	if r.b.conn != nil && r.b.conn.Resp3() {
		r.b.rep = resp.AppendDoubleBytes(r.b.rep, digits)
	} else {
		r.b.rep = resp.AppendBulk(r.b.rep, digits)
	}
	r.span(off)
}

// Bool writes a predicate reply in the connection's protocol: a RESP2 integer
// (:1/:0), or a RESP3 boolean (#t/#f) when the connection negotiated RESP3. It is
// the addReplyBool path: SISMEMBER, the EXPIRE family, SETNX, HSETNX, PERSIST,
// COPY, RENAMENX, and the SMISMEMBER elements.
func (r Reply) Bool(v bool) {
	off := len(r.b.rep)
	if r.b.conn != nil && r.b.conn.Resp3() {
		r.b.rep = resp.AppendBool(r.b.rep, v)
	} else {
		n := int64(0)
		if v {
			n = 1
		}
		r.b.rep = resp.AppendInt(r.b.rep, n)
	}
	r.span(off)
}

// Park declares this command produced no reply now: it decided to block, and a
// later owner step (a serving push, a firing timer) delivers its reply at this
// sequence through conn.CompleteBlocked. The slot is marked so DrainReplies
// skips it without advancing the reorder cursor, stalling every later reply in
// the ring until the parked sequence is completed. No span is recorded, so
// reply(i) is never read for a parked slot. Owner goroutine only; exactly one of
// Park or a reply-writing method per command.
func (r Reply) Park() { r.b.setParked(r.i) }
