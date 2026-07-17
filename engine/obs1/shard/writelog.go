package shard

// The durability seam (spec 2064/obs1 doc 04 sections 1 and 3.1). A write
// handler applies to RAM through the store as before, then emits the op's
// post-decision effect frame through the Ctx log, then writes its reply.
// With a log wired, the reply IS the relaxed ack: the frame sits in the
// group's WAL buffer when the client hears OK, and the flusher makes it
// durable behind the ack. With no log wired the runtime serves volatile,
// f3 parity, and the emission calls cost one nil check.
//
// The interface lives here because the command packages import shard and
// nothing else obs1; the composed implementation (flusher, committer,
// watermarks) is obs1.WriteLog one package up, wired in through
// Runtime.SetWriteLog before Start.

// WriteLog is what a write handler sees of the durability pipeline. Every
// method is owner-goroutine safe under the group-to-shard mapping: a group
// belongs to exactly one shard, so per-group state inside the
// implementation has a single writer, and cross-group admission is the
// implementation's own lock.
//
// An emission returns a nil error when the frame is buffered (the relaxed
// ack point) along with the frame's mark, the group and seq it took, which
// is what a strict ack later parks on. A non-nil error's text is the
// client reply, already in wire form ("ERR ..."): the handler fails the
// command with it and acks nothing, the doc 04 section 10 rule that the
// owner never acks what it could not frame. The mark is scalars rather
// than a shared struct so the implementation satisfies the seam without
// importing this package.
type WriteLog interface {
	// StrSet records the resulting value of a string write: SET verbatim,
	// the INCR family and APPEND and SETRANGE as the value the store now
	// holds (frames carry post-decision effects, doc 04 section 2).
	// expireAtMs is the absolute deadline riding on the key after the
	// write, 0 none; counter marks the INCR family for the doc 08 counter
	// encoding.
	StrSet(key, value []byte, expireAtMs int64, counter bool) (group uint16, seq uint64, err error)

	// KeyDel records a key removal of any type.
	KeyDel(key []byte) (group uint16, seq uint64, err error)

	// NotifyCommitted runs fn once a chain commit covering the marked
	// frame has landed and folded live (doc 04 section 3.2): it registers
	// on the committed watermark and raises barrier demand, so a pending
	// strict ack rides the barrier floor rather than the age trigger. An
	// already-covered mark fires fn before the call returns, on the
	// caller's goroutine; otherwise fn runs on the fold goroutine, so it
	// must not block (Conn.CompleteBlocked is safe from either). A mark
	// emitted under an epoch that has since been fenced never folds live,
	// so its fn never fires; the owner learns of the fence through the
	// lease machinery and fails the connection, not through this seam.
	NotifyCommitted(group uint16, seq uint64, fn func())
}

// WALMark names one emitted frame, the completion target a strict ack
// waits on: the group whose buffer took it and the per-group seq it drew.
type WALMark struct {
	Group uint16
	Seq   uint64
}

// LogStrSet emits a strset effect frame when a log is wired, and is free
// when none is. See WriteLog.StrSet for the contract; the mark lands on
// the Ctx when the connection asked for strict acks.
func (cx *Ctx) LogStrSet(key, value []byte, expireAtMs int64, counter bool) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.StrSet(key, value, expireAtMs, counter)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogKeyDel emits a keydel effect frame when a log is wired, and is free
// when none is.
func (cx *Ctx) LogKeyDel(key []byte) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.KeyDel(key)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// noteMark records an emitted frame on the running command when its
// connection asked for strict acks, one mark per touched group holding
// the group's highest seq: per-group seqs are monotone under the single
// owner, so waiting on the last frame covers every earlier one. A relaxed
// connection (the default, and every fan partial on one today) skips the
// append, so the relaxed path pays one atomic load per emission.
func (cx *Ctx) noteMark(group uint16, seq uint64) {
	if cx.curConn == nil || !cx.curConn.strictAck.Load() {
		return
	}
	for i := range cx.marks {
		if cx.marks[i].Group == group {
			cx.marks[i].Seq = seq
			return
		}
	}
	cx.marks = append(cx.marks, WALMark{Group: group, Seq: seq})
}

// GroupOfSlot maps a hash slot to its contiguous group among g groups,
// the doc 02 section 1.2 route Runtime.GroupOf uses. Exported so a
// WriteLog built outside a runtime (the server layer, tests) keys its
// per-group state on the same mapping the dispatcher routes by.
func GroupOfSlot(slot, g int) int {
	return groupOfSlot(slot, g)
}

// SetWriteLog wires the durability seam into every owner Ctx. Fixed
// before Start like the handler table: workers read cx.Log with plain
// loads on their own goroutines.
func (r *Runtime) SetWriteLog(l WriteLog) {
	if r.started {
		panic("shard: SetWriteLog after Start")
	}
	r.wlog = l
	for _, w := range r.workers {
		w.cx.Log = l
	}
}

// WriteLogged reports whether the connection's runtime has a durability
// pipeline wired, the dispatch-side guard that refuses a strict-ack
// request on a volatile node: with no log there are no frames and no
// watermark, so a strict write would hang forever instead of being
// stricter. Reads a fixed-before-Start field, safe from any goroutine.
func (c *Conn) WriteLogged() bool { return c.rt.wlog != nil }

// SetStrictAck flips the connection's ack mode (doc 04 section 3.2,
// AKI.DURABILITY): strict holds each write's reply until the chain
// commit covering its frames, relaxed (the default) acks on the buffer
// append. The dispatcher sets it reader-side before enqueueing the
// toggle's own reply, so every command it dispatches afterwards observes
// the new mode; a write already in flight on an owner may observe it
// early, which upgrades (or on a relaxed flip, relaxes) that one
// command's ack, the documented pipelining caveat.
func (c *Conn) SetStrictAck(on bool) { c.strictAck.Store(on) }

// StrictAck reports the connection's ack mode.
func (c *Conn) StrictAck() bool { return c.strictAck.Load() }

// SetWALInfo registers the durability INFO renderer: the function
// receives the stats text built so far and appends its own section
// (obs1.WriteLog.AppendInfo renders "# Durability"). Fixed before Start
// like SetNetInfo; it renders after the f3 section and before the
// transport's.
func (r *Runtime) SetWALInfo(f func([]byte) []byte) {
	if r.started {
		panic("shard: SetWALInfo after Start")
	}
	r.walInfo = f
}
