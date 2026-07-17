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
// A method returns nil when the frame is buffered (the relaxed ack point)
// and otherwise an error whose text is the client reply, already in wire
// form ("ERR ..."): the handler fails the command with it and acks
// nothing, the doc 04 section 10 rule that the owner never acks what it
// could not frame.
type WriteLog interface {
	// StrSet records the resulting value of a string write: SET verbatim,
	// the INCR family and APPEND and SETRANGE as the value the store now
	// holds (frames carry post-decision effects, doc 04 section 2).
	// expireAtMs is the absolute deadline riding on the key after the
	// write, 0 none; counter marks the INCR family for the doc 08 counter
	// encoding.
	StrSet(key, value []byte, expireAtMs int64, counter bool) error

	// KeyDel records a key removal of any type.
	KeyDel(key []byte) error
}

// LogStrSet emits a strset effect frame when a log is wired, and is free
// when none is. See WriteLog.StrSet for the contract.
func (cx *Ctx) LogStrSet(key, value []byte, expireAtMs int64, counter bool) error {
	if cx.Log == nil {
		return nil
	}
	return cx.Log.StrSet(key, value, expireAtMs, counter)
}

// LogKeyDel emits a keydel effect frame when a log is wired, and is free
// when none is.
func (cx *Ctx) LogKeyDel(key []byte) error {
	if cx.Log == nil {
		return nil
	}
	return cx.Log.KeyDel(key)
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
	for _, w := range r.workers {
		w.cx.Log = l
	}
}

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
