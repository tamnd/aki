package shard

import "sync/atomic"

// Strict ack delivery (spec 2064/obs1 doc 04 section 3.2). A write on a
// strict connection emits its frames, writes its reply, and then the reply
// parks here instead of emitting: the slot goes parked like a blocking
// verb's, the reorder cursor stalls at its sequence so pipelined commands
// behind it keep executing but answer in order, and the commit watermark
// callback delivers the held bytes through Conn.CompleteBlocked, the same
// loopback a BLPOP wake rides. No reader-side barrier is armed: unlike a
// blocking verb, a strict write finishes on the owner immediately, so
// later commands are free to run and only their output waits.

// strictHold parks command i's reply on its emitted marks' chain commit.
// Called from executeCmd only when the command accumulated marks, which
// implies a wired log and a strict connection at emission time.
func (w *worker) strictHold(b *hopBatch, i int) {
	if w.cx.parkFull {
		// The handler parked on backpressure and wrote no reply; the retry
		// that completes the command re-runs the handler and lands back here
		// with the retry's own marks.
		return
	}
	if b.fan(i) != nil {
		// A fan sub-command's partial merges on the connection writer, so
		// holding the slot here would stall the gather forever. Strict fan
		// acks land in the next slice; until then a fan write on a strict
		// connection acks relaxed, disclosed in the slice notes.
		return
	}
	if b.blocked(i) || b.stream(i) != nil {
		// No held bytes to deliver: the handler parked itself or streamed.
		// No registered write handler does either after emitting; this is a
		// safety rail, not a path.
		return
	}
	conn := b.conn
	seq := b.cmds[i].seq
	rep := append([]byte(nil), b.reply(i)...)
	b.setParked(i)
	marks := w.cx.marks
	if len(marks) == 1 {
		w.cx.Log.NotifyCommitted(marks[0].Group, marks[0].Seq, func() {
			conn.CompleteBlocked(seq, rep)
		})
		return
	}
	// A command that emitted to several groups acks on the last covering
	// commit: one countdown shared by the callbacks, each of which fires
	// exactly once. No point command does this today (one key, one group);
	// the countdown is here so a future multi-group emitter inherits the
	// right semantics instead of acking on its first group.
	left := new(atomic.Int32)
	left.Store(int32(len(marks)))
	for _, m := range marks {
		w.cx.Log.NotifyCommitted(m.Group, m.Seq, func() {
			if left.Add(-1) == 0 {
				conn.CompleteBlocked(seq, rep)
			}
		})
	}
}
