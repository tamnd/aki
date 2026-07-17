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
// Called from executeCmd only when the command accumulated marks and neither
// parked on backpressure nor scattered as a fan sub-command (the worker
// routes those two first), which implies a wired log, a strict connection at
// emission time, and a written reply in the slot.
func (w *worker) strictHold(b *hopBatch, i int) {
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

// holdFan parks a gathered fan reply on the coordinator's accumulated marks,
// the fan half of the strict contract: the sub-commands' partials merged on
// this writer goroutine, so the hold happens here, when the last partial has
// landed and the reply is assembled. The sequence stays unemitted, stalling
// the reorder cursor exactly as a point hold does, and the covering commits
// deliver the bytes through the same CompleteBlocked loopback. An
// already-covered mark fires inline on this goroutine; the loopback node it
// pushes lands on the outbound queue the current drain pass is still
// popping, so the reply still arrives in this pass. The coordinator is
// dropped after this call and only the closures keep its reply alive.
func (c *Conn) holdFan(seq uint32, fc *fanCmd) {
	rep := fc.out
	marks := fc.marks
	log := c.rt.wlog
	if len(marks) == 1 {
		log.NotifyCommitted(marks[0].Group, marks[0].Seq, func() {
			c.CompleteBlocked(seq, rep)
		})
		return
	}
	left := new(atomic.Int32)
	left.Store(int32(len(marks)))
	for _, m := range marks {
		log.NotifyCommitted(m.Group, m.Seq, func() {
			if left.Add(-1) == 0 {
				c.CompleteBlocked(seq, rep)
			}
		})
	}
}

// mergeMarks folds src into dst with noteMark's coalescing rule: one mark
// per group, at the group's highest seq. Both sides are tiny (one entry per
// touched WAL group), so the scan beats any map.
func mergeMarks(dst, src []WALMark) []WALMark {
outer:
	for _, m := range src {
		for j := range dst {
			if dst[j].Group == m.Group {
				if m.Seq > dst[j].Seq {
					dst[j].Seq = m.Seq
				}
				continue outer
			}
		}
		dst = append(dst, m)
	}
	return dst
}
