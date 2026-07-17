package shard

import (
	"encoding/binary"

	"github.com/tamnd/aki/obs1srv/resp"
)

// Tier-one multi-key fan-out (spec 2064/f3/03 section 6.2): the reader splits
// a multi-key command into per-shard sub-commands that all carry one reply
// sequence, each owner executes its slice as ordinary single-key work in its
// next batch and answers with a partial, and the partials gather at the
// connection's reply-reordering layer, where the last one to arrive emits the
// single client reply at that sequence. No barrier, no ticket, per-key
// atomicity only, which is what the tier-one commands promise. The scatter
// and gather plumbing here is deliberately command-agnostic (a kind byte and
// a partial encoding), so the tier-two intent commands can reuse the
// coordinator record and the sub-command chunking when they land.

// FanKind selects a fan-out command's partial encoding and final reply shape.
type FanKind uint8

const (
	// FanCount sums 8-byte little-endian partial counts into one integer
	// reply: DEL, UNLINK, EXISTS.
	FanCount FanKind = iota + 1

	// FanOK expects empty partials and replies +OK; a non-empty partial is an
	// error message and the first one becomes the reply: MSET.
	FanOK

	// FanMGet gathers per-key values into a bulk array in argument order. The
	// sub-command carries a trailing positions argument (2 bytes per key,
	// little-endian) the gather side reads back off the returning node.
	FanMGet

	// FanStats sums fixed-width per-shard counter blobs and renders the
	// INFO-style text reply: the evidence surface the LTM harness reads.
	FanStats

	// FanTxn gathers a cross-shard tier-two command's arm partials
	// (txnroute.go) and emits nothing: the reply arrives later on the
	// transaction's loopback node at the same sequence, once the barrier work
	// has run.
	FanTxn

	// FanWaitAOF gathers WAITAOF's empty per-shard partials. The subs carry
	// no payload; their arrival is the point, proof every shard has executed
	// and emitted everything this connection enqueued before the WAITAOF,
	// per-shard FIFO. The merge then snapshots the write log's commit
	// barrier and parks the reply on the covering commits (waitaof.go).
	FanWaitAOF
)

// maxFanKeys caps the keys one sub-command carries, so a sub-command's
// argument run (keys, MSET values, the positions argument) always fits a
// node's span table. A shard with more keys gets several sub-commands, each
// its own partial.
const maxFanKeys = 100

// fanCmd is the coordinator record: created by the reader before any
// sub-command is published, referenced by every sub-command's node slot, and
// mutated only by the connection's writer goroutine as partials arrive, so
// every field past the construction is plain single-consumer state.
type fanCmd struct {
	kind    FanKind
	pending int32
	sum     int64
	vals    [][]byte
	present []bool
	stats   []uint64
	errMsg  []byte
	out     []byte

	// marks accumulates the WAL marks the sub-commands' partials carried
	// (batch.go fanMarks), coalesced per group at the group's highest seq.
	// Non-empty only for a strict connection's fan write; the gathered
	// reply then parks on the covering commits (strict.go holdFan) instead
	// of delivering.
	marks []WALMark

	// txn is the FanTxn coordinator's transaction: the arm builtin reads it
	// on the owner to find the intent its key enqueues. Set before the first
	// enqueue and immutable afterwards, so the owner-side read is ordered by
	// the hop queue's publish.
	txn *Txn

	// waitaof is the FanWaitAOF coordinator's validated argument set,
	// carried from the reader to the merge, which hands it to holdWaitAOF
	// when the last partial lands.
	waitaof *waitAOFSpec
}

// DoFan enqueues one tier-one multi-key command: keys are the routed keys in
// argument order; vals, when non-nil, are the parallel values (MSET). The
// whole command takes one reply sequence, and the reply is emitted only when
// every shard's partial has gathered. Sizes are validated up front so a
// too-big command enqueues nothing and keeps its pipeline slot for the error
// reply.
func (c *Conn) DoFan(op byte, kind FanKind, keys, vals [][]byte) error {
	for i := range keys {
		need := len(keys[i]) + 2
		if vals != nil {
			need += len(vals[i])
		}
		if need > maxCmdBytes {
			return ErrTooBig
		}
	}
	if c.seq-c.emitted.Load() >= uint32(len(c.ring)) {
		if err := c.throttle(); err != nil {
			return err
		}
	}

	shards := len(c.rt.workers)
	fc := &fanCmd{kind: kind}
	if kind == FanMGet {
		fc.vals = make([][]byte, len(keys))
		fc.present = make([]bool, len(keys))
	}

	// Build every sub-command first: one shard at a time, the shard's keys in
	// argument order, chunked under the per-sub caps. Order within a shard is
	// argument order, which is what per-shard sub-batches preserve through
	// the hop. The count must be final before the first enqueue: a partial
	// can come back and merge while the scatter is still publishing, so the
	// coordinator's countdown is set once here and touched only by the
	// writer afterwards.
	order := make([]int, len(keys))
	for i, k := range keys {
		order[i] = c.rt.ShardOf(k)
	}
	type fanSub struct {
		sh   int
		argv [][]byte
	}
	var subs []fanSub
	for sh := 0; sh < shards; sh++ {
		var argv [][]byte
		var pos []byte
		kn := 0
		bytes := 0
		flushSub := func() {
			if kn == 0 {
				return
			}
			if kind == FanMGet {
				argv = append(argv, pos)
			}
			subs = append(subs, fanSub{sh: sh, argv: argv})
			argv = nil
			pos = nil
			kn = 0
			bytes = 0
		}
		for i := range keys {
			if order[i] != sh {
				continue
			}
			if kn > 0 && (kn >= maxFanKeys || bytes > c.rt.batchDataCap) {
				flushSub()
			}
			argv = append(argv, keys[i])
			bytes += len(keys[i])
			if vals != nil {
				argv = append(argv, vals[i])
				bytes += len(vals[i])
			}
			if kind == FanMGet {
				pos = binary.LittleEndian.AppendUint16(pos, uint16(i))
				bytes += 2
			}
			kn++
		}
		flushSub()
	}
	fc.pending = int32(len(subs))
	for _, sub := range subs {
		if err := c.enqueueFan(sub.sh, op, sub.argv, fc); err != nil {
			return err
		}
	}
	c.seq++
	return nil
}

// DoFanAll enqueues one keyless sub-command per shard, the S-way scatter the
// stats surface rides: every shard answers a partial and the gather renders
// one reply.
func (c *Conn) DoFanAll(op byte, kind FanKind) error {
	if c.seq-c.emitted.Load() >= uint32(len(c.ring)) {
		if err := c.throttle(); err != nil {
			return err
		}
	}
	// The countdown is final before the first enqueue; see DoFan.
	fc := &fanCmd{kind: kind, pending: int32(len(c.rt.workers))}
	for sh := range c.rt.workers {
		if err := c.enqueueFan(sh, op, nil, fc); err != nil {
			return err
		}
	}
	c.seq++
	return nil
}

// enqueueFan is Do's node handling for one sub-command: same node, same
// spill-to-fresh-node rule, plus the coordinator pointer in the command's
// slot. Every sub-command of one fan shares the connection's current
// sequence; DoFan advances it once at the end.
func (c *Conn) enqueueFan(sh int, op byte, argv [][]byte, fc *fanCmd) error {
	b := c.pending[sh]
	if b == nil {
		b = c.take()
		c.pending[sh] = b
	}
	if !b.add(op, c.seq, true, argv) {
		c.flushShard(sh)
		b = c.take()
		c.pending[sh] = b
		if !b.add(op, c.seq, true, argv) {
			return ErrTooBig
		}
	}
	b.setFan(int(b.n)-1, fc)
	return nil
}

// mergeFan folds one arriving partial into its coordinator. It runs on the
// connection's writer goroutine, the single consumer of the outbound queue,
// so the coordinator mutations are plain. When the last partial lands the
// final reply is built and delivered through the reorder ring like any other
// reply, which is what keeps a fan-out ordered against the single-key traffic
// around it.
func (c *Conn) mergeFan(fc *fanCmd, seq uint32, b *hopBatch, i int, emit func([]byte)) int {
	part := b.reply(i)
	switch fc.kind {
	case FanCount:
		if len(part) == 8 {
			fc.sum += int64(binary.LittleEndian.Uint64(part))
		}
	case FanOK:
		if len(part) > 0 && fc.errMsg == nil {
			fc.errMsg = append([]byte(nil), part...)
		}
	case FanMGet:
		cmd := &b.cmds[i]
		pos := b.arg(i, int(cmd.argn)-1)
		for k := 0; len(part) >= 4; k++ {
			n := binary.LittleEndian.Uint32(part)
			part = part[4:]
			p := int(binary.LittleEndian.Uint16(pos[2*k:]))
			if n == fanNil {
				continue
			}
			fc.present[p] = true
			fc.vals[p] = append(fc.vals[p][:0], part[:n]...)
			part = part[n:]
		}
	case FanStats:
		if fc.stats == nil {
			fc.stats = make([]uint64, len(part)/8)
		}
		for k := 0; k+8 <= len(part) && k/8 < len(fc.stats); k += 8 {
			fc.stats[k/8] += binary.LittleEndian.Uint64(part[k:])
		}
	}
	if ms := b.fanMark(i); len(ms) != 0 {
		// Two sub-commands can mark the same group (a chunked shard, or the
		// log's slot-to-group fold), so the merge coalesces exactly as
		// noteMark does: per-group seqs are monotone, the highest covers all.
		fc.marks = mergeMarks(fc.marks, ms)
	}
	fc.pending--
	if fc.pending > 0 {
		return 0
	}
	if fc.kind == FanTxn {
		// The arms have all executed; the reply comes later on the
		// transaction's loopback node, so there is nothing to emit here.
		// No tier-two arm emits WAL frames today; when one does, its marks
		// must ride the loopback reply, not this coordinator.
		return 0
	}
	if fc.kind == FanWaitAOF {
		// Every shard has drained this connection's earlier commands, so
		// the commit snapshot taken now covers even writes pipelined ahead
		// of the WAITAOF. The reply parks on that coverage (or comes back
		// through the same loopback inline when already covered) and the
		// sequence stays unemitted, the holdFan rule.
		c.holdWaitAOF(seq, fc.waitaof)
		return 0
	}
	fc.out = fc.out[:0]
	switch fc.kind {
	case FanCount:
		fc.out = resp.AppendInt(fc.out, fc.sum)
	case FanOK:
		if fc.errMsg != nil {
			fc.out = resp.AppendErrorBytes(fc.out, fc.errMsg)
		} else {
			fc.out = resp.AppendStatus(fc.out, "OK")
		}
	case FanMGet:
		fc.out = resp.AppendArrayHeader(fc.out, len(fc.vals))
		for p := range fc.vals {
			if fc.present[p] {
				fc.out = resp.AppendBulk(fc.out, fc.vals[p])
			} else {
				fc.out = resp.AppendNull(fc.out)
			}
		}
	case FanStats:
		fc.out = c.rt.renderStats(fc.out, fc.stats)
	}
	if len(fc.marks) != 0 {
		// A strict fan write parks its assembled reply on the covering
		// commits: the sequence stays unemitted so the reorder cursor stalls
		// exactly as a point hold does, and the commit callbacks bring the
		// bytes back through the CompleteBlocked loopback. An error reply
		// (a failing pair after committed ones) holds the same way, so no
		// reply on a strict connection ever races the frames it follows.
		c.holdFan(seq, fc)
		return 0
	}
	return c.deliver(seq, fc.out, emit)
}

// fanNil is the absent-value length marker in an MGET partial.
const fanNil = 0xffffffff

// FanCount writes a partial count for a FanCount fan-out: 8 bytes,
// little-endian.
func (r Reply) FanCount(n int64) {
	off := len(r.b.rep)
	r.b.rep = binary.LittleEndian.AppendUint64(r.b.rep, uint64(n))
	r.span(off)
}

// FanOK writes the empty all-good partial for a FanOK fan-out.
func (r Reply) FanOK() {
	r.span(len(r.b.rep))
}

// FanErrString writes an error partial for a FanOK fan-out; the first error
// any shard reports becomes the command's reply.
func (r Reply) FanErrString(msg string) {
	off := len(r.b.rep)
	r.b.rep = append(r.b.rep, msg...)
	r.span(off)
}

// Raw writes an already encoded partial verbatim: the FanMGet and FanStats
// handlers build their encoding in the shard scratch and hand it over whole.
func (r Reply) Raw(part []byte) {
	off := len(r.b.rep)
	r.b.rep = append(r.b.rep, part...)
	r.span(off)
}

// AppendFanValue appends one MGET partial entry: a 4-byte little-endian
// length then the bytes, or the absent marker.
func AppendFanValue(dst, val []byte, present bool) []byte {
	if !present {
		return binary.LittleEndian.AppendUint32(dst, fanNil)
	}
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(val)))
	return append(dst, val...)
}
