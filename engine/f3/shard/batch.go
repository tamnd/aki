package shard

import (
	"sync/atomic"
)

// OpError is the reserved op that carries a parse-side error through the hop,
// so an error reply keeps its place in the connection's pipeline order. Its
// single argument is the full message including the code prefix ("ERR ...").
// Every other op is a table position the runtime's registered handler vector
// gives meaning to; the shard layer never interprets them.
const OpError byte = 0xff

// hopCmd is one routed command inside a batch node: the per-connection reply
// sequence, the op, and the command's argument run inside the node's span
// table. A keyed command's first argument is its key, which is what routing
// and the prefetch stage read.
type hopCmd struct {
	seq   uint32
	op    byte
	keyed bool
	argn  uint16
	arg0  uint16
}

// span locates one argument inside the node's data buffer.
type span struct {
	off uint32
	len uint32
}

// repSpan locates one command's reply inside the node's reply buffer.
type repSpan struct {
	off uint32
	len uint32
}

// hopBatch is the unit both queue directions carry: a connection reader fills
// it and pushes it to one shard's inbound queue with a single atomic, the
// owner executes it and writes the replies into the same node, and the node
// travels back on the connection's outbound queue, so a batch costs one
// atomic each way and no allocation once the node exists.
type hopBatch struct {
	next atomic.Pointer[hopBatch] // MPSC link, owned by whichever queue holds the node
	conn *Conn                    // originating connection, reply routing

	n     uint16
	sn    uint16
	cmds  [batchCap]hopCmd
	spans [spanCap]span
	data  []byte

	// The reply side, written by the owning worker before the outbound push.
	// rep is the node's reply arena: replies land in RESP wire form, in
	// command order, located by reps.
	reps [batchCap]repSpan
	rep  []byte
}

func newBatch() *hopBatch {
	return &hopBatch{
		data: make([]byte, 0, batchDataCap),
		rep:  make([]byte, 0, repCap),
	}
}

// reset readies a recycled node for its next fill, keeping both buffers'
// grown capacity: a node that once carried an oversized command keeps the
// larger data buffer for its next life, the same rule the reply buffer has.
func (b *hopBatch) reset() {
	b.n = 0
	b.sn = 0
	b.data = b.data[:0]
	b.rep = b.rep[:0]
}

// add appends one command, copying its arguments into the node's data buffer.
// It reports false when the node is out of command slots, span slots, or data
// bytes, the signal to push this node and start a fresh one. A single command
// bigger than the data cap is admitted into an empty node by growing the
// buffer, up to maxCmdBytes; past that, add refuses even when empty and Do
// surfaces ErrTooBig.
func (b *hopBatch) add(op byte, seq uint32, keyed bool, args [][]byte) bool {
	if int(b.n) == batchCap {
		return false
	}
	if int(b.sn)+len(args) > spanCap {
		return false
	}
	need := 0
	for _, a := range args {
		need += len(a)
	}
	if len(b.data)+need > batchDataCap && b.n > 0 {
		return false
	}
	if need > maxCmdBytes {
		return false
	}
	c := &b.cmds[b.n]
	c.seq = seq
	c.op = op
	c.keyed = keyed
	c.argn = uint16(len(args))
	c.arg0 = b.sn
	for _, a := range args {
		off := uint32(len(b.data))
		b.data = append(b.data, a...)
		b.spans[b.sn] = span{off: off, len: uint32(len(a))}
		b.sn++
	}
	b.n++
	return true
}

// arg returns command i's argument k as a view into the node's data buffer.
func (b *hopBatch) arg(i, k int) []byte {
	s := b.spans[int(b.cmds[i].arg0)+k]
	return b.data[s.off : s.off+s.len]
}

// reply returns command i's wire bytes, valid until the node is recycled.
func (b *hopBatch) reply(i int) []byte {
	r := b.reps[i]
	return b.rep[r.off : r.off+r.len]
}
