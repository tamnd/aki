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

	// fans marks which commands are fan-out sub-commands: a non-nil entry is
	// the coordinator the writer merges that command's partial into instead of
	// emitting it. The slice is nil until a connection's first fan-out and the
	// hasFan flag keeps reset free on the point-op path.
	fans   []*fanCmd
	hasFan bool

	// streams marks which commands answered with a streamed reply: a non-nil
	// entry is the stream the writer serves in that command's pipeline slot.
	// Same lazy-slice-plus-flag shape as fans.
	streams   []*stream
	hasStream bool
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
	if b.hasFan {
		for i := range b.fans {
			b.fans[i] = nil
		}
		b.hasFan = false
	}
	if b.hasStream {
		for i := range b.streams {
			b.streams[i] = nil
		}
		b.hasStream = false
	}
	b.n = 0
	b.sn = 0
	b.data = b.data[:0]
	b.rep = b.rep[:0]
	// A node that carried a giant chunked-band command must not pin its grown
	// buffers forever; anything past the keep cap shrinks back.
	if cap(b.data) > keepNodeBytes {
		b.data = make([]byte, 0, batchDataCap)
	}
	if cap(b.rep) > keepNodeBytes {
		b.rep = make([]byte, 0, repCap)
	}
}

// setFan marks command i as a fan-out sub-command owned by fc.
func (b *hopBatch) setFan(i int, fc *fanCmd) {
	if b.fans == nil {
		b.fans = make([]*fanCmd, batchCap)
	}
	b.fans[i] = fc
	b.hasFan = true
}

// fan returns command i's coordinator, or nil for an ordinary command.
func (b *hopBatch) fan(i int) *fanCmd {
	if !b.hasFan {
		return nil
	}
	return b.fans[i]
}

// setStream marks command i as answered by a streamed reply.
func (b *hopBatch) setStream(i int, st *stream) {
	if b.streams == nil {
		b.streams = make([]*stream, batchCap)
	}
	b.streams[i] = st
	b.hasStream = true
}

// stream returns command i's streamed reply, or nil for an inline one.
func (b *hopBatch) stream(i int) *stream {
	if !b.hasStream {
		return nil
	}
	return b.streams[i]
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
