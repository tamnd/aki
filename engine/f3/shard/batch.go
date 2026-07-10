package shard

import (
	"strconv"
	"sync/atomic"
)

// The op codes the M0 runtime executes. PING and ECHO are the smoke surface;
// GET and SET exist so the batch drain has real index traffic to prefetch and
// the zero-alloc assertion has a keyed path to pin. The string point surface
// with its option forms is its own M0 slice on top of these.
const (
	OpPing byte = iota + 1
	OpEcho
	OpGet
	OpSet
	// OpError carries a parse-side error through the hop so an error reply
	// keeps its place in the connection's pipeline order. The argument is the
	// message after the "-ERR " prefix.
	OpError
)

// hopCmd is one routed command inside a batch node: the per-connection reply
// sequence, the op, and key/argument spans into the node's data buffer.
type hopCmd struct {
	seq  uint32
	op   byte
	koff uint32
	klen uint32
	aoff uint32
	alen uint32
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

	n    uint16
	dlen uint32
	cmds [batchCap]hopCmd
	data [batchDataCap]byte

	// The reply side, written by the owning worker before the outbound push.
	// rep is the node's reply arena: replies land in RESP wire form, in
	// command order, located by reps.
	reps [batchCap]repSpan
	rep  []byte
}

func newBatch() *hopBatch {
	return &hopBatch{rep: make([]byte, 0, repCap)}
}

// reset readies a recycled node for its next fill, keeping the reply buffer's
// grown capacity.
func (b *hopBatch) reset() {
	b.n = 0
	b.dlen = 0
	b.rep = b.rep[:0]
}

// add appends one command, copying key and arg into the node's data buffer.
// It reports false when the node is out of command slots or data bytes, the
// signal to push this node and start a fresh one.
func (b *hopBatch) add(op byte, seq uint32, key, arg []byte) bool {
	if int(b.n) == batchCap {
		return false
	}
	need := uint32(len(key) + len(arg))
	if b.dlen+need > batchDataCap {
		return false
	}
	c := &b.cmds[b.n]
	c.seq = seq
	c.op = op
	c.koff = b.dlen
	c.klen = uint32(copy(b.data[b.dlen:], key))
	c.aoff = b.dlen + c.klen
	c.alen = uint32(copy(b.data[c.aoff:], arg))
	b.dlen += need
	b.n++
	return true
}

func (b *hopBatch) key(i int) []byte {
	c := &b.cmds[i]
	return b.data[c.koff : c.koff+c.klen]
}

func (b *hopBatch) arg(i int) []byte {
	c := &b.cmds[i]
	return b.data[c.aoff : c.aoff+c.alen]
}

// reply returns command i's wire bytes, valid until the node is recycled.
func (b *hopBatch) reply(i int) []byte {
	r := b.reps[i]
	return b.rep[r.off : r.off+r.len]
}

// The reply builders. Each writes one command's RESP bytes into the node's
// reply arena and records the span; replies are built once, in wire form, in
// command order (F19's discipline, minus the presizing the RESP2 slice adds).

func (b *hopBatch) replyStatic(i int, s string) {
	off := uint32(len(b.rep))
	b.rep = append(b.rep, s...)
	b.reps[i] = repSpan{off: off, len: uint32(len(b.rep)) - off}
}

func (b *hopBatch) replyBulk(i int, v []byte) {
	off := uint32(len(b.rep))
	b.rep = append(b.rep, '$')
	b.rep = strconv.AppendInt(b.rep, int64(len(v)), 10)
	b.rep = append(b.rep, '\r', '\n')
	b.rep = append(b.rep, v...)
	b.rep = append(b.rep, '\r', '\n')
	b.reps[i] = repSpan{off: off, len: uint32(len(b.rep)) - off}
}

func (b *hopBatch) replyError(i int, msg []byte) {
	off := uint32(len(b.rep))
	b.rep = append(b.rep, "-ERR "...)
	b.rep = append(b.rep, msg...)
	b.rep = append(b.rep, '\r', '\n')
	b.reps[i] = repSpan{off: off, len: uint32(len(b.rep)) - off}
}
