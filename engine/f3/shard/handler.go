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

// Null writes the RESP2 null bulk.
func (r Reply) Null() {
	off := len(r.b.rep)
	r.b.rep = resp.AppendNull(r.b.rep)
	r.span(off)
}
