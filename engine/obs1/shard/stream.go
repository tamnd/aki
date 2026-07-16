package shard

import (
	"runtime"
	"strconv"
	"sync/atomic"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The streaming reply path (spec 2064/f3/09 section 2): a chunked-band read
// never materializes its value. The owning worker pumps the value chunk by
// chunk into a small SPSC ring, and the connection writer consumes the ring
// straight onto the socket behind the bulk header, so a 512MiB GET holds
// streamWindow chunk buffers, not 512MiB. The worker stays live while it has
// streams to pump and never parks on them, so the two sides cannot deadlock:
// a full ring only ever waits on the consumer, an empty ring only on the
// producer, and a dead client fails the stream instead of wedging the shard.

// streamWindow is the ring depth: how many chunks may sit produced and
// unconsumed. The stream's peak footprint is streamWindow chunk buffers.
const streamWindow = 4

// StreamSource yields a value's bytes in chunks. Next fills dst (at least
// store.ChunkSize bytes) and returns the chunk's length, zero once the value
// is exhausted. It is called only on the shard's owner goroutine.
type StreamSource interface {
	Next(dst []byte) (int, error)
}

// stream is one in-flight streamed reply: the SPSC ring between the owning
// worker (producer) and the connection writer (consumer). prod and cons are
// the only shared words; each slot's bytes are published by the prod bump and
// released by the cons bump.
type stream struct {
	conn  *Conn
	src   StreamSource
	total int64

	// raw carries a reply that is already RESP on the wire: the source yields
	// the whole reply bytes (a multi-bulk array header and every element),
	// self-framed, so the writer emits no bulk header and no trailer around
	// them. A single chunked bulk value (GET) leaves raw clear and gets the
	// $-header wrapper; a streamed multi-bulk enumeration (SMEMBERS) sets it.
	raw bool

	bufs [streamWindow][]byte
	lens [streamWindow]int32
	prod atomic.Uint32
	cons atomic.Uint32

	// failed flips when the source errors, the value comes up short, the
	// client is gone, or the shard aborts on shutdown. Both sides poll it.
	failed atomic.Bool

	// Producer-side bookkeeping, owner goroutine only.
	produced int64
	done     bool
}

// Stream records a streamed bulk reply of total bytes served by src. The
// reply's wire bytes are not in the node; the connection writer emits the
// header and the chunks when this command's turn in the pipeline order comes.
func (r Reply) Stream(total int64, src StreamSource) {
	st := &stream{conn: r.b.conn, src: src, total: total}
	r.b.setStream(r.i, st)
	r.span(len(r.b.rep))
}

// StreamRaw records a streamed reply whose src yields total bytes of finished
// RESP: the array header and every element, already framed. The writer serves
// them verbatim, with no bulk header and no trailer, so a large multi-bulk
// enumeration (SMEMBERS over a big set) never materializes one giant reply
// buffer, only the bounded chunk window the ring holds.
func (r Reply) StreamRaw(total int64, src StreamSource) {
	st := &stream{conn: r.b.conn, src: src, total: total, raw: true}
	r.b.setStream(r.i, st)
	r.span(len(r.b.rep))
}

// finish marks the stream done on the producer side and releases whatever
// the source pinned: a store.ChunkStream pins its shard's arena against
// compaction while it lives, so the release must happen at every producer
// exit, and it runs on the owner goroutine like the pins themselves.
func (st *stream) finish() {
	st.done = true
	if r, ok := st.src.(interface{ Release() }); ok {
		r.Release()
	}
}

// pump runs the producer side once: fill ring slots until the ring is full or
// the value is exhausted. It returns true when this stream needs no more
// pumping (finished or failed). Owner goroutine only.
func (st *stream) pump() bool {
	if st.done {
		return true
	}
	if st.conn.closed.Load() || st.failed.Load() {
		// The client is gone or the consumer failed; stop reading chunks for
		// it. The consumer side observes failed and unwinds.
		st.failed.Store(true)
		st.finish()
		return true
	}
	for st.produced < st.total {
		p := st.prod.Load()
		if p-st.cons.Load() == streamWindow {
			return false // ring full, waiting on the consumer
		}
		slot := p % streamWindow
		if st.bufs[slot] == nil {
			st.bufs[slot] = make([]byte, store.ChunkSize)
		}
		n, err := st.src.Next(st.bufs[slot])
		if err != nil || n == 0 || st.produced+int64(n) > st.total {
			// A read failure or a source short of its declared total: the
			// bulk header may already be on the wire, so the reply cannot be
			// repaired, only failed.
			st.failed.Store(true)
			st.finish()
			return true
		}
		st.lens[slot] = int32(n)
		st.produced += int64(n)
		st.prod.Store(p + 1)
	}
	st.finish()
	return true
}

// crlf is the bulk trailer the consumer emits after the last chunk.
var crlf = []byte("\r\n")

// header appends the stream's bulk header through emit. Writer side only, and
// exactly once per stream: the header commits the wire to st.total bytes.
func (st *stream) header(emit func([]byte)) {
	if st.raw {
		// The source frames its own reply, so there is no bulk header to emit;
		// the reorder cursor still commits to st.total bytes.
		return
	}
	var hdr [32]byte
	h := append(hdr[:0], '$')
	h = strconv.AppendInt(h, st.total, 10)
	h = append(h, '\r', '\n')
	emit(h)
}

// consumeOne emits the ring's next ready chunk and reports its length; ok is
// false when the ring holds nothing right now. Writer side only.
func (st *stream) consumeOne(emit func([]byte)) (n int64, ok bool) {
	k := st.cons.Load()
	if st.prod.Load() == k {
		return 0, false
	}
	slot := k % streamWindow
	ln := st.lens[slot]
	emit(st.bufs[slot][:ln])
	st.cons.Store(k + 1)
	return int64(ln), true
}

// emitStream runs the consumer side to completion: the bulk header, every
// chunk in order, the trailer. It blocks (spinning with yields) while the
// ring is empty, because RESP replies are ordered and nothing after this
// reply may be emitted early. It reports false when the stream failed; the
// header was already sent by then, so the connection is unrecoverable and the
// caller must tear it down. Writer side only; this is the goroutine driver's
// shape, where blocking parks a per-connection goroutine and convoys nothing.
// A consumer that must not block steps instead (SetStreamStep, StreamStep).
func (c *Conn) emitStream(st *stream, emit func([]byte)) bool {
	st.header(emit)
	var sent int64
	for sent < st.total {
		n, ok := st.consumeOne(emit)
		if !ok {
			if st.failed.Load() {
				c.failed = true
				return false
			}
			runtime.Gosched()
			continue
		}
		sent += n
	}
	if !st.raw {
		emit(crlf)
	}
	return true
}
