// Package networking drives the RESP codec over real connections. It owns the
// per-connection lifecycle: the accept loop, the input (query) buffer, the
// command parse-and-dispatch loop, the output buffer, and graceful shutdown.
// It interprets no command itself; a Handler is the seam the command-dispatch
// layer fills (doc 07 §5, doc 19 §3 and §4).
package networking

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/resp"
)

// Handler processes one fully parsed client command. The argv slice holds the
// command name in argv[0] and its arguments after; the handler writes its reply
// through c.Enc (or c.WriteRaw for the pooled static replies). The networking
// layer never inspects argv; that is the dispatch layer's job.
//
// argv and its backing bytes are owned by the connection only for the duration
// of the call. A handler that retains them past return must copy.
type Handler interface {
	Handle(c *Conn, argv [][]byte)
}

// HandlerFunc adapts an ordinary function to Handler.
type HandlerFunc func(c *Conn, argv [][]byte)

// Handle calls f(c, argv).
func (f HandlerFunc) Handle(c *Conn, argv [][]byte) { f(c, argv) }

// readChunk is the size of a single socket read appended to the query buffer.
const readChunk = 16 * 1024

// Conn is one client connection. Everything on it is touched by a single
// goroutine (the read loop), so the per-connection state needs no locking; only
// the cross-goroutine close path is atomic.
type Conn struct {
	server *Server
	raw    net.Conn
	br     *bufio.Reader

	id    uint64
	addr  string
	laddr string

	// Query (input) buffer. qbuf holds bytes read from the socket; pos is the
	// parse offset within it. Consumed bytes are compacted off the front before
	// each socket read so the buffer does not grow without bound across a long
	// pipeline.
	qbuf []byte
	pos  int

	// Output buffer and the encoder that writes framed replies into it. One
	// flush per pipeline batch sends it to the socket.
	outBuf *bytes.Buffer
	enc    *resp.Encoder

	// writeMu serializes raw socket writes. The read loop's flush holds it, and
	// so does Deliver, which another goroutine calls to push a pub/sub message.
	// That keeps two writers from interleaving bytes on the wire.
	writeMu sync.Mutex

	// Connection state visible to handlers and to CLIENT introspection later.
	db   int
	name string

	// session is an opaque slot the command layer attaches its own per-connection
	// state to (auth, MULTI, subscriptions). The networking layer never inspects
	// it, so transport stays free of command semantics.
	session any

	// Lifetime counters.
	totNetIn  uint64
	totNetOut uint64
	totCmds   uint64

	created         time.Time
	lastInteraction time.Time

	// closeAfterReply is set by Quit: the loop flushes the pending output and
	// then returns, closing the connection. closed is the atomic kill switch the
	// server flips on shutdown or CLIENT KILL.
	closeAfterReply bool
	closed          atomic.Bool
}

// ID returns the globally unique, never-reused connection id.
func (c *Conn) ID() uint64 { return c.id }

// RemoteAddr returns the client address as "ip:port" or a Unix socket path.
func (c *Conn) RemoteAddr() string { return c.addr }

// LocalAddr returns the server-side address the client connected to.
func (c *Conn) LocalAddr() string { return c.laddr }

// DB returns the currently selected logical database index.
func (c *Conn) DB() int { return c.db }

// SetDB selects a logical database index, as SELECT does.
func (c *Conn) SetDB(db int) { c.db = db }

// Session returns the command-layer session object, or nil if none is attached.
func (c *Conn) Session() any { return c.session }

// SetSession attaches the command-layer session object.
func (c *Conn) SetSession(s any) { c.session = s }

// Name returns the connection name set by CLIENT SETNAME.
func (c *Conn) Name() string { return c.name }

// SetName sets the connection name.
func (c *Conn) SetName(name string) { c.name = name }

// Created returns the time the connection was accepted.
func (c *Conn) Created() time.Time { return c.created }

// LastInteraction returns the time of the most recent command on the connection.
func (c *Conn) LastInteraction() time.Time { return c.lastInteraction }

// TotCmds returns the number of commands processed on the connection.
func (c *Conn) TotCmds() uint64 { return c.totCmds }

// TotNetIn returns the total bytes read from the connection.
func (c *Conn) TotNetIn() uint64 { return c.totNetIn }

// TotNetOut returns the total bytes written to the connection.
func (c *Conn) TotNetOut() uint64 { return c.totNetOut }

// Proto reports the negotiated RESP version (2 or 3).
func (c *Conn) Proto() int { return c.enc.Proto() }

// SetProto switches the RESP version, as HELLO does. The change takes effect
// for every reply encoded after this call.
func (c *Conn) SetProto(proto int) { c.enc.SetProto(proto) }

// Enc returns the reply encoder bound to this connection's output buffer. It
// already carries the connection's protocol version, so a handler builds one
// logical reply and the encoder picks the RESP2 or RESP3 shape.
func (c *Conn) Enc() *resp.Encoder { return c.enc }

// WriteRaw appends pre-framed bytes to the output buffer, the path for the
// pooled static replies in package resp. The bytes must be a complete, correctly
// framed RESP value.
func (c *Conn) WriteRaw(p []byte) { c.outBuf.Write(p) }

// Deliver writes a complete, pre-framed RESP value straight to the socket from
// another goroutine, the path a PUBLISH on one connection uses to push a message
// to a subscriber on another. It holds the write lock so it cannot interleave
// with the subscriber's own reply flush. A write to a closed socket returns an
// error the caller can ignore: the connection is going away anyway.
func (c *Conn) Deliver(p []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed.Load() {
		return net.ErrClosed
	}
	n, err := c.raw.Write(p)
	if n > 0 {
		c.totNetOut += uint64(n)
	}
	return err
}

// Quit asks the loop to flush the current output and then close the connection,
// the behaviour of the QUIT command. The reply already written stands.
func (c *Conn) Quit() { c.closeAfterReply = true }

// CloseASAP forces the connection shut from another goroutine (server shutdown
// or CLIENT KILL). It unblocks an in-progress socket read so the read loop
// observes the close and tears down.
func (c *Conn) CloseASAP() {
	if c.closed.CompareAndSwap(false, true) {
		_ = c.raw.Close()
	}
}

// serve is the per-connection read loop. It reads bytes into the query buffer,
// drains every complete command currently buffered through the handler, flushes
// the batched replies once, and then blocks for more input. It returns (and the
// caller closes the socket) on EOF, a socket error, a protocol error, or QUIT.
func (c *Conn) serve() {
	defer c.server.removeConn(c)
	defer func() { _ = c.raw.Close() }()

	for {
		if c.drain() {
			return
		}
		c.compact()
		if err := c.flush(); err != nil {
			return
		}
		if err := c.fill(); err != nil {
			return
		}
	}
}

// drain parses and dispatches every complete command currently in the query
// buffer. It returns true when the loop should terminate (a protocol error was
// reported, or QUIT asked to close).
func (c *Conn) drain() bool {
	for {
		argv, n, err := resp.ParseRequest(c.qbuf, c.pos, c.server.maxBulkLen)
		if errors.Is(err, resp.ErrNeedMore) {
			return false
		}
		if err != nil {
			// A protocol error is fatal: report it on the wire (its Error string
			// is already a RESP-ready "ERR ..." line) and close.
			var pe resp.ProtocolError
			if errors.As(err, &pe) {
				c.enc.WriteError(pe.Error())
				_ = c.flush()
			}
			return true
		}
		c.pos = n
		if argv == nil {
			// Blank line (a telnet heartbeat); skip and keep parsing.
			continue
		}
		c.lastInteraction = c.server.now()
		c.server.handler.Handle(c, argv)
		c.totCmds++
		if c.closeAfterReply {
			_ = c.flush()
			return true
		}
	}
}

// compact drops the consumed prefix of the query buffer so a long pipeline does
// not leave already-parsed bytes pinned in memory.
func (c *Conn) compact() {
	if c.pos == 0 {
		return
	}
	remaining := len(c.qbuf) - c.pos
	if remaining == 0 {
		c.qbuf = c.qbuf[:0]
	} else {
		copy(c.qbuf, c.qbuf[c.pos:])
		c.qbuf = c.qbuf[:remaining]
	}
	c.pos = 0
}

// fill reads one chunk of bytes from the socket and appends it to the query
// buffer. It applies the idle timeout as a read deadline when configured. A
// returned error (EOF, timeout, or a closed socket) ends the connection.
func (c *Conn) fill() error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	if c.server.idleTimeout > 0 {
		_ = c.raw.SetReadDeadline(c.server.now().Add(c.server.idleTimeout))
	}
	start := len(c.qbuf)
	if cap(c.qbuf)-start < readChunk {
		grown := make([]byte, start, start+readChunk)
		copy(grown, c.qbuf)
		c.qbuf = grown
	}
	c.qbuf = c.qbuf[:start+readChunk]
	nr, err := c.br.Read(c.qbuf[start:])
	c.qbuf = c.qbuf[:start+nr]
	if nr > 0 {
		c.totNetIn += uint64(nr)
	}
	return err
}

// flush writes the buffered replies to the socket in one call and resets the
// output buffer. A short or failed write ends the connection.
func (c *Conn) flush() error {
	if c.outBuf.Len() == 0 {
		return nil
	}
	c.writeMu.Lock()
	n, err := c.raw.Write(c.outBuf.Bytes())
	if n > 0 {
		c.totNetOut += uint64(n)
	}
	c.writeMu.Unlock()
	c.outBuf.Reset()
	return err
}
