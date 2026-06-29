// Package networking drives the RESP codec over real connections. It owns the
// per-connection lifecycle: the accept loop, the input (query) buffer, the
// command parse-and-dispatch loop, the output buffer, and graceful shutdown.
// It interprets no command itself; a Handler is the seam the command-dispatch
// layer fills (doc 07 §5, doc 19 §3 and §4).
package networking

import (
	"bytes"
	"errors"
	"net"
	"runtime/debug"
	"strconv"
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

// connCloser is the seam through which CloseASAP routes the shutdown of a
// connection that an event loop owns (the reactor net mode). The loop, not an
// arbitrary goroutine, must drive the socket close so it can deregister the fd
// from epoll before the fd number is freed. The goroutine net mode leaves this
// nil and closes the socket inline.
type connCloser interface {
	requestClose(c *Conn)
}

// PanicHandler is an optional capability a Handler can implement to turn a panic
// in a command goroutine into a crash report. The serve loop recovers a panic,
// calls OnPanic with the cause and the goroutine stack, and the handler is
// expected to write the report and stop the process. If the handler is missing
// or returns, the serve loop re-panics so the crash stays fatal.
type PanicHandler interface {
	OnPanic(cause any, stack []byte)
}

// readChunk is the size of a single socket read appended to the query buffer.
const readChunk = 16 * 1024

// crlfBytes is the RESP line terminator, shared by the zero-alloc reply writers.
var crlfBytes = []byte("\r\n")

// Conn is one client connection. Everything on it is touched by a single
// goroutine (the read loop), so the per-connection state needs no locking; only
// the cross-goroutine close path is atomic.
type Conn struct {
	server *Server
	raw    net.Conn

	id    uint64
	addr  string
	laddr string

	// Query (input) buffer. qbuf holds bytes read from the socket; pos is the
	// parse offset within it. Consumed bytes are compacted off the front before
	// each socket read so the buffer does not grow without bound across a long
	// pipeline.
	qbuf    []byte
	pos     int
	argvBuf [8][]byte // pre-allocated backing for the common ≤8-argument command

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

	// armedDeadline is the last read deadline we set on the socket. When
	// idle-timeout is enabled, fill() compares the next desired deadline
	// against this value and skips the setsockopt syscall when the clock
	// has moved by less than a quarter of the idle period, keeping steady
	// traffic cheap without meaningfully relaxing the timeout precision.
	armedDeadline time.Time

	// closedCh is closed once, by CloseASAP, so a goroutine parked on this
	// connection (a client blocked in BLPOP and friends) wakes when the server
	// force-closes the socket on shutdown or CLIENT KILL. closeOnce guards the
	// single close.
	closedCh  chan struct{}
	closeOnce sync.Once

	// Reactor (event-loop) net-mode fields. They are untouched on the default
	// goroutine-per-connection path; only the epoll reactor sets them.
	//
	//   - fd is the socket file descriptor the owning loop registered with epoll.
	//   - onLoop is true while an event loop owns this connection's I/O; CloseASAP
	//     reads it from another goroutine to route the close through the loop.
	//   - loop is that owning loop, used only when onLoop is true.
	//   - needHandoff is set inside drain when the loop must move a connection off
	//     the event loop (it is about to run a blocking command); only the loop
	//     goroutine reads and clears it.
	fd          int
	onLoop      atomic.Bool
	loop        connCloser
	needHandoff bool
}

// NewOfflineConn builds a connection that is not backed by a socket. The command
// layer uses it to replay commands internally, such as loading a dataset from the
// AOF at startup, where the replies are not sent anywhere. Output is encoded into
// an in-memory buffer the caller never reads.
func NewOfflineConn() *Conn {
	c := &Conn{outBuf: new(bytes.Buffer), closedCh: make(chan struct{})}
	c.enc = resp.NewEncoder(c.outBuf, 2)
	return c
}

// IsOffline reports whether the connection has no backing socket. The command
// layer uses it to know a command cannot truly block: a blocking command on an
// offline connection (a script's redis.call, the AOF replay) runs as its
// non-blocking equivalent instead of parking the goroutine forever.
func (c *Conn) IsOffline() bool { return c.raw == nil }

// Closed returns a channel that is closed when the connection is force-closed
// from another goroutine (server shutdown or CLIENT KILL). A blocking command
// selects on it so a parked client wakes instead of leaking its goroutine.
func (c *Conn) Closed() <-chan struct{} { return c.closedCh }

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

// WriteBulk appends a RESP bulk string reply ($) straight to the output buffer
// without the encoder's per-reply length-to-string allocation. The length header
// is formed on the stack with strconv.AppendInt (an int64 length is at most 19
// digits, which with the '$' and CRLF fits the 24-byte scratch, so no heap), and
// the payload is written in place with no scratch copy. A bulk string is framed
// identically in RESP2 and RESP3, so this is correct in both. It is the GET fast
// path's reply writer; the general path keeps using the encoder.
func (c *Conn) WriteBulk(data []byte) {
	var hdr [24]byte
	hdr[0] = '$'
	h := strconv.AppendInt(hdr[:1], int64(len(data)), 10)
	h = append(h, '\r', '\n')
	c.outBuf.Write(h)
	c.outBuf.Write(data)
	c.outBuf.Write(crlfBytes)
}

// OutBytes returns the bytes accumulated in the output buffer. It is used by an
// offline connection (NewOfflineConn) to read back a command's reply, for
// example when a script's redis.call runs a command and needs its RESP reply.
func (c *Conn) OutBytes() []byte { return c.outBuf.Bytes() }

// ResetOut clears the output buffer. An offline connection reused across several
// commands calls this between them so each reply starts clean.
func (c *Conn) ResetOut() { c.outBuf.Reset() }

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
//
// When an event loop owns the connection (reactor net mode), the actual socket
// close must run on that loop so it can deregister the fd from epoll before the
// fd number is freed. CloseASAP then only hands the connection to the loop and
// returns; the loop performs the close. On the goroutine path it closes inline.
func (c *Conn) CloseASAP() {
	if c.onLoop.Load() {
		c.loop.requestClose(c)
		return
	}
	if c.closed.CompareAndSwap(false, true) {
		c.closeOnce.Do(func() { close(c.closedCh) })
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
	defer func() {
		if r := recover(); r != nil {
			if ph, ok := c.server.handler.(PanicHandler); ok {
				ph.OnPanic(r, debug.Stack())
			}
			// No handler took over, or it returned without exiting. A panic
			// means inconsistent state, so keep the crash fatal.
			panic(r)
		}
	}()

	// Resolve the optional batch-complete hook once: it fires after each drained
	// pipeline so the handler can flush work it coalesced across the batch.
	batchHandler, _ := c.server.handler.(BatchHandler)

	for {
		term := c.drain()
		// Flush batched work after every drained pipeline, including the final one
		// before a QUIT or error closes the connection, so no buffered write is
		// left behind.
		if batchHandler != nil {
			batchHandler.OnBatchComplete(c)
		}
		if term {
			return
		}
		c.compact()
		if c.overQueryBufLimit() {
			// The buffered, not yet parseable input passed
			// client-query-buffer-limit. Redis closes the connection without a
			// reply in this case, so flush whatever is pending and return.
			_ = c.flush()
			return
		}
		if err := c.flush(); err != nil {
			return
		}
		if err := c.fill(); err != nil {
			return
		}
	}
}

// overQueryBufLimit reports whether the unparsed query buffer has grown past
// client-query-buffer-limit. A zero limit disables the check.
func (c *Conn) overQueryBufLimit() bool {
	limit := c.server.QueryBufLimit()
	return limit > 0 && int64(len(c.qbuf)) > limit
}

// drain parses and dispatches every complete command currently in the query
// buffer. It returns true when the loop should terminate (a protocol error was
// reported, or QUIT asked to close).
//
// The clock is read once per burst, not per command. A pipelined burst can carry
// dozens of commands, so a per-command time.Now would add that many vDSO reads to
// the hot loop; reading it once when the first command of the burst is handled and
// reusing it leaves lastInteraction accurate to the burst, which is far finer than
// the second-granularity idle field CLIENT LIST reports off it.
func (c *Conn) drain() bool {
	var now time.Time
	var haveNow bool
	for {
		argv, n, err := resp.ParseRequest(c.qbuf, c.pos, c.server.MaxBulkLen(), c.argvBuf[:])
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
		if argv == nil {
			// Blank line (a telnet heartbeat); skip and keep parsing.
			c.pos = n
			continue
		}
		// Reactor net mode: a loop goroutine must never park on one connection, so
		// a command that might block (BLPOP and the rest) is not run here. Leave the
		// parse offset at the start of this command, flag the handoff, and return so
		// the loop moves the connection to a dedicated goroutine that re-parses and
		// runs it. The goroutine path leaves onLoop false, so this never fires there.
		if c.onLoop.Load() && c.server.blockProber != nil && c.server.blockProber.MayBlock(argv) {
			c.needHandoff = true
			return true
		}
		if !haveNow {
			now = c.server.now()
			haveNow = true
		}
		c.pos = n
		c.lastInteraction = now
		c.server.handler.Handle(c, argv)
		c.totCmds++
		if c.closeAfterReply {
			_ = c.flush()
			return true
		}
	}
}

// compact drops the consumed prefix of the query buffer so a long pipeline does
// not leave already-parsed bytes pinned in memory. It also shrinks a large
// buffer that most of its capacity idle: a connection that processed one huge
// pipeline and now sits quiet would otherwise hold megabytes in reserve forever.
func (c *Conn) compact() {
	if c.pos == 0 {
		return
	}
	remaining := len(c.qbuf) - c.pos
	if remaining == 0 {
		// Shrink an oversized buffer back to readChunk so a bursty connection
		// does not permanently occupy several times the default capacity.
		if cap(c.qbuf) > 64*1024 {
			c.qbuf = make([]byte, 0, readChunk)
		} else {
			c.qbuf = c.qbuf[:0]
		}
	} else {
		if cap(c.qbuf) > 64*1024 && remaining < cap(c.qbuf)/4 {
			// Most of the buffer is consumed and the live tail is small; copy
			// it into a fresh readChunk-sized buffer rather than keeping the
			// big allocation around.
			fresh := make([]byte, remaining, readChunk)
			copy(fresh, c.qbuf[c.pos:])
			c.qbuf = fresh
		} else {
			copy(c.qbuf, c.qbuf[c.pos:])
			c.qbuf = c.qbuf[:remaining]
		}
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
	if to := c.server.IdleTimeout(); to > 0 {
		dl := c.server.now().Add(to)
		// Only re-arm the deadline if it has moved by more than a quarter of
		// the idle period, so steady traffic does not pay a setsockopt per read.
		if dl.Sub(c.armedDeadline) > to/4 {
			_ = c.raw.SetReadDeadline(dl)
			c.armedDeadline = dl
		}
	}
	start := len(c.qbuf)
	if cap(c.qbuf)-start < readChunk {
		grown := make([]byte, start, start+readChunk)
		copy(grown, c.qbuf)
		c.qbuf = grown
	}
	c.qbuf = c.qbuf[:start+readChunk]
	nr, err := c.raw.Read(c.qbuf[start:])
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
