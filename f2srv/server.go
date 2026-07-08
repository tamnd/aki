// Package f2srv is a thin RESP server over the f2raw engine. It exists to measure
// the base GET/SET/INCR/DEL point path over the wire against Redis and Valkey with
// as little server-side machinery between the socket and the engine as the wire
// protocol allows. It carries only the string point commands plus the handful of
// handshake commands a benchmark client sends; everything else answers with an
// error. It is not a compatible server, it is a measurement instrument.
//
// The connection layer is the proven raw-buffer design from f1srv: one read buffer
// per connection whose argv slices point straight into it (no per-argument
// allocation), and one batched reply buffer flushed once per drained pipeline, so a
// pipeline of N commands costs one read and one write.
package f2srv

import (
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/tamnd/aki/engine/f2raw"
)

// readBufSize is the initial per-connection read and reply buffer capacity. Both
// drivers grow it on demand for a value larger than the buffer.
const readBufSize = 16 << 10

// Server owns the listener and the shared f2raw store. One store serves every
// connection; the engine is lock-free across distinct keys, so connections never
// coordinate on the hot path.
type Server struct {
	store  *f2raw.Store
	nextID atomic.Int64

	// NetMode selects the network driver: "auto" (epoll reactor on Linux, goroutine
	// per connection elsewhere; the default), "go" (goroutine per connection), or
	// "reactor" (Linux epoll). ReactorLoops sets the epoll loop count (0 = GOMAXPROCS).
	NetMode      string
	ReactorLoops int

	ln net.Listener
	wg sync.WaitGroup
}

// New builds a server over store with the default auto network driver.
func New(store *f2raw.Store) *Server {
	return &Server{store: store, NetMode: "auto"}
}

// ListenAndServe binds addr and serves connections until the listener closes. On
// Linux with NetMode "auto" or "reactor" it hands the listener to the epoll driver;
// otherwise, and everywhere the reactor is unavailable, each connection runs on its
// own goroutine.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln
	if handled, rerr := serveWithReactor(s); handled {
		return rerr
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		c := &connState{
			srv:  s,
			conn: conn,
			id:   s.nextID.Add(1),
			rbuf: make([]byte, 0, readBufSize),
			out:  make([]byte, 0, readBufSize),
			vbuf: make([]byte, 0, 64),
		}
		go c.loop()
	}
}

// connState is one connection's parse-dispatch-reply state. rbuf holds bytes read
// from the socket not yet consumed into a complete command; argv is reused across
// commands and points into rbuf; out is the batched reply buffer; vbuf is the
// reused destination for a GET value copy.
type connState struct {
	srv       *Server
	conn      net.Conn
	id        int64
	rbuf      []byte
	out       []byte
	argv      [][]byte
	vbuf      []byte
	wantClose bool
}

// loop reads, drains every complete command, and flushes the batched replies until
// the peer closes or a protocol error ends the connection.
func (c *connState) loop() {
	defer c.conn.Close()
	for {
		if !c.fill() {
			return
		}
		if !c.drain() {
			return
		}
		if len(c.out) > 0 {
			if _, err := c.conn.Write(c.out); err != nil {
				return
			}
			c.out = c.out[:0]
		}
		if c.wantClose {
			return
		}
	}
}

// fill reads one chunk from the socket into rbuf, growing the buffer when it is
// full so a value larger than the initial buffer still parses.
func (c *connState) fill() bool {
	if len(c.rbuf) == cap(c.rbuf) {
		grown := make([]byte, len(c.rbuf), cap(c.rbuf)*2)
		copy(grown, c.rbuf)
		c.rbuf = grown
	}
	n, err := c.conn.Read(c.rbuf[len(c.rbuf):cap(c.rbuf)])
	if n > 0 {
		c.rbuf = c.rbuf[:len(c.rbuf)+n]
	}
	return err == nil
}

// drain parses and dispatches every complete command in rbuf, then compacts any
// partial trailing bytes to the front. It returns false on a protocol error.
func (c *connState) drain() bool {
	pos := 0
	for {
		argv, consumed, status := c.parse(c.rbuf[pos:])
		switch status {
		case parseOK:
			c.argv = argv
			pos += consumed
			if len(argv) > 0 {
				c.dispatch(argv)
			}
			if c.wantClose {
				if pos > 0 {
					c.rbuf = append(c.rbuf[:0], c.rbuf[pos:]...)
				}
				return true
			}
		case parseNeedMore:
			if pos > 0 {
				c.rbuf = append(c.rbuf[:0], c.rbuf[pos:]...)
			}
			return true
		case parseErr:
			c.writeErr("ERR Protocol error")
			return false
		}
	}
}

type parseStatus int

const (
	parseOK parseStatus = iota
	parseNeedMore
	parseErr
)

// parse reads one command from the front of b. A RESP multibulk (*N then N bulk
// strings) is the client path; a bare line is the inline path for a hand client.
// argv slices point into b. consumed is the byte count of the parsed command.
func (c *connState) parse(b []byte) (argv [][]byte, consumed int, status parseStatus) {
	if len(b) == 0 {
		return nil, 0, parseNeedMore
	}
	if b[0] != '*' {
		return c.parseInline(b)
	}
	count, i, ok := readIntLine(b, 1)
	if !ok {
		return nil, 0, parseNeedMore
	}
	if count <= 0 {
		return c.argv[:0], i, parseOK
	}
	argv = c.argv[:0]
	for k := 0; k < count; k++ {
		if i >= len(b) {
			return nil, 0, parseNeedMore
		}
		if b[i] != '$' {
			return nil, 0, parseErr
		}
		blen, ni, ok := readIntLine(b, i+1)
		if !ok {
			return nil, 0, parseNeedMore
		}
		i = ni
		if blen < 0 {
			argv = append(argv, nil)
			continue
		}
		if i+blen+2 > len(b) {
			return nil, 0, parseNeedMore
		}
		argv = append(argv, b[i:i+blen])
		i += blen + 2
	}
	return argv, i, parseOK
}

// parseInline handles a single space-separated line, enough for redis-cli's inline
// PING and manual probing. It is not the benchmark path.
func (c *connState) parseInline(b []byte) (argv [][]byte, consumed int, status parseStatus) {
	nl := indexByte(b, '\n')
	if nl < 0 {
		return nil, 0, parseNeedMore
	}
	line := b[:nl]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	argv = c.argv[:0]
	i := 0
	for i < len(line) {
		for i < len(line) && line[i] == ' ' {
			i++
		}
		if i >= len(line) {
			break
		}
		start := i
		for i < len(line) && line[i] != ' ' {
			i++
		}
		argv = append(argv, line[start:i])
	}
	return argv, nl + 1, parseOK
}

// readIntLine parses an optionally-negative base-10 integer terminated by CRLF,
// starting at b[i]. ok is false when the terminator is not yet in the buffer.
func readIntLine(b []byte, i int) (val int, next int, ok bool) {
	neg := false
	if i < len(b) && b[i] == '-' {
		neg = true
		i++
	}
	v := 0
	digits := 0
	for i < len(b) && b[i] >= '0' && b[i] <= '9' {
		v = v*10 + int(b[i]-'0')
		i++
		digits++
	}
	if i+1 >= len(b) {
		return 0, 0, false
	}
	if b[i] != '\r' || b[i+1] != '\n' {
		return 0, 0, false
	}
	if digits == 0 && !neg {
		return 0, 0, false
	}
	if neg {
		v = -v
	}
	return v, i + 2, true
}

func indexByte(b []byte, ch byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] == ch {
			return i
		}
	}
	return -1
}

// --- reply writers, all appending to the batched reply buffer c.out ---

func (c *connState) writeSimple(s string) {
	c.out = append(c.out, '+')
	c.out = append(c.out, s...)
	c.out = append(c.out, '\r', '\n')
}

func (c *connState) writeErr(s string) {
	c.out = append(c.out, '-')
	c.out = append(c.out, s...)
	c.out = append(c.out, '\r', '\n')
}

func (c *connState) writeInt(n int64) {
	c.out = append(c.out, ':')
	c.out = strconv.AppendInt(c.out, n, 10)
	c.out = append(c.out, '\r', '\n')
}

func (c *connState) writeBulk(b []byte) {
	c.out = append(c.out, '$')
	c.out = strconv.AppendInt(c.out, int64(len(b)), 10)
	c.out = append(c.out, '\r', '\n')
	c.out = append(c.out, b...)
	c.out = append(c.out, '\r', '\n')
}

func (c *connState) writeNil() {
	c.out = append(c.out, "$-1\r\n"...)
}

func (c *connState) writeArrayHeader(n int) {
	c.out = append(c.out, '*')
	c.out = strconv.AppendInt(c.out, int64(n), 10)
	c.out = append(c.out, '\r', '\n')
}
