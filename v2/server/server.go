// Package server puts a minimal RESP front end on the v2 engine so the standard
// redis-benchmark saturation harness can drive it directly. It speaks just enough
// of the protocol to run the GET/SET workload the v2 pivot targets: multibulk
// commands for GET, SET, PING, plus the handful of introspection commands
// redis-benchmark issues at startup (CONFIG, COMMAND, INFO, DBSIZE, FLUSHALL,
// SELECT). It is deliberately separate from the v1 networking stack so the v2
// engine can be measured on its own.
//
// The hot path is a raw-buffer loop, not a bufio reader. Each turn reads one
// chunk straight off the socket, parses every complete command sitting in the
// buffer in place, runs each one appending its reply to a single output buffer,
// and writes that buffer back in one syscall. Command arguments are sub-slices of
// the read buffer, so steady-state parsing copies nothing: a GET key is read in
// place, and a SET value is copied once by the engine when it lands in the log.
// This removes the per-argument scratch copy and the dozen bufio method calls a
// command used to cost, which is where the GET saturation gap against Valkey
// lived once the engine itself stopped being the bottleneck.
package server

import (
	"bytes"
	"errors"
	"net"
	"strconv"
	"sync"

	"github.com/tamnd/aki/v2/store"
)

// Server serves the v2 store over RESP on a TCP listener.
type Server struct {
	store *store.Store
	ln    net.Listener

	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

// New builds a Server over an existing store. Call Serve to accept connections.
func New(s *store.Store) *Server {
	return &Server{store: s, conns: make(map[net.Conn]struct{})}
}

// ListenAndServe binds addr and serves until Close. It returns the bind error if
// the listen fails; otherwise it blocks in the accept loop.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln
	return s.serve()
}

// Addr returns the bound address, useful when ListenAndServe was given ":0".
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

func (s *Server) serve() error {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = c.Close()
			return nil
		}
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.handle(c)
	}
}

// Close stops accepting and drops all open connections.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.ln != nil {
		_ = s.ln.Close()
	}
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

const (
	readChunk = 64 * 1024 // initial read buffer, grows for an oversize single command
	writeCap  = 64 * 1024 // initial reply buffer, grows as a pipeline burst fills it
)

func (s *Server) handle(c net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		_ = c.Close()
	}()

	conn := &connState{
		buf: make([]byte, readChunk),
		out: make([]byte, 0, writeCap),
	}
	// start..end is the unparsed window inside buf.
	start, end := 0, 0
	for {
		// Drain every complete command currently in the buffer, appending replies
		// to conn.out. Arguments point straight into buf, valid until the next read.
		for {
			args, n, ok, perr := parseCommand(conn.buf[start:end], conn)
			if perr != nil {
				return
			}
			if !ok {
				break
			}
			start += n
			if len(args) > 0 {
				conn.out = s.dispatch(conn.out, args)
			}
		}
		// One write back per drained burst.
		if len(conn.out) > 0 {
			if _, err := c.Write(conn.out); err != nil {
				return
			}
			conn.out = conn.out[:0]
		}
		// Slide the leftover partial command to the front.
		if start > 0 {
			copy(conn.buf, conn.buf[start:end])
			end -= start
			start = 0
		}
		// A single command larger than the buffer: grow and keep reading it.
		if end == len(conn.buf) {
			nb := make([]byte, len(conn.buf)*2)
			copy(nb, conn.buf[:end])
			conn.buf = nb
		}
		n, err := c.Read(conn.buf[end:])
		if n > 0 {
			end += n
		}
		if err != nil {
			return
		}
	}
}

// connState carries the per-connection read buffer, reply buffer, and a reusable
// argument slice so steady-state command parsing does not allocate.
type connState struct {
	buf  []byte
	out  []byte
	args [][]byte
}

var errProtocol = errors.New("protocol error")

// parseCommand tries to parse one RESP multibulk command (or an inline command)
// from the front of buf. On success it returns the argument slices (pointing into
// buf), the number of bytes consumed, and ok=true. When buf holds only a partial
// command it returns ok=false with no error, signalling the caller to read more.
func parseCommand(buf []byte, conn *connState) (args [][]byte, consumed int, ok bool, err error) {
	if len(buf) == 0 {
		return nil, 0, false, nil
	}
	if buf[0] != '*' {
		// Inline command (redis-cli, a bare PING). redis-benchmark never uses it.
		nl := bytes.IndexByte(buf, '\n')
		if nl < 0 {
			return nil, 0, false, nil
		}
		return splitInline(trimCR(buf[:nl]), conn), nl + 1, true, nil
	}
	nl := bytes.IndexByte(buf, '\n')
	if nl < 0 {
		return nil, 0, false, nil
	}
	n, okp := atoiBytes(trimCR(buf[1:nl]))
	if !okp || n < 0 {
		return nil, 0, false, errProtocol
	}
	pos := nl + 1
	a := conn.args[:0]
	for i := 0; i < n; i++ {
		if pos >= len(buf) {
			return nil, 0, false, nil
		}
		if buf[pos] != '$' {
			return nil, 0, false, errProtocol
		}
		rel := bytes.IndexByte(buf[pos:], '\n')
		if rel < 0 {
			return nil, 0, false, nil
		}
		hdrEnd := pos + rel
		blen, okb := atoiBytes(trimCR(buf[pos+1 : hdrEnd]))
		if !okb || blen < 0 {
			return nil, 0, false, errProtocol
		}
		dataStart := hdrEnd + 1
		dataEnd := dataStart + blen
		if dataEnd+2 > len(buf) { // value bytes plus trailing CRLF
			return nil, 0, false, nil
		}
		a = append(a, buf[dataStart:dataEnd])
		pos = dataEnd + 2
	}
	conn.args = a
	return a, pos, true, nil
}

// trimCR drops a single trailing carriage return, leaving the line content.
func trimCR(b []byte) []byte {
	if n := len(b); n > 0 && b[n-1] == '\r' {
		return b[:n-1]
	}
	return b
}

func splitInline(line []byte, conn *connState) [][]byte {
	a := conn.args[:0]
	start := -1
	for i := 0; i < len(line); i++ {
		if line[i] == ' ' {
			if start >= 0 {
				a = append(a, line[start:i])
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		a = append(a, line[start:])
	}
	conn.args = a
	return a
}

// atoiBytes parses a non-negative decimal integer from b with no allocation.
// ok is false on an empty slice or a non-digit byte.
func atoiBytes(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

var (
	respOK       = []byte("+OK\r\n")
	respPong     = []byte("+PONG\r\n")
	respNil      = []byte("$-1\r\n")
	respEmptyArr = []byte("*0\r\n")
	respInfo     = []byte("# Server\r\nredis_version:7.4.0\r\n")
)

// dispatch runs one command and appends its reply to out, returning the grown
// buffer. It switches on command length first so the GET/SET hot paths reach
// their compare quickly.
func (s *Server) dispatch(out []byte, args [][]byte) []byte {
	cmd := args[0]
	switch len(cmd) {
	case 3:
		if eqFold(cmd, "get") {
			return s.cmdGet(out, args)
		}
		if eqFold(cmd, "set") {
			return s.cmdSet(out, args)
		}
	case 4:
		if eqFold(cmd, "ping") {
			return append(out, respPong...)
		}
		if eqFold(cmd, "info") {
			return appendBulk(out, respInfo)
		}
	case 6:
		if eqFold(cmd, "config") {
			return append(out, respEmptyArr...)
		}
		if eqFold(cmd, "dbsize") {
			return appendInt(out, int64(s.store.Len()))
		}
		if eqFold(cmd, "select") {
			return append(out, respOK...)
		}
	case 7:
		if eqFold(cmd, "command") {
			return append(out, respEmptyArr...)
		}
	case 8:
		if eqFold(cmd, "flushall") {
			return append(out, respOK...)
		}
	}
	// Unknown command: a benign OK keeps redis-benchmark moving for anything we
	// did not special-case.
	return append(out, respOK...)
}

func (s *Server) cmdGet(out []byte, args [][]byte) []byte {
	if len(args) != 2 {
		return appendErr(out, "wrong number of arguments for 'get'")
	}
	val, found, err := s.store.Get(args[1])
	if err != nil {
		return appendErr(out, err.Error())
	}
	if !found {
		return append(out, respNil...)
	}
	return appendBulk(out, val)
}

func (s *Server) cmdSet(out []byte, args [][]byte) []byte {
	if len(args) < 3 {
		return appendErr(out, "wrong number of arguments for 'set'")
	}
	if err := s.store.Set(args[1], args[2]); err != nil {
		return appendErr(out, err.Error())
	}
	return append(out, respOK...)
}

func appendBulk(out, b []byte) []byte {
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(b)), 10)
	out = append(out, '\r', '\n')
	out = append(out, b...)
	return append(out, '\r', '\n')
}

func appendInt(out []byte, n int64) []byte {
	out = append(out, ':')
	out = strconv.AppendInt(out, n, 10)
	return append(out, '\r', '\n')
}

func appendErr(out []byte, msg string) []byte {
	out = append(out, "-ERR "...)
	out = append(out, msg...)
	return append(out, '\r', '\n')
}

// eqFold reports whether cmd equals the ASCII lowercase want, case-insensitively.
// want must already be lowercase.
func eqFold(cmd []byte, want string) bool {
	if len(cmd) != len(want) {
		return false
	}
	for i := 0; i < len(cmd); i++ {
		c := cmd[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != want[i] {
			return false
		}
	}
	return true
}
