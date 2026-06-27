// Package server puts a minimal RESP front end on the v2 engine so the standard
// redis-benchmark saturation harness can drive it directly. It speaks just enough
// of the protocol to run the GET/SET workload the v2 pivot targets: multibulk
// commands for GET, SET, PING, plus the handful of introspection commands
// redis-benchmark issues at startup (CONFIG, COMMAND, INFO, DBSIZE, FLUSHALL,
// SELECT). It is deliberately separate from the v1 networking stack so the v2
// engine can be measured on its own.
//
// The hot path is shaped like a fast RESP server: one goroutine per connection,
// a buffered reader and writer, and a flush deferred until the read buffer drains
// so a pipelined burst becomes one writev. Command argument slices are reused
// across commands on a connection, so steady-state GET/SET parsing does not
// allocate.
package server

import (
	"bufio"
	"errors"
	"io"
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
			tc.SetNoDelay(true)
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			c.Close()
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
		s.ln.Close()
	}
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Server) handle(c net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		c.Close()
	}()

	r := bufio.NewReaderSize(c, 64*1024)
	w := bufio.NewWriterSize(c, 64*1024)
	conn := &connState{r: r, w: w}
	for {
		if err := s.handleOne(conn); err != nil {
			return
		}
		// Flush only when the read buffer is drained, so a pipelined burst of N
		// commands collapses into a single write back to the client.
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

// connState carries the per-connection reader, writer, and a reusable argument
// scratch so steady-state command parsing does not allocate.
type connState struct {
	r       *bufio.Reader
	w       *bufio.Writer
	args    [][]byte
	scratch []byte    // reusable backing store for one command's argument bytes
	offs    []argSpan // per-argument spans into scratch, rebuilt each command
}

var errProtocol = errors.New("protocol error")

func (s *Server) handleOne(conn *connState) error {
	args, err := readCommand(conn)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return nil
	}
	return s.dispatch(conn.w, args)
}

// readCommand parses one RESP multibulk command (or an inline command) into
// conn.args, reusing the backing slices where possible.
func readCommand(conn *connState) ([][]byte, error) {
	r := conn.r
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, nil
	}
	if line[0] != '*' {
		// Inline command: split on spaces. redis-benchmark does not use this, but
		// redis-cli and a bare PING do.
		return splitInline(line, conn), nil
	}
	n, ok := atoiBytes(line[1:])
	if !ok || n < 0 {
		return nil, errProtocol
	}
	// Copy each argument into the connection's reusable scratch buffer rather than
	// allocating one slice per argument. The arg slices are rebuilt to point into
	// scratch only after every argument is read, so a mid-command buffer grow does
	// not leave an earlier arg pointing at stale storage. Steady state allocates
	// nothing: scratch settles at the largest command seen and is reused.
	conn.scratch = conn.scratch[:0]
	if cap(conn.offs) < n {
		conn.offs = make([]argSpan, n)
	}
	conn.offs = conn.offs[:n]
	for i := 0; i < n; i++ {
		hdr, err := readLine(r)
		if err != nil {
			return nil, err
		}
		if len(hdr) == 0 || hdr[0] != '$' {
			return nil, errProtocol
		}
		blen, ok := atoiBytes(hdr[1:])
		if !ok || blen < 0 {
			return nil, errProtocol
		}
		off := len(conn.scratch)
		need := off + blen
		if cap(conn.scratch) < need {
			ns := make([]byte, need, need*2)
			copy(ns, conn.scratch)
			conn.scratch = ns
		}
		conn.scratch = conn.scratch[:need]
		if _, err := io.ReadFull(r, conn.scratch[off:need]); err != nil {
			return nil, err
		}
		// Discard trailing CRLF.
		if _, err := r.Discard(2); err != nil {
			return nil, err
		}
		conn.offs[i] = argSpan{off: off, len: blen}
	}
	if cap(conn.args) < n {
		conn.args = make([][]byte, n)
	}
	conn.args = conn.args[:n]
	for i := 0; i < n; i++ {
		conn.args[i] = conn.scratch[conn.offs[i].off : conn.offs[i].off+conn.offs[i].len]
	}
	return conn.args, nil
}

// argSpan records where one argument lives inside connState.scratch, so the arg
// slices can be rebuilt after the scratch buffer has finished growing.
type argSpan struct{ off, len int }

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

// readLine reads through the next CRLF and returns the line without it.
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	n := len(line)
	if n >= 2 && line[n-2] == '\r' {
		return line[:n-2], nil
	}
	return line[:n-1], nil
}

func splitInline(line []byte, conn *connState) [][]byte {
	conn.args = conn.args[:0]
	start := -1
	for i := 0; i < len(line); i++ {
		if line[i] == ' ' {
			if start >= 0 {
				conn.args = append(conn.args, line[start:i])
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		conn.args = append(conn.args, line[start:])
	}
	return conn.args
}

var (
	respOK       = []byte("+OK\r\n")
	respPong     = []byte("+PONG\r\n")
	respNil      = []byte("$-1\r\n")
	respEmptyArr = []byte("*0\r\n")
	respZero     = []byte(":0\r\n")
)

func (s *Server) dispatch(w *bufio.Writer, args [][]byte) error {
	cmd := args[0]
	switch len(cmd) {
	case 3:
		if eqFold(cmd, "get") {
			return s.cmdGet(w, args)
		}
		if eqFold(cmd, "set") {
			return s.cmdSet(w, args)
		}
	case 4:
		if eqFold(cmd, "ping") {
			_, err := w.Write(respPong)
			return err
		}
		if eqFold(cmd, "info") {
			return writeBulk(w, []byte("# Server\r\nredis_version:7.4.0\r\n"))
		}
	case 6:
		if eqFold(cmd, "config") {
			_, err := w.Write(respEmptyArr)
			return err
		}
		if eqFold(cmd, "dbsize") {
			return writeInt(w, int64(s.store.Len()))
		}
		if eqFold(cmd, "select") {
			_, err := w.Write(respOK)
			return err
		}
	case 7:
		if eqFold(cmd, "command") {
			_, err := w.Write(respEmptyArr)
			return err
		}
	case 8:
		if eqFold(cmd, "flushall") {
			_, err := w.Write(respOK)
			return err
		}
	}
	// Unknown command: a benign OK keeps redis-benchmark moving for anything we
	// did not special-case.
	_, err := w.Write(respOK)
	return err
}

func (s *Server) cmdGet(w *bufio.Writer, args [][]byte) error {
	if len(args) != 2 {
		return writeErr(w, "wrong number of arguments for 'get'")
	}
	val, found, err := s.store.Get(args[1])
	if err != nil {
		return writeErr(w, err.Error())
	}
	if !found {
		_, werr := w.Write(respNil)
		return werr
	}
	return writeBulk(w, val)
}

func (s *Server) cmdSet(w *bufio.Writer, args [][]byte) error {
	if len(args) < 3 {
		return writeErr(w, "wrong number of arguments for 'set'")
	}
	if err := s.store.Set(args[1], args[2]); err != nil {
		return writeErr(w, err.Error())
	}
	_, err := w.Write(respOK)
	return err
}

func writeBulk(w *bufio.Writer, b []byte) error {
	w.WriteByte('$')
	w.Write(strconv.AppendInt(w.AvailableBuffer(), int64(len(b)), 10))
	w.WriteString("\r\n")
	w.Write(b)
	_, err := w.WriteString("\r\n")
	return err
}

func writeInt(w *bufio.Writer, n int64) error {
	w.WriteByte(':')
	w.Write(strconv.AppendInt(w.AvailableBuffer(), n, 10))
	_, err := w.WriteString("\r\n")
	return err
}

func writeErr(w *bufio.Writer, msg string) error {
	w.WriteString("-ERR ")
	w.WriteString(msg)
	_, err := w.WriteString("\r\n")
	return err
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

var _ = respZero
