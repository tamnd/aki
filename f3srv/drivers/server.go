package drivers

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/tamnd/aki/engine/f3/shard"
)

// defaultArenaBytes is the per-shard arena when the caller leaves it zero,
// generous for the smoke surface; the product sizing rides the maxmemory work.
const defaultArenaBytes = 256 << 20

// Options configures a server.
type Options struct {
	// Addr is the TCP listen address; ":0" picks a free port.
	Addr string
	// Shards is the owner worker count; non-positive takes shard.DefaultShards.
	Shards int
	// ArenaBytes is the arena size per shard; non-positive takes the default.
	ArenaBytes int
	// SegBytes is the arena segment size per shard; non-positive takes the
	// store default.
	SegBytes int
}

// Server is the M0 smoke server: the goroutine-per-connection driver over the
// shard runtime, answering PING and ECHO through the full hop path so the
// runtime is exercised end to end from a raw socket. The parser below is the
// smoke parser; the RESP2 slice replaces it and this file keeps only the
// accept loop and the connection pairing.
type Server struct {
	rt     *shard.Runtime
	ln     net.Listener
	closed atomic.Bool
	conns  sync.WaitGroup
}

// Listen builds the runtime, starts its workers, and binds the listener.
// Serve must be called to accept.
func Listen(o Options) (*Server, error) {
	if o.Shards <= 0 {
		o.Shards = shard.DefaultShards()
	}
	if o.ArenaBytes <= 0 {
		o.ArenaBytes = defaultArenaBytes
	}
	ln, err := net.Listen("tcp", o.Addr)
	if err != nil {
		return nil, err
	}
	s := &Server{rt: shard.New(o.Shards, o.ArenaBytes, o.SegBytes), ln: ln}
	s.rt.Start()
	return s, nil
}

// Addr reports the bound listen address.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Serve accepts connections until Close. It returns nil after a Close and the
// accept error otherwise.
func (s *Server) Serve() error {
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			return err
		}
		s.conns.Add(1)
		go func() {
			defer s.conns.Done()
			s.handle(nc)
		}()
	}
}

// Close stops accepting, waits for the connection handlers, and stops the
// shard workers.
func (s *Server) Close() error {
	s.closed.Store(true)
	err := s.ln.Close()
	s.conns.Wait()
	s.rt.Stop()
	return err
}

// handle runs one connection: a reader goroutine (this one) parses and routes,
// a writer goroutine drains the reply queue in request order onto the socket.
func (s *Server) handle(nc net.Conn) {
	defer nc.Close()
	c := s.rt.NewConn()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		bw := bufio.NewWriter(nc)
		emit := func(rep []byte) { bw.Write(rep) }
		for c.Wait() {
			c.DrainReplies(emit)
			if bw.Flush() != nil {
				return
			}
		}
		c.DrainReplies(emit)
		bw.Flush()
	}()

	br := bufio.NewReader(nc)
	for {
		args, err := readCommand(br)
		if err != nil {
			break
		}
		if err := s.dispatch(c, args); err != nil {
			break
		}
		// A drained read buffer is the pipeline boundary: publish everything
		// batched so far, one atomic push per touched shard.
		if br.Buffered() == 0 {
			c.Flush()
		}
	}
	c.Close()
	<-writerDone
}

// dispatch routes one parsed command. Unknown verbs and arity errors travel
// through the hop as OpError so their replies keep pipeline order.
func (s *Server) dispatch(c *shard.Conn, args [][]byte) error {
	verb := string(bytes.ToUpper(args[0]))
	switch verb {
	case "PING":
		switch len(args) {
		case 1:
			return c.Do(shard.OpPing, nil, nil)
		case 2:
			return c.Do(shard.OpPing, nil, args[1])
		}
		return c.Do(shard.OpError, nil, []byte("wrong number of arguments for 'ping' command"))
	case "ECHO":
		if len(args) == 2 {
			return c.Do(shard.OpEcho, nil, args[1])
		}
		return c.Do(shard.OpError, nil, []byte("wrong number of arguments for 'echo' command"))
	default:
		return c.Do(shard.OpError, nil, fmt.Appendf(nil, "unknown command '%s'", args[0]))
	}
}

// The smoke parser. It reads just enough of RESP to carry PING and ECHO: the
// array-of-bulk-strings form every client sends, plus the inline form for a
// bare netcat. It allocates per command and enforces only sanity bounds; the
// RESP2 slice replaces it wholesale with the fuzzed zero-copy parser, and
// nothing outside this file calls it.

var errProtocol = errors.New("drivers: protocol error")

const (
	smokeMaxArgs = 64
	smokeMaxBulk = 1 << 20
)

func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-1]
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line, nil
}

func readCommand(br *bufio.Reader) ([][]byte, error) {
	for {
		line, err := readLine(br)
		if err != nil {
			return nil, err
		}
		if len(line) == 0 {
			continue // empty inline line, ignored like the real server
		}
		if line[0] != '*' {
			return bytes.Fields(line), nil
		}
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil || n < 1 || n > smokeMaxArgs {
			return nil, errProtocol
		}
		args := make([][]byte, n)
		for i := 0; i < n; i++ {
			hdr, err := readLine(br)
			if err != nil {
				return nil, err
			}
			if len(hdr) < 2 || hdr[0] != '$' {
				return nil, errProtocol
			}
			l, err := strconv.Atoi(string(hdr[1:]))
			if err != nil || l < 0 || l > smokeMaxBulk {
				return nil, errProtocol
			}
			buf := make([]byte, l+2)
			if _, err := readFull(br, buf); err != nil {
				return nil, err
			}
			if buf[l] != '\r' || buf[l+1] != '\n' {
				return nil, errProtocol
			}
			args[i] = buf[:l]
		}
		return args, nil
	}
}

func readFull(br *bufio.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := br.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
