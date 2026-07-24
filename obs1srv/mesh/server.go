package mesh

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
)

// Hooks binds the verbs to the node. A nil hook refuses its verb with a
// clean error the caller's fallback path handles; in particular Repl is
// nil until the doc 04 section 7 hot standby lands, which makes M.REPL
// the seam the milestone asks for: the wire shape exists, the machinery
// answers "not enabled", and the fallback is takeover replay.
type Hooks struct {
	// Int carries one cross-node intent leg; the reply payload goes back
	// to the caller. Fallback on any failure: the intent aborts cleanly
	// on the caller's side, never half-applies here.
	Int func(payload [][]byte) ([][]byte, error)
	// Wake nudges the blocking machinery for a group's key. Fallback:
	// the blocked client's poll tick.
	Wake func(group uint16, key []byte) error
	// Repl accepts one hot-standby frame batch. Fallback: takeover
	// replay from the chain.
	Repl func(frames [][]byte) error
	// Hint is the read-the-chain-now nudge for a log domain, never
	// authority. Fallback: the follower's own poll cadence.
	Hint func(dd uint8) error
}

// ServerConfig wires a listener.
type ServerConfig struct {
	Secret string
	Hooks  Hooks
}

// Server accepts mesh connections, authenticates each, and serves verbs.
type Server struct {
	cfg ServerConfig
	ln  net.Listener

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

// Serve starts the accept loop on ln and returns immediately.
func Serve(ln net.Listener, cfg ServerConfig) (*Server, error) {
	if ln == nil {
		return nil, fmt.Errorf("mesh: server needs a listener")
	}
	if cfg.Secret == "" {
		return nil, fmt.Errorf("mesh: server needs a shared secret")
	}
	s := &Server{cfg: cfg, ln: ln, conns: make(map[net.Conn]struct{})}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr is the listener's address, for wiring peers in tests and settings.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Close stops accepting, closes every live connection, and waits for the
// per-connection goroutines to drain.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	err := s.ln.Close()
	s.wg.Wait()
	return err
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = c.Close()
			return
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveConn(c)
	}
}

// serveConn runs one authenticated connection: frames in, dispatch each
// request on its own goroutine, replies serialized by the write mutex so
// they may interleave in completion order.
func (s *Server) serveConn(c net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		_ = c.Close()
	}()

	var wmu sync.Mutex
	write := func(parts ...[]byte) error {
		wmu.Lock()
		defer wmu.Unlock()
		_, err := c.Write(appendFrame(nil, parts...))
		return err
	}

	authed := false
	var fc frameConn
	var pending sync.WaitGroup
	defer pending.Wait()
	tmp := make([]byte, readChunk)
	for {
		n, rerr := c.Read(tmp)
		if n > 0 {
			ferr := fc.feed(tmp[:n], func(args [][]byte) error {
				if len(args) < 2 {
					return errors.New("mesh: short frame")
				}
				id, verb := args[0], string(args[1])
				if !authed {
					if verb != VerbAuth || len(args) != 4 ||
						subtle.ConstantTimeCompare(args[2], []byte(s.cfg.Secret)) != 1 {
						_ = write(id, []byte("err"), []byte("auth required"))
						return errors.New("mesh: auth failed")
					}
					authed = true
					return write(id, []byte("ok"))
				}
				req := args[2:]
				pending.Add(1)
				go func() {
					defer pending.Done()
					out, herr := s.dispatch(verb, req)
					if herr != nil {
						_ = write(id, []byte("err"), []byte(herr.Error()))
						return
					}
					_ = write(append([][]byte{id, []byte("ok")}, out...)...)
				}()
				return nil
			})
			if ferr != nil {
				return
			}
		}
		if rerr != nil {
			return
		}
	}
}

func (s *Server) dispatch(verb string, args [][]byte) ([][]byte, error) {
	h := s.cfg.Hooks
	switch verb {
	case VerbInt:
		if h.Int == nil {
			return nil, errors.New("int not wired")
		}
		return h.Int(args)
	case VerbWake:
		if h.Wake == nil {
			return nil, errors.New("wake not wired")
		}
		if len(args) != 2 {
			return nil, errors.New("wake wants group and key")
		}
		g, err := strconv.ParseUint(string(args[0]), 10, 16)
		if err != nil {
			return nil, errors.New("wake wants a group number")
		}
		return nil, h.Wake(uint16(g), args[1])
	case VerbRepl:
		if h.Repl == nil {
			return nil, errors.New("repl not enabled")
		}
		return nil, h.Repl(args)
	case VerbHint:
		if h.Hint == nil {
			return nil, errors.New("hint not wired")
		}
		if len(args) != 1 {
			return nil, errors.New("hint wants a log domain")
		}
		dd, err := strconv.ParseUint(string(args[0]), 10, 8)
		if err != nil {
			return nil, errors.New("hint wants a domain number")
		}
		return nil, h.Hint(uint8(dd))
	default:
		return nil, fmt.Errorf("unknown mesh verb %q", verb)
	}
}
