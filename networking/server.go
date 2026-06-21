package networking

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/resp"
)

// Config holds the listener and connection settings the server reads at start.
// The zero Config is usable: it listens on no address, so a caller must set at
// least Addr or UnixSocket. Defaults that match Redis are applied in New for the
// fields left zero.
type Config struct {
	// Addr is the TCP listen address ("host:port"); empty disables the TCP
	// listener. Use ":0" to let the OS pick a free port (handy in tests).
	Addr string
	// UnixSocket is the filesystem path for a Unix domain socket; empty disables
	// it. Unix connections bypass the protected-mode notion by construction.
	UnixSocket string
	// UnixSocketPerm is chmod'd onto the socket file right after bind.
	UnixSocketPerm os.FileMode
	// MaxClients caps simultaneous connections; 0 means unlimited. At the cap a
	// new connection is accepted, sent "-ERR max number of clients reached", and
	// closed, matching Redis's acceptCommonHandler.
	MaxClients int
	// MaxBulkLen caps a single bulk argument (proto-max-bulk-len). 0 selects
	// resp.DefaultMaxBulkLen.
	MaxBulkLen int64
	// IdleTimeout closes a connection after this much inactivity; 0 disables it.
	IdleTimeout time.Duration
	// TCPKeepAlive is the SetKeepAlivePeriod applied to accepted TCP sockets; 0
	// leaves the OS default and does not enable keepalive.
	TCPKeepAlive time.Duration
}

// Server accepts connections on its listeners and runs one goroutine per
// connection. It owns the client registry and the graceful-shutdown path; it
// delegates every command to its Handler.
type Server struct {
	handler     Handler
	maxClients  int
	maxBulkLen  int64
	idleTimeout time.Duration
	keepAlive   time.Duration

	nextID atomic.Uint64

	mu          sync.Mutex
	conns       map[uint64]*Conn
	listeners   []net.Listener
	unixSocket  string
	clientCount int
	closed      bool

	wg sync.WaitGroup

	// nowFn is the clock, overridable in tests.
	nowFn func() time.Time
}

// New builds a Server from cfg and the command handler. It does not open any
// socket; call ListenAndServe.
func New(cfg Config, handler Handler) *Server {
	maxBulk := cfg.MaxBulkLen
	if maxBulk <= 0 {
		maxBulk = resp.DefaultMaxBulkLen
	}
	return &Server{
		handler:     handler,
		maxClients:  cfg.MaxClients,
		maxBulkLen:  maxBulk,
		idleTimeout: cfg.IdleTimeout,
		keepAlive:   cfg.TCPKeepAlive,
		conns:       make(map[uint64]*Conn),
		nowFn:       time.Now,
	}
}

func (s *Server) now() time.Time { return s.nowFn() }

// ListenAndServe opens the configured listeners and serves until Close. It
// returns nil on a clean Close and the bind error if a listener cannot open.
func (s *Server) ListenAndServe(cfg Config) error {
	var lns []net.Listener
	if cfg.Addr != "" {
		ln, err := net.Listen("tcp", cfg.Addr)
		if err != nil {
			return err
		}
		lns = append(lns, ln)
	}
	if cfg.UnixSocket != "" {
		ln, err := net.Listen("unix", cfg.UnixSocket)
		if err != nil {
			closeAll(lns)
			return err
		}
		perm := cfg.UnixSocketPerm
		if perm == 0 {
			perm = 0o700
		}
		if err := os.Chmod(cfg.UnixSocket, perm); err != nil {
			closeAll(append(lns, ln))
			return err
		}
		s.unixSocket = cfg.UnixSocket
		lns = append(lns, ln)
	}
	if len(lns) == 0 {
		return errors.New("networking: no listen address configured")
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		closeAll(lns)
		return net.ErrClosed
	}
	s.listeners = lns
	s.mu.Unlock()

	var serveWG sync.WaitGroup
	for _, ln := range lns {
		serveWG.Add(1)
		go func(ln net.Listener) {
			defer serveWG.Done()
			s.acceptLoop(ln)
		}(ln)
	}
	serveWG.Wait()
	return nil
}

// Addr returns the address of the first TCP listener, useful when the config
// used ":0". It returns nil before ListenAndServe has bound a socket.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ln := range s.listeners {
		if _, ok := ln.(*net.TCPListener); ok {
			return ln.Addr()
		}
	}
	if len(s.listeners) > 0 {
		return s.listeners[0].Addr()
	}
	return nil
}

// acceptLoop accepts connections on one listener until the listener is closed.
func (s *Server) acceptLoop(ln net.Listener) {
	for {
		nc, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return
			}
			// A transient accept error (e.g. EMFILE) should not kill the loop.
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return
		}
		s.onAccept(nc)
	}
}

// onAccept applies socket options, enforces maxclients, and starts the read
// loop for a freshly accepted connection.
func (s *Server) onAccept(nc net.Conn) {
	if tcp, ok := nc.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		if s.keepAlive > 0 {
			_ = tcp.SetKeepAlive(true)
			_ = tcp.SetKeepAlivePeriod(s.keepAlive)
		}
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = nc.Close()
		return
	}
	if s.maxClients > 0 && s.clientCount >= s.maxClients {
		s.mu.Unlock()
		_, _ = nc.Write(resp.ReplyMaxClients)
		_ = nc.Close()
		return
	}
	id := s.nextID.Add(1)
	now := s.now()
	c := &Conn{
		server:          s,
		raw:             nc,
		br:              bufio.NewReaderSize(nc, readChunk),
		id:              id,
		addr:            addrString(nc.RemoteAddr()),
		laddr:           addrString(nc.LocalAddr()),
		outBuf:          new(bytes.Buffer),
		created:         now,
		lastInteraction: now,
	}
	c.enc = resp.NewEncoder(c.outBuf, 2)
	s.conns[id] = c
	s.clientCount++
	s.wg.Add(1)
	s.mu.Unlock()

	go func() {
		defer s.wg.Done()
		c.serve()
	}()
}

// DisconnectHandler is an optional interface a Handler may implement to learn
// when a connection's read loop has exited, so it can drop any per-connection
// state it holds (pub/sub subscriptions, for one). It is called once per
// connection, from that connection's own goroutine.
type DisconnectHandler interface {
	OnDisconnect(c *Conn)
}

// removeConn unregisters a connection when its read loop exits.
func (s *Server) removeConn(c *Conn) {
	s.mu.Lock()
	if _, ok := s.conns[c.id]; ok {
		delete(s.conns, c.id)
		s.clientCount--
	}
	s.mu.Unlock()
	if dh, ok := s.handler.(DisconnectHandler); ok {
		dh.OnDisconnect(c)
	}
}

// CountClients returns the number of currently connected clients.
func (s *Server) CountClients() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clientCount
}

// Snapshot returns the live connections at the moment of the call. The slice is
// a copy, so the caller can iterate without holding the registry lock, which is
// what CLIENT LIST needs.
func (s *Server) Snapshot() []*Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Conn, 0, len(s.conns))
	for _, c := range s.conns {
		out = append(out, c)
	}
	return out
}

// ConnByID returns the connection with the given id, or nil if none is live.
func (s *Server) ConnByID(id uint64) *Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns[id]
}

// Close stops accepting, force-closes every live connection so their read loops
// return, waits for them to finish, and removes the Unix socket file. It is safe
// to call once; a second call is a no-op.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	lns := s.listeners
	s.listeners = nil
	conns := make([]*Conn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	unixSocket := s.unixSocket
	s.mu.Unlock()

	closeAll(lns)
	for _, c := range conns {
		c.CloseASAP()
	}
	s.wg.Wait()
	if unixSocket != "" {
		_ = os.Remove(unixSocket)
	}
	return nil
}

func closeAll(lns []net.Listener) {
	for _, ln := range lns {
		_ = ln.Close()
	}
}

func addrString(a net.Addr) string {
	if a == nil {
		return ""
	}
	return a.String()
}
