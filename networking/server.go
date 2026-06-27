package networking

import (
	"bytes"
	"errors"
	"fmt"
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
	// QueryBufLimit caps the per-connection query buffer (client-query-buffer-limit).
	// 0 means no limit. A connection whose buffered, not yet parsed input grows past
	// it is closed.
	QueryBufLimit int64
	// IdleTimeout closes a connection after this much inactivity; 0 disables it.
	IdleTimeout time.Duration
	// TCPKeepAlive is the SetKeepAlivePeriod applied to accepted TCP sockets; 0
	// leaves the OS default and does not enable keepalive.
	TCPKeepAlive time.Duration
	// NetMode selects how TCP connections are serviced: "goroutine" (the default,
	// one read-loop goroutine per connection) or "reactor" (a small set of epoll
	// event loops, each servicing a shard of connections). "reactor" applies to
	// TCP on Linux only; on any other platform, and for Unix-socket connections,
	// the goroutine path is used regardless. An empty value means "goroutine".
	NetMode string
}

// Server accepts connections on its listeners and runs one goroutine per
// connection. It owns the client registry and the graceful-shutdown path; it
// delegates every command to its Handler.
type Server struct {
	handler    Handler
	maxClients int
	// maxBulkLen caps a single bulk argument. It is held atomically so CONFIG SET
	// proto-max-bulk-len can change it while the server runs; the parser reads it
	// per request.
	maxBulkLen atomic.Int64
	// idleTimeout and keepAlive are nanosecond durations held atomically so
	// CONFIG SET timeout and CONFIG SET tcp-keepalive can change them while the
	// server runs. The read path loads them per use.
	idleTimeout atomic.Int64
	keepAlive   atomic.Int64
	// queryBufLimit caps the per-connection query buffer. It is held atomically so
	// CONFIG SET client-query-buffer-limit can change it while the server runs; the
	// read loop loads it per connection. 0 means no limit.
	queryBufLimit atomic.Int64

	nextID atomic.Uint64

	mu          sync.Mutex
	conns       map[uint64]*Conn
	listeners   []net.Listener
	unixSocket  string
	clientCount int
	closed      bool

	wg sync.WaitGroup

	// netMode is the configured TCP service strategy ("goroutine" or "reactor").
	// reactor holds the running epoll reactor when netMode is "reactor" and the
	// platform supports it; it is nil otherwise, and TCP connections then take the
	// goroutine path. blockProber, when the handler implements it, lets the reactor
	// move a possibly-blocking command off an event loop.
	netMode     string
	reactor     netReactor
	blockProber BlockProber

	// nowFn is the clock, overridable in tests.
	nowFn func() time.Time
}

// BlockProber is an optional Handler capability the reactor uses. MayBlock
// reports whether a parsed command might park the connection (BLPOP and the rest
// of the blocking family). A reactor event loop must not park on one connection,
// so a command MayBlock approves is handed to a dedicated goroutine instead of
// running on the loop. The goroutine net mode never calls it.
type BlockProber interface {
	MayBlock(argv [][]byte) bool
}

// netReactor is the epoll event-loop server, built only on platforms that
// support it (Linux). The Server holds it behind this interface so the rest of
// the package compiles everywhere; newReactor returns (nil, false) on
// unsupported platforms and the Server falls back to the goroutine path.
type netReactor interface {
	// register hands an accepted TCP connection to an event loop. It returns false
	// if the connection cannot be adopted (for example its fd cannot be resolved),
	// and the caller then serves it on the goroutine path.
	register(c *Conn) bool
	// shutdown stops every loop, reaping the connections they own, and returns once
	// the loops have exited.
	shutdown()
}

// New builds a Server from cfg and the command handler. It does not open any
// socket; call ListenAndServe.
func New(cfg Config, handler Handler) *Server {
	maxBulk := cfg.MaxBulkLen
	if maxBulk <= 0 {
		maxBulk = resp.DefaultMaxBulkLen
	}
	s := &Server{
		handler:    handler,
		maxClients: cfg.MaxClients,
		conns:      make(map[uint64]*Conn),
		netMode:    cfg.NetMode,
		nowFn:      time.Now,
	}
	if bp, ok := handler.(BlockProber); ok {
		s.blockProber = bp
	}
	s.maxBulkLen.Store(maxBulk)
	s.idleTimeout.Store(int64(cfg.IdleTimeout))
	s.keepAlive.Store(int64(cfg.TCPKeepAlive))
	s.queryBufLimit.Store(cfg.QueryBufLimit)
	return s
}

// IdleTimeout reports the current idle timeout. 0 means no timeout.
func (s *Server) IdleTimeout() time.Duration { return time.Duration(s.idleTimeout.Load()) }

// SetIdleTimeout changes the idle timeout. It takes effect on the next read on
// each connection, so CONFIG SET timeout applies without a restart.
func (s *Server) SetIdleTimeout(d time.Duration) { s.idleTimeout.Store(int64(d)) }

// TCPKeepAlive reports the current keepalive period. 0 leaves the OS default.
func (s *Server) TCPKeepAlive() time.Duration { return time.Duration(s.keepAlive.Load()) }

// SetTCPKeepAlive changes the keepalive period. It applies to connections
// accepted after the change, the same as Redis.
func (s *Server) SetTCPKeepAlive(d time.Duration) { s.keepAlive.Store(int64(d)) }

// MaxBulkLen reports the current single-argument bulk cap.
func (s *Server) MaxBulkLen() int64 { return s.maxBulkLen.Load() }

// SetMaxBulkLen changes the bulk cap. A zero or negative value resets it to the
// default, and the next parsed request uses the new limit, so CONFIG SET
// proto-max-bulk-len applies without a restart.
func (s *Server) SetMaxBulkLen(n int64) {
	if n <= 0 {
		n = resp.DefaultMaxBulkLen
	}
	s.maxBulkLen.Store(n)
}

// QueryBufLimit reports the current per-connection query buffer cap. 0 means no
// limit.
func (s *Server) QueryBufLimit() int64 { return s.queryBufLimit.Load() }

// SetQueryBufLimit changes the query buffer cap. A zero or negative value clears
// it, and the next read on each connection uses the new value, so CONFIG SET
// client-query-buffer-limit applies without a restart.
func (s *Server) SetQueryBufLimit(n int64) {
	if n < 0 {
		n = 0
	}
	s.queryBufLimit.Store(n)
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

	// In reactor net mode, start the epoll event loops before accepting. On a
	// platform that does not support it newReactor returns (nil, false) and TCP
	// connections fall back to the goroutine path, so the server still starts.
	if s.netMode == "reactor" {
		if r, ok := newReactor(s); ok {
			s.mu.Lock()
			s.reactor = r
			s.mu.Unlock()
		}
	}

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
			fmt.Fprintf(os.Stderr, "aki: acceptLoop exiting on accept error: %v\n", err)
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
		if ka := s.TCPKeepAlive(); ka > 0 {
			_ = tcp.SetKeepAlive(true)
			_ = tcp.SetKeepAlivePeriod(ka)
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
		id:              id,
		addr:            addrString(nc.RemoteAddr()),
		laddr:           addrString(nc.LocalAddr()),
		outBuf:          new(bytes.Buffer),
		created:         now,
		lastInteraction: now,
		closedCh:        make(chan struct{}),
		fd:              -1,
	}
	c.enc = resp.NewEncoder(c.outBuf, 2)
	s.conns[id] = c
	s.clientCount++
	// One wg ticket per connection. Whoever finally tears the connection down owns
	// it: the goroutine read loop below, or, in reactor mode, the event loop when
	// it reaps the connection (or the handoff goroutine if a blocking command moved
	// it off the loop).
	s.wg.Add(1)
	reactor := s.reactor
	s.mu.Unlock()

	// Reactor net mode: a TCP connection is adopted by an event loop. Unix-socket
	// connections, and any connection a loop declines, take the goroutine path.
	if reactor != nil {
		if _, ok := nc.(*net.TCPConn); ok {
			if reactor.register(c) {
				return
			}
		}
	}

	go func() {
		defer s.wg.Done()
		c.serve()
	}()
}

// startGoroutineServe moves a connection the reactor handed off (it is about to
// run a blocking command, which a loop must not park on) onto its own read-loop
// goroutine, where parking is safe. The connection keeps the wg ticket it was
// accepted with; this goroutine's defer now owns it. The caller (the event loop)
// has already deregistered the fd from epoll and cleared onLoop.
func (s *Server) startGoroutineServe(c *Conn) {
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

// BatchHandler is an optional interface a Handler may implement to learn when a
// connection has finished draining the batch of pipelined commands currently
// buffered, just before it blocks for more input. It is the seam where a handler
// flushes work it batched across the pipeline, such as an append-only-file write
// buffer, so a pipeline of N writes costs one syscall. It mirrors Redis's
// beforeSleep and is called from the connection's own goroutine.
type BatchHandler interface {
	OnBatchComplete(c *Conn)
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
	reactor := s.reactor
	unixSocket := s.unixSocket
	s.mu.Unlock()

	closeAll(lns)
	// Reactor event loops own their connections' sockets, so the loops, not this
	// goroutine, must close those fds (they deregister from epoll first). Shutting
	// the reactor down reaps every connection a loop still owns and exits the
	// loops, leaving only goroutine-path and handed-off connections to force-close
	// below.
	if reactor != nil {
		reactor.shutdown()
	}
	s.mu.Lock()
	conns := make([]*Conn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
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
