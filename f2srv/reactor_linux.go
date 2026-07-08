//go:build linux

package f2srv

import (
	"net"
	"runtime"
	"sync"
	"syscall"
)

// The epoll event-loop network driver for f2srv, ported down from f1srv's reactor to
// the string-only measurement path: no blocking commands, no pub/sub, no transactions,
// so none of the park or cross-loop delivery machinery is carried here. What remains is
// the one thing the goroutine-per-connection model cannot shed at saturation: a
// connection goroutine parked on a blocking Read is woken by the Go netpoller once per
// read batch, so a large share of cycles go to the scheduler moving goroutines on and
// off a P rather than to serving commands. Redis and Valkey pay none of that because one
// thread runs one epoll loop over every connection. This driver does the same: M loops
// (M = GOMAXPROCS by default), each owning one epoll instance and a disjoint, fd-sharded
// set of connections, so a connection is only ever touched by its own loop and the
// per-loop table needs no lock.
//
// A connection is taken over whole: on accept the loop dups the socket fd out of the
// net.Conn, closes the net.Conn to release the runtime's netpoller registration, and
// from then on reads and writes the dup with raw non-blocking syscalls. The parse,
// dispatch, and reply code is exactly the goroutine path's shared drain and reply
// writers; the only difference is who reads the socket and who flushes out.

const reactorMaxEvents = 256

// reactorConn is one connection owned entirely by a single loop. cs is the shared
// parse-dispatch-reply state; fd is the dup'd non-blocking socket; writeBlocked records
// that a short write left bytes in cs.out and the loop has switched epoll interest to
// EPOLLOUT and stopped reading until the peer drains.
type reactorConn struct {
	cs           *connState
	fd           int
	writeBlocked bool
}

// reactorLoop is one event loop: one epoll instance, one self-pipe used to wake the
// loop for connection hand-off and shutdown, and the fd-indexed table of connections it
// owns. Only the loop goroutine touches conns and the epoll set; the accept goroutine
// reaches the loop solely through the mutex-guarded pending queue plus a pipe poke.
type reactorLoop struct {
	srv   *Server
	epfd  int
	wakeR int // self-pipe read end, registered in epoll
	wakeW int // self-pipe write end, poked by the accept goroutine

	conns []*reactorConn // indexed by fd, grown as fds climb; loop-owned, lock-free

	mu      sync.Mutex
	pending []*reactorConn // accepted connections awaiting adoption by this loop
	closing bool           // set by stop(); the loop tears down on the next wake
}

// serveWithReactor runs the epoll driver only when NetMode is an explicit "reactor", returning
// handled true so ListenAndServe does not also run the goroutine accept loop. It starts
// one loop goroutine per GOMAXPROCS (or ReactorLoops), then accepts on the caller's
// goroutine and hands each new connection to a loop round-robin. On any setup failure it
// reports handled false so the caller falls back to the goroutine driver on the same
// already-bound listener.
func serveWithReactor(s *Server) (bool, error) {
	// The epoll reactor benchmarks slower than the goroutine-per-conn path on every
	// box measured (WSL 32c and a bare-metal 6c VPS), P1 and P16 alike, and degrades
	// further as server cores climb. So it stays strictly opt-in: "auto" and the empty
	// default both run the goroutine driver; only an explicit "reactor" engages epoll.
	if s.NetMode != "reactor" {
		return false, nil
	}
	n := s.ReactorLoops
	if n < 1 {
		n = runtime.GOMAXPROCS(0)
	}
	if n < 1 {
		n = 1
	}
	loops := make([]*reactorLoop, 0, n)
	for i := 0; i < n; i++ {
		lp, err := newReactorLoop(s)
		if err != nil {
			for _, l := range loops {
				l.stop()
			}
			return false, nil
		}
		loops = append(loops, lp)
	}
	for _, lp := range loops {
		s.wg.Add(1)
		go func(l *reactorLoop) {
			defer s.wg.Done()
			l.run()
		}(lp)
	}
	var next uint64
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			for _, l := range loops {
				l.stop()
			}
			return true, nil
		}
		rc, ok := s.adopt(conn)
		if !ok {
			_ = conn.Close()
			continue
		}
		loops[next%uint64(len(loops))].enqueue(rc)
		next++
	}
}

// adopt takes ownership of a freshly accepted connection: it dups the socket fd, closes
// the net.Conn so the Go runtime stops polling the original fd (the socket stays alive
// through the dup), and makes the dup non-blocking. The returned reactorConn carries a
// fresh connState with the same buffer sizing the goroutine path uses.
func (s *Server) adopt(conn net.Conn) (*reactorConn, bool) {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, false
	}
	_ = tc.SetNoDelay(true)
	raw, err := tc.SyscallConn()
	if err != nil {
		return nil, false
	}
	var dupfd int
	var dupErr error
	ctlErr := raw.Control(func(fd uintptr) {
		dupfd, dupErr = syscall.Dup(int(fd))
	})
	if ctlErr != nil || dupErr != nil {
		if dupErr == nil {
			_ = syscall.Close(dupfd)
		}
		return nil, false
	}
	if err := syscall.SetNonblock(dupfd, true); err != nil {
		_ = syscall.Close(dupfd)
		return nil, false
	}
	cs := &connState{
		srv:  s,
		id:   s.nextID.Add(1),
		rbuf: make([]byte, 0, readBufSize),
		out:  make([]byte, 0, readBufSize),
		vbuf: make([]byte, 0, 64),
	}
	return &reactorConn{cs: cs, fd: dupfd}, true
}

// newReactorLoop creates an epoll instance and a non-blocking self-pipe, registering the
// pipe's read end so a poke wakes a loop that is parked in epoll_wait.
func newReactorLoop(s *Server) (*reactorLoop, error) {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	var p [2]int
	if err := syscall.Pipe2(p[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		_ = syscall.Close(epfd)
		return nil, err
	}
	l := &reactorLoop{srv: s, epfd: epfd, wakeR: p[0], wakeW: p[1]}
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(p[0])}
	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, p[0], &ev); err != nil {
		_ = syscall.Close(epfd)
		_ = syscall.Close(p[0])
		_ = syscall.Close(p[1])
		return nil, err
	}
	return l, nil
}

// enqueue hands a connection to the loop and pokes it awake. It runs on the accept
// goroutine, so it only touches the mutex-guarded queue, never the loop's conn table.
func (l *reactorLoop) enqueue(rc *reactorConn) {
	l.mu.Lock()
	l.pending = append(l.pending, rc)
	l.mu.Unlock()
	l.poke()
}

// stop asks the loop to tear down at its next wake. It runs on the accept goroutine.
func (l *reactorLoop) stop() {
	l.mu.Lock()
	l.closing = true
	l.mu.Unlock()
	l.poke()
}

func (l *reactorLoop) poke() {
	var b [1]byte
	_, _ = syscall.Write(l.wakeW, b[:])
}

// run is the loop: park in epoll_wait, then service every ready fd. The wake fd adopts
// pending connections or triggers shutdown; every other fd is a client socket.
func (l *reactorLoop) run() {
	events := make([]syscall.EpollEvent, reactorMaxEvents)
	for {
		n, err := syscall.EpollWait(l.epfd, events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			l.shutdown()
			return
		}
		for i := 0; i < n; i++ {
			ev := &events[i]
			fd := int(ev.Fd)
			if fd == l.wakeR {
				if l.drainWake() {
					l.shutdown()
					return
				}
				continue
			}
			rc := l.get(fd)
			if rc == nil {
				continue
			}
			if ev.Events&(syscall.EPOLLHUP|syscall.EPOLLERR) != 0 {
				l.closeConn(rc)
				continue
			}
			if ev.Events&syscall.EPOLLOUT != 0 {
				if !l.flush(rc) {
					continue // flush closed the connection
				}
			}
			if ev.Events&(syscall.EPOLLIN|syscall.EPOLLRDHUP) != 0 {
				if rc.writeBlocked {
					continue // backpressure: hold reads until the peer drains our output
				}
				l.serviceRead(rc)
			}
		}
	}
}

// drainWake empties the self-pipe, adopts any queued connections, and reports whether a
// shutdown was requested. Registration happens here, on the loop goroutine, so the conn
// table and epoll set stay single-threaded.
func (l *reactorLoop) drainWake() (closing bool) {
	var buf [128]byte
	for {
		n, err := syscall.Read(l.wakeR, buf[:])
		if n <= 0 || err != nil {
			break
		}
	}
	l.mu.Lock()
	closing = l.closing
	pending := l.pending
	l.pending = nil
	l.mu.Unlock()
	for _, rc := range pending {
		l.adoptConn(rc)
	}
	return closing
}

// adoptConn installs a connection in the loop's table and registers its fd for reads.
func (l *reactorLoop) adoptConn(rc *reactorConn) {
	for len(l.conns) <= rc.fd {
		l.conns = append(l.conns, nil)
	}
	l.conns[rc.fd] = rc
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLRDHUP, Fd: int32(rc.fd)}
	if err := syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_ADD, rc.fd, &ev); err != nil {
		l.conns[rc.fd] = nil
		_ = syscall.Close(rc.fd)
		rc.fd = -1
	}
}

func (l *reactorLoop) get(fd int) *reactorConn {
	if fd >= 0 && fd < len(l.conns) {
		return l.conns[fd]
	}
	return nil
}

// serviceRead does one non-blocking read into the connection's buffer, drains every
// complete command it now holds, and flushes the batched reply. Level-triggered epoll
// re-reports the fd next turn if the read left more on the socket, so one read per
// readiness keeps work bounded without starving other connections.
func (l *reactorLoop) serviceRead(rc *reactorConn) {
	cs := rc.cs
	if len(cs.rbuf) == cap(cs.rbuf) {
		grown := make([]byte, len(cs.rbuf), cap(cs.rbuf)*2)
		copy(grown, cs.rbuf)
		cs.rbuf = grown
	}
	n, err := syscall.Read(rc.fd, cs.rbuf[len(cs.rbuf):cap(cs.rbuf)])
	if n > 0 {
		cs.rbuf = cs.rbuf[:len(cs.rbuf)+n]
		if !cs.drain() {
			l.flush(rc) // best-effort protocol-error reply
			l.closeConn(rc)
			return
		}
		l.flush(rc) // owns the post-drain flush and the QUIT close
		return
	}
	if n == 0 {
		l.closeConn(rc) // peer closed
		return
	}
	if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK || err == syscall.EINTR {
		return // nothing ready now; epoll will re-report
	}
	l.closeConn(rc)
}

// flush writes as much of cs.out as the socket accepts. On a short write it keeps the
// unsent remainder at the front of cs.out, switches epoll interest to EPOLLOUT, and sets
// writeBlocked so the loop stops reading until the peer drains. It returns false when it
// has closed the connection (write error, or a completed QUIT).
func (l *reactorLoop) flush(rc *reactorConn) bool {
	cs := rc.cs
	buf := cs.out
	for len(buf) > 0 {
		n, err := syscall.Write(rc.fd, buf)
		if n > 0 {
			buf = buf[n:]
			continue
		}
		switch err {
		case syscall.EAGAIN:
			cs.out = append(cs.out[:0], buf...) // shift remainder to the front
			if !rc.writeBlocked {
				l.modInterest(rc, syscall.EPOLLOUT|syscall.EPOLLRDHUP)
				rc.writeBlocked = true
			}
			return true
		case syscall.EINTR:
			continue
		default:
			l.closeConn(rc)
			return false
		}
	}
	cs.out = cs.out[:0]
	if rc.writeBlocked {
		l.modInterest(rc, syscall.EPOLLIN|syscall.EPOLLRDHUP)
		rc.writeBlocked = false
	}
	if cs.wantClose {
		l.closeConn(rc)
		return false
	}
	return true
}

func (l *reactorLoop) modInterest(rc *reactorConn, events uint32) {
	ev := syscall.EpollEvent{Events: events, Fd: int32(rc.fd)}
	_ = syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_MOD, rc.fd, &ev)
}

// closeConn deregisters and closes a connection. It is idempotent: a second call after
// flush has already closed the fd is a no-op, so the read and write paths can both call
// it without coordinating.
func (l *reactorLoop) closeConn(rc *reactorConn) {
	if rc.fd < 0 {
		return
	}
	_ = syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_DEL, rc.fd, nil)
	_ = syscall.Close(rc.fd)
	if rc.fd < len(l.conns) {
		l.conns[rc.fd] = nil
	}
	rc.fd = -1
}

// shutdown closes every owned connection and the loop's own descriptors. It runs on the
// loop goroutine after stop() has been observed, so no other goroutine is touching the
// conn table.
func (l *reactorLoop) shutdown() {
	for _, rc := range l.conns {
		if rc != nil && rc.fd >= 0 {
			_ = syscall.Close(rc.fd)
			rc.fd = -1
		}
	}
	l.conns = nil
	_ = syscall.Close(l.epfd)
	_ = syscall.Close(l.wakeR)
	_ = syscall.Close(l.wakeW)
	l.mu.Lock()
	for _, rc := range l.pending {
		if rc.fd >= 0 {
			_ = syscall.Close(rc.fd)
			rc.fd = -1
		}
	}
	l.pending = nil
	l.mu.Unlock()
}
