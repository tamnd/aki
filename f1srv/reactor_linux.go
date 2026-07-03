//go:build linux

package f1srv

import (
	"net"
	"runtime"
	"sync"
	"syscall"
)

// The epoll event-loop network driver. It is an alternative to the goroutine-per-
// connection loop in conn.go, built to remove the one cost that model cannot shed at
// saturation: a connection goroutine is parked on a blocking Read and woken by the Go
// netpoller once per read batch, so a large fraction of cycles go to the scheduler
// moving goroutines on and off a P rather than to serving commands. Redis and Valkey
// pay none of that because one thread runs one epoll loop over every connection. This
// driver does the same: M loops (M = GOMAXPROCS), each owning one epoll instance and a
// disjoint, fd-sharded set of connections, so a connection is only ever touched by its
// own loop and the per-loop connection table needs no lock.
//
// A connection is taken over whole: on accept the loop dups the socket fd out of the
// net.Conn, closes the net.Conn to release the runtime's netpoller registration, and
// from then on reads and writes the dup with raw non-blocking syscalls. The parse,
// dispatch, and reply code is exactly the goroutine path's: the loop fills connState.rbuf
// with a raw read, calls the shared drain, and writes connState.out with a raw write. The
// only difference between the two drivers is who reads the socket and who flushes out.

const reactorMaxEvents = 256

// reactorConn is one connection owned entirely by a single loop. cs is the shared
// parse-dispatch-reply state; fd is the dup'd non-blocking socket; writeBlocked records
// that a short write left bytes in cs.out and the loop has switched epoll interest to
// EPOLLOUT and stopped reading until the peer drains.
type reactorConn struct {
	cs           *connState
	fd           int
	writeBlocked bool

	// loop is the owning loop, set at adoption, so a blocking command's park handoff can reach the
	// loop's epoll set and wake queue. parkCancel is created when a blocking command parks and closed
	// by the loop on a peer disconnect, so the park goroutine unwinds without replying; parkCancelled
	// records that close so finishPark closes the connection rather than replying. All three are only
	// touched on the loop goroutine except parkCancel's close, which the park goroutine observes
	// through the channel.
	loop          *reactorLoop
	parkCancel    chan struct{}
	parkCancelled bool
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
	cross   []crossMsg     // pub/sub frames posted by publishers on other loops, drained on wake
	done    []*reactorConn // connections whose park goroutine finished, awaiting flush + resume
	closing bool           // set by stop(); the loop tears down on the next wake
}

// crossMsg is one pub/sub message frame handed to a loop for a connection it owns. A publisher
// running on any loop posts it to the subscriber's loop through the mutex-guarded cross queue and
// a wake poke; the owning loop drains the queue on its own goroutine and writes the frame, so a
// connection's output is only ever touched by its own loop.
type crossMsg struct {
	rc    *reactorConn
	frame []byte
}

// serveWithReactor runs the epoll driver when NetMode is "reactor", returning handled
// true so ListenAndServe does not also run the goroutine accept loop. It starts one loop
// goroutine per GOMAXPROCS, then accepts on the caller's goroutine and hands each new
// connection to a loop round-robin. On any setup failure it reports handled false so the
// caller falls back to the goroutine driver on the same already-bound listener.
func serveWithReactor(s *Server) (bool, error) {
	if s.cfg.NetMode != "reactor" {
		return false, nil
	}
	n := runtime.GOMAXPROCS(0)
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
	_ = tc.Close() // release the runtime's fd; the socket lives on via dupfd
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
		id:   s.nextConnID.Add(1),
		rbuf: make([]byte, 0, s.cfg.ReadBufSize),
		out:  make([]byte, 0, s.cfg.ReadBufSize),
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
			if rc.cs.parked {
				// A blocking command holds this connection on a park goroutine. Reads are disarmed, so
				// the only event that matters is the peer going away: cancel the park so its goroutine
				// unwinds and finishPark closes the connection.
				if ev.Events&(syscall.EPOLLHUP|syscall.EPOLLERR|syscall.EPOLLRDHUP) != 0 {
					l.cancelPark(rc)
				}
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
	cross := l.cross
	l.cross = nil
	done := l.done
	l.done = nil
	l.mu.Unlock()
	for _, rc := range pending {
		l.adoptConn(rc)
	}
	// Resume every connection whose blocking command finished on its park goroutine: flush the reply
	// it wrote and re-arm reads, or tear the connection down if the peer left while it waited.
	for _, rc := range done {
		l.finishPark(rc)
	}
	// Deliver every pub/sub frame posted by a publisher on another loop. A connection that has
	// since closed carries fd < 0 and is skipped. The frame is appended to the subscriber's out
	// buffer and flushed through the same machinery the read path uses, so backpressure and the
	// EPOLLOUT remainder are handled uniformly.
	for _, m := range cross {
		if m.rc.fd < 0 {
			continue
		}
		m.rc.cs.out = append(m.rc.cs.out, m.frame...)
		l.flush(m.rc)
	}
	return closing
}

// crossDeliver posts a pub/sub message frame to this loop for one of its connections and wakes it.
// It runs on a publisher's goroutine (any loop), so it only touches the mutex-guarded cross queue,
// never the owning loop's conn table or the connection's out buffer.
func (l *reactorLoop) crossDeliver(rc *reactorConn, frame []byte) {
	l.mu.Lock()
	l.cross = append(l.cross, crossMsg{rc: rc, frame: frame})
	l.mu.Unlock()
	l.poke()
}

// begin drives a blocking command that found its keys empty. It runs on the loop goroutine, inside
// the drain that dispatched the command, and satisfies the reactorParker interface connState.park
// holds. It flushes the replies from any earlier pipelined commands, disarms reads on this
// connection so the shared loop is never held by one waiter, and hands the command to a dedicated
// park goroutine that reruns it with blocking enabled. The goroutine owns cs.out from here until
// parkDone posts the connection back to the loop.
func (rc *reactorConn) begin(rerun func()) {
	l := rc.loop
	if len(rc.cs.out) > 0 {
		if !l.flush(rc) {
			return // flush closed the connection; there is nothing left to park
		}
	}
	if rc.fd < 0 {
		return
	}
	rc.cs.parked = true
	rc.parkCancel = make(chan struct{})
	rc.cs.parkCancel = rc.parkCancel
	l.modInterest(rc, syscall.EPOLLRDHUP) // keep only the disconnect signal while it waits
	go rc.runPark(rerun)
}

// runPark reruns the blocking command on its own goroutine with blockable set, so the rerun parks on
// its wait channel and timeout exactly like the goroutine driver and writes its reply into cs.out.
// When the rerun returns, whether served, timed out, or cancelled, parkDone hands the connection
// back to the loop for the flush and read re-arm.
func (rc *reactorConn) runPark(rerun func()) {
	rc.cs.blockable = true
	rerun()
	rc.cs.blockable = false
	rc.loop.parkDone(rc)
}

// parkDone queues a finished park for its loop and wakes the loop. It runs on the park goroutine, so
// it only touches the mutex-guarded done queue and the wake pipe, never the loop's conn table.
func (l *reactorLoop) parkDone(rc *reactorConn) {
	l.mu.Lock()
	l.done = append(l.done, rc)
	l.mu.Unlock()
	l.poke()
}

// finishPark resumes a connection whose park goroutine has returned. It runs on the loop goroutine
// from drainWake. When the peer left while the command waited, cancelPark has already unwound the
// goroutine and the connection is torn down. Otherwise the reply the goroutine wrote is flushed, any
// pipelined commands that arrived after the blocking one are drained, and read interest is restored.
func (l *reactorLoop) finishPark(rc *reactorConn) {
	cs := rc.cs
	cs.parked = false
	rc.parkCancel = nil
	cs.parkCancel = nil
	if rc.parkCancelled {
		rc.parkCancelled = false
		l.closeConn(rc)
		return
	}
	// Drain commands the peer pipelined behind the blocking one. drain compacted them to the front of
	// rbuf when it parked; a following blocking command re-parks through begin, which flushes the
	// reply already sitting in cs.out and re-arms the wait, so this returns with the connection parked.
	if len(cs.rbuf) > 0 {
		if !cs.drain() {
			l.flush(rc)
			l.closeConn(rc)
			return
		}
		if cs.parked {
			return
		}
	}
	if !l.flush(rc) {
		return // flush closed the connection
	}
	if rc.fd >= 0 && !rc.writeBlocked {
		l.modInterest(rc, syscall.EPOLLIN|syscall.EPOLLRDHUP)
	}
}

// cancelPark tells a parked connection's goroutine to unwind without replying, because the peer has
// disconnected. Closing parkCancel wakes the rerun's select; the goroutine returns and parkDone runs
// finishPark, which sees parkCancelled and closes the connection. It runs on the loop goroutine and
// is idempotent, so a burst of HUP and RDHUP events cancels once.
func (l *reactorLoop) cancelPark(rc *reactorConn) {
	if !rc.parkCancelled {
		rc.parkCancelled = true
		close(rc.parkCancel)
	}
}

// adoptConn installs a connection in the loop's table and registers its fd for reads. It also
// installs the connection's pub/sub deliver hook: a message frame for this connection is posted
// to this loop and written on this loop's goroutine, so the connection's output stays
// single-threaded even when the publisher runs on another loop.
func (l *reactorLoop) adoptConn(rc *reactorConn) {
	rc.loop = l
	// Wire the blocking-command park facility: a blocking command that finds its keys empty calls
	// cs.park.begin, which runs on this loop and hands the command to a park goroutine.
	rc.cs.park = rc
	rc.cs.deliver = func(frame []byte) { l.crossDeliver(rc, frame) }
	for len(l.conns) <= rc.fd {
		l.conns = append(l.conns, nil)
	}
	l.conns[rc.fd] = rc
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN | syscall.EPOLLRDHUP, Fd: int32(rc.fd)}
	if err := syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_ADD, rc.fd, &ev); err != nil {
		l.conns[rc.fd] = nil
		_ = syscall.Close(rc.fd)
		rc.fd = -1
		return
	}
	// The connection is now live in the loop. Count it toward connected_clients; closeConn and
	// shutdown, the two ways it can leave, drop the count back. A registration that failed above
	// returned early, so it is never counted and never double-decremented.
	l.srv.clients.Add(1)
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
		if cs.parked {
			// A blocking command in this batch parked. begin() already flushed the replies from any
			// earlier commands and handed cs.out to the park goroutine, so the loop must not flush here.
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
	// Release any watches, open transaction, or pub/sub subscriptions this connection held, so
	// the watch table's refcounts and the global watching gate and the pub/sub registry do not
	// leak past the closed connection, the same teardown the goroutine driver runs after loop
	// returns.
	rc.cs.discardTx()
	rc.cs.unsubscribeAll()
	_ = syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_DEL, rc.fd, nil)
	_ = syscall.Close(rc.fd)
	if rc.fd < len(l.conns) {
		l.conns[rc.fd] = nil
	}
	rc.fd = -1
	l.srv.clients.Add(-1) // matches the increment in adoptConn
}

// shutdown closes every owned connection and the loop's own descriptors. It runs on the
// loop goroutine after stop() has been observed, so no other goroutine is touching the
// conn table.
func (l *reactorLoop) shutdown() {
	for _, rc := range l.conns {
		if rc != nil && rc.fd >= 0 {
			_ = syscall.Close(rc.fd)
			rc.fd = -1
			l.srv.clients.Add(-1) // these were counted at adoptConn; closeConn never ran for them
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
