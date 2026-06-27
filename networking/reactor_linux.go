//go:build linux

package networking

import (
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
)

// reactor_linux.go is the epoll event-loop networking strategy (Spec/2064/reactor,
// task #101). It is selected by NetMode "reactor" and applies to TCP connections
// on Linux. Instead of one read-loop goroutine per connection, a small set of
// event loops each own a shard of connections: a loop calls epoll_wait once, gets
// a batch of ready connections, and services each inline. The park that the
// goroutine model pays once per connection per pipeline batch is paid once per
// epoll_wait here, amortized over every connection that woke in that call. That
// amortization is the point: at shallow pipeline depth the per-batch wake is most
// of the per-request cost, and collapsing many wakes into one is what lets aki's
// engine show against a multi-threaded-I/O rival (design doc, "the problem").
//
// The loop must never park on one connection, so a command that might block
// (BLPOP and the rest) is detected in Conn.drain before it runs and the
// connection is handed off to a dedicated goroutine that runs the existing
// blocking machinery; the fd leaves the loop for the connection's remaining life.
// Everything else (the hot GET/SET/INCR path) runs on the loop.

const (
	// maxReactorEvents bounds one epoll_wait's returned events. A larger batch
	// amortizes the wake over more connections; 1024 is plenty for the loop count
	// the reactor runs and keeps the per-loop event buffer small.
	maxReactorEvents = 1024
	// epollTimeoutMs bounds how long a loop blocks in epoll_wait with no activity.
	// Wakeups for registration, close, and shutdown come through the self-pipe, so
	// this is only a safety net that lets an idle loop re-check its stop flag.
	epollTimeoutMs = 1000
)

// reactor holds the running event loops. The Server keeps it behind the
// netReactor interface so the package compiles on non-Linux platforms.
type reactor struct {
	loops  []*evLoop
	loopWG sync.WaitGroup
}

// evLoop is one epoll event loop. It owns a disjoint shard of connections: it is
// the only goroutine that reads, writes, or reaps them, so their per-connection
// state needs no lock, the same single-threaded invariant Conn.serve relies on.
// Cross-goroutine work (a new registration from the accept path, a close from
// CLIENT KILL or shutdown) is queued under mu and folded in at the top of each
// loop turn, then the hot path runs lock-free.
type evLoop struct {
	reactor      *reactor
	server       *Server
	handler      Handler
	batchHandler BatchHandler

	epfd     int
	wakeR    int
	wakeW    int
	stopping atomic.Bool

	// conns maps a registered socket fd to its connection. Only the loop goroutine
	// touches it.
	conns map[int32]*Conn

	mu          sync.Mutex
	pendingReg  []*Conn
	pendingKill []*Conn
}

// newReactor builds and starts the event loops. It runs one loop per GOMAXPROCS,
// so the existing scheduler-parallelism knob also sizes the reactor. It returns
// (nil, false) only if the kernel refuses an epoll or pipe fd, in which case the
// server falls back to the goroutine path.
func newReactor(s *Server) (netReactor, bool) {
	n := max(runtime.GOMAXPROCS(0), 1)
	r := &reactor{}
	bh, _ := s.handler.(BatchHandler)
	for range n {
		l, err := newEvLoop(s, r, bh)
		if err != nil {
			for _, made := range r.loops {
				made.closeFds()
			}
			return nil, false
		}
		r.loops = append(r.loops, l)
	}
	for _, l := range r.loops {
		r.loopWG.Add(1)
		go l.run()
	}
	return r, true
}

// newEvLoop creates a loop's epoll instance and self-pipe, and arms the pipe's
// read end so a write to the pipe wakes a blocked epoll_wait. It does not start
// the loop goroutine.
func newEvLoop(s *Server, r *reactor, bh BatchHandler) (*evLoop, error) {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	var p [2]int
	if err := syscall.Pipe2(p[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		_ = syscall.Close(epfd)
		return nil, err
	}
	l := &evLoop{
		reactor:      r,
		server:       s,
		handler:      s.handler,
		batchHandler: bh,
		epfd:         epfd,
		wakeR:        p[0],
		wakeW:        p[1],
		conns:        make(map[int32]*Conn),
	}
	var ev syscall.EpollEvent
	ev.Events = syscall.EPOLLIN
	ev.Fd = int32(l.wakeR)
	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, l.wakeR, &ev); err != nil {
		l.closeFds()
		return nil, err
	}
	return l, nil
}

func (l *evLoop) closeFds() {
	_ = syscall.Close(l.epfd)
	_ = syscall.Close(l.wakeR)
	_ = syscall.Close(l.wakeW)
}

// register adopts an accepted TCP connection. It resolves the socket fd, assigns
// the connection to a loop by fd, and queues it for that loop to arm. It returns
// false if the fd cannot be resolved, and the caller serves the connection on the
// goroutine path instead.
func (r *reactor) register(c *Conn) bool {
	sc, ok := c.raw.(syscall.Conn)
	if !ok {
		return false
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return false
	}
	fd := -1
	if cerr := rc.Control(func(p uintptr) { fd = int(p) }); cerr != nil || fd < 0 {
		return false
	}
	l := r.loops[fd%len(r.loops)]
	c.fd = fd
	c.loop = l
	// onLoop publishes the loop assignment to a racing CloseASAP; set it after fd
	// and loop are in place. The enqueue under mu publishes fd to the loop.
	c.onLoop.Store(true)
	l.mu.Lock()
	l.pendingReg = append(l.pendingReg, c)
	l.mu.Unlock()
	l.wake()
	return true
}

// requestClose queues a connection for the loop to close. CloseASAP routes here
// for a loop-owned connection so the loop, which owns the fd, deregisters it from
// epoll before the fd number is freed.
func (l *evLoop) requestClose(c *Conn) {
	l.mu.Lock()
	l.pendingKill = append(l.pendingKill, c)
	l.mu.Unlock()
	l.wake()
}

// shutdown stops every loop and waits for them to exit. Each loop reaps the
// connections it owns as it stops.
func (r *reactor) shutdown() {
	for _, l := range r.loops {
		l.stopping.Store(true)
		l.wake()
	}
	r.loopWG.Wait()
}

// run is the event loop. It folds in queued registrations and closes, blocks in
// epoll_wait, and services each ready connection inline.
func (l *evLoop) run() {
	defer l.reactor.loopWG.Done()
	events := make([]syscall.EpollEvent, maxReactorEvents)
	for {
		l.processPending()
		if l.stopping.Load() {
			l.shutdownConns()
			return
		}
		n, err := syscall.EpollWait(l.epfd, events, epollTimeoutMs)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			l.shutdownConns()
			return
		}
		for i := range n {
			fd := events[i].Fd
			if int(fd) == l.wakeR {
				l.drainWake()
				continue
			}
			c := l.conns[fd]
			if c == nil {
				continue
			}
			l.safeService(c)
		}
	}
}

// processPending arms newly registered connections and reaps requested closes. It
// runs once per loop turn so the hot service path below stays lock-free.
func (l *evLoop) processPending() {
	l.mu.Lock()
	reg := l.pendingReg
	kill := l.pendingKill
	l.pendingReg = nil
	l.pendingKill = nil
	l.mu.Unlock()
	for _, c := range reg {
		var ev syscall.EpollEvent
		ev.Events = syscall.EPOLLIN
		ev.Fd = int32(c.fd)
		if err := syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_ADD, c.fd, &ev); err != nil {
			l.reap(c)
			continue
		}
		l.conns[int32(c.fd)] = c
	}
	for _, c := range kill {
		l.reap(c)
	}
}

// safeService runs service under the same panic contract as Conn.serve: a panic
// in a command is reported through the optional PanicHandler and then re-raised
// so the crash stays fatal (a panic that escapes a goroutine aborts the process).
func (l *evLoop) safeService(c *Conn) {
	defer func() {
		if r := recover(); r != nil {
			if ph, ok := l.handler.(PanicHandler); ok {
				ph.OnPanic(r, debug.Stack())
			}
			panic(r)
		}
	}()
	l.service(c)
}

// service handles one readiness on a connection: read the available bytes once,
// drain the complete commands, run the batch-complete hook, and flush. It mirrors
// one turn of Conn.serve's body. Level-triggered epoll re-reports the fd next turn
// if a single read did not consume everything, so one read per turn cannot lose
// buffered input and it bounds the work one connection does before the loop moves
// on (design doc D4).
func (l *evLoop) service(c *Conn) {
	if err := c.fill(); err != nil {
		l.reap(c)
		return
	}
	term := c.drain()
	if l.batchHandler != nil {
		l.batchHandler.OnBatchComplete(c)
	}
	if c.needHandoff {
		c.needHandoff = false
		// Flush the replies produced for the commands before the blocking one so
		// they reach the client now, then move the connection to its own goroutine,
		// where parking on the blocking command is safe.
		_ = c.flush()
		l.detach(c)
		c.server.startGoroutineServe(c)
		return
	}
	if term {
		l.reap(c)
		return
	}
	c.compact()
	if c.overQueryBufLimit() {
		_ = c.flush()
		l.reap(c)
		return
	}
	if err := c.flush(); err != nil {
		l.reap(c)
		return
	}
}

// detach removes a connection from the loop without closing it, for handoff to a
// goroutine. It deregisters the fd (so level-triggered epoll stops reporting it),
// drops it from the loop's map, and clears the loop ownership so CloseASAP closes
// it inline from now on.
func (l *evLoop) detach(c *Conn) {
	fd := c.fd
	if fd >= 0 {
		_ = syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_DEL, fd, nil)
		delete(l.conns, int32(fd))
	}
	c.fd = -1
	// Clear onLoop before loop so a racing CloseASAP that reads onLoop==false takes
	// the inline-close path and never dereferences a nil loop.
	c.onLoop.Store(false)
	c.loop = nil
}

// reap tears a loop-owned connection down: deregister the fd, close the socket
// (waking any goroutine parked on Closed), drop the registry entry, and release
// the connection's wg ticket. The fd guard makes it idempotent and makes it skip
// a connection that was already detached for handoff, so a queued close that
// raced a handoff does not double-account.
func (l *evLoop) reap(c *Conn) {
	fd := c.fd
	if fd < 0 {
		return
	}
	c.fd = -1
	_ = syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_DEL, fd, nil)
	delete(l.conns, int32(fd))
	if c.closed.CompareAndSwap(false, true) {
		c.closeOnce.Do(func() { close(c.closedCh) })
		_ = c.raw.Close()
	}
	c.onLoop.Store(false)
	c.server.removeConn(c)
	c.server.wg.Done()
}

// shutdownConns reaps every connection the loop owns, including any still queued
// for registration, then closes the loop's own fds. It runs as the loop exits.
func (l *evLoop) shutdownConns() {
	l.mu.Lock()
	reg := l.pendingReg
	l.pendingReg = nil
	l.pendingKill = nil
	l.mu.Unlock()
	for _, c := range reg {
		l.reap(c)
	}
	for _, c := range l.conns {
		l.reap(c)
	}
	l.closeFds()
}

// wake nudges the loop out of epoll_wait by writing one byte to the self-pipe. A
// pending byte already in the pipe is enough, so a write that returns EAGAIN
// (pipe full of prior wakes) is fine to drop.
func (l *evLoop) wake() {
	for {
		_, err := syscall.Write(l.wakeW, wakeByte)
		if err == syscall.EINTR {
			continue
		}
		return
	}
}

// drainWake empties the self-pipe after a wake so it does not keep reporting
// readable.
func (l *evLoop) drainWake() {
	var buf [256]byte
	for {
		n, err := syscall.Read(l.wakeR, buf[:])
		if err == syscall.EINTR {
			continue
		}
		if n < len(buf) || err != nil {
			return
		}
	}
}

var wakeByte = []byte{1}
