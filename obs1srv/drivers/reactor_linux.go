//go:build linux

package drivers

import (
	"encoding/binary"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/dispatch"
	"github.com/tamnd/aki/obs1srv/resp"
)

// The raw-epoll event-loop driver (doc 08 section 4.2), ported from the
// quarantined f1srv/reactor_linux.go with the M10 pull-forward corrections.
// M loops, each owning one epoll instance, one eventfd for wakes, and a
// disjoint fd-sharded connection table: a connection is only ever touched by
// its own loop, so the per-loop tables need no lock. On accept the fd is
// dup'd out of the Go runtime and the net.Conn closed, so the netpoller never
// sees the socket again; reads and writes are raw non-blocking syscalls.
//
// The loop owns parse and reply serialization. Each connection keeps its
// resumable resp.Parser and shard.Conn; the loop reads, parses, dispatches
// through the unchanged shard inbound MPSC, and drains replies through the
// existing reorder ring, of which it is the single consumer. The new edge is
// outbound: a shard owner that completes a batch must wake the loop that owns
// the connection, and that wake rides the SetWriterBatchNotify seam into this
// loop's eventfd, under the same publish-then-check proof the waker carries
// (the loop parks the connection with ParkWriter before it sleeps, so a push
// that lands in the gap is seen by the park's re-check or claims the notify).
// The wake is batched (slice 4): a worker's drain pass marks each claimed
// connection dirty on its loop (markDirty) and ends with one eventfd write
// per touched loop (WakeLoop), so the owner-to-loop syscall traffic is
// O(touched loops) per pass, not O(dirty connections).
//
// Streamed giant replies are Class OF on the loop (slice 5, doc 08 sections
// 3.3, 3.5, and 9.1): the connection runs in step mode (SetStreamStep), so a
// due stream becomes a cursor whose chunks the loop emits incrementally,
// bounded per pass by streamQuanta and per buffer by the reply-buffer
// headroom. The loop writes what the socket accepts, arms EPOLLOUT on a short
// write, and resumes on writability or on the pump's chunk wake; it never
// materializes the value in the output buffer and never blocks on one
// connection's ring. A slow client past the OutBufLimitBytes hard cap is
// disconnected mid-cycle, that connection only, and a failed stream likewise
// drops only its own connection.

// reactorMaxEvents is one epoll_wait's event budget per turn.
const reactorMaxEvents = 256

// loopBufFree caps each per-loop buffer free list (doc 08 section 6.2: a
// fixed-capacity free list per network thread, never sync.Pool). 256 buffers
// of each kind is 32MiB per loop at the defaults, which covers a loop's share
// of the 512-conn gate shape with room, so a fully active shard leases and
// returns against the list with no allocator traffic; past the cap a returned
// buffer goes to the GC, so a connection spike cannot permanently inflate the
// pool footprint (the L19 pool-cap lesson).
const loopBufFree = 256

// streamQuanta bounds the streamed-reply chunks one service pass emits before
// the loop requeues the connection behind its own eventfd: a fast-reading
// giant GET shares the loop with every other connection on the fd shard
// instead of owning it for the value's duration, which is the Class OF yield
// (doc 08 section 9.1) expressed on the consumer side.
const streamQuanta = 4

// closeFD is syscall.Close for the dup'd connection fds, behind a variable so
// the fd-lifecycle test can count every close. Each adopted (or adoption-
// refused) dup must pass through here exactly once: no leak, no double close.
var closeFD = syscall.Close

// eventfd2 flags, mirrored from the kernel ABI; the stdlib syscall package
// has the syscall number but not the flag names.
const (
	efdCloexec  = 0x80000 // EFD_CLOEXEC
	efdNonblock = 0x800   // EFD_NONBLOCK
)

// reactorBackend is the netBackend the server drives: it owns the loops and
// the accept handoff. Loops start at construction (they idle in epoll_wait),
// serve runs the accept loop on the caller's goroutine, stop tears everything
// down exactly once.
type reactorBackend struct {
	s     *Server
	loops []*reactorLoop
	wg    sync.WaitGroup
	once  sync.Once
}

// defaultNetLoops is the frozen lab 19 answer to doc 08 section 4.2's
// loop-count contradiction: neither M = shards nor M = cores minus shards.
// The knee on the gate box's 8-cpu server mask sits at 3 loops for shard
// counts 3, 4, and 5 alike (GET 64B P16/512: 2.05/6.14/6.65/6.65/4.70/3.47
// Mops at loops 1/2/3/4/6/8, with SET and p99 breaking the 3-vs-4 tie
// toward 3), so the loop count follows the core budget alone: the 2/5
// network share of the doc 03 section 2.2 split, the complement of
// shard.DefaultShards' 3/5.
func defaultNetLoops() int {
	n := runtime.GOMAXPROCS(0) * 2 / 5
	if n < 1 {
		n = 1
	}
	return n
}

// newReactorBackend builds the loops or reports why it cannot; the caller
// logs the fallback. Loop count is NetLoops, defaulting to defaultNetLoops
// (lab 19's frozen verdict).
func newReactorBackend(s *Server, o Options) (netBackend, error) {
	n := o.NetLoops
	if n < 1 {
		n = defaultNetLoops()
	}
	b := &reactorBackend{s: s}
	for i := 0; i < n; i++ {
		l, err := newReactorLoop(b)
		if err != nil {
			b.stopLoops()
			return nil, err
		}
		b.loops = append(b.loops, l)
	}
	for _, l := range b.loops {
		b.wg.Add(1)
		go l.run()
	}
	return b, nil
}

// serve accepts until the listener closes and hands each connection to its
// fd-sharded loop. Loop teardown belongs to stop (driven by Close), not to
// this return: the accept loop can exit while connections are still live.
func (b *reactorBackend) serve() error {
	for {
		nc, err := b.s.ln.Accept()
		if err != nil {
			if b.s.isClosed() {
				return nil
			}
			return err
		}
		fd, ok := adoptFD(nc)
		if !ok {
			continue
		}
		b.loops[fd%len(b.loops)].enqueue(fd)
	}
}

// stop shuts the loops down and joins them, then releases the loop
// descriptors. The epoll and event fds outlive the join on purpose: a late
// accept racing Close can still call enqueue, whose wake write must never
// land on a closed (and possibly reused) fd. enqueue wakes under the loop
// mutex and checks closing there, so after every loop has observed closing
// no wake can start, and closing the fds after the join is safe.
func (b *reactorBackend) stop() {
	b.once.Do(func() {
		for _, l := range b.loops {
			l.stop()
		}
		b.wg.Wait()
		b.stopLoops()
	})
}

// wakes sums the loops' owner-to-loop eventfd writes for NetStats.
func (b *reactorBackend) wakes() uint64 {
	var n uint64
	for _, l := range b.loops {
		l.mu.Lock()
		n += l.ownerWakes
		l.mu.Unlock()
	}
	return n
}

// stopLoops closes the per-loop descriptors, for teardown and for the
// construction error path (where loops never ran).
func (b *reactorBackend) stopLoops() {
	for _, l := range b.loops {
		_ = syscall.Close(l.epfd)
		_ = syscall.Close(l.evfd)
	}
}

// adoptFD takes a freshly accepted connection whole: dup the socket fd out of
// the runtime, close the net.Conn (the socket lives on through the dup), then
// re-set TCP_NODELAY and nonblocking on the dup itself. The NODELAY re-set is
// not redundant: doc 08 section 6.3 names the silently-missing-NODELAY dup as
// the trap that puts a 40ms Nagle floor under P1, so the option is set on the
// exact fd the loop will write, not trusted to the runtime's copy.
func adoptFD(nc net.Conn) (int, bool) {
	tc, ok := nc.(*net.TCPConn)
	if !ok {
		_ = nc.Close()
		return -1, false
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		_ = tc.Close()
		return -1, false
	}
	dupfd := -1
	ctlErr := raw.Control(func(fd uintptr) {
		if d, derr := syscall.Dup(int(fd)); derr == nil {
			dupfd = d
		}
	})
	_ = tc.Close()
	if ctlErr != nil || dupfd < 0 {
		if dupfd >= 0 {
			_ = closeFD(dupfd)
		}
		return -1, false
	}
	if syscall.SetsockoptInt(dupfd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1) != nil ||
		syscall.SetNonblock(dupfd, true) != nil {
		_ = closeFD(dupfd)
		return -1, false
	}
	return dupfd, true
}

// reactorLoop is one event loop: one epoll instance, one eventfd, and the
// fd-indexed table of connections it owns. Only the loop goroutine touches
// conns and the epoll interest set; other goroutines reach the loop solely
// through the mutex-guarded pending and dirty queues plus an eventfd wake.
type reactorLoop struct {
	b    *reactorBackend
	epfd int
	evfd int

	conns []*reactorConn // indexed by fd; loop goroutine only

	// The buffer free lists (doc 08 section 6.2): buffers are leased, not
	// owned, so a parked-clean connection holds no read or reply buffer at
	// all. Loop-goroutine only, like conns, so no lock; capped at loopBufFree.
	rfree [][]byte // read buffers, len readBufSize
	ofree [][]byte // reply buffers, len 0, cap replyBuf

	mu      sync.Mutex
	pending []int          // accepted fds awaiting adoption
	dirty   []*reactorConn // conns owed a writer service, posted by owners
	// dirtySpare is the drained dirty list's backing, swapped back in by
	// drainWake so the steady wake traffic appends into recycled capacity
	// instead of regrowing from nil every drain.
	dirtySpare []*reactorConn
	closing    bool

	// ownerWakes counts the owner-to-loop eventfd writes (WakeLoop calls that
	// wrote), the figure the slice 4 batching exists to shrink. Written under
	// mu, so effectively single-writer; aggregated on read by backend.wakes.
	ownerWakes uint64
}

// reactorConn is one connection owned entirely by a single loop: the dup'd
// fd, the shard connection, the resumable parser, and the read and reply
// buffers. Everything here is loop-goroutine state except queued, which the
// owner-side notify path claims.
type reactorConn struct {
	loop *reactorLoop
	fd   int
	sc   *shard.Conn
	cs   *connState
	p    resp.Parser

	// Read buffer, same discipline as readLoop (doc 08 section 2.1): rbuf
	// holds [pos, n) unparsed bytes, compacted or grown at pass boundaries.
	// Leased from the loop's free list on first read, nil while the
	// connection is parked clean (doc 08 section 6.2).
	rbuf []byte
	pos  int
	n    int

	// out is the pending reply bytes. DrainReplies and stepStream emit into
	// it; flushOut writes as much as the socket takes and keeps the
	// remainder. A streamed giant reply feeds it one headroom-bounded
	// quantum at a time (stepStream), so its working bound is the reply
	// buffer plus the point-reply backlog, never the value; OutBufLimitBytes
	// is the hard cap behind that (doc 08 section 3.5).
	out  []byte
	emit func([]byte)

	// armed is the epoll interest currently registered, so rearm only pays
	// the epoll_ctl when the wanted set changes.
	armed uint32

	// writeBlocked: a short write left bytes in out; interest is EPOLLOUT
	// and reads hold until the peer drains (backpressure).
	writeBlocked bool

	// throttled: complete commands sit unparsed in rbuf because the pipeline
	// window is full. Reads are disarmed entirely (a level-triggered fd we
	// refuse to read would spin the loop); the window reopening on a drain
	// resumes the parse and re-arms.
	throttled bool

	// closing: a protocol error was answered; the connection drains its owed
	// replies, flushes, and closes, reading nothing more.
	closing bool

	// queued marks the conn as sitting in its loop's dirty list, so racing
	// owner marks fold into one dirty entry (and one service). drainWake
	// clears it before servicing, so a mark landing mid-service queues fresh
	// and forces a new WakeLoop; that ordering is what lets a marker whose
	// CAS loses skip the wake delivery safely.
	queued atomic.Bool
}

func newReactorLoop(b *reactorBackend) (*reactorLoop, error) {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	evfd, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, efdCloexec|efdNonblock, 0)
	if errno != 0 {
		_ = syscall.Close(epfd)
		return nil, errno
	}
	l := &reactorLoop{b: b, epfd: epfd, evfd: int(evfd)}
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(l.evfd)}
	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, l.evfd, &ev); err != nil {
		_ = syscall.Close(epfd)
		_ = syscall.Close(l.evfd)
		return nil, err
	}
	return l, nil
}

// enqueue hands an accepted fd to the loop. It runs on the accept goroutine.
// The wake stays under the mutex so it can never start after stop has marked
// the loop closing, which is what lets stop close the eventfd after the join.
func (l *reactorLoop) enqueue(fd int) {
	l.mu.Lock()
	if l.closing {
		l.mu.Unlock()
		_ = closeFD(fd)
		return
	}
	l.pending = append(l.pending, fd)
	l.wake()
	l.mu.Unlock()
}

// stop asks the loop to tear down at its next wake.
func (l *reactorLoop) stop() {
	l.mu.Lock()
	l.closing = true
	l.wake()
	l.mu.Unlock()
}

// markDirty is the mark half of the owner-to-loop wake edge (the connection's
// SetWriterBatchNotify hook): it queues rc on the loop's dirty list and
// reports whether this call queued it. The queued flag folds racing owners
// into one dirty entry, and drainWake clears it before servicing so a mark
// landing mid-service re-queues. No eventfd write happens here; the caller
// owes the loop one WakeLoop after its marks (the worker sends one per
// touched loop at the end of its drain pass), and a false return means an
// earlier mark's entry is still queued with that delivery behind it.
func (l *reactorLoop) markDirty(rc *reactorConn) bool {
	if !rc.queued.CompareAndSwap(false, true) {
		return false
	}
	l.mu.Lock()
	l.dirty = append(l.dirty, rc)
	l.mu.Unlock()
	return true
}

// WakeLoop is the delivery half (shard.LoopWaker): one eventfd write covering
// every markDirty since the loop's last drain. The write stays under the
// mutex for the same reason enqueue's does: once stop has marked the loop
// closing no new wake can start, so backend.stop can close the eventfd after
// the join without a worker's late delivery landing on a reused fd. The
// eventfd is a counter, so a write after drainWake's read leaves the fd
// readable and epoll reports it again: a connection marked dirty after the
// loop's list swap always gets the loop back, which is the lost-wake proof
// this batching leans on.
func (l *reactorLoop) WakeLoop() {
	l.mu.Lock()
	if !l.closing {
		l.ownerWakes++
		l.wake()
	}
	l.mu.Unlock()
}

// wake pokes the loop's eventfd. The write adds to the eventfd counter, so
// it cannot be lost: the counter stays readable (and epoll stays ready) from
// the write until the loop's drain, whether the loop was parked or mid-pass.
func (l *reactorLoop) wake() {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 1)
	for {
		if _, err := syscall.Write(l.evfd, buf[:]); err != syscall.EINTR {
			return
		}
	}
}

// run is the loop: park in epoll_wait, service every ready fd. The eventfd
// delivers adoptions, owner notifies, and shutdown; every other fd is a
// client socket.
func (l *reactorLoop) run() {
	defer l.b.wg.Done()
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
			if fd == l.evfd {
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
			if ev.Events&syscall.EPOLLOUT != 0 && rc.writeBlocked {
				if !l.flushOut(rc) {
					continue
				}
				if !rc.writeBlocked {
					// The block lifted: drain whatever completed while
					// writes were held, then fall back to normal interest.
					l.service(rc)
					continue
				}
			}
			if ev.Events&(syscall.EPOLLIN|syscall.EPOLLRDHUP) != 0 &&
				!rc.writeBlocked && !rc.throttled && !rc.closing {
				l.serviceRead(rc)
			}
		}
	}
}

// drainWake empties the eventfd, adopts pending fds, and services dirty
// connections. It reports whether shutdown was requested. Adoption happens
// here, on the loop goroutine, so the conn table stays single-threaded.
func (l *reactorLoop) drainWake() (closing bool) {
	var buf [8]byte
	for {
		if _, err := syscall.Read(l.evfd, buf[:]); err != syscall.EINTR {
			break
		}
	}
	l.mu.Lock()
	if l.closing {
		l.mu.Unlock()
		return true
	}
	pending := l.pending
	l.pending = nil
	dirty := l.dirty
	l.dirty = l.dirtySpare
	l.dirtySpare = nil
	l.mu.Unlock()
	for _, fd := range pending {
		l.adopt(fd)
	}
	for i, rc := range dirty {
		dirty[i] = nil // do not pin the conn from the spare's backing
		rc.queued.Store(false)
		if rc.fd < 0 {
			continue
		}
		l.service(rc)
	}
	if dirty != nil {
		l.dirtySpare = dirty[:0]
	}
	return false
}

// adopt installs an accepted fd: shard connection, notify hook, counter
// registration, epoll registration, and the initial writer park so the first
// owner completion claims a wake.
func (l *reactorLoop) adopt(fd int) {
	rc := &reactorConn{
		loop: l,
		fd:   fd,
		sc:   l.b.s.rt.NewConn(),
	}
	rc.cs = &connState{sc: rc.sc}
	// No buffers yet: they are leased on first use (serviceRead for rbuf, the
	// emit below for out) and returned when the connection parks clean, so an
	// idle connection holds no buffers at all (doc 08 section 6.2). The nil
	// check in emit is the lease point for the reply buffer; append on a leased
	// buffer is exactly append on the old owned one.
	rc.emit = func(rep []byte) {
		if rc.out == nil {
			rc.out = l.leaseOut()
		}
		rc.out = append(rc.out, rep...)
	}
	// Step mode: a streamed reply becomes a cursor the loop advances with
	// StreamStep instead of a blocking emit; the loop must never wait on one
	// connection's chunk ring while the rest of its fd shard is ready.
	rc.sc.SetStreamStep()
	// Before any traffic, per the seam's contract: owners read the hook
	// through the inbound queue's ordering. The batched form: a worker's
	// drain pass marks each claimed connection dirty here and sends one
	// WakeLoop per touched loop at the pass end; wakes claimed outside a pass
	// (Close, a stream abort) mark and deliver immediately through the same
	// pair.
	rc.sc.SetWriterBatchNotify(l, func() bool { return l.markDirty(rc) })
	for len(l.conns) <= fd {
		l.conns = append(l.conns, nil)
	}
	l.conns[fd] = rc
	rc.armed = syscall.EPOLLIN | uint32(syscall.EPOLLRDHUP)
	ev := syscall.EpollEvent{Events: rc.armed, Fd: int32(fd)}
	if err := syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_ADD, fd, &ev); err != nil {
		l.conns[fd] = nil
		rc.sc.Close()
		_ = closeFD(fd)
		rc.fd = -1
		return
	}
	l.b.s.register(rc.cs)
	rc.sc.ParkWriter() // fresh conn, nothing queued; parks clean
}

// leaseRbuf hands out a read buffer: the free list first, the allocator past
// it. Loop goroutine only.
func (l *reactorLoop) leaseRbuf() []byte {
	if n := len(l.rfree); n > 0 {
		b := l.rfree[n-1]
		l.rfree[n-1] = nil
		l.rfree = l.rfree[:n-1]
		return b
	}
	return make([]byte, l.b.s.readBuf)
}

// leaseOut is leaseRbuf for the reply buffer.
func (l *reactorLoop) leaseOut() []byte {
	if n := len(l.ofree); n > 0 {
		b := l.ofree[n-1]
		l.ofree[n-1] = nil
		l.ofree = l.ofree[:n-1]
		return b
	}
	return make([]byte, 0, l.b.s.replyBuf)
}

// releaseIdle returns a cleanly parked connection's buffers to the loop's
// free lists, the lease-not-own half of doc 08 section 6.2: an idle
// connection holds nothing, so 512 mostly-parked connections cost their
// reactorConn structs and not 64MiB of buffers. Clean means genuinely idle:
// no unparsed bytes, no unsent reply, no hold that will resume with state in
// hand. Only stock-sized buffers go back on the list; a grown buffer drops to
// the GC here, which is the pooling policy's grown-buffer rule and also what
// makes the old 16x-shrink heuristic mostly moot on this driver.
func (l *reactorLoop) releaseIdle(rc *reactorConn) {
	if rc.closing || rc.throttled || rc.writeBlocked || rc.pos != 0 || rc.n != 0 || len(rc.out) != 0 {
		return
	}
	if rc.rbuf != nil {
		if len(rc.rbuf) == l.b.s.readBuf && len(l.rfree) < loopBufFree {
			l.rfree = append(l.rfree, rc.rbuf)
		}
		rc.rbuf = nil
	}
	if rc.out != nil {
		if cap(rc.out) == l.b.s.replyBuf && len(l.ofree) < loopBufFree {
			l.ofree = append(l.ofree, rc.out)
		}
		rc.out = nil
	}
}

func (l *reactorLoop) get(fd int) *reactorConn {
	if fd >= 0 && fd < len(l.conns) {
		return l.conns[fd]
	}
	return nil
}

// serviceRead does one non-blocking read into the connection's buffer and
// runs the service pump over what arrived. Level-triggered epoll re-reports
// the fd next turn if the read left bytes on the socket, so one read per
// readiness keeps the loop's time per connection bounded.
func (l *reactorLoop) serviceRead(rc *reactorConn) {
	if rc.rbuf == nil {
		rc.rbuf = l.leaseRbuf()
	}
	if rc.n == len(rc.rbuf) {
		if rc.pos > 0 {
			rc.n = copy(rc.rbuf, rc.rbuf[rc.pos:rc.n])
			rc.pos = 0
		} else {
			// One command larger than the buffer: grow, there is no other
			// way to see its end.
			bigger := make([]byte, 2*len(rc.rbuf))
			rc.n = copy(bigger, rc.rbuf[:rc.n])
			rc.rbuf = bigger
		}
	}
	m, err := syscall.Read(rc.fd, rc.rbuf[rc.n:])
	rc.cs.reads.bump()
	if m > 0 {
		rc.n += m
		l.service(rc)
		return
	}
	if err == syscall.EAGAIN || err == syscall.EINTR {
		return
	}
	// Zero is the peer's clean close; anything else is a dead socket.
	l.closeConn(rc)
}

// service is the pump: parse and dispatch what the window allows, drain
// completed replies into the output buffer, flush, and either resume the
// parse backlog or park the writer side. The ParkWriter at the bottom is the
// lost-wake guard: it must be the last touch before the loop goes back to
// epoll_wait, so an owner push landing after the drain either shows up in
// the park's re-check or claims the notify that brings the loop back here.
func (l *reactorLoop) service(rc *reactorConn) {
	for {
		if !l.parsePass(rc) {
			return // dispatch failed; the conn is closed
		}
		rc.sc.DrainReplies(rc.emit)
		yielded := l.stepStream(rc)
		if rc.fd < 0 {
			return // a write error inside the step closed the conn
		}
		if rc.sc.Failed() || rc.sc.StreamAborted() {
			// A streamed reply died after its header went out; nothing
			// coherent can follow. Best-effort flush, then drop, mid-cycle,
			// this conn only. StreamAborted covers the write-blocked case,
			// where the step never ran to observe the failure.
			_ = l.flushOut(rc)
			if rc.fd >= 0 {
				l.closeConn(rc)
			}
			return
		}
		if !l.flushOut(rc) {
			return // write error closed the conn
		}
		if rc.closing {
			if !rc.sc.Owes() && len(rc.out) == 0 {
				l.closeConn(rc)
				return
			}
		} else if rc.throttled && rc.sc.CanEnqueue() && !rc.sc.Blocked() {
			// The drain opened the pipeline window and no block is outstanding;
			// consume the parse backlog before parking. A still-blocked
			// connection stays parked with its backlog until the serving push
			// drains the barrier, then this same edge re-parses it.
			continue
		}
		if yielded {
			// The stream still has ready chunks but this pass's quanta are
			// spent; stepStream's requeue guarantees the loop comes back.
			// Parking would spin (ParkWriter re-checks stream readiness), so
			// the writer stays unparked and the dirty entry is the wake.
			break
		}
		if rc.writeBlocked && rc.sc.StreamReady() {
			// Chunks are ready but the socket is full: the resume edge is
			// EPOLLOUT, already armed by the short write. Same no-park
			// reasoning as the yield above; completions that land meanwhile
			// queue at the connection and drain when writability returns.
			break
		}
		if rc.sc.ParkWriter() {
			l.releaseIdle(rc)
			break
		}
		// Replies landed between the drain and the park; go around.
	}
	l.rearm(rc)
}

// stepStream advances the connection's in-progress streamed reply, the Class
// OF quantum on the consumer side: emit ready chunks into the output buffer
// only while it has headroom under the reply-buffer mark (doc 08 section 3.5,
// the backlog-headroom check before building the next chunk), flush between
// quanta, and stop after streamQuanta rounds. A stream still ready past the
// budget requeues the connection behind the loop's own eventfd, so every
// other ready fd is serviced between quanta; a stream stalled on the socket
// resumes on EPOLLOUT, and one stalled on the ring resumes on the pump's
// writer wake. It reports whether it stopped on the quanta budget with the
// stream still ready (the caller must not park the writer then).
func (l *reactorLoop) stepStream(rc *reactorConn) bool {
	if !rc.sc.StreamReady() {
		return false
	}
	for q := 0; q < streamQuanta; q++ {
		if rc.writeBlocked || rc.fd < 0 || !rc.sc.StreamReady() {
			return false
		}
		headroom := l.b.s.replyBuf - len(rc.out)
		if headroom <= 0 {
			if !l.flushOut(rc) {
				return false
			}
			continue
		}
		rc.sc.StreamStep(rc.emit, headroom)
		if rc.sc.Failed() {
			return false
		}
		if !l.flushOut(rc) {
			return false
		}
	}
	if !rc.writeBlocked && rc.sc.StreamReady() {
		l.requeue(rc)
		return true
	}
	return false
}

// requeue marks rc dirty on its own loop and pokes the eventfd, the yield
// half of the Class OF discipline: the pass's remaining stream work runs on a
// later drainWake turn, after every other fd ready in this epoll batch has
// been serviced. A false markDirty means an earlier mark is still queued with
// a delivery behind it, so the loop is coming back regardless.
func (l *reactorLoop) requeue(rc *reactorConn) {
	if !l.markDirty(rc) {
		return
	}
	l.mu.Lock()
	if !l.closing {
		l.wake()
	}
	l.mu.Unlock()
}

// parsePass parses every complete command the buffer holds while the
// pipeline window is open and dispatches each through the shard hop, then
// publishes the batch (the section 2.2 boundary). It stops early when the
// window fills: the Do throttle must never engage on the loop thread (its
// wait paths would block or spin the whole fd shard), so the window is
// checked before every dispatch and the leftover stays in rbuf as the
// throttle backlog. It returns false when it closed the connection.
func (l *reactorLoop) parsePass(rc *reactorConn) bool {
	if rc.closing {
		return true
	}
	cmds := uint64(0)
	throttled := false
	for rc.pos < rc.n {
		// The pipeline window closing and an unresolved block both stop the
		// parse the same way: the leftover stays in rbuf as the backlog and the
		// resume edge (a drain that clears CanEnqueue, or a serving push that
		// clears Blocked) re-runs parsePass. Folding Blocked in here means a
		// command pipelined behind a BLPOP does not run until the block's reply
		// goes out, and it costs one relaxed load on the open-window path.
		if !rc.sc.CanEnqueue() || rc.sc.Blocked() {
			throttled = true
			break
		}
		args, consumed, st := rc.p.Next(rc.rbuf[rc.pos:rc.n])
		if st == resp.NeedMore {
			break
		}
		if st == resp.ProtoErr {
			// Answer in pipeline order, then drain and close: the stream
			// cannot be resynced after a framing error.
			_ = rc.sc.Do(shard.OpError, false, [][]byte{[]byte("ERR Protocol error: " + rc.p.LastError())})
			rc.closing = true
			break
		}
		rc.pos += consumed
		if len(args) == 0 {
			continue
		}
		cmds++
		if derr := dispatch.Dispatch(rc.sc, args); derr != nil {
			l.closeConn(rc)
			return false
		}
	}
	rc.throttled = throttled
	if cmds > 0 {
		rc.cs.commands.add(cmds)
		rc.cs.batches.bump()
	}
	if rc.pos == rc.n {
		rc.pos, rc.n = 0, 0
		// A giant inbound bulk grew the buffer; once drained, drop it so a
		// megabytes-wide buffer never lingers. The next read leases a stock
		// one; a grown buffer never enters the free list.
		if len(rc.rbuf) > 16*l.b.s.readBuf {
			rc.rbuf = nil
		}
	} else if rc.pos > 0 && rc.n-rc.pos <= compactMax {
		rc.n = copy(rc.rbuf, rc.rbuf[rc.pos:rc.n])
		rc.pos = 0
	}
	rc.sc.Flush()
	return true
}

// flushOut writes as much of out as the socket accepts. On EAGAIN it keeps
// the unsent remainder at the front of out and flips writeBlocked, which
// swings interest to EPOLLOUT and holds reads until the peer drains. It
// returns false when a write error closed the connection.
func (l *reactorLoop) flushOut(rc *reactorConn) bool {
	if len(rc.out) == 0 || rc.fd < 0 {
		return rc.fd >= 0
	}
	buf := rc.out
	wrote := false
	for len(buf) > 0 {
		// Count the attempt before the syscall, the countedWriter discipline:
		// a reader that saw these bytes arrive must also see the counter
		// moved, and bumping after the return leaves a window where the loop
		// goroutine is descheduled between the kernel delivering the bytes
		// and the bump (a NetStats snapshot taken there undercounts). EAGAIN
		// and EINTR attempts are write(2) calls too, so counting them is
		// what the counter's name says.
		rc.cs.writes.bump()
		n, err := syscall.Write(rc.fd, buf)
		if n > 0 {
			wrote = true
			buf = buf[n:]
			continue
		}
		switch err {
		case syscall.EAGAIN:
			rc.out = append(rc.out[:0], buf...) // shift remainder to the front
			if lim := l.b.s.outLimit; lim > 0 && len(rc.out) > lim {
				// The client's unread backlog passed the hard cap (doc 08
				// section 3.5, client-output-buffer-limit): disconnect it,
				// mid-cycle, this connection only, and count the event for
				// INFO. The shard conn's Close fails any in-flight stream on
				// the producer side, so nothing leaks.
				l.b.s.outbufDrops.Add(1)
				l.closeConn(rc)
				return false
			}
			rc.writeBlocked = true
			if wrote {
				l.b.s.flushes.Add(1)
			}
			l.rearm(rc)
			return true
		case syscall.EINTR:
			continue
		default:
			l.closeConn(rc)
			return false
		}
	}
	rc.out = rc.out[:0]
	// A streamed giant reply grew the buffer; give the pages back once sent.
	// nil, not a fresh allocation: the next emit leases from the free list,
	// and the grown buffer never enters it.
	if cap(rc.out) > 4*l.b.s.replyBuf {
		rc.out = nil
	}
	if wrote {
		l.b.s.flushes.Add(1)
	}
	if rc.writeBlocked {
		rc.writeBlocked = false
		l.rearm(rc)
	}
	return true
}

// rearm recomputes and applies the connection's epoll interest. Reads are
// armed only when nothing holds them: not write backpressure, not the
// pipeline-window throttle, not a close in progress. A throttled connection
// drops even EPOLLRDHUP, because a level-triggered readable fd the loop
// refuses to read would spin epoll_wait; a peer that dies while throttled is
// caught by EPOLLHUP/EPOLLERR (always delivered) or by the write path.
func (l *reactorLoop) rearm(rc *reactorConn) {
	if rc.fd < 0 {
		return
	}
	var want uint32
	if rc.writeBlocked {
		want |= syscall.EPOLLOUT
	} else if !rc.throttled && !rc.closing {
		want |= syscall.EPOLLIN | uint32(syscall.EPOLLRDHUP)
	}
	if want == rc.armed {
		return
	}
	ev := syscall.EpollEvent{Events: want, Fd: int32(rc.fd)}
	_ = syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_MOD, rc.fd, &ev)
	rc.armed = want
}

// closeConn deregisters and closes a connection: epoll DEL, one close of the
// dup'd fd, shard conn close (which drops in-flight replies by contract),
// counter fold. Idempotent through the fd sentinel so the read and write
// paths can both call it without coordinating.
func (l *reactorLoop) closeConn(rc *reactorConn) {
	if rc.fd < 0 {
		return
	}
	_ = syscall.EpollCtl(l.epfd, syscall.EPOLL_CTL_DEL, rc.fd, nil)
	_ = closeFD(rc.fd)
	if rc.fd < len(l.conns) {
		l.conns[rc.fd] = nil
	}
	rc.fd = -1
	rc.sc.Close()
	l.b.s.unregister(rc.cs)
	// Recycle the buffers regardless of their content: the fd is gone, so any
	// unparsed bytes or unsent reply are dead, and a read fully overwrites
	// [0, n) before the parser sees a leased buffer again. Stock sizes only,
	// same rule as releaseIdle.
	if rc.rbuf != nil {
		if len(rc.rbuf) == l.b.s.readBuf && len(l.rfree) < loopBufFree {
			l.rfree = append(l.rfree, rc.rbuf)
		}
		rc.rbuf = nil
	}
	if rc.out != nil {
		if cap(rc.out) == l.b.s.replyBuf && len(l.ofree) < loopBufFree {
			l.ofree = append(l.ofree, rc.out[:0])
		}
		rc.out = nil
	}
}

// shutdown closes every owned connection and any fds still waiting for
// adoption. It runs on the loop goroutine after stop has been observed. The
// epoll and event fds stay open; backend.stop closes them after the join
// (see stop for the race this avoids).
func (l *reactorLoop) shutdown() {
	for _, rc := range l.conns {
		if rc != nil && rc.fd >= 0 {
			l.closeConn(rc)
		}
	}
	l.conns = nil
	l.mu.Lock()
	pending := l.pending
	l.pending = nil
	l.dirty = nil
	l.mu.Unlock()
	for _, fd := range pending {
		_ = closeFD(fd)
	}
}
