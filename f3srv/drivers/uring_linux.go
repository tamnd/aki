//go:build linux

package drivers

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/dispatch"
	"github.com/tamnd/aki/f3srv/resp"
)

// The io_uring driver (doc 08 section 4.3, M10 slice 3 proper): the reactor's
// architecture with the per-op syscalls deleted. Same M fd-sharded loops
// (lab 19's frozen count), same dup'd fds with TCP_NODELAY re-set on the dup,
// same per-conn resumable resp.Parser and shard.Conn, same inbound MPSC and
// reorder-ring drain with the Owes() boundary, same batched owner-to-loop
// wake through the SetWriterBatchNotify seam. What changes is the syscall
// shape: reads are IORING_OP_RECV completions, writes are IORING_OP_SEND
// submissions, the owner wake is an eventfd read armed inside the ring, and
// one io_uring_enter per loop pass submits everything staged and waits, so
// the loop's syscall traffic is O(passes), not O(ops).
//
// Deliberately single-shot recv, resubmitted per completion: the resubmission
// is an SQE riding the pass's one enter, so it costs no syscall, and multishot
// recv (which needs registered provided-buffer rings) buys back only the SQE
// prep, a measured follow-up per labs/f3/m0/21_uring, not a correctness or
// syscall-count difference.
//
// The correctness cliffs carried from the reactor campaign all apply and are
// answered the same way: the loop is the reorder ring's single consumer; the
// lost-wake proof is the eventfd counter plus the park-then-recheck ordering
// (the armed IORING_OP_READ completes immediately when a wake landed while it
// was unarmed, because the counter stays readable); a failed stream drops its
// connection only; streamed giants emit in streamQuanta-bounded steps and
// requeue behind the loop's own eventfd so they never convoy the fd shard.
//
// Async-send discipline, the one genuinely new edge: a submitted send's
// buffer must not move until its CQE, so the connection double-buffers (out
// stages, inflight is submitted), and every read-buffer move (compact, grow,
// shrink) happens only while no recv is pending, in ensureRecv, right before
// the next recv is armed. Ops in flight across a close are fenced by a
// per-fd-slot generation in the CQE user_data: closeConn bumps it, cancels
// the pending ops, and stale completions (including fd-number reuse by a
// later accept) drop on the mismatch.

// uring ring sizing per loop: the SQ bounds one pass's staged ops (getSQE
// flushes early if a pathological pass overruns it), the CQ is oversized so
// completions land without the kernel-side overflow path in normal traffic.
const (
	uringSQEntries = 1024
	uringCQEntries = 4096
)

// user_data layout: kind in the top byte, the fd slot's generation in the
// middle 32 bits, the fd in the low 24.
const (
	udKindEvRead = 1
	udKindRecv   = 2
	udKindSend   = 3
	udKindCancel = 4
)

func packUD(kind uint8, gen uint32, fd int) uint64 {
	return uint64(kind)<<56 | uint64(gen)<<24 | uint64(fd)&0xffffff
}

func unpackUD(ud uint64) (kind uint8, gen uint32, fd int) {
	return uint8(ud >> 56), uint32(ud >> 24), int(ud & 0xffffff)
}

// uringBackend is the netBackend the server drives, the reactor's twin: loops
// start at construction, serve runs the accept loop on the caller's
// goroutine, stop tears everything down exactly once.
type uringBackend struct {
	s     *Server
	loops []*uringLoop
	wg    sync.WaitGroup
	once  sync.Once
}

// newURingBackend builds the loops or reports why it cannot; the caller logs
// the fallback. The availability probe answers ENOSYS, the io_uring_disabled
// sysctl, seccomp denial, and pre-5.6 kernels missing RECV/SEND in one place.
func newURingBackend(s *Server, o Options) (netBackend, error) {
	if !uringAvailable() {
		return nil, errors.New("io_uring unavailable (kernel, sysctl, or seccomp)")
	}
	n := o.NetLoops
	if n < 1 {
		n = defaultNetLoops()
	}
	b := &uringBackend{s: s}
	for i := 0; i < n; i++ {
		l, err := newURingLoop(b)
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
// fd-sharded loop, exactly the reactor's accept shape (adoptFD re-sets
// TCP_NODELAY and nonblocking on the dup).
func (b *uringBackend) serve() error {
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

// stop shuts the loops down, joins them, then closes the rings and eventfds.
// The descriptors outlive the join for the reactor's reason: a late enqueue
// racing Close must never write a reused fd, and enqueue wakes under the loop
// mutex where closing is checked. Closing the ring fd after the join also
// reaps every operation still in flight (cancellation is the ring teardown's
// contract), so shutdown never waits on a slow peer.
func (b *uringBackend) stop() {
	b.once.Do(func() {
		for _, l := range b.loops {
			l.stop()
		}
		b.wg.Wait()
		b.stopLoops()
	})
}

// wakes sums the loops' owner-to-loop eventfd writes for NetStats.
func (b *uringBackend) wakes() uint64 {
	var n uint64
	for _, l := range b.loops {
		l.mu.Lock()
		n += l.ownerWakes
		l.mu.Unlock()
	}
	return n
}

// stopLoops releases the per-loop descriptors, for teardown and for the
// construction error path.
func (b *uringBackend) stopLoops() {
	for _, l := range b.loops {
		l.ring.close()
		_ = syscall.Close(l.evfd)
	}
}

// uringLoop is one event loop: one ring, one eventfd armed as a ring read,
// and the fd-indexed table of connections it owns. Only the loop goroutine
// touches the ring, conns, and gens; other goroutines reach it solely through
// the mutex-guarded pending and dirty queues plus an eventfd write.
type uringLoop struct {
	b    *uringBackend
	ring *uring
	evfd int

	conns []*uringConn // indexed by fd; loop goroutine only
	gens  []uint32     // per-fd-slot generation; loop goroutine only

	evBuf [8]byte // the armed eventfd read's landing pad

	mu      sync.Mutex
	pending []int        // accepted fds awaiting adoption
	dirty   []*uringConn // conns owed a writer service, posted by owners
	closing bool

	// ownerWakes counts the owner-to-loop eventfd writes, same figure as the
	// reactor's. Written under mu; aggregated by backend.wakes.
	ownerWakes uint64
}

// uringConn is one connection owned entirely by a single loop. Everything
// here is loop-goroutine state except queued, which the owner-side notify
// path claims.
type uringConn struct {
	loop *uringLoop
	fd   int
	gen  uint32
	sc   *shard.Conn
	cs   *connState
	p    resp.Parser

	// Read buffer, [pos, n) unparsed. The kernel writes [n, len) while a recv
	// is pending, so compaction, growth, and shrink happen only in ensureRecv,
	// under recvPending == false.
	rbuf []byte
	pos  int
	n    int

	// out stages reply bytes; inflight is the buffer a submitted send owns
	// until its CQE. The two swap in queueSend, so the working bound per
	// connection is two reply buffers plus the point backlog, and
	// OutBufLimitBytes is the hard cap behind that.
	out      []byte
	inflight []byte
	emit     func([]byte)

	recvPending bool
	sendPending bool

	// throttled: complete commands sit unparsed in rbuf because the pipeline
	// window is full; the recv stays unarmed until a drain reopens the window.
	throttled bool

	// closing: a protocol error was answered; drain the owed replies, flush,
	// close, read nothing more.
	closing bool

	// dropAfterSend: a stream failed after its header went out; whatever is
	// staged gets one best-effort send and the connection drops on its CQE,
	// the async form of the reactor's flush-then-close.
	dropAfterSend bool

	// queued folds racing owner marks into one dirty entry, exactly the
	// reactor's protocol (see markDirty).
	queued atomic.Bool
}

func newURingLoop(b *uringBackend) (*uringLoop, error) {
	r, err := newURing(uringSQEntries, uringCQEntries)
	if err != nil {
		return nil, err
	}
	if r.params.features&uringFeatNoDrop == 0 {
		// Pre-5.5 rings can drop completions on CQ overflow; a driver that can
		// lose a recv completion is a driver that can hang a connection.
		r.close()
		return nil, errors.New("kernel io_uring lacks IORING_FEAT_NODROP")
	}
	evfd, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, efdCloexec|efdNonblock, 0)
	if errno != 0 {
		r.close()
		return nil, errno
	}
	l := &uringLoop{b: b, ring: r, evfd: int(evfd)}
	return l, nil
}

// enqueue hands an accepted fd to the loop, from the accept goroutine. The
// wake stays under the mutex so it can never start after stop marked the loop
// closing, which is what lets stop close the eventfd after the join.
func (l *uringLoop) enqueue(fd int) {
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
func (l *uringLoop) stop() {
	l.mu.Lock()
	l.closing = true
	l.wake()
	l.mu.Unlock()
}

// markDirty is the mark half of the owner-to-loop wake edge, byte-for-byte
// the reactor's: queue rc on the dirty list once, report whether this call
// queued it, and leave the delivery to the caller's WakeLoop.
func (l *uringLoop) markDirty(rc *uringConn) bool {
	if !rc.queued.CompareAndSwap(false, true) {
		return false
	}
	l.mu.Lock()
	l.dirty = append(l.dirty, rc)
	l.mu.Unlock()
	return true
}

// WakeLoop is the delivery half (shard.LoopWaker): one eventfd write covering
// every markDirty since the loop's last drain. The eventfd is a counter, so a
// write landing while the ring read is unarmed (between its completion and
// the re-arm) leaves the count readable and the re-armed read completes
// immediately: the lost-wake proof is the reactor's, carried whole.
func (l *uringLoop) WakeLoop() {
	l.mu.Lock()
	if !l.closing {
		l.ownerWakes++
		l.wake()
	}
	l.mu.Unlock()
}

// wake pokes the loop's eventfd; the counter write cannot be lost.
func (l *uringLoop) wake() {
	var buf [8]byte
	buf[0] = 1
	for {
		if _, err := syscall.Write(l.evfd, buf[:]); err != syscall.EINTR {
			return
		}
	}
}

// armEvRead keeps one read of the eventfd pending in the ring; its completion
// is the wake delivery (adoptions, owner notifies, stream requeues, shutdown).
func (l *uringLoop) armEvRead() {
	sqe := l.ring.getSQE()
	sqe.opcode = uringOpRead
	sqe.fd = int32(l.evfd)
	sqe.addr = uint64(uintptr(unsafe.Pointer(&l.evBuf[0])))
	sqe.len = 8
	sqe.userData = packUD(udKindEvRead, 0, l.evfd)
}

// run is the loop: one submit-and-wait enter per pass, then service every
// completion. The enter both publishes the previous pass's staged SQEs
// (recv re-arms, sends, cancels, the eventfd re-arm) and parks until at
// least one completion is ready.
func (l *uringLoop) run() {
	defer l.b.wg.Done()
	l.armEvRead()
	for {
		if err := l.ring.submitAndWait(); err != nil {
			l.shutdown()
			return
		}
		stop := false
		l.ring.reap(func(cqe uringCQE) {
			if l.handleCQE(cqe) {
				stop = true
			}
		})
		if stop {
			l.shutdown()
			return
		}
	}
}

// handleCQE routes one completion; it reports whether shutdown was requested.
func (l *uringLoop) handleCQE(cqe uringCQE) bool {
	kind, gen, fd := unpackUD(cqe.userData)
	switch kind {
	case udKindEvRead:
		if l.drainWake() {
			return true
		}
		l.armEvRead()
	case udKindRecv:
		rc := l.get(fd)
		if rc == nil || rc.gen != gen {
			return false // stale: the conn closed with this op in flight
		}
		rc.recvPending = false
		l.completeRecv(rc, cqe.res)
	case udKindSend:
		rc := l.get(fd)
		if rc == nil || rc.gen != gen {
			return false
		}
		rc.sendPending = false
		l.completeSend(rc, cqe.res)
	case udKindCancel:
		// The cancel's own completion carries nothing; the canceled op's CQE
		// (ECANCELED or a late success) is dropped by the generation fence.
	}
	return false
}

// drainWake empties the pending and dirty queues under the mutex, adopts new
// fds, and services dirty connections, the reactor's drainWake with the
// eventfd read already consumed by the ring. It reports shutdown.
func (l *uringLoop) drainWake() (closing bool) {
	l.mu.Lock()
	if l.closing {
		l.mu.Unlock()
		return true
	}
	pending := l.pending
	l.pending = nil
	dirty := l.dirty
	l.dirty = nil
	l.mu.Unlock()
	for _, fd := range pending {
		l.adopt(fd)
	}
	for _, rc := range dirty {
		rc.queued.Store(false)
		if rc.fd < 0 {
			continue
		}
		l.service(rc)
		l.ensureRecv(rc)
	}
	return false
}

// adopt installs an accepted fd: shard connection, notify hook, counter
// registration, the initial writer park, and the first armed recv.
func (l *uringLoop) adopt(fd int) {
	rc := &uringConn{
		loop:     l,
		fd:       fd,
		sc:       l.b.s.rt.NewConn(),
		rbuf:     make([]byte, readBufSize),
		out:      make([]byte, 0, l.b.s.replyBuf),
		inflight: make([]byte, 0, l.b.s.replyBuf),
	}
	rc.cs = &connState{sc: rc.sc}
	rc.emit = func(rep []byte) { rc.out = append(rc.out, rep...) }
	rc.sc.SetStreamStep()
	rc.sc.SetWriterBatchNotify(l, func() bool { return l.markDirty(rc) })
	for len(l.conns) <= fd {
		l.conns = append(l.conns, nil)
		l.gens = append(l.gens, 0)
	}
	rc.gen = l.gens[fd]
	l.conns[fd] = rc
	l.b.s.register(rc.cs)
	rc.sc.ParkWriter() // fresh conn, nothing queued; parks clean
	l.ensureRecv(rc)
}

func (l *uringLoop) get(fd int) *uringConn {
	if fd >= 0 && fd < len(l.conns) {
		return l.conns[fd]
	}
	return nil
}

// ensureRecv settles the read buffer and arms the next recv when nothing
// holds reads: no recv already pending, no pipeline-window throttle, no close
// in progress, and no staged backlog behind an in-flight send (the write-side
// backpressure that mirrors the reactor's EPOLLOUT read hold). All rbuf moves
// live here because this is the one point where no kernel write can race
// them.
func (l *uringLoop) ensureRecv(rc *uringConn) {
	if rc.fd < 0 || rc.recvPending || rc.throttled || rc.closing || rc.dropAfterSend {
		return
	}
	if rc.sendPending && len(rc.out) > 0 {
		return
	}
	if rc.pos == rc.n {
		rc.pos, rc.n = 0, 0
		// A giant inbound bulk grew the buffer; once drained, shrink back so
		// an idle connection does not pin megabytes.
		if len(rc.rbuf) > 16*readBufSize {
			rc.rbuf = make([]byte, readBufSize)
		}
	} else if rc.pos > 0 && (rc.n-rc.pos <= compactMax || rc.n == len(rc.rbuf)) {
		rc.n = copy(rc.rbuf, rc.rbuf[rc.pos:rc.n])
		rc.pos = 0
	}
	if rc.n == len(rc.rbuf) {
		// One command larger than the buffer: grow, there is no other way to
		// see its end (pos is 0 here, the compact above ran).
		bigger := make([]byte, 2*len(rc.rbuf))
		rc.n = copy(bigger, rc.rbuf[:rc.n])
		rc.rbuf = bigger
	}
	sqe := l.ring.getSQE()
	sqe.opcode = uringOpRecv
	sqe.fd = int32(rc.fd)
	sqe.addr = uint64(uintptr(unsafe.Pointer(&rc.rbuf[rc.n])))
	sqe.len = uint32(len(rc.rbuf) - rc.n)
	sqe.userData = packUD(udKindRecv, rc.gen, rc.fd)
	rc.recvPending = true
}

// completeRecv handles one recv completion: bytes, peer close, or error.
func (l *uringLoop) completeRecv(rc *uringConn, res int32) {
	rc.cs.reads.bump()
	if rc.dropAfterSend {
		// The connection is already condemned; bytes the peer sent after the
		// stream failure never dispatch, and no recv re-arms.
		return
	}
	if res > 0 {
		rc.n += int(res)
		l.service(rc)
		l.ensureRecv(rc)
		return
	}
	if res < 0 {
		switch syscall.Errno(-res) {
		case syscall.EAGAIN, syscall.EINTR:
			l.ensureRecv(rc)
			return
		case syscall.ECANCELED:
			return
		}
	}
	// Zero is the peer's clean close; anything else is a dead socket.
	l.closeConn(rc)
}

// completeSend handles one send completion: advance or retire the inflight
// buffer, resume whatever the in-flight send was holding (the next staged
// buffer, a stalled stream, the read side), and close when a drain-then-close
// state finished draining.
func (l *uringLoop) completeSend(rc *uringConn, res int32) {
	if res < 0 {
		switch syscall.Errno(-res) {
		case syscall.EAGAIN, syscall.EINTR:
			l.submitSend(rc)
			return
		case syscall.ECANCELED:
			return
		}
		l.closeConn(rc)
		return
	}
	l.b.s.flushes.Add(1)
	if int(res) < len(rc.inflight) {
		// Short send: the remainder stays owned by inflight and resubmits.
		// A dropAfterSend conn got its one best-effort attempt; drop now
		// rather than chase a stalled peer, the reactor's EAGAIN-then-close.
		rc.inflight = rc.inflight[res:]
		if rc.dropAfterSend {
			l.closeConn(rc)
			return
		}
		l.submitSend(rc)
		return
	}
	rc.inflight = rc.inflight[:0]
	// A streamed giant reply grew the buffer; give the pages back once sent.
	if cap(rc.inflight) > 4*l.b.s.replyBuf {
		rc.inflight = make([]byte, 0, l.b.s.replyBuf)
	}
	if rc.dropAfterSend {
		if len(rc.out) == 0 {
			l.closeConn(rc)
			return
		}
		if !l.queueSend(rc) {
			return
		}
		return
	}
	if !l.queueSend(rc) {
		return
	}
	if rc.closing && !rc.sc.Owes() && len(rc.out) == 0 && !rc.sendPending {
		l.closeConn(rc)
		return
	}
	if rc.sc.StreamReady() {
		// The stream was stalled on this send's buffer; advance it.
		l.service(rc)
	}
	l.ensureRecv(rc)
}

// submitSend arms a send for the inflight buffer. The writes counter bumps
// per submission, the countedWriter attempt discipline: every send op is one
// kernel write as the akinet counters see it.
func (l *uringLoop) submitSend(rc *uringConn) {
	rc.cs.writes.bump()
	sqe := l.ring.getSQE()
	sqe.opcode = uringOpSend
	sqe.fd = int32(rc.fd)
	sqe.addr = uint64(uintptr(unsafe.Pointer(&rc.inflight[0])))
	sqe.len = uint32(len(rc.inflight))
	sqe.opFlags = msgNoSignal
	sqe.userData = packUD(udKindSend, rc.gen, rc.fd)
	rc.sendPending = true
}

// queueSend ships the staged output: swap out into inflight and submit when
// no send is pending, and enforce OutBufLimitBytes on the backlog a pending
// send is holding up (doc 08 section 3.5's client-output-buffer-limit,
// counted and disconnected mid-cycle, that connection only). It returns false
// when it closed the connection.
func (l *uringLoop) queueSend(rc *uringConn) bool {
	if rc.fd < 0 {
		return false
	}
	if rc.sendPending {
		if lim := l.b.s.outLimit; lim > 0 && len(rc.out) > lim {
			l.b.s.outbufDrops.Add(1)
			l.closeConn(rc)
			return false
		}
		return true
	}
	if len(rc.out) == 0 {
		return true
	}
	rc.out, rc.inflight = rc.inflight[:0], rc.out
	l.submitSend(rc)
	return true
}

// service is the pump, the reactor's shape with the flush turned into a send
// submission: parse and dispatch what the window allows, drain completed
// replies, step the stream, ship the staged bytes, and either resume the
// parse backlog or park the writer. The ParkWriter at the bottom is the same
// lost-wake guard: it is the last touch before the loop goes back to the
// ring wait, so an owner push landing after the drain either shows in the
// park's re-check or claims the notify.
func (l *uringLoop) service(rc *uringConn) {
	if rc.dropAfterSend {
		return // condemned; the pending send's CQE finishes the close
	}
	for {
		if !l.parsePass(rc) {
			return // dispatch failed; the conn is closed
		}
		rc.sc.DrainReplies(rc.emit)
		yielded := l.stepStream(rc)
		if rc.fd < 0 {
			return // a close inside the step (cap, cancel) took the conn
		}
		if rc.sc.Failed() || rc.sc.StreamAborted() {
			// A streamed reply died after its header went out; nothing
			// coherent can follow. One best-effort send of what is staged,
			// then drop, mid-cycle, this conn only.
			rc.dropAfterSend = true
			if !rc.sendPending && len(rc.out) == 0 {
				l.closeConn(rc)
				return
			}
			_ = l.queueSend(rc)
			return
		}
		if !l.queueSend(rc) {
			return // the output cap closed the conn
		}
		if rc.closing {
			if !rc.sc.Owes() && len(rc.out) == 0 && !rc.sendPending {
				l.closeConn(rc)
				return
			}
		} else if rc.throttled && rc.sc.CanEnqueue() {
			// The drain opened the pipeline window; consume the parse
			// backlog before parking.
			continue
		}
		if yielded {
			// The stream still has ready chunks but this pass's quanta are
			// spent; stepStream's requeue guarantees the loop comes back.
			// Parking would spin (ParkWriter re-checks stream readiness).
			break
		}
		if rc.sendPending && rc.sc.StreamReady() {
			// Chunks are ready but both buffers are full behind the in-flight
			// send: the resume edge is the send CQE (completeSend re-enters
			// service). Same no-park reasoning as the yield above.
			break
		}
		if rc.sc.ParkWriter() {
			break
		}
		// Replies landed between the drain and the park; go around.
	}
}

// stepStream advances the connection's in-progress streamed reply in
// headroom-bounded quanta, the reactor's Class OF discipline with the flush
// replaced by a send submission: emit while the staging buffer has headroom,
// ship between quanta, stop after streamQuanta rounds. A stream still ready
// past the budget requeues the connection behind the loop's own eventfd; one
// stalled behind the in-flight send resumes on the send CQE. It reports
// whether it stopped on the quanta budget with a requeue posted.
func (l *uringLoop) stepStream(rc *uringConn) bool {
	if !rc.sc.StreamReady() {
		return false
	}
	for q := 0; q < streamQuanta; q++ {
		if rc.fd < 0 || !rc.sc.StreamReady() {
			return false
		}
		headroom := l.b.s.replyBuf - len(rc.out)
		if headroom <= 0 {
			if !l.queueSend(rc) {
				return false
			}
			if len(rc.out) > 0 {
				// Staging full behind an in-flight send: the send CQE is the
				// resume edge, and the pump's no-park break covers the gap.
				return false
			}
			continue
		}
		rc.sc.StreamStep(rc.emit, headroom)
		if rc.sc.Failed() {
			return false
		}
		if !l.queueSend(rc) {
			return false
		}
	}
	if rc.sc.StreamReady() && len(rc.out) < l.b.s.replyBuf {
		l.requeue(rc)
		return true
	}
	return false
}

// requeue marks rc dirty on its own loop and pokes the eventfd, the Class OF
// yield: the remaining stream work runs on a later wake turn, after every
// other connection ready in this pass has been serviced.
func (l *uringLoop) requeue(rc *uringConn) {
	if !l.markDirty(rc) {
		return
	}
	l.mu.Lock()
	if !l.closing {
		l.wake()
	}
	l.mu.Unlock()
}

// parsePass parses every complete command the buffer holds while the pipeline
// window is open and dispatches each through the shard hop, then publishes
// the batch (the section 2.2 boundary), the reactor's parsePass minus the
// buffer housekeeping (that lives in ensureRecv, where no pending recv can
// race it). It returns false when it closed the connection.
func (l *uringLoop) parsePass(rc *uringConn) bool {
	if rc.closing {
		return true
	}
	cmds := uint64(0)
	throttled := false
	for rc.pos < rc.n {
		if !rc.sc.CanEnqueue() {
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
	rc.sc.Flush()
	return true
}

// closeConn deregisters and closes a connection: cancel the ops still in
// flight (a pending recv holds the socket's file reference past our fd close,
// so without the cancel a silent peer would pin the connection open), bump
// the slot generation so their late completions drop, one close of the dup'd
// fd, shard conn close, counter fold. Idempotent through the fd sentinel.
func (l *uringLoop) closeConn(rc *uringConn) {
	if rc.fd < 0 {
		return
	}
	fd := rc.fd
	if rc.recvPending {
		l.cancelOp(packUD(udKindRecv, rc.gen, fd))
	}
	if rc.sendPending {
		l.cancelOp(packUD(udKindSend, rc.gen, fd))
	}
	l.gens[fd]++
	l.conns[fd] = nil
	rc.fd = -1
	_ = closeFD(fd)
	rc.sc.Close()
	l.b.s.unregister(rc.cs)
}

// cancelOp stages an IORING_OP_ASYNC_CANCEL for the op with the given
// user_data; it rides the pass's enter like everything else.
func (l *uringLoop) cancelOp(target uint64) {
	sqe := l.ring.getSQE()
	sqe.opcode = uringOpAsyncCancel
	sqe.fd = -1
	sqe.addr = target
	sqe.userData = packUD(udKindCancel, 0, 0)
}

// shutdown closes every owned connection and any fds still waiting for
// adoption, on the loop goroutine after stop has been observed. Ops still in
// flight are reaped by the ring fd close in backend.stop, which cancels
// everything, so no cancel ceremony is needed here.
func (l *uringLoop) shutdown() {
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
