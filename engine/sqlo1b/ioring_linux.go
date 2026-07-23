package sqlo1b

import (
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// io_uring by raw syscall (doc 04 section 12). One ring, one
// submitter goroutine that owns the SQ, one reaper that owns the CQ,
// and the same dedicated sync goroutine as iopool, because fsync
// never enters the ring: it blocks in the kernel's worker-thread
// fallback and the write-then-fdatasync shape is faster anyway.
// SQPOLL and IOPOLL are out of scope with reasons recorded in the
// doc; this is the plain enter-per-batch discipline.

// Syscall numbers are asm-generic and identical on amd64 and arm64.
const (
	sysRingSetup    = 425
	sysRingEnter    = 426
	sysRingRegister = 427
)

const (
	ringOffSQRing = 0x0
	ringOffSQEs   = 0x10000000

	ringEnterGetEvents = 1 << 0
	ringFeatSingleMmap = 1 << 0

	ringOpNop        = 0
	ringOpReadFixed  = 4
	ringOpWriteFixed = 5
	ringOpRead       = 22
	ringOpWrite      = 23

	ringRegisterBuffers = 0
	ringRegisterProbe   = 8
	ringOpSupported     = 1 << 0

	sqeSize = 64
	cqeSize = 16
)

// ringSQE is struct io_uring_sqe for the ops this backend submits;
// the tail 22 bytes cover the union fields it never sets.
type ringSQE struct {
	opcode   uint8
	flags    uint8
	ioprio   uint16
	fd       int32
	off      uint64
	addr     uint64
	len      uint32
	opflags  uint32
	userData uint64
	bufIndex uint16
	_        [22]byte
}

// ringCQE is struct io_uring_cqe.
type ringCQE struct {
	userData uint64
	res      int32
	flags    uint32
}

type ringSQOffsets struct {
	head, tail, ringMask, ringEntries uint32
	flags, dropped, array, resv1      uint32
	userAddr                          uint64
}

type ringCQOffsets struct {
	head, tail, ringMask, ringEntries uint32
	overflow, cqes, flags, resv1      uint32
	userAddr                          uint64
}

// ringParams is struct io_uring_params.
type ringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFD         uint32
	resv         [3]uint32
	sqOff        ringSQOffsets
	cqOff        ringCQOffsets
}

// The kernel ABI is fixed; a drifting struct is a build error.
const (
	_ = uint(unsafe.Sizeof(ringSQE{}) - sqeSize)
	_ = uint(sqeSize - unsafe.Sizeof(ringSQE{}))
	_ = uint(unsafe.Sizeof(ringCQE{}) - cqeSize)
	_ = uint(cqeSize - unsafe.Sizeof(ringCQE{}))
	_ = uint(unsafe.Sizeof(ringParams{}) - 120)
	_ = uint(120 - unsafe.Sizeof(ringParams{}))
)

// uring is the mapped ring: fd, the shared SQ and CQ views, and the
// SQE array. Only the submitter touches the SQ, only the reaper
// advances the CQ head, so the ring itself needs no lock.
type uring struct {
	fd     int
	mem    []byte
	sqeMem []byte

	sqHead, sqTail *uint32
	sqMask         uint32
	sqEntries      uint32
	sqArray        []uint32
	sqes           []ringSQE
	tail           uint32 // submitter-local shadow of *sqTail

	cqHead, cqTail *uint32
	cqMask         uint32
	cqEntries      uint32
	cqes           []ringCQE
}

func word(mem []byte, off uint32) *uint32 {
	return (*uint32)(unsafe.Pointer(&mem[off]))
}

// ringSetup creates and maps a ring. Setup failures that mean "this
// machine cannot ring" wrap ErrRingUnsupported so the caller can
// fall back; anything else is a real error.
func ringSetup(depth uint32) (*uring, error) {
	var p ringParams
	fd, _, errno := syscall.Syscall(sysRingSetup, uintptr(depth), uintptr(unsafe.Pointer(&p)), 0)
	if errno != 0 {
		if errno == syscall.ENOSYS || errno == syscall.EPERM || errno == syscall.EINVAL {
			return nil, fmt.Errorf("%w: setup: %v", ErrRingUnsupported, errno)
		}
		return nil, fmt.Errorf("sqlo1b: io_uring setup: %v", errno)
	}
	u := &uring{fd: int(fd)}
	if p.features&ringFeatSingleMmap == 0 {
		u.close()
		return nil, fmt.Errorf("%w: kernel lacks single-mmap rings (pre 5.4)", ErrRingUnsupported)
	}
	size := max(p.sqOff.array+p.sqEntries*4, p.cqOff.cqes+p.cqEntries*cqeSize)
	mem, err := syscall.Mmap(u.fd, ringOffSQRing, int(size),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		u.close()
		return nil, fmt.Errorf("sqlo1b: io_uring ring mmap: %v", err)
	}
	u.mem = mem
	sqeMem, err := syscall.Mmap(u.fd, ringOffSQEs, int(p.sqEntries)*sqeSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		u.close()
		return nil, fmt.Errorf("sqlo1b: io_uring sqe mmap: %v", err)
	}
	u.sqeMem = sqeMem

	u.sqHead = word(mem, p.sqOff.head)
	u.sqTail = word(mem, p.sqOff.tail)
	u.sqMask = *word(mem, p.sqOff.ringMask)
	u.sqEntries = p.sqEntries
	u.sqArray = unsafe.Slice(word(mem, p.sqOff.array), p.sqEntries)
	u.sqes = unsafe.Slice((*ringSQE)(unsafe.Pointer(&sqeMem[0])), p.sqEntries)
	u.tail = *u.sqTail

	u.cqHead = word(mem, p.cqOff.head)
	u.cqTail = word(mem, p.cqOff.tail)
	u.cqMask = *word(mem, p.cqOff.ringMask)
	u.cqEntries = p.cqEntries
	u.cqes = unsafe.Slice((*ringCQE)(unsafe.Pointer(&mem[p.cqOff.cqes])), p.cqEntries)
	return u, nil
}

func (u *uring) close() {
	if u.sqeMem != nil {
		syscall.Munmap(u.sqeMem)
		u.sqeMem = nil
	}
	if u.mem != nil {
		syscall.Munmap(u.mem)
		u.mem = nil
	}
	if u.fd >= 0 {
		syscall.Close(u.fd)
		u.fd = -1
	}
}

// push places one SQE at the submitter-local tail; enter publishes.
// The caller keeps pushes per enter at or below sqEntries.
func (u *uring) push(s ringSQE) {
	idx := u.tail & u.sqMask
	u.sqes[idx] = s
	u.sqArray[idx] = idx
	u.tail++
}

// publish makes pushed SQEs visible to the kernel. Submitter-only,
// like push and tail: the reaper waits through enter without ever
// touching the SQ side.
func (u *uring) publish() {
	atomic.StoreUint32(u.sqTail, u.tail)
}

// enter submits published SQEs and optionally waits for completions,
// reporting how many SQEs the kernel consumed. EINTR retries, and so
// do EAGAIN and EBUSY: both mean kernel-side pressure that the
// concurrently running reaper relieves, and the reservation bound
// keeps them rare. The kernel consumes from its own head, so a retry
// never resubmits what an interrupted call already took.
func (u *uring) enter(toSubmit, minComplete, flags uint32) (int, error) {
	consumed := 0
	for {
		n, _, errno := syscall.Syscall6(sysRingEnter, uintptr(u.fd),
			uintptr(toSubmit)-uintptr(consumed), uintptr(minComplete), uintptr(flags), 0, 0)
		consumed += int(n)
		switch errno {
		case 0:
			return consumed, nil
		case syscall.EINTR:
			continue
		case syscall.EAGAIN, syscall.EBUSY:
			runtime.Gosched()
			continue
		default:
			return consumed, fmt.Errorf("sqlo1b: io_uring enter: %v", errno)
		}
	}
}

// RingProbe reports whether this machine can run the ring backend:
// ring setup succeeds, the register-probe op exists (kernel 5.6, the
// same floor as OP_READ), the nop, read, and write ops are supported,
// and a nop makes the submit-complete round trip. Startup calls this
// once and falls back to iopool on any ErrRingUnsupported.
func RingProbe() error {
	u, err := ringSetup(4)
	if err != nil {
		return err
	}
	defer u.close()
	var probe [16 + 256*8]byte
	if _, _, errno := syscall.Syscall6(sysRingRegister, uintptr(u.fd),
		ringRegisterProbe, uintptr(unsafe.Pointer(&probe[0])), 256, 0, 0); errno != 0 {
		return fmt.Errorf("%w: register probe: %v", ErrRingUnsupported, errno)
	}
	for _, op := range []uint8{ringOpNop, ringOpRead, ringOpWrite} {
		flags := binary.LittleEndian.Uint16(probe[16+int(op)*8+2:])
		if flags&ringOpSupported == 0 {
			return fmt.Errorf("%w: op %d not supported by this kernel", ErrRingUnsupported, op)
		}
	}
	u.push(ringSQE{opcode: ringOpNop, userData: 1})
	u.publish()
	if _, err := u.enter(1, 1, ringEnterGetEvents); err != nil {
		return fmt.Errorf("%w: nop: %v", ErrRingUnsupported, err)
	}
	head := atomic.LoadUint32(u.cqHead)
	if atomic.LoadUint32(u.cqTail) == head {
		return fmt.Errorf("%w: nop submitted but never completed", ErrRingUnsupported)
	}
	c := u.cqes[head&u.cqMask]
	atomic.StoreUint32(u.cqHead, head+1)
	if c.userData != 1 || c.res != 0 {
		return fmt.Errorf("%w: nop completed user_data %d res %d", ErrRingUnsupported, c.userData, c.res)
	}
	return nil
}

// ringCloseData is the reaper's shutdown sentinel. The submitter
// sends it as a nop only after in-flight work reaches zero, because
// the CQ promises no order and the sentinel must be the last word.
const ringCloseData = ^uint64(0)

// ringSlot tracks one in-flight request: the owner's tag, the
// expected transfer size, and the buffer, referenced here so it stays
// reachable for as long as the kernel owns it.
type ringSlot struct {
	tag  uint64
	want uint32
	buf  []byte
}

// IORing is the Linux backend: Submit batches SQEs behind one enter
// call, which is the point (the amortization iopool approximates by
// coalescing adjacent requests, the ring gets across arbitrary ones).
// Completions post to the same mailbox contract as IOPool.
type IORing struct {
	u          *uring
	f          *os.File
	extentSize uint32
	comp       chan<- IOResult

	// The registered pool: one page-aligned anonymous mapping cut
	// into GroupSize buffers the kernel holds pinned, so fixed-op
	// requests skip the per-IO pin and the buffers satisfy O_DIRECT
	// alignment for free. regIdx maps a buffer's base address to its
	// registration index; built at setup, read-only after, so the
	// submitter needs no lock for the lookup.
	regMem   []byte
	regIdx   map[uintptr]uint16
	fixedOps atomic.Uint64

	// Submission telemetry: enters counts submit-side enter syscalls
	// (the reaper's waits are not in it), entered counts the SQEs they
	// carried, so entered/enters is the realized batch size the INFO
	// surface reports in slice 4.
	enters  atomic.Uint64
	entered atomic.Uint64

	sub   chan []IOReq
	syncq chan uint64
	wg    sync.WaitGroup

	mu       sync.Mutex
	cond     *sync.Cond
	inflight int
	slots    []ringSlot
	free     []uint32
}

var _ Backend = (*IORing)(nil)

// NewIORing sets up a ring of the given depth over f's descriptor and
// starts the submitter, reaper, and sync goroutines. regBufs is the
// registered-buffer pool size in GroupSize buffers, 0 for none; the
// final constant comes from the ringpool lab on the gate box.
// Completions post to comp; the caller sizes it to its in-flight
// window and closes the backend before dropping comp. On
// ErrRingUnsupported the caller falls back to NewIOPool.
func NewIORing(f *os.File, extentSize uint32, depth, regBufs int, comp chan<- IOResult) (*IORing, error) {
	if depth < 1 {
		return nil, fmt.Errorf("sqlo1b: ring depth %d", depth)
	}
	u, err := ringSetup(uint32(depth))
	if err != nil {
		return nil, err
	}
	r := &IORing{
		u:          u,
		f:          f,
		extentSize: extentSize,
		comp:       comp,
		sub:        make(chan []IOReq, 64),
		syncq:      make(chan uint64, 16),
		slots:      make([]ringSlot, u.cqEntries),
		free:       make([]uint32, 0, u.cqEntries),
	}
	if regBufs > 0 {
		if err := r.register(regBufs); err != nil {
			u.close()
			return nil, err
		}
	}
	for i := range uint32(u.cqEntries) {
		r.free = append(r.free, i)
	}
	r.cond = sync.NewCond(&r.mu)
	r.wg.Go(r.submitter)
	r.wg.Go(r.reaper)
	r.wg.Go(r.syncer)
	return r, nil
}

// register maps and pins the buffer pool. Failure wraps
// ErrRingUnsupported: registration charges RLIMIT_MEMLOCK, and a box
// that refuses it (ENOMEM) or forbids it (EPERM) runs the fallback
// exactly like a box with no ring at all. The fixed read and write
// ops predate this backend's kernel floor, so a successful
// registration means they work.
func (r *IORing) register(regBufs int) error {
	mem, err := syscall.Mmap(-1, 0, regBufs*GroupSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return fmt.Errorf("sqlo1b: registered pool mmap: %v", err)
	}
	iov := make([]syscall.Iovec, regBufs)
	for i := range iov {
		iov[i].Base = &mem[i*GroupSize]
		iov[i].SetLen(GroupSize)
	}
	if _, _, errno := syscall.Syscall6(sysRingRegister, uintptr(r.u.fd),
		ringRegisterBuffers, uintptr(unsafe.Pointer(&iov[0])), uintptr(regBufs), 0, 0); errno != 0 {
		syscall.Munmap(mem)
		return fmt.Errorf("%w: register %d buffers: %v", ErrRingUnsupported, regBufs, errno)
	}
	r.regMem = mem
	r.regIdx = make(map[uintptr]uint16, regBufs)
	for i := range regBufs {
		r.regIdx[uintptr(unsafe.Pointer(&mem[i*GroupSize]))] = uint16(i)
	}
	return nil
}

// RegBufs is the registered pool size in buffers.
func (r *IORing) RegBufs() int { return len(r.regIdx) }

// RegBuf is the i-th registered buffer, GroupSize bytes and
// GroupSize-aligned (the pool is page-backed), so it satisfies
// O_DIRECT's alignment demands as-is. A request whose Buf starts at a
// pool buffer's base rides the fixed opcodes; any other buffer,
// including a sub-slice past the base, takes the plain path.
func (r *IORing) RegBuf(i int) []byte {
	return r.regMem[i*GroupSize : (i+1)*GroupSize : (i+1)*GroupSize]
}

// OpenDirect opens the data file with O_DIRECT for the ring backend's
// own-caching mode. Every IO against it must keep offset, length, and
// buffer address block-aligned; registered pool buffers and
// group-aligned requests satisfy that by construction. Callers treat
// failure like ErrRingUnsupported: not every filesystem honors
// O_DIRECT (tmpfs refuses it), and the buffered path is the fallback.
func OpenDirect(name string) (*os.File, error) {
	f, err := os.OpenFile(name, os.O_RDWR|syscall.O_DIRECT, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: O_DIRECT open: %v", ErrRingUnsupported, err)
	}
	return f, nil
}

// Submit enqueues a batch; the submitter goroutine turns it into
// SQEs. Same contract as IOPool.Submit: order between requests is
// not promised, the owner sequences through tags and Sync, and
// Submit is not safe against a concurrent Close.
func (r *IORing) Submit(reqs []IOReq) { r.sub <- reqs }

// Sync hands the fsync to the dedicated sync goroutine, never to the
// ring. Same covering rule as iopool: the sync covers the writes
// whose completions the owner already saw.
func (r *IORing) Sync(tag uint64) { r.syncq <- tag }

// Close drains queued work, waits for the ring to empty, and tears
// the ring down. The owner must not Submit or Sync after calling it.
func (r *IORing) Close() {
	close(r.sub)
	close(r.syncq)
	r.wg.Wait()
	r.u.close() // closing the ring fd also unregisters the pool
	if r.regMem != nil {
		syscall.Munmap(r.regMem)
		r.regMem = nil
	}
}

func (r *IORing) abs(q *IOReq) int64 {
	return int64(q.Ext)*int64(r.extentSize) + int64(q.Off)
}

// ringPend is the submitter's accumulation window: SQEs pushed but
// not yet entered, with the slot ids and owner tags it needs to fail
// the refused suffix if the kernel rejects part of a flush.
type ringPend struct {
	ids  []uint32
	tags []uint64
}

// submitter owns the SQ. It validates like iopool (a bad request
// fails its whole batch without touching the file), reserves in-flight
// room against the CQ bound so the ring can never overflow, and
// accumulates SQEs across Submit batches, entering per ringFlushNow:
// on the batch target (adaptive down under CQ pressure), on a full
// SQ, or on the drain-window tick when the queue goes empty. Reaping
// runs on its own goroutine, so a held batch never delays the
// completions of what was already entered.
func (r *IORing) submitter() {
	fd := int32(r.f.Fd())
	pend := ringPend{
		ids:  make([]uint32, 0, r.u.sqEntries),
		tags: make([]uint64, 0, r.u.sqEntries),
	}
	for {
		var reqs []IOReq
		var ok bool
		if len(pend.ids) > 0 {
			select {
			case reqs, ok = <-r.sub:
			default:
				// Drain-window tick: nothing queued behind us, so
				// holding the batch would only add latency.
				r.flush(&pend)
				reqs, ok = <-r.sub
			}
		} else {
			reqs, ok = <-r.sub
		}
		if !ok {
			break
		}
		if err := validateIOReqs(r.extentSize, reqs); err != nil {
			for i := range reqs {
				r.comp <- IOResult{Tag: reqs[i].Tag, Err: err}
			}
			continue
		}
		for start := 0; start < len(reqs); {
			room := int(r.u.sqEntries) - len(pend.ids)
			if room == 0 {
				r.flush(&pend)
				room = int(r.u.sqEntries)
			}
			want := min(len(reqs)-start, room)
			n, inflight := r.tryReserve(want)
			if n == 0 {
				// The CQ bound is met by in-flight work; anything we
				// hold must enter before blocking, or its completions
				// could never free the room we are waiting for.
				r.flush(&pend)
				n, inflight = r.reserve(want)
			}
			chunk := reqs[start : start+n]
			for i := range chunk {
				q := &chunk[i]
				addr := uintptr(unsafe.Pointer(&q.Buf[0]))
				bufIdx, fixed := r.regIdx[addr]
				fixed = fixed && len(q.Buf) <= GroupSize
				var op uint8
				switch {
				case q.Op == OpRead && fixed:
					op = ringOpReadFixed
				case q.Op == OpRead:
					op = ringOpRead
				case fixed:
					op = ringOpWriteFixed
				default:
					op = ringOpWrite
				}
				if fixed {
					r.fixedOps.Add(1)
				} else {
					bufIdx = 0
				}
				id := r.slot(q)
				pend.ids = append(pend.ids, id)
				pend.tags = append(pend.tags, q.Tag)
				r.u.push(ringSQE{
					opcode:   op,
					fd:       fd,
					off:      uint64(r.abs(q)),
					addr:     uint64(addr),
					len:      uint32(len(q.Buf)),
					userData: uint64(id),
					bufIndex: bufIdx,
				})
			}
			if ringFlushNow(len(pend.ids), int(r.u.sqEntries), inflight, int(r.u.cqEntries), false) {
				r.flush(&pend)
			}
			start += n
		}
	}
	r.flush(&pend)
	// Shutdown: the sentinel nop must be the only thing left in
	// flight, because completions promise no order.
	r.mu.Lock()
	for r.inflight > 0 {
		r.cond.Wait()
	}
	r.mu.Unlock()
	for {
		r.u.push(ringSQE{opcode: ringOpNop, userData: ringCloseData})
		r.u.publish()
		if _, err := r.u.enter(1, 0, 0); err == nil {
			return
		}
		r.u.tail--
		r.u.publish()
		runtime.Gosched()
	}
}

// flush publishes and enters everything pending. On a partial refusal
// the kernel took the first consumed SQEs (they will complete through
// the CQ); the tail rewinds over the rest, which are failed to their
// owners and their reservations released.
func (r *IORing) flush(pend *ringPend) {
	n := len(pend.ids)
	if n == 0 {
		return
	}
	r.u.publish()
	r.enters.Add(1)
	r.entered.Add(uint64(n))
	consumed, err := r.u.enter(uint32(n), 0, 0)
	if err != nil {
		r.u.tail -= uint32(n - consumed)
		r.u.publish()
		r.mu.Lock()
		for _, id := range pend.ids[consumed:] {
			r.slots[id] = ringSlot{}
			r.free = append(r.free, id)
		}
		r.inflight -= n - consumed
		r.cond.Signal()
		r.mu.Unlock()
		for _, tag := range pend.tags[consumed:] {
			r.comp <- IOResult{Tag: tag, Err: err}
		}
	}
	pend.ids = pend.ids[:0]
	pend.tags = pend.tags[:0]
}

// EnterStats reports submit-side enter syscalls and the SQEs they
// carried; their ratio is the realized batch size.
func (r *IORing) EnterStats() (enters, entered uint64) {
	return r.enters.Load(), r.entered.Load()
}

// tryReserve grants what fits under the CQ bound right now, possibly
// nothing, and reports in-flight after the grant for the batching
// decision. The submitter must not block while it holds unsubmitted
// SQEs, so the blocking form stays separate.
func (r *IORing) tryReserve(n int) (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n = max(min(n, int(r.u.cqEntries)-r.inflight), 0)
	r.inflight += n
	return n, r.inflight
}

// reserve blocks until at least one request fits under the CQ bound,
// so a completion can never be dropped to overflow; it grants what
// fits and reports in-flight after the grant.
func (r *IORing) reserve(n int) (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.inflight >= int(r.u.cqEntries) {
		r.cond.Wait()
	}
	n = min(n, int(r.u.cqEntries)-r.inflight)
	r.inflight += n
	return n, r.inflight
}

// slot files one reserved request into the in-flight table; reserve
// already guaranteed a free entry.
func (r *IORing) slot(q *IOReq) uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.free[len(r.free)-1]
	r.free = r.free[:len(r.free)-1]
	r.slots[id] = ringSlot{tag: q.Tag, want: uint32(len(q.Buf)), buf: q.Buf}
	return id
}

// reaper owns the CQ head: it waits in enter GETEVENTS when the ring
// is quiet, drains ready completions otherwise, and exits on the
// shutdown sentinel.
func (r *IORing) reaper() {
	u := r.u
	for {
		head := atomic.LoadUint32(u.cqHead)
		if atomic.LoadUint32(u.cqTail) == head {
			if _, err := u.enter(0, 1, ringEnterGetEvents); err != nil {
				// The ring is wedged; without completions the
				// backend cannot honor its contract. This is a
				// kernel-level failure with no honest recovery.
				panic(fmt.Sprintf("sqlo1b: io_uring wait: %v", err))
			}
			continue
		}
		c := u.cqes[head&u.cqMask]
		atomic.StoreUint32(u.cqHead, head+1)
		if c.userData == ringCloseData {
			return
		}
		r.mu.Lock()
		s := r.slots[c.userData]
		r.slots[c.userData] = ringSlot{}
		r.free = append(r.free, uint32(c.userData))
		r.inflight--
		r.cond.Signal()
		r.mu.Unlock()
		var err error
		switch {
		case c.res < 0:
			err = fmt.Errorf("sqlo1b: ring io: %v", syscall.Errno(-c.res))
		case uint32(c.res) != s.want:
			err = fmt.Errorf("sqlo1b: ring io moved %d of %d bytes", c.res, s.want)
		}
		r.comp <- IOResult{Tag: s.tag, Err: err}
	}
}

// syncer is the same dedicated fsync goroutine as iopool's: barriers
// stay off the ring by construction.
func (r *IORing) syncer() {
	for tag := range r.syncq {
		r.comp <- IOResult{Tag: tag, Err: r.f.Sync()}
	}
}
