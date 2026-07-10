//go:build linux

package drivers

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// The io_uring ABI, in-package over the raw stdlib syscall numbers, per the
// no-dependency rule: setup, mmap of the SQ/CQ rings, SQE staging, one
// submit-and-wait enter per loop pass, and the register PROBE the startup
// availability check reads. Everything here is the ring plumbing; the driver
// logic lives in uring_linux.go. The ring is single-submitter by design: only
// the owning loop goroutine touches the SQ side, so the only atomics are the
// ones the kernel contract requires (release on the SQ tail, acquire on the
// CQ tail, and the kernel-written SQ head and flags words).

// The io_uring syscall numbers, identical on amd64 and arm64 (every
// architecture past the y2038 unification shares them).
const (
	sysIoUringSetup    = 425
	sysIoUringEnter    = 426
	sysIoUringRegister = 427
)

// Ring mmap offsets.
const (
	uringOffSQRing = 0x0
	uringOffCQRing = 0x8000000
	uringOffSQEs   = 0x10000000
)

// Setup flags and features.
const (
	uringSetupCQSize = 1 << 3 // IORING_SETUP_CQSIZE: honor params.cqEntries

	uringFeatSingleMmap = 1 << 0 // IORING_FEAT_SINGLE_MMAP
	uringFeatNoDrop     = 1 << 1 // IORING_FEAT_NODROP
)

// Enter flags.
const uringEnterGetEvents = 1 << 0 // IORING_ENTER_GETEVENTS

// Opcodes the driver uses.
const (
	uringOpNop         = 0
	uringOpAsyncCancel = 14
	uringOpRead        = 22
	uringOpSend        = 26
	uringOpRecv        = 27
)

// Register opcodes.
const uringRegisterProbe = 8 // IORING_REGISTER_PROBE

// uringProbeOpSupported is the flags bit in one probe op entry.
const uringProbeOpSupported = 1 << 0 // IO_URING_OP_SUPPORTED

// msgNoSignal keeps a send to a dead peer from raising SIGPIPE; the raw send
// path answers with EPIPE on the CQE instead, which the driver maps to a
// close like any other socket death.
const msgNoSignal = 0x4000 // MSG_NOSIGNAL

// io_sqring_offsets, kernel ABI layout.
type uringSQOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	userAddr    uint64
}

// io_cqring_offsets, kernel ABI layout.
type uringCQOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	userAddr    uint64
}

// io_uring_params, kernel ABI layout.
type uringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFd         uint32
	resv         [3]uint32
	sqOff        uringSQOffsets
	cqOff        uringCQOffsets
}

// io_uring_sqe, kernel ABI layout, 64 bytes. The unions are flattened to the
// fields this driver uses: off is offset/addr2, opFlags is msg_flags and
// friends, spliceFdIn covers file_index too.
type uringSQE struct {
	opcode      uint8
	flags       uint8
	ioprio      uint16
	fd          int32
	off         uint64
	addr        uint64
	len         uint32
	opFlags     uint32
	userData    uint64
	bufIndex    uint16
	personality uint16
	spliceFdIn  int32
	addr3       uint64
	pad2        uint64
}

// io_uring_cqe, kernel ABI layout, 16 bytes.
type uringCQE struct {
	userData uint64
	res      int32
	flags    uint32
}

// ioUringSetup wraps io_uring_setup(2).
func ioUringSetup(entries uint32, p *uringParams) (int, error) {
	fd, _, errno := syscall.Syscall(sysIoUringSetup, uintptr(entries), uintptr(unsafe.Pointer(p)), 0)
	if errno != 0 {
		return -1, errno
	}
	return int(fd), nil
}

// ioUringEnter wraps io_uring_enter(2).
func ioUringEnter(fd int, toSubmit, minComplete uint32, flags uint32) (int, error) {
	n, _, errno := syscall.Syscall6(sysIoUringEnter, uintptr(fd), uintptr(toSubmit), uintptr(minComplete), uintptr(flags), 0, 0)
	if errno != 0 {
		return int(n), errno
	}
	return int(n), nil
}

// ioUringRegister wraps io_uring_register(2).
func ioUringRegister(fd int, opcode uint32, arg unsafe.Pointer, nrArgs uint32) error {
	_, _, errno := syscall.Syscall6(sysIoUringRegister, uintptr(fd), uintptr(opcode), uintptr(arg), uintptr(nrArgs), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// uring is one mmap'd ring: the fd, the mapped SQ/CQ regions, and the raw
// pointers into them. Single-submitter: only the owning loop goroutine calls
// getSQE, flush, and submitAndWait; reap is loop-goroutine-only too.
type uring struct {
	fd     int
	params uringParams

	sqRing []byte
	cqRing []byte // aliases sqRing under FEAT_SINGLE_MMAP
	sqeMem []byte

	sqHead    *uint32 // kernel-written consumer index
	sqTail    *uint32 // ours, release-stored
	sqMask    uint32
	sqFlags   *uint32
	sqDropped *uint32
	sqArray   []uint32
	sqes      []uringSQE

	cqHead    *uint32 // ours
	cqTail    *uint32 // kernel-written, acquire-loaded
	cqMask    uint32
	cqCQEs    []uringCQE
	cqOverflw *uint32

	// sqeTail is the local staged tail: SQEs prepped since the last publish.
	// staged counts them for the next enter's to_submit.
	sqeTail uint32
	staged  uint32
}

// newURing sets up a ring with sqEntries submission slots and a CQ sized
// cqEntries (rounded up by the kernel), and mmaps everything.
func newURing(sqEntries, cqEntries uint32) (*uring, error) {
	r := &uring{}
	r.params.flags = uringSetupCQSize
	r.params.cqEntries = cqEntries
	fd, err := ioUringSetup(sqEntries, &r.params)
	if err != nil {
		return nil, err
	}
	r.fd = fd
	if err := r.mmap(); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	return r, nil
}

func (r *uring) mmap() error {
	p := &r.params
	sqSize := int(p.sqOff.array) + int(p.sqEntries)*4
	cqSize := int(p.cqOff.cqes) + int(p.cqEntries)*int(unsafe.Sizeof(uringCQE{}))
	if p.features&uringFeatSingleMmap != 0 && cqSize > sqSize {
		sqSize = cqSize
	}
	sq, err := syscall.Mmap(r.fd, uringOffSQRing, sqSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		return err
	}
	r.sqRing = sq
	if p.features&uringFeatSingleMmap != 0 {
		r.cqRing = sq
	} else {
		cq, err := syscall.Mmap(r.fd, uringOffCQRing, cqSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
		if err != nil {
			_ = syscall.Munmap(sq)
			r.sqRing = nil
			return err
		}
		r.cqRing = cq
	}
	sqeBytes := int(p.sqEntries) * int(unsafe.Sizeof(uringSQE{}))
	sqem, err := syscall.Mmap(r.fd, uringOffSQEs, sqeBytes, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		r.munmapRings()
		return err
	}
	r.sqeMem = sqem

	at := func(m []byte, off uint32) *uint32 { return (*uint32)(unsafe.Pointer(&m[off])) }
	r.sqHead = at(r.sqRing, p.sqOff.head)
	r.sqTail = at(r.sqRing, p.sqOff.tail)
	r.sqMask = *at(r.sqRing, p.sqOff.ringMask)
	r.sqFlags = at(r.sqRing, p.sqOff.flags)
	r.sqDropped = at(r.sqRing, p.sqOff.dropped)
	r.sqArray = unsafe.Slice((*uint32)(unsafe.Pointer(&r.sqRing[p.sqOff.array])), p.sqEntries)
	r.sqes = unsafe.Slice((*uringSQE)(unsafe.Pointer(&r.sqeMem[0])), p.sqEntries)
	r.cqHead = at(r.cqRing, p.cqOff.head)
	r.cqTail = at(r.cqRing, p.cqOff.tail)
	r.cqMask = *at(r.cqRing, p.cqOff.ringMask)
	r.cqOverflw = at(r.cqRing, p.cqOff.overflow)
	r.cqCQEs = unsafe.Slice((*uringCQE)(unsafe.Pointer(&r.cqRing[p.cqOff.cqes])), p.cqEntries)
	r.sqeTail = *r.sqTail
	return nil
}

func (r *uring) munmapRings() {
	if r.cqRing != nil && &r.cqRing[0] != &r.sqRing[0] {
		_ = syscall.Munmap(r.cqRing)
	}
	if r.sqRing != nil {
		_ = syscall.Munmap(r.sqRing)
	}
	r.sqRing, r.cqRing = nil, nil
}

// close unmaps and closes the ring. Closing the ring fd cancels every pending
// operation and drops their file references, which is what shutdown leans on.
func (r *uring) close() {
	if r.sqeMem != nil {
		_ = syscall.Munmap(r.sqeMem)
		r.sqeMem = nil
	}
	r.munmapRings()
	if r.fd >= 0 {
		_ = syscall.Close(r.fd)
		r.fd = -1
	}
}

// getSQE hands out the next free submission slot, zeroed. When the ring is
// full it flushes the staged entries with a plain submit first; the SQ is
// sized so that only a pathological pass gets here.
func (r *uring) getSQE() *uringSQE {
	for r.sqeTail-atomic.LoadUint32(r.sqHead) >= r.params.sqEntries {
		// Ring full: hand the kernel what is staged and retry.
		_ = r.flush()
	}
	idx := r.sqeTail & r.sqMask
	sqe := &r.sqes[idx]
	*sqe = uringSQE{}
	r.sqArray[idx] = idx
	r.sqeTail++
	r.staged++
	return sqe
}

// publish makes the staged SQEs visible to the kernel and returns the count
// for enter's to_submit.
func (r *uring) publish() uint32 {
	atomic.StoreUint32(r.sqTail, r.sqeTail)
	n := r.staged
	r.staged = 0
	return n
}

// flush submits the staged SQEs without waiting.
func (r *uring) flush() error {
	n := r.publish()
	if n == 0 {
		return nil
	}
	for {
		_, err := ioUringEnter(r.fd, n, 0, 0)
		if err == syscall.EINTR {
			continue
		}
		return err
	}
}

// submitAndWait publishes the staged SQEs and waits for at least one CQE, the
// one io_uring_enter per loop pass. A CQ that overflowed into the kernel-side
// backlog (FEAT_NODROP) is flushed by the same GETEVENTS call.
func (r *uring) submitAndWait() error {
	n := r.publish()
	for {
		_, err := ioUringEnter(r.fd, n, 1, uringEnterGetEvents)
		if err == syscall.EINTR {
			// The submit half consumed the entries before the wait was
			// interrupted; do not resubmit them.
			n = 0
			continue
		}
		return err
	}
}

// reap drains every completion currently visible, calling fn for each. It
// returns the number handled. fn may stage new SQEs; it must not reap.
func (r *uring) reap(fn func(uringCQE)) int {
	head := *r.cqHead
	tail := atomic.LoadUint32(r.cqTail)
	n := 0
	for head != tail {
		cqe := r.cqCQEs[head&r.cqMask]
		head++
		n++
		atomic.StoreUint32(r.cqHead, head)
		fn(cqe)
	}
	return n
}

// uringProbe asks the kernel which opcodes this ring supports, the
// io_uring_register(PROBE) form. It returns a bitmap-ish slice indexed by
// opcode.
func uringProbe(fd int) ([]bool, error) {
	const nOps = 64
	// struct io_uring_probe: 16-byte header then nOps 8-byte op entries.
	buf := make([]byte, 16+nOps*8)
	if err := ioUringRegister(fd, uringRegisterProbe, unsafe.Pointer(&buf[0]), nOps); err != nil {
		return nil, err
	}
	sup := make([]bool, nOps)
	for i := 0; i < nOps; i++ {
		flags := uint16(buf[16+i*8+2]) | uint16(buf[16+i*8+3])<<8
		sup[i] = flags&uringProbeOpSupported != 0
	}
	return sup, nil
}

// errURingMissingOps is the availability verdict when the kernel has io_uring
// but not the opcodes this driver is built on (send/recv landed in 5.6).
var errURingMissingOps = errors.New("kernel io_uring lacks RECV/SEND/READ/ASYNC_CANCEL support")

// checkURingOps verifies the opcodes the driver submits.
func checkURingOps(fd int) error {
	sup, err := uringProbe(fd)
	if err != nil {
		return err
	}
	for _, op := range []int{uringOpAsyncCancel, uringOpRead, uringOpSend, uringOpRecv} {
		if op >= len(sup) || !sup[op] {
			return errURingMissingOps
		}
	}
	return nil
}

// uringAvailable probes once per process whether the kernel can run this
// driver: io_uring_setup succeeds (not ENOSYS, not sysctl- or seccomp-denied)
// and the required opcodes probe as supported. The result is cached; the
// answer cannot change under a running process.
var uringAvailable = sync.OnceValue(func() bool {
	var p uringParams
	fd, err := ioUringSetup(2, &p)
	if err != nil {
		return false
	}
	defer func() { _ = syscall.Close(fd) }()
	return checkURingOps(fd) == nil
})
