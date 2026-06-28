//go:build linux

package uring

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"syscall"
)

// io_uring syscall numbers. They are 425/426/427 on every Linux architecture aki
// targets (x86_64 and arm64 share the generic numbering for these), so one set
// covers the build.
const (
	sysSetup = 425
	sysEnter = 426
)

// Opcodes for the operations aki submits. The numeric values are the kernel's
// IORING_OP_* constants and are stable ABI. They live here, not in the shared
// file, because only the Linux build uses them.
const (
	opNop  uint8 = 0
	opSend uint8 = 26
	opRecv uint8 = 27
)

// mmap offsets the kernel defines for the io_uring regions. There is no offCQRing
// here because we require IORING_FEAT_SINGLE_MMAP, where the CQ ring shares the SQ
// ring's mapping at offSQRing; the separate 0x8000000 offset only matters on the
// pre-5.4 kernels New rejects.
const (
	offSQRing uintptr = 0
	offSQEs   uintptr = 0x10000000
)

// io_uring_enter flags.
const (
	enterGetEvents uint32 = 1 << 0
)

// feature flag: when set the SQ and CQ rings live in one mapping, which every
// kernel since 5.4 reports. We rely on it and reject a kernel that does not.
const featSingleMMAP uint32 = 1 << 0

// ioSQRingOffsets mirrors struct io_sqring_offsets: the byte offsets, inside the
// mmap'd SQ ring, of each field the kernel and userspace share.
type ioSQRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	resv2       uint64
}

// ioCQRingOffsets mirrors struct io_cqring_offsets.
type ioCQRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	resv2       uint64
}

// ioUringParams mirrors struct io_uring_params, passed to io_uring_setup. The
// kernel fills sqEntries, cqEntries, features and both offset blocks on return.
type ioUringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFD         uint32
	resv         [3]uint32
	sqOff        ioSQRingOffsets
	cqOff        ioCQRingOffsets
}

// sqe mirrors struct io_uring_sqe, 64 bytes. Only the fields aki sets are named;
// the rest are padding kept so the struct size and field offsets match the ABI.
type sqe struct {
	opcode      uint8
	flags       uint8
	ioprio      uint16
	fd          int32
	off         uint64 // offset 8 (off / addr2)
	addr        uint64 // offset 16
	len         uint32 // offset 24
	rwFlags     uint32 // offset 28
	userData    uint64 // offset 32
	bufIndex    uint16 // offset 40
	personality uint16
	spliceFD    int32  // offset 44
	addr3       uint64 // offset 48
	pad2        uint64 // offset 56
}

// cqe mirrors struct io_uring_cqe, 16 bytes.
type cqe struct {
	userData uint64
	res      int32
	flags    uint32
}

// Ring is a single io_uring instance: its ring fd, the three mmap'd regions, and
// cached pointers into the shared head and tail words. One Ring is owned by one
// goroutine (a reactor loop), so it carries no lock of its own; the only
// synchronization is the atomic load/store the ABI requires on the head and tail
// words shared with the kernel.
type Ring struct {
	fd int

	sqRing []byte
	cqRing []byte
	sqesMM []byte

	// SQ shared words and the SQE array, as pointers into sqRing / sqesMM.
	sqHead    *uint32
	sqTail    *uint32
	sqMask    uint32
	sqEntries uint32
	sqArray   []uint32 // index ring -> sqe index
	sqes      []sqe

	// local tail: SQEs we have prepared but not yet published to the kernel.
	sqLocalTail uint32

	// CQ shared words and the CQE array.
	cqHead *uint32
	cqTail *uint32
	cqMask uint32
	cqes   []cqe
}

// New sets up an io_uring with room for entries submission slots and returns a
// ready Ring. entries is rounded up to a power of two by the kernel. It returns
// ErrUnsupported wrapped with detail if the kernel refuses setup or reports a
// feature set this binding does not implement.
func New(entries uint32) (*Ring, error) {
	if entries == 0 {
		entries = 256
	}
	var p ioUringParams
	fd, _, errno := syscall.Syscall(sysSetup, uintptr(entries), uintptr(unsafe.Pointer(&p)), 0)
	if errno != 0 {
		return nil, fmt.Errorf("%w: io_uring_setup: %v", ErrUnsupported, errno)
	}
	if p.features&featSingleMMAP == 0 {
		_ = syscall.Close(int(fd))
		return nil, fmt.Errorf("%w: kernel lacks IORING_FEAT_SINGLE_MMAP", ErrUnsupported)
	}
	r := &Ring{fd: int(fd), sqEntries: p.sqEntries}

	// With single-mmap the SQ and CQ rings share one mapping; size it to the larger
	// of the two computed extents.
	sqRingBytes := p.sqOff.array + p.sqEntries*4
	cqRingBytes := p.cqOff.cqes + p.cqEntries*uint32(unsafe.Sizeof(cqe{}))
	ringBytes := max(sqRingBytes, cqRingBytes)

	ring, err := mmapRing(int(fd), offSQRing, int(ringBytes))
	if err != nil {
		_ = syscall.Close(int(fd))
		return nil, fmt.Errorf("%w: mmap sq ring: %v", ErrUnsupported, err)
	}
	r.sqRing = ring
	r.cqRing = ring // same mapping

	sqes, err := mmapRing(int(fd), offSQEs, int(p.sqEntries)*int(unsafe.Sizeof(sqe{})))
	if err != nil {
		_ = munmap(ring)
		_ = syscall.Close(int(fd))
		return nil, fmt.Errorf("%w: mmap sqes: %v", ErrUnsupported, err)
	}
	r.sqesMM = sqes

	base := unsafe.Pointer(&ring[0])
	r.sqHead = (*uint32)(unsafe.Add(base, p.sqOff.head))
	r.sqTail = (*uint32)(unsafe.Add(base, p.sqOff.tail))
	r.sqMask = *(*uint32)(unsafe.Add(base, p.sqOff.ringMask))
	r.sqArray = unsafe.Slice((*uint32)(unsafe.Add(base, p.sqOff.array)), p.sqEntries)
	r.sqes = unsafe.Slice((*sqe)(unsafe.Pointer(&sqes[0])), p.sqEntries)

	r.cqHead = (*uint32)(unsafe.Add(base, p.cqOff.head))
	r.cqTail = (*uint32)(unsafe.Add(base, p.cqOff.tail))
	r.cqMask = *(*uint32)(unsafe.Add(base, p.cqOff.ringMask))
	r.cqes = unsafe.Slice((*cqe)(unsafe.Add(base, p.cqOff.cqes)), p.cqEntries)

	r.sqLocalTail = atomic.LoadUint32(r.sqTail)
	return r, nil
}

// Close unmaps the rings and closes the ring fd. The Ring must not be used after.
func (r *Ring) Close() error {
	if r.sqesMM != nil {
		_ = munmap(r.sqesMM)
		r.sqesMM = nil
	}
	if r.sqRing != nil {
		_ = munmap(r.sqRing)
		r.sqRing = nil
		r.cqRing = nil
	}
	if r.fd >= 0 {
		err := syscall.Close(r.fd)
		r.fd = -1
		return err
	}
	return nil
}

// nextSQE claims the next free submission entry against the kernel's consumed
// head. It returns nil when the ring is full, so the caller submits what it has
// and retries. The returned sqe is zeroed before use so stale fields from a prior
// submission never leak into this one.
func (r *Ring) nextSQE() *sqe {
	next := r.sqLocalTail + 1
	head := atomic.LoadUint32(r.sqHead)
	if next-head > r.sqEntries {
		return nil
	}
	idx := r.sqLocalTail & r.sqMask
	e := &r.sqes[idx]
	*e = sqe{}
	// The array slot must point at this sqe index; with a one-to-one ring we keep
	// array[idx] == idx, set once here each time the slot is used.
	r.sqArray[idx] = idx
	r.sqLocalTail = next
	return e
}

// PrepNop queues a no-op completion carrying userData. It exists to exercise and
// test the submit/reap path without touching a file descriptor.
func (r *Ring) PrepNop(userData uint64) bool {
	e := r.nextSQE()
	if e == nil {
		return false
	}
	e.opcode = opNop
	e.userData = userData
	return true
}

// PrepRecv queues a recv of up to len(buf) bytes from fd into buf, carrying
// userData. The buffer must stay alive and unmodified until the matching
// completion is reaped. Returns false if the submission ring is full.
func (r *Ring) PrepRecv(userData uint64, fd int, buf []byte) bool {
	e := r.nextSQE()
	if e == nil {
		return false
	}
	e.opcode = opRecv
	e.fd = int32(fd)
	if len(buf) > 0 {
		e.addr = uint64(uintptr(unsafe.Pointer(&buf[0])))
	}
	e.len = uint32(len(buf))
	e.userData = userData
	return true
}

// PrepSend queues a send of buf to fd, carrying userData. The buffer must stay
// alive and unmodified until the matching completion is reaped. Returns false if
// the submission ring is full.
func (r *Ring) PrepSend(userData uint64, fd int, buf []byte) bool {
	e := r.nextSQE()
	if e == nil {
		return false
	}
	e.opcode = opSend
	e.fd = int32(fd)
	if len(buf) > 0 {
		e.addr = uint64(uintptr(unsafe.Pointer(&buf[0])))
	}
	e.len = uint32(len(buf))
	e.userData = userData
	return true
}

// publish makes every prepared SQE visible to the kernel by advancing the shared
// tail with a release store, then returns how many are newly pending.
func (r *Ring) publish() uint32 {
	old := atomic.LoadUint32(r.sqTail)
	atomic.StoreUint32(r.sqTail, r.sqLocalTail)
	return r.sqLocalTail - old
}

// Submit publishes prepared SQEs and tells the kernel to process them without
// waiting for any completion. It returns the number the kernel consumed.
func (r *Ring) Submit() (int, error) {
	return r.enter(0)
}

// SubmitAndWait publishes prepared SQEs and blocks in io_uring_enter until at
// least minComplete completions are available. It returns the number the kernel
// consumed from the submission queue.
func (r *Ring) SubmitAndWait(minComplete uint32) (int, error) {
	return r.enter(minComplete)
}

func (r *Ring) enter(minComplete uint32) (int, error) {
	toSubmit := r.publish()
	var flags uintptr
	if minComplete > 0 {
		flags = uintptr(enterGetEvents)
	}
	n, _, errno := syscall.Syscall6(sysEnter, uintptr(r.fd), uintptr(toSubmit),
		uintptr(minComplete), flags, 0, 0)
	if errno != 0 {
		return int(n), fmt.Errorf("io_uring_enter: %v", errno)
	}
	return int(n), nil
}

// Reap copies up to len(into) ready completions into into and advances the CQ
// head past them, returning how many it copied. It does not block; pair it with
// SubmitAndWait when completions are required.
func (r *Ring) Reap(into []Completion) int {
	head := atomic.LoadUint32(r.cqHead)
	tail := atomic.LoadUint32(r.cqTail) // acquire: kernel published these
	n := 0
	for head != tail && n < len(into) {
		c := &r.cqes[head&r.cqMask]
		into[n] = Completion{UserData: c.userData, Res: c.res, Flags: c.flags}
		n++
		head++
	}
	atomic.StoreUint32(r.cqHead, head) // release: kernel may reuse these slots
	return n
}

// mmapRing maps one io_uring region. MAP_POPULATE faults the pages in up front so
// the first submission does not take a minor fault on the hot path. The offset is
// one of the kernel's magic IORING_OFF_* values, which select the region rather
// than a file position.
func mmapRing(fd int, offset uintptr, length int) ([]byte, error) {
	return syscall.Mmap(fd, int64(offset), length,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED|syscall.MAP_POPULATE)
}

func munmap(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return syscall.Munmap(b)
}
