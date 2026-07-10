//go:build linux

// Lab 21: io_uring vs epoll echo loops (M10 slice 3).
// The question doc 08 section 4.3 makes this lab answer before the full
// server A/B: how much of the reactor's per-op syscall floor (one read and
// one write per round trip, measured at 537ns + 876ns on the gate box) does
// a batched io_uring loop actually delete, in syscalls/op and ns/op, with
// everything else held equal. Both arms are the same single-threaded echo
// loop over dup'd nonblocking fds on loopback; the only difference is the
// event and I/O mechanism. The ring code mirrors f3srv/drivers/
// uringring_linux.go, inlined because labs are self-contained binaries.
//
// One invocation runs one cell: -mode epoll|uring, -conns, -pipeline, -msg,
// -seconds. It prints mode, conns, pipeline, ops, ns/op, and syscalls/op
// (server side only: epoll_wait+read+write for epoll, io_uring_enter for
// uring). run.sh sweeps the cells and README.md holds the table and verdict.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	sysIoUringSetup = 425
	sysIoUringEnter = 426

	uringOffSQRing = 0x0
	uringOffCQRing = 0x8000000
	uringOffSQEs   = 0x10000000

	uringSetupCQSize    = 1 << 3
	uringFeatSingleMmap = 1 << 0
	uringEnterGetEvents = 1 << 0

	opSend = 26
	opRecv = 27

	msgNoSignal = 0x4000
)

type sqOffsets struct {
	head, tail, ringMask, ringEntries, flags, dropped, array, resv1 uint32
	userAddr                                                        uint64
}

type cqOffsets struct {
	head, tail, ringMask, ringEntries, overflow, cqes, flags, resv1 uint32
	userAddr                                                        uint64
}

type uringParams struct {
	sqEntries, cqEntries, flags, sqThreadCPU, sqThreadIdle, features, wqFd uint32
	resv                                                                   [3]uint32
	sqOff                                                                  sqOffsets
	cqOff                                                                  cqOffsets
}

type sqe struct {
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

type cqe struct {
	userData uint64
	res      int32
	flags    uint32
}

type ring struct {
	fd      int
	params  uringParams
	sqRing  []byte
	cqRing  []byte
	sqeMem  []byte
	sqHead  *uint32
	sqTail  *uint32
	sqMask  uint32
	sqArray []uint32
	sqes    []sqe
	cqHead  *uint32
	cqTail  *uint32
	cqMask  uint32
	cqes    []cqe
	sqeTail uint32
	staged  uint32
	enters  uint64
}

func newRing(entries uint32) (*ring, error) {
	r := &ring{}
	r.params.flags = uringSetupCQSize
	r.params.cqEntries = entries * 4
	fd, _, errno := syscall.Syscall(sysIoUringSetup, uintptr(entries), uintptr(unsafe.Pointer(&r.params)), 0)
	if errno != 0 {
		return nil, errno
	}
	r.fd = int(fd)
	p := &r.params
	sqSize := int(p.sqOff.array) + int(p.sqEntries)*4
	cqSize := int(p.cqOff.cqes) + int(p.cqEntries)*int(unsafe.Sizeof(cqe{}))
	if p.features&uringFeatSingleMmap != 0 && cqSize > sqSize {
		sqSize = cqSize
	}
	sq, err := syscall.Mmap(r.fd, uringOffSQRing, sqSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		return nil, err
	}
	r.sqRing = sq
	if p.features&uringFeatSingleMmap != 0 {
		r.cqRing = sq
	} else {
		cq, err := syscall.Mmap(r.fd, uringOffCQRing, cqSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
		if err != nil {
			return nil, err
		}
		r.cqRing = cq
	}
	sqem, err := syscall.Mmap(r.fd, uringOffSQEs, int(p.sqEntries)*int(unsafe.Sizeof(sqe{})), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		return nil, err
	}
	r.sqeMem = sqem
	at := func(m []byte, off uint32) *uint32 { return (*uint32)(unsafe.Pointer(&m[off])) }
	r.sqHead = at(r.sqRing, p.sqOff.head)
	r.sqTail = at(r.sqRing, p.sqOff.tail)
	r.sqMask = *at(r.sqRing, p.sqOff.ringMask)
	r.sqArray = unsafe.Slice((*uint32)(unsafe.Pointer(&r.sqRing[p.sqOff.array])), p.sqEntries)
	r.sqes = unsafe.Slice((*sqe)(unsafe.Pointer(&r.sqeMem[0])), p.sqEntries)
	r.cqHead = at(r.cqRing, p.cqOff.head)
	r.cqTail = at(r.cqRing, p.cqOff.tail)
	r.cqMask = *at(r.cqRing, p.cqOff.ringMask)
	r.cqes = unsafe.Slice((*cqe)(unsafe.Pointer(&r.cqRing[p.cqOff.cqes])), p.cqEntries)
	r.sqeTail = *r.sqTail
	return r, nil
}

func (r *ring) getSQE() *sqe {
	for r.sqeTail-atomic.LoadUint32(r.sqHead) >= r.params.sqEntries {
		_ = r.enter(r.publish(), 0, 0)
	}
	idx := r.sqeTail & r.sqMask
	e := &r.sqes[idx]
	*e = sqe{}
	r.sqArray[idx] = idx
	r.sqeTail++
	r.staged++
	return e
}

func (r *ring) publish() uint32 {
	atomic.StoreUint32(r.sqTail, r.sqeTail)
	n := r.staged
	r.staged = 0
	return n
}

func (r *ring) enter(toSubmit, minComplete, flags uint32) error {
	for {
		r.enters++
		_, _, errno := syscall.Syscall6(sysIoUringEnter, uintptr(r.fd), uintptr(toSubmit), uintptr(minComplete), uintptr(flags), 0, 0)
		if errno == syscall.EINTR {
			toSubmit = 0
			continue
		}
		if errno != 0 {
			return errno
		}
		return nil
	}
}

func (r *ring) reap(fn func(cqe)) int {
	head := *r.cqHead
	tail := atomic.LoadUint32(r.cqTail)
	n := 0
	for head != tail {
		c := r.cqes[head&r.cqMask]
		head++
		n++
		atomic.StoreUint32(r.cqHead, head)
		fn(c)
	}
	return n
}

// econn is one echo connection in either arm: a buffer, the fd, and for the
// uring arm whether a send owns the buffer right now.
type econn struct {
	fd      int
	buf     []byte
	sending bool
}

// serveURing echoes on the ring until stop: recv CQE stages a send of the
// bytes, send CQE re-arms the recv, one GETEVENTS enter per pass.
func serveURing(conns []*econn, msgBytes int, stop *atomic.Bool, echoed *uint64) uint64 {
	r, err := newRing(1024)
	if err != nil {
		fatal("io_uring_setup: %v (kernel too old or io_uring disabled?)", err)
	}
	byFD := map[int]*econn{}
	armRecv := func(c *econn) {
		e := r.getSQE()
		e.opcode = opRecv
		e.fd = int32(c.fd)
		e.addr = uint64(uintptr(unsafe.Pointer(&c.buf[0])))
		e.len = uint32(len(c.buf))
		e.userData = uint64(c.fd) << 1
	}
	for _, c := range conns {
		byFD[c.fd] = c
		armRecv(c)
	}
	live := len(conns)
	for live > 0 && !stop.Load() {
		if err := r.enter(r.publish(), 1, uringEnterGetEvents); err != nil {
			fatal("io_uring_enter: %v", err)
		}
		r.reap(func(c cqe) {
			ec := byFD[int(c.userData>>1)]
			isSend := c.userData&1 == 1
			if c.res <= 0 {
				if live > 0 {
					live--
				}
				return
			}
			if isSend {
				ec.sending = false
				armRecv(ec)
				return
			}
			// Recv completed: echo the bytes back and count the messages.
			atomic.AddUint64(echoed, uint64(int(c.res)/msgBytes))
			e := r.getSQE()
			e.opcode = opSend
			e.fd = int32(ec.fd)
			e.addr = uint64(uintptr(unsafe.Pointer(&ec.buf[0])))
			e.len = uint32(c.res)
			e.opFlags = msgNoSignal
			e.userData = uint64(ec.fd)<<1 | 1
			ec.sending = true
		})
	}
	return r.enters
}

// serveEpoll is the reactor-shaped arm: epoll_wait, one read per ready fd,
// one write of what was read, all counted.
func serveEpoll(conns []*econn, msgBytes int, stop *atomic.Bool, echoed *uint64) uint64 {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		fatal("epoll_create1: %v", err)
	}
	byFD := map[int]*econn{}
	for _, c := range conns {
		byFD[c.fd] = c
		ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(c.fd)}
		if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, c.fd, &ev); err != nil {
			fatal("epoll_ctl: %v", err)
		}
	}
	var sys uint64
	events := make([]syscall.EpollEvent, 128)
	live := len(conns)
	for live > 0 && !stop.Load() {
		sys++
		n, err := syscall.EpollWait(epfd, events, 100)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			fatal("epoll_wait: %v", err)
		}
		for i := 0; i < n; i++ {
			c := byFD[int(events[i].Fd)]
			sys++
			rn, err := syscall.Read(c.fd, c.buf)
			if rn <= 0 || err != nil {
				_ = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_DEL, c.fd, nil)
				live--
				continue
			}
			atomic.AddUint64(echoed, uint64(rn/msgBytes))
			for off := 0; off < rn; {
				sys++
				wn, err := syscall.Write(c.fd, c.buf[off:rn])
				if err == syscall.EAGAIN {
					continue // loopback with room; a busy retry keeps the lab honest
				}
				if err != nil || wn <= 0 {
					live--
					break
				}
				off += wn
			}
		}
	}
	return sys
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "lab21: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	mode := flag.String("mode", "uring", "echo loop: epoll or uring")
	nconns := flag.Int("conns", 64, "client connections")
	pipeline := flag.Int("pipeline", 16, "messages per client burst")
	msgBytes := flag.Int("msg", 64, "message size in bytes")
	seconds := flag.Int("seconds", 8, "timed window")
	flag.Parse()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fatal("listen: %v", err)
	}
	addr := ln.Addr().String()

	// Server side: accept, dup the fds out of the runtime, set nonblocking.
	conns := make([]*econn, 0, *nconns)
	accepted := make(chan *econn, *nconns)
	go func() {
		for i := 0; i < *nconns; i++ {
			nc, err := ln.Accept()
			if err != nil {
				fatal("accept: %v", err)
			}
			tc := nc.(*net.TCPConn)
			_ = tc.SetNoDelay(true)
			raw, err := tc.SyscallConn()
			if err != nil {
				fatal("rawconn: %v", err)
			}
			var dup int
			cerr := raw.Control(func(fd uintptr) {
				d, err := syscall.Dup(int(fd))
				if err != nil {
					fatal("dup: %v", err)
				}
				dup = d
			})
			if cerr != nil {
				fatal("control: %v", cerr)
			}
			_ = nc.Close()
			_ = syscall.SetNonblock(dup, true)
			// Re-set NODELAY on the dup, the reactor's known trap.
			_ = syscall.SetsockoptInt(dup, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
			bufSize := *pipeline * *msgBytes
			if bufSize < 4096 {
				bufSize = 4096
			}
			accepted <- &econn{fd: dup, buf: make([]byte, bufSize)}
		}
	}()

	// Client side: C pipelined round-trippers over the runtime's own netpoll.
	var stop atomic.Bool
	var echoed uint64
	var wg sync.WaitGroup
	burst := make([]byte, *pipeline**msgBytes)
	for i := range burst {
		burst[i] = byte(i)
	}
	for i := 0; i < *nconns; i++ {
		nc, err := net.Dial("tcp", addr)
		if err != nil {
			fatal("dial: %v", err)
		}
		_ = nc.(*net.TCPConn).SetNoDelay(true)
		wg.Add(1)
		go func(nc net.Conn) {
			defer wg.Done()
			defer func() { _ = nc.Close() }()
			got := make([]byte, len(burst))
			for !stop.Load() {
				if _, err := nc.Write(burst); err != nil {
					return
				}
				for n := 0; n < len(got); {
					m, err := nc.Read(got[n:])
					if err != nil {
						return
					}
					n += m
				}
			}
		}(nc)
	}
	for i := 0; i < *nconns; i++ {
		conns = append(conns, <-accepted)
	}

	timer := time.AfterFunc(time.Duration(*seconds)*time.Second, func() { stop.Store(true) })
	defer timer.Stop()
	start := time.Now()
	var sys uint64
	switch *mode {
	case "uring":
		sys = serveURing(conns, *msgBytes, &stop, &echoed)
	case "epoll":
		sys = serveEpoll(conns, *msgBytes, &stop, &echoed)
	default:
		fatal("unknown mode %q", *mode)
	}
	elapsed := time.Since(start)
	stop.Store(true)
	for _, c := range conns {
		_ = syscall.Close(c.fd)
	}
	wg.Wait()

	ops := atomic.LoadUint64(&echoed)
	if ops == 0 {
		fatal("no messages echoed")
	}
	fmt.Printf("mode=%s conns=%d pipeline=%d msg=%dB ops=%d ns/op=%.0f syscalls/op=%.3f\n",
		*mode, *nconns, *pipeline, *msgBytes, ops,
		float64(elapsed.Nanoseconds())/float64(ops), float64(sys)/float64(ops))
}
