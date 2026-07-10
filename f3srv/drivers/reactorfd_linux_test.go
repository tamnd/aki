//go:build linux

package drivers

import (
	"bufio"
	"bytes"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestReactorCloseFDLifecycle is the fd-lifecycle proof for the reactor
// shutdown contract: Close with a stream in flight to a slow reader, a
// write-blocked reply backlog, and idle clients pending must terminate within
// a deadline, and every dup'd connection fd must go through closeFD exactly
// once with no error. The counting seam catches both failure modes the dup
// design invites: a leak (a conn the shutdown walk missed) shows as a missing
// close, a double close (two paths racing to the same fd) shows as an EBADF
// from the second call or a count above one. Close returning at all is the
// abortStreams proof: the mid-flight stream's consumer unwinds through the
// loop notifier instead of waiting forever on chunks that stopped coming.
func TestReactorCloseFDLifecycle(t *testing.T) {
	if wantNetDriver() != NetReactor {
		t.Skip("fd lifecycle is reactor-only; the goroutine driver never dups the fd")
	}

	var mu sync.Mutex
	closes := make(map[int]int)
	var errs []error
	orig := closeFD
	closeFD = func(fd int) error {
		err := orig(fd)
		mu.Lock()
		closes[fd]++
		if err != nil {
			errs = append(errs, err)
		}
		mu.Unlock()
		return err
	}
	defer func() { closeFD = orig }()

	srv, err := Listen(Options{Addr: "127.0.0.1:0", Shards: 1, ArenaBytes: 64 << 20, SegBytes: 1 << 20, NetLoops: 1, ConnShape: testConnShape(), NetDriver: testNetDriver()})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()

	dial := func() net.Conn {
		nc, err := net.Dial("tcp", srv.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { nc.Close() })
		return nc
	}

	// A healthy control connection seeds the data and proves the server up.
	ctl := dial()
	cbr := bufio.NewReader(ctl)
	buildGiant(t, ctl, cbr, "giant", 16<<20)
	blob := bytes.Repeat([]byte("y"), 8<<10)
	var req bytes.Buffer
	req.WriteString("*3\r\n$3\r\nSET\r\n$4\r\nblob\r\n$" + strconv.Itoa(len(blob)) + "\r\n")
	req.Write(blob)
	req.WriteString("\r\n")
	if _, err := ctl.Write(req.Bytes()); err != nil {
		t.Fatal(err)
	}
	expect(t, cbr, "+OK\r\n")
	send(t, ctl, "PING")
	expect(t, cbr, "+PONG\r\n")

	// The stream in flight: a slow reader asks for 16MiB and reads nothing,
	// so the stream stalls with most of the value still shard-side.
	slow := dialSlow(t, srv.Addr().String(), 16<<10)
	send(t, slow, "GET", "giant")

	// The write-blocked backlog: pipelined point reads never drained.
	hog := dialSlow(t, srv.Addr().String(), 4<<10)
	if _, err := hog.Write(bytes.Repeat([]byte(cmd("GET", "blob")), 512)); err != nil {
		t.Fatal(err)
	}

	idle := dial()
	_ = idle
	time.Sleep(300 * time.Millisecond)

	done := make(chan struct{})
	go func() { srv.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Close hung with a stream in flight and slow clients pending")
	}

	mu.Lock()
	defer mu.Unlock()
	total := 0
	for fd, n := range closes {
		total += n
		if n != 1 {
			t.Errorf("fd %d closed %d times, want exactly once", fd, n)
		}
	}
	if total != 4 {
		t.Errorf("%d fd closes, want 4 (one per adopted connection)", total)
	}
	for _, err := range errs {
		t.Errorf("closeFD returned %v; a failure here is a double close or a bookkeeping bug", err)
	}
}
