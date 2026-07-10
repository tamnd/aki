package drivers

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// The Class OF slice (M10 pull-forward slice 5): a streamed giant reply to a
// slow reader must not convoy its event loop, must not buffer the whole value
// driver-side, and a client past the output-buffer hard cap is dropped, that
// connection only. These tests run on both drivers; the goroutine driver
// satisfies the convoy contract trivially (a writer goroutine per connection)
// and serves as the behavioral baseline the reactor must match.

// giantPat is the byte pattern for the streamed value, position-dependent so
// a mixed-up chunk order or a hole shows up in verification.
func giantPat(i int) byte { return byte(i*7 + 13) }

// buildGiant SETs a chunked value of total bytes under key, growing it with
// bounded APPENDs so the client side never holds more than piece bytes and
// the heap baseline taken afterwards stays honest.
func buildGiant(t *testing.T, nc net.Conn, br *bufio.Reader, key string, total int) {
	t.Helper()
	const piece = 4 << 20
	buf := make([]byte, piece)
	for off := 0; off < total; off += piece {
		n := min(piece, total-off)
		for i := 0; i < n; i++ {
			buf[i] = giantPat(off + i)
		}
		verb, reply := "APPEND", ":"+strconv.Itoa(off+n)+"\r\n"
		if off == 0 {
			verb, reply = "SET", "+OK\r\n"
		}
		var req bytes.Buffer
		req.WriteString("*3\r\n$" + strconv.Itoa(len(verb)) + "\r\n" + verb + "\r\n")
		req.WriteString("$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n")
		req.WriteString("$" + strconv.Itoa(n) + "\r\n")
		req.Write(buf[:n])
		req.WriteString("\r\n")
		if _, err := nc.Write(req.Bytes()); err != nil {
			t.Fatal(err)
		}
		expect(t, br, reply)
	}
}

// dialSlow dials with a fixed small receive buffer, set before the TCP
// handshake so the window scaling and the advertised window are negotiated
// small from the start. Shrinking the buffer on an established connection
// instead traps the kernel's window recovery: rcv_ssthresh stays clamped and
// the sender crawls through zero-window persist probes at tens of KB/s when
// the reader finally drains (found the hard way on the Linux leg). A fixed
// buffer also turns receive autotuning off, so the kernel-absorbed backlog
// stays a known constant in the tests' arithmetic.
func dialSlow(t *testing.T, addr string, rcvbuf int) net.Conn {
	t.Helper()
	d := net.Dialer{Control: func(network, address string, c syscall.RawConn) error {
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, rcvbuf)
		}); err != nil {
			return err
		}
		return serr
	}}
	nc, err := d.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

// heapAlloc settles the collector and returns the live heap.
func heapAlloc() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// TestStreamSlowReaderSharesLoop is the PL-4 loop-convoy tripwire. One event
// loop (NetLoops: 1), a 32MiB chunked value, and a slow reader (64KiB receive
// buffer) that requests it and reads nothing: the stream stalls against a
// full socket with almost all of the value still shard-side. While it is
// stalled, the test asserts two things. A fast connection on the same loop
// completes point ops within a deadline, which is the convoy contract: the
// loop emits stream chunks in bounded quanta and yields, it never sits on one
// connection. And the process heap stays far under the value size, which is
// the incremental-emit contract: the driver holds at most a reply buffer of
// stream bytes per connection, never the whole value (a driver that buffers
// the value to dodge the convoy fails here instead). The kernel cannot hide
// the value either: with the small fixed client receive buffer the socket
// path holds a few MiB at most (the sender's buffer plus the receive window),
// so the stream really is mid-flight while the assertions run. The slow
// reader then drains and verifies every byte, and the connection answers a
// trailing PING, proving the stalled stream resumed cleanly rather than being
// dropped.
func TestStreamSlowReaderSharesLoop(t *testing.T) {
	const total = 32 << 20

	srv, err := Listen(Options{Addr: "127.0.0.1:0", Shards: 1, ArenaBytes: 96 << 20, SegBytes: 1 << 20, NetLoops: 1, ConnShape: testConnShape(), NetDriver: testNetDriver()})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })

	fast, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fast.Close() })
	fbr := bufio.NewReader(fast)
	buildGiant(t, fast, fbr, "giant", total)

	base := heapAlloc()

	slow := dialSlow(t, srv.Addr().String(), 64<<10)
	send(t, slow, "GET", "giant")
	// Let the stream engage and stall against the small receive window.
	time.Sleep(300 * time.Millisecond)

	// The convoy assertion: point ops on the same loop, under a deadline. A
	// loop stuck emitting (or spinning on) the stalled stream turns these
	// reads into timeouts.
	if err := fast.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		send(t, fast, "PING")
		expect(t, fbr, "+PONG\r\n")
	}
	send(t, fast, "SET", "k", "v")
	expect(t, fbr, "+OK\r\n")
	send(t, fast, "GET", "k")
	expect(t, fbr, "$1\r\nv\r\n")

	// The no-buffering assertion: the whole value driver-side would be a
	// >=32MiB heap jump; the streamed path holds a reply buffer's worth.
	if grew := int64(heapAlloc()) - int64(base); grew > 16<<20 {
		t.Fatalf("heap grew %d bytes with a %d-byte stream in flight; the driver is buffering the value", grew, total)
	}

	// Drain and verify, then prove the connection survived the stall.
	if err := slow.SetDeadline(time.Now().Add(120 * time.Second)); err != nil {
		t.Fatal(err)
	}
	sbr := bufio.NewReader(slow)
	hdr, err := sbr.ReadString('\n')
	if err != nil {
		t.Fatalf("bulk header: %v", err)
	}
	if hdr != "$"+strconv.Itoa(total)+"\r\n" {
		t.Fatalf("bulk header = %q, want $%d", hdr, total)
	}
	chunk := make([]byte, 64<<10)
	for off := 0; off < total; {
		n := min(len(chunk), total-off)
		if _, err := io.ReadFull(sbr, chunk[:n]); err != nil {
			t.Fatalf("bulk body at %d: %v", off, err)
		}
		for i := 0; i < n; i++ {
			if chunk[i] != giantPat(off+i) {
				t.Fatalf("byte %d = %#x, want %#x", off+i, chunk[i], giantPat(off+i))
			}
		}
		off += n
	}
	tail := make([]byte, 2)
	if _, err := io.ReadFull(sbr, tail); err != nil || string(tail) != "\r\n" {
		t.Fatalf("bulk trailer = %q, %v", tail, err)
	}
	send(t, slow, "PING")
	expect(t, sbr, "+PONG\r\n")
}

// TestStreamClientGoneMidStream closes the client while its streamed reply is
// stalled mid-flight: the shard-side stream must unwind (the pump fails it,
// the arena pins release), the connection is dropped alone, and the server
// keeps answering other clients and shuts down cleanly afterwards. A leaked
// stream shows up as Close hanging, which the deadline in TestMain-less form
// here is the t.Cleanup path plus the healthy-conn check.
func TestStreamClientGoneMidStream(t *testing.T) {
	const total = 8 << 20

	srv, err := Listen(Options{Addr: "127.0.0.1:0", Shards: 1, ArenaBytes: 32 << 20, SegBytes: 1 << 20, NetLoops: 1, ConnShape: testConnShape(), NetDriver: testNetDriver()})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()

	fast, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fast.Close() })
	fbr := bufio.NewReader(fast)
	buildGiant(t, fast, fbr, "giant", total)

	slow := dialSlow(t, srv.Addr().String(), 16<<10)
	send(t, slow, "GET", "giant")
	time.Sleep(200 * time.Millisecond)
	// Read a little so the header and first chunks moved, then vanish.
	if _, err := io.ReadFull(slow, make([]byte, 8<<10)); err != nil {
		t.Fatal(err)
	}
	slow.Close()

	// The server must stay healthy on the other connection while the dead
	// stream unwinds behind the scenes.
	if err := fast.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		send(t, fast, "PING")
		expect(t, fbr, "+PONG\r\n")
	}
	send(t, fast, "GET", "giant")
	hdr, err := fbr.ReadString('\n')
	if err != nil || hdr != "$"+strconv.Itoa(total)+"\r\n" {
		t.Fatalf("bulk header = %q, %v", hdr, err)
	}
	if _, err := io.CopyN(io.Discard, fbr, total+2); err != nil {
		t.Fatal(err)
	}

	// Clean shutdown with the unwound stream is part of the contract; a
	// leaked stream or pin hangs Close. Close waits for live connections, so
	// the healthy one goes first.
	fast.Close()
	done := make(chan struct{})
	go func() { srv.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Close hung after a client died mid-stream")
	}
}

// TestSlowClientOutbufDrop covers the doc 08 section 3.5 hard cap on the
// reactor: a client that pipelines point reads and never drains them piles
// replies into the driver-side output buffer once the socket backs up, and
// past OutBufLimitBytes the driver disconnects it, that connection only,
// counting the event as net_disconnects_outbuf. The goroutine driver has no
// equivalent backlog (its writer blocks on the socket), so the test is
// reactor-only.
func TestSlowClientOutbufDrop(t *testing.T) {
	if wantNetDriver() != NetReactor {
		t.Skip("output-buffer cap is a reactor discipline; the goroutine writer blocks on the socket instead")
	}

	srv, err := Listen(Options{Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 16 << 20, SegBytes: 1 << 20, NetLoops: 1, OutBufLimitBytes: 256 << 10, ConnShape: testConnShape(), NetDriver: testNetDriver()})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })

	fast, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fast.Close() })
	fbr := bufio.NewReader(fast)
	blob := bytes.Repeat([]byte("x"), 8<<10)
	var req bytes.Buffer
	req.WriteString("*3\r\n$3\r\nSET\r\n$4\r\nblob\r\n$" + strconv.Itoa(len(blob)) + "\r\n")
	req.Write(blob)
	req.WriteString("\r\n")
	if _, err := fast.Write(req.Bytes()); err != nil {
		t.Fatal(err)
	}
	expect(t, fbr, "+OK\r\n")

	// The hog: 2048 pipelined 8KiB reads, 16MiB of replies it never reads.
	// The kernel absorbs a few MiB at most against the small receive window;
	// the rest lands in the driver's output buffer and trips the cap.
	hog := dialSlow(t, srv.Addr().String(), 4<<10)
	pipe := bytes.Repeat([]byte(cmd("GET", "blob")), 2048)
	if _, err := hog.Write(pipe); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for srv.NetStats().OutbufDisconnects == 0 {
		if time.Now().After(deadline) {
			t.Fatal("client never dropped at the output-buffer cap")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// That connection only: the well-behaved one still answers.
	if err := fast.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	send(t, fast, "PING")
	expect(t, fbr, "+PONG\r\n")
	if got := srv.NetStats().OutbufDisconnects; got != 1 {
		t.Fatalf("OutbufDisconnects = %d, want 1", got)
	}
}
