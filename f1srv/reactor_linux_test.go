//go:build linux

package f1srv

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestReactorWirePath drives the epoll reactor over a real socket on the Linux kernel:
// the point path, a pipeline of many commands in one write (one read, one flush on the
// loop side), a value larger than the read buffer so rbuf grows mid-parse, QUIT
// discarding a command queued behind it, and a clean EOF close. It is the reactor
// counterpart to the goroutine-path command tests; the parse-dispatch-reply code is
// shared, so this proves the reactor's own read, drain, flush, and close.
func TestReactorWirePath(t *testing.T) {
	rw, cleanup := dialTestServerMode(t, "reactor")
	defer cleanup()

	cmd(t, rw, "PING")
	if got := readReply(t, rw); got != "+PONG" {
		t.Fatalf("PING: got %q", got)
	}

	cmd(t, rw, "SET", "k", "v")
	if got := readReply(t, rw); got != "+OK" {
		t.Fatalf("SET: got %q", got)
	}
	cmd(t, rw, "GET", "k")
	if got := readReply(t, rw); got != "$v" {
		t.Fatalf("GET: got %q", got)
	}

	cmd(t, rw, "INCR", "n")
	if got := readReply(t, rw); got != ":1" {
		t.Fatalf("INCR: got %q", got)
	}

	// A pipeline written in one shot: the reactor reads it, drains every command, and
	// flushes all replies in one write.
	var pipe strings.Builder
	for i := 0; i < 64; i++ {
		fmt.Fprintf(&pipe, "*3\r\n$3\r\nSET\r\n$4\r\nk%03d\r\n$1\r\nx\r\n", i)
	}
	if _, err := rw.WriteString(pipe.String()); err != nil {
		t.Fatalf("pipeline write: %v", err)
	}
	if err := rw.Flush(); err != nil {
		t.Fatalf("pipeline flush: %v", err)
	}
	for i := 0; i < 64; i++ {
		if got := readReply(t, rw); got != "+OK" {
			t.Fatalf("pipeline SET %d: got %q", i, got)
		}
	}

	// A value larger than the 4 KiB test read buffer (and under f1raw's 64 KiB cap)
	// forces rbuf to grow while a single command is still incomplete.
	big := strings.Repeat("A", 50_000)
	cmd(t, rw, "SET", "big", big)
	if got := readReply(t, rw); got != "+OK" {
		t.Fatalf("SET big: got %q", got)
	}
	cmd(t, rw, "GET", "big")
	if got := readReply(t, rw); got != "$"+big {
		t.Fatalf("GET big: length mismatch (%d bytes)", len(got)-1)
	}

	// QUIT is answered, then the command queued behind it is discarded and the socket
	// closes.
	if _, err := rw.WriteString("QUIT\r\nPING\r\n"); err != nil {
		t.Fatalf("quit write: %v", err)
	}
	if err := rw.Flush(); err != nil {
		t.Fatalf("quit flush: %v", err)
	}
	if got := readReply(t, rw); got != "+OK" {
		t.Fatalf("QUIT: got %q", got)
	}
	if _, err := rw.ReadString('\n'); err == nil {
		t.Fatalf("expected EOF after QUIT, read a reply")
	}
}

// TestReactorConcurrentConns opens more connections than there are loops so several land
// on the same fd-sharded loop, and drives each independently. It checks that per-loop,
// per-connection state stays isolated and that the wake/adopt hand-off from the accept
// goroutine works under load.
func TestReactorConcurrentConns(t *testing.T) {
	cfg := Config{Addr: "127.0.0.1:0", IndexBuckets: 1 << 14, ArenaBytes: 1 << 22, ReadBufSize: 4 << 10, IncrStripes: 256, NetMode: "reactor"}
	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ListenAndServe()
	defer srv.Close()
	addr := srv.Addr()

	const conns = 32
	const perConn = 200
	var wg sync.WaitGroup
	errs := make(chan error, conns)
	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				errs <- fmt.Errorf("conn %d dial: %w", id, err)
				return
			}
			defer conn.Close()
			br := bufio.NewReader(conn)
			bw := bufio.NewWriter(conn)
			for i := 0; i < perConn; i++ {
				key := fmt.Sprintf("c%d:k%d", id, i)
				fmt.Fprintf(bw, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nx\r\n", len(key), key)
				fmt.Fprintf(bw, "*2\r\n$4\r\nINCR\r\n$%d\r\ncnt%d\r\n", len("cnt")+len(fmt.Sprint(id)), id)
				if err := bw.Flush(); err != nil {
					errs <- fmt.Errorf("conn %d flush: %w", id, err)
					return
				}
				line, err := br.ReadString('\n')
				if err != nil || line != "+OK\r\n" {
					errs <- fmt.Errorf("conn %d SET reply %q err %v", id, line, err)
					return
				}
				line, err = br.ReadString('\n')
				if err != nil || line != fmt.Sprintf(":%d\r\n", i+1) {
					errs <- fmt.Errorf("conn %d INCR reply %q want :%d err %v", id, line, i+1, err)
					return
				}
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// startReactorServer starts a reactor-mode server on an ephemeral port and returns its
// address plus a cleanup. Unlike dialTestServerMode it opens no connection, so a test can
// drive several independent clients against the same loops, which is what the blocking-park
// path needs: one client blocks while another wakes it.
func startReactorServer(t *testing.T) (string, func()) {
	t.Helper()
	cfg := Config{Addr: "127.0.0.1:0", IndexBuckets: 1 << 12, ArenaBytes: 1 << 20, ReadBufSize: 4 << 10, IncrStripes: 64, NetMode: "reactor"}
	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ListenAndServe()
	return srv.Addr(), func() { srv.Close() }
}

func dialRW(t *testing.T, addr string) (net.Conn, *bufio.ReadWriter) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
}

// TestReactorBlockingParkServed proves the async-park facility on the epoll loop: a BLPOP on
// an empty key must not reply immediately (the reactor disarms reads and hands the command to
// a park goroutine), and a later LPUSH from another connection wakes it with the pushed
// element. This is the semantics the goroutine driver gives for free by parking the
// connection's own goroutine; here the connection stays on the shared loop, so the test
// guards against both a lost wake and a busy reply that would mean the command never parked.
func TestReactorBlockingParkServed(t *testing.T) {
	addr, stop := startReactorServer(t)
	defer stop()

	blkConn, blk := dialRW(t, addr)
	defer blkConn.Close()
	cmd(t, blk, "BLPOP", "mylist", "0")

	// The blocked client must stay silent until a push arrives. A short read deadline that
	// times out is the proof it parked rather than replying nil right away.
	_ = blkConn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if _, err := blk.ReadString('\n'); err == nil {
		t.Fatal("BLPOP replied before any push; it did not park")
	}
	_ = blkConn.SetReadDeadline(time.Time{})

	pushConn, push := dialRW(t, addr)
	defer pushConn.Close()
	cmd(t, push, "LPUSH", "mylist", "hello")
	if got := readReply(t, push); got != ":1" {
		t.Fatalf("LPUSH: got %q", got)
	}

	// The park goroutine wrote its reply into cs.out and posted the connection back to the
	// loop, which flushed it: a two-element array of the key name and the popped value.
	_ = blkConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if got := readReply(t, blk); got != "*2" {
		t.Fatalf("BLPOP header: got %q", got)
	}
	if got := readReply(t, blk); got != "$mylist" {
		t.Fatalf("BLPOP key: got %q", got)
	}
	if got := readReply(t, blk); got != "$hello" {
		t.Fatalf("BLPOP value: got %q", got)
	}
}

// TestReactorBlockingParkDisconnect proves the peer-disconnect path: a client that closes its
// socket while blocked in BLPOP must unwind the park goroutine (through parkCancel) without
// consuming a later push, and must not wedge or crash the loop. After the disconnect a fresh
// client pushes and pops the same key, which confirms the waiter was fully removed and the
// loop still serves.
func TestReactorBlockingParkDisconnect(t *testing.T) {
	addr, stop := startReactorServer(t)
	defer stop()

	blkConn, blk := dialRW(t, addr)
	cmd(t, blk, "BLPOP", "gone", "0")
	// Let the command reach the loop and park before the peer vanishes.
	_ = blkConn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if _, err := blk.ReadString('\n'); err == nil {
		t.Fatal("BLPOP replied before parking")
	}
	blkConn.Close()
	// Give the loop a moment to observe EPOLLRDHUP and cancel the park.
	time.Sleep(150 * time.Millisecond)

	// A push after the disconnect must not have been delivered to the dead waiter: the value
	// stays in the list and a fresh BLPOP pops it.
	otherConn, other := dialRW(t, addr)
	defer otherConn.Close()
	cmd(t, other, "LPUSH", "gone", "world")
	if got := readReply(t, other); got != ":1" {
		t.Fatalf("LPUSH after disconnect: got %q", got)
	}
	cmd(t, other, "BLPOP", "gone", "0")
	if got := readReply(t, other); got != "*2" {
		t.Fatalf("BLPOP header: got %q", got)
	}
	if got := readReply(t, other); got != "$gone" {
		t.Fatalf("BLPOP key: got %q", got)
	}
	if got := readReply(t, other); got != "$world" {
		t.Fatalf("BLPOP value: got %q", got)
	}
}
