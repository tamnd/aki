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
