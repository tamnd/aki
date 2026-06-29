//go:build linux

package networking

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// The uring net mode runs the epoll reactor with a per-loop io_uring that batches
// a turn's writes into one io_uring_enter. On a kernel without io_uring the loop
// falls back to the plain reactor path, so these tests assert correctness either
// way: the batched flush, or its fallback, must deliver the same bytes the
// goroutine path does. The on-kernel proof that the batch path itself fires is the
// uring package's own test plus the saturation benchmark.

// TestUringServeSingle checks one command and reply through the uring reactor.
func TestUringServeSingle(t *testing.T) {
	srv, addr := startTestServer(t, Config{NetMode: "uring"})
	defer srv.Close()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	if got := readLine(t, conn); got != "+PONG" {
		t.Fatalf("PING = %q want +PONG", got)
	}
}

// TestUringServePipeline checks that a single connection's multi-reply batch comes
// back whole and in order: the connection's outBuf holds several replies and the
// batched send must deliver all of them.
func TestUringServePipeline(t *testing.T) {
	srv, addr := startTestServer(t, Config{NetMode: "uring"})
	defer srv.Close()

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()

	if _, err := conn.Write([]byte("PING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\nPING a\r\n")); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	for _, w := range []string{"+PONG", "$2", "hi", "$1", "a"} {
		if got := readLineFrom(t, r); got != w {
			t.Fatalf("pipeline line = %q want %q", got, w)
		}
	}
}

// TestUringServeManyConns drives many connections at once so a single epoll_wait
// turn wakes more than one of them and the loop sends their replies in one batch.
// Every connection must get its own correct echo, which is what proves the batch
// maps each completion back to the right connection's buffer.
func TestUringServeManyConns(t *testing.T) {
	srv, addr := startTestServer(t, Config{NetMode: "uring"})
	defer srv.Close()

	const conns = 64
	const rounds = 50
	var wg sync.WaitGroup
	errc := make(chan error, conns)
	for i := range conns {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := net.Dial("tcp", addr)
			if err != nil {
				errc <- err
				return
			}
			defer c.Close()
			r := bufio.NewReader(c)
			for j := range rounds {
				want := fmt.Sprintf("c%d-%d", id, j)
				if _, err := fmt.Fprintf(c, "*2\r\n$4\r\nECHO\r\n$%d\r\n%s\r\n", len(want), want); err != nil {
					errc <- err
					return
				}
				_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
				hdr, err := r.ReadString('\n')
				if err != nil {
					errc <- fmt.Errorf("conn %d round %d header: %w", id, j, err)
					return
				}
				if hdr != fmt.Sprintf("$%d\r\n", len(want)) {
					errc <- fmt.Errorf("conn %d round %d hdr = %q", id, j, hdr)
					return
				}
				body, err := r.ReadString('\n')
				if err != nil {
					errc <- err
					return
				}
				if body != want+"\r\n" {
					errc <- fmt.Errorf("conn %d round %d body = %q want %q", id, j, body, want)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Error(err)
	}
}

// TestUringServeQuit checks the terminal path: QUIT's +OK is flushed inside drain
// before the connection closes, so the uring loop reaps it without a batched send
// and the client still sees the reply then EOF.
func TestUringServeQuit(t *testing.T) {
	srv, addr := startTestServer(t, Config{NetMode: "uring"})
	defer srv.Close()

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()

	if _, err := conn.Write([]byte("QUIT\r\n")); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	if got := readLineFrom(t, r); got != "+OK" {
		t.Fatalf("QUIT = %q want +OK", got)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := r.ReadByte(); err == nil {
		t.Fatal("expected EOF after QUIT reply")
	}
}
