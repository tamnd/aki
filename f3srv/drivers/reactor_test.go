package drivers

import (
	"bufio"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests target the reactor's own cliffs: driver selection, the
// parse-spanning-reads discipline when the loop owns the read buffer, the
// pipeline-window throttle, and the owner-to-loop wake edge under many
// connections. They run on whatever driver AKI_NET selects, so the goroutine
// driver gets the same coverage for free and the ubuntu reactor leg is the
// one that exercises the epoll paths.

// TestNetDriverUnknown pins that a typo'd driver name fails Listen loudly
// instead of falling back to something the operator did not ask for.
func TestNetDriverUnknown(t *testing.T) {
	if _, err := Listen(Options{Addr: "127.0.0.1:0", NetDriver: "epollish"}); err == nil {
		t.Fatal("Listen accepted NetDriver \"epollish\", want an error")
	}
}

// TestNetDriverReactorRequest pins the explicit-request contract: asking for
// the reactor is valid everywhere, runs the reactor on Linux, and falls back
// to the goroutine driver (reported honestly) on platforms without epoll.
func TestNetDriverReactorRequest(t *testing.T) {
	srv, err := Listen(Options{Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 4 << 20, SegBytes: 1 << 18, NetDriver: NetReactor})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })

	want := NetGoroutine
	if runtime.GOOS == "linux" {
		want = NetReactor
	}
	if got := srv.NetStats().Driver; got != want {
		t.Fatalf("driver = %q, want %q", got, want)
	}

	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	br := bufio.NewReader(nc)
	if _, err := nc.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+PONG\r\n")
}

// TestRaggedPipelineEverySplit is the resp parse_test every-split replay at
// the driver level: one pipelined request cut at every byte boundary into two
// writes, with a pause so the halves land as separate reads. The parser must
// resume mid-command from the driver's buffer at every cut, and the replies
// must come back whole and in order every time.
func TestRaggedPipelineEverySplit(t *testing.T) {
	srv, err := Listen(Options{Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 8 << 20, SegBytes: 1 << 18, ConnShape: testConnShape(), NetDriver: testNetDriver()})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })

	req := cmd("SET", "rag", "split-me") + "PING\r\n" + cmd("GET", "rag") + cmd("APPEND", "rag", "!") + cmd("STRLEN", "rag")
	want := "+OK\r\n+PONG\r\n$8\r\nsplit-me\r\n:9\r\n:9\r\n"

	for i := 1; i < len(req); i++ {
		nc, err := net.Dial("tcp", srv.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		br := bufio.NewReader(nc)
		_ = nc.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := nc.Write([]byte(req[:i])); err != nil {
			t.Fatalf("split %d: %v", i, err)
		}
		// Give the first half time to be read alone, so the split truly
		// spans two reads instead of coalescing in the socket buffer.
		time.Sleep(200 * time.Microsecond)
		if _, err := nc.Write([]byte(req[i:])); err != nil {
			t.Fatalf("split %d: %v", i, err)
		}
		got := make([]byte, len(want))
		for n := 0; n < len(got); {
			m, err := br.Read(got[n:])
			if err != nil {
				t.Fatalf("split %d: read after %q: %v", i, got[:n], err)
			}
			n += m
		}
		if string(got) != want {
			t.Fatalf("split %d: replies %q, want %q", i, got, want)
		}
		_ = nc.Close()
	}
}

// TestPipelineDeeperThanWindow sends one write far deeper than the reply
// window (1024), which on the reactor forces the throttle: the loop must stop
// parsing with the window full, disarm reads without spinning, resume the
// backlog as drains open the window, and still answer everything in order.
func TestPipelineDeeperThanWindow(t *testing.T) {
	_, nc, br := startServer(t)

	const n = 5000
	var req strings.Builder
	for i := 0; i < n; i++ {
		req.WriteString(cmd("SET", fmt.Sprintf("w%04d", i%97), "v"))
	}
	_ = nc.SetDeadline(time.Now().Add(30 * time.Second))
	go func() {
		// Written concurrently: the server must drain replies while the
		// client is still sending, or both sides deadlock on full buffers.
		_, _ = nc.Write([]byte(req.String()))
	}()
	expect(t, br, strings.Repeat("+OK\r\n", n))
}

// TestManyConnsWakeEdge is the starvation watchdog over real sockets: many
// connections doing P1-shaped rounds (one command, one reply, repeat), where
// every single reply rides the owner-to-loop wake. A lost wake anywhere
// surfaces as a read deadline on that connection, not a silent hang.
func TestManyConnsWakeEdge(t *testing.T) {
	srv, err := Listen(Options{Addr: "127.0.0.1:0", Shards: 4, ArenaBytes: 16 << 20, SegBytes: 1 << 18, ConnShape: testConnShape(), NetDriver: testNetDriver()})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })

	const conns = 16
	rounds := 400
	if testing.Short() {
		rounds = 80
	}

	var wg sync.WaitGroup
	errs := make(chan error, conns)
	for cid := 0; cid < conns; cid++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			nc, err := net.Dial("tcp", srv.Addr().String())
			if err != nil {
				errs <- err
				return
			}
			defer nc.Close()
			br := bufio.NewReader(nc)
			for r := 0; r < rounds; r++ {
				k := fmt.Sprintf("c%02d-%03d", cid, r%53)
				_ = nc.SetDeadline(time.Now().Add(10 * time.Second))
				if _, err := nc.Write([]byte(cmd("SET", k, "v"))); err != nil {
					errs <- fmt.Errorf("conn %d round %d: %v", cid, r, err)
					return
				}
				buf := make([]byte, 5)
				for n := 0; n < len(buf); {
					m, err := br.Read(buf[n:])
					if err != nil {
						errs <- fmt.Errorf("conn %d round %d: reply starved: %v", cid, r, err)
						return
					}
					n += m
				}
				if string(buf) != "+OK\r\n" {
					errs <- fmt.Errorf("conn %d round %d: reply %q", cid, r, buf)
					return
				}
			}
		}(cid)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
