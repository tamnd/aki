package f1srv

import (
	"bufio"
	"net"
	"testing"
	"time"
)

// dialTwoGo starts one server on the goroutine driver and returns two independent client
// connections to it plus a cleanup. The blocking list commands only park on the goroutine
// driver, so these tests force "go" regardless of F1SRV_TEST_NET: under the reactor a park
// would stall the shared loop, and the blocking commands there serve non-blocking, which is
// covered by the immediate-serve assertions rather than the wakeup ones.
func dialTwoGo(t *testing.T) (a, b *bufio.ReadWriter, cleanup func()) {
	t.Helper()
	cfg := Config{Addr: "127.0.0.1:0", IndexBuckets: 1 << 12, ArenaBytes: 1 << 20, ReadBufSize: 4 << 10, IncrStripes: 64, NetMode: "go"}
	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ListenAndServe()
	dial := func() (*bufio.ReadWriter, net.Conn) {
		conn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		return bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), conn
	}
	rwA, connA := dial()
	rwB, connB := dial()
	cleanup = func() {
		connA.Close()
		connB.Close()
		srv.Close()
	}
	return rwA, rwB, cleanup
}

// A BLPOP with an element already present pops it without blocking and replies with the
// [key, element] pair, scanning the listed keys left to right and skipping the empty ones.
// BLPOP takes the head, BRPOP the tail.
func TestListBPopImmediate(t *testing.T) {
	rw, _, cleanup := dialTwoGo(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "k", "a", "b", "c")
	expect(t, rw, ":3")

	// A missing first key is skipped; the pair comes from the first non-empty list, head first.
	cmd(t, rw, "BLPOP", "miss", "k", "0")
	expect(t, rw, "*2")
	expect(t, rw, "$k")
	expect(t, rw, "$a")

	// BRPOP takes the tail of the same list.
	cmd(t, rw, "BRPOP", "k", "0")
	expect(t, rw, "*2")
	expect(t, rw, "$k")
	expect(t, rw, "$c")

	if got := lrangeCall(t, rw, "LRANGE", "k", "0", "-1"); !eqStrs(got, []string{"b"}) {
		t.Fatalf("k after bpop = %v", got)
	}
}

// A BLPOP on keys that stay empty for the whole timeout replies with a null array.
func TestListBLPopTimeout(t *testing.T) {
	rw, _, cleanup := dialTwoGo(t)
	defer cleanup()

	start := time.Now()
	cmd(t, rw, "BLPOP", "empty1", "empty2", "0.15")
	expect(t, rw, "*-1")
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Fatalf("BLPOP returned after %v, want at least the timeout", waited)
	}
}

// A BLPOP that blocks on a missing key wakes when another connection pushes to it and delivers
// the pushed element exactly once. This is the cross-goroutine path: the blocked client parks
// its own goroutine and a push on a second connection signals it.
func TestListBLPopWakeup(t *testing.T) {
	rwA, rwB, cleanup := dialTwoGo(t)
	defer cleanup()

	done := make(chan string, 1)
	go func() {
		cmd(t, rwA, "BLPOP", "wk", "0") // block forever until woken
		header := readReply(t, rwA)     // "*2"
		key := readReply(t, rwA)        // "$wk"
		val := readReply(t, rwA)        // "$hello"
		done <- header + " " + key + " " + val
	}()

	// Give the blocking client time to park before the push arrives, so the wakeup path runs
	// rather than the immediate-serve path.
	time.Sleep(50 * time.Millisecond)
	cmd(t, rwB, "RPUSH", "wk", "hello")
	expect(t, rwB, ":1")

	select {
	case got := <-done:
		if got != "*2 $wk $hello" {
			t.Fatalf("woken reply = %q, want %q", got, "*2 $wk $hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BLPOP did not wake after RPUSH")
	}

	// The element was delivered to the blocked client, so the key is empty again.
	cmd(t, rwB, "EXISTS", "wk")
	expect(t, rwB, ":0")
}

// BLMOVE and BRPOPLPUSH move an element when the source has one, blocking on the source
// otherwise; a timeout replies with a null bulk, not a null array.
func TestListBLMoveImmediate(t *testing.T) {
	rw, _, cleanup := dialTwoGo(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "src", "a", "b", "c")
	expect(t, rw, ":3")

	// BLMOVE src dst LEFT RIGHT: head of src to tail of dst.
	cmd(t, rw, "BLMOVE", "src", "dst", "LEFT", "RIGHT", "0")
	expect(t, rw, "$a")

	// BRPOPLPUSH is BLMOVE src dst RIGHT LEFT: tail of src to head of dst.
	cmd(t, rw, "BRPOPLPUSH", "src", "dst", "0")
	expect(t, rw, "$c")

	if got := lrangeCall(t, rw, "LRANGE", "dst", "0", "-1"); !eqStrs(got, []string{"c", "a"}) {
		t.Fatalf("dst after moves = %v", got)
	}

	// A move off an empty source times out with a null bulk and creates nothing.
	cmd(t, rw, "BLMOVE", "ghost", "gg", "LEFT", "RIGHT", "0.1")
	expect(t, rw, "$-1")
	cmd(t, rw, "EXISTS", "gg")
	expect(t, rw, ":0")

	// A bad direction token is a syntax error.
	cmd(t, rw, "BLMOVE", "src", "dst", "UP", "DOWN", "0")
	if got := readReply(t, rw); got != "-ERR syntax error" {
		t.Fatalf("bad direction = %q", got)
	}
}

// A BLMOVE blocked on an empty source wakes when a push lands on the source and moves the
// element to the destination.
func TestListBLMoveWakeup(t *testing.T) {
	rwA, rwB, cleanup := dialTwoGo(t)
	defer cleanup()

	done := make(chan string, 1)
	go func() {
		cmd(t, rwA, "BLMOVE", "ms", "md", "LEFT", "RIGHT", "0")
		done <- readReply(t, rwA) // "$v"
	}()

	time.Sleep(50 * time.Millisecond)
	cmd(t, rwB, "RPUSH", "ms", "v")
	expect(t, rwB, ":1")

	select {
	case got := <-done:
		if got != "$v" {
			t.Fatalf("woken move reply = %q, want %q", got, "$v")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BLMOVE did not wake after RPUSH")
	}

	if got := lrangeCall(t, rwB, "LRANGE", "md", "0", "-1"); !eqStrs(got, []string{"v"}) {
		t.Fatalf("md after woken move = %v", got)
	}
	cmd(t, rwB, "EXISTS", "ms")
	expect(t, rwB, ":0")
}

// BLMPOP serves LMPOP's nested [key, [elements]] reply when a key has elements, and replies
// with a null array on timeout.
func TestListBLMPop(t *testing.T) {
	rw, _, cleanup := dialTwoGo(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "m", "x", "y", "z")
	expect(t, rw, ":3")

	// Skip the missing key, pop up to COUNT from the head of the first non-empty list.
	cmd(t, rw, "BLMPOP", "0", "2", "nope", "m", "LEFT", "COUNT", "2")
	expect(t, rw, "*2")
	expect(t, rw, "$m")
	expect(t, rw, "*2")
	expect(t, rw, "$x")
	expect(t, rw, "$y")
	if got := lrangeCall(t, rw, "LRANGE", "m", "0", "-1"); !eqStrs(got, []string{"z"}) {
		t.Fatalf("m after blmpop = %v", got)
	}

	// Every listed key empty for the whole timeout is a null array.
	start := time.Now()
	cmd(t, rw, "BLMPOP", "0.15", "2", "e1", "e2", "LEFT")
	expect(t, rw, "*-1")
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Fatalf("BLMPOP returned after %v, want at least the timeout", waited)
	}

	// numkeys must be positive.
	cmd(t, rw, "BLMPOP", "0", "0", "k", "LEFT")
	if got := readReply(t, rw); got != "-ERR numkeys should be greater than 0" {
		t.Fatalf("numkeys 0 = %q", got)
	}
}

// A blocking command against a plain string key is WRONGTYPE, the same as its non-blocking
// sibling, so the block never parks on a type it can never pop from.
func TestListBlockingWrongType(t *testing.T) {
	rw, _, cleanup := dialTwoGo(t)
	defer cleanup()

	cmd(t, rw, "SET", "s", "v")
	expect(t, rw, "+OK")
	for _, args := range [][]string{
		{"BLPOP", "s", "0"},
		{"BRPOP", "s", "0"},
		{"BLMOVE", "s", "d", "LEFT", "RIGHT", "0"},
		{"BRPOPLPUSH", "s", "d", "0"},
		{"BLMPOP", "0", "1", "s", "LEFT"},
	} {
		cmd(t, rw, args...)
		if got := readReply(t, rw); got[0] != '-' || got[:10] != "-WRONGTYPE" {
			t.Fatalf("%v on string = %q, want WRONGTYPE", args, got)
		}
	}
}
