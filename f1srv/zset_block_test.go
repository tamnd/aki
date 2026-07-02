package f1srv

import (
	"bufio"
	"testing"
	"time"
)

// readBulk reads one RESP bulk-string reply element and returns its payload without the "$"
// marker, failing when the reply is not a bulk string.
func readBulk(t *testing.T, rw *bufio.ReadWriter) string {
	t.Helper()
	got := readReply(t, rw)
	if len(got) == 0 || got[0] != '$' {
		t.Fatalf("reply = %q, want a bulk string", got)
	}
	return got[1:]
}

// BZPOPMIN and BZPOPMAX return [key, member, score] when a listed key already has members,
// serving the lowest or highest scorer without ever blocking.
func TestBZPopReady(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "10", "a", "5", "b", "3", "c")
	expect(t, rw, ":3")

	cmd(t, rw, "BZPOPMIN", "z", "0")
	expect(t, rw, "*3")
	if k := readBulk(t, rw); k != "z" {
		t.Fatalf("key = %q, want z", k)
	}
	if m := readBulk(t, rw); m != "c" {
		t.Fatalf("member = %q, want c (lowest score)", m)
	}
	if s := readBulk(t, rw); s != "3" {
		t.Fatalf("score = %q, want 3", s)
	}

	cmd(t, rw, "BZPOPMAX", "z", "0")
	expect(t, rw, "*3")
	if k := readBulk(t, rw); k != "z" {
		t.Fatalf("key = %q, want z", k)
	}
	if m := readBulk(t, rw); m != "a" {
		t.Fatalf("member = %q, want a (highest score)", m)
	}
	if s := readBulk(t, rw); s != "10" {
		t.Fatalf("score = %q, want 10", s)
	}
}

// A multi-key BZPOPMIN serves the first key that has members, scanning left to right.
func TestBZPopFirstNonEmpty(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "second", "7", "x")
	expect(t, rw, ":1")

	cmd(t, rw, "BZPOPMIN", "first", "second", "0")
	expect(t, rw, "*3")
	if k := readBulk(t, rw); k != "second" {
		t.Fatalf("key = %q, want second", k)
	}
	if m := readBulk(t, rw); m != "x" {
		t.Fatalf("member = %q, want x", m)
	}
	if s := readBulk(t, rw); s != "7" {
		t.Fatalf("score = %q, want 7", s)
	}
}

// A short positive timeout on empty keys returns a null array once it fires.
func TestBZPopTimeout(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	start := time.Now()
	cmd(t, rw, "BZPOPMIN", "nope", "0.15")
	expect(t, rw, "*-1")
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("returned after %v, want it to wait out the ~150ms timeout", elapsed)
	}
}

// A blocked BZPOPMIN wakes when another connection adds a member, and delivers it exactly once.
func TestBZPopWakeup(t *testing.T) {
	rw, pusher, cleanup := dialTwoGo(t)
	defer cleanup()

	done := make(chan []string, 1)
	go func() {
		cmd(t, rw, "BZPOPMIN", "wq", "5")
		hdr := readReply(t, rw)
		k := readReply(t, rw)
		m := readReply(t, rw)
		s := readReply(t, rw)
		done <- []string{hdr, k, m, s}
	}()

	// Give the reader a moment to park before the add.
	time.Sleep(50 * time.Millisecond)
	cmd(t, pusher, "ZADD", "wq", "42", "woken")
	expect(t, pusher, ":1")

	select {
	case got := <-done:
		if got[0] != "*3" || got[1] != "$wq" || got[2] != "$woken" || got[3] != "$42" {
			t.Fatalf("woken reply = %v, want [*3 $wq $woken $42]", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BZPOPMIN did not wake on ZADD")
	}
}

// BZMPOP pops up to COUNT extreme members from the first non-empty key and nests them as
// [key, [[member, score], ...]].
func TestBZMPopReady(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")

	cmd(t, rw, "BZMPOP", "0", "2", "miss", "z", "MIN", "COUNT", "2")
	expect(t, rw, "*2")
	if k := readBulk(t, rw); k != "z" {
		t.Fatalf("key = %q, want z", k)
	}
	expect(t, rw, "*2")
	expect(t, rw, "*2")
	if m := readBulk(t, rw); m != "a" {
		t.Fatalf("member = %q, want a", m)
	}
	if s := readBulk(t, rw); s != "1" {
		t.Fatalf("score = %q, want 1", s)
	}
	expect(t, rw, "*2")
	if m := readBulk(t, rw); m != "b" {
		t.Fatalf("member = %q, want b", m)
	}
	if s := readBulk(t, rw); s != "2" {
		t.Fatalf("score = %q, want 2", s)
	}
}

// BZMPOP MAX with no COUNT pops the single highest scorer.
func TestBZMPopMax(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "9", "top", "3", "c")
	expect(t, rw, ":3")

	cmd(t, rw, "BZMPOP", "0", "1", "z", "MAX")
	expect(t, rw, "*2")
	if k := readBulk(t, rw); k != "z" {
		t.Fatalf("key = %q, want z", k)
	}
	expect(t, rw, "*1")
	expect(t, rw, "*2")
	if m := readBulk(t, rw); m != "top" {
		t.Fatalf("member = %q, want top", m)
	}
	if s := readBulk(t, rw); s != "9" {
		t.Fatalf("score = %q, want 9", s)
	}
}

// BZMPOP on all-empty keys returns a null array after its timeout.
func TestBZMPopTimeout(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "BZMPOP", "0.15", "2", "a", "b", "MIN")
	expect(t, rw, "*-1")
}

// A blocked BZMPOP wakes on a ZADD to any of its keys.
func TestBZMPopWakeup(t *testing.T) {
	rw, pusher, cleanup := dialTwoGo(t)
	defer cleanup()

	done := make(chan string, 1)
	go func() {
		cmd(t, rw, "BZMPOP", "5", "2", "k1", "k2", "MIN")
		done <- readReply(t, rw) // *2 header on success
	}()

	time.Sleep(50 * time.Millisecond)
	cmd(t, pusher, "ZADD", "k2", "1", "m")
	expect(t, pusher, ":1")

	select {
	case hdr := <-done:
		if hdr != "*2" {
			t.Fatalf("woken header = %q, want *2", hdr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BZMPOP did not wake on ZADD")
	}
}

// The blocking zset pops reject bad arguments the same way Redis does, and error WRONGTYPE on
// a string key encountered in scan order.
func TestBZPopErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "BZPOPMIN", "onlykey")
	expect(t, rw, "-ERR wrong number of arguments for 'bzpopmin' command")
	cmd(t, rw, "BZPOPMIN")
	expect(t, rw, "-ERR wrong number of arguments for 'bzpopmin' command")
	cmd(t, rw, "BZPOPMIN", "k", "notanumber")
	expect(t, rw, "-ERR timeout is not a float or out of range")
	cmd(t, rw, "BZPOPMIN", "k", "-1")
	expect(t, rw, "-ERR timeout is negative")

	cmd(t, rw, "SET", "str", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "BZPOPMIN", "str", "0")
	expect(t, rw, "-"+wrongType)
	// A string in scan order errors before a later servable key is reached.
	cmd(t, rw, "ZADD", "good", "1", "m")
	expect(t, rw, ":1")
	cmd(t, rw, "BZPOPMIN", "str", "good", "0")
	expect(t, rw, "-"+wrongType)

	// BZMPOP argument surface.
	cmd(t, rw, "BZMPOP", "0", "0", "MIN")
	expect(t, rw, "-ERR wrong number of arguments for 'bzmpop' command")
	cmd(t, rw, "BZMPOP", "0", "0", "k", "MIN")
	expect(t, rw, "-ERR numkeys should be greater than 0")
	cmd(t, rw, "BZMPOP", "0", "xx", "k", "MIN")
	expect(t, rw, "-ERR numkeys should be greater than 0")
	cmd(t, rw, "BZMPOP", "notanum", "1", "k", "MIN")
	expect(t, rw, "-ERR timeout is not a float or out of range")
	cmd(t, rw, "BZMPOP", "0", "1", "k", "BOGUS")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "BZMPOP", "0", "1", "z", "MIN", "COUNT", "0")
	expect(t, rw, "-ERR count should be greater than 0")
	cmd(t, rw, "BZMPOP", "0", "1", "z", "MIN", "COUNT", "1", "EXTRA")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "BZMPOP", "0", "1", "str", "MIN")
	expect(t, rw, "-"+wrongType)
}
