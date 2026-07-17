package sqlo1

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestServerListDequeSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	// Pushes land one at a time in argument order, so a multi-element
	// LPUSH reads back reversed; pops answer nearest-end-first.
	send("LPUSH", "l", "a", "b", "c")
	expect(t, r, ":3\r\n")
	send("RPUSH", "l", "d")
	expect(t, r, ":4\r\n")
	send("LPOP", "l")
	expect(t, r, "$1\r\nc\r\n")
	send("RPOP", "l")
	expect(t, r, "$1\r\nd\r\n")

	// A count answers an array; the pop that empties the list deletes
	// the key, and then the two shapes miss differently: nil bulk
	// without a count, null array with one.
	send("LPOP", "l", "2")
	expect(t, r, respArr("b", "a"))
	send("TYPE", "l")
	expect(t, r, "+none\r\n")
	send("LPOP", "l")
	expect(t, r, "$-1\r\n")
	send("RPOP", "l", "2")
	expect(t, r, "*-1\r\n")

	// The X variants leave a missing key missing and answer 0, and
	// work like the plain forms on a live one.
	send("LPUSHX", "l", "a")
	expect(t, r, ":0\r\n")
	send("RPUSHX", "l", "a")
	expect(t, r, ":0\r\n")
	send("TYPE", "l")
	expect(t, r, "+none\r\n")
	send("RPUSH", "l2", "a", "b")
	expect(t, r, ":2\r\n")
	send("LPUSHX", "l2", "z")
	expect(t, r, ":3\r\n")
	send("RPUSHX", "l2", "y")
	expect(t, r, ":4\r\n")
	send("LPOP", "l2")
	expect(t, r, "$1\r\nz\r\n")
	send("RPOP", "l2")
	expect(t, r, "$1\r\ny\r\n")

	// Count zero on a live key is the empty array, and the count
	// doors are Redis's: the positive-range text for bad or negative,
	// the arity text past one count.
	send("LPOP", "l2", "0")
	expect(t, r, "*0\r\n")
	send("LPOP", "l2", "-1")
	expect(t, r, "-ERR value is out of range, must be positive\r\n")
	send("RPOP", "l2", "x")
	expect(t, r, "-ERR value is out of range, must be positive\r\n")
	send("LPOP", "l2", "1", "2")
	expect(t, r, "-ERR wrong number of arguments for 'lpop' command\r\n")
	send("LPUSH", "l2")
	expect(t, r, "-ERR wrong number of arguments for 'lpush' command\r\n")
	send("LPUSHX", "l2")
	expect(t, r, "-ERR wrong number of arguments for 'lpushx' command\r\n")

	// WRONGTYPE across the family.
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("LPUSH", "str", "a")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("RPUSHX", "str", "a")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("LPOP", "str")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("RPOP", "str", "2")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

// TestServerListDequeNoded drives the surface across the noded
// upgrade: a big push answers quicklist, pops come back in order from
// both ends, and an over-count drain deletes the key.
func TestServerListDequeNoded(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	args := []string{"RPUSH", "big"}
	for i := range 200 {
		args = append(args, fmt.Sprintf("e%03d", i))
	}
	send(args...)
	expect(t, r, ":200\r\n")
	send("OBJECT", "ENCODING", "big")
	expect(t, r, "$9\r\nquicklist\r\n")

	send("LPOP", "big")
	expect(t, r, "$4\r\ne000\r\n")
	send("RPOP", "big")
	expect(t, r, "$4\r\ne199\r\n")
	want := make([]string, 0, 150)
	for i := 1; i <= 150; i++ {
		want = append(want, fmt.Sprintf("e%03d", i))
	}
	send("LPOP", "big", "150")
	expect(t, r, respArr(want...))

	// 48 remain; an over-count right pop drains tail-first and the
	// key dies.
	want = want[:0]
	for i := 198; i >= 151; i-- {
		want = append(want, fmt.Sprintf("e%03d", i))
	}
	send("RPOP", "big", "100")
	expect(t, r, respArr(want...))
	send("TYPE", "big")
	expect(t, r, "+none\r\n")
}

func TestServerLMPopSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	// The first non-empty key in listed order answers with [key,
	// elems], COUNT caps, no key ready is the null array.
	send("RPUSH", "m2", "a", "b", "c")
	expect(t, r, ":3\r\n")
	send("LMPOP", "2", "m1", "m2", "LEFT")
	expect(t, r, "*2\r\n$2\r\nm2\r\n"+respArr("a"))
	send("LMPOP", "2", "m1", "m2", "RIGHT", "COUNT", "5")
	expect(t, r, "*2\r\n$2\r\nm2\r\n"+respArr("c", "b"))
	send("LMPOP", "2", "m1", "m2", "LEFT")
	expect(t, r, "*-1\r\n")

	// The doors: numkeys, the direction token, COUNT.
	send("LMPOP", "0", "m1", "LEFT")
	expect(t, r, "-ERR numkeys should be greater than 0\r\n")
	send("LMPOP", "x", "m1", "LEFT")
	expect(t, r, "-ERR numkeys should be greater than 0\r\n")
	send("LMPOP", "2", "m1", "LEFT")
	expect(t, r, "-ERR syntax error\r\n")
	send("LMPOP", "1", "m1", "MID")
	expect(t, r, "-ERR syntax error\r\n")
	send("LMPOP", "1", "m1", "LEFT", "COUNT", "0")
	expect(t, r, "-ERR count should be greater than 0\r\n")
	send("LMPOP", "1", "m1", "LEFT", "COUNT", "x")
	expect(t, r, "-ERR count should be greater than 0\r\n")
	send("LMPOP", "1", "m1", "LEFT", "EXTRA")
	expect(t, r, "-ERR syntax error\r\n")

	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("LMPOP", "1", "str", "LEFT")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

func TestServerBListTimeouts(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	// Present data serves immediately, no blocking.
	send("RPUSH", "bl", "a", "b")
	expect(t, r, ":2\r\n")
	send("BLPOP", "bl", "0")
	expect(t, r, respArr("bl", "a"))
	send("BRPOP", "nokey", "bl", "0")
	expect(t, r, respArr("bl", "b"))
	send("RPUSH", "bl", "c")
	expect(t, r, ":1\r\n")
	send("BLMPOP", "0", "2", "nokey", "bl", "LEFT")
	expect(t, r, "*2\r\n$2\r\nbl\r\n"+respArr("c"))

	// An empty wait lapses to the null array at the deadline.
	start := time.Now()
	send("BLPOP", "nokey", "0.1")
	expect(t, r, "*-1\r\n")
	if el := time.Since(start); el < 80*time.Millisecond {
		t.Fatalf("timeout lapsed after %v, before the deadline", el)
	}
	send("BLMPOP", "0.1", "1", "nokey", "RIGHT", "COUNT", "2")
	expect(t, r, "*-1\r\n")

	// The timeout doors.
	send("BLPOP", "bl", "x")
	expect(t, r, "-ERR timeout is not a float or out of range\r\n")
	send("BLPOP", "bl", "-1")
	expect(t, r, "-ERR timeout is negative\r\n")
	send("BLMPOP", "-0.5", "1", "bl", "LEFT")
	expect(t, r, "-ERR timeout is negative\r\n")
	send("BLMPOP", "0", "0", "bl", "LEFT")
	expect(t, r, "-ERR numkeys should be greater than 0\r\n")

	// A blocked pop on a wrong-typed key errors immediately.
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("BLPOP", "str", "0.01")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("BLMPOP", "0.01", "1", "str", "LEFT")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

func TestServerBListBlocking(t *testing.T) {
	dial := startServerMulti(t)
	send := func(c net.Conn, args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	// A waiter blocks until another client's push wakes it; the
	// dispatch-exit broadcast is the only signal a push needs.
	waiter, wr := dial()
	pusher, pr := dial()
	send(waiter, "BLPOP", "bk", "5")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "RPUSH", "bk", "w")
	expect(t, pr, ":1\r\n")
	expect(t, wr, respArr("bk", "w"))
	send(pusher, "TYPE", "bk")
	expect(t, pr, "+none\r\n")

	// Two waiters on one key: the longer-blocked one wins the single
	// pushed element, the other lapses.
	w1, r1 := dial()
	w2, r2 := dial()
	send(w1, "BRPOP", "fifo", "5")
	time.Sleep(100 * time.Millisecond)
	send(w2, "BRPOP", "fifo", "0.5")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "RPUSH", "fifo", "one")
	expect(t, pr, ":1\r\n")
	expect(t, r1, respArr("fifo", "one"))
	expect(t, r2, "*-1\r\n")

	// BLMPOP wakes on data across its key list and takes the batch.
	w3, r3 := dial()
	send(w3, "BLMPOP", "5", "2", "mk1", "mk2", "LEFT", "COUNT", "2")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "RPUSH", "mk2", "a", "b", "c")
	expect(t, pr, ":3\r\n")
	expect(t, r3, "*2\r\n$3\r\nmk2\r\n"+respArr("a", "b"))

	// A key turning the wrong type wakes the waiter into the error.
	w4, r4 := dial()
	send(w4, "BLPOP", "turnstr", "5")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "SET", "turnstr", "v")
	expect(t, pr, "+OK\r\n")
	expect(t, r4, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}
