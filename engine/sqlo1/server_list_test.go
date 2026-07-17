package sqlo1

import (
	"fmt"
	"net"
	"strings"
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

// TestServerListPositionalSurface is the inline-tier wire surface for
// LLEN, LINDEX, LSET, and LRANGE: the index and clamp grammar, the two
// LSET error doors, and the shared parser and type doors.
func TestServerListPositionalSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("RPUSH", "l", "a", "b", "c", "d", "e")
	expect(t, r, ":5\r\n")
	send("LLEN", "l")
	expect(t, r, ":5\r\n")
	send("LLEN", "nope")
	expect(t, r, ":0\r\n")

	// LINDEX both signs; out of range either way is the nil bulk.
	send("LINDEX", "l", "0")
	expect(t, r, "$1\r\na\r\n")
	send("LINDEX", "l", "4")
	expect(t, r, "$1\r\ne\r\n")
	send("LINDEX", "l", "-1")
	expect(t, r, "$1\r\ne\r\n")
	send("LINDEX", "l", "-5")
	expect(t, r, "$1\r\na\r\n")
	send("LINDEX", "l", "5")
	expect(t, r, "$-1\r\n")
	send("LINDEX", "l", "-6")
	expect(t, r, "$-1\r\n")
	send("LINDEX", "nope", "0")
	expect(t, r, "$-1\r\n")

	// LRANGE clamp grammar: full, windows, negatives, inverted and
	// past-the-end empties, and the over-wide clamp.
	send("LRANGE", "l", "0", "-1")
	expect(t, r, respArr("a", "b", "c", "d", "e"))
	send("LRANGE", "l", "1", "3")
	expect(t, r, respArr("b", "c", "d"))
	send("LRANGE", "l", "-2", "-1")
	expect(t, r, respArr("d", "e"))
	send("LRANGE", "l", "3", "1")
	expect(t, r, "*0\r\n")
	send("LRANGE", "l", "5", "10")
	expect(t, r, "*0\r\n")
	send("LRANGE", "l", "-100", "100")
	expect(t, r, respArr("a", "b", "c", "d", "e"))
	send("LRANGE", "nope", "0", "-1")
	expect(t, r, "*0\r\n")

	// LSET both signs, then its two error doors.
	send("LSET", "l", "1", "B")
	expect(t, r, "+OK\r\n")
	send("LSET", "l", "-1", "E")
	expect(t, r, "+OK\r\n")
	send("LRANGE", "l", "0", "-1")
	expect(t, r, respArr("a", "B", "c", "d", "E"))
	send("LSET", "l", "5", "x")
	expect(t, r, "-ERR index out of range\r\n")
	send("LSET", "l", "-6", "x")
	expect(t, r, "-ERR index out of range\r\n")
	send("LSET", "nope", "0", "x")
	expect(t, r, "-ERR no such key\r\n")

	// The parser doors: non-integer indexes and the arities.
	send("LINDEX", "l", "x")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("LSET", "l", "x", "v")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("LRANGE", "l", "0", "x")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("LLEN", "l", "x")
	expect(t, r, "-ERR wrong number of arguments for 'llen' command\r\n")
	send("LINDEX", "l")
	expect(t, r, "-ERR wrong number of arguments for 'lindex' command\r\n")
	send("LSET", "l", "0")
	expect(t, r, "-ERR wrong number of arguments for 'lset' command\r\n")
	send("LRANGE", "l", "0")
	expect(t, r, "-ERR wrong number of arguments for 'lrange' command\r\n")

	// WRONGTYPE across the four.
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	for _, cmd := range [][]string{
		{"LLEN", "str"}, {"LINDEX", "str", "0"},
		{"LSET", "str", "0", "v"}, {"LRANGE", "str", "0", "-1"},
	} {
		send(cmd...)
		expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	}
}

// TestServerListPositionalNoded runs the positional family over a
// 3000-element noded list: about 24 nodes, so the full-range walk
// crosses the 16-node prefetch round and the windows land mid-node.
func TestServerListPositionalNoded(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	const n = 3000
	elems := make([]string, n)
	for i := range elems {
		elems[i] = fmt.Sprintf("e%04d", i)
	}
	send(append([]string{"RPUSH", "big"}, elems...)...)
	expect(t, r, fmt.Sprintf(":%d\r\n", n))
	send("OBJECT", "ENCODING", "big")
	expect(t, r, "$9\r\nquicklist\r\n")
	send("LLEN", "big")
	expect(t, r, fmt.Sprintf(":%d\r\n", n))

	// Indexes across the node span, both signs, and the out-of-range
	// nil.
	send("LINDEX", "big", "0")
	expect(t, r, "$5\r\ne0000\r\n")
	send("LINDEX", "big", "1500")
	expect(t, r, "$5\r\ne1500\r\n")
	send("LINDEX", "big", "2999")
	expect(t, r, "$5\r\ne2999\r\n")
	send("LINDEX", "big", "-1")
	expect(t, r, "$5\r\ne2999\r\n")
	send("LINDEX", "big", "-3000")
	expect(t, r, "$5\r\ne0000\r\n")
	send("LINDEX", "big", "3000")
	expect(t, r, "$-1\r\n")

	// A mid-node window, a clamped tail window, and the full walk
	// across two prefetch rounds.
	send("LRANGE", "big", "130", "134")
	expect(t, r, respArr("e0130", "e0131", "e0132", "e0133", "e0134"))
	send("LRANGE", "big", "2995", "4000")
	expect(t, r, respArr("e2995", "e2996", "e2997", "e2998", "e2999"))
	send("LRANGE", "big", "0", "-1")
	expect(t, r, respArr(elems...))

	// LSET mid-list with a far larger element: the touched node grows
	// in place, the neighbors and the count hold.
	big := strings.Repeat("B", 500)
	send("LSET", "big", "1500", big)
	expect(t, r, "+OK\r\n")
	send("LINDEX", "big", "1500")
	expect(t, r, fmt.Sprintf("$%d\r\n%s\r\n", len(big), big))
	send("LRANGE", "big", "1499", "1501")
	expect(t, r, respArr("e1499", big, "e1501"))
	send("LLEN", "big")
	expect(t, r, fmt.Sprintf(":%d\r\n", n))

	// An inline list whose LSET replacement is too big for the inline
	// root upgrades to quicklist with everything else in place.
	send("RPUSH", "up", "a", "b", "c")
	expect(t, r, ":3\r\n")
	send("OBJECT", "ENCODING", "up")
	expect(t, r, "$8\r\nlistpack\r\n")
	huge := strings.Repeat("H", 2100)
	send("LSET", "up", "1", huge)
	expect(t, r, "+OK\r\n")
	send("OBJECT", "ENCODING", "up")
	expect(t, r, "$9\r\nquicklist\r\n")
	send("LRANGE", "up", "0", "-1")
	expect(t, r, respArr("a", huge, "c"))
	send("LLEN", "up")
	expect(t, r, ":3\r\n")
}

// TestServerListTrim is LTRIM over the wire: the clamp grammar on both
// tiers, the always-OK contract, the empty-window key delete, and the
// doors.
func TestServerListTrim(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("RPUSH", "l", "a", "b", "c", "d", "e")
	expect(t, r, ":5\r\n")
	send("LTRIM", "l", "1", "-2")
	expect(t, r, "+OK\r\n")
	send("LRANGE", "l", "0", "-1")
	expect(t, r, respArr("b", "c", "d"))
	send("LTRIM", "l", "-100", "100")
	expect(t, r, "+OK\r\n")
	send("LLEN", "l")
	expect(t, r, ":3\r\n")

	// The empty window deletes the key; a missing key is still OK.
	send("LTRIM", "l", "5", "1")
	expect(t, r, "+OK\r\n")
	send("TYPE", "l")
	expect(t, r, "+none\r\n")
	send("LTRIM", "l", "0", "-1")
	expect(t, r, "+OK\r\n")

	// A noded list trims across node boundaries and stays exact.
	const n = 3000
	elems := make([]string, n)
	for i := range elems {
		elems[i] = fmt.Sprintf("e%04d", i)
	}
	send(append([]string{"RPUSH", "big"}, elems...)...)
	expect(t, r, fmt.Sprintf(":%d\r\n", n))
	send("LTRIM", "big", "1000", "1004")
	expect(t, r, "+OK\r\n")
	send("LRANGE", "big", "0", "-1")
	expect(t, r, respArr("e1000", "e1001", "e1002", "e1003", "e1004"))
	send("LLEN", "big")
	expect(t, r, ":5\r\n")

	// The capped-feed shape: push then trim to the cap, every op.
	send("DEL", "feed")
	expect(t, r, ":0\r\n")
	for i := range 10 {
		send("LPUSH", "feed", fmt.Sprintf("f%d", i))
		expect(t, r, fmt.Sprintf(":%d\r\n", min(i+1, 5)))
		send("LTRIM", "feed", "0", "3")
		expect(t, r, "+OK\r\n")
	}
	send("LRANGE", "feed", "0", "-1")
	expect(t, r, respArr("f9", "f8", "f7", "f6"))

	// The doors: parse, arity, wrong type.
	send("LTRIM", "big", "x", "1")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("LTRIM", "big", "0")
	expect(t, r, "-ERR wrong number of arguments for 'ltrim' command\r\n")
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("LTRIM", "str", "0", "1")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

// TestServerListScan is the wire grammar for LINSERT, LREM, and LPOS:
// both directions, rank skips, COUNT and MAXLEN shapes, the option
// errors word for word, and the doors.
func TestServerListScan(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("RPUSH", "l", "a", "b", "c", "b", "a", "b")
	expect(t, r, ":6\r\n")

	// LPOS: first match, rank skips, both directions, COUNT, MAXLEN.
	send("LPOS", "l", "b")
	expect(t, r, ":1\r\n")
	send("LPOS", "l", "b", "RANK", "2")
	expect(t, r, ":3\r\n")
	send("LPOS", "l", "b", "RANK", "-1")
	expect(t, r, ":5\r\n")
	send("LPOS", "l", "b", "COUNT", "0")
	expect(t, r, "*3\r\n:1\r\n:3\r\n:5\r\n")
	send("LPOS", "l", "b", "RANK", "-1", "COUNT", "2")
	expect(t, r, "*2\r\n:5\r\n:3\r\n")
	send("LPOS", "l", "a", "MAXLEN", "1")
	expect(t, r, ":0\r\n")
	send("LPOS", "l", "b", "MAXLEN", "1")
	expect(t, r, "$-1\r\n")
	send("LPOS", "l", "nope")
	expect(t, r, "$-1\r\n")
	send("LPOS", "l", "nope", "COUNT", "0")
	expect(t, r, "*0\r\n")
	send("LPOS", "missing", "x")
	expect(t, r, "$-1\r\n")
	send("LPOS", "missing", "x", "COUNT", "2")
	expect(t, r, "*0\r\n")

	// The option errors, word for word.
	send("LPOS", "l", "b", "RANK", "0")
	expect(t, r, "-ERR RANK can't be zero. Use 1 to start searching from the first matching element in the head, or a negative rank to start searching backward from the tail.\r\n")
	send("LPOS", "l", "b", "COUNT", "-1")
	expect(t, r, "-ERR COUNT can't be negative\r\n")
	send("LPOS", "l", "b", "MAXLEN", "-1")
	expect(t, r, "-ERR MAXLEN can't be negative\r\n")
	send("LPOS", "l", "b", "RANK", "x")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("LPOS", "l", "b", "BOGUS", "1")
	expect(t, r, "-ERR syntax error\r\n")
	send("LPOS", "l", "b", "RANK")
	expect(t, r, "-ERR syntax error\r\n")

	// LINSERT: before, after, missing pivot, missing key.
	send("LINSERT", "l", "BEFORE", "c", "x")
	expect(t, r, ":7\r\n")
	send("LINSERT", "l", "after", "c", "y")
	expect(t, r, ":8\r\n")
	send("LRANGE", "l", "0", "-1")
	expect(t, r, respArr("a", "b", "x", "c", "y", "b", "a", "b"))
	send("LINSERT", "l", "BEFORE", "nope", "z")
	expect(t, r, ":-1\r\n")
	send("LINSERT", "missing", "BEFORE", "a", "b")
	expect(t, r, ":0\r\n")
	send("LINSERT", "l", "SIDEWAYS", "a", "b")
	expect(t, r, "-ERR syntax error\r\n")

	// LREM: head-directed, tail-directed, all, and a miss.
	send("LREM", "l", "1", "b")
	expect(t, r, ":1\r\n")
	send("LRANGE", "l", "0", "-1")
	expect(t, r, respArr("a", "x", "c", "y", "b", "a", "b"))
	send("LREM", "l", "-1", "b")
	expect(t, r, ":1\r\n")
	send("LRANGE", "l", "0", "-1")
	expect(t, r, respArr("a", "x", "c", "y", "b", "a"))
	send("LREM", "l", "0", "a")
	expect(t, r, ":2\r\n")
	send("LRANGE", "l", "0", "-1")
	expect(t, r, respArr("x", "c", "y", "b"))
	send("LREM", "l", "3", "nope")
	expect(t, r, ":0\r\n")
	send("LREM", "missing", "0", "a")
	expect(t, r, ":0\r\n")

	// Removing the last element deletes the key.
	send("LREM", "l", "0", "x")
	expect(t, r, ":1\r\n")
	send("LREM", "l", "0", "c")
	expect(t, r, ":1\r\n")
	send("LREM", "l", "0", "y")
	expect(t, r, ":1\r\n")
	send("LREM", "l", "0", "b")
	expect(t, r, ":1\r\n")
	send("TYPE", "l")
	expect(t, r, "+none\r\n")

	// A noded list scans the same grammar across node boundaries.
	const n = 300
	elems := make([]string, n)
	for i := range elems {
		elems[i] = fmt.Sprintf("v%02d", i%7)
	}
	send(append([]string{"RPUSH", "big"}, elems...)...)
	expect(t, r, fmt.Sprintf(":%d\r\n", n))
	send("OBJECT", "ENCODING", "big")
	expect(t, r, "$9\r\nquicklist\r\n")
	send("LPOS", "big", "v03", "RANK", "-1")
	expect(t, r, ":297\r\n")
	send("LPOS", "big", "v03", "COUNT", "3")
	expect(t, r, "*3\r\n:3\r\n:10\r\n:17\r\n")
	send("LINSERT", "big", "AFTER", "v06", "mark")
	expect(t, r, ":301\r\n")
	send("LRANGE", "big", "5", "8")
	expect(t, r, respArr("v05", "v06", "mark", "v00"))
	send("LREM", "big", "0", "v01")
	expect(t, r, ":43\r\n")
	send("LLEN", "big")
	expect(t, r, ":258\r\n")

	// The doors: arity and wrong type.
	send("LREM", "big", "0")
	expect(t, r, "-ERR wrong number of arguments for 'lrem' command\r\n")
	send("LINSERT", "big", "BEFORE", "p")
	expect(t, r, "-ERR wrong number of arguments for 'linsert' command\r\n")
	send("LPOS", "big")
	expect(t, r, "-ERR wrong number of arguments for 'lpos' command\r\n")
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	for _, cmd := range [][]string{
		{"LPOS", "str", "a"},
		{"LINSERT", "str", "BEFORE", "a", "b"},
		{"LREM", "str", "0", "a"},
	} {
		send(cmd...)
		expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	}
}
