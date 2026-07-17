package sqlo1

import (
	"bufio"
	"net"
	"sync"
	"testing"
	"time"
)

// startServerMulti runs one Server and returns a dialer, so blocking
// tests can hold several clients against the same data.
func startServerMulti(t *testing.T) func() (net.Conn, *bufio.Reader) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	srv, err := NewServer(NewMemStore())
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	return func() (net.Conn, *bufio.Reader) {
		t.Helper()
		c, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		c.SetDeadline(time.Now().Add(10 * time.Second))
		return c, bufio.NewReader(c)
	}
}

func TestServerZPopSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	expect(t, r, ":5\r\n")

	// ZPOPMIN and ZPOPMAX: one by default, count pops that many
	// nearest-end-first, over-count drains, absent key empties.
	send("ZPOPMIN", "z")
	expect(t, r, respArr("a", "1"))
	send("ZPOPMAX", "z")
	expect(t, r, respArr("e", "5"))
	send("ZPOPMIN", "z", "2")
	expect(t, r, respArr("b", "2", "c", "3"))
	send("ZPOPMAX", "z", "10")
	expect(t, r, respArr("d", "4"))
	send("ZPOPMIN", "z")
	expect(t, r, "*0\r\n")
	send("TYPE", "z")
	expect(t, r, "+none\r\n")
	send("ZPOPMIN", "nokey", "3")
	expect(t, r, "*0\r\n")
	send("ZPOPMIN", "z", "0")
	expect(t, r, "*0\r\n")

	// The pop doors: the family's own syntax text past one count, the
	// positive-range text for a bad or negative count.
	send("ZPOPMIN", "z", "2", "3")
	expect(t, r, "-ERR syntax error, ZPOPMIN/ZPOPMAX only support a single count argument\r\n")
	send("ZPOPMAX", "z", "-1")
	expect(t, r, "-ERR value is out of range, must be positive\r\n")
	send("ZPOPMIN", "z", "x")
	expect(t, r, "-ERR value is out of range, must be positive\r\n")

	// Scores print Redis-style: integral as integers, else shortest.
	send("ZADD", "zf", "1.5", "p", "-2", "q")
	expect(t, r, ":2\r\n")
	send("ZPOPMIN", "zf", "2")
	expect(t, r, respArr("q", "-2", "p", "1.5"))

	// ZMPOP: first non-empty key answers with [key, pairs], COUNT
	// caps, no key ready is the null array.
	send("ZADD", "m2", "1", "a", "2", "b", "3", "c")
	expect(t, r, ":3\r\n")
	send("ZMPOP", "2", "m1", "m2", "MIN")
	expect(t, r, "*2\r\n$2\r\nm2\r\n*1\r\n"+respArr("a", "1"))
	send("ZMPOP", "2", "m1", "m2", "MAX", "COUNT", "5")
	expect(t, r, "*2\r\n$2\r\nm2\r\n*2\r\n"+respArr("c", "3")+respArr("b", "2"))
	send("ZMPOP", "2", "m1", "m2", "MIN")
	expect(t, r, "*-1\r\n")

	// The ZMPOP doors: numkeys, the direction token, COUNT.
	send("ZMPOP", "0", "m1", "MIN")
	expect(t, r, "-ERR numkeys should be greater than 0\r\n")
	send("ZMPOP", "x", "m1", "MIN")
	expect(t, r, "-ERR numkeys should be greater than 0\r\n")
	send("ZMPOP", "2", "m1", "MIN")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZMPOP", "1", "m1", "MID")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZMPOP", "1", "m1", "MIN", "COUNT", "0")
	expect(t, r, "-ERR count should be greater than 0\r\n")
	send("ZMPOP", "1", "m1", "MIN", "COUNT", "x")
	expect(t, r, "-ERR count should be greater than 0\r\n")
	send("ZMPOP", "1", "m1", "MIN", "EXTRA")
	expect(t, r, "-ERR syntax error\r\n")

	// WRONGTYPE across the family.
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("ZPOPMIN", "str")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZMPOP", "1", "str", "MIN")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZRANDMEMBER", "str")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("BZPOPMIN", "str", "0.01")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("BZMPOP", "0.01", "1", "str", "MAX")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

func TestServerZRandMemberSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("ZADD", "z", "1", "only")
	expect(t, r, ":1\r\n")

	// Single form: a bulk from the set, nil on a missing key.
	send("ZRANDMEMBER", "z")
	expect(t, r, "$4\r\nonly\r\n")
	send("ZRANDMEMBER", "nokey")
	expect(t, r, "$-1\r\n")

	// Count form: positive distinct caps at the cardinality, negative
	// draws with replacement, WITHSCORES interleaves, a missing key
	// empties.
	send("ZRANDMEMBER", "z", "5")
	expect(t, r, respArr("only"))
	send("ZRANDMEMBER", "z", "3", "WITHSCORES")
	expect(t, r, respArr("only", "1"))
	send("ZRANDMEMBER", "z", "-3")
	expect(t, r, respArr("only", "only", "only"))
	send("ZRANDMEMBER", "z", "-2", "WITHSCORES")
	expect(t, r, respArr("only", "1", "only", "1"))
	send("ZRANDMEMBER", "nokey", "3")
	expect(t, r, "*0\r\n")
	send("ZRANDMEMBER", "z", "0")
	expect(t, r, "*0\r\n")

	// The doors: the integer text for a bad count, the range text for
	// the unnegatable minimum, syntax for a bad fourth argument.
	send("ZRANDMEMBER", "z", "x")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("ZRANDMEMBER", "z", "-9223372036854775808")
	expect(t, r, "-ERR value is out of range\r\n")
	send("ZRANDMEMBER", "z", "2", "NOPE")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZRANDMEMBER", "z", "2", "WITHSCORES", "MORE")
	expect(t, r, "-ERR syntax error\r\n")
}

func TestServerBZPopTimeouts(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	// Present data serves immediately, no blocking.
	send("ZADD", "z", "1", "a", "2", "b")
	expect(t, r, ":2\r\n")
	send("BZPOPMIN", "z", "0")
	expect(t, r, respArr("z", "a", "1"))
	send("BZPOPMAX", "nokey", "z", "0")
	expect(t, r, respArr("z", "b", "2"))
	send("ZADD", "z", "3", "c")
	expect(t, r, ":1\r\n")
	send("BZMPOP", "0", "2", "nokey", "z", "MIN")
	expect(t, r, "*2\r\n$1\r\nz\r\n*1\r\n"+respArr("c", "3"))

	// An empty wait lapses to the null array at the deadline.
	start := time.Now()
	send("BZPOPMIN", "nokey", "0.1")
	expect(t, r, "*-1\r\n")
	if el := time.Since(start); el < 80*time.Millisecond {
		t.Fatalf("timeout lapsed after %v, before the deadline", el)
	}
	send("BZMPOP", "0.1", "1", "nokey", "MAX", "COUNT", "2")
	expect(t, r, "*-1\r\n")

	// The timeout doors.
	send("BZPOPMIN", "z", "x")
	expect(t, r, "-ERR timeout is not a float or out of range\r\n")
	send("BZPOPMIN", "z", "nan")
	expect(t, r, "-ERR timeout is not a float or out of range\r\n")
	send("BZPOPMIN", "z", "-1")
	expect(t, r, "-ERR timeout is negative\r\n")
	send("BZMPOP", "-0.5", "1", "z", "MIN")
	expect(t, r, "-ERR timeout is negative\r\n")
	send("BZMPOP", "0", "0", "z", "MIN")
	expect(t, r, "-ERR numkeys should be greater than 0\r\n")
}

func TestServerBZPopBlocking(t *testing.T) {
	dial := startServerMulti(t)
	send := func(c net.Conn, args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	// A waiter blocks until another client's ZADD wakes it.
	waiter, wr := dial()
	pusher, pr := dial()
	send(waiter, "BZPOPMIN", "bk", "5")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "ZADD", "bk", "7", "w")
	expect(t, pr, ":1\r\n")
	expect(t, wr, respArr("bk", "w", "7"))
	send(pusher, "TYPE", "bk")
	expect(t, pr, "+none\r\n")

	// Two waiters on one key: the longer-blocked one wins the single
	// added member, the other lapses.
	w1, r1 := dial()
	w2, r2 := dial()
	send(w1, "BZPOPMAX", "fifo", "5")
	time.Sleep(100 * time.Millisecond)
	send(w2, "BZPOPMAX", "fifo", "0.5")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "ZADD", "fifo", "3", "one")
	expect(t, pr, ":1\r\n")
	expect(t, r1, respArr("fifo", "one", "3"))
	expect(t, r2, "*-1\r\n")

	// BZMPOP wakes on data across its key list and takes the batch.
	w3, r3 := dial()
	send(w3, "BZMPOP", "5", "2", "mk1", "mk2", "MIN", "COUNT", "2")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "ZADD", "mk2", "1", "a", "2", "b", "3", "c")
	expect(t, pr, ":3\r\n")
	expect(t, r3, "*2\r\n$3\r\nmk2\r\n*2\r\n"+respArr("a", "1")+respArr("b", "2"))

	// A key turning the wrong type wakes the waiter into the error.
	w4, r4 := dial()
	send(w4, "BZPOPMIN", "turnstr", "5")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "SET", "turnstr", "v")
	expect(t, pr, "+OK\r\n")
	expect(t, r4, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

// TestServerBZPopManyWaiters hammers one key with waiters and members
// under race: every added member goes to exactly one waiter.
func TestServerBZPopManyWaiters(t *testing.T) {
	dial := startServerMulti(t)
	const n = 8
	got := make(chan string, n)
	var wg sync.WaitGroup
	for range n {
		c, r := dial()
		if _, err := c.Write([]byte(respCmd("BZPOPMIN", "swarm", "8"))); err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(r *bufio.Reader) {
			defer wg.Done()
			line, err := r.ReadString('\n')
			if err != nil {
				t.Errorf("waiter read: %v", err)
				return
			}
			if line != "*3\r\n" {
				t.Errorf("waiter reply header %q", line)
				return
			}
			var member string
			for i := range 3 {
				if _, err := r.ReadString('\n'); err != nil {
					t.Errorf("waiter read: %v", err)
					return
				}
				body, err := r.ReadString('\n')
				if err != nil {
					t.Errorf("waiter read: %v", err)
					return
				}
				if i == 1 {
					member = body[:len(body)-2]
				}
			}
			got <- member
		}(r)
	}
	time.Sleep(150 * time.Millisecond)

	pusher, pr := dial()
	for i := range n {
		if _, err := pusher.Write([]byte(respCmd("ZADD", "swarm", "1", string(rune('a'+i))))); err != nil {
			t.Fatal(err)
		}
		expect(t, pr, ":1\r\n")
	}
	wg.Wait()
	close(got)
	seen := map[string]bool{}
	for m := range got {
		if seen[m] {
			t.Fatalf("member %q served twice", m)
		}
		seen[m] = true
	}
	if len(seen) != n {
		t.Fatalf("%d members served, want %d", len(seen), n)
	}
}
