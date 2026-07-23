package sqlo1

import (
	"net"
	"strconv"
	"testing"
	"time"
)

// xrow is one XREAD reply row: the two-array of key and entry array.
func xrow(key string, ents ...string) string {
	return "*2\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n" + xarr(ents...)
}

// TestXreadWire pins the XREAD surface against Redis 8.8: the parse
// ladder with XREAD's own unbalanced text, the per-key type-then-ID
// order, the exclusive bare-ms start, the $ and + special IDs, and
// the empty-stream omission were all recorded live before landing.
func TestXreadWire(t *testing.T) {
	do, _ := dispatchServer(t)
	do("XADD", "s", "5-1", "f", "v1")
	do("XADD", "s", "5-3", "f", "v2")
	do("XADD", "s", "7-0", "f", "v3")
	do("SET", "str", "x")

	errs := []struct {
		args []string
		want string
	}{
		{[]string{"XREAD"}, "-ERR wrong number of arguments for 'xread' command\r\n"},
		{[]string{"XREAD", "STREAMS"}, "-ERR wrong number of arguments for 'xread' command\r\n"},
		{[]string{"XREAD", "STREAMS", "s"}, "-ERR wrong number of arguments for 'xread' command\r\n"},
		{[]string{"XREAD", "STREAMS", "s", "0", "extra"},
			"-ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.\r\n"},
		{[]string{"XREAD", "STREAMS", "s1", "s2", "0"},
			"-ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.\r\n"},
		{[]string{"XREAD", "FOO", "STREAMS", "s", "0"}, "-ERR syntax error\r\n"},
		{[]string{"XREAD", "COUNT", "1", "BLOCK", "10"}, "-ERR syntax error\r\n"},
		{[]string{"XREAD", "BLOCK", "10", "STREAMS"}, "-ERR syntax error\r\n"},
		{[]string{"XREAD", "COUNT", "abc", "STREAMS", "s", "0"},
			"-ERR value is not an integer or out of range\r\n"},
		{[]string{"XREAD", "BLOCK", "abc", "STREAMS", "s", "0"},
			"-ERR timeout is not an integer or out of range\r\n"},
		{[]string{"XREAD", "BLOCK", "-1", "STREAMS", "s", "0"},
			"-ERR timeout is negative\r\n"},
		{[]string{"XREAD", "STREAMS", "s", "-"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XREAD", "STREAMS", "s", "(5"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
		// Keys resolve one at a time with the type check before that
		// key's ID parse, so the error follows the leftmost bad key.
		{[]string{"XREAD", "STREAMS", "str", "0"},
			"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XREAD", "STREAMS", "str", "notanid"},
			"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XREAD", "STREAMS", "s", "notanid", "str", "0"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XREAD", "STREAMS", "str", "0", "s", "notanid"},
			"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		// A blocked read on a wrong-typed key errors immediately.
		{[]string{"XREAD", "BLOCK", "100", "STREAMS", "str", "$"},
			"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
	}
	for _, tc := range errs {
		if got := do(tc.args...); got != tc.want {
			t.Fatalf("%v = %q, want %q", tc.args, got, tc.want)
		}
	}

	// The exclusive start: a bare ms reads as ms-0, an explicit ID
	// serves strictly after itself, the tail answers the null array.
	e1, e2, e3 := xent("5-1", "f", "v1"), xent("5-3", "f", "v2"), xent("7-0", "f", "v3")
	if got := do("XREAD", "STREAMS", "s", "0"); got != "*1\r\n"+xrow("s", e1, e2, e3) {
		t.Fatal(got)
	}
	if got := do("XREAD", "STREAMS", "s", "5"); got != "*1\r\n"+xrow("s", e1, e2, e3) {
		t.Fatal(got)
	}
	if got := do("XREAD", "STREAMS", "s", "5-1"); got != "*1\r\n"+xrow("s", e2, e3) {
		t.Fatal(got)
	}
	if got := do("XREAD", "STREAMS", "s", "7-0"); got != "*-1\r\n" {
		t.Fatal(got)
	}
	if got := do("XREAD", "STREAMS", "s", "18446744073709551615-18446744073709551615"); got != "*-1\r\n" {
		t.Fatal(got)
	}

	// COUNT caps per stream; zero and negatives read as unlimited, and
	// a repeated option's last value wins, all lowercase-tolerant.
	if got := do("XREAD", "COUNT", "2", "STREAMS", "s", "0"); got != "*1\r\n"+xrow("s", e1, e2) {
		t.Fatal(got)
	}
	if got := do("XREAD", "COUNT", "0", "STREAMS", "s", "0"); got != "*1\r\n"+xrow("s", e1, e2, e3) {
		t.Fatal(got)
	}
	if got := do("XREAD", "COUNT", "-3", "STREAMS", "s", "0"); got != "*1\r\n"+xrow("s", e1, e2, e3) {
		t.Fatal(got)
	}
	if got := do("XREAD", "count", "2", "count", "1", "streams", "s", "0"); got != "*1\r\n"+xrow("s", e1) {
		t.Fatal(got)
	}

	// $ answers nothing without BLOCK, + answers the newest live
	// entry, and both read a missing key as empty.
	if got := do("XREAD", "STREAMS", "s", "$"); got != "*-1\r\n" {
		t.Fatal(got)
	}
	if got := do("XREAD", "STREAMS", "s", "+"); got != "*1\r\n"+xrow("s", e3) {
		t.Fatal(got)
	}
	if got := do("XREAD", "COUNT", "5", "STREAMS", "s", "+"); got != "*1\r\n"+xrow("s", e3) {
		t.Fatal(got)
	}
	for _, id := range []string{"$", "+", "0"} {
		if got := do("XREAD", "STREAMS", "nosuch", id); got != "*-1\r\n" {
			t.Fatalf("id %s: %s", id, got)
		}
	}

	// Multi-key: empty streams drop their rows, served ones keep the
	// argument order, and a duplicate key renders twice.
	do("XADD", "s2", "9-0", "g", "w")
	e4 := xent("9-0", "g", "w")
	if got := do("XREAD", "STREAMS", "s", "s2", "5-3", "0"); got != "*2\r\n"+xrow("s", e3)+xrow("s2", e4) {
		t.Fatal(got)
	}
	if got := do("XREAD", "STREAMS", "nosuch", "s2", "0", "0"); got != "*1\r\n"+xrow("s2", e4) {
		t.Fatal(got)
	}
	if got := do("XREAD", "STREAMS", "s2", "s2", "0", "0"); got != "*2\r\n"+xrow("s2", e4)+xrow("s2", e4) {
		t.Fatal(got)
	}

	// An emptied stream keeps its generated ID: + falls back to it
	// like $, so neither serves until a genuinely new entry lands.
	do("XADD", "e", "5-5", "f", "v")
	do("XTRIM", "e", "MINID", "6")
	if got := do("XREAD", "STREAMS", "e", "+"); got != "*-1\r\n" {
		t.Fatal(got)
	}
	if got := do("XREAD", "STREAMS", "e", "$"); got != "*-1\r\n" {
		t.Fatal(got)
	}
	do("XADD", "e", "6-6", "f", "w")
	if got := do("XREAD", "STREAMS", "e", "+"); got != "*1\r\n"+xrow("e", xent("6-6", "f", "w")) {
		t.Fatal(got)
	}
}

// TestXreadgroupBlockWire pins the non-waiting halves of XREADGROUP
// BLOCK: a history ID never blocks, available entries serve
// immediately, NOGROUP errors immediately, and an empty > wait lapses
// to the null array at the deadline.
func TestXreadgroupBlockWire(t *testing.T) {
	do, _ := dispatchServer(t)
	do("XADD", "gs", "1-1", "f", "v")
	if got := do("XGROUP", "CREATE", "gs", "g", "0"); got != "+OK\r\n" {
		t.Fatal(got)
	}

	// A history ID renders its row immediately even under BLOCK.
	if got := do("XREADGROUP", "GROUP", "g", "c1", "BLOCK", "5000", "STREAMS", "gs", "0"); got != "*1\r\n"+xrow("gs") {
		t.Fatal(got)
	}
	// Available new entries serve immediately under BLOCK.
	if got := do("XREADGROUP", "GROUP", "g", "c1", "BLOCK", "5000", "STREAMS", "gs", ">"); got != "*1\r\n"+xrow("gs", xent("1-1", "f", "v")) {
		t.Fatal(got)
	}
	// NOGROUP outranks the wait.
	if got := do("XREADGROUP", "GROUP", "nog", "c1", "BLOCK", "5000", "STREAMS", "gs", ">"); got != "-NOGROUP No such key 'gs' or consumer group 'nog' in XREADGROUP with GROUP option\r\n" {
		t.Fatal(got)
	}
	// A mixed list with a history stream answers at once: the history
	// row renders, the empty > stream drops.
	if got := do("XREADGROUP", "GROUP", "g", "c1", "BLOCK", "5000", "STREAMS", "gs", "gs", ">", "0"); got != "*1\r\n"+xrow("gs", xent("1-1", "f", "v")) {
		t.Fatal(got)
	}
	// An all-new read with nothing to deliver waits out its timeout.
	start := time.Now()
	if got := do("XREADGROUP", "GROUP", "g", "c1", "BLOCK", "60", "STREAMS", "gs", ">"); got != "*-1\r\n" {
		t.Fatal(got)
	}
	if el := time.Since(start); el < 40*time.Millisecond {
		t.Fatalf("timeout lapsed after %v, before the deadline", el)
	}
}

// TestXreadBlocking drives real connections: wakes on XADD, the
// broadcast serve to every waiting reader, the frozen $ resolution,
// and the XREADGROUP FIFO hand-off with its PEL side effects.
func TestXreadBlocking(t *testing.T) {
	dial := startServerMulti(t)
	send := func(c net.Conn, args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}
	pusher, pr := dial()

	// BLOCK 0 waits forever and wakes on the write's dispatch-exit
	// broadcast; two readers on one stream both see the same entry.
	w1, r1 := dial()
	w2, r2 := dial()
	send(w1, "XREAD", "BLOCK", "0", "STREAMS", "bs", "$")
	send(w2, "XREAD", "BLOCK", "5000", "STREAMS", "bs", "$")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "XADD", "bs", "1-1", "f", "v")
	expect(t, pr, "$3\r\n1-1\r\n")
	want := "*1\r\n" + xrow("bs", xent("1-1", "f", "v"))
	expect(t, r1, want)
	expect(t, r2, want)

	// A multi-key wait serves the stream that got the data.
	w3, r3 := dial()
	send(w3, "XREAD", "BLOCK", "5000", "STREAMS", "bs", "bs2", "$", "$")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "XADD", "bs2", "2-2", "g", "w")
	expect(t, pr, "$3\r\n2-2\r\n")
	expect(t, r3, "*1\r\n"+xrow("bs2", xent("2-2", "g", "w")))

	// + on an empty stream falls back to the generated ID and blocks
	// until the first genuinely new entry.
	send(pusher, "XGROUP", "CREATE", "ps", "g", "$", "MKSTREAM")
	expect(t, pr, "+OK\r\n")
	w4, r4 := dial()
	send(w4, "XREAD", "BLOCK", "5000", "STREAMS", "ps", "+")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "XADD", "ps", "3-3", "f", "v")
	expect(t, pr, "$3\r\n3-3\r\n")
	expect(t, r4, "*1\r\n"+xrow("ps", xent("3-3", "f", "v")))

	// $ freezes at registration: a delete-and-refill below the frozen
	// ID cannot wake the reader, the pinned once-resolved rule.
	w5, r5 := dial()
	send(w5, "XREAD", "BLOCK", "600", "STREAMS", "bs", "$")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "DEL", "bs")
	expect(t, pr, ":1\r\n")
	send(pusher, "XADD", "bs", "0-5", "f", "lo")
	expect(t, pr, "$3\r\n0-5\r\n")
	expect(t, r5, "*-1\r\n")

	// An empty XREAD wait lapses to the null array at the deadline.
	w6, r6 := dial()
	start := time.Now()
	send(w6, "XREAD", "BLOCK", "100", "STREAMS", "quiet", "$")
	expect(t, r6, "*-1\r\n")
	if el := time.Since(start); el < 80*time.Millisecond {
		t.Fatalf("timeout lapsed after %v, before the deadline", el)
	}

	// Two blocked group consumers: the longer-blocked one takes the
	// first entry, the delivery lands in the PEL, and the next entry
	// goes to the next ticket.
	send(pusher, "XGROUP", "CREATE", "bg", "g", "$", "MKSTREAM")
	expect(t, pr, "+OK\r\n")
	g1, gr1 := dial()
	g2, gr2 := dial()
	send(g1, "XREADGROUP", "GROUP", "g", "c1", "BLOCK", "5000", "STREAMS", "bg", ">")
	time.Sleep(100 * time.Millisecond)
	send(g2, "XREADGROUP", "GROUP", "g", "c2", "BLOCK", "5000", "STREAMS", "bg", ">")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "XADD", "bg", "4-4", "f", "a")
	expect(t, pr, "$3\r\n4-4\r\n")
	expect(t, gr1, "*1\r\n"+xrow("bg", xent("4-4", "f", "a")))
	send(pusher, "XADD", "bg", "5-5", "f", "b")
	expect(t, pr, "$3\r\n5-5\r\n")
	expect(t, gr2, "*1\r\n"+xrow("bg", xent("5-5", "f", "b")))
	send(pusher, "XPENDING", "bg", "g")
	expect(t, pr, "*4\r\n:2\r\n$3\r\n4-4\r\n$3\r\n5-5\r\n*2\r\n*2\r\n$2\r\nc1\r\n$1\r\n1\r\n*2\r\n$2\r\nc2\r\n$1\r\n1\r\n")

	// A group destroyed mid-block unblocks its waiter with NOGROUP.
	g3, gr3 := dial()
	send(g3, "XREADGROUP", "GROUP", "g", "c3", "BLOCK", "5000", "STREAMS", "bg", ">")
	time.Sleep(100 * time.Millisecond)
	send(pusher, "XGROUP", "DESTROY", "bg", "g")
	expect(t, pr, ":1\r\n")
	expect(t, gr3, "-NOGROUP No such key 'bg' or consumer group 'g' in XREADGROUP with GROUP option\r\n")
}
