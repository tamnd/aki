package sqlo1

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// startServer runs a Server over MemStore on a loopback listener and
// returns a connected client plus a reader over its replies.
func startServer(t *testing.T) (net.Conn, *bufio.Reader) {
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

	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(10 * time.Second))
	return c, bufio.NewReader(c)
}

func expect(t *testing.T, r *bufio.Reader, want string) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatalf("reading reply (want %q): %v", want, err)
	}
	if string(got) != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
}

func TestServerCommandSurface(t *testing.T) {
	c, r := startServer(t)

	send := func(s string) {
		t.Helper()
		if _, err := c.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}

	send("*1\r\n$4\r\nPING\r\n")
	expect(t, r, "+PONG\r\n")

	send("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
	expect(t, r, "$5\r\nhello\r\n")

	send("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nval\r\n")
	expect(t, r, "+OK\r\n")

	send("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	expect(t, r, "$3\r\nval\r\n")

	send("*2\r\n$3\r\nGET\r\n$4\r\nnope\r\n")
	expect(t, r, "$-1\r\n")

	send("*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$3\r\n100\r\n")
	expect(t, r, ":1\r\n")

	send("*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n")
	expect(t, r, ":100\r\n")

	send("*2\r\n$3\r\nTTL\r\n$4\r\nnope\r\n")
	expect(t, r, ":-2\r\n")

	send("*3\r\n$3\r\nDEL\r\n$1\r\nk\r\n$4\r\nnope\r\n")
	expect(t, r, ":1\r\n")

	send("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	expect(t, r, "$-1\r\n")

	// Unknown command and wrong arity reply with errors, connection stays up.
	send("*1\r\n$5\r\nHELLO\r\n")
	expect(t, r, "-ERR unknown command 'HELLO'\r\n")
	send("*1\r\n$3\r\nGET\r\n")
	expect(t, r, "-ERR wrong number of arguments for 'get' command\r\n")
	send("*1\r\n$4\r\nPING\r\n")
	expect(t, r, "+PONG\r\n")
}

func TestServerPipelining(t *testing.T) {
	c, r := startServer(t)

	// One write, three commands; replies must come back in order.
	burst := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n" +
		"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n" +
		"*2\r\n$3\r\nGET\r\n$1\r\na\r\n"
	if _, err := c.Write([]byte(burst)); err != nil {
		t.Fatal(err)
	}
	expect(t, r, "+OK\r\n+OK\r\n$1\r\n1\r\n")
}

func TestServerInlineCommands(t *testing.T) {
	c, r := startServer(t)
	if _, err := c.Write([]byte("PING\r\nSET k v\r\nGET k\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, r, "+PONG\r\n+OK\r\n$1\r\nv\r\n")
}

func TestServerProtocolErrorCloses(t *testing.T) {
	c, r := startServer(t)
	if _, err := c.Write([]byte("*1\r\n:bad\r\n")); err != nil {
		t.Fatal(err)
	}
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(line, "-ERR Protocol error") {
		t.Fatalf("reply = %q, want a protocol error", line)
	}
	if _, err := r.ReadByte(); err != io.EOF {
		t.Fatalf("connection still open after protocol error, err = %v", err)
	}
}

// dispatchServer builds a Server over MemStore with a fake clock and
// returns a direct-dispatch helper, so command semantics are tested
// without sockets and expiry without sleeping.
func dispatchServer(t *testing.T) (do func(args ...string) string, clock *int64) {
	t.Helper()
	s, err := NewServer(NewMemStore())
	if err != nil {
		t.Fatal(err)
	}
	clock = new(int64)
	*clock = 1_000_000
	s.now = func() int64 { return *clock }
	return func(args ...string) string {
		bs := make([][]byte, len(args))
		for i, a := range args {
			bs[i] = []byte(a)
		}
		return string(s.dispatch(nil, bs))
	}, clock
}

// TestServerExpirySemantics drives dispatch directly with a fake clock, so
// lazy expiry is tested without sleeping.
func TestServerExpirySemantics(t *testing.T) {
	do, clockp := dispatchServer(t)

	if got := do("SET", "k", "v"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := do("TTL", "k"); got != ":-1\r\n" {
		t.Fatalf("TTL with no expiry = %q, want -1", got)
	}
	if got := do("EXPIRE", "k", "10"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("TTL", "k"); got != ":10\r\n" {
		t.Fatal(got)
	}

	*clockp += 9_500
	if got := do("TTL", "k"); got != ":1\r\n" {
		t.Fatalf("TTL rounds up, got %q", got)
	}

	*clockp += 501 // past the deadline
	if got := do("GET", "k"); got != "$-1\r\n" {
		t.Fatalf("expired key still readable: %q", got)
	}
	if got := do("TTL", "k"); got != ":-2\r\n" {
		t.Fatalf("TTL after lapse = %q, want -2", got)
	}
	if got := do("EXPIRE", "k", "10"); got != ":0\r\n" {
		t.Fatalf("EXPIRE on lapsed key = %q, want 0", got)
	}

	// EXPIRE with a non-positive ttl deletes, like Redis.
	do("SET", "k2", "v")
	if got := do("EXPIRE", "k2", "0"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("GET", "k2"); got != "$-1\r\n" {
		t.Fatalf("key survived EXPIRE 0: %q", got)
	}
}

// TestServerSetOptions covers the SET option surface: NX, XX, GET,
// KEEPTTL, and the four expiry forms.
func TestServerSetOptions(t *testing.T) {
	do, clockp := dispatchServer(t)

	if got := do("SET", "k", "v1", "EX", "100"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := do("TTL", "k"); got != ":100\r\n" {
		t.Fatalf("SET EX did not stamp: %q", got)
	}

	// A plain SET discards the TTL; KEEPTTL keeps it.
	if got := do("SET", "k", "v2"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := do("TTL", "k"); got != ":-1\r\n" {
		t.Fatalf("plain SET kept the TTL: %q", got)
	}
	do("SET", "k", "v3", "PX", "1500")
	if got := do("TTL", "k"); got != ":2\r\n" {
		t.Fatalf("PX 1500 rounds to 2: %q", got)
	}
	if got := do("SET", "k", "v4", "KEEPTTL"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := do("TTL", "k"); got != ":2\r\n" {
		t.Fatalf("KEEPTTL lost the TTL: %q", got)
	}

	// NX and XX gate on existence; GET returns the old value either way.
	if got := do("SET", "k", "v5", "NX"); got != "$-1\r\n" {
		t.Fatalf("NX overwrote: %q", got)
	}
	if got := do("SET", "missing", "v", "XX"); got != "$-1\r\n" {
		t.Fatalf("XX created: %q", got)
	}
	if got := do("GET", "missing"); got != "$-1\r\n" {
		t.Fatal(got)
	}
	if got := do("SET", "k", "v6", "GET"); got != "$2\r\nv4\r\n" {
		t.Fatalf("SET GET old value = %q", got)
	}
	if got := do("GET", "k"); got != "$2\r\nv6\r\n" {
		t.Fatal(got)
	}
	if got := do("SET", "k", "v7", "NX", "GET"); got != "$2\r\nv6\r\n" {
		t.Fatalf("blocked NX GET = %q", got)
	}
	if got := do("GET", "k"); got != "$2\r\nv6\r\n" {
		t.Fatalf("blocked NX GET wrote anyway: %q", got)
	}
	if got := do("SET", "fresh", "v", "GET"); got != "$-1\r\n" {
		t.Fatalf("SET GET on missing = %q", got)
	}
	if got := do("GET", "fresh"); got != "$1\r\nv\r\n" {
		t.Fatal(got)
	}

	// EXAT and PXAT are absolute; a past stamp leaves the key gone.
	at := (*clockp + 60_000) / 1000
	do("SET", "k", "v8", "EXAT", intArg(at))
	if got := do("TTL", "k"); got != ":60\r\n" {
		t.Fatalf("EXAT TTL = %q", got)
	}
	if got := do("SET", "k", "v9", "EXAT", intArg(*clockp/1000-10)); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := do("GET", "k"); got != "$-1\r\n" {
		t.Fatalf("past EXAT left the key: %q", got)
	}

	// Option validation.
	for _, bad := range [][]string{
		{"SET", "k", "v", "EX", "0"},
		{"SET", "k", "v", "PX", "-1"},
		{"SET", "k", "v", "EX", "9999999999999999999"},
	} {
		if got := do(bad...); got != "-ERR value is not an integer or out of range\r\n" &&
			got != "-ERR invalid expire time in 'set' command\r\n" {
			t.Fatalf("%v = %q", bad, got)
		}
	}
	for _, bad := range [][]string{
		{"SET", "k", "v", "NX", "XX"},
		{"SET", "k", "v", "EX", "10", "KEEPTTL"},
		{"SET", "k", "v", "EX"},
		{"SET", "k", "v", "BOGUS"},
		{"SET", "k", "v", "EX", "10", "PX", "10"},
	} {
		if got := do(bad...); got != "-ERR syntax error\r\n" {
			t.Fatalf("%v = %q, want syntax error", bad, got)
		}
	}
}

// TestServerStringPointCommands covers SETNX, SETEX, PSETEX, GETDEL,
// GETEX, STRLEN, SUBSTR, TYPE, and OBJECT ENCODING.
func TestServerStringPointCommands(t *testing.T) {
	do, clockp := dispatchServer(t)

	if got := do("SETNX", "k", "v"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("SETNX", "k", "w"); got != ":0\r\n" {
		t.Fatal(got)
	}
	if got := do("GET", "k"); got != "$1\r\nv\r\n" {
		t.Fatalf("losing SETNX wrote: %q", got)
	}

	if got := do("SETEX", "e", "10", "v"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := do("TTL", "e"); got != ":10\r\n" {
		t.Fatal(got)
	}
	if got := do("SETEX", "e", "0", "v"); got != "-ERR invalid expire time in 'setex' command\r\n" {
		t.Fatal(got)
	}
	do("PSETEX", "e", "500", "v")
	if got := do("TTL", "e"); got != ":1\r\n" {
		t.Fatal(got)
	}

	if got := do("GETDEL", "k"); got != "$1\r\nv\r\n" {
		t.Fatal(got)
	}
	if got := do("GET", "k"); got != "$-1\r\n" {
		t.Fatalf("GETDEL left the key: %q", got)
	}
	if got := do("GETDEL", "k"); got != "$-1\r\n" {
		t.Fatal(got)
	}

	do("SET", "g", "v", "EX", "100")
	if got := do("GETEX", "g"); got != "$1\r\nv\r\n" {
		t.Fatal(got)
	}
	if got := do("TTL", "g"); got != ":100\r\n" {
		t.Fatalf("bare GETEX touched the TTL: %q", got)
	}
	if got := do("GETEX", "g", "PERSIST"); got != "$1\r\nv\r\n" {
		t.Fatal(got)
	}
	if got := do("TTL", "g"); got != ":-1\r\n" {
		t.Fatalf("GETEX PERSIST kept the TTL: %q", got)
	}
	do("GETEX", "g", "EX", "30")
	if got := do("TTL", "g"); got != ":30\r\n" {
		t.Fatal(got)
	}
	if got := do("GETEX", "g", "EXAT", intArg(*clockp/1000-10)); got != "$1\r\nv\r\n" {
		t.Fatal(got)
	}
	if got := do("GET", "g"); got != "$-1\r\n" {
		t.Fatalf("past GETEX EXAT left the key: %q", got)
	}
	if got := do("GETEX", "g", "EX", "10"); got != "$-1\r\n" {
		t.Fatal(got)
	}
	if got := do("GETEX", "g", "PERSIST", "EX", "10"); got != "-ERR syntax error\r\n" {
		t.Fatal(got)
	}
	if got := do("GETEX", "g", "EX", "0"); got != "-ERR invalid expire time in 'getex' command\r\n" {
		t.Fatal(got)
	}

	do("SET", "s", "Hello World")
	if got := do("STRLEN", "s"); got != ":11\r\n" {
		t.Fatal(got)
	}
	if got := do("STRLEN", "missing"); got != ":0\r\n" {
		t.Fatal(got)
	}
	for _, c := range [][2]string{
		{"0 4", "$5\r\nHello\r\n"},
		{"-5 -1", "$5\r\nWorld\r\n"},
		{"0 -1", "$11\r\nHello World\r\n"},
		{"5 1", "$0\r\n\r\n"},
		{"20 30", "$0\r\n\r\n"},
		{"-100 2", "$3\r\nHel\r\n"},
	} {
		var a, b string
		fmt.Sscanf(c[0], "%s %s", &a, &b)
		if got := do("SUBSTR", "s", a, b); got != c[1] {
			t.Fatalf("SUBSTR s %s = %q, want %q", c[0], got, c[1])
		}
	}
	if got := do("SUBSTR", "missing", "0", "-1"); got != "$0\r\n\r\n" {
		t.Fatal(got)
	}

	if got := do("TYPE", "s"); got != "+string\r\n" {
		t.Fatal(got)
	}
	if got := do("TYPE", "missing"); got != "+none\r\n" {
		t.Fatal(got)
	}

	do("SET", "n", "12345")
	do("SET", "z", "0123")
	do("SET", "long", strings.Repeat("x", 45))
	for _, c := range [][2]string{
		{"n", "$3\r\nint\r\n"},
		{"z", "$6\r\nembstr\r\n"},
		{"s", "$6\r\nembstr\r\n"},
		{"long", "$3\r\nraw\r\n"},
	} {
		if got := do("OBJECT", "ENCODING", c[0]); got != c[1] {
			t.Fatalf("OBJECT ENCODING %s = %q, want %q", c[0], got, c[1])
		}
	}
	if got := do("OBJECT", "ENCODING", "missing"); got != "-ERR no such key\r\n" {
		t.Fatal(got)
	}

	// Millisecond exactness through dispatch: alive at expiry-1ms, gone at
	// expiry.
	do("SET", "p", "v", "PX", "500")
	*clockp += 499
	if got := do("GET", "p"); got != "$1\r\nv\r\n" {
		t.Fatalf("key died 1ms early: %q", got)
	}
	*clockp++
	if got := do("GET", "p"); got != "$-1\r\n" {
		t.Fatalf("key alive at its millisecond: %q", got)
	}
	if got := do("TTL", "p"); got != ":-2\r\n" {
		t.Fatal(got)
	}
}

// TestServerRopePointSurface runs the point surface against a value
// past the rope boundary, so the commands cross the representation
// ladder: encoding answers rope, STRLEN comes from the root, SUBSTR
// reads only the window's chunks, GETDEL retires the plane.
func TestServerRopePointSurface(t *testing.T) {
	do, clockp := dispatchServer(t)

	big := make([]byte, DefaultRopeMin+123_457)
	for i := range big {
		big[i] = byte('a' + (i*131)%23)
	}

	if got := do("SET", "big", string(big)); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := do("OBJECT", "ENCODING", "big"); got != "$4\r\nrope\r\n" {
		t.Fatalf("encoding = %q", got)
	}
	if got := do("STRLEN", "big"); got != fmt.Sprintf(":%d\r\n", len(big)) {
		t.Fatalf("STRLEN = %q", got)
	}
	if got := do("GET", "big"); got != string(AppendBulk(nil, big)) {
		t.Fatal("GET of rope did not round-trip")
	}

	// Windows chosen to cross chunk boundaries and hit the tail.
	for _, w := range [][2]int{{8190, 8195}, {0, 0}, {len(big) - 10, len(big) - 1}, {16384, 40000}} {
		want := string(AppendBulk(nil, big[w[0]:w[1]+1]))
		if got := do("SUBSTR", "big", intArg(int64(w[0])), intArg(int64(w[1]))); got != want {
			t.Fatalf("SUBSTR big %d %d mismatch", w[0], w[1])
		}
	}
	if got := do("SUBSTR", "big", "-10", "-1"); got != string(AppendBulk(nil, big[len(big)-10:])) {
		t.Fatal("negative SUBSTR on rope mismatch")
	}

	// TTL lives on the root record.
	if got := do("EXPIRE", "big", "50"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("TTL", "big"); got != ":50\r\n" {
		t.Fatal(got)
	}
	*clockp += 50_000
	if got := do("GET", "big"); got != "$-1\r\n" {
		t.Fatalf("expired rope still readable: %q", got)
	}

	do("SET", "big", string(big))
	if got := do("GETDEL", "big"); got != string(AppendBulk(nil, big)) {
		t.Fatal("GETDEL of rope did not return the value")
	}
	if got := do("GET", "big"); got != "$-1\r\n" {
		t.Fatal(got)
	}
}

func intArg(n int64) string {
	return strconv.FormatInt(n, 10)
}

// TestServerRangeSurface covers APPEND, SETRANGE, and GETRANGE through
// dispatch, including the TTL survival rule for the growing writes.
func TestServerRangeSurface(t *testing.T) {
	do, _ := dispatchServer(t)

	if got := do("APPEND", "k", "Hello"); got != ":5\r\n" {
		t.Fatal(got)
	}
	if got := do("APPEND", "k", " World"); got != ":11\r\n" {
		t.Fatal(got)
	}
	if got := do("GET", "k"); got != "$11\r\nHello World\r\n" {
		t.Fatal(got)
	}

	if got := do("GETRANGE", "k", "0", "4"); got != "$5\r\nHello\r\n" {
		t.Fatal(got)
	}
	if got := do("GETRANGE", "k", "-5", "-1"); got != "$5\r\nWorld\r\n" {
		t.Fatal(got)
	}
	if got := do("GETRANGE", "missing", "0", "-1"); got != "$0\r\n\r\n" {
		t.Fatal(got)
	}

	if got := do("SETRANGE", "k", "6", "Redis"); got != ":11\r\n" {
		t.Fatal(got)
	}
	if got := do("GET", "k"); got != "$11\r\nHello Redis\r\n" {
		t.Fatal(got)
	}
	if got := do("SETRANGE", "k", "11", "!!"); got != ":13\r\n" {
		t.Fatal(got)
	}
	if got := do("GET", "k"); got != "$13\r\nHello Redis!!\r\n" {
		t.Fatal(got)
	}

	// A far offset on a missing key zero-fills the gap.
	if got := do("SETRANGE", "z", "5", "x"); got != ":6\r\n" {
		t.Fatal(got)
	}
	if got := do("GET", "z"); got != "$6\r\n\x00\x00\x00\x00\x00x\r\n" {
		t.Fatalf("gap not zero-filled: %q", got)
	}

	// An empty patch writes nothing: no key created, length reported.
	if got := do("SETRANGE", "none", "0", ""); got != ":0\r\n" {
		t.Fatal(got)
	}
	if got := do("TYPE", "none"); got != "+none\r\n" {
		t.Fatalf("empty SETRANGE created the key: %q", got)
	}
	if got := do("SETRANGE", "k", "100", ""); got != ":13\r\n" {
		t.Fatal(got)
	}
	if got := do("STRLEN", "k"); got != ":13\r\n" {
		t.Fatal(got)
	}

	if got := do("SETRANGE", "k", "-1", "x"); got != "-ERR offset is out of range\r\n" {
		t.Fatal(got)
	}
	if got := do("SETRANGE", "k", "abc", "x"); got != "-ERR value is not an integer or out of range\r\n" {
		t.Fatal(got)
	}
	if got := do("SETRANGE", "k", "536870912", "x"); got != "-ERR string exceeds maximum allowed size (proto-max-bulk-len)\r\n" {
		t.Fatal(got)
	}

	// APPEND and SETRANGE keep the TTL, unlike SET.
	do("SET", "t", "v", "EX", "100")
	do("APPEND", "t", "v")
	if got := do("TTL", "t"); got != ":100\r\n" {
		t.Fatalf("APPEND touched the TTL: %q", got)
	}
	do("SETRANGE", "t", "0", "x")
	if got := do("TTL", "t"); got != ":100\r\n" {
		t.Fatalf("SETRANGE touched the TTL: %q", got)
	}
}

// TestServerRangeRope runs the range surface across the rope boundary
// at the server's production chunk size, mirrored against an oracle
// byte slice.
func TestServerRangeRope(t *testing.T) {
	do, _ := dispatchServer(t)

	want := pat(DefaultRopeMin+3000, 21)
	if got := do("SET", "big", string(want)); got != "+OK\r\n" {
		t.Fatal(got)
	}

	// Patch crossing an 8 KiB chunk boundary, then a far grow that
	// opens a lazy gap, then an append on the grown rope.
	p1 := pat(20, 22)
	if got := do("SETRANGE", "big", "8190", string(p1)); got != fmt.Sprintf(":%d\r\n", len(want)) {
		t.Fatal(got)
	}
	want = oracleSetRange(want, 8190, p1)

	p2 := pat(100, 23)
	off2 := len(want) + 40_000
	want = oracleSetRange(want, off2, p2)
	if got := do("SETRANGE", "big", intArg(int64(off2)), string(p2)); got != fmt.Sprintf(":%d\r\n", len(want)) {
		t.Fatal(got)
	}

	p3 := pat(50, 24)
	want = append(want, p3...)
	if got := do("APPEND", "big", string(p3)); got != fmt.Sprintf(":%d\r\n", len(want)) {
		t.Fatal(got)
	}

	if got := do("STRLEN", "big"); got != fmt.Sprintf(":%d\r\n", len(want)) {
		t.Fatal(got)
	}
	if got := do("OBJECT", "ENCODING", "big"); got != "$4\r\nrope\r\n" {
		t.Fatal(got)
	}
	for _, w := range [][2]int{
		{8180, 8230},                                   // across the patched boundary
		{len(want) - 60, len(want) - 1},                // the appended tail
		{off2 - 50, off2 + 50},                         // gap edge into the far patch
		{DefaultRopeMin, off2 - 40_100},                // inside the pre-grow tail
		{DefaultRopeMin + 2990, DefaultRopeMin + 3010}, // old tail into the gap zeros
	} {
		wantReply := string(AppendBulk(nil, want[w[0]:w[1]+1]))
		if got := do("GETRANGE", "big", intArg(int64(w[0])), intArg(int64(w[1]))); got != wantReply {
			t.Fatalf("GETRANGE %d %d mismatch", w[0], w[1])
		}
	}
	if got := do("GET", "big"); got != string(AppendBulk(nil, want)) {
		t.Fatal("full GET did not match the oracle")
	}
}
