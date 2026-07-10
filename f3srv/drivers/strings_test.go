package drivers

import (
	"net"
	"strconv"
	"testing"
	"time"
)

// cmd renders one command in RESP array form.
func cmd(args ...string) string {
	s := "*" + strconv.Itoa(len(args)) + "\r\n"
	for _, a := range args {
		s += "$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n"
	}
	return s
}

func send(t *testing.T, nc net.Conn, args ...string) {
	t.Helper()
	if _, err := nc.Write([]byte(cmd(args...))); err != nil {
		t.Fatal(err)
	}
}

// TestStringPointSurface walks SET/GET/STRLEN/EXISTS/DEL/TYPE over a socket,
// the owner-path point commands of the M0 string slice.
func TestStringPointSurface(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "hello")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$5\r\nhello\r\n")
	send(t, nc, "STRLEN", "k")
	expect(t, br, ":5\r\n")
	send(t, nc, "EXISTS", "k")
	expect(t, br, ":1\r\n")
	send(t, nc, "TYPE", "k")
	expect(t, br, "+string\r\n")

	send(t, nc, "GET", "missing")
	expect(t, br, "$-1\r\n")
	send(t, nc, "STRLEN", "missing")
	expect(t, br, ":0\r\n")
	send(t, nc, "EXISTS", "missing")
	expect(t, br, ":0\r\n")
	send(t, nc, "TYPE", "missing")
	expect(t, br, "+none\r\n")

	send(t, nc, "DEL", "k")
	expect(t, br, ":1\r\n")
	send(t, nc, "DEL", "k")
	expect(t, br, ":0\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$-1\r\n")

	// An integer value takes the int cell and reads back byte-identical.
	send(t, nc, "SET", "n", "-9223372036854775808")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "n")
	expect(t, br, "$20\r\n-9223372036854775808\r\n")
	send(t, nc, "STRLEN", "n")
	expect(t, br, ":20\r\n")
}

// TestSetOptions exercises the NX/XX/GET/KEEPTTL/expiry option matrix against
// the Redis answers.
func TestSetOptions(t *testing.T) {
	_, nc, br := startServer(t)

	// NX writes a missing key, refuses a present one.
	send(t, nc, "SET", "k", "a", "NX")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "k", "b", "NX")
	expect(t, br, "$-1\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\na\r\n")

	// XX is the mirror.
	send(t, nc, "SET", "x", "a", "XX")
	expect(t, br, "$-1\r\n")
	send(t, nc, "SET", "k", "b", "XX")
	expect(t, br, "+OK\r\n")

	// GET returns the old value; on a guard-suppressed write it still does.
	send(t, nc, "SET", "k", "c", "GET")
	expect(t, br, "$1\r\nb\r\n")
	send(t, nc, "SET", "k", "d", "NX", "GET")
	expect(t, br, "$1\r\nc\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\nc\r\n")
	send(t, nc, "SET", "fresh", "v", "GET")
	expect(t, br, "$-1\r\n")

	// Case-insensitive options.
	send(t, nc, "SET", "k", "e", "xx", "get")
	expect(t, br, "$1\r\nc\r\n")

	// Syntax errors: NX with XX, KEEPTTL with a unit, stray token, EX with a
	// missing or non-integer argument.
	send(t, nc, "SET", "k", "v", "NX", "XX")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "SET", "k", "v", "KEEPTTL", "EX", "10")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "SET", "k", "v", "EX", "10", "KEEPTTL")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "SET", "k", "v", "BOGUS")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "SET", "k", "v", "EX")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "SET", "k", "v", "EX", "ten")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")

	// Expire times must be strictly positive and must not overflow.
	send(t, nc, "SET", "k", "v", "EX", "0")
	expect(t, br, "-ERR invalid expire time in 'set' command\r\n")
	send(t, nc, "SET", "k", "v", "PX", "-1")
	expect(t, br, "-ERR invalid expire time in 'set' command\r\n")
	send(t, nc, "SET", "k", "v", "EX", "9223372036854775807")
	expect(t, br, "-ERR invalid expire time in 'set' command\r\n")

	// A bad expire time writes nothing.
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\ne\r\n")
}

// TestSetExpiry drives a real PX deadline through the socket: the key answers
// until the deadline and reads as absent after it, the lazy-expiry contract.
func TestSetExpiry(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v", "PX", "40")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\nv\r\n")

	time.Sleep(80 * time.Millisecond)
	send(t, nc, "GET", "k")
	expect(t, br, "$-1\r\n")
	send(t, nc, "EXISTS", "k")
	expect(t, br, ":0\r\n")
	send(t, nc, "TYPE", "k")
	expect(t, br, "+none\r\n")

	// A plain SET clears the deadline.
	send(t, nc, "SET", "k", "v", "PX", "40")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "k", "w")
	expect(t, br, "+OK\r\n")
	time.Sleep(80 * time.Millisecond)
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\nw\r\n")

	// KEEPTTL carries it.
	send(t, nc, "SET", "t", "v", "PX", "40")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "t", "w", "KEEPTTL")
	expect(t, br, "+OK\r\n")
	time.Sleep(80 * time.Millisecond)
	send(t, nc, "GET", "t")
	expect(t, br, "$-1\r\n")
}

// TestPipelinedStrings sends a mixed string batch in one write and expects
// every reply back in request order, crossing shards.
func TestPipelinedStrings(t *testing.T) {
	_, nc, br := startServer(t)

	req := cmd("SET", "a", "1") +
		cmd("SET", "b", "two") +
		cmd("GET", "a") +
		cmd("STRLEN", "b") +
		cmd("GET", "nope") +
		cmd("DEL", "a") +
		cmd("EXISTS", "a") +
		cmd("TYPE", "b")
	if _, err := nc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+OK\r\n+OK\r\n$1\r\n1\r\n:3\r\n$-1\r\n:1\r\n:0\r\n+string\r\n")
}
