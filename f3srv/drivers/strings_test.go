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

// TestGetdel walks GETDEL: it returns the value and removes the key in one
// step, reads a missing or already-taken key as nil, and reaches only the
// string keyspace, the same as GET.
func TestGetdel(t *testing.T) {
	_, nc, br := startServer(t)

	// Present key: the value comes back and the key is gone after.
	send(t, nc, "SET", "k", "hello")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GETDEL", "k")
	expect(t, br, "$5\r\nhello\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$-1\r\n")
	send(t, nc, "EXISTS", "k")
	expect(t, br, ":0\r\n")

	// A second GETDEL and a GETDEL of a never-set key both read as nil.
	send(t, nc, "GETDEL", "k")
	expect(t, br, "$-1\r\n")
	send(t, nc, "GETDEL", "missing")
	expect(t, br, "$-1\r\n")

	// An int-cell value round-trips byte-identical through GETDEL.
	send(t, nc, "SET", "n", "-42")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GETDEL", "n")
	expect(t, br, "$3\r\n-42\r\n")
	send(t, nc, "EXISTS", "n")
	expect(t, br, ":0\r\n")

	// A live TTL key still returns its value and is then gone.
	send(t, nc, "SET", "t", "vv", "EX", "100")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GETDEL", "t")
	expect(t, br, "$2\r\nvv\r\n")
	send(t, nc, "GET", "t")
	expect(t, br, "$-1\r\n")
}

// TestGetset walks GETSET: it writes the new value and returns the old one, or
// nil when the key had none, reaching only the string keyspace like GET.
func TestGetset(t *testing.T) {
	_, nc, br := startServer(t)

	// A first GETSET on a missing key returns nil and writes the value.
	send(t, nc, "GETSET", "k", "one")
	expect(t, br, "$-1\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$3\r\none\r\n")

	// A second returns the old value and overwrites it.
	send(t, nc, "GETSET", "k", "two")
	expect(t, br, "$3\r\none\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$3\r\ntwo\r\n")

	// An int-cell old value round-trips byte-identical.
	send(t, nc, "SET", "n", "-42")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GETSET", "n", "7")
	expect(t, br, "$3\r\n-42\r\n")
	send(t, nc, "GET", "n")
	expect(t, br, "$1\r\n7\r\n")
}

// TestSetnx walks SETNX: it writes and replies 1 only when the key is absent,
// and replies 0 without touching the value once one is present.
func TestSetnx(t *testing.T) {
	_, nc, br := startServer(t)

	// First write takes, reports 1.
	send(t, nc, "SETNX", "k", "one")
	expect(t, br, ":1\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$3\r\none\r\n")

	// Second is refused, reports 0, leaves the value.
	send(t, nc, "SETNX", "k", "two")
	expect(t, br, ":0\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$3\r\none\r\n")

	// After a delete the key is writable again.
	send(t, nc, "DEL", "k")
	expect(t, br, ":1\r\n")
	send(t, nc, "SETNX", "k", "three")
	expect(t, br, ":1\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$5\r\nthree\r\n")
}

// TestGenericExpiryRead walks the read-only expiry queries TTL, PTTL,
// EXPIRETIME, and PEXPIRETIME across the three states each reports: a missing
// key answers -2, a key with no deadline answers -1, and a key with a deadline
// answers its remaining or absolute time.
func TestGenericExpiryRead(t *testing.T) {
	_, nc, br := startServer(t)

	// Missing key is -2 for every query.
	for _, q := range []string{"TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME"} {
		send(t, nc, q, "missing")
		expect(t, br, ":-2\r\n")
	}

	// A key with no deadline is -1 for every query.
	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")
	for _, q := range []string{"TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME"} {
		send(t, nc, q, "k")
		expect(t, br, ":-1\r\n")
	}

	// A set key exists but carries no key-level deadline, so it too is -1.
	if _, err := nc.Write([]byte(cmd("SADD", "s", "m"))); err != nil {
		t.Fatal(err)
	}
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "s")
	expect(t, br, ":-1\r\n")

	// A key with a 100s deadline reports remaining time within a second of it,
	// and the absolute queries agree: EXPIRETIME is PEXPIRETIME floored to
	// seconds off the one fixed deadline.
	send(t, nc, "SET", "e", "v", "EX", "100")
	expect(t, br, "+OK\r\n")
	if ttl := readInt(t, nc, br, "TTL", "e"); ttl < 96 || ttl > 100 {
		t.Fatalf("TTL = %d, want within (96,100]", ttl)
	}
	if pttl := readInt(t, nc, br, "PTTL", "e"); pttl < 95001 || pttl > 100000 {
		t.Fatalf("PTTL = %d, want within (95000,100000]", pttl)
	}
	pexp := readInt(t, nc, br, "PEXPIRETIME", "e")
	exp := readInt(t, nc, br, "EXPIRETIME", "e")
	if pexp <= 0 || exp != pexp/1000 {
		t.Fatalf("EXPIRETIME %d and PEXPIRETIME %d disagree", exp, pexp)
	}
}

// TestGetex walks GETEX: it returns the value like GET while optionally setting
// or clearing the deadline in the same step, and errors on a bad option without
// touching the key.
func TestGetex(t *testing.T) {
	_, nc, br := startServer(t)

	// A missing key is nil for every form.
	send(t, nc, "GETEX", "missing")
	expect(t, br, "$-1\r\n")
	send(t, nc, "GETEX", "missing", "PERSIST")
	expect(t, br, "$-1\r\n")

	// No option returns the value and leaves the TTL untouched.
	send(t, nc, "SET", "k", "hello")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GETEX", "k")
	expect(t, br, "$5\r\nhello\r\n")
	send(t, nc, "TTL", "k")
	expect(t, br, ":-1\r\n")

	// EX on a slotless key returns the value and adds a deadline, which the
	// record had to be rebuilt to carry.
	send(t, nc, "GETEX", "k", "EX", "100")
	expect(t, br, "$5\r\nhello\r\n")
	if ttl := readInt(t, nc, br, "TTL", "k"); ttl < 96 || ttl > 100 {
		t.Fatalf("TTL after GETEX EX = %d, want within (96,100]", ttl)
	}

	// PERSIST returns the value and clears the deadline in place.
	send(t, nc, "GETEX", "k", "PERSIST")
	expect(t, br, "$5\r\nhello\r\n")
	send(t, nc, "TTL", "k")
	expect(t, br, ":-1\r\n")

	// PX sets a millisecond deadline; PXAT sets an absolute one and the two
	// agree on the same key when it is re-armed.
	send(t, nc, "GETEX", "k", "PX", "100000")
	expect(t, br, "$5\r\nhello\r\n")
	if pttl := readInt(t, nc, br, "PTTL", "k"); pttl < 95001 || pttl > 100000 {
		t.Fatalf("PTTL after GETEX PX = %d, want within (95000,100000]", pttl)
	}

	// An int-cell value round-trips byte-identical through GETEX.
	send(t, nc, "SET", "n", "-42")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GETEX", "n", "EX", "50")
	expect(t, br, "$3\r\n-42\r\n")
	if ttl := readInt(t, nc, br, "TTL", "n"); ttl < 46 || ttl > 50 {
		t.Fatalf("TTL after GETEX EX on int = %d, want within (46,50]", ttl)
	}

	// A bad expiry and a bad option both error, and neither changes the key.
	send(t, nc, "GETEX", "k", "EX", "0")
	expect(t, br, "-ERR invalid expire time in 'getex' command\r\n")
	send(t, nc, "GETEX", "k", "PERSIST", "EX", "5")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "GETEX", "k", "BOGUS")
	expect(t, br, "-ERR syntax error\r\n")
	if pttl := readInt(t, nc, br, "PTTL", "k"); pttl < 95001 || pttl > 100000 {
		t.Fatalf("PTTL after failed GETEX = %d, want the PX deadline unchanged", pttl)
	}
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

// TestIncrFamily walks INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT and their error
// edges against the Redis texts.
func TestIncrFamily(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "INCR", "n")
	expect(t, br, ":1\r\n")
	send(t, nc, "INCRBY", "n", "41")
	expect(t, br, ":42\r\n")
	send(t, nc, "DECR", "n")
	expect(t, br, ":41\r\n")
	send(t, nc, "DECRBY", "n", "40")
	expect(t, br, ":1\r\n")
	send(t, nc, "GET", "n")
	expect(t, br, "$1\r\n1\r\n")

	// A canonical text value converts; anything else refuses.
	send(t, nc, "SET", "t", "99")
	expect(t, br, "+OK\r\n")
	send(t, nc, "INCR", "t")
	expect(t, br, ":100\r\n")
	send(t, nc, "SET", "t", "abc")
	expect(t, br, "+OK\r\n")
	send(t, nc, "INCR", "t")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")
	send(t, nc, "INCRBY", "n", "ten")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")

	// Overflow edges.
	send(t, nc, "SET", "m", "9223372036854775807")
	expect(t, br, "+OK\r\n")
	send(t, nc, "INCR", "m")
	expect(t, br, "-ERR increment or decrement would overflow\r\n")
	send(t, nc, "SET", "m", "-9223372036854775808")
	expect(t, br, "+OK\r\n")
	send(t, nc, "DECR", "m")
	expect(t, br, "-ERR increment or decrement would overflow\r\n")
	send(t, nc, "DECRBY", "m", "-9223372036854775808")
	expect(t, br, "-ERR decrement would overflow\r\n")

	// INCRBYFLOAT: create from zero, then in-place add, TTL untouched.
	send(t, nc, "INCRBYFLOAT", "f", "10.5")
	expect(t, br, "$4\r\n10.5\r\n")
	send(t, nc, "INCRBYFLOAT", "f", "0.1")
	expect(t, br, "$4\r\n10.6\r\n")
	send(t, nc, "INCRBYFLOAT", "f", "-5")
	expect(t, br, "$3\r\n5.6\r\n")
	send(t, nc, "INCRBYFLOAT", "f", "5e3")
	expect(t, br, "$6\r\n5005.6\r\n")
	send(t, nc, "INCRBYFLOAT", "f", "nan")
	expect(t, br, "-ERR value is not a valid float\r\n")
	send(t, nc, "INCRBYFLOAT", "f", "inf")
	expect(t, br, "-ERR increment would produce NaN or Infinity\r\n")
	send(t, nc, "SET", "t", "abc")
	expect(t, br, "+OK\r\n")
	send(t, nc, "INCRBYFLOAT", "t", "1")
	expect(t, br, "-ERR value is not a valid float\r\n")
}

// TestStringValueOps walks APPEND, SETRANGE, and GETRANGE (with its SUBSTR
// alias) over the socket.
func TestStringValueOps(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "APPEND", "k", "hello")
	expect(t, br, ":5\r\n")
	send(t, nc, "APPEND", "k", " world")
	expect(t, br, ":11\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$11\r\nhello world\r\n")

	// SETRANGE inside, past the end with a zero gap, and the empty-value
	// probe that never writes.
	send(t, nc, "SETRANGE", "k", "6", "there")
	expect(t, br, ":11\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$11\r\nhello there\r\n")
	send(t, nc, "SETRANGE", "gap", "3", "xy")
	expect(t, br, ":5\r\n")
	send(t, nc, "GET", "gap")
	expect(t, br, "$5\r\n\x00\x00\x00xy\r\n")
	send(t, nc, "SETRANGE", "nope", "0", "")
	expect(t, br, ":0\r\n")
	send(t, nc, "EXISTS", "nope")
	expect(t, br, ":0\r\n")
	send(t, nc, "SETRANGE", "k", "-1", "x")
	expect(t, br, "-ERR offset is out of range\r\n")
	send(t, nc, "SETRANGE", "k", "536870912", "x")
	expect(t, br, "-ERR string exceeds maximum allowed size (proto-max-bulk-len)\r\n")

	// GETRANGE clamping, negative indexes, and the wholly negative reversed
	// range that answers empty before folding.
	send(t, nc, "GETRANGE", "k", "0", "4")
	expect(t, br, "$5\r\nhello\r\n")
	send(t, nc, "GETRANGE", "k", "-5", "-1")
	expect(t, br, "$5\r\nthere\r\n")
	send(t, nc, "GETRANGE", "k", "0", "-1")
	expect(t, br, "$11\r\nhello there\r\n")
	send(t, nc, "GETRANGE", "k", "5", "100000")
	expect(t, br, "$6\r\n there\r\n")
	send(t, nc, "GETRANGE", "k", "-100", "-200")
	expect(t, br, "$0\r\n\r\n")
	send(t, nc, "GETRANGE", "k", "9", "5")
	expect(t, br, "$0\r\n\r\n")
	send(t, nc, "GETRANGE", "missing", "0", "-1")
	expect(t, br, "$0\r\n\r\n")
	send(t, nc, "SUBSTR", "k", "0", "4")
	expect(t, br, "$5\r\nhello\r\n")

	// APPEND onto an int cell goes through the digits.
	send(t, nc, "INCR", "n")
	expect(t, br, ":1\r\n")
	send(t, nc, "APPEND", "n", "0")
	expect(t, br, ":2\r\n")
	send(t, nc, "INCR", "n")
	expect(t, br, ":11\r\n")
}

// TestBitmapPointSurface walks SETBIT and GETBIT over the socket: the previous
// bit reply, the MSB-first byte contract a plain GET has to see, sparse growth,
// the past-end and missing-key zero, the int-cell wrinkle, and the offset and
// value error surfaces.
func TestBitmapPointSurface(t *testing.T) {
	_, nc, br := startServer(t)

	// SETBIT returns the previous bit; the byte contract puts bit 0 at the MSB,
	// so after SETBIT k 0 1 a GET reads 0x80.
	send(t, nc, "SETBIT", "k", "0", "1")
	expect(t, br, ":0\r\n")
	send(t, nc, "SETBIT", "k", "0", "1")
	expect(t, br, ":1\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\n\x80\r\n")
	send(t, nc, "GETBIT", "k", "0")
	expect(t, br, ":1\r\n")
	send(t, nc, "GETBIT", "k", "7")
	expect(t, br, ":0\r\n")

	// A SETBIT past the end zero-extends; the addressed bit lands in a new byte
	// and the gap reads back as zeros.
	send(t, nc, "SETBIT", "k", "17", "1")
	expect(t, br, ":0\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$3\r\n\x80\x00\x40\r\n")
	send(t, nc, "GETBIT", "k", "17")
	expect(t, br, ":1\r\n")

	// Clearing a bit returns its old value and leaves a zero.
	send(t, nc, "SETBIT", "k", "0", "0")
	expect(t, br, ":1\r\n")
	send(t, nc, "GETBIT", "k", "0")
	expect(t, br, ":0\r\n")

	// GETBIT past the end and on a missing key answers 0 and never creates it.
	send(t, nc, "GETBIT", "k", "99999")
	expect(t, br, ":0\r\n")
	send(t, nc, "GETBIT", "missing", "0")
	expect(t, br, ":0\r\n")
	send(t, nc, "EXISTS", "missing")
	expect(t, br, ":0\r\n")

	// The int-cell wrinkle: SET 255 reads bit for bit off the decimal text, so
	// bit 0 (MSB of '2'=0x32) is 0 and bit 2 is 1.
	send(t, nc, "SET", "n", "255")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GETBIT", "n", "0")
	expect(t, br, ":0\r\n")
	send(t, nc, "GETBIT", "n", "2")
	expect(t, br, ":1\r\n")

	// Error surfaces: a negative offset, an offset past the 4Gib bit ceiling,
	// a non-integer offset, and a bit value that is not 0 or 1.
	send(t, nc, "SETBIT", "k", "-1", "1")
	expect(t, br, "-ERR bit offset is not an integer or out of range\r\n")
	send(t, nc, "SETBIT", "k", "4294967296", "1")
	expect(t, br, "-ERR bit offset is not an integer or out of range\r\n")
	send(t, nc, "GETBIT", "k", "abc")
	expect(t, br, "-ERR bit offset is not an integer or out of range\r\n")
	send(t, nc, "SETBIT", "k", "0", "2")
	expect(t, br, "-ERR bit is not an integer or out of range\r\n")
	send(t, nc, "SETBIT", "k", "0", "x")
	expect(t, br, "-ERR bit is not an integer or out of range\r\n")
}

// TestBitmapRangeSurface walks BITCOUNT and BITPOS over the socket against the
// Redis documentation examples: the whole-value count, the BYTE and BIT ranged
// forms with negative indices, the clear-bit past-end reply and the explicit
// end that suppresses it, and the argument error surfaces.
func TestBitmapRangeSurface(t *testing.T) {
	_, nc, br := startServer(t)

	// BITCOUNT, the documented "foobar" figures: whole value 26, byte 0 alone 4,
	// byte 1 alone 6, and the 5..30 BIT window 17.
	send(t, nc, "SET", "mykey", "foobar")
	expect(t, br, "+OK\r\n")
	send(t, nc, "BITCOUNT", "mykey")
	expect(t, br, ":26\r\n")
	send(t, nc, "BITCOUNT", "mykey", "0", "0")
	expect(t, br, ":4\r\n")
	send(t, nc, "BITCOUNT", "mykey", "1", "1")
	expect(t, br, ":6\r\n")
	send(t, nc, "BITCOUNT", "mykey", "1", "1", "BYTE")
	expect(t, br, ":6\r\n")
	send(t, nc, "BITCOUNT", "mykey", "5", "30", "BIT")
	expect(t, br, ":17\r\n")

	// Negative indices count from the end, and an empty range answers 0.
	send(t, nc, "BITCOUNT", "mykey", "-2", "-1")
	expect(t, br, ":7\r\n")
	send(t, nc, "BITCOUNT", "mykey", "2", "1")
	expect(t, br, ":0\r\n")

	// A missing key counts 0.
	send(t, nc, "BITCOUNT", "absent")
	expect(t, br, ":0\r\n")

	// BITPOS clear-bit search: the first 0 in 0xff 0xf0 0x00 is bit 12.
	send(t, nc, "SET", "k0", "\xff\xf0\x00")
	expect(t, br, "+OK\r\n")
	send(t, nc, "BITPOS", "k0", "0")
	expect(t, br, ":12\r\n")

	// BITPOS set-bit search with a start byte, and the BIT-unit range.
	send(t, nc, "SET", "k1", "\x00\xff\xf0")
	expect(t, br, "+OK\r\n")
	send(t, nc, "BITPOS", "k1", "1")
	expect(t, br, ":8\r\n")
	send(t, nc, "BITPOS", "k1", "1", "2")
	expect(t, br, ":16\r\n")
	send(t, nc, "BITPOS", "k1", "1", "0", "-1", "BIT")
	expect(t, br, ":8\r\n")

	// All-set value: a clear-bit search with no explicit end reports the first
	// bit past the value; an explicit end suppresses that and answers -1.
	send(t, nc, "SET", "k2", "\xff\xff\xff")
	expect(t, br, "+OK\r\n")
	send(t, nc, "BITPOS", "k2", "0")
	expect(t, br, ":24\r\n")
	send(t, nc, "BITPOS", "k2", "0", "0", "-1")
	expect(t, br, ":-1\r\n")

	// All-zero value: no set bit anywhere.
	send(t, nc, "SET", "k3", "\x00\x00\x00")
	expect(t, br, "+OK\r\n")
	send(t, nc, "BITPOS", "k3", "1")
	expect(t, br, ":-1\r\n")

	// Missing key: -1 for a set bit, 0 for a clear bit.
	send(t, nc, "BITPOS", "gone", "1")
	expect(t, br, ":-1\r\n")
	send(t, nc, "BITPOS", "gone", "0")
	expect(t, br, ":0\r\n")

	// Error surfaces: a bad unit token, a non-integer range, and a bit argument
	// that is not 0 or 1.
	send(t, nc, "BITCOUNT", "mykey", "0", "0", "NIBBLE")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "BITCOUNT", "mykey", "x", "0")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")
	send(t, nc, "BITPOS", "mykey", "2")
	expect(t, br, "-ERR The bit argument must be 1 or 0.\r\n")
}

func TestBitfieldSurface(t *testing.T) {
	_, nc, br := startServer(t)

	// SET returns the old field value and writes the new one; GET reads it back.
	send(t, nc, "BITFIELD", "bf", "SET", "u8", "0", "255")
	expect(t, br, "*1\r\n:0\r\n")
	send(t, nc, "BITFIELD", "bf", "GET", "u8", "0")
	expect(t, br, "*1\r\n:255\r\n")

	// INCRBY defaults to WRAP: 255+10 wraps to 9 in a u8.
	send(t, nc, "BITFIELD", "bf", "INCRBY", "u8", "0", "10")
	expect(t, br, "*1\r\n:9\r\n")
	// OVERFLOW SAT clamps 9+300 to the u8 ceiling 255.
	send(t, nc, "BITFIELD", "bf", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "300")
	expect(t, br, "*1\r\n:255\r\n")
	// OVERFLOW FAIL returns null on 255+10 and writes nothing.
	send(t, nc, "BITFIELD", "bf", "OVERFLOW", "FAIL", "INCRBY", "u8", "0", "10")
	expect(t, br, "*1\r\n$-1\r\n")

	// A signed read of 0xFF is -1; SET returns that old value, then GET sees -128.
	send(t, nc, "BITFIELD", "bf", "SET", "i8", "0", "-128")
	expect(t, br, "*1\r\n:-1\r\n")
	send(t, nc, "BITFIELD", "bf", "GET", "i8", "0")
	expect(t, br, "*1\r\n:-128\r\n")

	// A '#'-prefixed offset scales by the type width: #1 of u8 is byte 1. Two
	// sub-ops in one command return a two-element array.
	send(t, nc, "BITFIELD", "bf", "SET", "u8", "#1", "200", "GET", "u8", "#1")
	expect(t, br, "*2\r\n:0\r\n:200\r\n")

	// BITFIELD_RO serves a GET: byte 0 is 0x80, read as u8 that is 128.
	send(t, nc, "BITFIELD_RO", "bf", "GET", "u8", "0")
	expect(t, br, "*1\r\n:128\r\n")

	// Sub-ops chain left to right over a fresh key: INCRBY i5 to 1, then GET u4 0
	// reads the low nibble, still 0.
	send(t, nc, "BITFIELD", "k2", "INCRBY", "i5", "100", "1", "GET", "u4", "0")
	expect(t, br, "*2\r\n:1\r\n:0\r\n")

	// Error surfaces: u64 is not a valid type, BITFIELD_RO rejects SET, an unknown
	// OVERFLOW mode and a truncated GET both fail before any write.
	send(t, nc, "BITFIELD", "e", "GET", "u64", "0")
	expect(t, br, "-ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.\r\n")
	send(t, nc, "BITFIELD_RO", "bf", "SET", "u8", "0", "1")
	expect(t, br, "-ERR BITFIELD_RO only supports the GET subcommand\r\n")
	send(t, nc, "BITFIELD", "bf", "OVERFLOW", "BLAH")
	expect(t, br, "-ERR Invalid OVERFLOW type specified\r\n")
	send(t, nc, "BITFIELD", "bf", "GET", "u8")
	expect(t, br, "-ERR syntax error\r\n")
}

// TestBitopSurface drives BITOP end to end over the RESP wire. The keys are
// chosen to hash to one shard so the command takes the co-located fast path;
// the cross-shard refusal is exercised in the cross slice. The masks 0xFFF0 and
// 0x0FFFAA cover the zero-pad (the AND third byte, the OR/XOR carry) and the two
// result lengths, and the destination reads back byte for byte.
func TestBitopSurface(t *testing.T) {
	_, nc, br := startServer(t)

	// bk1 (2 bytes) and bk3 (3 bytes): the result length is the longer source,
	// the shorter one zero-pads past its end.
	send(t, nc, "SET", "bk1", "\xff\xf0")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "bk3", "\x0f\xff\xaa")
	expect(t, br, "+OK\r\n")

	// AND: ff&0f=0f, f0&ff=f0, (missing)&aa=00.
	send(t, nc, "BITOP", "AND", "dand", "bk1", "bk3")
	expect(t, br, ":3\r\n")
	send(t, nc, "GET", "dand")
	expect(t, br, "$3\r\n\x0f\xf0\x00\r\n")

	// OR: the shorter source carries the longer one's tail through.
	send(t, nc, "BITOP", "OR", "dor", "bk1", "bk3")
	expect(t, br, ":3\r\n")
	send(t, nc, "GET", "dor")
	expect(t, br, "$3\r\n\xff\xff\xaa\r\n")

	// XOR: ff^0f=f0, f0^ff=0f, 00^aa=aa.
	send(t, nc, "BITOP", "XOR", "dxor", "bk1", "bk3")
	expect(t, br, ":3\r\n")
	send(t, nc, "GET", "dxor")
	expect(t, br, "$3\r\n\xf0\x0f\xaa\r\n")

	// NOT is the single-source complement at that source's length.
	send(t, nc, "BITOP", "NOT", "dnot", "bk1")
	expect(t, br, ":2\r\n")
	send(t, nc, "GET", "dnot")
	expect(t, br, "$2\r\n\x00\x0f\r\n")

	// All sources missing: the result is empty, the destination is deleted, the
	// reply is 0.
	send(t, nc, "BITOP", "OR", "gone", "res", "out")
	expect(t, br, ":0\r\n")
	send(t, nc, "GET", "gone")
	expect(t, br, "$-1\r\n")

	// Aliasing: the destination is also a source. The result at each chunk depends
	// only on the sources at that chunk, so an aliased destination stays correct.
	send(t, nc, "SET", "bor", "\xff\xf0")
	expect(t, br, "+OK\r\n")
	send(t, nc, "BITOP", "AND", "bor", "bor", "bk3")
	expect(t, br, ":3\r\n")
	send(t, nc, "GET", "bor")
	expect(t, br, "$3\r\n\x0f\xf0\x00\r\n")

	// Error surfaces: NOT wants exactly one source, an unknown op is a syntax
	// error, and a source-less BITOP fails arity before it routes.
	send(t, nc, "BITOP", "NOT", "dnot", "bk1", "bk3")
	expect(t, br, "-ERR BITOP NOT must be called with a single source key.\r\n")
	send(t, nc, "BITOP", "FOO", "dand", "bk1")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "BITOP", "AND", "dand")
	expect(t, br, "-ERR wrong number of arguments for 'bitop' command\r\n")
}

// TestBitopCrossSurface drives BITOP where the destination and sources span
// shards, so the command takes the F17 cross-shard coordinator instead of the
// co-located fast path. On the two-shard test server k1/k2/box/a2 hash to shard 0
// and dand/dor/dxor/dnot/s2/a1 to shard 1, so each op below really does cross a
// shard boundary. The results are byte-for-byte the co-located answers: the
// coordinator streams the same algebra over hops.
func TestBitopCrossSurface(t *testing.T) {
	_, nc, br := startServer(t)

	// k1, k2 both on shard 0; the destinations are on shard 1, so the write hop
	// lands on a different owner than the source read hop.
	send(t, nc, "SET", "k1", "\xff\xf0")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "k2", "\x0f\xff\xaa")
	expect(t, br, "+OK\r\n")

	send(t, nc, "BITOP", "AND", "dand", "k1", "k2")
	expect(t, br, ":3\r\n")
	send(t, nc, "GET", "dand")
	expect(t, br, "$3\r\n\x0f\xf0\x00\r\n")

	send(t, nc, "BITOP", "OR", "dor", "k1", "k2")
	expect(t, br, ":3\r\n")
	send(t, nc, "GET", "dor")
	expect(t, br, "$3\r\n\xff\xff\xaa\r\n")

	send(t, nc, "BITOP", "XOR", "dxor", "k1", "k2")
	expect(t, br, ":3\r\n")
	send(t, nc, "GET", "dxor")
	expect(t, br, "$3\r\n\xf0\x0f\xaa\r\n")

	send(t, nc, "BITOP", "NOT", "dnot", "k1")
	expect(t, br, ":2\r\n")
	send(t, nc, "GET", "dnot")
	expect(t, br, "$2\r\n\x00\x0f\r\n")

	// Sources on two different shards (k1 on 0, s2 on 1): the coordinator reads
	// each with its own hop, one group per shard.
	send(t, nc, "SET", "s2", "\x0f\xff\xaa")
	expect(t, br, "+OK\r\n")
	send(t, nc, "BITOP", "AND", "box", "k1", "s2")
	expect(t, br, ":3\r\n")
	send(t, nc, "GET", "box")
	expect(t, br, "$3\r\n\x0f\xf0\x00\r\n")

	// All sources missing across shards: the result is empty, the destination is
	// deleted, the reply is 0.
	send(t, nc, "BITOP", "OR", "a2", "res", "out")
	expect(t, br, ":0\r\n")
	send(t, nc, "GET", "a2")
	expect(t, br, "$-1\r\n")

	// Aliasing across shards: the destination a1 (shard 1) is also a source, the
	// other source k1 is on shard 0. Read-before-write per chunk keeps it correct.
	send(t, nc, "SET", "a1", "\xf0\x0f")
	expect(t, br, "+OK\r\n")
	send(t, nc, "BITOP", "AND", "a1", "a1", "k1")
	expect(t, br, ":2\r\n")
	send(t, nc, "GET", "a1")
	expect(t, br, "$2\r\n\xf0\x00\r\n")

	// Error surfaces still fire on the cross path: NOT wants one source, an
	// unknown op is a syntax error.
	send(t, nc, "BITOP", "NOT", "dnot", "k1", "k2")
	expect(t, br, "-ERR BITOP NOT must be called with a single source key.\r\n")
	send(t, nc, "BITOP", "FOO", "dand", "k1")
	expect(t, br, "-ERR syntax error\r\n")
}

// bitopOracle is the byte model for the multi-chunk cross test: the result is as
// long as the longest source, shorter sources zero-pad, and NOT complements the
// single source.
func bitopOracle(op string, srcs ...[]byte) []byte {
	if op == "NOT" {
		out := make([]byte, len(srcs[0]))
		for i := range out {
			out[i] = ^srcs[0][i]
		}
		return out
	}
	ml := 0
	for _, s := range srcs {
		if len(s) > ml {
			ml = len(s)
		}
	}
	out := make([]byte, ml)
	for i := 0; i < ml; i++ {
		var acc byte
		if op == "AND" {
			acc = 0xFF
		}
		for _, s := range srcs {
			var v byte
			if i < len(s) {
				v = s[i]
			}
			switch op {
			case "AND":
				acc &= v
			case "OR":
				acc |= v
			case "XOR":
				acc ^= v
			}
		}
		out[i] = acc
	}
	return out
}

// TestBitopCrossMultiChunk drives cross-shard BITOP over sources that span
// several chunks with different lengths, so the coordinator's streaming loop runs
// many chunks, the AND short-circuit fires past the shorter source, and the
// zero-pad carries the longer source through. The two sources sit on different
// shards and the destinations on a third, so every chunk pays a read hop per
// source shard and a write hop to the destination. The result is checked against
// the byte oracle.
func TestBitopCrossMultiChunk(t *testing.T) {
	_, nc, br := startServer(t)

	// k1 (shard 0) spans just over three chunks; s2 (shard 1) just under two, so
	// AND short-circuits over the top chunk and OR/XOR carry k1 through.
	k1 := make([]byte, 200000)
	for i := range k1 {
		k1[i] = byte(i*7 + 1)
	}
	s2 := make([]byte, 130000)
	for i := range s2 {
		s2[i] = byte(i*13 + 5)
	}
	send(t, nc, "SET", "k1", string(k1))
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "s2", string(s2))
	expect(t, br, "+OK\r\n")

	// dand and dor are on shard 1, the write-hop owner, distinct from k1's shard.
	send(t, nc, "BITOP", "AND", "dand", "k1", "s2")
	expect(t, br, ":200000\r\n")
	send(t, nc, "GET", "dand")
	expectBulk(t, br, bitopOracle("AND", k1, s2))

	send(t, nc, "BITOP", "OR", "dor", "k1", "s2")
	expect(t, br, ":200000\r\n")
	send(t, nc, "GET", "dor")
	expectBulk(t, br, bitopOracle("OR", k1, s2))

	send(t, nc, "BITOP", "XOR", "dxor", "k1", "s2")
	expect(t, br, ":200000\r\n")
	send(t, nc, "GET", "dxor")
	expectBulk(t, br, bitopOracle("XOR", k1, s2))

	send(t, nc, "BITOP", "NOT", "dnot", "k1")
	expect(t, br, ":200000\r\n")
	send(t, nc, "GET", "dnot")
	expectBulk(t, br, bitopOracle("NOT", k1))
}
