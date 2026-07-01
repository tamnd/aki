package f1srv

import (
	"bufio"
	"testing"
)

// TestMemoryUsage returns a positive estimate for a key that exists and a null reply for one that
// does not, across every type, and validates the SAMPLES option.
func TestMemoryUsage(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MEMORY", "USAGE", "nope")
	expect(t, rw, "$-1")

	cmd(t, rw, "SET", "s", "hello")
	expect(t, rw, "+OK")
	assertPositiveInt(t, rw, "MEMORY", "USAGE", "s")
	// SAMPLES is accepted and does not change the reply shape.
	assertPositiveInt(t, rw, "MEMORY", "USAGE", "s", "SAMPLES", "5")
	assertPositiveInt(t, rw, "MEMORY", "USAGE", "s", "SAMPLES", "0")

	cmd(t, rw, "HSET", "h", "f", "v")
	expect(t, rw, ":1")
	assertPositiveInt(t, rw, "MEMORY", "USAGE", "h")

	cmd(t, rw, "SADD", "set", "a", "b")
	expect(t, rw, ":2")
	assertPositiveInt(t, rw, "MEMORY", "USAGE", "set")

	cmd(t, rw, "ZADD", "z", "1", "a")
	expect(t, rw, ":1")
	assertPositiveInt(t, rw, "MEMORY", "USAGE", "z")

	cmd(t, rw, "RPUSH", "l", "a", "b", "c")
	expect(t, rw, ":3")
	assertPositiveInt(t, rw, "MEMORY", "USAGE", "l")

	cmd(t, rw, "XADD", "st", "1-1", "f", "v")
	expect(t, rw, "$1-1")
	assertPositiveInt(t, rw, "MEMORY", "USAGE", "st")
}

// TestMemoryUsageErrors matches the argument and option errors both servers give.
func TestMemoryUsageErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "s", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "MEMORY", "USAGE")
	expect(t, rw, "-ERR wrong number of arguments for 'memory|usage' command")
	cmd(t, rw, "MEMORY", "USAGE", "s", "SAMPLES", "foo")
	expect(t, rw, "-ERR value is not an integer or out of range")
	cmd(t, rw, "MEMORY", "USAGE", "s", "SAMPLES")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "MEMORY", "USAGE", "s", "FOO", "5")
	expect(t, rw, "-ERR syntax error")
}

// TestMemoryStubs matches the stable-text subcommands verbatim.
func TestMemoryStubs(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MEMORY", "PURGE")
	expect(t, rw, "+OK")
	cmd(t, rw, "MEMORY", "DOCTOR")
	expect(t, rw, "$"+memoryDoctorEmpty)
	cmd(t, rw, "MEMORY", "MALLOC-STATS")
	expect(t, rw, "$"+memoryMallocStats)
	cmd(t, rw, "MEMORY")
	expect(t, rw, "-ERR wrong number of arguments for 'memory' command")
	cmd(t, rw, "MEMORY", "FROBNICATE")
	expect(t, rw, "-ERR unknown subcommand 'FROBNICATE'. Try MEMORY HELP.")
}

// TestMemoryStats returns a well-formed flat field/value array whose keys.count reflects the live
// top-level key count.
func TestMemoryStats(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "1")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "b", "2")
	expect(t, rw, "+OK")

	cmd(t, rw, "MEMORY", "STATS")
	// STATS is a flat array of bulk field then integer value, the same shape both servers use.
	hdr := readReply(t, rw)
	if len(hdr) < 2 || hdr[0] != '*' {
		t.Fatalf("MEMORY STATS header = %q, want an array", hdr)
	}
	n, ok := parseInt64Strict([]byte(hdr[1:]))
	if !ok || n%2 != 0 {
		t.Fatalf("MEMORY STATS array length = %q, want a positive even count", hdr)
	}
	found := false
	for i := int64(0); i < n; i += 2 {
		field := readReply(t, rw) // $bulk field
		value := readReply(t, rw) // :int value
		if field == "$keys.count" {
			if value != ":2" {
				t.Fatalf("keys.count = %q, want :2", value)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("MEMORY STATS missing keys.count field")
	}
}

// assertPositiveInt sends a command and asserts its reply is a RESP integer greater than zero.
func assertPositiveInt(t *testing.T, rw *bufio.ReadWriter, args ...string) {
	t.Helper()
	cmd(t, rw, args...)
	got := readReply(t, rw)
	if len(got) < 2 || got[0] != ':' {
		t.Fatalf("%v reply = %q, want an integer", args, got)
	}
	n, ok := parseInt64Strict([]byte(got[1:]))
	if !ok || n <= 0 {
		t.Fatalf("%v reply = %q, want a positive integer", args, got)
	}
}
