package drivers

import (
	"bufio"
	"net"
	"testing"
)

// DUMP and RESTORE round-trip a key through its opaque serialization over the
// wire, the single-key single-shard path (dispatch/dump.go). These drive the real
// server, so they cover the full chain: the per-type DumpKey/RestoreKey wrappers,
// the M8 snapshot encoders they reuse, the CRC64 envelope, and the option parse.
// The payload is binary, so each case captures the DUMP bulk verbatim and hands it
// straight back to RESTORE, exactly as a client would.

// dumpKey sends DUMP key and returns the raw payload bytes of the bulk reply, the
// argument a following RESTORE carries verbatim.
func dumpKey(t *testing.T, nc net.Conn, br *bufio.Reader, key string) string {
	t.Helper()
	send(t, nc, "DUMP", key)
	return readBulk(t, br)
}

// TestDumpRestoreRoundTrip round-trips one key of every type: populate it, DUMP the
// payload, DEL the key, RESTORE from the payload, and read the value back. A
// collection round-trips at full fidelity because RESTORE rebuilds through the same
// M8 snapshot encoder a checkpoint uses, so scores, fields, order, and stream ids
// all survive the trip.
func TestDumpRestoreRoundTrip(t *testing.T) {
	_, nc, br := startServer(t)

	// String.
	send(t, nc, "SET", "s", "hello")
	expect(t, br, "+OK\r\n")
	payload := dumpKey(t, nc, br, "s")
	send(t, nc, "DEL", "s")
	expect(t, br, ":1\r\n")
	send(t, nc, "RESTORE", "s", "0", payload)
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "s")
	expect(t, br, "$5\r\nhello\r\n")

	// Set.
	send(t, nc, "SADD", "st", "a", "b", "c")
	expect(t, br, ":3\r\n")
	payload = dumpKey(t, nc, br, "st")
	send(t, nc, "DEL", "st")
	expect(t, br, ":1\r\n")
	send(t, nc, "RESTORE", "st", "0", payload)
	expect(t, br, "+OK\r\n")
	send(t, nc, "SCARD", "st")
	expect(t, br, ":3\r\n")
	send(t, nc, "SISMEMBER", "st", "b")
	expect(t, br, ":1\r\n")

	// Sorted set: scores must survive.
	send(t, nc, "ZADD", "z", "1.5", "x", "2.5", "y")
	expect(t, br, ":2\r\n")
	payload = dumpKey(t, nc, br, "z")
	send(t, nc, "DEL", "z")
	expect(t, br, ":1\r\n")
	send(t, nc, "RESTORE", "z", "0", payload)
	expect(t, br, "+OK\r\n")
	send(t, nc, "ZSCORE", "z", "y")
	expect(t, br, "$3\r\n2.5\r\n")
	send(t, nc, "ZCARD", "z")
	expect(t, br, ":2\r\n")

	// Hash: field and value must survive.
	send(t, nc, "HSET", "h", "f1", "v1", "f2", "v2")
	expect(t, br, ":2\r\n")
	payload = dumpKey(t, nc, br, "h")
	send(t, nc, "DEL", "h")
	expect(t, br, ":1\r\n")
	send(t, nc, "RESTORE", "h", "0", payload)
	expect(t, br, "+OK\r\n")
	send(t, nc, "HGET", "h", "f2")
	expect(t, br, "$2\r\nv2\r\n")

	// List: order must survive.
	send(t, nc, "RPUSH", "l", "one", "two", "three")
	expect(t, br, ":3\r\n")
	payload = dumpKey(t, nc, br, "l")
	send(t, nc, "DEL", "l")
	expect(t, br, ":1\r\n")
	send(t, nc, "RESTORE", "l", "0", payload)
	expect(t, br, "+OK\r\n")
	send(t, nc, "LRANGE", "l", "0", "-1")
	expect(t, br, "*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n")

	// Stream: entry id and fields must survive.
	send(t, nc, "XADD", "x", "1-1", "field", "val")
	expect(t, br, "$3\r\n1-1\r\n")
	payload = dumpKey(t, nc, br, "x")
	send(t, nc, "DEL", "x")
	expect(t, br, ":1\r\n")
	send(t, nc, "RESTORE", "x", "0", payload)
	expect(t, br, "+OK\r\n")
	send(t, nc, "XLEN", "x")
	expect(t, br, ":1\r\n")
	send(t, nc, "XRANGE", "x", "-", "+")
	expect(t, br, "*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$5\r\nfield\r\n$3\r\nval\r\n")
}

// TestDumpMissingKey pins DUMP on a key present nowhere: the null bulk, not an
// empty payload, so a client can tell an absent key from a zero-length value.
func TestDumpMissingKey(t *testing.T) {
	_, nc, br := startServer(t)
	send(t, nc, "DUMP", "nope")
	expect(t, br, "$-1\r\n")
}

// TestRestoreBusykeyAndReplace pins the existence guard: a RESTORE onto a live key
// answers BUSYKEY, and the same call with REPLACE clears the old key first and
// installs the payload.
func TestRestoreBusykeyAndReplace(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "old")
	expect(t, br, "+OK\r\n")
	payload := dumpKey(t, nc, br, "k")

	// A different value lives at the key now; RESTORE without REPLACE refuses.
	send(t, nc, "SET", "k", "current")
	expect(t, br, "+OK\r\n")
	send(t, nc, "RESTORE", "k", "0", payload)
	expect(t, br, "-BUSYKEY Target key name already exists.\r\n")

	// REPLACE overwrites: the restored payload wins.
	send(t, nc, "RESTORE", "k", "0", payload, "REPLACE")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$3\r\nold\r\n")

	// REPLACE across types too: a set payload replaces a live string.
	send(t, nc, "SADD", "src", "m")
	expect(t, br, ":1\r\n")
	setPayload := dumpKey(t, nc, br, "src")
	send(t, nc, "RESTORE", "k", "0", setPayload, "REPLACE")
	expect(t, br, "+OK\r\n")
	send(t, nc, "TYPE", "k")
	expect(t, br, "+set\r\n")
}

// TestRestoreTTL pins the deadline argument: a non-zero ttl restores the key with
// a live PTTL, and ABSTTL reads the argument as an absolute unix-ms deadline. A
// zero ttl restores a persistent key.
func TestRestoreTTL(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")
	payload := dumpKey(t, nc, br, "k")
	send(t, nc, "DEL", "k")
	expect(t, br, ":1\r\n")

	// Relative ttl: a large window keeps the key live with a positive PTTL.
	send(t, nc, "RESTORE", "k", "100000", payload)
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\nv\r\n")
	send(t, nc, "TTL", "k")
	expect(t, br, ":100\r\n")

	// Persistent restore: ttl 0 leaves no deadline.
	send(t, nc, "RESTORE", "k", "0", payload, "REPLACE")
	expect(t, br, "+OK\r\n")
	send(t, nc, "TTL", "k")
	expect(t, br, ":-1\r\n")

	// ABSTTL with a deadline already in the past drops the key on next read.
	send(t, nc, "RESTORE", "k", "1", payload, "REPLACE", "ABSTTL")
	expect(t, br, "+OK\r\n")
	send(t, nc, "EXISTS", "k")
	expect(t, br, ":0\r\n")
}

// TestRestoreBadPayload pins the envelope guard: a flipped byte fails the CRC and
// answers the version-or-checksum error, and a truncated blob too short for the
// footer fails the same way, so a corrupt argument never reaches a type rebuild.
func TestRestoreBadPayload(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "hello")
	expect(t, br, "+OK\r\n")
	payload := dumpKey(t, nc, br, "k")
	send(t, nc, "DEL", "k")
	expect(t, br, ":1\r\n")

	// Flip a byte in the body: the CRC no longer covers it.
	corrupt := []byte(payload)
	corrupt[0] ^= 0xff
	send(t, nc, "RESTORE", "k", "0", string(corrupt))
	expect(t, br, "-ERR DUMP payload version or checksum are wrong\r\n")

	// A blob too short for the ten-byte footer fails the same guard.
	send(t, nc, "RESTORE", "k", "0", "short")
	expect(t, br, "-ERR DUMP payload version or checksum are wrong\r\n")

	// The intact payload still restores, proving the guard rejected only the
	// tampered forms.
	send(t, nc, "RESTORE", "k", "0", payload)
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$5\r\nhello\r\n")
}

// TestRestoreSyntax pins the option-tail parse: an unknown option and an IDLETIME
// missing its value are both syntax errors before any key is touched, and a valid
// IDLETIME/FREQ pair is accepted and ignored (f3 keeps no LRU or LFU clock to seed).
func TestRestoreSyntax(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")
	payload := dumpKey(t, nc, br, "k")
	send(t, nc, "DEL", "k")
	expect(t, br, ":1\r\n")

	send(t, nc, "RESTORE", "k", "0", payload, "BOGUS")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "RESTORE", "k", "0", payload, "IDLETIME")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "RESTORE", "k", "0", payload, "IDLETIME", "notanint")
	expect(t, br, "-ERR syntax error\r\n")

	send(t, nc, "RESTORE", "k", "0", payload, "IDLETIME", "50", "FREQ", "5")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\nv\r\n")
}
