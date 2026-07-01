package f1srv

import (
	"encoding/hex"
	"testing"
)

// hexBlob turns a hex string captured from a live server into the raw bytes RESTORE takes. The
// cross-engine cases embed blobs produced by Redis 8.8 and Valkey 9.1 so the decoder is exercised
// against real footers (RDB version 14 and 80) and the LZF form Redis writes for a long value.
func hexBlob(t *testing.T, s string) string {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return string(b)
}

// TestDumpRestoreRoundTrip proves aki restores exactly what aki dumped, across the string shapes
// that take different RDB encodings: a short raw string, a canonical integer (int-encoded), the
// empty string, a value with embedded NUL and high bytes, and a long value.
func TestDumpRestoreRoundTrip(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	longVal := ""
	for i := 0; i < 400; i++ {
		longVal += "x"
	}
	cases := []string{"hello", "12345", "-42", "007", "", "a\x00b\xffc", longVal}
	for _, val := range cases {
		cmd(t, rw, "SET", "src", val)
		expect(t, rw, "+OK")

		cmd(t, rw, "DUMP", "src")
		reply := readReply(t, rw)
		if len(reply) < 1 || reply[0] != '$' {
			t.Fatalf("DUMP %q reply = %q, want a bulk", val, reply)
		}
		blob := reply[1:]

		// Restore under a fresh name and confirm the value survives the round trip.
		cmd(t, rw, "DEL", "dst")
		drainReply(t, rw)
		cmd(t, rw, "RESTORE", "dst", "0", blob)
		expect(t, rw, "+OK")
		cmd(t, rw, "GET", "dst")
		expect(t, rw, "$"+val)
	}
}

// TestRestoreCrossEngine restores blobs produced by live Redis 8.8 and Valkey 9.1. The two servers
// stamp different RDB versions (14 and 80) and therefore different checksums, so this is the real
// interop test: aki must accept both, including the LZF-compressed long value Redis and Valkey emit.
func TestRestoreCrossEngine(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	long := ""
	for i := 0; i < 400; i++ {
		long += "ab"[i%2 : i%2+1]
	}
	cases := []struct {
		name string
		hex  string
		want string
	}{
		{"redis-hello", "000568656c6c6f0e00112907093607347b", "hello"},
		{"valkey-hello", "000568656c6c6f5000ac5816e7fb6647fe", "hello"},
		{"redis-int", "00c139300e00efcf3258c0cf1cc8", "12345"},
		{"valkey-int", "00c13930500052be23b60dae6f4d", "12345"},
		{"redis-lzf", "00c30d419002616261e0ff01e07a010161620e004d58b9eb60ee99eb", long},
		{"valkey-lzf", "00c30d419002616261e0ff01e07a010161625000f029a805ad8fea6e", long},
	}
	for _, tc := range cases {
		cmd(t, rw, "DEL", tc.name)
		drainReply(t, rw)
		cmd(t, rw, "RESTORE", tc.name, "0", hexBlob(t, tc.hex))
		expect(t, rw, "+OK")
		cmd(t, rw, "GET", tc.name)
		expect(t, rw, "$"+tc.want)
	}
}

// TestRestoreErrors matches the argument, checksum, and existence errors both servers give, and
// proves REPLACE overwrites a destination of a different type (a hash) with the restored string.
func TestRestoreErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	good := hexBlob(t, "000568656c6c6f0e00112907093607347b") // Redis DUMP of "hello"

	cmd(t, rw, "RESTORE", "k")
	expect(t, rw, "-ERR wrong number of arguments for 'restore' command")
	cmd(t, rw, "RESTORE", "k", "notint", good)
	expect(t, rw, "-ERR value is not an integer or out of range")
	cmd(t, rw, "RESTORE", "k", "-1", good)
	expect(t, rw, "-ERR Invalid TTL value, must be >= 0")
	cmd(t, rw, "RESTORE", "k", "0", "garbage")
	expect(t, rw, "-ERR DUMP payload version or checksum are wrong")

	// A one-bit flip in the checksum is rejected.
	bad := []byte(good)
	bad[len(bad)-1] ^= 0x01
	cmd(t, rw, "RESTORE", "k", "0", string(bad))
	expect(t, rw, "-ERR DUMP payload version or checksum are wrong")

	// First restore succeeds; a second without REPLACE is a BUSYKEY, with REPLACE it overwrites.
	cmd(t, rw, "RESTORE", "k", "0", good)
	expect(t, rw, "+OK")
	cmd(t, rw, "RESTORE", "k", "0", good)
	expect(t, rw, "-BUSYKEY Target key name already exists.")
	cmd(t, rw, "RESTORE", "k", "0", good, "REPLACE")
	expect(t, rw, "+OK")

	// REPLACE onto a hash drops the whole collection and lands the string.
	cmd(t, rw, "HSET", "h", "f", "v")
	expect(t, rw, ":1")
	cmd(t, rw, "RESTORE", "h", "0", good, "REPLACE")
	expect(t, rw, "+OK")
	cmd(t, rw, "GET", "h")
	expect(t, rw, "$hello")

	// Option validation.
	cmd(t, rw, "RESTORE", "opt", "0", good, "FROB")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "RESTORE", "opt", "0", good, "FREQ", "999")
	expect(t, rw, "-ERR Invalid frequency value, must be >= 0 and <= 255")
	cmd(t, rw, "RESTORE", "opt", "0", good, "IDLETIME")
	expect(t, rw, "-ERR syntax error")
	// IDLETIME and FREQ are accepted with a valid value.
	cmd(t, rw, "RESTORE", "opt", "0", good, "IDLETIME", "10", "FREQ", "5")
	expect(t, rw, "+OK")
}

// TestRestoreTTL sets a relative and an absolute expiry through RESTORE and confirms the key carries
// a positive remaining TTL.
func TestRestoreTTL(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	good := hexBlob(t, "000568656c6c6f0e00112907093607347b")

	cmd(t, rw, "RESTORE", "rel", "100000", good)
	expect(t, rw, "+OK")
	cmd(t, rw, "PTTL", "rel")
	if r := readReply(t, rw); r[0] != ':' || r == ":-1" || r == ":-2" {
		t.Fatalf("PTTL rel = %q, want a positive ttl", r)
	}

	// A far-future absolute timestamp in milliseconds.
	cmd(t, rw, "RESTORE", "abs", "99999999999999", good, "ABSTTL")
	expect(t, rw, "+OK")
	cmd(t, rw, "PTTL", "abs")
	if r := readReply(t, rw); r[0] != ':' || r == ":-1" || r == ":-2" {
		t.Fatalf("PTTL abs = %q, want a positive ttl", r)
	}
}

// TestDumpMissingAndArgs covers DUMP's null reply for a missing key, its arity error, and the
// current scope boundary: a non-string DUMP is refused until the collection type slices land.
func TestDumpMissingAndArgs(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "DUMP", "nope")
	expect(t, rw, "$-1")
	cmd(t, rw, "DUMP")
	expect(t, rw, "-ERR wrong number of arguments for 'dump' command")

	cmd(t, rw, "HSET", "h", "f", "v")
	expect(t, rw, ":1")
	cmd(t, rw, "DUMP", "h")
	expect(t, rw, "-ERR DUMP of this type is not supported yet")
}
