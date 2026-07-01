package f1srv

import (
	"encoding/hex"
	"testing"
)

// redisBigHashHex and valkeyBigHashHex are live DUMP blobs of a 200-field hash (field1..field200 =>
// value1..value200) from Redis 8.8 and Valkey 9.1. Both are the LZF-compressed listpack form (type
// 16), identical except for the RDB version and checksum in the footer, so they exercise the LZF
// decoder and the listpack forward walk on a real large-hash payload.
const redisBigHashHex = "10c346244ecf13cf0e00009001866669656c6431078676616c75652007600f0032a00f2007600f0033a00f2007600f0034a00f2007600f0035a00f2007600f0036a00f2007600f0037a00f2007600f0038a00f2007600f0039a00f02390787600f03313008878090200880110031c011200880110032c011200880110033c011200880110034c011200880110035c011200880110036c011200880110037c011200880110038c011200880110039c01120086011003220aa60b34008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa8011200860110033c0b34008801120aa60234008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa8011200860110034c0b340088011c0b34008801120aa60354008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa8011200860110035c0b340088011c0b340088011c0b34008801120aa60474008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa8011200860110036c0b340088011c0b340088011c0b340088011c0b34008801120aa60594008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa8011200860110037c0b340088011c0b340088011c0b340088011c0b340088011c0b34008801120aa606b4008801120aa80112008801120aa80112008801120aa80112008801120aa8011200860110038c0b340088011c0b340088011c0b340088011c0b340088011c0b340088011c0b34008801120aa607d4008801120aa80112008801120aa80112008801120aa8011200860110039c0b340088011c0b340088011c0b340088011c0b340088011c0b340088011c0b340088011c0b34008801120aa608f4008801120aa80112008801120aa801102390888601104313030098860126009a0130031e000132009a0130032e000132009a0130033e000132009a0130034e000132009a0130035e000132009a0130036e000132009a0130037e000132009a0130038e000132009a0130039e00013200980130031e000c74009a01320bd80db4009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda013200980130032e000c74009a013e000c74009a01320bd803b4009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda013200980130033e000c74009a013e000c74009a013e000c74009a01320bd804f4009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda013200980130034e000c74009a013e000c74009a013e000c74009a013e000c74009a01320bd80634009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda013200980130035e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a01320bd80774009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda013200980130036e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a01320bd808b4009a01320bda0132009a01320bda0132009a01320bda013200980130037e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a01320bd809f4009a01320bda0132009a01320bda013200980130038e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a01320bd80b34009a01320bda013200980130039e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a01320bd80c7400960130032e700cf20090109ff0e004f21711e3694c994"
const valkeyBigHashHex = "10c346244ecf13cf0e00009001866669656c6431078676616c75652007600f0032a00f2007600f0033a00f2007600f0034a00f2007600f0035a00f2007600f0036a00f2007600f0037a00f2007600f0038a00f2007600f0039a00f02390787600f03313008878090200880110031c011200880110032c011200880110033c011200880110034c011200880110035c011200880110036c011200880110037c011200880110038c011200880110039c01120086011003220aa60b34008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa8011200860110033c0b34008801120aa60234008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa8011200860110034c0b340088011c0b34008801120aa60354008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa8011200860110035c0b340088011c0b340088011c0b34008801120aa60474008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa8011200860110036c0b340088011c0b340088011c0b340088011c0b34008801120aa60594008801120aa80112008801120aa80112008801120aa80112008801120aa80112008801120aa8011200860110037c0b340088011c0b340088011c0b340088011c0b340088011c0b34008801120aa606b4008801120aa80112008801120aa80112008801120aa80112008801120aa8011200860110038c0b340088011c0b340088011c0b340088011c0b340088011c0b340088011c0b34008801120aa607d4008801120aa80112008801120aa80112008801120aa8011200860110039c0b340088011c0b340088011c0b340088011c0b340088011c0b340088011c0b340088011c0b34008801120aa608f4008801120aa80112008801120aa801102390888601104313030098860126009a0130031e000132009a0130032e000132009a0130033e000132009a0130034e000132009a0130035e000132009a0130036e000132009a0130037e000132009a0130038e000132009a0130039e00013200980130031e000c74009a01320bd80db4009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda013200980130032e000c74009a013e000c74009a01320bd803b4009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda013200980130033e000c74009a013e000c74009a013e000c74009a01320bd804f4009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda013200980130034e000c74009a013e000c74009a013e000c74009a013e000c74009a01320bd80634009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda013200980130035e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a01320bd80774009a01320bda0132009a01320bda0132009a01320bda0132009a01320bda013200980130036e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a01320bd808b4009a01320bda0132009a01320bda0132009a01320bda013200980130037e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a01320bd809f4009a01320bda0132009a01320bda013200980130038e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a01320bd80b34009a01320bda013200980130039e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a013e000c74009a01320bd80c7400960130032e700cf20090109ff5000f25060f0fbf5ba11"

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
// current scope boundary: a collection type without its own slice yet is refused, while the types
// that do have a slice (string, hash) round-trip through their own tests.
func TestDumpMissingAndArgs(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "DUMP", "nope")
	expect(t, rw, "$-1")
	cmd(t, rw, "DUMP")
	expect(t, rw, "-ERR wrong number of arguments for 'dump' command")

	// A set has no slice yet, so DUMP still refuses it.
	cmd(t, rw, "SADD", "set", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "DUMP", "set")
	expect(t, rw, "-ERR DUMP of this type is not supported yet")
}

// TestDumpRestoreHashRoundTrip dumps a hash and restores it under a fresh name, then reads every
// field back. The fields cover a plain string value, an integer value that int-encodes, the empty
// value, and a value with an embedded NUL and a high byte, so the RDB string forms inside the hash
// body all get exercised.
func TestDumpRestoreHashRoundTrip(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	fields := [][2]string{{"f1", "v1"}, {"n", "12345"}, {"empty", ""}, {"raw", "a\x00b\xffc"}}
	args := []string{"HSET", "src"}
	for _, fv := range fields {
		args = append(args, fv[0], fv[1])
	}
	cmd(t, rw, args...)
	expect(t, rw, ":4")

	cmd(t, rw, "DUMP", "src")
	reply := readReply(t, rw)
	if len(reply) < 1 || reply[0] != '$' {
		t.Fatalf("DUMP hash reply = %q, want a bulk", reply)
	}
	blob := reply[1:]

	cmd(t, rw, "DEL", "dst")
	drainReply(t, rw)
	cmd(t, rw, "RESTORE", "dst", "0", blob)
	expect(t, rw, "+OK")
	cmd(t, rw, "HLEN", "dst")
	expect(t, rw, ":4")
	for _, fv := range fields {
		cmd(t, rw, "HGET", "dst", fv[0])
		expect(t, rw, "$"+fv[1])
	}
}

// TestRestoreHashCrossEngine restores hash blobs produced by live Redis 8.8 and Valkey 9.1. Both
// servers write the listpack encoding (type 16) for a hash, small ones inline and large ones
// LZF-compressed, so this proves aki decodes the listpack form and the LZF wrapper both emit, across
// the two RDB versions and checksums.
func TestRestoreHashCrossEngine(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// Small hash {f1:v1, f2:v2} as an inline listpack, from each server.
	small := map[string][2]string{
		"redis-small":  {"101717000000040082663103827631038266320382763203ff0e0071a3947346e7b6cd", ""},
		"valkey-small": {"101717000000040082663103827631038266320382763203ff5000ccd2859d8b86c548", ""},
	}
	for name, tc := range small {
		cmd(t, rw, "DEL", name)
		drainReply(t, rw)
		cmd(t, rw, "RESTORE", name, "0", hexBlob(t, tc[0]))
		expect(t, rw, "+OK")
		cmd(t, rw, "HLEN", name)
		expect(t, rw, ":2")
		cmd(t, rw, "HGET", name, "f1")
		expect(t, rw, "$v1")
		cmd(t, rw, "HGET", name, "f2")
		expect(t, rw, "$v2")
	}

	// Large hash of 200 fields field1..field200 => value1..value200, which both servers emit as an
	// LZF-compressed listpack (type 16, LZF string). This drives the LZF decoder and the listpack
	// forward walk over a blob big enough to span every string encoding the packer chose.
	big := []string{
		hexBlob(t, redisBigHashHex),
		hexBlob(t, valkeyBigHashHex),
	}
	for i, blob := range big {
		key := "big" + string(rune('0'+i))
		cmd(t, rw, "DEL", key)
		drainReply(t, rw)
		cmd(t, rw, "RESTORE", key, "0", blob)
		expect(t, rw, "+OK")
		cmd(t, rw, "HLEN", key)
		expect(t, rw, ":200")
		cmd(t, rw, "HGET", key, "field1")
		expect(t, rw, "$value1")
		cmd(t, rw, "HGET", key, "field200")
		expect(t, rw, "$value200")
	}
}
