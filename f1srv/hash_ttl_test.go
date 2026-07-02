package f1srv

import (
	"bufio"
	"testing"
	"time"
)

// Every reply expectation in this file was captured from a live Redis 8.8.0 on a scratch
// port before it was written, so the tests pin f1raw's hash-field-TTL surface to Redis
// byte-for-byte (spec 2064/f1_rewrite_ltm/12). Where a reply is a live count of remaining
// seconds or milliseconds the test asserts the stable envelope Redis guarantees (100 for a
// fresh 100s field TTL, -1 for a field with no TTL, -2 for a missing field, and the flag
// codes for HPERSIST) rather than a sub-second-varying exact value. Lazy expiry is verified
// with a short real sleep past a millisecond deadline: f1raw reaps an expired field on the
// next point read, so the observable outcome after the sleep is deterministic.

// TestHashFieldTTLLifecycle covers HEXPIRE, HTTL, HPTTL, HEXPIRETIME, HPEXPIRETIME, and
// HPERSIST across a set field, a no-TTL field, and a missing field.
func TestHashFieldTTLLifecycle(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "f1", "v1", "f2", "v2", "f3", "v3")
	expect(t, rw, ":3")

	// HEXPIRE returns *1 with :1 (applied) for a field that exists and takes a TTL.
	cmd(t, rw, "HEXPIRE", "h", "100", "FIELDS", "1", "f1")
	expect(t, rw, "*1")
	expect(t, rw, ":1")

	// HTTL of the field with a fresh 100s TTL reads 100.
	cmd(t, rw, "HTTL", "h", "FIELDS", "1", "f1")
	expect(t, rw, "*1")
	expect(t, rw, ":100")

	// HPTTL reads within (99000, 100000] ms; assert the stable ceiling of 100000.
	cmd(t, rw, "HPTTL", "h", "FIELDS", "1", "f1")
	expect(t, rw, "*1")
	expectIntNear(t, rw, 100000, 2000)

	// HEXPIRETIME/HPEXPIRETIME are absolute; assert they are in the future, not exact.
	cmd(t, rw, "HEXPIRETIME", "h", "FIELDS", "1", "f1")
	expect(t, rw, "*1")
	expectIntPositive(t, rw)
	cmd(t, rw, "HPEXPIRETIME", "h", "FIELDS", "1", "f1")
	expect(t, rw, "*1")
	expectIntPositive(t, rw)

	// A field with no TTL reads -1; a missing field reads -2.
	cmd(t, rw, "HTTL", "h", "FIELDS", "1", "f2")
	expect(t, rw, "*1")
	expect(t, rw, ":-1")
	cmd(t, rw, "HTTL", "h", "FIELDS", "1", "nope")
	expect(t, rw, "*1")
	expect(t, rw, ":-2")

	// HPERSIST removes the TTL: :1 for a field that had one, :-1 for one that did not,
	// :-2 for a missing field. After persisting, HTTL reads -1.
	cmd(t, rw, "HPERSIST", "h", "FIELDS", "1", "f1")
	expect(t, rw, "*1")
	expect(t, rw, ":1")
	cmd(t, rw, "HTTL", "h", "FIELDS", "1", "f1")
	expect(t, rw, "*1")
	expect(t, rw, ":-1")
	cmd(t, rw, "HPERSIST", "h", "FIELDS", "1", "f2")
	expect(t, rw, "*1")
	expect(t, rw, ":-1")
	cmd(t, rw, "HPERSIST", "h", "FIELDS", "1", "nope")
	expect(t, rw, "*1")
	expect(t, rw, ":-2")
}

// TestHashFieldTTLConditions covers the NX/XX/GT/LT guards on HEXPIRE.
func TestHashFieldTTLConditions(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1")
	expect(t, rw, ":1")

	// NX sets only when no TTL exists: first applies, second is rejected (:0).
	cmd(t, rw, "HEXPIRE", "h", "100", "NX", "FIELDS", "1", "a")
	expect(t, rw, "*1")
	expect(t, rw, ":1")
	cmd(t, rw, "HEXPIRE", "h", "200", "NX", "FIELDS", "1", "a")
	expect(t, rw, "*1")
	expect(t, rw, ":0")

	// XX sets only when a TTL already exists: applies here.
	cmd(t, rw, "HEXPIRE", "h", "200", "XX", "FIELDS", "1", "a")
	expect(t, rw, "*1")
	expect(t, rw, ":1")

	// GT sets only when the new TTL is greater: a lower value is rejected, a higher one applies.
	cmd(t, rw, "HEXPIRE", "h", "50", "GT", "FIELDS", "1", "a")
	expect(t, rw, "*1")
	expect(t, rw, ":0")
	cmd(t, rw, "HEXPIRE", "h", "500", "GT", "FIELDS", "1", "a")
	expect(t, rw, "*1")
	expect(t, rw, ":1")

	// LT sets only when the new TTL is smaller: a higher value is rejected, a lower one applies.
	cmd(t, rw, "HEXPIRE", "h", "9999", "LT", "FIELDS", "1", "a")
	expect(t, rw, "*1")
	expect(t, rw, ":0")
	cmd(t, rw, "HEXPIRE", "h", "10", "LT", "FIELDS", "1", "a")
	expect(t, rw, "*1")
	expect(t, rw, ":1")
}

// TestHashFieldTTLPastDeletes checks that a past deadline deletes the field immediately,
// returning :2 (the delete flag), and that lazy expiry drops a field once its ms TTL elapses.
func TestHashFieldTTLPastDeletes(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "x", "1", "y", "2")
	expect(t, rw, ":2")

	// HEXPIREAT in the past deletes the field now and reports :2.
	cmd(t, rw, "HEXPIREAT", "h", "1", "FIELDS", "1", "x")
	expect(t, rw, "*1")
	expect(t, rw, ":2")
	cmd(t, rw, "HEXISTS", "h", "x")
	expect(t, rw, ":0")
	cmd(t, rw, "HGET", "h", "x")
	expect(t, rw, "$-1")

	// The other field is still live; the hash still exists.
	cmd(t, rw, "HGET", "h", "y")
	expect(t, rw, "$2")

	// A near-future ms TTL: the field is present before it elapses and gone after.
	cmd(t, rw, "HPEXPIRE", "h", "30", "FIELDS", "1", "y")
	expect(t, rw, "*1")
	expect(t, rw, ":1")
	time.Sleep(80 * time.Millisecond)
	cmd(t, rw, "HGET", "h", "y")
	expect(t, rw, "$-1")
	cmd(t, rw, "HEXISTS", "h", "y")
	expect(t, rw, ":0")
	// The hash is empty now, so the key is gone.
	cmd(t, rw, "EXISTS", "h")
	expect(t, rw, ":0")
}

// TestHashFieldTTLHGetEx covers HGETEX read-and-set-TTL and HGETEX PERSIST.
func TestHashFieldTTLHGetEx(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "f", "val", "g", "gval")
	expect(t, rw, ":2")

	// HGETEX with no TTL option is a plain multi-get.
	cmd(t, rw, "HGETEX", "h", "FIELDS", "1", "f")
	expect(t, rw, "*1")
	expect(t, rw, "$val")

	// HGETEX EX sets the TTL and returns the value; HTTL then reads 100.
	cmd(t, rw, "HGETEX", "h", "EX", "100", "FIELDS", "1", "f")
	expect(t, rw, "*1")
	expect(t, rw, "$val")
	cmd(t, rw, "HTTL", "h", "FIELDS", "1", "f")
	expect(t, rw, "*1")
	expect(t, rw, ":100")

	// HGETEX PERSIST clears the TTL; HTTL then reads -1.
	cmd(t, rw, "HGETEX", "h", "PERSIST", "FIELDS", "1", "f")
	expect(t, rw, "*1")
	expect(t, rw, "$val")
	cmd(t, rw, "HTTL", "h", "FIELDS", "1", "f")
	expect(t, rw, "*1")
	expect(t, rw, ":-1")

	// A missing field returns a nil element.
	cmd(t, rw, "HGETEX", "h", "FIELDS", "1", "zzz")
	expect(t, rw, "*1")
	expect(t, rw, "$-1")
}

// TestHashFieldTTLHGetDel covers HGETDEL returning the value and removing the field.
func TestHashFieldTTLHGetDel(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "d1", "a", "d2", "b", "d3", "c")
	expect(t, rw, ":3")

	cmd(t, rw, "HGETDEL", "h", "FIELDS", "2", "d1", "d2")
	expect(t, rw, "*2")
	expect(t, rw, "$a")
	expect(t, rw, "$b")

	cmd(t, rw, "HEXISTS", "h", "d1")
	expect(t, rw, ":0")
	cmd(t, rw, "HGET", "h", "d3")
	expect(t, rw, "$c")

	// A missing field returns a nil element and deletes nothing.
	cmd(t, rw, "HGETDEL", "h", "FIELDS", "1", "gone")
	expect(t, rw, "*1")
	expect(t, rw, "$-1")

	// Deleting the last field removes the hash key.
	cmd(t, rw, "HGETDEL", "h", "FIELDS", "1", "d3")
	expect(t, rw, "*1")
	expect(t, rw, "$c")
	cmd(t, rw, "EXISTS", "h")
	expect(t, rw, ":0")
}

// TestHashFieldTTLOverwriteDropsTTL checks that re-setting a field with HSET clears its TTL,
// matching Redis: a plain HSET writes a fresh field with no expiry.
func TestHashFieldTTLOverwriteDropsTTL(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "f", "v")
	expect(t, rw, ":1")
	cmd(t, rw, "HEXPIRE", "h", "100", "FIELDS", "1", "f")
	expect(t, rw, "*1")
	expect(t, rw, ":1")
	cmd(t, rw, "HSET", "h", "f", "v2")
	expect(t, rw, ":0")
	cmd(t, rw, "HTTL", "h", "FIELDS", "1", "f")
	expect(t, rw, "*1")
	expect(t, rw, ":-1")
}

// TestHashFieldTTLRenameCarries checks RENAME moves per-field TTLs to the destination and
// leaves no orphan under the source.
func TestHashFieldTTLRenameCarries(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "src", "f1", "v1", "f2", "v2")
	expect(t, rw, ":2")
	cmd(t, rw, "HEXPIRE", "src", "100", "FIELDS", "1", "f1")
	expect(t, rw, "*1")
	expect(t, rw, ":1")
	cmd(t, rw, "RENAME", "src", "dst")
	expect(t, rw, "+OK")

	// The TTL follows f1 to dst; f2 stays TTL-free.
	cmd(t, rw, "HTTL", "dst", "FIELDS", "1", "f1")
	expect(t, rw, "*1")
	expect(t, rw, ":100")
	cmd(t, rw, "HTTL", "dst", "FIELDS", "1", "f2")
	expect(t, rw, "*1")
	expect(t, rw, ":-1")
	cmd(t, rw, "EXISTS", "src")
	expect(t, rw, ":0")
}

// TestHashFieldTTLCopyCarries checks COPY duplicates per-field TTLs to the destination and
// leaves the source's TTLs in place.
func TestHashFieldTTLCopyCarries(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "src", "f1", "v1", "f2", "v2")
	expect(t, rw, ":2")
	cmd(t, rw, "HEXPIRE", "src", "100", "FIELDS", "1", "f1")
	expect(t, rw, "*1")
	expect(t, rw, ":1")
	cmd(t, rw, "COPY", "src", "cpy")
	expect(t, rw, ":1")

	cmd(t, rw, "HTTL", "cpy", "FIELDS", "1", "f1")
	expect(t, rw, "*1")
	expect(t, rw, ":100")
	// The source keeps its TTL after the copy.
	cmd(t, rw, "HTTL", "src", "FIELDS", "1", "f1")
	expect(t, rw, "*1")
	expect(t, rw, ":100")
}

// TestHashFieldTTLErrors pins the three Redis 8.8 error families for the FIELDS clause:
// setters (HEXPIRE), readers (HTTL), and HGETEX, plus the shared negative/overflow cases.
func TestHashFieldTTLErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "f", "v", "g", "w")
	expect(t, rw, ":2")

	// Setter family (HEXPIRE): too few fields is an arity error; a bad numfields is the
	// numFields message; a wrong token where FIELDS is expected is unknown-argument.
	cmd(t, rw, "HEXPIRE", "h", "100", "FIELDS", "2", "f")
	expect(t, rw, "-ERR wrong number of arguments")
	cmd(t, rw, "HEXPIRE", "h", "100", "FIELDS", "0", "f")
	expect(t, rw, "-ERR Parameter `numFields` should be greater than 0")
	cmd(t, rw, "HEXPIRE", "h", "100", "NOPE", "1", "f")
	expect(t, rw, "-ERR unknown argument: NOPE")
	cmd(t, rw, "HEXPIRE", "h", "-5", "FIELDS", "1", "f")
	expect(t, rw, "-ERR invalid expire time, must be >= 0")

	// Reader family (HTTL): any count mismatch is the numfields-must-match message; a bad
	// numfields is the positive-integer message; a wrong token is the FIELDS-position message.
	cmd(t, rw, "HTTL", "h", "FIELDS", "2", "f")
	expect(t, rw, "-ERR The `numfields` parameter must match the number of arguments")
	cmd(t, rw, "HTTL", "h", "FIELDS", "0", "f")
	expect(t, rw, "-ERR Number of fields must be a positive integer")
	cmd(t, rw, "HTTL", "h", "NOPE", "1", "f")
	expect(t, rw, "-ERR Mandatory argument FIELDS is missing or not at the right position")

	// HGETEX family: a bad numfields is its own message; a second TTL option is the one-of
	// error; a negative EX is the shared negative-time message.
	cmd(t, rw, "HGETEX", "h", "FIELDS", "0", "f")
	expect(t, rw, "-ERR invalid number of fields")
	cmd(t, rw, "HGETEX", "h", "EX", "100", "PERSIST", "FIELDS", "1", "f")
	expect(t, rw, "-ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified")
	cmd(t, rw, "HGETEX", "h", "EX", "-5", "FIELDS", "1", "f")
	expect(t, rw, "-ERR invalid expire time, must be >= 0")

	// Overflow on a setter is the invalid-expire-time-in-command message.
	cmd(t, rw, "HEXPIRE", "h", "9999999999999999", "FIELDS", "1", "f")
	expect(t, rw, "-ERR invalid expire time in 'hexpire' command")
}

// TestHashFieldTTLWrongType checks the type guard: a hash-field-TTL command on a string key
// is a WRONGTYPE error, and on a missing key HEXPIRE reports -2 for every field.
func TestHashFieldTTLWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "str", "x")
	expect(t, rw, "+OK")
	cmd(t, rw, "HEXPIRE", "str", "100", "FIELDS", "1", "f")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
	cmd(t, rw, "HTTL", "str", "FIELDS", "1", "f")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")

	// A missing key reports -2 for each requested field.
	cmd(t, rw, "HTTL", "nokey", "FIELDS", "1", "f")
	expect(t, rw, "*1")
	expect(t, rw, ":-2")
}

// expectIntNear reads a ":N" reply and fails unless N is within tol of want. It is for the
// live-millisecond reads (HPTTL) whose exact value drifts sub-second between the set and the read.
func expectIntNear(t *testing.T, rw *bufio.ReadWriter, want, tol int) {
	t.Helper()
	got := readReply(t, rw)
	if len(got) == 0 || got[0] != ':' {
		t.Fatalf("reply = %q, want an integer near %d", got, want)
	}
	n := atoiSigned(got[1:])
	if n < want-tol || n > want+tol {
		t.Fatalf("reply = %q, want within %d of %d", got, tol, want)
	}
}

// expectIntPositive reads a ":N" reply and fails unless N is strictly positive. It is for the
// absolute-time reads (HEXPIRETIME/HPEXPIRETIME) whose exact value is a wall-clock deadline.
func expectIntPositive(t *testing.T, rw *bufio.ReadWriter) {
	t.Helper()
	got := readReply(t, rw)
	if len(got) == 0 || got[0] != ':' {
		t.Fatalf("reply = %q, want a positive integer", got)
	}
	if atoiSigned(got[1:]) <= 0 {
		t.Fatalf("reply = %q, want a positive integer", got)
	}
}

// atoiSigned parses a base-10 signed integer from a reply body, no error path: the callers
// only pass RESP integer payloads.
func atoiSigned(s string) int {
	neg := false
	i := 0
	if len(s) > 0 && s[0] == '-' {
		neg = true
		i = 1
	}
	n := 0
	for ; i < len(s); i++ {
		n = n*10 + int(s[i]-'0')
	}
	if neg {
		return -n
	}
	return n
}
