package f1srv

import "testing"

// Every reply expectation in this file was captured from a live Redis 8.8.0 on a scratch
// port before it was written, so the tests pin f1raw's TTL surface to Redis byte-for-byte
// (spec 2064/f1_rewrite_ltm/11). Where a reply is a live count of remaining seconds the
// test asserts the stable envelope Redis guarantees (100 for a fresh 100s TTL, -1 for no
// expiry, -2 for missing) rather than a sub-second-varying exact millisecond.

// TestExpireBasic covers the plain EXPIRE/TTL/PTTL/EXPIRETIME/PEXPIRETIME/PERSIST cycle.
func TestExpireBasic(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXPIRE", "k", "100")
	expect(t, rw, ":1")
	// A fresh 100s TTL reads exactly 100 on TTL (Redis rounds ms up by 500 before the
	// second divide) and something just under 100000 on PTTL.
	cmd(t, rw, "TTL", "k")
	expect(t, rw, ":100")
	cmd(t, rw, "PERSIST", "k")
	expect(t, rw, ":1")
	// Second PERSIST finds no expiry to clear.
	cmd(t, rw, "PERSIST", "k")
	expect(t, rw, ":0")
	cmd(t, rw, "TTL", "k")
	expect(t, rw, ":-1")

	// EXPIRE on a missing key is a no-op returning 0.
	cmd(t, rw, "EXPIRE", "missing", "100")
	expect(t, rw, ":0")
	cmd(t, rw, "TTL", "missing")
	expect(t, rw, ":-2")
	cmd(t, rw, "PTTL", "missing")
	expect(t, rw, ":-2")
	cmd(t, rw, "EXPIRETIME", "missing")
	expect(t, rw, ":-2")
	cmd(t, rw, "PEXPIRETIME", "missing")
	expect(t, rw, ":-2")

	// EXPIRETIME/PEXPIRETIME are -1 on a key with no expiry.
	cmd(t, rw, "EXPIRETIME", "k")
	expect(t, rw, ":-1")
	cmd(t, rw, "PEXPIRETIME", "k")
	expect(t, rw, ":-1")
}

// TestExpireConditions pins the NX/XX/GT/LT guard and its incompatibility errors to the
// exact Redis 8.8 replies. The TTL after each step confirms the guard let the write
// through or held it back as Redis does.
func TestExpireConditions(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "g", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXPIRE", "g", "100")
	expect(t, rw, ":1")

	// NX on a key that already has a TTL is refused.
	cmd(t, rw, "EXPIRE", "g", "200", "NX")
	expect(t, rw, ":0")
	// XX on a key that has a TTL is applied.
	cmd(t, rw, "EXPIRE", "g", "200", "XX")
	expect(t, rw, ":1")
	cmd(t, rw, "TTL", "g")
	expect(t, rw, ":200")
	// GT with a smaller new TTL does not fire; with a larger one it does.
	cmd(t, rw, "EXPIRE", "g", "100", "GT")
	expect(t, rw, ":0")
	cmd(t, rw, "EXPIRE", "g", "300", "GT")
	expect(t, rw, ":1")
	cmd(t, rw, "TTL", "g")
	expect(t, rw, ":300")
	// LT with a larger new TTL does not fire; with a smaller one it does.
	cmd(t, rw, "EXPIRE", "g", "500", "LT")
	expect(t, rw, ":0")
	cmd(t, rw, "EXPIRE", "g", "50", "LT")
	expect(t, rw, ":1")
	cmd(t, rw, "TTL", "g")
	expect(t, rw, ":50")

	// On a key with no current expiry, infinity is the current deadline: NX and LT fire,
	// GT does not.
	cmd(t, rw, "SET", "nx", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXPIRE", "nx", "100", "NX")
	expect(t, rw, ":1")

	cmd(t, rw, "SET", "gt", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXPIRE", "gt", "100", "GT")
	expect(t, rw, ":0")
	cmd(t, rw, "TTL", "gt")
	expect(t, rw, ":-1")

	cmd(t, rw, "SET", "lt", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXPIRE", "lt", "100", "LT")
	expect(t, rw, ":1")
	cmd(t, rw, "TTL", "lt")
	expect(t, rw, ":100")

	// The two incompatible pairings, in any order, error with the exact Redis strings.
	cmd(t, rw, "EXPIRE", "g", "100", "NX", "GT")
	expect(t, rw, "-ERR NX and XX, GT or LT options at the same time are not compatible")
	cmd(t, rw, "EXPIRE", "g", "100", "NX", "XX")
	expect(t, rw, "-ERR NX and XX, GT or LT options at the same time are not compatible")
	cmd(t, rw, "EXPIRE", "g", "100", "GT", "LT")
	expect(t, rw, "-ERR GT and LT options at the same time are not compatible")
	// A valid XX+GT combination is accepted: g has a 50s TTL, XX is satisfied and 100 is
	// greater than the current 50s deadline, so the extend fires.
	cmd(t, rw, "EXPIRE", "g", "100", "XX", "GT")
	expect(t, rw, ":1")
	// An unrecognized option errors before the compatibility check.
	cmd(t, rw, "EXPIRE", "g", "100", "ZZ")
	expect(t, rw, "-ERR Unsupported option ZZ")
	// Wrong arity.
	cmd(t, rw, "EXPIRE", "g")
	expect(t, rw, "-ERR wrong number of arguments")
}

// TestExpireLazyReap covers the past-deadline delete: a computed deadline at or before now
// deletes the key immediately and still returns 1, and a PEXPIREAT to the past makes the
// key read as gone on the very next touch (lazy expiry, spec 11 section 3).
func TestExpireLazyReap(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// A negative relative EXPIRE deletes now and returns 1.
	cmd(t, rw, "SET", "p", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXPIRE", "p", "-1")
	expect(t, rw, ":1")
	cmd(t, rw, "EXISTS", "p")
	expect(t, rw, ":0")
	cmd(t, rw, "TTL", "p")
	expect(t, rw, ":-2")

	// PEXPIREAT to an absolute past ms: the key is dead on the next read.
	cmd(t, rw, "SET", "q", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "PEXPIREAT", "q", "1")
	expect(t, rw, ":1")
	cmd(t, rw, "GET", "q")
	expect(t, rw, "$-1")
	cmd(t, rw, "EXISTS", "q")
	expect(t, rw, ":0")
	cmd(t, rw, "TTL", "q")
	expect(t, rw, ":-2")

	// A future PEXPIREAT sets a real TTL that EXPIRETIME/PEXPIRETIME read back absolutely.
	cmd(t, rw, "SET", "r", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "PEXPIREAT", "r", "9999999999000")
	expect(t, rw, ":1")
	cmd(t, rw, "EXPIRETIME", "r")
	expect(t, rw, ":9999999999")
	cmd(t, rw, "PEXPIRETIME", "r")
	expect(t, rw, ":9999999999000")
}

// TestSetClearsTTL confirms a plain SET drops any existing TTL (spec 11 section 2.5), the
// same as Redis with no KEEPTTL.
func TestSetClearsTTL(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "s", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXPIRE", "s", "100")
	expect(t, rw, ":1")
	cmd(t, rw, "TTL", "s")
	expect(t, rw, ":100")
	// Rewriting the value clears the TTL.
	cmd(t, rw, "SET", "s", "w")
	expect(t, rw, "+OK")
	cmd(t, rw, "TTL", "s")
	expect(t, rw, ":-1")
}

// TestSetOptions covers SET's EX/PX/EXAT/PXAT/KEEPTTL/NX/XX/GET options and their errors,
// all pinned to Redis 8.8.
func TestSetOptions(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// The four expiry units set a real TTL that the readers see.
	cmd(t, rw, "SET", "k", "v1", "EX", "100")
	expect(t, rw, "+OK")
	cmd(t, rw, "TTL", "k")
	expect(t, rw, ":100")
	cmd(t, rw, "SET", "k", "v3", "EXAT", "9999999999")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXPIRETIME", "k")
	expect(t, rw, ":9999999999")
	cmd(t, rw, "SET", "k", "v4", "PXAT", "9999999999000")
	expect(t, rw, "+OK")
	cmd(t, rw, "PEXPIRETIME", "k")
	expect(t, rw, ":9999999999000")
	// KEEPTTL preserves the deadline; a plain SET clears it.
	cmd(t, rw, "SET", "k", "v5", "KEEPTTL")
	expect(t, rw, "+OK")
	cmd(t, rw, "PEXPIRETIME", "k")
	expect(t, rw, ":9999999999000")
	cmd(t, rw, "SET", "k", "v6")
	expect(t, rw, "+OK")
	cmd(t, rw, "TTL", "k")
	expect(t, rw, ":-1")

	// GET returns the old value, or nil when the key was absent.
	cmd(t, rw, "SET", "gk", "old")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "gk", "new", "GET")
	expect(t, rw, "$old")
	cmd(t, rw, "GET", "gk")
	expect(t, rw, "$new")
	cmd(t, rw, "SET", "nope", "val", "GET")
	expect(t, rw, "$-1")

	// NX only on a missing key, XX only on an existing key.
	cmd(t, rw, "SET", "n1", "v", "NX")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "n1", "v2", "NX")
	expect(t, rw, "$-1")
	cmd(t, rw, "GET", "n1")
	expect(t, rw, "$v")
	cmd(t, rw, "SET", "n1", "v3", "XX")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "x1", "v", "XX")
	expect(t, rw, "$-1")

	// NX GET on an existing key returns the old value and does not write.
	cmd(t, rw, "SET", "gk2", "old")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "gk2", "new", "NX", "GET")
	expect(t, rw, "$old")
	cmd(t, rw, "GET", "gk2")
	expect(t, rw, "$old")

	// A past absolute deadline writes then reaps: the key is gone on the next read.
	cmd(t, rw, "SET", "past", "v", "EXAT", "1")
	expect(t, rw, "+OK")
	cmd(t, rw, "GET", "past")
	expect(t, rw, "$-1")

	// Error surface, byte-identical to Redis.
	cmd(t, rw, "SET", "k", "v", "EX", "10", "PX", "10000")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "SET", "k", "v", "EX", "10", "KEEPTTL")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "SET", "k", "v", "NX", "XX")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "SET", "k", "v", "EX")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "SET", "k", "v", "EX", "abc")
	expect(t, rw, "-ERR value is not an integer or out of range")
	cmd(t, rw, "SET", "k", "v", "EX", "0")
	expect(t, rw, "-ERR invalid expire time in 'set' command")
	cmd(t, rw, "SET", "k", "v", "EX", "-5")
	expect(t, rw, "-ERR invalid expire time in 'set' command")
	cmd(t, rw, "SET", "k", "v", "FOO")
	expect(t, rw, "-ERR syntax error")

	// GET against a key holding a non-string is WRONGTYPE and writes nothing.
	cmd(t, rw, "RPUSH", "lst", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "SET", "lst", "v", "GET")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
}

// TestGetEx covers GETEX's read-and-retTTL behavior, its options, and its errors, pinned to
// Redis 8.8.
func TestGetEx(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "g", "hello")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXPIRE", "g", "100")
	expect(t, rw, ":1")
	// A no-option GETEX returns the value and leaves the TTL alone.
	cmd(t, rw, "GETEX", "g")
	expect(t, rw, "$hello")
	cmd(t, rw, "TTL", "g")
	expect(t, rw, ":100")
	// EX changes the TTL, PERSIST removes it.
	cmd(t, rw, "GETEX", "g", "EX", "200")
	expect(t, rw, "$hello")
	cmd(t, rw, "TTL", "g")
	expect(t, rw, ":200")
	cmd(t, rw, "GETEX", "g", "PERSIST")
	expect(t, rw, "$hello")
	cmd(t, rw, "TTL", "g")
	expect(t, rw, ":-1")
	// EXAT sets an absolute deadline.
	cmd(t, rw, "GETEX", "g", "EXAT", "9999999999")
	expect(t, rw, "$hello")
	cmd(t, rw, "EXPIRETIME", "g")
	expect(t, rw, ":9999999999")

	// A missing key is a nil reply, with or without an option, and no key is created.
	cmd(t, rw, "GETEX", "missing")
	expect(t, rw, "$-1")
	cmd(t, rw, "GETEX", "missing", "EX", "100")
	expect(t, rw, "$-1")
	cmd(t, rw, "EXISTS", "missing")
	expect(t, rw, ":0")

	// A past deadline reads the value then deletes the key.
	cmd(t, rw, "SET", "p", "bye")
	expect(t, rw, "+OK")
	cmd(t, rw, "GETEX", "p", "EXAT", "1")
	expect(t, rw, "$bye")
	cmd(t, rw, "EXISTS", "p")
	expect(t, rw, ":0")

	// Error surface.
	cmd(t, rw, "GETEX", "g", "EX", "10", "PERSIST")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "GETEX", "g", "EX", "10", "PX", "100")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "GETEX", "g", "EX")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "GETEX", "g", "EX", "0")
	expect(t, rw, "-ERR invalid expire time in 'getex' command")
	cmd(t, rw, "GETEX", "g", "EX", "-1")
	expect(t, rw, "-ERR invalid expire time in 'getex' command")
	cmd(t, rw, "GETEX", "g", "KEEPTTL")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "GETEX")
	expect(t, rw, "-ERR wrong number of arguments for 'getex' command")

	// GETEX on a non-string is WRONGTYPE.
	cmd(t, rw, "RPUSH", "lst", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "GETEX", "lst")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
}
