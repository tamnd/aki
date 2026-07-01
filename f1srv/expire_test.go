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
