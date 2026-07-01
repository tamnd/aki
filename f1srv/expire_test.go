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

// TestTypedLazyExpiry confirms a typed read sees an expired key as absent, the way Redis
// runs expireIfNeeded on every key lookup (spec 11 section 3). The point-path handlers
// read their element rows directly, so without the dispatch-boundary reap an expired hash
// would still answer HGET; PEXPIREAT to an absolute past ms makes each key dead
// deterministically, no sleep. Every reply here matches live Redis 8.8.
func TestTypedLazyExpiry(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// Build one key of every type, then expire each to an absolute past ms.
	cmd(t, rw, "HSET", "h", "f1", "v1", "f2", "v2")
	expect(t, rw, ":2")
	cmd(t, rw, "SADD", "s", "a", "b", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "RPUSH", "l", "x", "y", "z")
	expect(t, rw, ":3")
	cmd(t, rw, "SETBIT", "bm", "5", "1")
	expect(t, rw, ":0")
	for _, k := range []string{"h", "s", "z", "l", "bm"} {
		cmd(t, rw, "PEXPIREAT", k, "1")
		expect(t, rw, ":1")
	}

	// Hash reads see nothing.
	cmd(t, rw, "HGET", "h", "f1")
	expect(t, rw, "$-1")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":0")
	cmd(t, rw, "HEXISTS", "h", "f1")
	expect(t, rw, ":0")
	cmd(t, rw, "HGETALL", "h")
	expect(t, rw, "*0")

	// Set reads see nothing.
	cmd(t, rw, "SISMEMBER", "s", "a")
	expect(t, rw, ":0")
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":0")
	cmd(t, rw, "SMEMBERS", "s")
	expect(t, rw, "*0")

	// Zset reads see nothing.
	cmd(t, rw, "ZSCORE", "z", "a")
	expect(t, rw, "$-1")
	cmd(t, rw, "ZCARD", "z")
	expect(t, rw, ":0")
	cmd(t, rw, "ZRANGE", "z", "0", "-1")
	expect(t, rw, "*0")

	// List reads see nothing.
	cmd(t, rw, "LLEN", "l")
	expect(t, rw, ":0")
	cmd(t, rw, "LINDEX", "l", "0")
	expect(t, rw, "$-1")
	cmd(t, rw, "LRANGE", "l", "0", "-1")
	expect(t, rw, "*0")

	// Bitmap reads see a zero bitmap.
	cmd(t, rw, "GETBIT", "bm", "5")
	expect(t, rw, ":0")
	cmd(t, rw, "BITCOUNT", "bm")
	expect(t, rw, ":0")

	// TYPE and DBSIZE agree the whole keyspace is empty.
	cmd(t, rw, "TYPE", "h")
	expect(t, rw, "+none")
	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":0")

	// A write on an expired key starts fresh: the old rows are gone, the new field is 1.
	cmd(t, rw, "HSET", "h", "g", "w")
	expect(t, rw, ":1")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":1")
	cmd(t, rw, "TTL", "h")
	expect(t, rw, ":-1")
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

// TestStringTTLFamily covers the string commands that set or drop a value together with its
// TTL: SETEX, PSETEX, SETNX, GETDEL, and GETSET. Every reply here was captured from live
// Redis 8.8.0.
func TestStringTTLFamily(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// SETEX writes the value and a seconds TTL, readable back through TTL.
	cmd(t, rw, "SETEX", "a", "100", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "GET", "a")
	expect(t, rw, "$v")
	cmd(t, rw, "TTL", "a")
	expect(t, rw, ":100")
	// PSETEX is the millisecond form.
	cmd(t, rw, "PSETEX", "b", "100000", "w")
	expect(t, rw, "+OK")
	cmd(t, rw, "TTL", "b")
	expect(t, rw, ":100")
	// SETEX overwrites any existing type, and the key reads back as a string.
	cmd(t, rw, "RPUSH", "lst", "x")
	expect(t, rw, ":1")
	cmd(t, rw, "SETEX", "lst", "100", "now-a-string")
	expect(t, rw, "+OK")
	cmd(t, rw, "TYPE", "lst")
	expect(t, rw, "+string")
	cmd(t, rw, "GET", "lst")
	expect(t, rw, "$now-a-string")

	// SETEX / PSETEX time must be strictly positive, checked before any write.
	cmd(t, rw, "SET", "keep", "orig")
	expect(t, rw, "+OK")
	cmd(t, rw, "SETEX", "keep", "0", "z")
	expect(t, rw, "-ERR invalid expire time in 'setex' command")
	cmd(t, rw, "SETEX", "keep", "-1", "z")
	expect(t, rw, "-ERR invalid expire time in 'setex' command")
	cmd(t, rw, "PSETEX", "keep", "0", "z")
	expect(t, rw, "-ERR invalid expire time in 'psetex' command")
	cmd(t, rw, "GET", "keep")
	expect(t, rw, "$orig")
	cmd(t, rw, "SETEX", "keep", "notanint", "z")
	expect(t, rw, "-ERR value is not an integer or out of range")
	cmd(t, rw, "SETEX", "keep", "100")
	expect(t, rw, "-ERR wrong number of arguments for 'setex' command")
	cmd(t, rw, "PSETEX", "keep", "100")
	expect(t, rw, "-ERR wrong number of arguments for 'psetex' command")

	// SETNX writes only when the key is absent, and carries no TTL.
	cmd(t, rw, "SETNX", "n", "first")
	expect(t, rw, ":1")
	cmd(t, rw, "TTL", "n")
	expect(t, rw, ":-1")
	cmd(t, rw, "SETNX", "n", "second")
	expect(t, rw, ":0")
	cmd(t, rw, "GET", "n")
	expect(t, rw, "$first")
	cmd(t, rw, "SETNX", "n")
	expect(t, rw, "-ERR wrong number of arguments for 'setnx' command")
	// An expired key is absent to SETNX.
	cmd(t, rw, "SET", "gone", "old")
	expect(t, rw, "+OK")
	cmd(t, rw, "PEXPIREAT", "gone", "1")
	expect(t, rw, ":1")
	cmd(t, rw, "SETNX", "gone", "fresh")
	expect(t, rw, ":1")
	cmd(t, rw, "GET", "gone")
	expect(t, rw, "$fresh")

	// GETDEL returns the value and deletes the key, clearing its TTL.
	cmd(t, rw, "SETEX", "gd", "100", "bye")
	expect(t, rw, "+OK")
	cmd(t, rw, "GETDEL", "gd")
	expect(t, rw, "$bye")
	cmd(t, rw, "EXISTS", "gd")
	expect(t, rw, ":0")
	cmd(t, rw, "GETDEL", "gd")
	expect(t, rw, "$-1")
	// GETDEL on a non-string is WRONGTYPE and does not delete.
	cmd(t, rw, "RPUSH", "gdlst", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "GETDEL", "gdlst")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
	cmd(t, rw, "EXISTS", "gdlst")
	expect(t, rw, ":1")
	cmd(t, rw, "GETDEL")
	expect(t, rw, "-ERR wrong number of arguments for 'getdel' command")

	// GETSET returns the old value, sets the new one, and clears any TTL.
	cmd(t, rw, "SETEX", "gs", "100", "old")
	expect(t, rw, "+OK")
	cmd(t, rw, "GETSET", "gs", "new")
	expect(t, rw, "$old")
	cmd(t, rw, "GET", "gs")
	expect(t, rw, "$new")
	cmd(t, rw, "TTL", "gs")
	expect(t, rw, ":-1")
	cmd(t, rw, "GETSET", "fresh", "created")
	expect(t, rw, "$-1")
	cmd(t, rw, "GET", "fresh")
	expect(t, rw, "$created")
	cmd(t, rw, "GETSET", "gdlst", "z")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
	cmd(t, rw, "GETSET", "gs")
	expect(t, rw, "-ERR wrong number of arguments for 'getset' command")
}
