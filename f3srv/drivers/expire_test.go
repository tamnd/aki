package drivers

import "testing"

// TestExpireStringBasics drives EXPIRE/PEXPIRE and the read side together: a set
// key with no TTL reads -1, EXPIRE installs one that TTL/PTTL then report, and
// PERSIST clears it back to -1. A missing key is 0 from EXPIRE and -2 from TTL.
func TestExpireStringBasics(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "TTL", "k")
	expect(t, br, ":-1\r\n")

	send(t, nc, "EXPIRE", "k", "100")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "k")
	expect(t, br, ":100\r\n")

	// PEXPIRE takes milliseconds; PTTL reports close to the set value.
	send(t, nc, "PEXPIRE", "k", "50000")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "k")
	expect(t, br, ":50\r\n")

	send(t, nc, "PERSIST", "k")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "k")
	expect(t, br, ":-1\r\n")

	// A missing key: EXPIRE reports 0, TTL reports -2.
	send(t, nc, "EXPIRE", "gone", "100")
	expect(t, br, ":0\r\n")
	send(t, nc, "TTL", "gone")
	expect(t, br, ":-2\r\n")
}

// TestExpirePastDeletes checks the documented quirk: EXPIRE with a past or
// non-positive instant deletes the key and still returns 1, both for the
// relative forms and the absolute EXPIREAT/PEXPIREAT.
func TestExpirePastDeletes(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "a", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "EXPIRE", "a", "-1")
	expect(t, br, ":1\r\n")
	send(t, nc, "EXISTS", "a")
	expect(t, br, ":0\r\n")

	send(t, nc, "SET", "b", "v")
	expect(t, br, "+OK\r\n")
	// EXPIREAT with an absolute instant far in the past deletes b.
	send(t, nc, "EXPIREAT", "b", "1")
	expect(t, br, ":1\r\n")
	send(t, nc, "EXISTS", "b")
	expect(t, br, ":0\r\n")

	send(t, nc, "SET", "c", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "PEXPIREAT", "c", "1000")
	expect(t, br, ":1\r\n")
	send(t, nc, "EXISTS", "c")
	expect(t, br, ":0\r\n")
}

// TestExpireFlags exercises NX/XX/GT/LT against the current deadline, the
// infinite-TTL treatment of a key with no expiry, and the incompatible-flag
// error.
func TestExpireFlags(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")

	// NX sets only when there is no TTL: first sets, second is refused.
	send(t, nc, "EXPIRE", "k", "100", "NX")
	expect(t, br, ":1\r\n")
	send(t, nc, "EXPIRE", "k", "200", "NX")
	expect(t, br, ":0\r\n")
	send(t, nc, "TTL", "k")
	expect(t, br, ":100\r\n")

	// GT sets only a strictly greater deadline; a smaller one is refused.
	send(t, nc, "EXPIRE", "k", "50", "GT")
	expect(t, br, ":0\r\n")
	send(t, nc, "EXPIRE", "k", "300", "GT")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "k")
	expect(t, br, ":300\r\n")

	// LT sets only a strictly smaller deadline.
	send(t, nc, "EXPIRE", "k", "400", "LT")
	expect(t, br, ":0\r\n")
	send(t, nc, "EXPIRE", "k", "120", "LT")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "k")
	expect(t, br, ":120\r\n")

	// XX needs an existing TTL: after PERSIST it refuses, and GT on a persistent
	// key (infinite TTL) also refuses since nothing is greater than infinity.
	send(t, nc, "PERSIST", "k")
	expect(t, br, ":1\r\n")
	send(t, nc, "EXPIRE", "k", "100", "XX")
	expect(t, br, ":0\r\n")
	send(t, nc, "EXPIRE", "k", "100", "GT")
	expect(t, br, ":0\r\n")
	// LT on a persistent key sets, since any finite TTL is less than infinity.
	send(t, nc, "EXPIRE", "k", "100", "LT")
	expect(t, br, ":1\r\n")

	// NX with another condition is the incompatible-flag error.
	send(t, nc, "EXPIRE", "k", "100", "NX", "GT")
	expect(t, br, "-ERR NX and XX, GT or LT options at the same time are not compatible\r\n")
}

// TestExpireErrorsAndCollection checks the argument errors and the honest
// not-yet answer for a collection type that cannot carry a key TTL yet.
func TestExpireErrorsAndCollection(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "EXPIRE", "k", "notanint")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")
	send(t, nc, "EXPIRE", "k", "100", "BOGUS")
	expect(t, br, "-ERR Unsupported option BOGUS\r\n")

	// A stream key is present but cannot hold a TTL yet: an honest not-yet error,
	// never a 0 that would falsely mean the key is absent. Set, zset, hash, and list
	// keys are supported now, so the interim error is tested against the stream, the
	// one type that still is not.
	// An explicit id keeps the XADD reply deterministic so the bulk reads cleanly.
	send(t, nc, "XADD", "st", "1-1", "f", "v")
	expectBulk(t, br, []byte("1-1"))
	send(t, nc, "EXPIRE", "st", "100")
	expect(t, br, "-ERR EXPIRE on a stream key is not supported yet\r\n")
}

// TestExpireSetKey drives the EXPIRE family over a set key end to end: the
// inline deadline reads back through TTL, PERSIST clears it, the flags gate the
// same way strings do, and a past instant deletes the set.
func TestExpireSetKey(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SADD", "s", "a", "b")
	expect(t, br, ":2\r\n")
	send(t, nc, "TTL", "s")
	expect(t, br, ":-1\r\n")

	// EXPIRE installs a deadline TTL then reports; the members survive it.
	send(t, nc, "EXPIRE", "s", "100")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "s")
	expect(t, br, ":100\r\n")
	send(t, nc, "SCARD", "s")
	expect(t, br, ":2\r\n")

	// GT only raises the deadline; a lower one is refused, a higher one takes.
	send(t, nc, "EXPIRE", "s", "50", "GT")
	expect(t, br, ":0\r\n")
	send(t, nc, "EXPIRE", "s", "300", "GT")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "s")
	expect(t, br, ":300\r\n")

	// PERSIST clears the deadline, then XX (needs a TTL) refuses.
	send(t, nc, "PERSIST", "s")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "s")
	expect(t, br, ":-1\r\n")
	send(t, nc, "EXPIRE", "s", "100", "XX")
	expect(t, br, ":0\r\n")

	// A past instant deletes the set and still returns 1.
	send(t, nc, "PEXPIREAT", "s", "1000")
	expect(t, br, ":1\r\n")
	send(t, nc, "EXISTS", "s")
	expect(t, br, ":0\r\n")
	send(t, nc, "TTL", "s")
	expect(t, br, ":-2\r\n")
}

// TestExpireZsetKey drives the EXPIRE family over a zset key end to end, the same
// shape as the set case: the inline deadline reads back through TTL, PERSIST
// clears it, the flags gate the same way strings do, and a past instant deletes
// the zset.
func TestExpireZsetKey(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "ZADD", "z", "1", "a", "2", "b")
	expect(t, br, ":2\r\n")
	send(t, nc, "TTL", "z")
	expect(t, br, ":-1\r\n")

	// EXPIRE installs a deadline TTL then reports; the members survive it.
	send(t, nc, "EXPIRE", "z", "100")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "z")
	expect(t, br, ":100\r\n")
	send(t, nc, "ZCARD", "z")
	expect(t, br, ":2\r\n")

	// GT only raises the deadline; a lower one is refused, a higher one takes.
	send(t, nc, "EXPIRE", "z", "50", "GT")
	expect(t, br, ":0\r\n")
	send(t, nc, "EXPIRE", "z", "300", "GT")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "z")
	expect(t, br, ":300\r\n")

	// PERSIST clears the deadline, then XX (needs a TTL) refuses.
	send(t, nc, "PERSIST", "z")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "z")
	expect(t, br, ":-1\r\n")
	send(t, nc, "EXPIRE", "z", "100", "XX")
	expect(t, br, ":0\r\n")

	// A past instant deletes the zset and still returns 1.
	send(t, nc, "PEXPIREAT", "z", "1000")
	expect(t, br, ":1\r\n")
	send(t, nc, "EXISTS", "z")
	expect(t, br, ":0\r\n")
	send(t, nc, "TTL", "z")
	expect(t, br, ":-2\r\n")
}

// TestExpireHashKey drives the key-level EXPIRE family over a hash key end to end,
// the same shape as the set and zset cases. The key-level deadline is distinct from
// a per-field HEXPIRE TTL: EXPIRE sets the whole-key deadline, PERSIST clears it,
// and a past instant drops the whole hash.
func TestExpireHashKey(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "HSET", "h", "f", "1", "g", "2")
	expect(t, br, ":2\r\n")
	send(t, nc, "TTL", "h")
	expect(t, br, ":-1\r\n")

	// EXPIRE installs a key deadline TTL then reports; the fields survive it.
	send(t, nc, "EXPIRE", "h", "100")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "h")
	expect(t, br, ":100\r\n")
	send(t, nc, "HLEN", "h")
	expect(t, br, ":2\r\n")

	// GT only raises the deadline; a lower one is refused, a higher one takes.
	send(t, nc, "EXPIRE", "h", "50", "GT")
	expect(t, br, ":0\r\n")
	send(t, nc, "EXPIRE", "h", "300", "GT")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "h")
	expect(t, br, ":300\r\n")

	// PERSIST clears the key deadline, then XX (needs a TTL) refuses.
	send(t, nc, "PERSIST", "h")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "h")
	expect(t, br, ":-1\r\n")
	send(t, nc, "EXPIRE", "h", "100", "XX")
	expect(t, br, ":0\r\n")

	// A past instant deletes the whole hash and still returns 1.
	send(t, nc, "PEXPIREAT", "h", "1000")
	expect(t, br, ":1\r\n")
	send(t, nc, "EXISTS", "h")
	expect(t, br, ":0\r\n")
	send(t, nc, "TTL", "h")
	expect(t, br, ":-2\r\n")
}

// TestExpireListKey drives the EXPIRE family over a list key end to end, the same
// shape as the set and zset cases: the inline deadline reads back through TTL,
// PERSIST clears it, the flags gate the same way strings do, and a past instant
// deletes the list.
func TestExpireListKey(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RPUSH", "l", "a", "b")
	expect(t, br, ":2\r\n")
	send(t, nc, "TTL", "l")
	expect(t, br, ":-1\r\n")

	// EXPIRE installs a deadline TTL then reports; the elements survive it.
	send(t, nc, "EXPIRE", "l", "100")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "l")
	expect(t, br, ":100\r\n")
	send(t, nc, "LLEN", "l")
	expect(t, br, ":2\r\n")

	// GT only raises the deadline; a lower one is refused, a higher one takes.
	send(t, nc, "EXPIRE", "l", "50", "GT")
	expect(t, br, ":0\r\n")
	send(t, nc, "EXPIRE", "l", "300", "GT")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "l")
	expect(t, br, ":300\r\n")

	// PERSIST clears the deadline, then XX (needs a TTL) refuses.
	send(t, nc, "PERSIST", "l")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "l")
	expect(t, br, ":-1\r\n")
	send(t, nc, "EXPIRE", "l", "100", "XX")
	expect(t, br, ":0\r\n")

	// A past instant deletes the list and still returns 1.
	send(t, nc, "PEXPIREAT", "l", "1000")
	expect(t, br, ":1\r\n")
	send(t, nc, "EXISTS", "l")
	expect(t, br, ":0\r\n")
	send(t, nc, "TTL", "l")
	expect(t, br, ":-2\r\n")
}
