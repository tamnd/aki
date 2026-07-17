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
// not-yet answer for a collection key, which cannot carry a key TTL yet.
func TestExpireErrorsAndCollection(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "EXPIRE", "k", "notanint")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")
	send(t, nc, "EXPIRE", "k", "100", "BOGUS")
	expect(t, br, "-ERR Unsupported option BOGUS\r\n")

	// A set key is present but cannot hold a TTL yet: an honest not-yet error,
	// never a 0 that would falsely mean the key is absent.
	send(t, nc, "SADD", "s", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "EXPIRE", "s", "100")
	expect(t, br, "-ERR EXPIRE on a set key is not supported yet\r\n")
}
