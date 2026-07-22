package drivers

import "testing"

// TestIncrexDefault increments by 1 from an absent key (starting at 0) and
// replies the [new value, actual increment] pair each time.
func TestIncrexDefault(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "INCREX", "k")
	expect(t, br, "*2\r\n:1\r\n:1\r\n")
	send(t, nc, "INCREX", "k")
	expect(t, br, "*2\r\n:2\r\n:1\r\n")
}

// TestIncrexByInt increments by an explicit integer, including a negative
// decrement.
func TestIncrexByInt(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "100")
	expect(t, br, "+OK\r\n")
	send(t, nc, "INCREX", "k", "BYINT", "5")
	expect(t, br, "*2\r\n:105\r\n:5\r\n")
	send(t, nc, "INCREX", "k", "BYINT", "-10")
	expect(t, br, "*2\r\n:95\r\n:-10\r\n")
}

// TestIncrexByFloat increments by a float and replies the pair as RESP2 bulk
// strings.
func TestIncrexByFloat(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "1.5")
	expect(t, br, "+OK\r\n")
	send(t, nc, "INCREX", "k", "BYFLOAT", "0.25")
	expect(t, br, "*2\r\n$4\r\n1.75\r\n$4\r\n0.25\r\n")
}

// TestIncrexBoundsSkip skips the write when the result would cross a bound and
// replies [current, 0], leaving the key untouched.
func TestIncrexBoundsSkip(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "99")
	expect(t, br, "+OK\r\n")
	send(t, nc, "INCREX", "k", "BYINT", "5", "UBOUND", "100")
	expect(t, br, "*2\r\n:99\r\n:0\r\n")
	// Untouched.
	send(t, nc, "GET", "k")
	expect(t, br, "$2\r\n99\r\n")
}

// TestIncrexSaturate caps the result at the crossed bound and reports the
// saturated delta.
func TestIncrexSaturate(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "99")
	expect(t, br, "+OK\r\n")
	send(t, nc, "INCREX", "k", "BYINT", "5", "UBOUND", "100", "SATURATE")
	expect(t, br, "*2\r\n:100\r\n:1\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$3\r\n100\r\n")
}

// TestIncrexExpiry installs a TTL with EX and clears it with PERSIST.
func TestIncrexExpiry(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "INCREX", "k", "BYINT", "1", "EX", "100")
	expect(t, br, "*2\r\n:1\r\n:1\r\n")
	send(t, nc, "TTL", "k")
	if ttl := readIntReply(t, br, "TTL"); ttl < 90 || ttl > 100 {
		t.Fatalf("TTL = %d, want near 100", ttl)
	}

	send(t, nc, "INCREX", "k", "BYINT", "1", "PERSIST")
	expect(t, br, "*2\r\n:2\r\n:1\r\n")
	send(t, nc, "TTL", "k")
	expect(t, br, ":-1\r\n")
}

// TestIncrexENX sets the TTL only when the key currently has none: it leaves an
// existing TTL in place while still applying the increment, and installs the TTL
// on a fresh key.
func TestIncrexENX(t *testing.T) {
	_, nc, br := startServer(t)

	// Existing TTL far in the future: ENX applies the increment but does not
	// reset the deadline to the smaller value.
	send(t, nc, "SET", "k", "10", "EX", "500")
	expect(t, br, "+OK\r\n")
	send(t, nc, "INCREX", "k", "BYINT", "1", "EX", "10", "ENX")
	expect(t, br, "*2\r\n:11\r\n:1\r\n")
	send(t, nc, "TTL", "k")
	if ttl := readIntReply(t, br, "TTL"); ttl < 400 {
		t.Fatalf("TTL = %d, want the original ~500 left intact", ttl)
	}

	// Fresh key with no TTL: ENX installs the deadline.
	send(t, nc, "INCREX", "fresh", "BYINT", "1", "EX", "100", "ENX")
	expect(t, br, "*2\r\n:1\r\n:1\r\n")
	send(t, nc, "TTL", "fresh")
	if ttl := readIntReply(t, br, "TTL"); ttl < 90 || ttl > 100 {
		t.Fatalf("TTL = %d, want near 100", ttl)
	}
}

// TestIncrexErrors rejects a non-integer stored value, an inverted bound pair,
// and a bad expiry, and BYINT rejects a stored float.
func TestIncrexErrors(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "abc")
	expect(t, br, "+OK\r\n")
	send(t, nc, "INCREX", "k")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")

	send(t, nc, "SET", "f", "1.5")
	expect(t, br, "+OK\r\n")
	send(t, nc, "INCREX", "f", "BYINT", "1")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")

	send(t, nc, "INCREX", "n", "LBOUND", "10", "UBOUND", "5")
	expect(t, br, "-ERR LBOUND must be less than or equal to UBOUND\r\n")

	send(t, nc, "INCREX", "n", "EX", "0")
	expect(t, br, "-ERR invalid expire time in 'increx' command\r\n")

	send(t, nc, "INCREX", "n", "ENX")
	expect(t, br, "-ERR syntax error\r\n")
}
