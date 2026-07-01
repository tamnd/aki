package f1srv

import "testing"

// INCRBYFLOAT adds a float to a string key, treating a missing key as zero and echoing the new
// value in Redis's human format. Every reply here was captured from live Redis 8.8.0 and Valkey
// 9.1.0, which agree byte for byte.
func TestIncrByFloat(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// A missing key starts from zero, so the increment is the result.
	cmd(t, rw, "INCRBYFLOAT", "n", "3.5")
	expect(t, rw, "$3.5")

	// 10.5 + 0.1 is the classic double-rounding case: the reply is the 17-fractional-digit form
	// with trailing zeros trimmed, the same bytes Redis prints.
	cmd(t, rw, "SET", "m", "10.5")
	expect(t, rw, "+OK")
	cmd(t, rw, "INCRBYFLOAT", "m", "0.1")
	expect(t, rw, "$10.59999999999999964")
	// GET reads back exactly what INCRBYFLOAT stored.
	cmd(t, rw, "GET", "m")
	expect(t, rw, "$10.59999999999999964")

	// Scientific and integer-valued results collapse to a bare integer with no decimal point.
	cmd(t, rw, "INCRBYFLOAT", "z", "5.0e3")
	expect(t, rw, "$5000")
	// A field driven back to zero prints "0", never "-0".
	cmd(t, rw, "INCRBYFLOAT", "s0", "5")
	expect(t, rw, "$5")
	cmd(t, rw, "INCRBYFLOAT", "s0", "-5")
	expect(t, rw, "$0")
}

// INCRBYFLOAT accepts every float form strtold does (leading '+', bare '.5', trailing '5.',
// scientific, hex) and rejects the forms it does not (spaces, empty, NaN, underscores, and
// literals that overflow or underflow the double range).
func TestIncrByFloatParsing(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	good := []struct{ arg, want string }{
		{".5", "$0.5"},
		{"5.", "$5"},
		{"+5", "$5"},
		{"3.0e3", "$3000"},
		{"0x10", "$16"},
		{"0x1.8p1", "$3"},
	}
	for _, g := range good {
		cmd(t, rw, "DEL", "k")
		readReply(t, rw)
		cmd(t, rw, "INCRBYFLOAT", "k", g.arg)
		expect(t, rw, g.want)
	}

	for _, bad := range []string{"nan", " 5", "5 ", "", "1_000", "1e400", "1e-400", "2e-324", "abc"} {
		cmd(t, rw, "DEL", "k")
		readReply(t, rw)
		cmd(t, rw, "INCRBYFLOAT", "k", bad)
		expect(t, rw, "-ERR value is not a valid float")
	}
}

// A stored value that is not a valid float cannot be incremented, an increment that would land
// on infinity is rejected before the write, and INCRBYFLOAT against a non-string key is
// WRONGTYPE. A rejected call leaves the key as it was.
func TestIncrByFloatErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "s", "hello")
	expect(t, rw, "+OK")
	cmd(t, rw, "INCRBYFLOAT", "s", "1")
	expect(t, rw, "-ERR value is not a valid float")
	// The value survived the rejected increment.
	cmd(t, rw, "GET", "s")
	expect(t, rw, "$hello")

	cmd(t, rw, "SET", "big", "1e308")
	expect(t, rw, "+OK")
	cmd(t, rw, "INCRBYFLOAT", "big", "1e308")
	expect(t, rw, "-ERR increment would produce NaN or Infinity")

	// An explicit infinity increment parses fine but fails once it lands in the result.
	cmd(t, rw, "INCRBYFLOAT", "inf", "inf")
	expect(t, rw, "-ERR increment would produce NaN or Infinity")

	cmd(t, rw, "RPUSH", "l", "x")
	expect(t, rw, ":1")
	cmd(t, rw, "INCRBYFLOAT", "l", "1")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")

	cmd(t, rw, "INCRBYFLOAT", "onlykey")
	expect(t, rw, "-ERR wrong number of arguments for 'incrbyfloat' command")
}

// INCRBYFLOAT keeps a key's TTL, since it rewrites the value without touching the expire row.
func TestIncrByFloatKeepsTTL(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "10", "EX", "100")
	expect(t, rw, "+OK")
	cmd(t, rw, "INCRBYFLOAT", "k", "0.5")
	expect(t, rw, "$10.5")
	cmd(t, rw, "TTL", "k")
	expect(t, rw, ":100")
}

// HINCRBYFLOAT is INCRBYFLOAT for a hash field: a missing field starts from zero and creates the
// field (and the hash), the reply is the human-format value, and HGET reads back the same bytes.
func TestHIncrByFloat(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HINCRBYFLOAT", "h", "f", "10.5")
	expect(t, rw, "$10.5")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":1")
	cmd(t, rw, "HINCRBYFLOAT", "h", "f", "0.1")
	expect(t, rw, "$10.59999999999999964")
	cmd(t, rw, "HGET", "h", "f")
	expect(t, rw, "$10.59999999999999964")

	// A second field starts from zero and bumps the count.
	cmd(t, rw, "HINCRBYFLOAT", "h", "g", "3")
	expect(t, rw, "$3")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":2")
}

// HINCRBYFLOAT's error surface: a field that is not a float is a hash-specific error, an
// infinite result is rejected, a bad increment is "not a valid float", WRONGTYPE for a string
// key, and a missing increment is an arity error.
func TestHIncrByFloatErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "bad", "abc", "big", "1e308")
	expect(t, rw, ":2")
	cmd(t, rw, "HINCRBYFLOAT", "h", "bad", "1")
	expect(t, rw, "-ERR hash value is not a float")
	cmd(t, rw, "HINCRBYFLOAT", "h", "big", "1e308")
	expect(t, rw, "-ERR increment would produce NaN or Infinity")
	cmd(t, rw, "HINCRBYFLOAT", "h", "f", "nan")
	expect(t, rw, "-ERR value is not a valid float")

	cmd(t, rw, "SET", "s", "x")
	expect(t, rw, "+OK")
	cmd(t, rw, "HINCRBYFLOAT", "s", "f", "1")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")

	cmd(t, rw, "HINCRBYFLOAT", "h", "f")
	expect(t, rw, "-ERR wrong number of arguments for 'hincrbyfloat' command")
}

// HINCRBYFLOAT keeps the hash's TTL, the same as the string form keeps a string key's TTL.
func TestHIncrByFloatKeepsTTL(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "f", "10")
	expect(t, rw, ":1")
	cmd(t, rw, "EXPIRE", "h", "100")
	expect(t, rw, ":1")
	cmd(t, rw, "HINCRBYFLOAT", "h", "f", "0.5")
	expect(t, rw, "$10.5")
	cmd(t, rw, "TTL", "h")
	expect(t, rw, ":100")
}
