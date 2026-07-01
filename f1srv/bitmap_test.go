package f1srv

import "testing"

// SETBIT grows the string to fit the addressed bit, returns the prior bit, and GETBIT reads it
// back; a bit past the current length reads 0.
func TestBitmapSetGetBit(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SETBIT", "k", "7", "1")
	expect(t, rw, ":0") // no prior bit
	cmd(t, rw, "GETBIT", "k", "7")
	expect(t, rw, ":1")
	cmd(t, rw, "GETBIT", "k", "0")
	expect(t, rw, ":0")

	// Setting an already-set bit returns the old value 1.
	cmd(t, rw, "SETBIT", "k", "7", "0")
	expect(t, rw, ":1")
	cmd(t, rw, "GETBIT", "k", "7")
	expect(t, rw, ":0")

	// A far bit grows the value; the raw string length reflects the highest touched byte.
	cmd(t, rw, "SETBIT", "k", "100", "1")
	expect(t, rw, ":0")
	cmd(t, rw, "GETBIT", "k", "100")
	expect(t, rw, ":1")
	cmd(t, rw, "GETBIT", "k", "100000")
	expect(t, rw, ":0")
}

// The bit-to-byte mapping is MSB-first: setting bit 0 makes byte 0 equal 0x80, which GET sees.
func TestBitmapBitToByteMapping(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SETBIT", "b", "0", "1")
	expect(t, rw, ":0")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$\x80") // one byte 0x80

	cmd(t, rw, "DEL", "b")
	expect(t, rw, ":1")
	cmd(t, rw, "SETBIT", "b", "7", "1")
	expect(t, rw, ":0")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$\x01") // one byte 0x01
}

// BITCOUNT counts set bits over the whole value and over BYTE and BIT ranges, including negative
// indexes, matching the canonical "foobar" -> 26 example.
func TestBitmapBitcount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "mykey", "foobar")
	expect(t, rw, "+OK")

	cmd(t, rw, "BITCOUNT", "mykey")
	expect(t, rw, ":26")
	cmd(t, rw, "BITCOUNT", "mykey", "0", "0")
	expect(t, rw, ":4")
	cmd(t, rw, "BITCOUNT", "mykey", "1", "1")
	expect(t, rw, ":6")
	cmd(t, rw, "BITCOUNT", "mykey", "0", "-1")
	expect(t, rw, ":26")
	cmd(t, rw, "BITCOUNT", "mykey", "5", "30", "BIT")
	expect(t, rw, ":17")
	cmd(t, rw, "BITCOUNT", "mykey", "0", "0", "BYTE")
	expect(t, rw, ":4")

	// A start past the end, and a reversed range, both count nothing.
	cmd(t, rw, "BITCOUNT", "mykey", "100", "200")
	expect(t, rw, ":0")
	cmd(t, rw, "BITCOUNT", "mykey", "5", "1")
	expect(t, rw, ":0")

	// A missing key counts zero; a lone start index is a syntax error (Redis 8.8 behavior).
	cmd(t, rw, "BITCOUNT", "nope")
	expect(t, rw, ":0")
	cmd(t, rw, "BITCOUNT", "mykey", "0")
	expect(t, rw, "-ERR syntax error")
}

// BITPOS finds the first set or clear bit, honors start/end and BYTE/BIT units, and returns the
// zero-padded tail position when hunting a clear bit with no explicit end.
func TestBitmapBitpos(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// 0xff 0xf0 0x00: first 0 bit is at index 12, first 1 bit at index 0.
	cmd(t, rw, "SET", "k", "\xff\xf0\x00")
	expect(t, rw, "+OK")
	cmd(t, rw, "BITPOS", "k", "0")
	expect(t, rw, ":12")
	cmd(t, rw, "BITPOS", "k", "1")
	expect(t, rw, ":0")
	cmd(t, rw, "BITPOS", "k", "1", "2")
	expect(t, rw, ":-1") // bytes 2.. are all zero
	cmd(t, rw, "BITPOS", "k", "1", "0", "-1", "BIT")
	expect(t, rw, ":0")

	// All ones, hunting a clear bit with no end: the answer is the first bit past the string.
	cmd(t, rw, "SET", "ones", "\xff\xff\xff")
	expect(t, rw, "+OK")
	cmd(t, rw, "BITPOS", "ones", "0")
	expect(t, rw, ":24")
	// With an explicit end the same search returns -1 rather than the padded tail.
	cmd(t, rw, "BITPOS", "ones", "0", "0", "-1")
	expect(t, rw, ":-1")

	// A missing key: a clear bit is found at 0, a set bit never.
	cmd(t, rw, "BITPOS", "missing", "0")
	expect(t, rw, ":0")
	cmd(t, rw, "BITPOS", "missing", "1")
	expect(t, rw, ":-1")
}

// A bitmap command against a key holding a collection fails with WRONGTYPE, and out-of-range or
// malformed offsets and bit values report the Redis-shaped errors.
func TestBitmapErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "x")
	expect(t, rw, ":1")
	cmd(t, rw, "SETBIT", "l", "0", "1")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "GETBIT", "l", "0")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "BITCOUNT", "l")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "BITPOS", "l", "0")
	expect(t, rw, "-"+wrongType)

	cmd(t, rw, "SETBIT", "k", "-1", "1")
	expect(t, rw, "-ERR bit offset is not an integer or out of range")
	cmd(t, rw, "SETBIT", "k", "4294967296", "1")
	expect(t, rw, "-ERR bit offset is not an integer or out of range")
	cmd(t, rw, "SETBIT", "k", "5", "2")
	expect(t, rw, "-ERR bit is not an integer or out of range")
	cmd(t, rw, "BITPOS", "k", "2")
	expect(t, rw, "-ERR The bit argument must be 1 or 0.")
}
