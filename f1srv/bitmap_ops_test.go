package f1srv

import "testing"

// BITOP folds source strings with AND/OR/XOR/NOT into a destination, zero-padding shorter sources
// to the longest, and reports the result length.
func TestBitopFold(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "abc") // 0x61 0x62 0x63
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "b", "abd") // 0x61 0x62 0x64
	expect(t, rw, "+OK")

	// AND of "abc" and "abd": 0x61 & 0x61, 0x62 & 0x62, 0x63 & 0x64 = 0x61 0x62 0x60.
	cmd(t, rw, "BITOP", "AND", "d", "a", "b")
	expect(t, rw, ":3")
	cmd(t, rw, "GET", "d")
	expect(t, rw, "$\x61\x62\x60")

	// OR: 0x63 | 0x64 = 0x67.
	cmd(t, rw, "BITOP", "OR", "d", "a", "b")
	expect(t, rw, ":3")
	cmd(t, rw, "GET", "d")
	expect(t, rw, "$\x61\x62\x67")

	// XOR: 0x00 0x00 0x07.
	cmd(t, rw, "BITOP", "XOR", "d", "a", "b")
	expect(t, rw, ":3")
	cmd(t, rw, "GET", "d")
	expect(t, rw, "$\x00\x00\x07")

	// NOT over "abc": complement each byte.
	cmd(t, rw, "BITOP", "NOT", "d", "a")
	expect(t, rw, ":3")
	cmd(t, rw, "GET", "d")
	expect(t, rw, "$\x9e\x9d\x9c")
}

// A shorter source is zero-padded to the longest source; the result takes the longest length.
func TestBitopPadding(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "long", "\xff\xff\xff\xff")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "short", "\x0f")
	expect(t, rw, "+OK")

	// AND with the zero-padded short key: 0x0f then three zero bytes.
	cmd(t, rw, "BITOP", "AND", "dst", "long", "short")
	expect(t, rw, ":4")
	cmd(t, rw, "GET", "dst")
	expect(t, rw, "$\x0f\x00\x00\x00")

	// OR keeps the long key's tail intact.
	cmd(t, rw, "BITOP", "OR", "dst", "long", "short")
	expect(t, rw, ":4")
	cmd(t, rw, "GET", "dst")
	expect(t, rw, "$\xff\xff\xff\xff")
}

// An all-missing fold produces an empty result: the destination is deleted and 0 returned.
func TestBitopEmptyDeletesDest(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "dst", "seed")
	expect(t, rw, "+OK")
	cmd(t, rw, "BITOP", "AND", "dst", "nope1", "nope2")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "dst")
	expect(t, rw, ":0")
}

// BITOP rejects a bad operation, a NOT with more than one source, and a non-string source.
func TestBitopErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "x")
	expect(t, rw, "+OK")
	cmd(t, rw, "BITOP", "NAND", "d", "a")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "BITOP", "NOT", "d", "a", "a")
	expect(t, rw, "-ERR BITOP NOT must be called with a single source key.")

	cmd(t, rw, "RPUSH", "l", "y")
	expect(t, rw, ":1")
	cmd(t, rw, "BITOP", "AND", "d", "a", "l")
	expect(t, rw, "-"+wrongType)
}

// BITFIELD GET/SET/INCRBY round-trip a signed field: SET returns the old value, INCRBY the new,
// GET the current, and an unset field reads 0.
func TestBitfieldGetSetIncr(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// A fresh key: GET reads 0.
	cmd(t, rw, "BITFIELD", "bf", "GET", "i8", "0")
	expect(t, rw, "*1")
	expect(t, rw, ":0")

	// SET i8 #0 100 returns the old value 0.
	cmd(t, rw, "BITFIELD", "bf", "SET", "i8", "#0", "100")
	expect(t, rw, "*1")
	expect(t, rw, ":0")

	// INCRBY i8 #0 10 -> 110, returned as the new value.
	cmd(t, rw, "BITFIELD", "bf", "INCRBY", "i8", "#0", "10")
	expect(t, rw, "*1")
	expect(t, rw, ":110")

	// GET reflects the stored value.
	cmd(t, rw, "BITFIELD", "bf", "GET", "i8", "#0")
	expect(t, rw, "*1")
	expect(t, rw, ":110")

	// Multiple ops in one call reply in order.
	cmd(t, rw, "BITFIELD", "bf", "SET", "u8", "#0", "255", "GET", "u8", "#0")
	expect(t, rw, "*2")
	expect(t, rw, ":110") // old value of the i8/u8 byte
	expect(t, rw, ":255")
}

// OVERFLOW WRAP, SAT, and FAIL each change how an INCRBY past the type range resolves.
func TestBitfieldOverflow(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// WRAP (the default): i8 at 127, +10 wraps to -119.
	cmd(t, rw, "BITFIELD", "w", "SET", "i8", "0", "127", "INCRBY", "i8", "0", "10")
	expect(t, rw, "*2")
	expect(t, rw, ":0")
	expect(t, rw, ":-119")

	// SAT: i8 at 120, +100 saturates to 127.
	cmd(t, rw, "BITFIELD", "s", "OVERFLOW", "SAT", "SET", "i8", "0", "120", "INCRBY", "i8", "0", "100")
	expect(t, rw, "*2")
	expect(t, rw, ":0")
	expect(t, rw, ":127")

	// FAIL: u8 at 255, +1 fails, replying nil, and the field keeps 255.
	cmd(t, rw, "BITFIELD", "f", "OVERFLOW", "FAIL", "SET", "u8", "0", "255", "INCRBY", "u8", "0", "1", "GET", "u8", "0")
	expect(t, rw, "*3")
	expect(t, rw, ":0")
	expect(t, rw, "$-1")
	expect(t, rw, ":255")
}

// BITFIELD_RO answers GET but rejects any write subcommand.
func TestBitfieldReadOnly(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "BITFIELD", "ro", "SET", "u16", "0", "515")
	expect(t, rw, "*1")
	expect(t, rw, ":0")
	cmd(t, rw, "BITFIELD_RO", "ro", "GET", "u16", "0")
	expect(t, rw, "*1")
	expect(t, rw, ":515")
	cmd(t, rw, "BITFIELD_RO", "ro", "SET", "u16", "0", "1")
	expect(t, rw, "-ERR BITFIELD_RO only supports the GET subcommand")
}

// BITFIELD reports the Redis-shaped errors for a bad type, a bad offset, and a non-string key.
func TestBitfieldErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "BITFIELD", "bf", "GET", "x8", "0")
	expect(t, rw, "-ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
	cmd(t, rw, "BITFIELD", "bf", "GET", "u64", "0")
	expect(t, rw, "-ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
	cmd(t, rw, "BITFIELD", "bf", "GET", "i8", "-1")
	expect(t, rw, "-ERR bit offset is not an integer or out of range")
	cmd(t, rw, "BITFIELD", "bf", "SOMETHING", "i8", "0")
	expect(t, rw, "-ERR syntax error")

	cmd(t, rw, "RPUSH", "l", "y")
	expect(t, rw, ":1")
	cmd(t, rw, "BITFIELD", "l", "GET", "i8", "0")
	expect(t, rw, "-"+wrongType)
}
