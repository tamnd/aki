package f1srv

import "testing"

// TYPE reports the type name of a key, or "none" when the key is missing.
func TestTypeReportsKind(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "TYPE", "missing")
	expect(t, rw, "+none")

	cmd(t, rw, "SET", "str", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "TYPE", "str")
	expect(t, rw, "+string")

	cmd(t, rw, "HSET", "h", "f", "v")
	expect(t, rw, ":1")
	cmd(t, rw, "TYPE", "h")
	expect(t, rw, "+hash")

	cmd(t, rw, "SADD", "s", "m")
	expect(t, rw, ":1")
	cmd(t, rw, "TYPE", "s")
	expect(t, rw, "+set")

	cmd(t, rw, "TYPE")
	expect(t, rw, "-ERR wrong number of arguments for 'type' command")
}

// A set starts intset when its first member is an integer and stays intset while it holds only
// integers under the entry limit.
func TestObjectEncodingSetIntset(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "ints", "1", "2", "3")
	expect(t, rw, ":3")
	cmd(t, rw, "OBJECT", "ENCODING", "ints")
	expect(t, rw, "$intset")

	// A member with a leading zero is not an integer to Redis, so it breaks the intset.
	cmd(t, rw, "SADD", "leadzero", "1", "007")
	expect(t, rw, ":2")
	cmd(t, rw, "OBJECT", "ENCODING", "leadzero")
	expect(t, rw, "$listpack")
}

// A non-integer first (or later) member makes a small set listpack, and a member longer than
// the listpack value limit pushes it to hashtable.
func TestObjectEncodingSetListpackAndHashtable(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "words", "alpha", "beta")
	expect(t, rw, ":2")
	cmd(t, rw, "OBJECT", "ENCODING", "words")
	expect(t, rw, "$listpack")

	// A 65-byte member exceeds set-max-listpack-value (64), so the set is hashtable.
	long := make([]byte, 65)
	for i := range long {
		long[i] = 'x'
	}
	cmd(t, rw, "SADD", "big", string(long))
	expect(t, rw, ":1")
	cmd(t, rw, "OBJECT", "ENCODING", "big")
	expect(t, rw, "$hashtable")
}

// Growing past set-max-listpack-entries (128) upgrades a listpack set to hashtable, and the
// upgrade is one-way: removing members back under the limit keeps it hashtable.
func TestObjectEncodingSetUpgradeIsOneWay(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// 129 non-integer members: over the listpack entry limit, so hashtable.
	args := []string{"SADD", "many"}
	for i := 0; i < 129; i++ {
		args = append(args, "m"+itoa(i))
	}
	cmd(t, rw, args...)
	expect(t, rw, ":129")
	cmd(t, rw, "OBJECT", "ENCODING", "many")
	expect(t, rw, "$hashtable")

	// Remove almost everything: still hashtable, never downgrades.
	rem := []string{"SREM", "many"}
	for i := 0; i < 128; i++ {
		rem = append(rem, "m"+itoa(i))
	}
	cmd(t, rw, rem...)
	expect(t, rw, ":128")
	cmd(t, rw, "OBJECT", "ENCODING", "many")
	expect(t, rw, "$hashtable")
}

// A STORE result carries the encoding its members imply: an all-integer intersection is intset,
// a word union is listpack.
func TestObjectEncodingStoreResult(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "1", "2", "3")
	expect(t, rw, ":3")
	cmd(t, rw, "SADD", "b", "2", "3", "4")
	expect(t, rw, ":3")
	cmd(t, rw, "SINTERSTORE", "dstints", "a", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "OBJECT", "ENCODING", "dstints")
	expect(t, rw, "$intset")

	cmd(t, rw, "SADD", "c", "x", "y")
	expect(t, rw, ":2")
	cmd(t, rw, "SUNIONSTORE", "dstwords", "c", "a")
	expect(t, rw, ":5")
	cmd(t, rw, "OBJECT", "ENCODING", "dstwords")
	expect(t, rw, "$listpack")
}

// String encodings: int for a canonical integer, embstr for a short string, raw for a long one.
func TestObjectEncodingString(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "n", "12345")
	expect(t, rw, "+OK")
	cmd(t, rw, "OBJECT", "ENCODING", "n")
	expect(t, rw, "$int")

	cmd(t, rw, "SET", "short", "hello")
	expect(t, rw, "+OK")
	cmd(t, rw, "OBJECT", "ENCODING", "short")
	expect(t, rw, "$embstr")

	long := make([]byte, 45)
	for i := range long {
		long[i] = 'y'
	}
	cmd(t, rw, "SET", "long", string(long))
	expect(t, rw, "+OK")
	cmd(t, rw, "OBJECT", "ENCODING", "long")
	expect(t, rw, "$raw")
}

// OBJECT ENCODING/REFCOUNT/IDLETIME/FREQ on a missing key reply with a nil bulk, matching
// Redis 8.8 and Valkey 9.1 (both look the key up first and return the null reply). For a
// present key REFCOUNT and IDLETIME answer, while FREQ refuses under the non-LFU default
// policy exactly as the references do.
func TestObjectMiscForms(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "OBJECT", "ENCODING", "missing")
	expect(t, rw, "$-1")

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "OBJECT", "REFCOUNT", "k")
	expect(t, rw, ":1")
	cmd(t, rw, "OBJECT", "IDLETIME", "k")
	expect(t, rw, ":0")
	cmd(t, rw, "OBJECT", "FREQ", "k")
	expect(t, rw, "-ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.")
	cmd(t, rw, "OBJECT", "REFCOUNT", "missing")
	expect(t, rw, "$-1")
	cmd(t, rw, "OBJECT", "IDLETIME", "missing")
	expect(t, rw, "$-1")
	cmd(t, rw, "OBJECT", "FREQ", "missing")
	expect(t, rw, "$-1")
}

// parseInt64Strict matches Redis string2ll: it accepts canonical integers (including -0) and
// rejects empty, lone sign, leading '+', leading zeros, and overflow.
func TestParseInt64Strict(t *testing.T) {
	ok := []struct {
		in   string
		want int64
	}{
		{"0", 0}, {"-0", 0}, {"7", 7}, {"-7", -7}, {"12345", 12345},
		{"9223372036854775807", 9223372036854775807},
		{"-9223372036854775808", -9223372036854775808},
	}
	for _, c := range ok {
		v, got := parseInt64Strict([]byte(c.in))
		if !got || v != c.want {
			t.Fatalf("parseInt64Strict(%q) = %d,%v, want %d,true", c.in, v, got, c.want)
		}
	}
	bad := []string{"", "-", "+", "+1", "007", "-05", "1a", "a1", " 1", "1 ", "9223372036854775808", "-9223372036854775809"}
	for _, s := range bad {
		if _, got := parseInt64Strict([]byte(s)); got {
			t.Fatalf("parseInt64Strict(%q) = true, want false", s)
		}
	}
}
