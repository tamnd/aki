package f1srv

import "testing"

// HINCRBY adds a signed integer to a hash field, creating the field and the hash when
// absent. Every reply and error here was captured from live Redis 8.8.0.
func TestHIncrBy(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// A missing field on a missing hash starts from zero, so the increment is the result and
	// the hash comes into existence with one field.
	cmd(t, rw, "HINCRBY", "h", "f", "10")
	expect(t, rw, ":10")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":1")

	// A second increment on the same field accumulates, a negative one subtracts, and neither
	// changes the field count.
	cmd(t, rw, "HINCRBY", "h", "f", "5")
	expect(t, rw, ":15")
	cmd(t, rw, "HINCRBY", "h", "f", "-20")
	expect(t, rw, ":-5")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":1")

	// The stored value reads back as the canonical decimal, so HGET agrees with the last reply.
	cmd(t, rw, "HGET", "h", "f")
	expect(t, rw, "$-5")

	// A new field on an existing hash starts from zero too and bumps the count.
	cmd(t, rw, "HINCRBY", "h", "g", "3")
	expect(t, rw, ":3")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":2")
}

// The increment argument follows Redis's strict integer rules: no '+', no leading zeros, no
// spaces, no fractional part, and it must fit in an int64.
func TestHIncrByBadIncrement(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	for _, bad := range []string{"+5", "007", "5.0", "9223372036854775808", "-9223372036854775809", "abc", " 5", "5 "} {
		cmd(t, rw, "HINCRBY", "h", "f", bad)
		expect(t, rw, "-ERR value is not an integer or out of range")
	}
	// A rejected increment never creates the field or the hash.
	cmd(t, rw, "EXISTS", "h")
	expect(t, rw, ":0")
}

// A field that does not hold a strict integer cannot be incremented, and the same strict
// rules apply: a stored leading-zero or space-padded value is not an integer.
func TestHIncrByBadStored(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "f", "007", "g", "3 ", "w", "notanumber")
	expect(t, rw, ":3")
	for _, field := range []string{"f", "g", "w"} {
		cmd(t, rw, "HINCRBY", "h", field, "1")
		expect(t, rw, "-ERR hash value is not an integer")
	}
}

// A sum past the int64 range is rejected before the write, in both directions, and the
// field keeps its old value. The most negative int64 is reachable, one more overflows.
func TestHIncrByOverflow(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "f", "9223372036854775807")
	expect(t, rw, ":1")
	cmd(t, rw, "HINCRBY", "h", "f", "1")
	expect(t, rw, "-ERR increment or decrement would overflow")
	cmd(t, rw, "HGET", "h", "f")
	expect(t, rw, "$9223372036854775807")

	// Walk a fresh field to the most negative int64, which is in range, then one past it.
	cmd(t, rw, "HINCRBY", "h", "n", "-9223372036854775808")
	expect(t, rw, ":-9223372036854775808")
	cmd(t, rw, "HINCRBY", "h", "n", "-1")
	expect(t, rw, "-ERR increment or decrement would overflow")
}

// HINCRBY against a string key is WRONGTYPE, and a missing third argument is an arity error.
func TestHIncrByErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "s", "x")
	expect(t, rw, "+OK")
	cmd(t, rw, "HINCRBY", "s", "f", "1")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")

	cmd(t, rw, "HINCRBY", "h", "f")
	expect(t, rw, "-ERR wrong number of arguments for 'hincrby' command")
}

// HRANDFIELD's no-count form returns one field drawn from the hash, nil for a missing key,
// and never removes anything.
func TestHRandFieldNoCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HRANDFIELD", "nope")
	expect(t, rw, "$-1")

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2", "c", "3", "d", "4", "e", "5")
	expect(t, rw, ":5")

	fields := map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true}
	for i := 0; i < 50; i++ {
		cmd(t, rw, "HRANDFIELD", "h")
		got := readReply(t, rw)
		if len(got) == 0 || got[0] != '$' {
			t.Fatalf("no-count reply = %q, want a bulk string", got)
		}
		if !fields[got[1:]] {
			t.Fatalf("drew %q, not a field of the hash", got[1:])
		}
	}
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":5")
}

// A positive count returns distinct fields capped at the cardinality; a zero count and a
// missing key are both empty arrays.
func TestHRandFieldPositiveCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2", "c", "3", "d", "4", "e", "5")
	expect(t, rw, ":5")

	fields := map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true}
	for trial := 0; trial < 20; trial++ {
		cmd(t, rw, "HRANDFIELD", "h", "3")
		got := readArray(t, rw)
		if len(got) != 3 {
			t.Fatalf("count 3 returned %d fields, want 3", len(got))
		}
		seen := map[string]bool{}
		for _, f := range got {
			if !fields[f] {
				t.Fatalf("returned %q, not a field", f)
			}
			if seen[f] {
				t.Fatalf("returned %q twice, positive count must be distinct", f)
			}
			seen[f] = true
		}
	}

	// A count over the cardinality caps at the whole hash, still distinct.
	cmd(t, rw, "HRANDFIELD", "h", "10")
	got := readArray(t, rw)
	if len(got) != 5 {
		t.Fatalf("count 10 over a 5-field hash returned %d, want 5", len(got))
	}

	cmd(t, rw, "HRANDFIELD", "h", "0")
	expect(t, rw, "*0")
	cmd(t, rw, "HRANDFIELD", "nope", "3")
	expect(t, rw, "*0")
	cmd(t, rw, "HRANDFIELD", "nope", "-3")
	expect(t, rw, "*0")
}

// A negative count returns exactly abs(count) fields with replacement, so it can exceed the
// cardinality and may repeat fields.
func TestHRandFieldNegativeCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2", "c", "3")
	expect(t, rw, ":3")

	fields := map[string]bool{"a": true, "b": true, "c": true}
	cmd(t, rw, "HRANDFIELD", "h", "-8")
	got := readArray(t, rw)
	if len(got) != 8 {
		t.Fatalf("count -8 returned %d, want 8 (with replacement)", len(got))
	}
	for _, f := range got {
		if !fields[f] {
			t.Fatalf("returned %q, not a field", f)
		}
	}
}

// WITHVALUES interleaves each drawn field with its value, so the array is field, value pairs
// and each field's value matches what HSET stored.
func TestHRandFieldWithValues(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2", "c", "3", "d", "4", "e", "5")
	expect(t, rw, ":5")

	want := map[string]string{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"}

	cmd(t, rw, "HRANDFIELD", "h", "3", "WITHVALUES")
	got := readArray(t, rw)
	if len(got) != 6 {
		t.Fatalf("count 3 WITHVALUES returned %d entries, want 6", len(got))
	}
	seen := map[string]bool{}
	for i := 0; i < len(got); i += 2 {
		f, v := got[i], got[i+1]
		if want[f] != v {
			t.Fatalf("field %q paired with value %q, want %q", f, v, want[f])
		}
		if seen[f] {
			t.Fatalf("field %q repeated under a positive count", f)
		}
		seen[f] = true
	}

	// Negative count keeps the pairing under replacement.
	cmd(t, rw, "HRANDFIELD", "h", "-4", "WITHVALUES")
	got = readArray(t, rw)
	if len(got) != 8 {
		t.Fatalf("count -4 WITHVALUES returned %d entries, want 8", len(got))
	}
	for i := 0; i < len(got); i += 2 {
		if want[got[i]] != got[i+1] {
			t.Fatalf("field %q paired with value %q, want %q", got[i], got[i+1], want[got[i]])
		}
	}
}

// HRANDFIELD's error surface: WRONGTYPE in both the no-count and count forms, a bad option
// token is a syntax error, and a non-integer count is rejected.
func TestHRandFieldErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "s", "x")
	expect(t, rw, "+OK")
	cmd(t, rw, "HRANDFIELD", "s")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
	cmd(t, rw, "HRANDFIELD", "s", "2")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")

	cmd(t, rw, "HSET", "h", "a", "1")
	expect(t, rw, ":1")
	cmd(t, rw, "HRANDFIELD", "h", "2", "NOPE")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "HRANDFIELD", "h", "xx")
	expect(t, rw, "-ERR value is not an integer or out of range")

	// A token past WITHVALUES is a syntax error, matching Redis, which parses the count and
	// then rejects the extra token rather than reporting an arity error.
	cmd(t, rw, "HRANDFIELD", "h", "2", "WITHVALUES", "extra")
	expect(t, rw, "-ERR syntax error")
}
