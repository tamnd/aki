package hash

import "testing"

// The hash arithmetic verbs (spec 2064/f3/10 section 7.3), driven through the
// same runtime harness as the rest of the point suite. HINCRBY and HINCRBYFLOAT
// each create the field at zero, apply the delta, write the canonical rendering
// back, and reply the new value; the error paths leave the field and the key
// untouched. WRONGTYPE for both verbs is checked centrally in TestWrongType.

func TestHincrby(t *testing.T) {
	c := newHarness(t).NewConn()
	// A missing key creates the hash and the field at zero, then adds.
	wantInt(t, do(t, c, opHincrby, "h", "n", "5"), 5)
	wantInt(t, do(t, c, opHincrby, "h", "n", "3"), 8)
	wantInt(t, do(t, c, opHincrby, "h", "n", "-10"), -2)
	// The stored value is the decimal rendering, readable as a plain field.
	wantBulk(t, do(t, c, opHget, "h", "n"), "-2")
	// A second field on the same hash starts from zero independently.
	wantInt(t, do(t, c, opHincrby, "h", "m", "100"), 100)
	wantInt(t, do(t, c, opHlen, "h"), 2)
}

func TestHincrbyErrors(t *testing.T) {
	c := newHarness(t).NewConn()
	// A non-integer delta errors before anything is touched.
	wantErr(t, do(t, c, opHincrby, "h", "n", "1.5"),
		"ERR value is not an integer or out of range")
	wantErr(t, do(t, c, opHincrby, "h", "n", "abc"),
		"ERR value is not an integer or out of range")
	// The failed delta never created the key.
	wantInt(t, do(t, c, opHlen, "h"), 0)
	wantNil(t, do(t, c, opHget, "h", "n"))

	// A field holding a non-integer value errors on HINCRBY, unchanged.
	do(t, c, opHset, "h", "s", "notanumber")
	wantErr(t, do(t, c, opHincrby, "h", "s", "1"),
		"ERR hash value is not an integer")
	wantBulk(t, do(t, c, opHget, "h", "s"), "notanumber")

	// Overflow past int64 errors and leaves the field at its old value.
	do(t, c, opHset, "h", "big", "9223372036854775807")
	wantErr(t, do(t, c, opHincrby, "h", "big", "1"),
		"ERR increment or decrement would overflow")
	wantBulk(t, do(t, c, opHget, "h", "big"), "9223372036854775807")
	// Underflow past the negative bound errors the same way.
	do(t, c, opHset, "h", "small", "-9223372036854775808")
	wantErr(t, do(t, c, opHincrby, "h", "small", "-1"),
		"ERR increment or decrement would overflow")
	wantBulk(t, do(t, c, opHget, "h", "small"), "-9223372036854775808")
}

func TestHincrbyfloat(t *testing.T) {
	c := newHarness(t).NewConn()
	// A missing key creates the hash and the field at zero (values chosen to be
	// exact in binary so the shortest round-trip rendering is unambiguous).
	wantBulk(t, do(t, c, opHincrbyfloat, "h", "n", "3.5"), "3.5")
	wantBulk(t, do(t, c, opHincrbyfloat, "h", "n", "1"), "4.5")
	wantBulk(t, do(t, c, opHincrbyfloat, "h", "n", "-0.5"), "4")
	wantBulk(t, do(t, c, opHget, "h", "n"), "4")
	// HINCRBYFLOAT reads an integer-valued field the same as a float one.
	do(t, c, opHset, "h", "i", "10")
	wantBulk(t, do(t, c, opHincrbyfloat, "h", "i", "0.5"), "10.5")
	// Exponent form parses and renders in shortest fixed form.
	wantBulk(t, do(t, c, opHincrbyfloat, "h", "e", "5.0e3"), "5000")
}

func TestHincrbyfloatErrors(t *testing.T) {
	c := newHarness(t).NewConn()
	// A non-float delta errors before anything is touched.
	wantErr(t, do(t, c, opHincrbyfloat, "h", "n", "notafloat"),
		"ERR value is not a valid float")
	wantInt(t, do(t, c, opHlen, "h"), 0)

	// A field holding a non-float value errors, unchanged.
	do(t, c, opHset, "h", "s", "notanumber")
	wantErr(t, do(t, c, opHincrbyfloat, "h", "s", "1.0"),
		"ERR hash value is not a float")
	wantBulk(t, do(t, c, opHget, "h", "s"), "notanumber")

	// An infinite increment argument is rejected up front with Redis 8.8's own
	// message, distinct from the result-overflow message below, and leaves the
	// field unchanged.
	do(t, c, opHset, "h", "f", "1.5")
	for _, inf := range []string{"inf", "+inf", "-inf"} {
		wantErr(t, do(t, c, opHincrbyfloat, "h", "f", inf),
			"ERR value is NaN or Infinity")
	}
	wantBulk(t, do(t, c, opHget, "h", "f"), "1.5")

	// A finite increment whose sum overflows to Infinity gets the other message.
	do(t, c, opHset, "h", "huge", "1e308")
	wantErr(t, do(t, c, opHincrbyfloat, "h", "huge", "1e308"),
		"ERR increment would produce NaN or Infinity")
	wantBulk(t, do(t, c, opHget, "h", "huge"), "1e308")

	// The infinite-increment rejection on a missing key strands no empty hash,
	// the same validate-before-create discipline the string band keeps.
	wantErr(t, do(t, c, opHincrbyfloat, "nokey", "f", "inf"),
		"ERR value is NaN or Infinity")
	wantInt(t, do(t, c, opHlen, "nokey"), 0)
	wantNil(t, do(t, c, opHget, "nokey", "f"))
}
