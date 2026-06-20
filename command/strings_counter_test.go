package command

import "testing"

func TestIncrDecr(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "INCR n"); got != ":1" {
		t.Fatalf("INCR new = %q want :1", got)
	}
	if got := sendLine(t, r, c, "INCR n"); got != ":2" {
		t.Fatalf("INCR = %q want :2", got)
	}
	if got := sendLine(t, r, c, "DECR n"); got != ":1" {
		t.Fatalf("DECR = %q want :1", got)
	}
	if got := sendLine(t, r, c, "DECR fresh"); got != ":-1" {
		t.Fatalf("DECR fresh = %q want :-1", got)
	}
}

func TestIncrByDecrBy(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "INCRBY n 10"); got != ":10" {
		t.Fatalf("INCRBY = %q want :10", got)
	}
	if got := sendLine(t, r, c, "DECRBY n 4"); got != ":6" {
		t.Fatalf("DECRBY = %q want :6", got)
	}
	// A negative increment is allowed and runs the other way.
	if got := sendLine(t, r, c, "INCRBY n -16"); got != ":-10" {
		t.Fatalf("INCRBY negative = %q want :-10", got)
	}
}

func TestIncrNotInteger(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k hello")
	if got := sendLine(t, r, c, "INCR k"); got != "-ERR value is not an integer or out of range" {
		t.Fatalf("INCR on non-int = %q", got)
	}
	// A bad increment argument is rejected too.
	if got := sendLine(t, r, c, "INCRBY n notanumber"); got != "-ERR value is not an integer or out of range" {
		t.Fatalf("INCRBY bad arg = %q", got)
	}
}

func TestIncrOverflow(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET n 9223372036854775807")
	if got := sendLine(t, r, c, "INCR n"); got != "-ERR increment or decrement would overflow" {
		t.Fatalf("INCR overflow = %q", got)
	}
	// DECRBY with the smallest int64 cannot be negated and is an overflow.
	if got := sendLine(t, r, c, "DECRBY x -9223372036854775808"); got != "-ERR increment or decrement would overflow" {
		t.Fatalf("DECRBY min = %q", got)
	}
}

func TestIncrByFloat(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k 10.50")
	if got := bulk(t, r, c, "INCRBYFLOAT k 0.1"); got != "10.6" {
		t.Fatalf("INCRBYFLOAT = %q want 10.6", got)
	}
	if got := bulk(t, r, c, "INCRBYFLOAT k -5"); got != "5.6" {
		t.Fatalf("INCRBYFLOAT negative = %q want 5.6", got)
	}
	// A whole-number result drops the fractional part.
	_ = sendLine(t, r, c, "SET w 5.0e3")
	if got := bulk(t, r, c, "INCRBYFLOAT w 2.0e2"); got != "5200" {
		t.Fatalf("INCRBYFLOAT whole = %q want 5200", got)
	}
	// A missing key starts at 0.
	if got := bulk(t, r, c, "INCRBYFLOAT fresh 3.5"); got != "3.5" {
		t.Fatalf("INCRBYFLOAT fresh = %q want 3.5", got)
	}
}

func TestIncrByFloatErrors(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k notafloat")
	if got := sendLine(t, r, c, "INCRBYFLOAT k 1.0"); got != "-ERR value is not a float or out of range" {
		t.Fatalf("INCRBYFLOAT on non-float = %q", got)
	}
	if got := sendLine(t, r, c, "INCRBYFLOAT n notanumber"); got != "-ERR value is not a float or out of range" {
		t.Fatalf("INCRBYFLOAT bad arg = %q", got)
	}
}
