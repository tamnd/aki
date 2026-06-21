package command

import "testing"

func TestZAddAndScore(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "ZADD z 1 alice 2 bob"); got != ":2" {
		t.Fatalf("ZADD = %q want :2", got)
	}
	if got := bulk(t, r, c, "ZSCORE z alice"); got != "1" {
		t.Fatalf("ZSCORE alice = %q want 1", got)
	}
	if got := bulk(t, r, c, "ZSCORE z bob"); got != "2" {
		t.Fatalf("ZSCORE bob = %q want 2", got)
	}
	// A missing member scores nil.
	if got := sendLine(t, r, c, "ZSCORE z carol"); got != "$-1" {
		t.Fatalf("ZSCORE carol = %q want nil", got)
	}
	if got := sendLine(t, r, c, "ZCARD z"); got != ":2" {
		t.Fatalf("ZCARD = %q want :2", got)
	}
}

func TestZAddUpdateAndCH(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b")
	// Re-adding with a new score counts 0 new members without CH.
	if got := sendLine(t, r, c, "ZADD z 5 a"); got != ":0" {
		t.Fatalf("ZADD update no CH = %q want :0", got)
	}
	if got := bulk(t, r, c, "ZSCORE z a"); got != "5" {
		t.Fatalf("ZSCORE a = %q want 5", got)
	}
	// CH counts the changed member.
	if got := sendLine(t, r, c, "ZADD z CH 9 a"); got != ":1" {
		t.Fatalf("ZADD CH update = %q want :1", got)
	}
	// CH does not count a re-add with the same score.
	if got := sendLine(t, r, c, "ZADD z CH 9 a"); got != ":0" {
		t.Fatalf("ZADD CH same score = %q want :0", got)
	}
}

func TestZAddNXXX(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a")
	// NX does not update an existing member but adds a new one.
	if got := sendLine(t, r, c, "ZADD z NX 5 a 5 b"); got != ":1" {
		t.Fatalf("ZADD NX = %q want :1", got)
	}
	if got := bulk(t, r, c, "ZSCORE z a"); got != "1" {
		t.Fatalf("ZSCORE a after NX = %q want 1", got)
	}
	// XX updates an existing member and refuses to add a new one.
	if got := sendLine(t, r, c, "ZADD z XX CH 7 a 7 c"); got != ":1" {
		t.Fatalf("ZADD XX = %q want :1", got)
	}
	if got := sendLine(t, r, c, "ZSCORE z c"); got != "$-1" {
		t.Fatalf("ZSCORE c after XX = %q want nil", got)
	}
}

func TestZAddGTLT(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 5 a")
	// GT keeps the higher score.
	if got := sendLine(t, r, c, "ZADD z GT CH 3 a"); got != ":0" {
		t.Fatalf("ZADD GT lower = %q want :0", got)
	}
	if got := sendLine(t, r, c, "ZADD z GT CH 8 a"); got != ":1" {
		t.Fatalf("ZADD GT higher = %q want :1", got)
	}
	if got := bulk(t, r, c, "ZSCORE z a"); got != "8" {
		t.Fatalf("ZSCORE a after GT = %q want 8", got)
	}
	// LT keeps the lower score.
	if got := sendLine(t, r, c, "ZADD z LT CH 9 a"); got != ":0" {
		t.Fatalf("ZADD LT higher = %q want :0", got)
	}
	if got := sendLine(t, r, c, "ZADD z LT CH 2 a"); got != ":1" {
		t.Fatalf("ZADD LT lower = %q want :1", got)
	}
	// GT still adds a brand new member.
	if got := sendLine(t, r, c, "ZADD z GT 1 fresh"); got != ":1" {
		t.Fatalf("ZADD GT new member = %q want :1", got)
	}
}

func TestZAddIncr(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a")
	if got := bulk(t, r, c, "ZADD z INCR 10 a"); got != "11" {
		t.Fatalf("ZADD INCR = %q want 11", got)
	}
	// NX INCR on an existing member returns nil.
	if got := sendLine(t, r, c, "ZADD z NX INCR 99 a"); got != "$-1" {
		t.Fatalf("ZADD NX INCR existing = %q want nil", got)
	}
	// GT INCR that would lower the score is blocked and returns nil.
	if got := sendLine(t, r, c, "ZADD z GT INCR -5 a"); got != "$-1" {
		t.Fatalf("ZADD GT INCR lower = %q want nil", got)
	}
	if got := bulk(t, r, c, "ZSCORE z a"); got != "11" {
		t.Fatalf("ZSCORE a after blocked INCR = %q want 11", got)
	}
}

func TestZAddErrors(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "ZADD z NX XX 1 a"); got != "-ERR XX and NX options at the same time are not compatible" {
		t.Fatalf("ZADD NX XX = %q", got)
	}
	if got := sendLine(t, r, c, "ZADD z GT LT 1 a"); got != "-ERR GT, LT, and NX options at the same time are not compatible" {
		t.Fatalf("ZADD GT LT = %q", got)
	}
	if got := sendLine(t, r, c, "ZADD z INCR 1 a 2 b"); got != "-ERR INCR option supports a single increment-element pair" {
		t.Fatalf("ZADD INCR two pairs = %q", got)
	}
	if got := sendLine(t, r, c, "ZADD z 1 a 2"); got != "-ERR syntax error" {
		t.Fatalf("ZADD odd args = %q", got)
	}
	if got := sendLine(t, r, c, "ZADD z notafloat a"); got != "-ERR value is not a valid float" {
		t.Fatalf("ZADD bad score = %q", got)
	}
}

func TestZIncrBy(t *testing.T) {
	r, c := startData(t)
	if got := bulk(t, r, c, "ZINCRBY z 2.5 a"); got != "2.5" {
		t.Fatalf("ZINCRBY new = %q want 2.5", got)
	}
	if got := bulk(t, r, c, "ZINCRBY z 1.5 a"); got != "4" {
		t.Fatalf("ZINCRBY existing = %q want 4", got)
	}
	if got := sendLine(t, r, c, "ZINCRBY z bad a"); got != "-ERR value is not a valid float" {
		t.Fatalf("ZINCRBY bad = %q", got)
	}
}

func TestZIncrByNaN(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z inf a")
	if got := sendLine(t, r, c, "ZINCRBY z -inf a"); got != "-ERR resulting score is not a number (NaN)" {
		t.Fatalf("ZINCRBY to NaN = %q", got)
	}
}

func TestZScoreInf(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z +inf a -inf b")
	if got := bulk(t, r, c, "ZSCORE z a"); got != "inf" {
		t.Fatalf("ZSCORE +inf = %q want inf", got)
	}
	if got := bulk(t, r, c, "ZSCORE z b"); got != "-inf" {
		t.Fatalf("ZSCORE -inf = %q want -inf", got)
	}
}

func TestZMScore(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b")
	got := array(t, r, c, "ZMSCORE z a missing b")
	want := []string{"1", "<nil>", "2"}
	if !equalSlice(got, want) {
		t.Fatalf("ZMSCORE = %v want %v", got, want)
	}
}

func TestZRem(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c")
	if got := sendLine(t, r, c, "ZREM z a c missing"); got != ":2" {
		t.Fatalf("ZREM = %q want :2", got)
	}
	if got := sendLine(t, r, c, "ZCARD z"); got != ":1" {
		t.Fatalf("ZCARD after ZREM = %q want :1", got)
	}
	// Removing the last member deletes the key.
	if got := sendLine(t, r, c, "ZREM z b"); got != ":1" {
		t.Fatalf("ZREM last = %q want :1", got)
	}
	if got := sendLine(t, r, c, "EXISTS z"); got != ":0" {
		t.Fatalf("EXISTS after draining = %q want :0", got)
	}
}

func TestZSetWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s hello")
	for _, cmd := range []string{
		"ZADD s 1 a", "ZINCRBY s 1 a", "ZSCORE s a",
		"ZMSCORE s a", "ZCARD s", "ZREM s a",
	} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
}
