package command

import "testing"

func TestZRangeByRank(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c 4 d 5 e")
	if got := array(t, r, c, "ZRANGE z 0 -1"); !equalSlice(got, []string{"a", "b", "c", "d", "e"}) {
		t.Fatalf("ZRANGE 0 -1 = %v", got)
	}
	if got := array(t, r, c, "ZRANGE z 1 3"); !equalSlice(got, []string{"b", "c", "d"}) {
		t.Fatalf("ZRANGE 1 3 = %v", got)
	}
	if got := array(t, r, c, "ZRANGE z 0 -1 REV"); !equalSlice(got, []string{"e", "d", "c", "b", "a"}) {
		t.Fatalf("ZRANGE REV = %v", got)
	}
	// Out-of-range start gives an empty array.
	if got := sendLine(t, r, c, "ZRANGE z 10 20"); got != "*0" {
		t.Fatalf("ZRANGE 10 20 = %q want *0", got)
	}
}

func TestZRangeWithScores(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b")
	got := array(t, r, c, "ZRANGE z 0 -1 WITHSCORES")
	if !equalSlice(got, []string{"a", "1", "b", "2"}) {
		t.Fatalf("ZRANGE WITHSCORES = %v", got)
	}
}

func TestZRevRange(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c")
	if got := array(t, r, c, "ZREVRANGE z 0 1"); !equalSlice(got, []string{"c", "b"}) {
		t.Fatalf("ZREVRANGE 0 1 = %v", got)
	}
	got := array(t, r, c, "ZREVRANGE z 0 -1 WITHSCORES")
	if !equalSlice(got, []string{"c", "3", "b", "2", "a", "1"}) {
		t.Fatalf("ZREVRANGE WITHSCORES = %v", got)
	}
}

func TestZRangeByScore(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c 4 d 5 e")
	if got := array(t, r, c, "ZRANGE z 2 4 BYSCORE"); !equalSlice(got, []string{"b", "c", "d"}) {
		t.Fatalf("ZRANGE BYSCORE = %v", got)
	}
	if got := array(t, r, c, "ZRANGE z 5 2 BYSCORE REV"); !equalSlice(got, []string{"e", "d", "c", "b"}) {
		t.Fatalf("ZRANGE BYSCORE REV = %v", got)
	}
	// Exclusive lower bound and +inf upper.
	if got := array(t, r, c, "ZRANGEBYSCORE z (1 +inf"); !equalSlice(got, []string{"b", "c", "d", "e"}) {
		t.Fatalf("ZRANGEBYSCORE (1 +inf = %v", got)
	}
	// LIMIT.
	if got := array(t, r, c, "ZRANGEBYSCORE z 1 5 LIMIT 1 2"); !equalSlice(got, []string{"b", "c"}) {
		t.Fatalf("ZRANGEBYSCORE LIMIT = %v", got)
	}
	if got := array(t, r, c, "ZREVRANGEBYSCORE z 5 1 LIMIT 0 2"); !equalSlice(got, []string{"e", "d"}) {
		t.Fatalf("ZREVRANGEBYSCORE LIMIT = %v", got)
	}
}

func TestZRangeByLex(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 0 a 0 b 0 c 0 d")
	if got := array(t, r, c, "ZRANGEBYLEX z [b [d"); !equalSlice(got, []string{"b", "c", "d"}) {
		t.Fatalf("ZRANGEBYLEX [b [d = %v", got)
	}
	if got := array(t, r, c, "ZRANGEBYLEX z - +"); !equalSlice(got, []string{"a", "b", "c", "d"}) {
		t.Fatalf("ZRANGEBYLEX - + = %v", got)
	}
	if got := array(t, r, c, "ZRANGEBYLEX z (a (d"); !equalSlice(got, []string{"b", "c"}) {
		t.Fatalf("ZRANGEBYLEX (a (d = %v", got)
	}
	if got := array(t, r, c, "ZREVRANGEBYLEX z + -"); !equalSlice(got, []string{"d", "c", "b", "a"}) {
		t.Fatalf("ZREVRANGEBYLEX + - = %v", got)
	}
	if got := array(t, r, c, "ZRANGE z [b [c BYLEX"); !equalSlice(got, []string{"b", "c"}) {
		t.Fatalf("ZRANGE BYLEX = %v", got)
	}
}

func TestZCountAndLexCount(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c 4 d")
	if got := sendLine(t, r, c, "ZCOUNT z 2 3"); got != ":2" {
		t.Fatalf("ZCOUNT 2 3 = %q want :2", got)
	}
	if got := sendLine(t, r, c, "ZCOUNT z (1 +inf"); got != ":3" {
		t.Fatalf("ZCOUNT (1 +inf = %q want :3", got)
	}
	_ = sendLine(t, r, c, "DEL z")
	_ = sendLine(t, r, c, "ZADD z 0 a 0 b 0 c 0 d")
	if got := sendLine(t, r, c, "ZLEXCOUNT z [b [d"); got != ":3" {
		t.Fatalf("ZLEXCOUNT = %q want :3", got)
	}
	if got := sendLine(t, r, c, "ZLEXCOUNT z - +"); got != ":4" {
		t.Fatalf("ZLEXCOUNT - + = %q want :4", got)
	}
}

func TestZRemRangeBy(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c 4 d 5 e")
	if got := sendLine(t, r, c, "ZREMRANGEBYRANK z 0 1"); got != ":2" {
		t.Fatalf("ZREMRANGEBYRANK = %q want :2", got)
	}
	if got := array(t, r, c, "ZRANGE z 0 -1"); !equalSlice(got, []string{"c", "d", "e"}) {
		t.Fatalf("after ZREMRANGEBYRANK = %v", got)
	}
	if got := sendLine(t, r, c, "ZREMRANGEBYSCORE z 4 5"); got != ":2" {
		t.Fatalf("ZREMRANGEBYSCORE = %q want :2", got)
	}
	if got := array(t, r, c, "ZRANGE z 0 -1"); !equalSlice(got, []string{"c"}) {
		t.Fatalf("after ZREMRANGEBYSCORE = %v", got)
	}
	// Drain via lex removes the key.
	_ = sendLine(t, r, c, "DEL z")
	_ = sendLine(t, r, c, "ZADD z 0 a 0 b 0 c")
	if got := sendLine(t, r, c, "ZREMRANGEBYLEX z - +"); got != ":3" {
		t.Fatalf("ZREMRANGEBYLEX = %q want :3", got)
	}
	if got := sendLine(t, r, c, "EXISTS z"); got != ":0" {
		t.Fatalf("EXISTS after lex drain = %q want :0", got)
	}
}

func TestZRangeStore(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD src 1 a 2 b 3 c 4 d")
	if got := sendLine(t, r, c, "ZRANGESTORE dst src 2 3 BYSCORE"); got != ":2" {
		t.Fatalf("ZRANGESTORE = %q want :2", got)
	}
	got := array(t, r, c, "ZRANGE dst 0 -1 WITHSCORES")
	if !equalSlice(got, []string{"b", "2", "c", "3"}) {
		t.Fatalf("dst after ZRANGESTORE = %v", got)
	}
	// An empty range deletes the destination.
	if got := sendLine(t, r, c, "ZRANGESTORE dst src 100 200 BYSCORE"); got != ":0" {
		t.Fatalf("ZRANGESTORE empty = %q want :0", got)
	}
	if got := sendLine(t, r, c, "EXISTS dst"); got != ":0" {
		t.Fatalf("EXISTS dst after empty store = %q want :0", got)
	}
}

func TestZRangeErrors(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a")
	if got := sendLine(t, r, c, "ZRANGE z 0 -1 LIMIT 0 1"); got != "-ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX" {
		t.Fatalf("ZRANGE rank LIMIT = %q", got)
	}
	if got := sendLine(t, r, c, "ZRANGE z 0 1 BYSCORE BYLEX"); got != "-ERR syntax error" {
		t.Fatalf("ZRANGE BYSCORE BYLEX = %q", got)
	}
	if got := sendLine(t, r, c, "ZRANGE z foo bar BYSCORE"); got != "-ERR min or max is not a float" {
		t.Fatalf("ZRANGE bad score = %q", got)
	}
	if got := sendLine(t, r, c, "ZRANGE z foo bar BYLEX"); got != "-ERR min or max not valid string range item" {
		t.Fatalf("ZRANGE bad lex = %q", got)
	}
	if got := sendLine(t, r, c, "ZRANGE z foo bar"); got != "-ERR value is not an integer or out of range" {
		t.Fatalf("ZRANGE bad rank = %q", got)
	}
	if got := sendLine(t, r, c, "ZRANGE z 0 -1 WITHSCORES BYLEX"); got != "-ERR syntax error" {
		t.Fatalf("ZRANGE BYLEX WITHSCORES = %q", got)
	}
}

func TestZRangeWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s hello")
	for _, cmd := range []string{
		"ZRANGE s 0 -1", "ZREVRANGE s 0 -1", "ZRANGEBYSCORE s 0 1", "ZRANGEBYLEX s - +",
		"ZCOUNT s 0 1", "ZLEXCOUNT s - +", "ZREMRANGEBYRANK s 0 1", "ZREMRANGEBYSCORE s 0 1",
		"ZREMRANGEBYLEX s - +", "ZRANGESTORE d s 0 -1",
	} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
}
