package command

import "testing"

func TestZRank(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c")
	if got := sendLine(t, r, c, "ZRANK z a"); got != ":0" {
		t.Fatalf("ZRANK a = %q want :0", got)
	}
	if got := sendLine(t, r, c, "ZRANK z c"); got != ":2" {
		t.Fatalf("ZRANK c = %q want :2", got)
	}
	if got := sendLine(t, r, c, "ZREVRANK z a"); got != ":2" {
		t.Fatalf("ZREVRANK a = %q want :2", got)
	}
	// A missing member is nil.
	if got := sendLine(t, r, c, "ZRANK z missing"); got != "$-1" {
		t.Fatalf("ZRANK missing = %q want nil", got)
	}
}

func TestZRankWithScore(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b")
	if got := sendLine(t, r, c, "ZRANK z b WITHSCORE"); got != "*2" {
		t.Fatalf("ZRANK WITHSCORE header = %q want *2", got)
	}
	if got := sendLineRead(t, r); got != ":1" {
		t.Fatalf("ZRANK WITHSCORE rank = %q want :1", got)
	}
	if got := bulk(t, r, c, ""); got != "2" {
		t.Fatalf("ZRANK WITHSCORE score = %q want 2", got)
	}
	// A missing member with WITHSCORE is a null array.
	if got := sendLine(t, r, c, "ZRANK z missing WITHSCORE"); got != "*-1" {
		t.Fatalf("ZRANK missing WITHSCORE = %q want *-1", got)
	}
}

func TestZPopMin(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c")
	got := array(t, r, c, "ZPOPMIN z")
	if !equalSlice(got, []string{"a", "1"}) {
		t.Fatalf("ZPOPMIN = %v want [a 1]", got)
	}
	got = array(t, r, c, "ZPOPMIN z 2")
	if !equalSlice(got, []string{"b", "2", "c", "3"}) {
		t.Fatalf("ZPOPMIN 2 = %v want [b 2 c 3]", got)
	}
	// The set is drained, so the key is gone.
	if got := sendLine(t, r, c, "EXISTS z"); got != ":0" {
		t.Fatalf("EXISTS after drain = %q want :0", got)
	}
}

func TestZPopMax(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c")
	got := array(t, r, c, "ZPOPMAX z 2")
	if !equalSlice(got, []string{"c", "3", "b", "2"}) {
		t.Fatalf("ZPOPMAX 2 = %v want [c 3 b 2]", got)
	}
}

func TestZPopEmpty(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "ZPOPMIN z"); got != "*0" {
		t.Fatalf("ZPOPMIN missing = %q want *0", got)
	}
	if got := sendLine(t, r, c, "ZPOPMIN z notint"); got != "-ERR value is not an integer or out of range" {
		t.Fatalf("ZPOPMIN bad count = %q", got)
	}
	if got := sendLine(t, r, c, "ZPOPMIN z -1"); got != "-ERR value is out of range, must be positive" {
		t.Fatalf("ZPOPMIN negative count = %q", got)
	}
}

func TestZRandMember(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c")
	// A single random member is one of the three.
	got := bulk(t, r, c, "ZRANDMEMBER z")
	if got != "a" && got != "b" && got != "c" {
		t.Fatalf("ZRANDMEMBER = %q want one of a b c", got)
	}
	// A positive count returns distinct members.
	picks := array(t, r, c, "ZRANDMEMBER z 3")
	if len(picks) != 3 {
		t.Fatalf("ZRANDMEMBER 3 len = %d want 3", len(picks))
	}
	seen := map[string]bool{}
	for _, p := range picks {
		if seen[p] {
			t.Fatalf("ZRANDMEMBER 3 returned a duplicate: %v", picks)
		}
		seen[p] = true
	}
	// A count larger than the set returns the whole set.
	if got := array(t, r, c, "ZRANDMEMBER z 10"); len(got) != 3 {
		t.Fatalf("ZRANDMEMBER 10 len = %d want 3", len(got))
	}
	// A negative count returns exactly that many, repeats allowed.
	if got := array(t, r, c, "ZRANDMEMBER z -5"); len(got) != 5 {
		t.Fatalf("ZRANDMEMBER -5 len = %d want 5", len(got))
	}
	// Count zero is an empty array.
	if got := sendLine(t, r, c, "ZRANDMEMBER z 0"); got != "*0" {
		t.Fatalf("ZRANDMEMBER 0 = %q want *0", got)
	}
}

func TestZRandMemberWithScores(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b")
	got := array(t, r, c, "ZRANDMEMBER z 2 WITHSCORES")
	if len(got) != 4 {
		t.Fatalf("ZRANDMEMBER WITHSCORES len = %d want 4", len(got))
	}
	// A missing key with no count is nil.
	if got := sendLine(t, r, c, "ZRANDMEMBER missing"); got != "$-1" {
		t.Fatalf("ZRANDMEMBER missing = %q want nil", got)
	}
}

func TestZRankPopRandWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s hello")
	for _, cmd := range []string{
		"ZRANK s a", "ZREVRANK s a", "ZPOPMIN s", "ZPOPMAX s", "ZRANDMEMBER s",
	} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
}
