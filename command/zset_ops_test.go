package command

import (
	"bufio"
	"fmt"
	"net"
	"testing"
)

func TestZUnion(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD a 1 x 2 y")
	_ = sendLine(t, r, c, "ZADD b 3 y 4 z")
	got := array(t, r, c, "ZUNION 2 a b WITHSCORES")
	if !equalSlice(got, []string{"x", "1", "z", "4", "y", "5"}) {
		t.Fatalf("ZUNION WITHSCORES = %v", got)
	}
	// Plain member list, sorted by combined score.
	if got := array(t, r, c, "ZUNION 2 a b"); !equalSlice(got, []string{"x", "z", "y"}) {
		t.Fatalf("ZUNION = %v", got)
	}
}

func TestZUnionWeightsAggregate(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD a 1 x 2 y")
	_ = sendLine(t, r, c, "ZADD b 3 y 4 z")
	got := array(t, r, c, "ZUNION 2 a b WEIGHTS 2 3 WITHSCORES")
	// x: 1*2=2, y: 2*2 + 3*3 = 13, z: 4*3=12
	if !equalSlice(got, []string{"x", "2", "z", "12", "y", "13"}) {
		t.Fatalf("ZUNION WEIGHTS = %v", got)
	}
	got = array(t, r, c, "ZUNION 2 a b AGGREGATE MAX WITHSCORES")
	// x:1, z:4, y: max(2,3)=3
	if !equalSlice(got, []string{"x", "1", "y", "3", "z", "4"}) {
		t.Fatalf("ZUNION AGGREGATE MAX = %v", got)
	}
	got = array(t, r, c, "ZUNION 2 a b AGGREGATE MIN WITHSCORES")
	// x:1, y:min(2,3)=2, z:4
	if !equalSlice(got, []string{"x", "1", "y", "2", "z", "4"}) {
		t.Fatalf("ZUNION AGGREGATE MIN = %v", got)
	}
}

func TestZInter(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD a 1 x 2 y 3 w")
	_ = sendLine(t, r, c, "ZADD b 3 y 4 z 5 w")
	got := array(t, r, c, "ZINTER 2 a b WITHSCORES")
	// y: 2+3=5, w: 3+5=8
	if !equalSlice(got, []string{"y", "5", "w", "8"}) {
		t.Fatalf("ZINTER WITHSCORES = %v", got)
	}
	// An empty operand yields an empty intersection.
	if got := sendLine(t, r, c, "ZINTER 2 a missing"); got != "*0" {
		t.Fatalf("ZINTER with missing = %q want *0", got)
	}
}

func TestZDiff(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD a 1 x 2 y 3 z")
	_ = sendLine(t, r, c, "ZADD b 9 y")
	got := array(t, r, c, "ZDIFF 2 a b WITHSCORES")
	if !equalSlice(got, []string{"x", "1", "z", "3"}) {
		t.Fatalf("ZDIFF WITHSCORES = %v", got)
	}
	// ZDIFF does not accept WEIGHTS or AGGREGATE.
	if got := sendLine(t, r, c, "ZDIFF 2 a b WEIGHTS 1 1"); got != "-ERR syntax error" {
		t.Fatalf("ZDIFF WEIGHTS = %q", got)
	}
}

func TestZUnionStore(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD a 1 x 2 y")
	_ = sendLine(t, r, c, "ZADD b 3 y 4 z")
	if got := sendLine(t, r, c, "ZUNIONSTORE dst 2 a b"); got != ":3" {
		t.Fatalf("ZUNIONSTORE = %q want :3", got)
	}
	got := array(t, r, c, "ZRANGE dst 0 -1 WITHSCORES")
	if !equalSlice(got, []string{"x", "1", "z", "4", "y", "5"}) {
		t.Fatalf("dst after ZUNIONSTORE = %v", got)
	}
}

func TestZInterStore(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD a 1 x 2 y")
	_ = sendLine(t, r, c, "ZADD b 3 y 4 z")
	if got := sendLine(t, r, c, "ZINTERSTORE dst 2 a b"); got != ":1" {
		t.Fatalf("ZINTERSTORE = %q want :1", got)
	}
	got := array(t, r, c, "ZRANGE dst 0 -1 WITHSCORES")
	if !equalSlice(got, []string{"y", "5"}) {
		t.Fatalf("dst after ZINTERSTORE = %v", got)
	}
	// An empty result deletes the destination.
	_ = sendLine(t, r, c, "ZADD c 1 only")
	if got := sendLine(t, r, c, "ZINTERSTORE dst 2 a c"); got != ":0" {
		t.Fatalf("ZINTERSTORE empty = %q want :0", got)
	}
	if got := sendLine(t, r, c, "EXISTS dst"); got != ":0" {
		t.Fatalf("EXISTS dst = %q want :0", got)
	}
}

func TestZDiffStore(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD a 1 x 2 y 3 z")
	_ = sendLine(t, r, c, "ZADD b 9 y")
	if got := sendLine(t, r, c, "ZDIFFSTORE dst 2 a b"); got != ":2" {
		t.Fatalf("ZDIFFSTORE = %q want :2", got)
	}
	got := array(t, r, c, "ZRANGE dst 0 -1 WITHSCORES")
	if !equalSlice(got, []string{"x", "1", "z", "3"}) {
		t.Fatalf("dst after ZDIFFSTORE = %v", got)
	}
}

func TestZInterCard(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD a 1 x 2 y 3 w")
	_ = sendLine(t, r, c, "ZADD b 3 y 4 z 5 w")
	if got := sendLine(t, r, c, "ZINTERCARD 2 a b"); got != ":2" {
		t.Fatalf("ZINTERCARD = %q want :2", got)
	}
	if got := sendLine(t, r, c, "ZINTERCARD 2 a b LIMIT 1"); got != ":1" {
		t.Fatalf("ZINTERCARD LIMIT 1 = %q want :1", got)
	}
	// LIMIT 0 means no limit.
	if got := sendLine(t, r, c, "ZINTERCARD 2 a b LIMIT 0"); got != ":2" {
		t.Fatalf("ZINTERCARD LIMIT 0 = %q want :2", got)
	}
	if got := sendLine(t, r, c, "ZINTERCARD 2 a b LIMIT -1"); got != "-ERR LIMIT can't be negative" {
		t.Fatalf("ZINTERCARD LIMIT -1 = %q", got)
	}
	if got := sendLine(t, r, c, "ZINTERCARD 0 a"); got != "-ERR numkeys should be greater than 0" {
		t.Fatalf("ZINTERCARD 0 = %q", got)
	}
}

// readNestedPairs reads the elements section of a ZMPOP/BZMPOP reply:
// *N on RESP2 followed by N × (*2, member, score).
func readNestedPairs(t *testing.T, r *bufio.Reader, c net.Conn) []string {
	t.Helper()
	hdr := sendLineRead(t, r)
	var n int
	if _, err := fmt.Sscanf(hdr, "*%d", &n); err != nil {
		t.Fatalf("expected nested pairs header, got %q", hdr)
	}
	var out []string
	for range n {
		ph := sendLineRead(t, r)
		if ph != "*2" {
			t.Fatalf("expected *2 pair header, got %q", ph)
		}
		out = append(out, bulk(t, r, c, ""), bulk(t, r, c, ""))
	}
	return out
}

func TestZMPop(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c")
	// MIN with no count pops the lowest pair nested as [[member, score], ...].
	if got := sendLine(t, r, c, "ZMPOP 1 z MIN"); got != "*2" {
		t.Fatalf("ZMPOP header = %q want *2", got)
	}
	if got := bulk(t, r, c, ""); got != "z" {
		t.Fatalf("ZMPOP key = %q want z", got)
	}
	got := readNestedPairs(t, r, c)
	if !equalSlice(got, []string{"a", "1"}) {
		t.Fatalf("ZMPOP pair = %v want [a 1]", got)
	}
	// MAX with COUNT pops the top two highest-first.
	if got := sendLine(t, r, c, "ZMPOP 1 z MAX COUNT 2"); got != "*2" {
		t.Fatalf("ZMPOP MAX header = %q", got)
	}
	_ = bulk(t, r, c, "")
	got = readNestedPairs(t, r, c)
	if !equalSlice(got, []string{"c", "3", "b", "2"}) {
		t.Fatalf("ZMPOP MAX COUNT = %v", got)
	}
	// Draining the last element removes the key.
	if got := sendLine(t, r, c, "EXISTS z"); got != ":0" {
		t.Fatalf("EXISTS after drain = %q want :0", got)
	}
}

func TestZMPopFirstNonEmpty(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD second 5 m")
	// The first key is empty, so the pop falls through to the second.
	if got := sendLine(t, r, c, "ZMPOP 2 first second MIN"); got != "*2" {
		t.Fatalf("ZMPOP header = %q", got)
	}
	if got := bulk(t, r, c, ""); got != "second" {
		t.Fatalf("ZMPOP key = %q want second", got)
	}
	_ = readNestedPairs(t, r, c)
	// All keys empty replies a null array.
	if got := sendLine(t, r, c, "ZMPOP 2 first second MIN"); got != "*-1" {
		t.Fatalf("ZMPOP all empty = %q want *-1", got)
	}
}

func TestZSetOpErrors(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD a 1 x")
	if got := sendLine(t, r, c, "ZUNION 0 a"); got != "-ERR at least 1 input key is needed for 'zunion' command" {
		t.Fatalf("ZUNION 0 = %q", got)
	}
	if got := sendLine(t, r, c, "ZUNION 3 a"); got != "-ERR syntax error" {
		t.Fatalf("ZUNION 3 a = %q", got)
	}
	if got := sendLine(t, r, c, "ZUNIONSTORE dst 2 a b WEIGHTS 1 q"); got != "-ERR weight value is not a float" {
		t.Fatalf("bad weight = %q", got)
	}
	if got := sendLine(t, r, c, "ZMPOP 1 a BOGUS"); got != "-ERR syntax error" {
		t.Fatalf("ZMPOP bad dir = %q", got)
	}
}

func TestZSetOpWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s hello")
	_ = sendLine(t, r, c, "ZADD z 1 a")
	for _, cmd := range []string{
		"ZUNION 2 z s", "ZINTER 2 z s", "ZDIFF 2 z s",
		"ZUNIONSTORE d 2 z s", "ZINTERCARD 2 z s", "ZMPOP 1 s MIN",
	} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
	// A non-zset destination is also WRONGTYPE.
	if got := sendLine(t, r, c, "ZUNIONSTORE s 1 z"); got != "-"+wrongTypeError {
		t.Fatalf("ZUNIONSTORE into string = %q want WRONGTYPE", got)
	}
}
