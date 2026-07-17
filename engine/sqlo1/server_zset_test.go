package sqlo1

import (
	"fmt"
	"testing"
)

func TestServerZSetSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	// Point writes: variadic ZADD counts created members, CH widens
	// the count to every member the command touched.
	send("ZADD", "z", "1", "a", "2", "b")
	expect(t, r, ":2\r\n")
	send("ZADD", "z", "1", "a", "3", "c")
	expect(t, r, ":1\r\n")
	send("ZADD", "z", "5", "a")
	expect(t, r, ":0\r\n")
	send("ZADD", "z", "CH", "6", "a", "9", "d")
	expect(t, r, ":2\r\n")
	send("ZADD", "z", "CH", "6", "a")
	expect(t, r, ":0\r\n")
	send("zadd", "z", "1")
	expect(t, r, "-ERR wrong number of arguments for 'zadd' command\r\n")
	send("ZADD", "z", "1", "a", "2")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZADD", "z", "notafloat", "a")
	expect(t, r, "-ERR value is not a valid float\r\n")
	send("ZADD", "z", "nan", "a")
	expect(t, r, "-ERR value is not a valid float\r\n")

	// Condition flags and their conflicts.
	send("ZADD", "z", "NX", "99", "a")
	expect(t, r, ":0\r\n")
	send("ZINCRBY", "z", "0", "a")
	expect(t, r, "$1\r\n6\r\n")
	send("ZADD", "z", "XX", "7", "nope")
	expect(t, r, ":0\r\n")
	send("ZADD", "z", "GT", "2", "a")
	expect(t, r, ":0\r\n")
	send("ZADD", "z", "GT", "CH", "8", "a")
	expect(t, r, ":1\r\n")
	send("ZADD", "z", "LT", "CH", "3", "a")
	expect(t, r, ":1\r\n")
	send("ZADD", "z", "NX", "XX", "1", "a")
	expect(t, r, "-ERR XX and NX options at the same time are not compatible\r\n")
	send("ZADD", "z", "GT", "NX", "1", "a")
	expect(t, r, "-ERR GT, LT, and/or NX options at the same time are not compatible\r\n")
	send("ZADD", "z", "GT", "LT", "1", "a")
	expect(t, r, "-ERR GT, LT, and/or NX options at the same time are not compatible\r\n")

	// INCR: one pair, a bulk score back, nil when a flag vetoes.
	send("ZADD", "z", "INCR", "2.5", "a")
	expect(t, r, "$3\r\n5.5\r\n")
	send("ZADD", "z", "NX", "INCR", "1", "a")
	expect(t, r, "$-1\r\n")
	send("ZADD", "z", "INCR", "1", "a", "2", "b")
	expect(t, r, "-ERR INCR option supports a single increment-element pair\r\n")
	send("ZADD", "z", "INCR", "inf", "a")
	expect(t, r, "$3\r\ninf\r\n")
	send("ZADD", "z", "INCR", "-inf", "a")
	expect(t, r, "-ERR resulting score is not a number (NaN)\r\n")

	// ZINCRBY: create at the increment, integral scores print bare.
	send("ZINCRBY", "zi", "3", "m")
	expect(t, r, "$1\r\n3\r\n")
	send("ZINCRBY", "zi", "0.5", "m")
	expect(t, r, "$3\r\n3.5\r\n")
	send("ZINCRBY", "zi", "-inf", "fresh")
	expect(t, r, "$4\r\n-inf\r\n")
	send("ZINCRBY", "zi", "1", "m", "stray")
	expect(t, r, "-ERR wrong number of arguments for 'zincrby' command\r\n")
	send("ZINCRBY", "zi", "notafloat", "m")
	expect(t, r, "-ERR value is not a valid float\r\n")

	// ZREM counts hits only; the last member kills the key.
	send("ZREM", "z", "b", "zz", "d")
	expect(t, r, ":2\r\n")
	send("ZREM", "z")
	expect(t, r, "-ERR wrong number of arguments for 'zrem' command\r\n")
	send("ZREM", "zi", "m", "fresh")
	expect(t, r, ":2\r\n")
	send("TYPE", "zi")
	expect(t, r, "+none\r\n")

	// TYPE and OBJECT ENCODING route through the root sniff, and the
	// encoding flips to skiplist past the inline count threshold.
	send("TYPE", "z")
	expect(t, r, "+zset\r\n")
	send("OBJECT", "ENCODING", "z")
	expect(t, r, "$8\r\nlistpack\r\n")
	args := []string{"ZADD", "big"}
	for i := range 140 {
		args = append(args, fmt.Sprintf("%d.5", i), fmt.Sprintf("w%03d", i))
	}
	send(args...)
	expect(t, r, ":140\r\n")
	send("OBJECT", "ENCODING", "big")
	expect(t, r, "$8\r\nskiplist\r\n")
	send("TYPE", "big")
	expect(t, r, "+zset\r\n")
	send("ZINCRBY", "big", "0", "w077")
	expect(t, r, "$4\r\n77.5\r\n")

	// Cross-type doors, and DEL takes a zset down whole.
	send("SET", "str", "plain")
	expect(t, r, "+OK\r\n")
	send("ZADD", "str", "1", "a")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZINCRBY", "str", "1", "a")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZREM", "str", "a")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("SADD", "s", "m")
	expect(t, r, ":1\r\n")
	send("ZADD", "s", "1", "m")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("DEL", "big")
	expect(t, r, ":1\r\n")
	send("TYPE", "big")
	expect(t, r, "+none\r\n")
	send("ZADD", "big", "1", "back")
	expect(t, r, ":1\r\n")
}

// TestAppendScore pins the d2string shape the zset replies use.
func TestAppendScore(t *testing.T) {
	cases := []struct {
		f    float64
		want string
	}{
		{0, "0"},
		{3, "3"},
		{-42, "-42"},
		{3.5, "3.5"},
		{-0.25, "-0.25"},
		{77.5, "77.5"},
		{1e100, "1e+100"},
		{4.611686018427388e18, "4611686018427387904"},
	}
	for _, c := range cases {
		if got := string(appendScore(nil, c.f)); got != c.want {
			t.Fatalf("appendScore(%g) = %q, want %q", c.f, got, c.want)
		}
	}
}

func TestServerZRankSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("ZADD", "z", "1", "a", "2", "b", "3", "c")
	expect(t, r, ":3\r\n")
	send("ZCARD", "z")
	expect(t, r, ":3\r\n")
	send("ZCARD", "nokey")
	expect(t, r, ":0\r\n")
	send("ZCARD", "z", "extra")
	expect(t, r, "-ERR wrong number of arguments for 'zcard' command\r\n")

	send("ZSCORE", "z", "b")
	expect(t, r, "$1\r\n2\r\n")
	send("ZSCORE", "z", "nope")
	expect(t, r, "$-1\r\n")
	send("ZSCORE", "nokey", "a")
	expect(t, r, "$-1\r\n")
	send("ZSCORE", "z")
	expect(t, r, "-ERR wrong number of arguments for 'zscore' command\r\n")
	send("ZMSCORE", "z", "a", "nope", "c")
	expect(t, r, "*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n3\r\n")
	send("ZMSCORE", "nokey", "a")
	expect(t, r, "*1\r\n$-1\r\n")
	send("ZMSCORE", "z")
	expect(t, r, "-ERR wrong number of arguments for 'zmscore' command\r\n")

	// The rank pair and the WITHSCORE shape, nil shape included: null
	// bulk plain, null array with the option.
	send("ZRANK", "z", "a")
	expect(t, r, ":0\r\n")
	send("ZRANK", "z", "c")
	expect(t, r, ":2\r\n")
	send("ZREVRANK", "z", "a")
	expect(t, r, ":2\r\n")
	send("ZREVRANK", "z", "c")
	expect(t, r, ":0\r\n")
	send("ZRANK", "z", "nope")
	expect(t, r, "$-1\r\n")
	send("ZRANK", "nokey", "a")
	expect(t, r, "$-1\r\n")
	send("ZRANK", "z", "c", "WITHSCORE")
	expect(t, r, "*2\r\n:2\r\n$1\r\n3\r\n")
	send("ZREVRANK", "z", "c", "withscore")
	expect(t, r, "*2\r\n:0\r\n$1\r\n3\r\n")
	send("ZRANK", "z", "nope", "WITHSCORE")
	expect(t, r, "*-1\r\n")
	send("ZREVRANK", "nokey", "a", "WITHSCORE")
	expect(t, r, "*-1\r\n")
	send("ZRANK", "z", "c", "BOGUS")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZRANK", "z", "c", "WITHSCORE", "extra")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZRANK", "z")
	expect(t, r, "-ERR wrong number of arguments for 'zrank' command\r\n")

	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	for _, cmd := range [][]string{
		{"ZSCORE", "str", "a"},
		{"ZMSCORE", "str", "a"},
		{"ZCARD", "str"},
		{"ZRANK", "str", "a"},
		{"ZREVRANK", "str", "a", "WITHSCORE"},
	} {
		send(cmd...)
		expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	}
}
