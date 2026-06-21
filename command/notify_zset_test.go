package command

import (
	"slices"
	"testing"
)

func TestNotifyZSetAddIncr(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:z*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}

	if got := sendLine(t, r1, c1, "ZADD z 1 a 2 b"); got != ":2" {
		t.Fatalf("ZADD = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:zadd") {
		t.Fatalf("zadd push = %v", msg)
	}

	// ZADD INCR fires zincr.
	if _, err := c1.Write([]byte("ZADD z INCR 5 a\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:zincr") {
		t.Fatalf("zadd incr push = %v", msg)
	}

	// ZINCRBY fires zincr.
	if _, err := c1.Write([]byte("ZINCRBY z 3 b\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:zincr") {
		t.Fatalf("zincrby push = %v", msg)
	}
}

func TestNotifyZSetRem(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "ZADD z 1 a 2 b"); got != ":2" {
		t.Fatalf("ZADD = %q", got)
	}
	_ = readResp(t, r2) // zadd

	// Remove one member: zrem fires, the set still has a member so no del.
	if got := sendLine(t, r1, c1, "ZREM z a"); got != ":1" {
		t.Fatalf("ZREM = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:zrem") {
		t.Fatalf("zrem push = %v", msg)
	}

	// Remove the last member: zrem then del.
	if got := sendLine(t, r1, c1, "ZREM z b"); got != ":1" {
		t.Fatalf("ZREM = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:zrem") {
		t.Fatalf("zrem push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("del push = %v", msg)
	}
}

func TestNotifyZSetPop(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "ZADD z 1 a 2 b"); got != ":2" {
		t.Fatalf("ZADD = %q", got)
	}
	_ = readResp(t, r2) // zadd

	// ZPOPMIN one member: zpopmin fires, set still has a member.
	if _, err := c1.Write([]byte("ZPOPMIN z\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:zpopmin") {
		t.Fatalf("zpopmin push = %v", msg)
	}

	// ZPOPMAX the last member: zpopmax then del.
	if _, err := c1.Write([]byte("ZPOPMAX z\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:zpopmax") {
		t.Fatalf("zpopmax push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("del push = %v", msg)
	}
}

func TestNotifyZSetRemRange(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "ZADD z 1 a 2 b 3 c"); got != ":3" {
		t.Fatalf("ZADD = %q", got)
	}
	_ = readResp(t, r2) // zadd

	// Remove a rank slice that leaves a member behind.
	if got := sendLine(t, r1, c1, "ZREMRANGEBYRANK z 0 0"); got != ":1" {
		t.Fatalf("ZREMRANGEBYRANK = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:zremrangebyrank") {
		t.Fatalf("zremrangebyrank push = %v", msg)
	}

	// Remove the rest by score: event then del.
	if got := sendLine(t, r1, c1, "ZREMRANGEBYSCORE z -inf +inf"); got != ":2" {
		t.Fatalf("ZREMRANGEBYSCORE = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:zremrangebyscore") {
		t.Fatalf("zremrangebyscore push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("del push = %v", msg)
	}
}

func TestNotifyZSetRangeStore(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "ZADD src 1 a 2 b"); got != ":2" {
		t.Fatalf("ZADD = %q", got)
	}
	_ = readResp(t, r2) // zadd

	// Store a non-empty range: zrangestore fires on the destination.
	if got := sendLine(t, r1, c1, "ZRANGESTORE dst src 0 -1"); got != ":2" {
		t.Fatalf("ZRANGESTORE = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:zrangestore") {
		t.Fatalf("zrangestore push = %v", msg)
	}

	// An empty range over an existing destination clears it with del.
	if got := sendLine(t, r1, c1, "ZRANGESTORE dst src 5 10"); got != ":0" {
		t.Fatalf("ZRANGESTORE empty = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("zrangestore del push = %v", msg)
	}
}

func TestNotifyZSetStore(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "ZADD a 1 x 2 y"); got != ":2" {
		t.Fatalf("ZADD a = %q", got)
	}
	_ = readResp(t, r2) // zadd a
	if got := sendLine(t, r1, c1, "ZADD b 3 y 4 z"); got != ":2" {
		t.Fatalf("ZADD b = %q", got)
	}
	_ = readResp(t, r2) // zadd b

	// Union writes a non-empty destination: zunionstore fires.
	if got := sendLine(t, r1, c1, "ZUNIONSTORE dst 2 a b"); got != ":3" {
		t.Fatalf("ZUNIONSTORE = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:zunionstore") {
		t.Fatalf("zunionstore push = %v", msg)
	}

	// Diff that comes back empty clears the existing destination with del.
	if got := sendLine(t, r1, c1, "ZDIFFSTORE dst 2 a a"); got != ":0" {
		t.Fatalf("ZDIFFSTORE = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("zdiffstore del push = %v", msg)
	}
}

func TestNotifyZSetMPop(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "ZADD z 1 a"); got != ":1" {
		t.Fatalf("ZADD = %q", got)
	}
	_ = readResp(t, r2) // zadd

	// ZMPOP MIN pops the only member: zpopmin then del.
	if _, err := c1.Write([]byte("ZMPOP 1 z MIN\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:zpopmin") {
		t.Fatalf("zmpop zpopmin push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("zmpop del push = %v", msg)
	}
}
