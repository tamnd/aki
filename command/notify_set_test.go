package command

import (
	"slices"
	"testing"
)

func TestNotifySetAddRem(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}

	if got := sendLine(t, r1, c1, "SADD s a b"); got != ":2" {
		t.Fatalf("SADD = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:sadd") {
		t.Fatalf("sadd push = %v", msg)
	}

	// Remove one member: srem fires, the set still has a member so no del.
	if got := sendLine(t, r1, c1, "SREM s a"); got != ":1" {
		t.Fatalf("SREM = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:srem") {
		t.Fatalf("srem push = %v", msg)
	}

	// Remove the last member: srem fires, then del as the set empties.
	if got := sendLine(t, r1, c1, "SREM s b"); got != ":1" {
		t.Fatalf("SREM = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:srem") {
		t.Fatalf("srem push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("del push = %v", msg)
	}
}

func TestNotifySetPop(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SADD s only"); got != ":1" {
		t.Fatalf("SADD = %q", got)
	}
	_ = readResp(t, r2) // sadd

	// SPOP the only member: spop fires, then del as the set empties.
	if _, err := c1.Write([]byte("SPOP s\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:spop") {
		t.Fatalf("spop push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("del push = %v", msg)
	}
}

func TestNotifySetMove(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SADD src m"); got != ":1" {
		t.Fatalf("SADD = %q", got)
	}
	_ = readResp(t, r2) // sadd src

	// Move the only member: srem on src, sadd on dst, del on src as it empties.
	if got := sendLine(t, r1, c1, "SMOVE src dst m"); got != ":1" {
		t.Fatalf("SMOVE = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:srem") {
		t.Fatalf("smove srem push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:sadd") {
		t.Fatalf("smove sadd push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("smove del push = %v", msg)
	}
}

func TestNotifySetStore(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SADD a x y"); got != ":2" {
		t.Fatalf("SADD = %q", got)
	}
	_ = readResp(t, r2) // sadd a
	if got := sendLine(t, r1, c1, "SADD b y z"); got != ":2" {
		t.Fatalf("SADD = %q", got)
	}
	_ = readResp(t, r2) // sadd b

	if got := sendLine(t, r1, c1, "SINTERSTORE dst a b"); got != ":1" {
		t.Fatalf("SINTERSTORE = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:sinterstore") {
		t.Fatalf("sinterstore push = %v", msg)
	}
}
