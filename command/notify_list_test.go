package command

import (
	"slices"
	"testing"
)

func TestNotifyListPush(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*push\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}

	if got := sendLine(t, r1, c1, "LPUSH l a"); got != ":1" {
		t.Fatalf("LPUSH = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:lpush") {
		t.Fatalf("lpush push = %v", msg)
	}
	if got := sendLine(t, r1, c1, "RPUSH l b"); got != ":2" {
		t.Fatalf("RPUSH = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:rpush") {
		t.Fatalf("rpush push = %v", msg)
	}
}

func TestNotifyListPop(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}

	if got := sendLine(t, r1, c1, "RPUSH l a b"); got != ":2" {
		t.Fatalf("RPUSH = %q", got)
	}
	_ = readResp(t, r2) // rpush event

	// Pop the first element: lpop fires, the list still has one element so no del.
	if _, err := c1.Write([]byte("LPOP l\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:lpop") {
		t.Fatalf("lpop push = %v", msg)
	}

	// Pop the last element: rpop fires, then del fires as the list empties.
	if _, err := c1.Write([]byte("RPOP l\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:rpop") {
		t.Fatalf("rpop push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("del push = %v", msg)
	}
}

func TestNotifyListModify(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "RPUSH l a b c"); got != ":3" {
		t.Fatalf("RPUSH = %q", got)
	}
	_ = readResp(t, r2) // rpush

	if got := sendLine(t, r1, c1, "LSET l 0 z"); got != "+OK" {
		t.Fatalf("LSET = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:lset") {
		t.Fatalf("lset push = %v", msg)
	}

	if got := sendLine(t, r1, c1, "LINSERT l BEFORE b x"); got != ":4" {
		t.Fatalf("LINSERT = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:linsert") {
		t.Fatalf("linsert push = %v", msg)
	}

	if got := sendLine(t, r1, c1, "LREM l 0 z"); got != ":1" {
		t.Fatalf("LREM = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:lrem") {
		t.Fatalf("lrem push = %v", msg)
	}

	if got := sendLine(t, r1, c1, "LTRIM l 0 0"); got != "+OK" {
		t.Fatalf("LTRIM = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:ltrim") {
		t.Fatalf("ltrim push = %v", msg)
	}
}

func TestNotifyListMove(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "RPUSH src a"); got != ":1" {
		t.Fatalf("RPUSH = %q", got)
	}
	_ = readResp(t, r2) // rpush src

	// The only element moves, so src empties: rpop on src, lpush on dst, del on src.
	if _, err := c1.Write([]byte("LMOVE src dst RIGHT LEFT\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:rpop") {
		t.Fatalf("move src pop push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:lpush") {
		t.Fatalf("move dst push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("move src del push = %v", msg)
	}
}

func TestNotifyListMPop(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "RPUSH l a"); got != ":1" {
		t.Fatalf("RPUSH = %q", got)
	}
	_ = readResp(t, r2) // rpush

	if _, err := c1.Write([]byte("LMPOP 1 l LEFT\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:lpop") {
		t.Fatalf("lmpop pop push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("lmpop del push = %v", msg)
	}
}
