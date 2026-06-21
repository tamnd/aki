package command

import (
	"slices"
	"testing"
)

func TestNotifyHashSetDel(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}

	if got := sendLine(t, r1, c1, "HSET h f1 v1 f2 v2"); got != ":2" {
		t.Fatalf("HSET = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:hset") {
		t.Fatalf("hset push = %v", msg)
	}

	// Delete one field: hdel fires, the hash still has a field so no del.
	if got := sendLine(t, r1, c1, "HDEL h f1"); got != ":1" {
		t.Fatalf("HDEL = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:hdel") {
		t.Fatalf("hdel push = %v", msg)
	}

	// Delete the last field: hdel fires, then del as the hash empties.
	if got := sendLine(t, r1, c1, "HDEL h f2"); got != ":1" {
		t.Fatalf("HDEL = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:hdel") {
		t.Fatalf("hdel push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("del push = %v", msg)
	}
}

func TestNotifyHashSetNX(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:hset\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "HSETNX h f v"); got != ":1" {
		t.Fatalf("HSETNX = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:hset") {
		t.Fatalf("hsetnx push = %v", msg)
	}
}

func TestNotifyHashIncr(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:hincr*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}

	if got := sendLine(t, r1, c1, "HINCRBY h n 5"); got != ":5" {
		t.Fatalf("HINCRBY = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:hincrby") {
		t.Fatalf("hincrby push = %v", msg)
	}

	if _, err := c1.Write([]byte("HINCRBYFLOAT h n 1.5\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:hincrbyfloat") {
		t.Fatalf("hincrbyfloat push = %v", msg)
	}
}

func TestNotifyHashExpire(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "HSET h f v"); got != ":1" {
		t.Fatalf("HSET = %q", got)
	}
	_ = readResp(t, r2) // hset

	// HEXPIRE on the field fires hexpire.
	if _, err := c1.Write([]byte("HEXPIRE h 100 FIELDS 1 f\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:hexpire") {
		t.Fatalf("hexpire push = %v", msg)
	}

	// HPERSIST clears it and fires hpersist.
	if _, err := c1.Write([]byte("HPERSIST h FIELDS 1 f\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:hpersist") {
		t.Fatalf("hpersist push = %v", msg)
	}
}

func TestNotifyHashGetDel(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "HSET h f v"); got != ":1" {
		t.Fatalf("HSET = %q", got)
	}
	_ = readResp(t, r2) // hset

	// HGETDEL removes the only field: hdel then del.
	if _, err := c1.Write([]byte("HGETDEL h FIELDS 1 f\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:hdel") {
		t.Fatalf("hgetdel hdel push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("hgetdel del push = %v", msg)
	}
}
