package command

import (
	"slices"
	"testing"
)

func TestNotifyDel(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("SUBSCRIBE __keyevent@0__:del\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)

	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SET foo bar"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "DEL foo"); got != ":1" {
		t.Fatalf("DEL = %q", got)
	}

	msg := readResp(t, r2)
	if !slices.Contains(msg, "__keyevent@0__:del") || !slices.Contains(msg, "foo") {
		t.Fatalf("del push = %v", msg)
	}
}

func TestNotifyExpire(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("SUBSCRIBE __keyevent@0__:expire\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)

	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SET foo bar"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "EXPIRE foo 100"); got != ":1" {
		t.Fatalf("EXPIRE = %q", got)
	}

	msg := readResp(t, r2)
	if !slices.Contains(msg, "__keyevent@0__:expire") || !slices.Contains(msg, "foo") {
		t.Fatalf("expire push = %v", msg)
	}
}

// TestNotifyExpirePast checks that an EXPIRE with a deadline in the past deletes
// the key and fires "del", not "expire", matching Redis.
func TestNotifyExpirePast(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)

	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SET foo bar"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	// SET fires first; drain it before triggering the past expiry.
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "EXPIRE foo -1"); got != ":1" {
		t.Fatalf("EXPIRE = %q", got)
	}

	msg := readResp(t, r2)
	if !slices.Contains(msg, "__keyevent@0__:del") {
		t.Fatalf("past-expire push = %v, want a del event", msg)
	}
}

func TestNotifyPersist(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("SUBSCRIBE __keyevent@0__:persist\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)

	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SET foo bar EX 100"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "PERSIST foo"); got != ":1" {
		t.Fatalf("PERSIST = %q", got)
	}

	msg := readResp(t, r2)
	if !slices.Contains(msg, "__keyevent@0__:persist") || !slices.Contains(msg, "foo") {
		t.Fatalf("persist push = %v", msg)
	}
}

func TestNotifyRename(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:rename_*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)

	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SET foo bar"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "RENAME foo baz"); got != "+OK" {
		t.Fatalf("RENAME = %q", got)
	}

	from := readResp(t, r2)
	if !slices.Contains(from, "__keyevent@0__:rename_from") || !slices.Contains(from, "foo") {
		t.Fatalf("rename_from push = %v", from)
	}
	to := readResp(t, r2)
	if !slices.Contains(to, "__keyevent@0__:rename_to") || !slices.Contains(to, "baz") {
		t.Fatalf("rename_to push = %v", to)
	}
}

func TestNotifyCopy(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("SUBSCRIBE __keyevent@0__:copy_to\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)

	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SET foo bar"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "COPY foo baz"); got != ":1" {
		t.Fatalf("COPY = %q", got)
	}

	msg := readResp(t, r2)
	if !slices.Contains(msg, "__keyevent@0__:copy_to") || !slices.Contains(msg, "baz") {
		t.Fatalf("copy_to push = %v", msg)
	}
}

// TestNotifyMove checks that MOVE fires move_from in the source database and
// move_to in the destination database, each on its own keyevent channel.
func TestNotifyMove(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	// One pattern catches the event on either database.
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@*__:move_*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)

	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SET foo bar"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "MOVE foo 1"); got != ":1" {
		t.Fatalf("MOVE = %q", got)
	}

	from := readResp(t, r2)
	if !slices.Contains(from, "__keyevent@0__:move_from") || !slices.Contains(from, "foo") {
		t.Fatalf("move_from push = %v", from)
	}
	to := readResp(t, r2)
	if !slices.Contains(to, "__keyevent@1__:move_to") || !slices.Contains(to, "foo") {
		t.Fatalf("move_to push = %v", to)
	}
}
