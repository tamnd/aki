package command

import "testing"

func TestFlushDB(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a 1")
	_ = sendLine(t, r, c, "SET b 2")
	if got := sendLine(t, r, c, "FLUSHDB"); got != "+OK" {
		t.Fatalf("FLUSHDB = %q", got)
	}
	if got := sendLine(t, r, c, "DBSIZE"); got != ":0" {
		t.Fatalf("DBSIZE after FLUSHDB = %q want :0", got)
	}
	// The database still works after a flush.
	_ = sendLine(t, r, c, "SET c 3")
	if got := bulk(t, r, c, "GET c"); got != "3" {
		t.Fatalf("GET c after reuse = %q want 3", got)
	}
}

func TestFlushDBLeavesOtherDB(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a 1")
	_ = sendLine(t, r, c, "SELECT 1")
	_ = sendLine(t, r, c, "SET b 2")
	_ = sendLine(t, r, c, "SELECT 0")
	_ = sendLine(t, r, c, "FLUSHDB")
	_ = sendLine(t, r, c, "SELECT 1")
	if got := sendLine(t, r, c, "DBSIZE"); got != ":1" {
		t.Fatalf("db1 DBSIZE after db0 FLUSHDB = %q want :1", got)
	}
}

func TestFlushAll(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a 1")
	_ = sendLine(t, r, c, "SELECT 2")
	_ = sendLine(t, r, c, "SET b 2")
	if got := sendLine(t, r, c, "FLUSHALL"); got != "+OK" {
		t.Fatalf("FLUSHALL = %q", got)
	}
	if got := sendLine(t, r, c, "DBSIZE"); got != ":0" {
		t.Fatalf("db2 DBSIZE after FLUSHALL = %q want :0", got)
	}
	_ = sendLine(t, r, c, "SELECT 0")
	if got := sendLine(t, r, c, "DBSIZE"); got != ":0" {
		t.Fatalf("db0 DBSIZE after FLUSHALL = %q want :0", got)
	}
}

func TestFlushAsync(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a 1")
	if got := sendLine(t, r, c, "FLUSHDB ASYNC"); got != "+OK" {
		t.Fatalf("FLUSHDB ASYNC = %q", got)
	}
	if got := sendLine(t, r, c, "FLUSHALL SYNC"); got != "+OK" {
		t.Fatalf("FLUSHALL SYNC = %q", got)
	}
}

func TestFlushBadOption(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "FLUSHDB BOGUS"); got != "-ERR syntax error" {
		t.Fatalf("FLUSHDB BOGUS = %q", got)
	}
	if got := sendLine(t, r, c, "FLUSHALL ASYNC EXTRA"); got != "-ERR syntax error" {
		t.Fatalf("FLUSHALL with extra = %q", got)
	}
}

func TestSwapDB(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k zero")
	_ = sendLine(t, r, c, "SELECT 1")
	_ = sendLine(t, r, c, "SET k one")
	_ = sendLine(t, r, c, "SELECT 0")
	if got := sendLine(t, r, c, "SWAPDB 0 1"); got != "+OK" {
		t.Fatalf("SWAPDB = %q", got)
	}
	// db0 now holds what db1 had.
	if got := bulk(t, r, c, "GET k"); got != "one" {
		t.Fatalf("GET k in db0 after swap = %q want one", got)
	}
	_ = sendLine(t, r, c, "SELECT 1")
	if got := bulk(t, r, c, "GET k"); got != "zero" {
		t.Fatalf("GET k in db1 after swap = %q want zero", got)
	}
}

func TestSwapDBSameIndex(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "SWAPDB 0 0"); got != "+OK" {
		t.Fatalf("SWAPDB 0 0 = %q", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "v" {
		t.Fatalf("GET k after self swap = %q want v", got)
	}
}

func TestSwapDBErrors(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "SWAPDB 0 99"); got != "-ERR DB index is out of range" {
		t.Fatalf("SWAPDB out of range = %q", got)
	}
	if got := sendLine(t, r, c, "SWAPDB x 1"); got != "-ERR value is not an integer or out of range" {
		t.Fatalf("SWAPDB bad int = %q", got)
	}
}
