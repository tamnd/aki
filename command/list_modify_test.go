package command

import "testing"

func TestLIndex(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b c")
	if got := bulk(t, r, c, "LINDEX k 0"); got != "a" {
		t.Fatalf("LINDEX 0 = %q want a", got)
	}
	if got := bulk(t, r, c, "LINDEX k -1"); got != "c" {
		t.Fatalf("LINDEX -1 = %q want c", got)
	}
	if got := bulk(t, r, c, "LINDEX k 5"); got != "<nil>" {
		t.Fatalf("LINDEX 5 = %q want nil", got)
	}
	if got := bulk(t, r, c, "LINDEX missing 0"); got != "<nil>" {
		t.Fatalf("LINDEX missing = %q want nil", got)
	}
}

func TestLSet(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b c")
	if got := sendLine(t, r, c, "LSET k 1 B"); got != "+OK" {
		t.Fatalf("LSET = %q want +OK", got)
	}
	if got := bulk(t, r, c, "LINDEX k 1"); got != "B" {
		t.Fatalf("after LSET LINDEX 1 = %q want B", got)
	}
	if got := sendLine(t, r, c, "LSET k -1 Z"); got != "+OK" {
		t.Fatalf("LSET -1 = %q", got)
	}
	if got := bulk(t, r, c, "LINDEX k -1"); got != "Z" {
		t.Fatalf("after LSET -1 = %q want Z", got)
	}
	if got := sendLine(t, r, c, "LSET k 9 X"); got != "-ERR index out of range" {
		t.Fatalf("LSET oob = %q", got)
	}
	if got := sendLine(t, r, c, "LSET missing 0 X"); got != "-ERR no such key" {
		t.Fatalf("LSET missing = %q", got)
	}
}

func TestLInsert(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b c")
	if got := sendLine(t, r, c, "LINSERT k BEFORE b X"); got != ":4" {
		t.Fatalf("LINSERT BEFORE = %q want :4", got)
	}
	if got := array(t, r, c, "LRANGE k 0 -1"); !equalSlice(got, []string{"a", "X", "b", "c"}) {
		t.Fatalf("after LINSERT BEFORE = %v", got)
	}
	if got := sendLine(t, r, c, "LINSERT k after b Y"); got != ":5" {
		t.Fatalf("LINSERT after (lowercase) = %q want :5", got)
	}
	if got := array(t, r, c, "LRANGE k 0 -1"); !equalSlice(got, []string{"a", "X", "b", "Y", "c"}) {
		t.Fatalf("after LINSERT AFTER = %v", got)
	}
	if got := sendLine(t, r, c, "LINSERT k BEFORE nope Z"); got != ":-1" {
		t.Fatalf("LINSERT missing pivot = %q want :-1", got)
	}
	if got := sendLine(t, r, c, "LINSERT missing BEFORE a b"); got != ":0" {
		t.Fatalf("LINSERT missing key = %q want :0", got)
	}
	if got := sendLine(t, r, c, "LINSERT k MIDDLE b Z"); got != "-ERR syntax error" {
		t.Fatalf("LINSERT bad dir = %q", got)
	}
}

func TestLRemPositive(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b a c a d")
	if got := sendLine(t, r, c, "LREM k 2 a"); got != ":2" {
		t.Fatalf("LREM 2 a = %q want :2", got)
	}
	if got := array(t, r, c, "LRANGE k 0 -1"); !equalSlice(got, []string{"b", "c", "a", "d"}) {
		t.Fatalf("after LREM 2 a = %v", got)
	}
}

func TestLRemNegative(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b a c a d")
	if got := sendLine(t, r, c, "LREM k -2 a"); got != ":2" {
		t.Fatalf("LREM -2 a = %q want :2", got)
	}
	if got := array(t, r, c, "LRANGE k 0 -1"); !equalSlice(got, []string{"a", "b", "c", "d"}) {
		t.Fatalf("after LREM -2 a = %v", got)
	}
}

func TestLRemAllAndDelete(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a a a")
	if got := sendLine(t, r, c, "LREM k 0 a"); got != ":3" {
		t.Fatalf("LREM 0 a = %q want :3", got)
	}
	if got := sendLine(t, r, c, "EXISTS k"); got != ":0" {
		t.Fatalf("emptied list should be deleted, EXISTS = %q", got)
	}
	if got := sendLine(t, r, c, "LREM missing 0 a"); got != ":0" {
		t.Fatalf("LREM missing = %q want :0", got)
	}
}

func TestLTrim(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b c d e")
	if got := sendLine(t, r, c, "LTRIM k 1 3"); got != "+OK" {
		t.Fatalf("LTRIM = %q want +OK", got)
	}
	if got := array(t, r, c, "LRANGE k 0 -1"); !equalSlice(got, []string{"b", "c", "d"}) {
		t.Fatalf("after LTRIM 1 3 = %v", got)
	}
	// A start past the stop empties and deletes the key.
	if got := sendLine(t, r, c, "LTRIM k 5 10"); got != "+OK" {
		t.Fatalf("LTRIM empty = %q", got)
	}
	if got := sendLine(t, r, c, "EXISTS k"); got != ":0" {
		t.Fatalf("trimmed-to-empty should be deleted, EXISTS = %q", got)
	}
	if got := sendLine(t, r, c, "LTRIM missing 0 -1"); got != "+OK" {
		t.Fatalf("LTRIM missing = %q want +OK", got)
	}
}

func TestListModifyWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s hello")
	for _, cmd := range []string{"LINDEX s 0", "LSET s 0 x", "LINSERT s BEFORE a b", "LREM s 0 a", "LTRIM s 0 1"} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
}
