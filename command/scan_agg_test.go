package command

import (
	"sort"
	"testing"
)

func TestHScan(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1 f2 v2 f3 v3")
	cursor, flat := scanReply(t, r, c, "HSCAN h 0")
	if cursor != "0" {
		t.Fatalf("HSCAN cursor = %q want 0", cursor)
	}
	got := map[string]string{}
	for i := 0; i+1 < len(flat); i += 2 {
		got[flat[i]] = flat[i+1]
	}
	if len(got) != 3 || got["f1"] != "v1" || got["f2"] != "v2" || got["f3"] != "v3" {
		t.Fatalf("HSCAN = %v", got)
	}
}

func TestHScanNoValues(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1 f2 v2")
	_, fields := scanReply(t, r, c, "HSCAN h 0 NOVALUES")
	sort.Strings(fields)
	if len(fields) != 2 || fields[0] != "f1" || fields[1] != "f2" {
		t.Fatalf("HSCAN NOVALUES = %v want [f1 f2]", fields)
	}
}

func TestHScanMatch(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h user:1 a user:2 b post:1 c")
	_, flat := scanReply(t, r, c, "HSCAN h 0 MATCH user:* NOVALUES")
	sort.Strings(flat)
	if len(flat) != 2 || flat[0] != "user:1" || flat[1] != "user:2" {
		t.Fatalf("HSCAN MATCH = %v", flat)
	}
}

func TestHScanMissingKey(t *testing.T) {
	r, c := startData(t)
	cursor, flat := scanReply(t, r, c, "HSCAN nope 0")
	if cursor != "0" || len(flat) != 0 {
		t.Fatalf("HSCAN missing = (%q,%v) want (0,[])", cursor, flat)
	}
}

func TestSScan(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD s a b c")
	_, members := scanReply(t, r, c, "SSCAN s 0")
	sort.Strings(members)
	if len(members) != 3 || members[0] != "a" || members[1] != "b" || members[2] != "c" {
		t.Fatalf("SSCAN = %v", members)
	}
}

func TestSScanMatch(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD s apple apricot banana")
	_, members := scanReply(t, r, c, "SSCAN s 0 MATCH ap* COUNT 100")
	sort.Strings(members)
	if len(members) != 2 || members[0] != "apple" || members[1] != "apricot" {
		t.Fatalf("SSCAN MATCH = %v", members)
	}
}

func TestZScan(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c")
	_, flat := scanReply(t, r, c, "ZSCAN z 0")
	got := map[string]string{}
	for i := 0; i+1 < len(flat); i += 2 {
		got[flat[i]] = flat[i+1]
	}
	if len(got) != 3 || got["a"] != "1" || got["b"] != "2" || got["c"] != "3" {
		t.Fatalf("ZSCAN = %v", got)
	}
}

func TestZScanMatch(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 one 2 two 3 three")
	_, flat := scanReply(t, r, c, "ZSCAN z 0 MATCH t*")
	got := map[string]string{}
	for i := 0; i+1 < len(flat); i += 2 {
		got[flat[i]] = flat[i+1]
	}
	if len(got) != 2 || got["two"] != "2" || got["three"] != "3" {
		t.Fatalf("ZSCAN MATCH = %v", got)
	}
}

func TestAggScanInvalidCursor(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD s a")
	if got := sendLine(t, r, c, "SSCAN s notanint"); got != "-ERR invalid cursor" {
		t.Fatalf("SSCAN bad cursor = %q", got)
	}
}

func TestAggScanBadOption(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD s a")
	if got := sendLine(t, r, c, "SSCAN s 0 COUNT 0"); got != "-ERR syntax error" {
		t.Fatalf("SSCAN COUNT 0 = %q", got)
	}
	// NOVALUES is only valid on HSCAN.
	if got := sendLine(t, r, c, "SSCAN s 0 NOVALUES"); got != "-ERR syntax error" {
		t.Fatalf("SSCAN NOVALUES = %q", got)
	}
}

func TestAggScanWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET str v")
	if got := sendLine(t, r, c, "HSCAN str 0"); got != "-"+wrongTypeError {
		t.Fatalf("HSCAN wrongtype = %q", got)
	}
	if got := sendLine(t, r, c, "SSCAN str 0"); got != "-"+wrongTypeError {
		t.Fatalf("SSCAN wrongtype = %q", got)
	}
	if got := sendLine(t, r, c, "ZSCAN str 0"); got != "-"+wrongTypeError {
		t.Fatalf("ZSCAN wrongtype = %q", got)
	}
}
