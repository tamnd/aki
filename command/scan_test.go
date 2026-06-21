package command

import (
	"bufio"
	"net"
	"sort"
	"strconv"
	"testing"
)

// scanReply reads a SCAN reply: a two-element array of a cursor bulk string and
// an array of key bulk strings. It returns the cursor and the keys.
func scanReply(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) (string, []string) {
	t.Helper()
	line := sendLine(t, r, c, cmd)
	if line != "*2" {
		t.Fatalf("expected *2 after %q, got %q", cmd, line)
	}
	cursor := bulk(t, r, c, "")
	hdr := sendLineRead(t, r)
	if hdr == "" || hdr[0] != '*' {
		t.Fatalf("expected key array header, got %q", hdr)
	}
	n, err := strconv.Atoi(hdr[1:])
	if err != nil {
		t.Fatalf("parse key array len %q: %v", hdr, err)
	}
	keys := make([]string, n)
	for i := range keys {
		keys[i] = bulk(t, r, c, "")
	}
	return cursor, keys
}

// scanAll drains a SCAN to completion and returns every key seen, sorted.
func scanAll(t *testing.T, r *bufio.Reader, c net.Conn, suffix string) []string {
	t.Helper()
	var got []string
	cursor := "0"
	for {
		next, keys := scanReply(t, r, c, "SCAN "+cursor+suffix)
		got = append(got, keys...)
		if next == "0" {
			break
		}
		cursor = next
	}
	sort.Strings(got)
	return got
}

func TestKeysPattern(t *testing.T) {
	r, c := startData(t)
	for _, k := range []string{"hello", "hallo", "hxllo", "hllo", "world"} {
		_ = sendLine(t, r, c, "SET "+k+" v")
	}
	got := array(t, r, c, "KEYS h?llo")
	sort.Strings(got)
	want := []string{"hallo", "hello", "hxllo"}
	if len(got) != len(want) {
		t.Fatalf("KEYS h?llo = %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("KEYS h?llo[%d] = %q want %q", i, got[i], want[i])
		}
	}
}

func TestKeysStar(t *testing.T) {
	r, c := startData(t)
	for _, k := range []string{"a", "b", "c"} {
		_ = sendLine(t, r, c, "SET "+k+" v")
	}
	got := array(t, r, c, "KEYS *")
	if len(got) != 3 {
		t.Fatalf("KEYS * = %v want 3 keys", got)
	}
}

func TestKeysEmptyDB(t *testing.T) {
	r, c := startData(t)
	got := array(t, r, c, "KEYS *")
	if len(got) != 0 {
		t.Fatalf("KEYS * on empty db = %v want none", got)
	}
}

func TestRandomKey(t *testing.T) {
	r, c := startData(t)
	if got := bulk(t, r, c, "RANDOMKEY"); got != "<nil>" {
		t.Fatalf("RANDOMKEY on empty db = %q want nil", got)
	}
	_ = sendLine(t, r, c, "SET only v")
	if got := bulk(t, r, c, "RANDOMKEY"); got != "only" {
		t.Fatalf("RANDOMKEY = %q want only", got)
	}
}

func TestScanFull(t *testing.T) {
	r, c := startData(t)
	want := make([]string, 0, 50)
	for i := range 50 {
		k := "key:" + strconv.Itoa(i)
		_ = sendLine(t, r, c, "SET "+k+" v")
		want = append(want, k)
	}
	sort.Strings(want)
	got := scanAll(t, r, c, " COUNT 7")
	if len(got) != len(want) {
		t.Fatalf("SCAN saw %d keys want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SCAN[%d] = %q want %q", i, got[i], want[i])
		}
	}
}

func TestScanMatch(t *testing.T) {
	r, c := startData(t)
	for _, k := range []string{"user:1", "user:2", "post:1", "post:2"} {
		_ = sendLine(t, r, c, "SET "+k+" v")
	}
	got := scanAll(t, r, c, " MATCH user:*")
	if len(got) != 2 || got[0] != "user:1" || got[1] != "user:2" {
		t.Fatalf("SCAN MATCH user:* = %v want [user:1 user:2]", got)
	}
}

func TestScanType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET str1 v")
	_ = sendLine(t, r, c, "SET str2 v")
	_ = sendLine(t, r, c, "LPUSH list1 a")
	got := scanAll(t, r, c, " TYPE string")
	if len(got) != 2 || got[0] != "str1" || got[1] != "str2" {
		t.Fatalf("SCAN TYPE string = %v want [str1 str2]", got)
	}
}

func TestScanEmpty(t *testing.T) {
	r, c := startData(t)
	cursor, keys := scanReply(t, r, c, "SCAN 0")
	if cursor != "0" || len(keys) != 0 {
		t.Fatalf("SCAN 0 on empty db = (%q,%v) want (0,[])", cursor, keys)
	}
}

func TestScanInvalidCursor(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "SCAN notanint"); got != "-ERR invalid cursor" {
		t.Fatalf("SCAN bad cursor = %q", got)
	}
}

func TestScanBadCount(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "SCAN 0 COUNT 0"); got != "-ERR syntax error" {
		t.Fatalf("SCAN COUNT 0 = %q", got)
	}
	if got := sendLine(t, r, c, "SCAN 0 BOGUS"); got != "-ERR syntax error" {
		t.Fatalf("SCAN BOGUS = %q", got)
	}
}

func TestStringMatch(t *testing.T) {
	cases := []struct {
		pat, s string
		want   bool
	}{
		{"*", "anything", true},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h*llo", "heeello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hxllo", false},
		{"h[a-z]llo", "hqllo", true},
		{"h[^ae]llo", "hbllo", true},
		{"h[^ae]llo", "hallo", false},
		{`\*`, "*", true},
		{`\*`, "a", false},
		{`foo\*bar`, "foo*bar", true},
		{"[", "[", false},
		{"[]", "x", false},
		{"a*b*c", "axxbyyc", true},
		{"a*b*c", "axxbyy", false},
	}
	for _, tc := range cases {
		if got := stringMatch([]byte(tc.pat), []byte(tc.s), false); got != tc.want {
			t.Errorf("stringMatch(%q, %q) = %v want %v", tc.pat, tc.s, got, tc.want)
		}
	}
}
