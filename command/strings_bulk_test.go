package command

import (
	"bufio"
	"net"
	"strconv"
	"testing"
)

// array reads an array reply header (*N\r\n) and then N bulk elements, returning
// the elements with "<nil>" for any null element.
func array(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) []string {
	t.Helper()
	line := sendLine(t, r, c, cmd)
	if line == "" || line[0] != '*' {
		t.Fatalf("expected array header after %q, got %q", cmd, line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		t.Fatalf("parse array len %q: %v", line, err)
	}
	out := make([]string, n)
	for i := range out {
		out[i] = bulk(t, r, c, "")
	}
	return out
}

func TestMSetMGet(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "MSET a alpha b beta c 42"); got != "+OK" {
		t.Fatalf("MSET = %q want +OK", got)
	}
	got := array(t, r, c, "MGET a b c missing")
	want := []string{"alpha", "beta", "42", "<nil>"}
	if len(got) != len(want) {
		t.Fatalf("MGET len = %d want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("MGET[%d] = %q want %q", i, got[i], want[i])
		}
	}
}

func TestMSetOddArgs(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "MSET a 1 b"); got != "-ERR wrong number of arguments for 'mset' command" {
		t.Fatalf("MSET odd = %q", got)
	}
}

func TestMSetNX(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "MSETNX a 1 b 2"); got != ":1" {
		t.Fatalf("MSETNX fresh = %q want :1", got)
	}
	// One key already exists, so the whole command is a no-op returning 0.
	if got := sendLine(t, r, c, "MSETNX b 9 c 3"); got != ":0" {
		t.Fatalf("MSETNX partial = %q want :0", got)
	}
	if got := bulk(t, r, c, "GET c"); got != "<nil>" {
		t.Fatalf("GET c = %q want nil (not written)", got)
	}
	if got := bulk(t, r, c, "GET b"); got != "2" {
		t.Fatalf("GET b = %q want 2 (unchanged)", got)
	}
}

func TestAppend(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "APPEND k hello"); got != ":5" {
		t.Fatalf("APPEND new = %q want :5", got)
	}
	if got := sendLine(t, r, c, "APPEND k world"); got != ":10" {
		t.Fatalf("APPEND existing = %q want :10", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "helloworld" {
		t.Fatalf("GET k = %q want helloworld", got)
	}
}

func TestStrlen(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k hello")
	if got := sendLine(t, r, c, "STRLEN k"); got != ":5" {
		t.Fatalf("STRLEN = %q want :5", got)
	}
	if got := sendLine(t, r, c, "STRLEN missing"); got != ":0" {
		t.Fatalf("STRLEN missing = %q want :0", got)
	}
}

func TestSetRange(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k HelloXWorld")
	if got := sendLine(t, r, c, "SETRANGE k 5 _"); got != ":11" {
		t.Fatalf("SETRANGE = %q want :11", got)
	}
	if got := bulk(t, r, c, "GET k"); got != "Hello_World" {
		t.Fatalf("GET k = %q want Hello_World", got)
	}
	// Past the end of a missing key zero-pads with NUL bytes.
	if got := sendLine(t, r, c, "SETRANGE pad 5 hi"); got != ":7" {
		t.Fatalf("SETRANGE pad = %q want :7", got)
	}
	if got := bulk(t, r, c, "GET pad"); got != "\x00\x00\x00\x00\x00hi" {
		t.Fatalf("GET pad = %q", got)
	}
	// A bad offset is an error.
	if got := sendLine(t, r, c, "SETRANGE k -1 x"); got != "-ERR offset is not an integer or out of range" {
		t.Fatalf("SETRANGE bad offset = %q", got)
	}
}

func TestSetRangeEmptyValue(t *testing.T) {
	r, c := startData(t)
	// An empty value on a missing key reports 0 and does not create the key.
	if got := sendLine(t, r, c, "SETRANGE k 5 \"\""); got != ":0" {
		t.Fatalf("SETRANGE empty = %q want :0", got)
	}
	if got := sendLine(t, r, c, "EXISTS k"); got != ":0" {
		t.Fatalf("EXISTS k = %q want :0", got)
	}
}

func TestGetRange(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k ThisIsAString")
	cases := []struct {
		cmd, want string
	}{
		{"GETRANGE k 0 3", "This"},
		{"GETRANGE k -6 -1", "String"},
		{"GETRANGE k 0 -1", "ThisIsAString"},
		{"GETRANGE k 6 100", "AString"},
		{"GETRANGE k 100 200", ""},
		{"GETRANGE k 5 3", ""},
		{"SUBSTR k 0 3", "This"},
		{"GETRANGE missing 0 -1", ""},
	}
	for _, tc := range cases {
		if got := bulk(t, r, c, tc.cmd); got != tc.want {
			t.Fatalf("%s = %q want %q", tc.cmd, got, tc.want)
		}
	}
}
