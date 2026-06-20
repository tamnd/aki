package command

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

// readLines sends cmd and reads n raw reply lines, each with the trailing CRLF
// stripped. It is used to check the nested array shape of LCS IDX replies.
func readLines(t *testing.T, r *bufio.Reader, c net.Conn, cmd string, n int) []string {
	t.Helper()
	if _, err := c.Write([]byte(cmd + "\r\n")); err != nil {
		t.Fatal(err)
	}
	out := make([]string, n)
	for i := range out {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read line %d after %q: %v", i, cmd, err)
		}
		out[i] = strings.TrimRight(line, "\r\n")
	}
	return out
}

func TestLCSBasic(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET key1 ohmytext")
	_ = sendLine(t, r, c, "SET key2 mynewtext")
	if got := bulk(t, r, c, "LCS key1 key2"); got != "mytext" {
		t.Fatalf("LCS = %q want mytext", got)
	}
	if got := sendLine(t, r, c, "LCS key1 key2 LEN"); got != ":6" {
		t.Fatalf("LCS LEN = %q want :6", got)
	}
}

func TestLCSMissingKeys(t *testing.T) {
	r, c := startData(t)
	// Both keys missing means an empty common subsequence.
	if got := bulk(t, r, c, "LCS a b"); got != "" {
		t.Fatalf("LCS missing = %q want empty", got)
	}
	if got := sendLine(t, r, c, "LCS a b LEN"); got != ":0" {
		t.Fatalf("LCS missing LEN = %q want :0", got)
	}
}

func TestLCSLenAndIdxConflict(t *testing.T) {
	r, c := startData(t)
	want := "-ERR If you want both the length and indexes, please just use IDX."
	if got := sendLine(t, r, c, "LCS a b LEN IDX"); got != want {
		t.Fatalf("LCS LEN IDX = %q", got)
	}
}

func TestLCSIdx(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET key1 ohmytext")
	_ = sendLine(t, r, c, "SET key2 mynewtext")
	// RESP2 flattens the map to *4. The two blocks are "text" (a[4..7],b[5..8])
	// then "my" (a[2..3],b[0..1]), end-first.
	want := []string{
		"*4",
		"$7", "matches",
		"*2",
		"*2",
		"*2", ":4", ":7",
		"*2", ":5", ":8",
		"*2",
		"*2", ":2", ":3",
		"*2", ":0", ":1",
		"$3", "len",
		":6",
	}
	got := readLines(t, r, c, "LCS key1 key2 IDX", len(want))
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("IDX line %d = %q want %q (full %v)", i, got[i], want[i], got)
		}
	}
}

func TestLCSIdxMinMatchLen(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET key1 ohmytext")
	_ = sendLine(t, r, c, "SET key2 mynewtext")
	// MINMATCHLEN 4 drops the "my" block (length 2), leaving only "text".
	want := []string{
		"*4",
		"$7", "matches",
		"*1",
		"*2",
		"*2", ":4", ":7",
		"*2", ":5", ":8",
		"$3", "len",
		":6",
	}
	got := readLines(t, r, c, "LCS key1 key2 IDX MINMATCHLEN 4", len(want))
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("IDX MINMATCHLEN line %d = %q want %q (full %v)", i, got[i], want[i], got)
		}
	}
}

func TestLCSIdxWithMatchLen(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET key1 ohmytext")
	_ = sendLine(t, r, c, "SET key2 mynewtext")
	want := []string{
		"*4",
		"$7", "matches",
		"*2",
		"*3",
		"*2", ":4", ":7",
		"*2", ":5", ":8",
		":4",
		"*3",
		"*2", ":2", ":3",
		"*2", ":0", ":1",
		":2",
		"$3", "len",
		":6",
	}
	got := readLines(t, r, c, "LCS key1 key2 IDX WITHMATCHLEN", len(want))
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("IDX WITHMATCHLEN line %d = %q want %q (full %v)", i, got[i], want[i], got)
		}
	}
}
