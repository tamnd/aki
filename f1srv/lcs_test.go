package f1srv

import (
	"bufio"
	"strings"
	"testing"
	"time"
)

// readReplyDeep reads one RESP2 reply, recursing into nested arrays, and renders it as a
// bracketed string: an array is "[e1 e2 ...]", a bulk is its content, an integer is its digits.
// The LCS IDX reply nests three levels deep, so the flat readArray helper cannot describe it.
func readReplyDeep(t *testing.T, rw *bufio.ReadWriter) string {
	t.Helper()
	line, err := rw.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	line = line[:len(line)-2]
	switch line[0] {
	case '+', '-':
		return line[1:]
	case ':':
		return line[1:]
	case '$':
		if line == "$-1" {
			return "-1"
		}
		n := 0
		for _, ch := range line[1:] {
			n = n*10 + int(ch-'0')
		}
		buf := make([]byte, n+2)
		if _, err := readFull(rw, buf); err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return string(buf[:n])
	case '*':
		if line == "*-1" {
			return "-1"
		}
		n := 0
		for _, ch := range line[1:] {
			n = n*10 + int(ch-'0')
		}
		parts := make([]string, n)
		for i := 0; i < n; i++ {
			parts[i] = readReplyDeep(t, rw)
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	t.Fatalf("bad reply: %q", line)
	return ""
}

// LCS returns the longest common subsequence of two string values. The canonical Redis example
// (ohmytext, mynewtext) has LCS "mytext" of length 6. Every reply here was captured from live
// Redis 8.8.0 and Valkey 9.1.0, which agree byte for byte.
func TestLCSBasic(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MSET", "key1", "ohmytext", "key2", "mynewtext")
	expect(t, rw, "+OK")

	cmd(t, rw, "LCS", "key1", "key2")
	expect(t, rw, "$mytext")

	cmd(t, rw, "LCS", "key1", "key2", "LEN")
	expect(t, rw, ":6")

	// A missing key is an empty string, so the LCS is empty and LEN is zero.
	cmd(t, rw, "LCS", "nope1", "nope2")
	expect(t, rw, "$")
	cmd(t, rw, "LCS", "nope1", "nope2", "LEN")
	expect(t, rw, ":0")

	// The LCS of a string with itself is the whole string.
	cmd(t, rw, "LCS", "key1", "key1")
	expect(t, rw, "$ohmytext")
}

// The IDX form returns the match blocks from the end of the strings toward the start, as a
// flat four-element array on RESP2: "matches", the block list, "len", the total length.
func TestLCSIdx(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MSET", "key1", "ohmytext", "key2", "mynewtext")
	expect(t, rw, "+OK")

	// LCS key1 key2 IDX: two blocks, "text" (a[4..7] b[5..8]) then "my" (a[2..3] b[0..1]).
	cmd(t, rw, "LCS", "key1", "key2", "IDX")
	got := readReplyDeep(t, rw)
	want := "[matches [[[4 7] [5 8]] [[2 3] [0 1]]] len 6]"
	if got != want {
		t.Fatalf("LCS IDX = %s, want %s", got, want)
	}
}

// WITHMATCHLEN tags each block with its length, and MINMATCHLEN drops blocks below a length
// while the reported total stays the full LCS length.
func TestLCSIdxOptions(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MSET", "key1", "ohmytext", "key2", "mynewtext")
	expect(t, rw, "+OK")

	cmd(t, rw, "LCS", "key1", "key2", "IDX", "WITHMATCHLEN")
	got := readReplyDeep(t, rw)
	want := "[matches [[[4 7] [5 8] 4] [[2 3] [0 1] 2]] len 6]"
	if got != want {
		t.Fatalf("LCS IDX WITHMATCHLEN = %s, want %s", got, want)
	}

	// MINMATCHLEN 4 keeps only the "text" block; len is still 6.
	cmd(t, rw, "LCS", "key1", "key2", "IDX", "MINMATCHLEN", "4", "WITHMATCHLEN")
	got = readReplyDeep(t, rw)
	want = "[matches [[[4 7] [5 8] 4]] len 6]"
	if got != want {
		t.Fatalf("LCS IDX MINMATCHLEN 4 = %s, want %s", got, want)
	}

	// A negative MINMATCHLEN is accepted and filters nothing.
	cmd(t, rw, "LCS", "key1", "key2", "IDX", "MINMATCHLEN", "-1")
	got = readReplyDeep(t, rw)
	want = "[matches [[[4 7] [5 8]] [[2 3] [0 1]]] len 6]"
	if got != want {
		t.Fatalf("LCS IDX MINMATCHLEN -1 = %s, want %s", got, want)
	}
}

// LCS's error surface: LEN with IDX is rejected, a non-string key is the LCS-specific type
// error (checked before option parsing), a bad option or a bad MINMATCHLEN is reported the way
// Redis reports it, and a single key is an arity error.
func TestLCSErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MSET", "key1", "ohmytext", "key2", "mynewtext")
	expect(t, rw, "+OK")

	cmd(t, rw, "LCS", "key1", "key2", "LEN", "IDX")
	expect(t, rw, "-ERR If you want both the length and indexes, please just use IDX.")

	cmd(t, rw, "LCS", "key1", "key2", "ZZ")
	expect(t, rw, "-ERR syntax error")

	cmd(t, rw, "LCS", "key1", "key2", "IDX", "MINMATCHLEN")
	expect(t, rw, "-ERR syntax error")

	cmd(t, rw, "LCS", "key1", "key2", "IDX", "MINMATCHLEN", "abc")
	expect(t, rw, "-ERR value is not an integer or out of range")

	// The type check comes before option parsing, so a list key errors even with LEN IDX after.
	cmd(t, rw, "RPUSH", "alist", "x")
	expect(t, rw, ":1")
	cmd(t, rw, "LCS", "alist", "key2")
	expect(t, rw, "-ERR The specified keys must contain string values")
	cmd(t, rw, "LCS", "alist", "key2", "LEN", "IDX")
	expect(t, rw, "-ERR The specified keys must contain string values")

	cmd(t, rw, "LCS", "key1")
	expect(t, rw, "-ERR wrong number of arguments for 'lcs' command")
}

// An expired key reads as an empty string for LCS, the same as a missing key.
func TestLCSExpiredKey(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "key1", "ohmytext")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "key2", "mynewtext", "PX", "1")
	expect(t, rw, "+OK")
	time.Sleep(30 * time.Millisecond)
	cmd(t, rw, "LCS", "key1", "key2", "LEN")
	expect(t, rw, ":0")
}
