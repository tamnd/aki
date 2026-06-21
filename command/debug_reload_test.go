package command

import (
	"bufio"
	"testing"
)

// TestDebugReloadRoundTrip seeds one key of every supported type, runs DEBUG
// RELOAD, and checks the data comes back intact through the snapshot codec.
func TestDebugReloadRoundTrip(t *testing.T) {
	r, c := startData(t)
	sendLine(t, r, c, "SET s hello")
	sendLine(t, r, c, "RPUSH l a b c")
	sendLine(t, r, c, "SADD st 1 2 3")
	sendLine(t, r, c, "HSET h f1 v1 f2 v2")
	sendLine(t, r, c, "ZADD z 1 a 2.5 b")

	if got := sendLine(t, r, c, "DEBUG RELOAD"); got != "+OK" {
		t.Fatalf("DEBUG RELOAD = %q want +OK", got)
	}

	if got := bulk(t, r, c, "GET s"); got != "hello" {
		t.Fatalf("GET s = %q", got)
	}
	if got := sendLine(t, r, c, "LRANGE l 0 -1"); got != "*3" {
		t.Fatalf("LRANGE l header = %q", got)
	}
	readArrayBody(t, r, 3)
	if got := sendLine(t, r, c, "SCARD st"); got != ":3" {
		t.Fatalf("SCARD st = %q", got)
	}
	if got := sendLine(t, r, c, "HLEN h"); got != ":2" {
		t.Fatalf("HLEN h = %q", got)
	}
	if got := bulk(t, r, c, "ZSCORE z b"); got != "2.5" {
		t.Fatalf("ZSCORE z b = %q", got)
	}
}

// TestDebugReloadKeepsTTL checks an absolute expiry survives a reload.
func TestDebugReloadKeepsTTL(t *testing.T) {
	r, c := startData(t)
	sendLine(t, r, c, "SET k v")
	sendLine(t, r, c, "EXPIRE k 1000")
	if got := sendLine(t, r, c, "DEBUG RELOAD"); got != "+OK" {
		t.Fatalf("DEBUG RELOAD = %q want +OK", got)
	}
	ttl := sendLine(t, r, c, "TTL k")
	if ttl != ":1000" && ttl != ":999" {
		t.Fatalf("TTL k after reload = %q want about 1000", ttl)
	}
}

// readArrayBody consumes n bulk replies that follow an array header.
func readArrayBody(t *testing.T, r *bufio.Reader, n int) {
	t.Helper()
	for range n {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read array header line: %v", err)
		}
		if len(line) == 0 || line[0] != '$' {
			t.Fatalf("array element header = %q", line)
		}
		if _, err := r.ReadString('\n'); err != nil {
			t.Fatalf("read array element body: %v", err)
		}
	}
}
