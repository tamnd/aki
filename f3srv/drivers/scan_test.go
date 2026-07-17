package drivers

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
)

// scanReply reads one SCAN reply: a two-element array of the next cursor bulk
// and the key page. It returns the cursor and the keys as a set, since SCAN
// leaves the page order unspecified and the page here also interleaves two
// shards' partials. A malformed envelope fails the test.
func scanReply(t *testing.T, br *bufio.Reader) (string, map[string]bool) {
	t.Helper()
	head, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read SCAN header: %v", err)
	}
	if head != "*2\r\n" {
		t.Fatalf("SCAN header = %q, want *2", head)
	}
	cursor := readBulkPayload(t, br)
	pageHead, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read SCAN page header: %v", err)
	}
	if len(pageHead) == 0 || pageHead[0] != '*' {
		t.Fatalf("SCAN page header = %q, want array", pageHead)
	}
	n, err := strconv.Atoi(strings.TrimSuffix(pageHead[1:], "\r\n"))
	if err != nil {
		t.Fatalf("SCAN page length %q: %v", pageHead, err)
	}
	keys := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		keys[readBulkPayload(t, br)] = true
	}
	return cursor, keys
}

// readBulkPayload reads one bulk string body, sharing the framing readBulk uses
// but off a reader already positioned at the bulk header.
func readBulkPayload(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	hdr, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read bulk header: %v", err)
	}
	if len(hdr) == 0 || hdr[0] != '$' {
		t.Fatalf("bulk header = %q", hdr)
	}
	blen, err := strconv.Atoi(strings.TrimSuffix(hdr[1:], "\r\n"))
	if err != nil {
		t.Fatalf("bulk length %q: %v", hdr, err)
	}
	buf := make([]byte, blen+2)
	if _, err := io.ReadFull(br, buf); err != nil {
		t.Fatalf("read bulk payload: %v", err)
	}
	return string(buf[:blen])
}

// scanAll drives a full SCAN loop from cursor 0 until the server answers cursor
// 0, folding every page into one key set. f3 answers the whole keyspace in a
// single page, so this makes one round, but the loop is written the way a real
// client iterates so the terminal-cursor contract is exercised.
func scanAll(t *testing.T, nc net.Conn, br *bufio.Reader, args ...string) map[string]bool {
	t.Helper()
	got := make(map[string]bool)
	cursor := "0"
	for {
		send(t, nc, append([]string{"SCAN", cursor}, args...)...)
		next, keys := scanReply(t, br)
		for k := range keys {
			got[k] = true
		}
		if next == "0" {
			return got
		}
		cursor = next
	}
}

// TestScanSpansEveryKeyspace checks SCAN walks every type f3 keeps, the string
// store and the five collection registries, and terminates with cursor 0 after
// one page. One key of each of the six types shows in a full SCAN, spread across
// both shards so the reply also exercises the gather concatenating each shard's
// partial under the cursor envelope.
func TestScanSpansEveryKeyspace(t *testing.T) {
	_, nc, br := startServer(t)

	// A fresh keyspace answers cursor 0 with an empty page.
	send(t, nc, "SCAN", "0")
	cursor, keys := scanReply(t, br)
	if cursor != "0" || len(keys) != 0 {
		t.Fatalf("SCAN 0 on a fresh server = (%q, %v), want (0, empty)", cursor, keys)
	}

	send(t, nc, "SET", "str:a", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SADD", "set:a", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "ZADD", "zset:a", "1", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "HSET", "hash:a", "f", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "RPUSH", "list:a", "e")
	expect(t, br, ":1\r\n")
	send(t, nc, "XADD", "stream:a", "*", "f", "v")
	if id := readBulk(t, br); len(id) == 0 {
		t.Fatal("XADD returned no id")
	}

	want := map[string]bool{
		"str:a": true, "set:a": true, "zset:a": true,
		"hash:a": true, "list:a": true, "stream:a": true,
	}
	got := scanAll(t, nc, br)
	for k := range want {
		if !got[k] {
			t.Fatalf("SCAN missed %q; got %v", k, got)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("SCAN = %v, want exactly %v", got, want)
	}
}

// TestScanMatchFilters checks the MATCH glob narrows the page, across keys of
// mixed types, and skips the non-matching keys.
func TestScanMatchFilters(t *testing.T) {
	_, nc, br := startServer(t)

	for _, k := range []string{"user:1", "user:2", "user:30"} {
		send(t, nc, "SET", k, "v")
		expect(t, br, "+OK\r\n")
	}
	send(t, nc, "SADD", "admin:1", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "RPUSH", "user:list", "e")
	expect(t, br, ":1\r\n")

	got := scanAll(t, nc, br, "MATCH", "user:*")
	for _, k := range []string{"user:1", "user:2", "user:30", "user:list"} {
		if !got[k] {
			t.Fatalf("SCAN MATCH user:* missed %q; got %v", k, got)
		}
	}
	if got["admin:1"] {
		t.Fatalf("SCAN MATCH user:* leaked admin:1; got %v", got)
	}
}

// TestScanTypeFilters checks the TYPE option restricts the walk to one type: a
// SCAN TYPE list sees only the list key though keys of other types share the
// keyspace, and a COUNT hint rides along without changing the answer.
func TestScanTypeFilters(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k:str", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "RPUSH", "k:list", "e")
	expect(t, br, ":1\r\n")
	send(t, nc, "SADD", "k:set", "m")
	expect(t, br, ":1\r\n")

	got := scanAll(t, nc, br, "TYPE", "list", "COUNT", "100")
	if !got["k:list"] || got["k:str"] || got["k:set"] {
		t.Fatalf("SCAN TYPE list = %v, want only k:list", got)
	}

	// An unrecognized type matches nothing rather than erroring.
	got = scanAll(t, nc, br, "TYPE", "nosuchtype")
	if len(got) != 0 {
		t.Fatalf("SCAN TYPE nosuchtype = %v, want empty", got)
	}
}

// TestScanReflectsDeletion checks the walk tracks the live keyspace: a deleted
// key stops showing, and after FLUSHALL the walk answers an empty page.
func TestScanReflectsDeletion(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MSET", "k:1", "a", "k:2", "b", "k:3", "c")
	expect(t, br, "+OK\r\n")
	send(t, nc, "DEL", "k:2")
	expect(t, br, ":1\r\n")

	got := scanAll(t, nc, br, "MATCH", "k:*")
	if !got["k:1"] || !got["k:3"] || got["k:2"] {
		t.Fatalf("SCAN after delete = %v, want k:1 and k:3", got)
	}

	send(t, nc, "FLUSHALL")
	expect(t, br, "+OK\r\n")
	if got := scanAll(t, nc, br); len(got) != 0 {
		t.Fatalf("SCAN after FLUSHALL = %v, want empty", got)
	}
}

// TestScanBadCursor checks a non-numeric cursor is one invalid-cursor error, not
// one error per shard, and that a malformed option and a non-positive COUNT are
// the syntax error Redis reports.
func TestScanBadCursor(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SCAN", "notanumber")
	expect(t, br, "-ERR invalid cursor\r\n")

	send(t, nc, "SCAN", "0", "MATCH")
	expect(t, br, "-ERR syntax error\r\n")

	send(t, nc, "SCAN", "0", "COUNT", "0")
	expect(t, br, "-ERR syntax error\r\n")

	send(t, nc, "SCAN", "0", "BOGUS", "x")
	expect(t, br, "-ERR syntax error\r\n")
}
