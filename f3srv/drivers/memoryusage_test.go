package drivers

import (
	"bufio"
	"strconv"
	"strings"
	"testing"
)

// TestMemoryUsageEveryType drives MEMORY USAGE over one key of each type through
// the real dispatch verb. The reply is an approximate resident-byte integer for a
// present key and a null for a missing one, matching Redis. The figures are the
// per-type footprints the demote loop already weighs, so the test asserts shape
// (a positive integer, and a larger value for a larger collection) rather than an
// exact byte count, which is implementation-defined and allowed to drift.
func TestMemoryUsageEveryType(t *testing.T) {
	_, nc, br := startServer(t)

	// A missing key answers null, not an error.
	send(t, nc, "MEMORY", "USAGE", "nope")
	expect(t, br, "$-1\r\n")

	// A string key reports a positive figure.
	send(t, nc, "SET", "s", "hello")
	expect(t, br, "+OK\r\n")
	send(t, nc, "MEMORY", "USAGE", "s")
	assertPositiveInt(t, br, "string")

	// A longer string charges more than a short one.
	send(t, nc, "SET", "s:short", "x")
	expect(t, br, "+OK\r\n")
	send(t, nc, "MEMORY", "USAGE", "s:short")
	shortBytes := readIntReply(t, br, "string short")
	send(t, nc, "SET", "s:long", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	expect(t, br, "+OK\r\n")
	send(t, nc, "MEMORY", "USAGE", "s:long")
	longBytes := readIntReply(t, br, "string long")
	if longBytes <= shortBytes {
		t.Fatalf("MEMORY USAGE long=%d not greater than short=%d", longBytes, shortBytes)
	}

	// SAMPLES is accepted and does not change the answer.
	send(t, nc, "MEMORY", "USAGE", "s", "SAMPLES", "0")
	assertPositiveInt(t, br, "string with SAMPLES")

	// Every collection type reports a positive figure.
	send(t, nc, "RPUSH", "l", "a", "b", "c")
	expect(t, br, ":3\r\n")
	send(t, nc, "MEMORY", "USAGE", "l")
	assertPositiveInt(t, br, "list")

	send(t, nc, "SADD", "set:int", "1", "2", "3")
	expect(t, br, ":3\r\n")
	send(t, nc, "MEMORY", "USAGE", "set:int")
	assertPositiveInt(t, br, "set")

	send(t, nc, "HSET", "h", "f", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "MEMORY", "USAGE", "h")
	assertPositiveInt(t, br, "hash")

	send(t, nc, "ZADD", "z", "1", "a", "2", "b")
	expect(t, br, ":2\r\n")
	send(t, nc, "MEMORY", "USAGE", "z")
	assertPositiveInt(t, br, "zset")

	send(t, nc, "XADD", "st", "*", "k", "v")
	readBulk(t, br) // the generated entry ID
	send(t, nc, "MEMORY", "USAGE", "st")
	assertPositiveInt(t, br, "stream")

	// A larger set charges more than a small one.
	send(t, nc, "MEMORY", "USAGE", "set:int")
	smallSet := readIntReply(t, br, "small set")
	for i := 0; i < 300; i++ {
		send(t, nc, "SADD", "set:big", "m"+strconv.Itoa(i))
		expect(t, br, ":1\r\n")
	}
	send(t, nc, "MEMORY", "USAGE", "set:big")
	bigSet := readIntReply(t, br, "big set")
	if bigSet <= smallSet {
		t.Fatalf("MEMORY USAGE big set=%d not greater than small set=%d", bigSet, smallSet)
	}

	// An unknown subcommand is an error.
	send(t, nc, "MEMORY", "NONSENSE")
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read unknown-subcommand reply: %v", err)
	}
	if !strings.HasPrefix(line, "-ERR Unknown MEMORY subcommand") {
		t.Fatalf("MEMORY NONSENSE reply = %q, want -ERR Unknown MEMORY subcommand prefix", line)
	}
}

// assertPositiveInt reads one RESP integer reply and fails unless it is positive.
func assertPositiveInt(t *testing.T, br *bufio.Reader, what string) int {
	t.Helper()
	n := readIntReply(t, br, what)
	if n <= 0 {
		t.Fatalf("%s: MEMORY USAGE = %d, want positive", what, n)
	}
	return n
}

// readIntReply reads a single RESP integer reply (":<n>\r\n") and returns it.
func readIntReply(t *testing.T, br *bufio.Reader, what string) int {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("%s: read reply: %v", what, err)
	}
	if len(line) < 4 || line[0] != ':' {
		t.Fatalf("%s: reply %q is not an integer", what, line)
	}
	n, err := strconv.Atoi(strings.TrimSuffix(line[1:], "\r\n"))
	if err != nil {
		t.Fatalf("%s: parse int %q: %v", what, line, err)
	}
	return n
}
