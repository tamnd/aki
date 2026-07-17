package drivers

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
)

// infoBody sends INFO and returns the whole bulk body, so a test can read the
// Keyspace db0 line, which readInfo skips (its value keys=N,... is not a plain
// counter). It leaves the connection drained for a following command.
func infoBody(t *testing.T, nc net.Conn, br *bufio.Reader) string {
	t.Helper()
	send(t, nc, "INFO")
	hdr, err := br.ReadString('\n')
	if err != nil || len(hdr) < 4 || hdr[0] != '$' {
		t.Fatalf("info header %q: %v", hdr, err)
	}
	n, err := strconv.Atoi(strings.TrimSuffix(hdr[1:], "\r\n"))
	if err != nil {
		t.Fatalf("info header %q: %v", hdr, err)
	}
	body := make([]byte, n+2)
	if _, err := io.ReadFull(br, body); err != nil {
		t.Fatal(err)
	}
	return string(body[:n])
}

// db0Line returns the db0 keyspace line of an INFO body, or "" when none shows.
func db0Line(body string) string {
	for _, line := range strings.Split(body, "\r\n") {
		if strings.HasPrefix(line, "db0:") {
			return line
		}
	}
	return ""
}

// TestInfoKeyspaceEmpty checks a fresh server renders the Keyspace section
// header with no db line, the way redis reports an empty keyspace. The header
// must be present (a redis client greps for "# Keyspace") but no db0 line shows
// until a key lands.
func TestInfoKeyspaceEmpty(t *testing.T) {
	_, nc, br := startServer(t)

	body := infoBody(t, nc, br)
	if !strings.Contains(body, "# Keyspace") {
		t.Fatalf("INFO missing the Keyspace section header:\n%s", body)
	}
	if line := db0Line(body); line != "" {
		t.Fatalf("fresh server shows a db line %q, want none", line)
	}
}

// TestInfoKeyspaceCounts checks the db0 line folds keys and expires across every
// keyspace: keys is the whole-keyspace live count, expires the count carrying a
// key-level TTL. One key of each of the six types is placed, three of them given
// a TTL, so the line must read keys=6,expires=3. A PERSIST then drops expires
// back without touching keys, and a DEL drops both.
func TestInfoKeyspaceCounts(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "s", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SADD", "st", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "ZADD", "zs", "1", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "HSET", "h", "f", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "RPUSH", "l", "e")
	expect(t, br, ":1\r\n")
	send(t, nc, "XADD", "str", "*", "f", "v")
	if id := readBulk(t, br); len(id) == 0 {
		t.Fatalf("XADD id = %q, want an entry id", id)
	}

	// No TTL anywhere yet: six keys, zero expires.
	if got := db0Line(infoBody(t, nc, br)); got != "db0:keys=6,expires=0,avg_ttl=0,subexpiry=0" {
		t.Fatalf("db0 line = %q, want keys=6,expires=0", got)
	}

	// Give a TTL to the string, the set, and the stream (three of the six).
	for _, k := range []string{"s", "st", "str"} {
		send(t, nc, "EXPIRE", k, "1000")
		expect(t, br, ":1\r\n")
	}
	if got := db0Line(infoBody(t, nc, br)); got != "db0:keys=6,expires=3,avg_ttl=0,subexpiry=0" {
		t.Fatalf("db0 line after 3 EXPIRE = %q, want keys=6,expires=3", got)
	}

	// PERSIST on the set clears one deadline: keys hold, expires drops to 2.
	send(t, nc, "PERSIST", "st")
	expect(t, br, ":1\r\n")
	if got := db0Line(infoBody(t, nc, br)); got != "db0:keys=6,expires=2,avg_ttl=0,subexpiry=0" {
		t.Fatalf("db0 line after PERSIST st = %q, want keys=6,expires=2", got)
	}

	// Deleting the volatile string key drops both counts by one.
	send(t, nc, "DEL", "s")
	expect(t, br, ":1\r\n")
	if got := db0Line(infoBody(t, nc, br)); got != "db0:keys=5,expires=1,avg_ttl=0,subexpiry=0" {
		t.Fatalf("db0 line after DEL s = %q, want keys=5,expires=1", got)
	}
}
