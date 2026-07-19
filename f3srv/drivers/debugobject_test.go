package drivers

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

// DEBUG OBJECT key returns the internal description line (dispatch/debugobject.go),
// built from the OBJECT ENCODING chain, the DUMP serialize length, and the per-key
// idle clock. These drive the real server and parse the fields a harness reads:
// encoding, serializedlength, and lru_seconds_idle, plus the no-such-key error.

// debugObjectLine sends DEBUG OBJECT and returns the status-line payload.
func debugObjectLine(t *testing.T, nc net.Conn, br *bufio.Reader, key string) string {
	t.Helper()
	send(t, nc, "DEBUG", "OBJECT", key)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read DEBUG OBJECT line: %v", err)
	}
	if len(line) == 0 || line[0] != '+' {
		t.Fatalf("DEBUG OBJECT %q: want status line, got %q", key, line)
	}
	return strings.TrimRight(line[1:], "\r\n")
}

// field pulls the "name:value" token out of a DEBUG OBJECT line.
func field(t *testing.T, line, name string) string {
	t.Helper()
	for _, tok := range strings.Fields(line) {
		if strings.HasPrefix(tok, name+":") {
			return tok[len(name)+1:]
		}
	}
	t.Fatalf("field %q not in DEBUG OBJECT line %q", name, line)
	return ""
}

// TestDebugObjectEncoding checks the encoding field reports each type's live band,
// the same value OBJECT ENCODING gives, so DEBUG OBJECT and OBJECT ENCODING agree.
func TestDebugObjectEncoding(t *testing.T) {
	_, nc, br := startServer(t)

	cases := []struct {
		setup []string
		key   string
		enc   string
	}{
		{[]string{"SET", "i", "12345"}, "i", "int"},
		{[]string{"SET", "s", "hello"}, "s", "embstr"},
		{[]string{"SADD", "st", "1", "2", "3"}, "st", "intset"},
		{[]string{"RPUSH", "l", "a", "b"}, "l", "listpack"},
		{[]string{"HSET", "h", "f", "v"}, "h", "listpack"},
		{[]string{"ZADD", "z", "1", "m"}, "z", "listpack"},
		{[]string{"XADD", "x", "1-1", "k", "v"}, "x", "stream"},
	}
	for _, c := range cases {
		send(t, nc, c.setup...)
		// Drain the setup reply, whatever shape it is.
		readReply(t, br)
		line := debugObjectLine(t, nc, br, c.key)
		if got := field(t, line, "encoding"); got != c.enc {
			t.Errorf("DEBUG OBJECT %s encoding: got %q, want %q (line %q)", c.key, got, c.enc, line)
		}
		// serializedlength is present and positive for a live value.
		if got := field(t, line, "serializedlength"); got == "0" || got == "" {
			t.Errorf("DEBUG OBJECT %s serializedlength: got %q, want > 0", c.key, got)
		}
		// A fresh key is idle zero seconds.
		if got := field(t, line, "lru_seconds_idle"); got != "0" {
			t.Errorf("DEBUG OBJECT %s lru_seconds_idle: got %q, want 0", c.key, got)
		}
	}
}

// TestDebugObjectMissing checks a key present in no keyspace is the no-such-key
// error, not a status line.
func TestDebugObjectMissing(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "DEBUG", "OBJECT", "nope")
	expect(t, br, "-ERR no such key\r\n")
}

// TestDebugObjectRefcount checks the shared-integer refcount parity: a small integer
// string reports the interned sentinel, a plain string reports one.
func TestDebugObjectRefcount(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "small", "100")
	expect(t, br, "+OK\r\n")
	line := debugObjectLine(t, nc, br, "small")
	if got := field(t, line, "refcount"); got != "2147483647" {
		t.Errorf("small-int refcount: got %q, want 2147483647 (line %q)", got, line)
	}

	send(t, nc, "SET", "big", "not-an-int")
	expect(t, br, "+OK\r\n")
	line = debugObjectLine(t, nc, br, "big")
	if got := field(t, line, "refcount"); got != "1" {
		t.Errorf("string refcount: got %q, want 1 (line %q)", got, line)
	}
}

// TestDebugObjectAcrossShards checks DEBUG OBJECT routes to the key's owner: a sweep
// of keys necessarily spans both shards, and every one must answer its own line
// rather than a no-such-key from the wrong shard.
func TestDebugObjectAcrossShards(t *testing.T) {
	_, nc, br := startServer(t)

	const n = 40
	for i := 0; i < n; i++ {
		key := "dk:" + itoa(i)
		send(t, nc, "SET", key, "v")
		expect(t, br, "+OK\r\n")
		line := debugObjectLine(t, nc, br, key)
		if got := field(t, line, "encoding"); got != "embstr" {
			t.Errorf("DEBUG OBJECT %s encoding: got %q, want embstr", key, got)
		}
	}
}
