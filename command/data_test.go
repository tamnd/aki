package command

import (
	"bufio"
	"net"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// startData brings up a server backed by an in-memory keyspace so the data
// commands have somewhere to read and write. It returns the client reader and
// connection like start does.
func startData(t *testing.T) (*bufio.Reader, net.Conn) {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "data.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	return start(t, Config{Engine: NewEngine(ks)})
}

// bulk reads a RESP bulk string reply ($N\r\n<payload>\r\n) and returns the
// payload, or the sentinel "<nil>" for a null bulk.
func bulk(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) string {
	t.Helper()
	line := sendLine(t, r, c, cmd)
	if line == "$-1" || line == "_" {
		return "<nil>"
	}
	if line == "" || line[0] != '$' {
		t.Fatalf("expected bulk header after %q, got %q", cmd, line)
	}
	payload, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read bulk payload after %q: %v", cmd, err)
	}
	return payload[:len(payload)-2] // strip trailing \r\n
}

func TestSetGet(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "SET foo bar"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	if got := bulk(t, r, c, "GET foo"); got != "bar" {
		t.Fatalf("GET foo = %q want bar", got)
	}
}

func TestGetMissing(t *testing.T) {
	r, c := startData(t)
	if got := bulk(t, r, c, "GET nope"); got != "<nil>" {
		t.Fatalf("GET missing = %q want nil", got)
	}
}

func TestSetOverwrite(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v1")
	_ = sendLine(t, r, c, "SET k v2")
	if got := bulk(t, r, c, "GET k"); got != "v2" {
		t.Fatalf("GET k = %q want v2", got)
	}
}

func TestSetUnknownOptionIsSyntaxError(t *testing.T) {
	r, c := startData(t)
	// A real option word is accepted; an unknown one is a syntax error.
	if got := sendLine(t, r, c, "SET k v EX 10"); got != "+OK" {
		t.Fatalf("SET k v EX 10 = %q want +OK", got)
	}
	if got := sendLine(t, r, c, "SET k v BOGUS"); got != "-ERR syntax error" {
		t.Fatalf("SET k v BOGUS = %q want syntax error", got)
	}
}

func TestDel(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a 1")
	_ = sendLine(t, r, c, "SET b 2")
	if got := sendLine(t, r, c, "DEL a b c"); got != ":2" {
		t.Fatalf("DEL a b c = %q want :2", got)
	}
	if got := bulk(t, r, c, "GET a"); got != "<nil>" {
		t.Fatalf("GET a after DEL = %q", got)
	}
}

func TestExists(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a 1")
	// A repeated key counts each time, matching Redis.
	if got := sendLine(t, r, c, "EXISTS a a missing"); got != ":2" {
		t.Fatalf("EXISTS a a missing = %q want :2", got)
	}
}

func TestType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s hello")
	if got := sendLine(t, r, c, "TYPE s"); got != "+string" {
		t.Fatalf("TYPE s = %q want +string", got)
	}
	if got := sendLine(t, r, c, "TYPE missing"); got != "+none" {
		t.Fatalf("TYPE missing = %q want +none", got)
	}
}

func TestDbsizeAndSelect(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET a 1")
	_ = sendLine(t, r, c, "SET b 2")
	if got := sendLine(t, r, c, "DBSIZE"); got != ":2" {
		t.Fatalf("DBSIZE = %q want :2", got)
	}
	// A different DB is independent and starts empty.
	if got := sendLine(t, r, c, "SELECT 1"); got != "+OK" {
		t.Fatalf("SELECT 1 = %q", got)
	}
	if got := sendLine(t, r, c, "DBSIZE"); got != ":0" {
		t.Fatalf("DBSIZE on db1 = %q want :0", got)
	}
	if got := bulk(t, r, c, "GET a"); got != "<nil>" {
		t.Fatalf("GET a on db1 = %q want nil", got)
	}
}

func TestNoEngineRepliesError(t *testing.T) {
	// A connection-only server (no engine) rejects data commands instead of
	// panicking.
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "SET k v"); got != "-ERR this server has no keyspace" {
		t.Fatalf("SET without engine = %q", got)
	}
}
