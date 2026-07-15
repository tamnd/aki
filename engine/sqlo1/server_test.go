package sqlo1

import (
	"bufio"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// startServer runs a Server over MemStore on a loopback listener and
// returns a connected client plus a reader over its replies.
func startServer(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go NewServer(NewMemStore()).Serve(l)

	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(10 * time.Second))
	return c, bufio.NewReader(c)
}

func expect(t *testing.T, r *bufio.Reader, want string) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatalf("reading reply (want %q): %v", want, err)
	}
	if string(got) != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
}

func TestServerCommandSurface(t *testing.T) {
	c, r := startServer(t)

	send := func(s string) {
		t.Helper()
		if _, err := c.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}

	send("*1\r\n$4\r\nPING\r\n")
	expect(t, r, "+PONG\r\n")

	send("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
	expect(t, r, "$5\r\nhello\r\n")

	send("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nval\r\n")
	expect(t, r, "+OK\r\n")

	send("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	expect(t, r, "$3\r\nval\r\n")

	send("*2\r\n$3\r\nGET\r\n$4\r\nnope\r\n")
	expect(t, r, "$-1\r\n")

	send("*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$3\r\n100\r\n")
	expect(t, r, ":1\r\n")

	send("*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n")
	expect(t, r, ":100\r\n")

	send("*2\r\n$3\r\nTTL\r\n$4\r\nnope\r\n")
	expect(t, r, ":-2\r\n")

	send("*3\r\n$3\r\nDEL\r\n$1\r\nk\r\n$4\r\nnope\r\n")
	expect(t, r, ":1\r\n")

	send("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	expect(t, r, "$-1\r\n")

	// Unknown command and wrong arity reply with errors, connection stays up.
	send("*1\r\n$5\r\nHELLO\r\n")
	expect(t, r, "-ERR unknown command 'HELLO'\r\n")
	send("*1\r\n$3\r\nGET\r\n")
	expect(t, r, "-ERR wrong number of arguments for 'get' command\r\n")
	send("*1\r\n$4\r\nPING\r\n")
	expect(t, r, "+PONG\r\n")
}

func TestServerPipelining(t *testing.T) {
	c, r := startServer(t)

	// One write, three commands; replies must come back in order.
	burst := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n" +
		"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n" +
		"*2\r\n$3\r\nGET\r\n$1\r\na\r\n"
	if _, err := c.Write([]byte(burst)); err != nil {
		t.Fatal(err)
	}
	expect(t, r, "+OK\r\n+OK\r\n$1\r\n1\r\n")
}

func TestServerInlineCommands(t *testing.T) {
	c, r := startServer(t)
	if _, err := c.Write([]byte("PING\r\nSET k v\r\nGET k\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, r, "+PONG\r\n+OK\r\n$1\r\nv\r\n")
}

func TestServerProtocolErrorCloses(t *testing.T) {
	c, r := startServer(t)
	if _, err := c.Write([]byte("*1\r\n:bad\r\n")); err != nil {
		t.Fatal(err)
	}
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(line, "-ERR Protocol error") {
		t.Fatalf("reply = %q, want a protocol error", line)
	}
	if _, err := r.ReadByte(); err != io.EOF {
		t.Fatalf("connection still open after protocol error, err = %v", err)
	}
}

// TestServerExpirySemantics drives dispatch directly with a fake clock, so
// lazy expiry is tested without sleeping.
func TestServerExpirySemantics(t *testing.T) {
	s := NewServer(NewMemStore())
	clock := int64(1_000_000)
	s.now = func() int64 { return clock }

	do := func(args ...string) string {
		bs := make([][]byte, len(args))
		for i, a := range args {
			bs[i] = []byte(a)
		}
		return string(s.dispatch(nil, bs))
	}

	if got := do("SET", "k", "v"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := do("TTL", "k"); got != ":-1\r\n" {
		t.Fatalf("TTL with no expiry = %q, want -1", got)
	}
	if got := do("EXPIRE", "k", "10"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("TTL", "k"); got != ":10\r\n" {
		t.Fatal(got)
	}

	clock += 9_500
	if got := do("TTL", "k"); got != ":1\r\n" {
		t.Fatalf("TTL rounds up, got %q", got)
	}

	clock += 501 // past the deadline
	if got := do("GET", "k"); got != "$-1\r\n" {
		t.Fatalf("expired key still readable: %q", got)
	}
	if got := do("TTL", "k"); got != ":-2\r\n" {
		t.Fatalf("TTL after lapse = %q, want -2", got)
	}
	if got := do("EXPIRE", "k", "10"); got != ":0\r\n" {
		t.Fatalf("EXPIRE on lapsed key = %q, want 0", got)
	}

	// EXPIRE with a non-positive ttl deletes, like Redis.
	do("SET", "k2", "v")
	if got := do("EXPIRE", "k2", "0"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("GET", "k2"); got != "$-1\r\n" {
		t.Fatalf("key survived EXPIRE 0: %q", got)
	}
}
