package command

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/networking"
)

// start brings up a real server with the command dispatcher behind it and
// returns a connected client reader plus the raw connection. The whole pipeline
// (socket, RESP parse, dispatch, reply encode) runs, so these are end-to-end.
func start(t *testing.T, cfg Config) (*bufio.Reader, net.Conn) {
	t.Helper()
	d := New(cfg)
	ncfg := networking.Config{Addr: "127.0.0.1:0"}
	srv := networking.New(ncfg, d)
	go func() { _ = srv.ListenAndServe(ncfg) }()
	t.Cleanup(func() { _ = srv.Close() })

	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind")
		}
		time.Sleep(time.Millisecond)
	}

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	return bufio.NewReader(conn), conn
}

// send writes an inline command and reads one reply line.
func sendLine(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) string {
	t.Helper()
	// Refresh the deadline per command so a test that issues thousands of round
	// trips (the HLL accuracy tests) is not bounded by the one-shot deadline set
	// at connection time, which the -race build would otherwise blow through.
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte(cmd + "\r\n")); err != nil {
		t.Fatal(err)
	}
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read after %q: %v", cmd, err)
	}
	return strings.TrimRight(line, "\r\n")
}

func TestPing(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "PING"); got != "+PONG" {
		t.Fatalf("PING = %q", got)
	}
	// PING with a message echoes it as a bulk string.
	if got := sendLine(t, r, c, "PING hello"); got != "$5" {
		t.Fatalf("PING msg header = %q", got)
	}
	if got := sendLine(t, r, c, ""); got != "hello" {
		t.Fatalf("PING msg body = %q", got)
	}
}

func TestPingTooManyArgs(t *testing.T) {
	r, c := start(t, Config{})
	got := sendLine(t, r, c, "PING a b")
	if !strings.HasPrefix(got, "-ERR wrong number of arguments") {
		t.Fatalf("PING a b = %q", got)
	}
}

func TestEcho(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "ECHO hi"); got != "$2" {
		t.Fatalf("ECHO header = %q", got)
	}
	if got := sendLine(t, r, c, ""); got != "hi" {
		t.Fatalf("ECHO body = %q", got)
	}
}

func TestUnknownCommand(t *testing.T) {
	r, c := start(t, Config{})
	got := sendLine(t, r, c, "NOSUCHCMD foo bar")
	if !strings.HasPrefix(got, "-ERR unknown command 'NOSUCHCMD'") {
		t.Fatalf("unknown = %q", got)
	}
	if !strings.Contains(got, "'foo', 'bar', ") {
		t.Fatalf("unknown args echo = %q", got)
	}
}

func TestSelectRange(t *testing.T) {
	r, c := start(t, Config{Databases: 16})
	if got := sendLine(t, r, c, "SELECT 0"); got != "+OK" {
		t.Fatalf("SELECT 0 = %q", got)
	}
	if got := sendLine(t, r, c, "SELECT 15"); got != "+OK" {
		t.Fatalf("SELECT 15 = %q", got)
	}
	if got := sendLine(t, r, c, "SELECT 16"); !strings.HasPrefix(got, "-ERR DB index is out of range") {
		t.Fatalf("SELECT 16 = %q", got)
	}
	if got := sendLine(t, r, c, "SELECT x"); !strings.HasPrefix(got, "-ERR value is not an integer") {
		t.Fatalf("SELECT x = %q", got)
	}
}

func TestArity(t *testing.T) {
	r, c := start(t, Config{})
	got := sendLine(t, r, c, "ECHO")
	if !strings.HasPrefix(got, "-ERR wrong number of arguments for 'echo'") {
		t.Fatalf("ECHO arity = %q", got)
	}
}

func TestCommandCount(t *testing.T) {
	r, c := start(t, Config{})
	got := sendLine(t, r, c, "COMMAND COUNT")
	if !strings.HasPrefix(got, ":") {
		t.Fatalf("COMMAND COUNT = %q", got)
	}
}

func TestUnknownSubcommand(t *testing.T) {
	r, c := start(t, Config{})
	got := sendLine(t, r, c, "COMMAND NOPE")
	if !strings.HasPrefix(got, "-ERR Unknown subcommand") {
		t.Fatalf("COMMAND NOPE = %q", got)
	}
}

func TestTime(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "TIME"); got != "*2" {
		t.Fatalf("TIME header = %q", got)
	}
	// Two bulk strings follow; read their headers and bodies.
	for i := range 2 {
		h := sendLineRead(t, r)
		if !strings.HasPrefix(h, "$") {
			t.Fatalf("TIME field %d header = %q", i, h)
		}
		_ = sendLineRead(t, r)
	}
}

func TestHelloDefaultProto(t *testing.T) {
	r, c := start(t, Config{})
	// HELLO with no version keeps the current protocol (2) and returns the map,
	// which on RESP2 is a flat 14-element array.
	if got := sendLine(t, r, c, "HELLO"); got != "*14" {
		t.Fatalf("HELLO header = %q", got)
	}
}

func TestHelloProto3(t *testing.T) {
	r, c := start(t, Config{})
	// HELLO 3 switches to RESP3, so the map header uses the % type.
	if got := sendLine(t, r, c, "HELLO 3"); got != "%7" {
		t.Fatalf("HELLO 3 header = %q", got)
	}
}

func TestHelloBadProto(t *testing.T) {
	r, c := start(t, Config{})
	got := sendLine(t, r, c, "HELLO 9")
	if !strings.HasPrefix(got, "-NOPROTO") {
		t.Fatalf("HELLO 9 = %q", got)
	}
}

func TestAuthNoPasswordSet(t *testing.T) {
	r, c := start(t, Config{})
	got := sendLine(t, r, c, "AUTH foo")
	if !strings.HasPrefix(got, "-ERR Client sent AUTH") {
		t.Fatalf("AUTH no pass = %q", got)
	}
}

func TestAuthFlow(t *testing.T) {
	r, c := start(t, Config{RequirePass: "secret"})
	// Before auth, a non-NoAuth command is refused.
	if got := sendLine(t, r, c, "ECHO hi"); !strings.HasPrefix(got, "-NOAUTH") {
		t.Fatalf("pre-auth ECHO = %q", got)
	}
	if got := sendLine(t, r, c, "AUTH wrong"); !strings.HasPrefix(got, "-WRONGPASS") {
		t.Fatalf("AUTH wrong = %q", got)
	}
	if got := sendLine(t, r, c, "AUTH secret"); got != "+OK" {
		t.Fatalf("AUTH secret = %q", got)
	}
	if got := sendLine(t, r, c, "ECHO hi"); got != "$2" {
		t.Fatalf("post-auth ECHO = %q", got)
	}
}

func TestReset(t *testing.T) {
	r, c := start(t, Config{})
	_ = sendLine(t, r, c, "SELECT 3")
	if got := sendLine(t, r, c, "RESET"); got != "+RESET" {
		t.Fatalf("RESET = %q", got)
	}
}

// sendLineRead reads a single already-pending reply line.
func sendLineRead(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read line: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}
