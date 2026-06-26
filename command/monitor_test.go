package command

import (
	"strings"
	"testing"
	"time"
)

// TestMonitorConfirm checks MONITOR replies +OK and puts the connection in
// monitor mode.
func TestMonitorConfirm(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "MONITOR"); got != "+OK" {
		t.Fatalf("MONITOR = %q want +OK", got)
	}
}

// TestMonitorFeed checks that a command run on one connection is streamed to a
// monitoring connection as a status line carrying the db, address, and quoted
// arguments.
func TestMonitorFeed(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if got := sendLine(t, r1, c1, "MONITOR"); got != "+OK" {
		t.Fatalf("MONITOR = %q want +OK", got)
	}
	if got := sendLine(t, r2, c2, "SET foo bar"); got != "+OK" {
		t.Fatalf("SET = %q want +OK", got)
	}
	line := sendLineRead(t, r1)
	// The feed line is a simple string the reader returns without its leading '+'.
	if !strings.HasSuffix(line, `"SET" "foo" "bar"`) {
		t.Fatalf("monitor line = %q want it to end with the quoted command", line)
	}
	if !strings.Contains(line, "[0 ") {
		t.Fatalf("monitor line = %q want it to name db 0", line)
	}
}

// TestMonitorSkipsAdmin checks an admin command (CONFIG GET) and AUTH are not
// streamed: the next line a monitor sees after them is an ordinary command, not
// the admin one.
func TestMonitorSkipsAdmin(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if got := sendLine(t, r1, c1, "MONITOR"); got != "+OK" {
		t.Fatalf("MONITOR = %q want +OK", got)
	}
	// CONFIG GET is admin, so it must not appear. GET is ordinary and must.
	// CONFIG GET returns a multi-bulk reply, so drain the whole thing.
	if _, err := c2.Write([]byte("CONFIG GET maxmemory\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r2, c2, "GET k"); got != "$-1" {
		t.Fatalf("GET = %q want $-1", got)
	}
	line := sendLineRead(t, r1)
	if strings.Contains(line, "CONFIG") {
		t.Fatalf("monitor line = %q must not contain the admin CONFIG command", line)
	}
	if !strings.HasSuffix(line, `"GET" "k"`) {
		t.Fatalf("monitor line = %q want the GET command", line)
	}
}

// TestFormatMonitorLine pins the exact wire shape of one feed line, including the
// zero-padded microseconds, the bracketed db and address, and the binary-safe
// argument escaping.
func TestFormatMonitorLine(t *testing.T) {
	ts := time.Unix(1700000000, 123456000)
	got := string(formatMonitorLine(ts, 3, "127.0.0.1:6390", [][]byte{
		[]byte("SET"), []byte("k"), []byte("a\nb"),
	}))
	want := "+1700000000.123456 [3 127.0.0.1:6390] \"SET\" \"k\" \"a\\nb\"\r\n"
	if got != want {
		t.Fatalf("formatMonitorLine =\n%q\nwant\n%q", got, want)
	}
}
