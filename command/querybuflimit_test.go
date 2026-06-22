package command

import (
	"strings"
	"testing"
	"time"
)

// TestClientQueryBufLimit checks that CONFIG SET client-query-buffer-limit makes
// the server close a connection whose buffered, not yet parseable input grows past
// the cap, while normal traffic under the cap is unaffected.
func TestClientQueryBufLimit(t *testing.T) {
	r, c := startData(t)

	// Drop the cap to a small value. The command itself is well under it.
	if got := sendLine(t, r, c, "CONFIG SET client-query-buffer-limit 100"); got != "+OK" {
		t.Fatalf("CONFIG SET client-query-buffer-limit = %q", got)
	}

	// A normal command stays under the cap and still works.
	if got := sendLine(t, r, c, "PING"); got != "+PONG" {
		t.Fatalf("PING under cap = %q want +PONG", got)
	}

	// Send a multibulk that declares a 150 byte argument but never terminates it.
	// The parser cannot complete the command, so the bytes pile up in the query
	// buffer past the 100 byte cap and the server closes the connection.
	req := "*1\r\n$150\r\n" + strings.Repeat("x", 150)
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatalf("write oversized request: %v", err)
	}

	// The next read sees the connection closed with no reply.
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := r.ReadString('\n'); err == nil {
		t.Fatal("expected the connection to close, got a reply")
	}
}
