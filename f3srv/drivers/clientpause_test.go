package drivers

import (
	"net"
	"testing"
	"time"
)

// heldByPause asserts a command already written to nc has not answered: it sets
// a short read deadline and expects the read to time out, proving the command is
// held at the pause gate rather than merely slow. The deadline is cleared on
// return so the connection stays usable for the reply once the pause lifts.
func heldByPause(t *testing.T, nc net.Conn, br interface{ ReadByte() (byte, error) }) {
	t.Helper()
	if err := nc.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	defer func() { _ = nc.SetReadDeadline(time.Time{}) }()
	if b, err := br.ReadByte(); err == nil {
		t.Fatalf("command answered during pause, got a byte %q", string([]byte{b}))
	}
}

// TestClientPauseAll checks CLIENT PAUSE (ALL, the default mode) holds a write
// bound for a shard until CLIENT UNPAUSE lifts it from another connection, and
// that the held command then runs and answers in order.
func TestClientPauseAll(t *testing.T) {
	srv, nc, br := startServer(t)
	nc2, br2 := dial(t, srv)

	if got := sendCmd(t, br, nc, "CLIENT", "PAUSE", "10000"); got != "OK" {
		t.Fatalf("CLIENT PAUSE 10000 = %v, want OK", got)
	}
	// A write must be held while the ALL pause is in force.
	writeCmd(t, nc, "SET", "k", "1")
	heldByPause(t, nc, br)

	// Lift from a second connection: CLIENT UNPAUSE is answered ahead of the
	// gate, so it flows even though the first connection is paused.
	if got := sendCmd(t, br2, nc2, "CLIENT", "UNPAUSE"); got != "OK" {
		t.Fatalf("CLIENT UNPAUSE = %v, want OK", got)
	}
	if got := readRESP(t, br); got != "OK" {
		t.Fatalf("held SET after UNPAUSE = %v, want OK", got)
	}
	// The write really ran.
	if got := sendCmd(t, br, nc, "GET", "k"); got != "1" {
		t.Fatalf("GET k = %v, want 1", got)
	}
}

// TestClientPauseWrite checks CLIENT PAUSE WRITE holds writes but lets reads
// flow: a GET answers immediately while a SET is held until UNPAUSE.
func TestClientPauseWrite(t *testing.T) {
	srv, nc, br := startServer(t)
	nc2, br2 := dial(t, srv)

	if got := sendCmd(t, br, nc, "SET", "k", "1"); got != "OK" {
		t.Fatalf("seed SET = %v, want OK", got)
	}
	if got := sendCmd(t, br, nc, "CLIENT", "PAUSE", "10000", "WRITE"); got != "OK" {
		t.Fatalf("CLIENT PAUSE WRITE = %v, want OK", got)
	}
	// A read flows under a WRITE-mode pause.
	if got := sendCmd(t, br, nc, "GET", "k"); got != "1" {
		t.Fatalf("GET during WRITE pause = %v, want 1 (reads must flow)", got)
	}
	// A write is held.
	writeCmd(t, nc, "SET", "k", "2")
	heldByPause(t, nc, br)

	if got := sendCmd(t, br2, nc2, "CLIENT", "UNPAUSE"); got != "OK" {
		t.Fatalf("CLIENT UNPAUSE = %v, want OK", got)
	}
	if got := readRESP(t, br); got != "OK" {
		t.Fatalf("held SET after UNPAUSE = %v, want OK", got)
	}
	if got := sendCmd(t, br, nc, "GET", "k"); got != "2" {
		t.Fatalf("GET k = %v, want 2 (held SET must take effect)", got)
	}
}

// TestClientPauseExpires checks a pause lifts on its own when its timeout passes,
// with no UNPAUSE: a SET issued during the pause answers only after the deadline.
func TestClientPauseExpires(t *testing.T) {
	_, nc, br := startServer(t)

	if got := sendCmd(t, br, nc, "CLIENT", "PAUSE", "300"); got != "OK" {
		t.Fatalf("CLIENT PAUSE 300 = %v, want OK", got)
	}
	writeCmd(t, nc, "SET", "k", "1")
	start := time.Now()
	if got := readRESP(t, br); got != "OK" {
		t.Fatalf("held SET = %v, want OK", got)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("SET answered after %v, want it held ~300ms by the pause", elapsed)
	}
}

// TestClientPauseBadArg checks the argument errors: a missing timeout is the
// arity error, a non-integer or negative timeout is the timeout error, and an
// unknown mode token is the syntax error.
func TestClientPauseBadArg(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "CLIENT", "PAUSE").(errorReply); !ok {
		t.Fatalf("CLIENT PAUSE with no timeout did not error")
	}
	if _, ok := sendCmd(t, br, nc, "CLIENT", "PAUSE", "abc").(errorReply); !ok {
		t.Fatalf("CLIENT PAUSE abc did not error")
	}
	if _, ok := sendCmd(t, br, nc, "CLIENT", "PAUSE", "-5").(errorReply); !ok {
		t.Fatalf("CLIENT PAUSE -5 did not error")
	}
	if _, ok := sendCmd(t, br, nc, "CLIENT", "PAUSE", "100", "MAYBE").(errorReply); !ok {
		t.Fatalf("CLIENT PAUSE 100 MAYBE did not error")
	}
}

// TestClientUnpauseNoPause checks CLIENT UNPAUSE answers +OK even when no pause
// is in force, and that a tail is the arity error.
func TestClientUnpauseNoPause(t *testing.T) {
	_, nc, br := startServer(t)
	if got := sendCmd(t, br, nc, "CLIENT", "UNPAUSE"); got != "OK" {
		t.Fatalf("CLIENT UNPAUSE (no pause) = %v, want OK", got)
	}
	if _, ok := sendCmd(t, br, nc, "CLIENT", "UNPAUSE", "x").(errorReply); !ok {
		t.Fatalf("CLIENT UNPAUSE x did not error")
	}
}
