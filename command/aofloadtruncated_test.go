package command

import (
	"strings"
	"testing"

	"github.com/tamnd/aki/networking"
)

// replayCtx builds the offline replay context loadAOF uses, so a test can drive
// replayAOF directly with a crafted buffer.
func replayCtx(d *Dispatcher) *Ctx {
	conn := networking.NewOfflineConn()
	sess := &session{authenticated: true}
	conn.SetSession(sess)
	return &Ctx{Conn: conn, d: d, sess: sess}
}

// A complete SET followed by a half written second command, the shape a crash
// leaves when it interrupts a write partway through.
const truncatedAOF = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n" + "*3\r\n$3\r\nSET\r\n$1\r\nx"

// TestAOFLoadTruncatedTolerated checks the default policy loads the commands
// before a truncated tail and stops without an error.
func TestAOFLoadTruncatedTolerated(t *testing.T) {
	d := newMetricsDispatcher(t)

	if err := d.replayAOF(replayCtx(d), []byte(truncatedAOF)); err != nil {
		t.Fatalf("replayAOF with default aof-load-truncated: %v", err)
	}
	if got := runReply(d, "GET", "k"); got != "$1\r\nv\r\n" {
		t.Fatalf("GET k after load = %q want the value from the complete command", got)
	}
}

// TestAOFLoadTruncatedStrict checks that with aof-load-truncated off a truncated
// tail aborts the load.
func TestAOFLoadTruncatedStrict(t *testing.T) {
	d := newMetricsDispatcher(t)
	if err := d.SetConfig("aof-load-truncated", "no"); err != nil {
		t.Fatalf("set aof-load-truncated: %v", err)
	}

	err := d.replayAOF(replayCtx(d), []byte(truncatedAOF))
	if err == nil {
		t.Fatal("replayAOF tolerated a truncated tail with aof-load-truncated off")
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("replayAOF error = %v want a truncation error", err)
	}
}

// TestAOFLoadCorruptionAlwaysAborts checks that a real protocol error in the
// middle aborts the load regardless of the policy, since it is not a truncated
// tail.
func TestAOFLoadCorruptionAlwaysAborts(t *testing.T) {
	d := newMetricsDispatcher(t)
	// A bulk length that is not a number is a protocol error, not a short read.
	bad := "*1\r\n$x\r\n"

	if err := d.replayAOF(replayCtx(d), []byte(bad)); err == nil {
		t.Fatal("replayAOF accepted a corrupt command under the default policy")
	}
}
