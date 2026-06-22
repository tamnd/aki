package command

import (
	"strings"
	"testing"

	"github.com/tamnd/aki/networking"
)

// runReply drives one command on a fresh offline connection and returns the raw
// reply bytes so the test can read error strings and confirm there was no reply.
func runReply(d *Dispatcher, args ...string) string {
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	conn := networking.NewOfflineConn()
	d.Handle(conn, argv)
	return string(conn.OutBytes())
}

// TestShutdownAbort checks SHUTDOWN ABORT reports there is nothing to cancel, since
// aki shuts down in one pass and never holds a pending shutdown.
func TestShutdownAbort(t *testing.T) {
	d := newMetricsDispatcher(t)
	d.SetShutdown(func() {})

	out := runReply(d, "SHUTDOWN", "ABORT")
	if !strings.Contains(out, "No shutdown in progress") {
		t.Fatalf("SHUTDOWN ABORT: want no-shutdown error, got %q", out)
	}
}

// TestShutdownConflict checks NOSAVE and SAVE together is a syntax error.
func TestShutdownConflict(t *testing.T) {
	d := newMetricsDispatcher(t)
	d.SetShutdown(func() {})

	out := runReply(d, "SHUTDOWN", "NOSAVE", "SAVE")
	if !strings.Contains(out, "syntax error") {
		t.Fatalf("SHUTDOWN NOSAVE SAVE: want syntax error, got %q", out)
	}
}

// TestShutdownBadArg checks an unknown argument is a syntax error.
func TestShutdownBadArg(t *testing.T) {
	d := newMetricsDispatcher(t)
	d.SetShutdown(func() {})

	out := runReply(d, "SHUTDOWN", "LATER")
	if !strings.Contains(out, "syntax error") {
		t.Fatalf("SHUTDOWN LATER: want syntax error, got %q", out)
	}
}

// TestShutdownNoContext checks SHUTDOWN errors when no shutdown callback is wired,
// which is the offline and test case.
func TestShutdownNoContext(t *testing.T) {
	d := newMetricsDispatcher(t)

	out := runReply(d, "SHUTDOWN", "NOSAVE")
	if !strings.Contains(out, "not available in this context") {
		t.Fatalf("SHUTDOWN with no callback: want not-available error, got %q", out)
	}
}

// TestShutdownNoSave checks SHUTDOWN NOSAVE fires the callback and sends no reply,
// the same as Redis closing the connection before it can answer.
func TestShutdownNoSave(t *testing.T) {
	d := newMetricsDispatcher(t)
	fired := false
	d.SetShutdown(func() { fired = true })

	out := runReply(d, "SHUTDOWN", "NOSAVE")
	if out != "" {
		t.Fatalf("SHUTDOWN NOSAVE: want no reply, got %q", out)
	}
	if !fired {
		t.Fatalf("SHUTDOWN NOSAVE: callback was not fired")
	}
}

// TestShutdownDefaultNoSavePoints checks a plain SHUTDOWN with no save points
// configured skips the snapshot and fires the callback.
func TestShutdownDefaultNoSavePoints(t *testing.T) {
	d := newMetricsDispatcher(t)
	if err := d.SetConfig("save", ""); err != nil {
		t.Fatalf("clear save points: %v", err)
	}
	fired := false
	d.SetShutdown(func() { fired = true })

	out := runReply(d, "SHUTDOWN")
	if out != "" {
		t.Fatalf("SHUTDOWN: want no reply, got %q", out)
	}
	if !fired {
		t.Fatalf("SHUTDOWN: callback was not fired")
	}
}

// TestHasSavePoints checks the save-point reading that drives the default save
// policy. An empty value or the explicit "" form means none.
func TestHasSavePoints(t *testing.T) {
	d := newMetricsDispatcher(t)

	if err := d.SetConfig("save", ""); err != nil {
		t.Fatalf("set save empty: %v", err)
	}
	if d.hasSavePoints() {
		t.Fatalf("empty save should report no save points")
	}

	if err := d.SetConfig("save", "3600 1 300 100"); err != nil {
		t.Fatalf("set save points: %v", err)
	}
	if !d.hasSavePoints() {
		t.Fatalf("non-empty save should report save points")
	}
}
