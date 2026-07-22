package drivers

import (
	"testing"
	"time"
)

// TestShutdownExits checks a plain SHUTDOWN and SHUTDOWN NOSAVE both reach the
// exit hook with code 0 and send no reply, the redis contract (a successful
// shutdown answers with silence as the process goes down). The hook is stubbed
// so the exit path runs without ending the test binary.
func TestShutdownExits(t *testing.T) {
	for _, form := range [][]string{{"SHUTDOWN"}, {"SHUTDOWN", "NOSAVE"}, {"SHUTDOWN", "SAVE"}} {
		srv, nc, _ := startServer(t)
		exited := make(chan int, 1)
		srv.exitFn = func(code int) { exited <- code }

		writeCmd(t, nc, form...)
		select {
		case code := <-exited:
			if code != 0 {
				t.Fatalf("%v exit code = %d, want 0", form, code)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%v did not reach the exit hook", form)
		}
	}
}

// TestShutdownAbort checks SHUTDOWN ABORT is the honest no-op: it answers the
// no-shutdown-in-progress error and never calls the exit hook, since f3 has no
// pending shutdown to cancel.
func TestShutdownAbort(t *testing.T) {
	srv, nc, br := startServer(t)
	srv.exitFn = func(int) { t.Fatalf("SHUTDOWN ABORT must not exit") }

	if _, ok := sendCmd(t, br, nc, "SHUTDOWN", "ABORT").(errorReply); !ok {
		t.Fatalf("SHUTDOWN ABORT did not error")
	}
	// The connection is still usable: the abort did not tear anything down.
	if got := sendCmd(t, br, nc, "PING"); got != "PONG" {
		t.Fatalf("PING after SHUTDOWN ABORT = %v, want PONG", got)
	}
}

// TestShutdownBadArg checks an unknown modifier and ABORT combined with an exit
// flag are both the syntax error, and neither exits.
func TestShutdownBadArg(t *testing.T) {
	srv, nc, br := startServer(t)
	srv.exitFn = func(int) { t.Fatalf("a rejected SHUTDOWN must not exit") }

	if _, ok := sendCmd(t, br, nc, "SHUTDOWN", "MAYBE").(errorReply); !ok {
		t.Fatalf("SHUTDOWN MAYBE did not error")
	}
	if _, ok := sendCmd(t, br, nc, "SHUTDOWN", "ABORT", "NOW").(errorReply); !ok {
		t.Fatalf("SHUTDOWN ABORT NOW did not error")
	}
}
