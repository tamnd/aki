package drivers

import (
	"testing"
)

// TestWait checks WAIT answers 0 on a standalone node (no replicas to ack) and
// refuses a non-integer argument.
func TestWait(t *testing.T) {
	_, nc, br := startServer(t)

	if got := sendCmd(t, br, nc, "WAIT", "0", "0"); got != int64(0) {
		t.Fatalf("WAIT 0 0 = %v, want 0", got)
	}
	if got := sendCmd(t, br, nc, "WAIT", "3", "100"); got != int64(0) {
		t.Fatalf("WAIT 3 100 = %v, want 0", got)
	}
	if _, ok := sendCmd(t, br, nc, "WAIT", "notanint", "0").(errorReply); !ok {
		t.Fatalf("WAIT with non-integer replica count did not error")
	}
}

// TestFailover checks FAILOVER reports the missing-replicas case bare and the
// nothing-in-progress case for ABORT.
func TestFailover(t *testing.T) {
	_, nc, br := startServer(t)

	if _, ok := sendCmd(t, br, nc, "FAILOVER").(errorReply); !ok {
		t.Fatalf("FAILOVER did not error on a standalone node")
	}
	if _, ok := sendCmd(t, br, nc, "FAILOVER", "ABORT").(errorReply); !ok {
		t.Fatalf("FAILOVER ABORT did not error with no failover in progress")
	}
}

// TestLatency checks the LATENCY family: RESET is zero, HISTORY and LATEST are
// empty, DOCTOR is a non-empty message, and an unknown subcommand errors.
func TestLatency(t *testing.T) {
	_, nc, br := startServer(t)

	if got := sendCmd(t, br, nc, "LATENCY", "RESET"); got != int64(0) {
		t.Fatalf("LATENCY RESET = %v, want 0", got)
	}
	if got, ok := sendCmd(t, br, nc, "LATENCY", "HISTORY", "event").([]any); !ok || len(got) != 0 {
		t.Fatalf("LATENCY HISTORY = %v, want empty array", got)
	}
	if got, ok := sendCmd(t, br, nc, "LATENCY", "LATEST").([]any); !ok || len(got) != 0 {
		t.Fatalf("LATENCY LATEST = %v, want empty array", got)
	}
	if got, ok := sendCmd(t, br, nc, "LATENCY", "DOCTOR").(string); !ok || len(got) == 0 {
		t.Fatalf("LATENCY DOCTOR = %v, want non-empty bulk", got)
	}
	if _, ok := sendCmd(t, br, nc, "LATENCY", "NOPE").(errorReply); !ok {
		t.Fatalf("LATENCY NOPE did not error")
	}
}

// TestSlowlog checks the SLOWLOG family: GET is empty, LEN is zero, RESET acks,
// and an unknown subcommand errors.
func TestSlowlog(t *testing.T) {
	_, nc, br := startServer(t)

	if got, ok := sendCmd(t, br, nc, "SLOWLOG", "GET").([]any); !ok || len(got) != 0 {
		t.Fatalf("SLOWLOG GET = %v, want empty array", got)
	}
	if got, ok := sendCmd(t, br, nc, "SLOWLOG", "GET", "10").([]any); !ok || len(got) != 0 {
		t.Fatalf("SLOWLOG GET 10 = %v, want empty array", got)
	}
	if got := sendCmd(t, br, nc, "SLOWLOG", "LEN"); got != int64(0) {
		t.Fatalf("SLOWLOG LEN = %v, want 0", got)
	}
	if got := sendCmd(t, br, nc, "SLOWLOG", "RESET"); got != "OK" {
		t.Fatalf("SLOWLOG RESET = %v, want OK", got)
	}
	if _, ok := sendCmd(t, br, nc, "SLOWLOG", "NOPE").(errorReply); !ok {
		t.Fatalf("SLOWLOG NOPE did not error")
	}
}
