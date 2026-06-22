package command

import (
	"strings"
	"testing"
)

// TestClusterCrossSlot checks that a multi-key command across two slots is
// rejected with CROSSSLOT only when cluster mode is on, and that keys sharing a
// hash tag still pass.
func TestClusterCrossSlot(t *testing.T) {
	r, c := startData(t)

	// Standalone mode never enforces CROSSSLOT, so a cross-slot MGET works.
	if got := sendArgs(t, r, c, "MSET", "a", "1", "b", "2"); got != "OK" {
		t.Fatalf("MSET in standalone = %v", got)
	}

	sendArgs(t, r, c, "CONFIG", "SET", "cluster-enabled", "yes")

	// a and b hash to different slots, so MGET is rejected.
	got := sendArgs(t, r, c, "MGET", "a", "b")
	e, ok := got.(cmdErr)
	if !ok || !strings.HasPrefix(string(e), "CROSSSLOT") {
		t.Fatalf("cross-slot MGET = %v want CROSSSLOT", got)
	}

	// A single-key command is always fine (a was set to 1 above).
	if got := sendArgs(t, r, c, "GET", "a"); got != "1" {
		t.Fatalf("single-key GET = %v want 1", got)
	}

	// Keys sharing a hash tag land in one slot, so the multi-key command passes.
	if got := sendArgs(t, r, c, "MGET", "{u}:a", "{u}:b"); got == nil {
		t.Fatal("tagged MGET returned nil reply")
	} else if _, isErr := got.(cmdErr); isErr {
		t.Fatalf("tagged MGET errored: %v", got)
	}
}

// TestClusterTxnCrossSlot checks that a MULTI/EXEC spanning two slots is rejected
// at EXEC even when each queued command is single-key, that a command whose own
// keys span slots is rejected at queue time and aborts the EXEC, and that a
// hash-tagged transaction runs.
func TestClusterTxnCrossSlot(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "CONFIG", "SET", "cluster-enabled", "yes")

	// Two single-key writes to different slots queue fine, but EXEC rejects the
	// whole transaction with CROSSSLOT.
	if got := sendArgs(t, r, c, "MULTI"); got != "OK" {
		t.Fatalf("MULTI = %v", got)
	}
	if got := sendArgs(t, r, c, "SET", "a", "1"); got != "QUEUED" {
		t.Fatalf("queue SET a = %v", got)
	}
	if got := sendArgs(t, r, c, "SET", "b", "2"); got != "QUEUED" {
		t.Fatalf("queue SET b = %v", got)
	}
	got := sendArgs(t, r, c, "EXEC")
	e, ok := got.(cmdErr)
	if !ok || !strings.HasPrefix(string(e), "CROSSSLOT") {
		t.Fatalf("cross-slot EXEC = %v want CROSSSLOT", got)
	}

	// A command whose own keys span slots is rejected when queued and the EXEC
	// aborts with EXECABORT, since the bad command never entered the queue.
	if got := sendArgs(t, r, c, "MULTI"); got != "OK" {
		t.Fatalf("MULTI = %v", got)
	}
	got = sendArgs(t, r, c, "MSET", "a", "1", "b", "2")
	if e, ok := got.(cmdErr); !ok || !strings.HasPrefix(string(e), "CROSSSLOT") {
		t.Fatalf("queue cross-slot MSET = %v want CROSSSLOT", got)
	}
	got = sendArgs(t, r, c, "EXEC")
	if e, ok := got.(cmdErr); !ok || !strings.HasPrefix(string(e), "EXECABORT") {
		t.Fatalf("EXEC after queue error = %v want EXECABORT", got)
	}

	// A hash-tagged transaction keeps every key in one slot and runs.
	if got := sendArgs(t, r, c, "MULTI"); got != "OK" {
		t.Fatalf("MULTI = %v", got)
	}
	sendArgs(t, r, c, "SET", "{u}:a", "1")
	sendArgs(t, r, c, "SET", "{u}:b", "2")
	got = sendArgs(t, r, c, "EXEC")
	arr, ok := got.([]any)
	if !ok || len(arr) != 2 {
		t.Fatalf("tagged EXEC = %v want two replies", got)
	}
}

// TestClusterSelectRestricted checks SELECT to a non-zero db is rejected in
// cluster mode and allowed in standalone mode.
func TestClusterSelectRestricted(t *testing.T) {
	r, c := startData(t)

	if got := sendArgs(t, r, c, "SELECT", "1"); got != "OK" {
		t.Fatalf("SELECT 1 in standalone = %v", got)
	}
	// Back to db 0 before turning cluster mode on.
	sendArgs(t, r, c, "SELECT", "0")
	sendArgs(t, r, c, "CONFIG", "SET", "cluster-enabled", "yes")

	got := sendArgs(t, r, c, "SELECT", "1")
	e, ok := got.(cmdErr)
	if !ok || !strings.Contains(string(e), "SELECT is not allowed in cluster mode") {
		t.Fatalf("SELECT 1 in cluster mode = %v want not-allowed error", got)
	}

	// SELECT 0 is still fine in cluster mode.
	if got := sendArgs(t, r, c, "SELECT", "0"); got != "OK" {
		t.Fatalf("SELECT 0 in cluster mode = %v", got)
	}
}
