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
