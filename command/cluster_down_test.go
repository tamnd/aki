package command

import (
	"strings"
	"testing"
)

// TestClusterDown checks that key commands are refused while the cluster has
// incomplete slot coverage, that reads-when-down serves reads but not writes,
// and that full coverage restores normal service.
func TestClusterDown(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "CONFIG", "SET", "cluster-enabled", "yes")

	// No slots assigned and require-full-coverage on (the default), so any key
	// command is refused.
	got := sendArgs(t, r, c, "SET", "a", "1")
	if e, ok := got.(cmdErr); !ok || string(e) != "CLUSTERDOWN The cluster is down" {
		t.Fatalf("SET while down = %v want cluster down", got)
	}

	// With reads-when-down on, a read is served and a write gets the read-only
	// variant.
	sendArgs(t, r, c, "CONFIG", "SET", "cluster-allow-reads-when-down", "yes")
	if got := sendArgs(t, r, c, "GET", "a"); got != nil {
		t.Fatalf("GET while down with reads allowed = %v want nil", got)
	}
	got = sendArgs(t, r, c, "SET", "a", "1")
	if e, ok := got.(cmdErr); !ok || string(e) != "CLUSTERDOWN The cluster is down and only accepts read commands" {
		t.Fatalf("SET while down with reads allowed = %v want read-only down", got)
	}

	// Cover every slot and the cluster serves writes again.
	sendArgs(t, r, c, "CLUSTER", "ADDSLOTSRANGE", "0", "16383")
	if got := sendArgs(t, r, c, "SET", "a", "1"); got != "OK" {
		t.Fatalf("SET after full coverage = %v want OK", got)
	}
	if got := sendArgs(t, r, c, "GET", "a"); got != "1" {
		t.Fatalf("GET after full coverage = %v want 1", got)
	}
}

// TestClusterHashSlotNotServed checks that with require-full-coverage off the
// cluster stays up but a key on an unassigned slot is still refused.
func TestClusterHashSlotNotServed(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "CONFIG", "SET", "cluster-enabled", "yes")
	sendArgs(t, r, c, "CONFIG", "SET", "cluster-require-full-coverage", "no")

	// State is ok, but the slot for this key is not served by this node.
	got := sendArgs(t, r, c, "SET", "a", "1")
	if e, ok := got.(cmdErr); !ok || !strings.Contains(string(e), "Hash slot not served") {
		t.Fatalf("SET on unserved slot = %v want hash slot not served", got)
	}

	sendArgs(t, r, c, "CLUSTER", "ADDSLOTSRANGE", "0", "16383")
	if got := sendArgs(t, r, c, "SET", "a", "1"); got != "OK" {
		t.Fatalf("SET after assigning slots = %v want OK", got)
	}
}
