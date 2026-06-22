package command

import (
	"fmt"
	"testing"
)

// TestStreamNodeMaxEntriesConfig checks that the radix-tree node counts in XINFO
// STREAM track CONFIG SET stream-node-max-entries instead of a fixed constant. The
// flat store has no real rax, so these counts are an approximation, but the
// approximation now follows the configured node capacity the way Redis does.
func TestStreamNodeMaxEntriesConfig(t *testing.T) {
	r, c := startData(t)

	// Add 25 entries with explicit ids so the count is deterministic.
	for i := 1; i <= 25; i++ {
		id := fmt.Sprintf("%d-1", i)
		if got := bulk(t, r, c, "XADD s "+id+" f v"); got != id {
			t.Fatalf("XADD %s = %q", id, got)
		}
	}

	// Under the default capacity of 100 all 25 entries fit in one node.
	toks := xinfoReply(t, r, c, "XINFO STREAM s")
	if got := valueAfter(t, toks, "radix-tree-keys"); got != "1" {
		t.Fatalf("radix-tree-keys default = %q want 1", got)
	}

	// Lowering the capacity to 10 splits 25 entries across ceil(25/10) = 3 nodes.
	if got := sendLine(t, r, c, "CONFIG SET stream-node-max-entries 10"); got != "+OK" {
		t.Fatalf("CONFIG SET stream-node-max-entries 10 = %q", got)
	}
	toks = xinfoReply(t, r, c, "XINFO STREAM s")
	if got := valueAfter(t, toks, "radix-tree-keys"); got != "3" {
		t.Fatalf("radix-tree-keys at 10 = %q want 3", got)
	}
	if got := valueAfter(t, toks, "radix-tree-nodes"); got != "4" {
		t.Fatalf("radix-tree-nodes at 10 = %q want 4", got)
	}

	// The FULL form reports the same counts.
	toks = xinfoReply(t, r, c, "XINFO STREAM s FULL")
	if got := valueAfter(t, toks, "radix-tree-keys"); got != "3" {
		t.Fatalf("radix-tree-keys FULL = %q want 3", got)
	}

	// A capacity of zero means no entry cap per node, so the stream is one node.
	if got := sendLine(t, r, c, "CONFIG SET stream-node-max-entries 0"); got != "+OK" {
		t.Fatalf("CONFIG SET stream-node-max-entries 0 = %q", got)
	}
	toks = xinfoReply(t, r, c, "XINFO STREAM s")
	if got := valueAfter(t, toks, "radix-tree-keys"); got != "1" {
		t.Fatalf("radix-tree-keys at 0 = %q want 1", got)
	}
}
