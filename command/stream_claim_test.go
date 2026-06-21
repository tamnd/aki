package command

import (
	"bufio"
	"net"
	"testing"
)

// setupGroupStream adds three entries and a group that has delivered all of them
// to consumer c1, so the PEL holds 1-1, 2-1, 3-1.
func setupGroupStream(t *testing.T) (*bufio.Reader, net.Conn) {
	t.Helper()
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = bulk(t, r, c, "XADD s 2-1 b 2")
	_ = bulk(t, r, c, "XADD s 3-1 c 3")
	_ = sendLine(t, r, c, "XGROUP CREATE s g1 0")
	_ = flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s >")
	return r, c
}

func TestXPendingSummary(t *testing.T) {
	r, c := setupGroupStream(t)
	toks := flatCmd(t, r, c, "XPENDING s g1")
	// [count, min, max, [[consumer, count]]]
	if toks[0] != "3" || toks[1] != "1-1" || toks[2] != "3-1" {
		t.Fatalf("XPENDING summary head = %v", toks)
	}
	if !contains(toks, "c1") {
		t.Fatalf("XPENDING summary missing consumer: %v", toks)
	}
}

func TestXPendingSummaryEmpty(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = sendLine(t, r, c, "XGROUP CREATE s g1 $")
	toks := flatCmd(t, r, c, "XPENDING s g1")
	if toks[0] != "0" || toks[1] != "<nil>" || toks[2] != "<nil>" {
		t.Fatalf("empty XPENDING summary = %v", toks)
	}
}

func TestXPendingExtended(t *testing.T) {
	r, c := setupGroupStream(t)
	toks := flatCmd(t, r, c, "XPENDING s g1 - + 10")
	for _, want := range []string{"1-1", "2-1", "3-1", "c1"} {
		if !contains(toks, want) {
			t.Fatalf("XPENDING extended missing %q in %v", want, toks)
		}
	}
	// COUNT caps the rows.
	toks = flatCmd(t, r, c, "XPENDING s g1 - + 1")
	if !contains(toks, "1-1") || contains(toks, "3-1") {
		t.Fatalf("XPENDING COUNT 1 = %v", toks)
	}
	// A high IDLE filter excludes the freshly delivered entries.
	toks = flatCmd(t, r, c, "XPENDING s g1 IDLE 999999 - + 10")
	if len(toks) != 0 {
		t.Fatalf("XPENDING IDLE high = %v want empty", toks)
	}
}

func TestXPendingNoGroup(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	if got := sendLine(t, r, c, "XPENDING s missing"); got != "-"+nogroupError("missing", "s") {
		t.Fatalf("XPENDING no group = %q", got)
	}
}

func TestXClaim(t *testing.T) {
	r, c := setupGroupStream(t)
	// Claim 1-1 for c2 with no idle guard.
	toks := flatCmd(t, r, c, "XCLAIM s g1 c2 0 1-1")
	if !contains(toks, "1-1") || !contains(toks, "a") {
		t.Fatalf("XCLAIM = %v", toks)
	}
	// Now 1-1 belongs to c2.
	toks = flatCmd(t, r, c, "XPENDING s g1 - + 10 c2")
	if !contains(toks, "1-1") {
		t.Fatalf("XPENDING c2 after claim = %v", toks)
	}
}

func TestXClaimJustID(t *testing.T) {
	r, c := setupGroupStream(t)
	toks := flatCmd(t, r, c, "XCLAIM s g1 c2 0 2-1 JUSTID")
	if len(toks) != 1 || toks[0] != "2-1" {
		t.Fatalf("XCLAIM JUSTID = %v", toks)
	}
}

func TestXClaimIdleGuard(t *testing.T) {
	r, c := setupGroupStream(t)
	// Entries were just delivered, so a large min-idle skips them all.
	toks := flatCmd(t, r, c, "XCLAIM s g1 c2 999999 1-1")
	if len(toks) != 0 {
		t.Fatalf("XCLAIM idle guard = %v want empty", toks)
	}
}

func TestXClaimForce(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = sendLine(t, r, c, "XGROUP CREATE s g1 $")
	// 1-1 was never delivered, so it is not in the PEL. FORCE claims it anyway.
	toks := flatCmd(t, r, c, "XCLAIM s g1 c2 0 1-1 FORCE")
	if !contains(toks, "1-1") {
		t.Fatalf("XCLAIM FORCE = %v", toks)
	}
	// Without FORCE a non-pending ID is skipped.
	toks = flatCmd(t, r, c, "XCLAIM s g1 c3 0 9-9")
	if len(toks) != 0 {
		t.Fatalf("XCLAIM non-pending = %v want empty", toks)
	}
}

func TestXAutoClaim(t *testing.T) {
	r, c := setupGroupStream(t)
	toks := flatCmd(t, r, c, "XAUTOCLAIM s g1 c2 0 0-0")
	// [cursor, [entries], [deleted]]
	if toks[0] != "0-0" {
		t.Fatalf("XAUTOCLAIM cursor = %q want 0-0", toks[0])
	}
	for _, want := range []string{"1-1", "2-1", "3-1"} {
		if !contains(toks, want) {
			t.Fatalf("XAUTOCLAIM missing %q in %v", want, toks)
		}
	}
}

func TestXAutoClaimJustID(t *testing.T) {
	r, c := setupGroupStream(t)
	toks := flatCmd(t, r, c, "XAUTOCLAIM s g1 c2 0 0-0 JUSTID")
	if toks[0] != "0-0" || !contains(toks, "2-1") {
		t.Fatalf("XAUTOCLAIM JUSTID = %v", toks)
	}
}

func TestXAutoClaimDeleted(t *testing.T) {
	r, c := setupGroupStream(t)
	// Delete a pending entry, then autoclaim should report it as deleted.
	_ = sendLine(t, r, c, "XDEL s 2-1")
	toks := flatCmd(t, r, c, "XAUTOCLAIM s g1 c2 0 0-0")
	if !contains(toks, "2-1") {
		t.Fatalf("XAUTOCLAIM deleted list missing 2-1: %v", toks)
	}
	// 2-1 is gone from the PEL now.
	toks = flatCmd(t, r, c, "XPENDING s g1 - + 10")
	if contains(toks, "2-1") {
		t.Fatalf("2-1 still pending after autoclaim: %v", toks)
	}
}

func TestXClaimWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	for _, cmd := range []string{"XPENDING k g", "XCLAIM k g c 0 1-1", "XAUTOCLAIM k g c 0 0-0"} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
}
