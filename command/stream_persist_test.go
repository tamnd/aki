package command

import (
	"bufio"
	"net"
	"testing"
)

// seedStreamWithGroup builds a stream with three entries, one deleted to move the
// max-deleted ID, a consumer group, and two consumers each holding one pending
// entry. It is the fixture the stream persistence tests reload and inspect.
func seedStreamWithGroup(t *testing.T, r *bufio.Reader, c net.Conn) {
	t.Helper()
	_ = bulk(t, r, c, "XADD s 1-1 f a")
	_ = bulk(t, r, c, "XADD s 2-2 f b")
	_ = bulk(t, r, c, "XADD s 3-3 f c")
	if got := sendLine(t, r, c, "XDEL s 1-1"); got != ":1" {
		t.Fatalf("XDEL = %q want :1", got)
	}
	if got := sendLine(t, r, c, "XGROUP CREATE s g1 0"); got != "+OK" {
		t.Fatalf("XGROUP CREATE = %q", got)
	}
	_ = flatCmd(t, r, c, "XREADGROUP GROUP g1 alice COUNT 1 STREAMS s >")
	_ = flatCmd(t, r, c, "XREADGROUP GROUP g1 bob COUNT 1 STREAMS s >")
}

// checkStreamAfterReload asserts the reloaded stream still has the right length,
// counters, group, pending entries, and consumers.
func checkStreamAfterReload(t *testing.T, r *bufio.Reader, c net.Conn) {
	t.Helper()
	if got := sendLine(t, r, c, "XLEN s"); got != ":2" {
		t.Fatalf("XLEN = %q want :2", got)
	}
	info := flatCmd(t, r, c, "XINFO STREAM s")
	if got := valueAfter(t, info, "entries-added"); got != "3" {
		t.Fatalf("entries-added = %q want 3", got)
	}
	if got := valueAfter(t, info, "max-deleted-entry-id"); got != "1-1" {
		t.Fatalf("max-deleted-entry-id = %q want 1-1", got)
	}
	if got := valueAfter(t, info, "last-generated-id"); got != "3-3" {
		t.Fatalf("last-generated-id = %q want 3-3", got)
	}

	groups := flatCmd(t, r, c, "XINFO GROUPS s")
	if !contains(groups, "g1") {
		t.Fatalf("XINFO GROUPS missing g1: %v", groups)
	}

	// The group PEL holds both delivered entries, one per consumer.
	pend := flatCmd(t, r, c, "XPENDING s g1")
	for _, want := range []string{"2-2", "3-3", "alice", "bob"} {
		if !contains(pend, want) {
			t.Fatalf("XPENDING missing %q: %v", want, pend)
		}
	}

	// The consumers survive and still own their pending entries, so an XACK from
	// the right consumer clears one.
	if got := sendLine(t, r, c, "XACK s g1 2-2"); got != ":1" {
		t.Fatalf("XACK = %q want :1", got)
	}
}

// TestDumpRestoreStream round-trips a stream with a group and pending entries
// through DUMP and RESTORE.
func TestDumpRestoreStream(t *testing.T) {
	r, c := startData(t)
	seedStreamWithGroup(t, r, c)

	sendCmd(t, c, []byte("DUMP"), []byte("s"))
	payload := readBulkBytes(t, r)
	if payload == nil {
		t.Fatal("DUMP s returned nil")
	}
	_ = sendLine(t, r, c, "DEL s")
	sendCmd(t, c, []byte("RESTORE"), []byte("s"), []byte("0"), payload)
	if got := readSimple(t, r); got != "+OK" {
		t.Fatalf("RESTORE = %q", got)
	}

	checkStreamAfterReload(t, r, c)
}

// TestDebugReloadStream round-trips a stream through the whole-file snapshot codec
// that DEBUG RELOAD, SAVE, and a full sync share.
func TestDebugReloadStream(t *testing.T) {
	r, c := startData(t)
	seedStreamWithGroup(t, r, c)

	if got := sendLine(t, r, c, "DEBUG RELOAD"); got != "+OK" {
		t.Fatalf("DEBUG RELOAD = %q want +OK", got)
	}

	checkStreamAfterReload(t, r, c)
}
