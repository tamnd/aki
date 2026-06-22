package command

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestXReadGroupPropagatesToAOF checks a > delivery records its group-state
// effects in the AOF as an XCLAIM per entry plus an XGROUP SETID, and that a
// reload rebuilds the pending entry from them.
func TestXReadGroupPropagatesToAOF(t *testing.T) {
	r, c := startData(t)
	dir := enableAOF(t, r, c)
	_ = sendLine(t, r, c, "BGREWRITEAOF")

	_ = sendArgs(t, r, c, "XADD", "s", "1-1", "f", "v")
	if got := sendArgs(t, r, c, "XGROUP", "CREATE", "s", "g", "0"); got != "OK" {
		t.Fatalf("XGROUP CREATE = %v", got)
	}
	// The first > read delivers entry 1-1 to consumer alice and tracks it.
	if got := sendArgs(t, r, c, "XREADGROUP", "GROUP", "g", "alice", "COUNT", "10", "STREAMS", "s", ">"); got == nil {
		t.Fatalf("XREADGROUP > returned nil")
	}

	incr := readIncrFile(t, filepath.Join(dir, "appendonlydir"))
	if !strings.Contains(incr, "XCLAIM") {
		t.Fatalf("incr missing XCLAIM: %q", incr)
	}
	if !strings.Contains(incr, "XGROUP") || !strings.Contains(incr, "SETID") {
		t.Fatalf("incr missing XGROUP SETID: %q", incr)
	}

	// Reload from the AOF and confirm the pending entry survived.
	if got := sendLine(t, r, c, "DEBUG LOADAOF"); got != "+OK" {
		t.Fatalf("DEBUG LOADAOF = %q", got)
	}
	pending := asArray(t, sendArgs(t, r, c, "XPENDING", "s", "g"))
	if len(pending) == 0 || pending[0] != int64(1) {
		t.Fatalf("XPENDING after reload = %v want count 1", pending)
	}
}

// TestXReadGroupNoAckAdvancesAOF checks a NOACK > delivery records only the
// XGROUP SETID, no XCLAIM, since it keeps no pending entry.
func TestXReadGroupNoAckAdvancesAOF(t *testing.T) {
	r, c := startData(t)
	dir := enableAOF(t, r, c)
	_ = sendLine(t, r, c, "BGREWRITEAOF")

	_ = sendArgs(t, r, c, "XADD", "s", "1-1", "f", "v")
	_ = sendArgs(t, r, c, "XGROUP", "CREATE", "s", "g", "0")
	_ = sendArgs(t, r, c, "XREADGROUP", "GROUP", "g", "alice", "NOACK", "STREAMS", "s", ">")

	incr := readIncrFile(t, filepath.Join(dir, "appendonlydir"))
	if strings.Contains(incr, "XCLAIM") {
		t.Fatalf("NOACK incr should not hold XCLAIM: %q", incr)
	}
	if !strings.Contains(incr, "SETID") {
		t.Fatalf("NOACK incr missing XGROUP SETID: %q", incr)
	}
}

// TestReplicationStreamsXReadGroup checks a > delivery on the master reaches a
// replica so the replica reports the same pending entry.
func TestReplicationStreamsXReadGroup(t *testing.T) {
	mr, mc, mHost, mPort := startDataAddr(t)
	rr, rc, _, _ := startDataAddr(t)

	if got := sendLine(t, rr, rc, "REPLICAOF "+mHost+" "+mPort); got != "+OK" {
		t.Fatalf("REPLICAOF = %q", got)
	}
	// Wait until the link is live so the stream commands flow over the command
	// stream rather than racing the full sync.
	_ = sendLine(t, mr, mc, "SET link up")
	waitForBulk(t, rr, rc, "link", "up")

	_ = sendArgs(t, mr, mc, "XADD", "s", "1-1", "f", "v")
	_ = sendArgs(t, mr, mc, "XGROUP", "CREATE", "s", "g", "0")

	// The group exists on the replica once the stream and group commands stream over.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		arr, ok := sendArgs(t, rr, rc, "XINFO", "GROUPS", "s").([]any)
		if ok && len(arr) == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Deliver the entry on the master, then poll the replica for the pending entry.
	_ = sendArgs(t, mr, mc, "XREADGROUP", "GROUP", "g", "alice", "COUNT", "10", "STREAMS", "s", ">")

	deadline = time.Now().Add(3 * time.Second)
	var last []any
	for time.Now().Before(deadline) {
		if arr, ok := sendArgs(t, rr, rc, "XPENDING", "s", "g").([]any); ok {
			last = arr
			if len(arr) > 0 && arr[0] == int64(1) {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("replica XPENDING = %v want count 1", last)
}
