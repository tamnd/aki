package stream

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The XNACK suite (spec 2064/f3/14 section 7.6, new in Redis 8.8): a negative
// acknowledgement that hands pending entries back to the group as unowned,
// immediately-claimable NACKs. Each id is one PEL point rewrite: the entry is
// disowned, its idle clock reset to the epoch, and its delivery count adjusted by
// the mode (SILENT decrements, FAIL keeps, FATAL saturates) or an explicit
// RETRYCOUNT. FORCE creates an unowned entry for a live-but-undelivered id. The
// reply is the integer count nacked.

// nackSetup delivers n entries to consumer c1 so they are pending with delivery
// count 1, the common starting point for the mode tests.
func nackSetup(t *testing.T, n int) *shard.Conn {
	t.Helper()
	c := newHarness(t).NewConn()
	seedGroup(t, c, n)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")
	return c
}

func TestXnackSilentDisownsAndResetsIdle(t *testing.T) {
	c := nackSetup(t, 3)

	wantInt(t, do(t, c, opXnack, "s", "g", "SILENT", "IDS", "1", "1-0"), 1)

	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if len(rows) != 3 {
		t.Fatalf("pending rows = %v, want 3", rows)
	}
	// 1-0 is now unowned: empty consumer, -1 idle, count decremented 1 -> 0.
	if rows[0][0] != "1-0" || rows[0][1] != "" || rows[0][2] != "-1" || rows[0][3] != "0" {
		t.Fatalf("row 1-0 = %v, want [1-0 \"\" -1 0]", rows[0])
	}
	// The others stay owned by c1.
	if rows[1][1] != "c1" || rows[2][1] != "c1" {
		t.Fatalf("rows 2,3 = %v %v, want owner c1", rows[1], rows[2])
	}
}

func TestXnackFailKeepsCount(t *testing.T) {
	c := nackSetup(t, 1)

	wantInt(t, do(t, c, opXnack, "s", "g", "FAIL", "IDS", "1", "1-0"), 1)
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if rows[0][1] != "" || rows[0][3] != "1" {
		t.Fatalf("row = %v, want unowned count 1 (FAIL keeps)", rows[0])
	}
}

func TestXnackFatalSaturates(t *testing.T) {
	c := nackSetup(t, 1)

	wantInt(t, do(t, c, opXnack, "s", "g", "FATAL", "IDS", "1", "1-0"), 1)
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if rows[0][3] != "65535" {
		t.Fatalf("count = %s, want 65535 (FATAL saturates the u16 slab)", rows[0][3])
	}
}

func TestXnackRetrycountOverridesMode(t *testing.T) {
	c := nackSetup(t, 2)

	// RETRYCOUNT wins over the mode: FATAL would saturate, but the explicit 5 stands.
	wantInt(t, do(t, c, opXnack, "s", "g", "FATAL", "IDS", "1", "1-0", "RETRYCOUNT", "5"), 1)
	// And over SILENT, which would have decremented.
	wantInt(t, do(t, c, opXnack, "s", "g", "SILENT", "IDS", "1", "2-0", "RETRYCOUNT", "7"), 1)

	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if rows[0][3] != "5" || rows[1][3] != "7" {
		t.Fatalf("counts = %s %s, want 5 and 7 (RETRYCOUNT override)", rows[0][3], rows[1][3])
	}
}

func TestXnackMakesImmediatelyClaimable(t *testing.T) {
	c := nackSetup(t, 1)

	// A fresh delivery is idle ~0, so a large min-idle claims nothing yet.
	res := autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c2", "100000", "0"), false)
	if len(res.entries) != 0 {
		t.Fatalf("pre-nack claim = %v, want none", res.entries)
	}
	// After a nack the idle clock is the epoch, so the same claim takes it at once.
	wantInt(t, do(t, c, opXnack, "s", "g", "FAIL", "IDS", "1", "1-0"), 1)
	res = autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c2", "100000", "0"), false)
	if len(res.entries) != 1 || res.entries[0].id != "1-0" {
		t.Fatalf("post-nack claim = %v, want [1-0]", res.entries)
	}
}

func TestXnackForceCreatesUnowned(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1) // 1-0 in the log, never delivered, so not pending

	// Without FORCE an undelivered id is skipped.
	wantInt(t, do(t, c, opXnack, "s", "g", "FAIL", "IDS", "1", "1-0"), 0)
	if sum := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any); sum[0].(string) != "0" {
		t.Fatalf("total = %v, want 0 (no force, no entry)", sum[0])
	}

	// FORCE creates it unowned from a zero baseline; FATAL then saturates the count.
	wantInt(t, do(t, c, opXnack, "s", "g", "FATAL", "IDS", "1", "1-0", "FORCE"), 1)
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if len(rows) != 1 || rows[0][0] != "1-0" || rows[0][1] != "" || rows[0][2] != "-1" || rows[0][3] != "65535" {
		t.Fatalf("row = %v, want [1-0 \"\" -1 65535]", rows[0])
	}
}

func TestXnackForceSkipsDeletedEntry(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	wantInt(t, do(t, c, opXdel, "s", "1-0"), 1) // log entry gone

	// FORCE creates nothing for an id whose log entry no longer exists.
	wantInt(t, do(t, c, opXnack, "s", "g", "FAIL", "IDS", "1", "1-0", "FORCE"), 0)
	if sum := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any); sum[0].(string) != "0" {
		t.Fatalf("total = %v, want 0", sum[0])
	}
}

func TestXnackMultipleAndSummary(t *testing.T) {
	c := nackSetup(t, 3)

	// Nack two of the three; the third stays owned by c1.
	wantInt(t, do(t, c, opXnack, "s", "g", "SILENT", "IDS", "2", "1-0", "2-0"), 2)

	sum := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any)
	// Total still counts the unowned NACKs; the pending ID range spans all three.
	if sum[0].(string) != "3" || sum[1].(string) != "1-0" || sum[2].(string) != "3-0" {
		t.Fatalf("summary head = %v, want [3 1-0 3-0 ...]", render(sum))
	}
	// The per-consumer breakdown drops the disowned entries: c1 owns only 3-0.
	cons := sum[3].([]any)
	if len(cons) != 1 {
		t.Fatalf("consumers = %v, want one", render(cons))
	}
	row := cons[0].([]any)
	if row[0].(string) != "c1" || row[1].(string) != "1" {
		t.Fatalf("consumer row = %v, want [c1 1]", render(row))
	}
}

func TestXnackMixedFoundAndForce(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 2)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "COUNT", "1", "STREAMS", "s", ">") // only 1-0 delivered

	// 1-0 is pending (found), 2-0 is only in the log (needs FORCE): both nacked.
	wantInt(t, do(t, c, opXnack, "s", "g", "FAIL", "IDS", "2", "1-0", "2-0", "FORCE"), 2)
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if len(rows) != 2 || rows[0][1] != "" || rows[1][1] != "" {
		t.Fatalf("rows = %v, want both unowned", rows)
	}
}

func TestXnackAlreadyUnowned(t *testing.T) {
	c := nackSetup(t, 1)

	wantInt(t, do(t, c, opXnack, "s", "g", "SILENT", "IDS", "1", "1-0"), 1) // 1 -> 0, unowned
	// Nacking an already-unowned entry still finds it and does not underflow.
	wantInt(t, do(t, c, opXnack, "s", "g", "SILENT", "IDS", "1", "1-0"), 1)
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if rows[0][1] != "" || rows[0][3] != "0" {
		t.Fatalf("row = %v, want unowned count 0", rows[0])
	}
}

func TestXnackNonPendingSkip(t *testing.T) {
	c := nackSetup(t, 1)

	// An id never delivered and not forced counts zero and changes nothing.
	wantInt(t, do(t, c, opXnack, "s", "g", "FAIL", "IDS", "1", "9-0"), 0)
	if sum := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any); sum[0].(string) != "1" {
		t.Fatalf("total = %v, want 1 unchanged", sum[0])
	}
}

func TestXnackErrors(t *testing.T) {
	c := nackSetup(t, 1)

	wantErr(t, do(t, c, opXnack, "s", "g", "MAYBE", "IDS", "1", "1-0"),
		"ERR mode must be SILENT, FAIL, or FATAL")
	wantErr(t, do(t, c, opXnack, "s", "g", "SILENT", "IDS", "0", "1-0"),
		"ERR numids must be a positive integer")
	wantErr(t, do(t, c, opXnack, "s", "g", "SILENT", "IDS", "x", "1-0"),
		"ERR numids must be a positive integer")
	wantErr(t, do(t, c, opXnack, "s", "g", "SILENT", "IDS", "3", "1-0"),
		"ERR number of IDs doesn't match numids")
	wantErr(t, do(t, c, opXnack, "s", "g", "SILENT", "IDS", "1", "1-0", "RETRYCOUNT", "-1"),
		"ERR Invalid RETRYCOUNT value, must be >= 0")
	wantErr(t, do(t, c, opXnack, "s", "g", "SILENT", "IDS", "1", "1-0", "BOGUS"),
		"ERR Unrecognized XNACK option 'BOGUS'")
	wantErr(t, do(t, c, opXnack, "s", "g", "SILENT", "FORCE"),
		"ERR syntax error, expected IDS keyword")
	wantErr(t, do(t, c, opXnack, "s", "g", "SILENT", "IDS", "1", "notanid"), errInvalidID)
}

func TestXnackNogroupAndWrongType(t *testing.T) {
	c := newHarness(t).NewConn()

	wantErr(t, do(t, c, opXnack, "nostream", "g", "SILENT", "IDS", "1", "1-0"),
		nogroupGeneric([]byte("nostream"), []byte("g")))
	seedGroup(t, c, 1)
	wantErr(t, do(t, c, opXnack, "s", "nogroup", "SILENT", "IDS", "1", "1-0"),
		nogroupGeneric([]byte("s"), []byte("nogroup")))

	do(t, c, opSet, "str", "x")
	wantErr(t, do(t, c, opXnack, "str", "g", "SILENT", "IDS", "1", "1-0"), wrongType)
}
