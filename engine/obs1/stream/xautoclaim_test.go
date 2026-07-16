package stream

import "testing"

// The XAUTOCLAIM suite (spec 2064/f3/14 section 7.7): a scan of the group PEL from a
// cursor that transfers every entry idle at least min-idle to the target consumer,
// drops pending entries whose log entry an XDEL removed (reported separately), and
// returns a cursor to resume from. The scan is budgeted at ten times COUNT so a cold
// PEL still returns promptly, and each claim is the same in-place slab rewrite XCLAIM
// does.

// autoClaimReply is a decoded XAUTOCLAIM reply: the resume cursor, the claimed
// entries (fields nil under justid), and the ids dropped as deleted.
type autoClaimReply struct {
	cursor  string
	entries []gEntry
	deleted []string
}

// autoClaimResultOf decodes the three-element XAUTOCLAIM reply. Under justid the
// second element is a flat id list; otherwise it is [id, [field value ...]] pairs.
func autoClaimResultOf(t *testing.T, raw []byte, justid bool) autoClaimReply {
	t.Helper()
	arr := decodeReply(t, raw).([]any)
	if len(arr) != 3 {
		t.Fatalf("reply = %v, want 3 elements", render(arr))
	}
	out := autoClaimReply{cursor: arr[0].(string)}
	for _, e := range arr[1].([]any) {
		if justid {
			out.entries = append(out.entries, gEntry{id: e.(string)})
			continue
		}
		ea := e.([]any)
		fa := ea[1].([]any)
		fs := make([]string, len(fa))
		for i := range fa {
			fs[i] = fa[i].(string)
		}
		out.entries = append(out.entries, gEntry{id: ea[0].(string), fields: fs})
	}
	for _, d := range arr[2].([]any) {
		out.deleted = append(out.deleted, d.(string))
	}
	return out
}

func TestXautoclaimClaimsIdleEntries(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">") // 1,2,3 pending under c1

	res := autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c2", "0", "0"), false)
	if res.cursor != "0-0" {
		t.Fatalf("cursor = %q, want 0-0 (reached end)", res.cursor)
	}
	if len(res.entries) != 3 || res.entries[0].id != "1-0" || res.entries[2].id != "3-0" {
		t.Fatalf("claimed = %v, want 1-0..3-0", res.entries)
	}
	if len(res.entries[0].fields) != 2 || res.entries[0].fields[0] != "f" || res.entries[0].fields[1] != "v1-0" {
		t.Fatalf("entry fields = %v, want [f v1-0]", res.entries[0].fields)
	}
	if len(res.deleted) != 0 {
		t.Fatalf("deleted = %v, want none", res.deleted)
	}
	// All three moved to c2 with the delivery count bumped to 2.
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if len(rows) != 3 {
		t.Fatalf("pending rows = %v, want 3", rows)
	}
	for _, r := range rows {
		if r[1] != "c2" || r[3] != "2" {
			t.Fatalf("row = %v, want owner c2 count 2", r)
		}
	}
}

func TestXautoclaimJustID(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 2)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	res := autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c2", "0", "0", "JUSTID"), true)
	if len(res.entries) != 2 || res.entries[0].id != "1-0" || res.entries[1].id != "2-0" {
		t.Fatalf("claimed ids = %v, want [1-0 2-0]", res.entries)
	}
	// JUSTID suppresses the count bump: still 1 after the claim.
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if rows[0][3] != "1" {
		t.Fatalf("count = %s, want 1 (no bump under JUSTID)", rows[0][3])
	}
}

func TestXautoclaimMinIdleGate(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 2)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	// Freshly delivered entries are idle ~0, so a large min-idle claims nothing.
	res := autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c2", "100000", "0"), false)
	if len(res.entries) != 0 || res.cursor != "0-0" {
		t.Fatalf("res = %+v, want no claims cursor 0-0", res)
	}
	if rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10")); rows[0][1] != "c1" {
		t.Fatalf("owner = %s, want c1 unchanged", rows[0][1])
	}
}

func TestXautoclaimCursorPaginates(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 5)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	r1 := autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c2", "0", "0", "COUNT", "2"), false)
	if len(r1.entries) != 2 || r1.entries[0].id != "1-0" || r1.entries[1].id != "2-0" || r1.cursor != "3-0" {
		t.Fatalf("page 1 = %+v, want [1-0 2-0] cursor 3-0", r1)
	}
	r2 := autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c2", "0", r1.cursor, "COUNT", "2"), false)
	if len(r2.entries) != 2 || r2.entries[0].id != "3-0" || r2.entries[1].id != "4-0" || r2.cursor != "5-0" {
		t.Fatalf("page 2 = %+v, want [3-0 4-0] cursor 5-0", r2)
	}
	r3 := autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c2", "0", r2.cursor, "COUNT", "2"), false)
	if len(r3.entries) != 1 || r3.entries[0].id != "5-0" || r3.cursor != "0-0" {
		t.Fatalf("page 3 = %+v, want [5-0] cursor 0-0", r3)
	}
}

func TestXautoclaimDeletedEntryReported(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")
	wantInt(t, do(t, c, opXdel, "s", "2-0"), 1)

	res := autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c2", "0", "0"), false)
	if len(res.entries) != 2 || res.entries[0].id != "1-0" || res.entries[1].id != "3-0" {
		t.Fatalf("claimed = %v, want [1-0 3-0]", res.entries)
	}
	if len(res.deleted) != 1 || res.deleted[0] != "2-0" {
		t.Fatalf("deleted = %v, want [2-0]", res.deleted)
	}
	// 2-0 is gone from the PEL; only the two live entries remain.
	if sum := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any); sum[0].(string) != "2" {
		t.Fatalf("total = %v, want 2", sum[0])
	}
}

func TestXautoclaimExclusiveStart(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	// "(1-0" excludes 1-0, so the scan starts at 2-0.
	res := autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c2", "0", "(1-0"), false)
	if len(res.entries) != 2 || res.entries[0].id != "2-0" || res.entries[1].id != "3-0" {
		t.Fatalf("claimed = %v, want [2-0 3-0] (1-0 excluded)", res.entries)
	}
}

func TestXautoclaimScanBudgetBounds(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 15)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">") // 15 fresh, idle ~0

	// COUNT 1 gives a scan budget of 10. With a min-idle none of the fresh entries
	// meet, the walk exhausts the budget without a claim and returns the 11th id as
	// the cursor, so a recovery loop still makes bounded progress over a cold PEL.
	res := autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c2", "100000", "0", "COUNT", "1"), false)
	if len(res.entries) != 0 || len(res.deleted) != 0 {
		t.Fatalf("res = %+v, want no claims", res)
	}
	if res.cursor != "11-0" {
		t.Fatalf("cursor = %q, want 11-0 (scan budget of 10 exhausted)", res.cursor)
	}
}

func TestXautoclaimSelfReclaim(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	// Reclaiming to the current owner keeps ownership and still counts a delivery.
	res := autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c1", "0", "0"), false)
	if len(res.entries) != 1 || res.entries[0].id != "1-0" {
		t.Fatalf("claimed = %v, want [1-0]", res.entries)
	}
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if rows[0][1] != "c1" || rows[0][3] != "2" {
		t.Fatalf("row = %v, want owner c1 count 2", rows[0])
	}
}

func TestXautoclaimEmptyPEL(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1) // group exists, nothing delivered, PEL still nil

	res := autoClaimResultOf(t, do(t, c, opXautoclaim, "s", "g", "c1", "0", "0"), false)
	if res.cursor != "0-0" || len(res.entries) != 0 || len(res.deleted) != 0 {
		t.Fatalf("res = %+v, want empty cursor 0-0", res)
	}
}

func TestXautoclaimErrors(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)

	wantErr(t, do(t, c, opXautoclaim, "s", "g", "c", "notanint", "0"),
		"ERR Invalid min-idle-time argument for XAUTOCLAIM")
	wantErr(t, do(t, c, opXautoclaim, "s", "g", "c", "0", "notanid"), errInvalidID)
	// An exclusive bound at the maximum id has no successor to start from.
	maxEx := "(18446744073709551615-18446744073709551615"
	wantErr(t, do(t, c, opXautoclaim, "s", "g", "c", "0", maxEx),
		"ERR invalid start ID for the interval")
	wantErr(t, do(t, c, opXautoclaim, "s", "g", "c", "0", "0", "COUNT", "0"), "ERR COUNT must be > 0")
	wantErr(t, do(t, c, opXautoclaim, "s", "g", "c", "0", "0", "COUNT", "-3"), "ERR COUNT must be > 0")
	wantErr(t, do(t, c, opXautoclaim, "s", "g", "c", "0", "0", "COUNT", "x"), "ERR COUNT must be > 0")
	wantErr(t, do(t, c, opXautoclaim, "s", "g", "c", "0", "0", "BOGUS"), "ERR syntax error")
}

func TestXautoclaimNogroupAndWrongType(t *testing.T) {
	c := newHarness(t).NewConn()

	wantErr(t, do(t, c, opXautoclaim, "nostream", "g", "c", "0", "0"),
		nogroupGeneric([]byte("nostream"), []byte("g")))
	seedGroup(t, c, 1)
	wantErr(t, do(t, c, opXautoclaim, "s", "nogroup", "c", "0", "0"),
		nogroupGeneric([]byte("s"), []byte("nogroup")))

	do(t, c, opSet, "str", "x")
	wantErr(t, do(t, c, opXautoclaim, "str", "g", "c", "0", "0"), wrongType)
}
