package stream

import (
	"strconv"
	"testing"
)

// The XCLAIM suite (spec 2064/f3/14 section 7.7): each claim is a point rewrite of
// the group PEL that moves an entry's ownership to a live consumer, gated by
// min-idle. FORCE creates a slab for a not-yet-pending entry that still exists in
// the log; JUSTID renders IDs only and suppresses the count bump; IDLE/TIME set
// the delivery clock and RETRYCOUNT the count outright. A pending entry whose log
// entry an XDEL removed is dropped from the PEL, never claimed.

// claimEntries decodes a default-form XCLAIM reply into gEntry rows (an [id, nil]
// pair decodes to fields nil, the deleted-entry shape).
func claimEntries(t *testing.T, raw []byte) []gEntry {
	t.Helper()
	arr := decodeReply(t, raw).([]any)
	out := make([]gEntry, 0, len(arr))
	for _, e := range arr {
		ea := e.([]any)
		id := ea[0].(string)
		if ea[1] == nil {
			out = append(out, gEntry{id: id})
			continue
		}
		fa := ea[1].([]any)
		fs := make([]string, len(fa))
		for i := range fa {
			fs[i] = fa[i].(string)
		}
		out = append(out, gEntry{id: id, fields: fs})
	}
	return out
}

// claimIDs decodes a JUSTID XCLAIM reply into its flat id list.
func claimIDs(t *testing.T, raw []byte) []string {
	t.Helper()
	arr := decodeReply(t, raw).([]any)
	out := make([]string, len(arr))
	for i, e := range arr {
		out[i] = e.(string)
	}
	return out
}

func TestXclaimReassignsOwner(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	got := claimEntries(t, do(t, c, opXclaim, "s", "g", "c2", "0", "1-0"))
	if len(got) != 1 || got[0].id != "1-0" ||
		len(got[0].fields) != 2 || got[0].fields[0] != "f" || got[0].fields[1] != "v1-0" {
		t.Fatalf("claim = %v, want [1-0 [f v1-0]]", got)
	}

	// Ownership moved to c2 and the delivery count rose to 2.
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if len(rows) != 1 || rows[0][1] != "c2" || rows[0][3] != "2" {
		t.Fatalf("pending = %v, want owner c2 count 2", rows)
	}
	// The old owner keeps nothing, the summary counts one under c2.
	sum := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any)
	owners := sum[3].([]any)
	if len(owners) != 1 || owners[0].([]any)[0].(string) != "c2" || owners[0].([]any)[1].(string) != "1" {
		t.Fatalf("summary owners = %v, want [[c2 1]]", owners)
	}
}

func TestXclaimJustIDNoCountBump(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	ids := claimIDs(t, do(t, c, opXclaim, "s", "g", "c2", "0", "1-0", "JUSTID"))
	if len(ids) != 1 || ids[0] != "1-0" {
		t.Fatalf("justid claim = %v, want [1-0]", ids)
	}
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if len(rows) != 1 || rows[0][1] != "c2" || rows[0][3] != "1" {
		t.Fatalf("pending = %v, want owner c2 count 1 (no bump)", rows)
	}
}

func TestXclaimMinIdleGate(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	// A freshly delivered entry is idle ~0, so a large min-idle claims nothing.
	if got := claimEntries(t, do(t, c, opXclaim, "s", "g", "c2", "100000", "1-0")); len(got) != 0 {
		t.Fatalf("claim = %v, want empty (idle below floor)", got)
	}
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if len(rows) != 1 || rows[0][1] != "c1" || rows[0][3] != "1" {
		t.Fatalf("pending = %v, want owner unchanged c1 count 1", rows)
	}
}

func TestXclaimForceCreatesPending(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1) // 1-0 exists in the log but was never delivered

	// Without FORCE a not-yet-pending entry is skipped.
	if got := claimEntries(t, do(t, c, opXclaim, "s", "g", "c1", "0", "1-0")); len(got) != 0 {
		t.Fatalf("claim = %v, want empty (not pending, no force)", got)
	}
	if sum := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any); sum[0].(string) != "0" {
		t.Fatalf("total = %v, want 0", sum[0])
	}

	// FORCE creates the pending slab and claims it, count 1.
	got := claimEntries(t, do(t, c, opXclaim, "s", "g", "c1", "0", "1-0", "FORCE"))
	if len(got) != 1 || got[0].id != "1-0" {
		t.Fatalf("force claim = %v, want [1-0 ...]", got)
	}
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if len(rows) != 1 || rows[0][1] != "c1" || rows[0][3] != "1" {
		t.Fatalf("pending = %v, want owner c1 count 1", rows)
	}
}

func TestXclaimForceMissingEntryNoop(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1) // only 1-0 exists

	// FORCE cannot create a pending entry for an ID with no log entry.
	if got := claimEntries(t, do(t, c, opXclaim, "s", "g", "c1", "0", "9-0", "FORCE")); len(got) != 0 {
		t.Fatalf("claim = %v, want empty (no such log entry)", got)
	}
	if sum := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any); sum[0].(string) != "0" {
		t.Fatalf("total = %v, want 0", sum[0])
	}
}

func TestXclaimDeletedEntryDropped(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")
	wantInt(t, do(t, c, opXdel, "s", "1-0"), 1)

	// The pending entry outlived its log entry: the claim drops it, not returns it.
	if got := claimEntries(t, do(t, c, opXclaim, "s", "g", "c2", "0", "1-0")); len(got) != 0 {
		t.Fatalf("claim = %v, want empty (entry deleted)", got)
	}
	if sum := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any); sum[0].(string) != "0" {
		t.Fatalf("total = %v, want 0 (dropped)", sum[0])
	}
}

func TestXclaimIDLEOverride(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	do(t, c, opXclaim, "s", "g", "c2", "0", "1-0", "IDLE", "5000")
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	idle, _ := strconv.Atoi(rows[0][2])
	if idle < 5000 {
		t.Fatalf("idle = %d, want at least 5000 (IDLE override)", idle)
	}
}

func TestXclaimTIMESetsClaimableIdle(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	// TIME pins the delivery clock to a far-past unix-ms, so a later claim with a
	// large min-idle still qualifies where a fresh delivery would not.
	do(t, c, opXclaim, "s", "g", "c2", "0", "1-0", "TIME", "1000")
	got := claimEntries(t, do(t, c, opXclaim, "s", "g", "c3", "100000", "1-0"))
	if len(got) != 1 || got[0].id != "1-0" {
		t.Fatalf("claim after TIME = %v, want [1-0 ...]", got)
	}
}

func TestXclaimRetryCountSetsExact(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	// RETRYCOUNT sets the count outright, never then auto-incremented.
	do(t, c, opXclaim, "s", "g", "c2", "0", "1-0", "RETRYCOUNT", "42")
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if rows[0][3] != "42" {
		t.Fatalf("count = %s, want 42", rows[0][3])
	}
	// Even with JUSTID the explicit count wins.
	do(t, c, opXclaim, "s", "g", "c3", "0", "1-0", "RETRYCOUNT", "7", "JUSTID")
	rows = pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if rows[0][3] != "7" {
		t.Fatalf("count = %s, want 7", rows[0][3])
	}
}

func TestXclaimSelfReclaimBumpsCount(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	// Reclaiming to the current owner keeps the owner and still counts a delivery.
	do(t, c, opXclaim, "s", "g", "c1", "0", "1-0")
	rows := pendingRows(t, do(t, c, opXpending, "s", "g", "-", "+", "10"))
	if len(rows) != 1 || rows[0][1] != "c1" || rows[0][3] != "2" {
		t.Fatalf("pending = %v, want owner c1 count 2", rows)
	}
}

func TestXclaimMixedSkipsNonPending(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">") // 1,2,3 pending
	wantInt(t, do(t, c, opXack, "s", "g", "2-0"), 1)                // 2-0 no longer pending

	got := claimEntries(t, do(t, c, opXclaim, "s", "g", "c2", "0", "1-0", "2-0", "3-0"))
	if len(got) != 2 || got[0].id != "1-0" || got[1].id != "3-0" {
		t.Fatalf("claim = %v, want [1-0 3-0] (2-0 not pending)", got)
	}
	sum := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any)
	if sum[0].(string) != "2" {
		t.Fatalf("total = %v, want 2", sum[0])
	}
}

func TestXclaimErrors(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()
	seedGroup(t, c, 1)

	wantErr(t, do(t, c, opXclaim, "s", "g", "c", "notanint", "1-0"),
		"ERR Invalid min-idle-time argument for XCLAIM")
	// A first non-ID token means no IDs were given.
	wantErr(t, do(t, c, opXclaim, "s", "g", "c", "0", "FORCE"), errInvalidID)
	wantErr(t, do(t, c, opXclaim, "s", "g", "c", "0", "1-0", "IDLE", "notanint"), errNotInt)
	wantErr(t, do(t, c, opXclaim, "s", "g", "c", "0", "1-0", "BOGUS"), "ERR syntax error")
}

func TestXclaimNogroupAndWrongType(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()

	wantErr(t, do(t, c, opXclaim, "nostream", "g", "c", "0", "1-0"),
		nogroupGeneric([]byte("nostream"), []byte("g")))
	seedGroup(t, c, 1)
	wantErr(t, do(t, c, opXclaim, "s", "nogroup", "c", "0", "1-0"),
		nogroupGeneric([]byte("s"), []byte("nogroup")))

	do(t, c, opSet, "str", "x")
	wantErr(t, do(t, c, opXclaim, "str", "g", "c", "0", "1-0"), wrongType)
}
