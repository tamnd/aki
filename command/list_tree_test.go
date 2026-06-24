package command

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
)

// rpushMany fills key with n elements e0..e(n-1) at the tail in batches, so the
// list crosses the listpack threshold and promotes to the btree-backed form.
func rpushMany(t *testing.T, r *bufio.Reader, c net.Conn, key string, n int) {
	t.Helper()
	const batch = 50
	for start := 0; start < n; start += batch {
		var b strings.Builder
		b.WriteString("RPUSH " + key)
		for i := start; i < start+batch && i < n; i++ {
			fmt.Fprintf(&b, " e%d", i)
		}
		_ = sendLine(t, r, c, b.String())
	}
}

// TestListPromotesToQuicklist checks a small list reports listpack and a large one
// flips to quicklist, the externally visible signal that it is btree-backed.
func TestListPromotesToQuicklist(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH small a b c")
	if got := bulk(t, r, c, "OBJECT ENCODING small"); got != "listpack" {
		t.Fatalf("small encoding = %q want listpack", got)
	}
	rpushMany(t, r, c, "big", 200)
	if got := bulk(t, r, c, "OBJECT ENCODING big"); got != "quicklist" {
		t.Fatalf("big encoding = %q want quicklist", got)
	}
	if got := sendLine(t, r, c, "LLEN big"); got != ":200" {
		t.Fatalf("LLEN big = %q want :200", got)
	}
}

// TestListLargePushPopOrder exercises the window push and pop ops against a
// promoted list, checking element order survives left and right moves.
func TestListLargePushPopOrder(t *testing.T) {
	r, c := startData(t)
	rpushMany(t, r, c, "l", 200)

	// LPUSH onto a promoted list keeps the reversed-run order at the head.
	if got := sendLine(t, r, c, "LPUSH l x y z"); got != ":203" {
		t.Fatalf("LPUSH = %q want :203", got)
	}
	// The head is now z, y, x, then the original e0.
	got := array(t, r, c, "LRANGE l 0 3")
	if !equalSlice(got, []string{"z", "y", "x", "e0"}) {
		t.Fatalf("LRANGE head = %v want z,y,x,e0", got)
	}
	// LINDEX resolves a point at both ends.
	if got := bulk(t, r, c, "LINDEX l 0"); got != "z" {
		t.Fatalf("LINDEX 0 = %q want z", got)
	}
	if got := bulk(t, r, c, "LINDEX l -1"); got != "e199" {
		t.Fatalf("LINDEX -1 = %q want e199", got)
	}

	// LPOP and RPOP trim the window from each end.
	if got := bulk(t, r, c, "LPOP l"); got != "z" {
		t.Fatalf("LPOP = %q want z", got)
	}
	if got := bulk(t, r, c, "RPOP l"); got != "e199" {
		t.Fatalf("RPOP = %q want e199", got)
	}
	got = array(t, r, c, "LPOP l 2")
	if !equalSlice(got, []string{"y", "x"}) {
		t.Fatalf("LPOP 2 = %v want y,x", got)
	}
	if got := sendLine(t, r, c, "LLEN l"); got != ":199" {
		t.Fatalf("LLEN = %q want :199", got)
	}
}

// TestListLargeLSet checks LSET writes a single position in a promoted list and
// reports an out-of-range index as an error without changing the list.
func TestListLargeLSet(t *testing.T) {
	r, c := startData(t)
	rpushMany(t, r, c, "l", 200)
	if got := sendLine(t, r, c, "LSET l 5 changed"); got != "+OK" {
		t.Fatalf("LSET = %q want OK", got)
	}
	if got := bulk(t, r, c, "LINDEX l 5"); got != "changed" {
		t.Fatalf("LINDEX 5 = %q want changed", got)
	}
	if got := bulk(t, r, c, "LINDEX l -1"); got != "e199" {
		t.Fatalf("LINDEX -1 = %q want e199", got)
	}
	if got := sendLine(t, r, c, "LSET l 999 nope"); got != "-ERR index out of range" {
		t.Fatalf("LSET oob = %q want index out of range", got)
	}
}

// TestListLargeLRangeNegatives checks the window cursor walk applies the Redis
// negative-index and clamp rules on a promoted list.
func TestListLargeLRangeNegatives(t *testing.T) {
	r, c := startData(t)
	rpushMany(t, r, c, "l", 200)
	got := array(t, r, c, "LRANGE l -3 -1")
	if !equalSlice(got, []string{"e197", "e198", "e199"}) {
		t.Fatalf("LRANGE -3 -1 = %v want e197,e198,e199", got)
	}
	got = array(t, r, c, "LRANGE l 0 -1")
	if len(got) != 200 {
		t.Fatalf("LRANGE 0 -1 len = %d want 200", len(got))
	}
	if got[0] != "e0" || got[199] != "e199" {
		t.Fatalf("LRANGE 0 -1 ends = %q..%q", got[0], got[199])
	}
	// A start past the end yields nothing.
	got = array(t, r, c, "LRANGE l 500 600")
	if len(got) != 0 {
		t.Fatalf("LRANGE past end = %v want empty", got)
	}
}

// TestListLargeDrainDeletesKey pops every element from a promoted list and checks
// the key and its sub-tree are torn down when the last element goes.
func TestListLargeDrainDeletesKey(t *testing.T) {
	r, c := startData(t)
	rpushMany(t, r, c, "l", 130)
	got := array(t, r, c, "LPOP l 130")
	if len(got) != 130 || got[0] != "e0" || got[129] != "e129" {
		t.Fatalf("LPOP 130 unexpected: len=%d", len(got))
	}
	if got := sendLine(t, r, c, "EXISTS l"); got != ":0" {
		t.Fatalf("EXISTS after drain = %q want :0", got)
	}
	if got := sendLine(t, r, c, "TYPE l"); got != "+none" {
		t.Fatalf("TYPE after drain = %q want +none", got)
	}
}

// TestListLargeLInsertLRem checks the bulk modify commands work on a promoted
// list. They read through the coll-aware getList, so they see every element.
func TestListLargeLInsertLRem(t *testing.T) {
	r, c := startData(t)
	rpushMany(t, r, c, "l", 200)
	// LINSERT before a pivot grows the list by one.
	if got := sendLine(t, r, c, "LINSERT l BEFORE e100 inserted"); got != ":201" {
		t.Fatalf("LINSERT = %q want :201", got)
	}
	if got := bulk(t, r, c, "LINDEX l 100"); got != "inserted" {
		t.Fatalf("LINDEX 100 = %q want inserted", got)
	}
	if got := bulk(t, r, c, "LINDEX l 101"); got != "e100" {
		t.Fatalf("LINDEX 101 = %q want e100", got)
	}
	// LREM removes a known element.
	if got := sendLine(t, r, c, "LREM l 0 inserted"); got != ":1" {
		t.Fatalf("LREM = %q want :1", got)
	}
	if got := sendLine(t, r, c, "LLEN l"); got != ":200" {
		t.Fatalf("LLEN after lrem = %q want :200", got)
	}
}

// TestListLargeLTrim checks LTRIM keeps the requested window of a promoted list.
func TestListLargeLTrim(t *testing.T) {
	r, c := startData(t)
	rpushMany(t, r, c, "l", 200)
	if got := sendLine(t, r, c, "LTRIM l 10 19"); got != "+OK" {
		t.Fatalf("LTRIM = %q want OK", got)
	}
	if got := sendLine(t, r, c, "LLEN l"); got != ":10" {
		t.Fatalf("LLEN after trim = %q want :10", got)
	}
	got := array(t, r, c, "LRANGE l 0 -1")
	if !equalSlice(got, []string{"e10", "e11", "e12", "e13", "e14", "e15", "e16", "e17", "e18", "e19"}) {
		t.Fatalf("LRANGE after trim = %v", got)
	}
}

// TestListLargeLPos checks LPOS finds an element's index in a promoted list.
func TestListLargeLPos(t *testing.T) {
	r, c := startData(t)
	rpushMany(t, r, c, "l", 200)
	if got := sendLine(t, r, c, "LPOS l e150"); got != ":150" {
		t.Fatalf("LPOS = %q want :150", got)
	}
	if got := sendLine(t, r, c, "LPOS l missing"); got != "$-1" {
		t.Fatalf("LPOS missing = %q want nil", got)
	}
}

// TestListLargeLMove moves an element across two promoted lists in one step.
func TestListLargeLMove(t *testing.T) {
	r, c := startData(t)
	rpushMany(t, r, c, "src", 200)
	rpushMany(t, r, c, "dst", 200)
	if got := bulk(t, r, c, "LMOVE src dst LEFT RIGHT"); got != "e0" {
		t.Fatalf("LMOVE = %q want e0", got)
	}
	if got := sendLine(t, r, c, "LLEN src"); got != ":199" {
		t.Fatalf("LLEN src = %q want :199", got)
	}
	if got := bulk(t, r, c, "LINDEX dst -1"); got != "e0" {
		t.Fatalf("dst tail = %q want e0", got)
	}
	if got := bulk(t, r, c, "LINDEX src 0"); got != "e1" {
		t.Fatalf("src head = %q want e1", got)
	}
}

// TestListLargeOverwriteWithString checks replacing a promoted list with a plain
// SET flips the type and tears the sub-tree down.
func TestListLargeOverwriteWithString(t *testing.T) {
	r, c := startData(t)
	rpushMany(t, r, c, "l", 200)
	if got := sendLine(t, r, c, "SET l plainstring"); got != "+OK" {
		t.Fatalf("SET over list = %q", got)
	}
	if got := sendLine(t, r, c, "TYPE l"); got != "+string" {
		t.Fatalf("TYPE = %q want +string", got)
	}
	if got := bulk(t, r, c, "GET l"); got != "plainstring" {
		t.Fatalf("GET = %q want plainstring", got)
	}
}

// TestListLargeDumpRestore round-trips a promoted list through DUMP/RESTORE and
// checks the elements, order and encoding survive.
func TestListLargeDumpRestore(t *testing.T) {
	r, c := startData(t)
	rpushMany(t, r, c, "l", 200)
	_ = dumpRestoreRoundTrip(t, r, c, "l")
	if got := sendLine(t, r, c, "LLEN l"); got != ":200" {
		t.Fatalf("LLEN after restore = %q want :200", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "quicklist" {
		t.Fatalf("encoding after restore = %q want quicklist", got)
	}
	if got := bulk(t, r, c, "LINDEX l 123"); got != "e123" {
		t.Fatalf("LINDEX 123 after restore = %q want e123", got)
	}
}

// TestListLargeDebugReload checks a promoted list survives DEBUG RELOAD.
func TestListLargeDebugReload(t *testing.T) {
	r, c := startData(t)
	rpushMany(t, r, c, "l", 200)
	if got := sendLine(t, r, c, "DEBUG RELOAD"); got != "+OK" {
		t.Fatalf("DEBUG RELOAD = %q", got)
	}
	if got := sendLine(t, r, c, "LLEN l"); got != ":200" {
		t.Fatalf("LLEN after reload = %q want :200", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "quicklist" {
		t.Fatalf("encoding after reload = %q want quicklist", got)
	}
	if got := bulk(t, r, c, "LINDEX l 50"); got != "e50" {
		t.Fatalf("LINDEX 50 after reload = %q want e50", got)
	}
}

// TestListLargeLPushHeadGrowth drives the head deep into negative positions with
// repeated LPUSH and checks the order encoding still walks correctly. The window
// head runs negative once enough left pushes happen, so this exercises the
// sign-flipped position key directly.
func TestListLargeLPushHeadGrowth(t *testing.T) {
	r, c := startData(t)
	// Promote first with a tail run, then push many onto the head one at a time.
	rpushMany(t, r, c, "l", 130)
	for i := 0; i < 60; i++ {
		_ = sendLine(t, r, c, "LPUSH l h"+strconv.Itoa(i))
	}
	if got := sendLine(t, r, c, "LLEN l"); got != ":190" {
		t.Fatalf("LLEN = %q want :190", got)
	}
	// The last head push is at index 0, the first tail element follows the head run.
	if got := bulk(t, r, c, "LINDEX l 0"); got != "h59" {
		t.Fatalf("LINDEX 0 = %q want h59", got)
	}
	if got := bulk(t, r, c, "LINDEX l 59"); got != "h0" {
		t.Fatalf("LINDEX 59 = %q want h0", got)
	}
	if got := bulk(t, r, c, "LINDEX l 60"); got != "e0" {
		t.Fatalf("LINDEX 60 = %q want e0", got)
	}
}
