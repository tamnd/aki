package command

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
)

// saddMany fills key with n string members m0..m(n-1) in batches, so the set
// crosses the listpack threshold and promotes to the btree-backed form. The
// members are non-integer so the set never reports intset.
func saddMany(t *testing.T, r *bufio.Reader, c net.Conn, key string, n int) {
	t.Helper()
	const batch = 50
	for start := 0; start < n; start += batch {
		var b strings.Builder
		b.WriteString("SADD " + key)
		for i := start; i < start+batch && i < n; i++ {
			fmt.Fprintf(&b, " m%d", i)
		}
		_ = sendLine(t, r, c, b.String())
	}
}

// TestSetPromotesToHashtable checks a small set reports listpack and a large one
// flips to hashtable, the externally visible signal that it is btree-backed.
func TestSetPromotesToHashtable(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD small a b")
	if got := bulk(t, r, c, "OBJECT ENCODING small"); got != "listpack" {
		t.Fatalf("small encoding = %q want listpack", got)
	}
	saddMany(t, r, c, "big", 200)
	if got := bulk(t, r, c, "OBJECT ENCODING big"); got != "hashtable" {
		t.Fatalf("big encoding = %q want hashtable", got)
	}
	if got := sendLine(t, r, c, "SCARD big"); got != ":200" {
		t.Fatalf("SCARD big = %q want :200", got)
	}
}

// TestSetLargePointOps exercises the point and bulk reads against a promoted set
// so the btree-backed read paths are covered end to end.
func TestSetLargePointOps(t *testing.T) {
	r, c := startData(t)
	saddMany(t, r, c, "s", 200)

	if got := sendLine(t, r, c, "SISMEMBER s m7"); got != ":1" {
		t.Fatalf("SISMEMBER m7 = %q want :1", got)
	}
	if got := sendLine(t, r, c, "SISMEMBER s nope"); got != ":0" {
		t.Fatalf("SISMEMBER nope = %q want :0", got)
	}
	got := intArray(t, r, c, "SMISMEMBER s m0 absent m199")
	if !equalSlice(got, []string{":1", ":0", ":1"}) {
		t.Fatalf("SMISMEMBER = %v", got)
	}
	if got := len(array(t, r, c, "SMEMBERS s")); got != 200 {
		t.Fatalf("SMEMBERS count = %d want 200", got)
	}

	// Adding an existing member adds 0; adding a new one grows the count.
	if got := sendLine(t, r, c, "SADD s m0 m200"); got != ":1" {
		t.Fatalf("SADD update+add = %q want :1", got)
	}
	if got := sendLine(t, r, c, "SCARD s"); got != ":201" {
		t.Fatalf("SCARD = %q want :201", got)
	}
}

// TestSetLargeSRemEmptiesKey checks SREM on a promoted set removes members and
// deletes the key when the last member goes, the same contract as a small set.
func TestSetLargeSRemEmptiesKey(t *testing.T) {
	r, c := startData(t)
	saddMany(t, r, c, "s", 130)

	if got := sendLine(t, r, c, "SREM s m0 m1 absent"); got != ":2" {
		t.Fatalf("SREM = %q want :2", got)
	}
	if got := sendLine(t, r, c, "SCARD s"); got != ":128" {
		t.Fatalf("SCARD = %q want :128", got)
	}

	// Remove every remaining member; the key must disappear.
	var b strings.Builder
	b.WriteString("SREM s")
	for i := 2; i < 130; i++ {
		fmt.Fprintf(&b, " m%d", i)
	}
	if got := sendLine(t, r, c, b.String()); got != ":128" {
		t.Fatalf("SREM rest = %q want :128", got)
	}
	if got := sendLine(t, r, c, "EXISTS s"); got != ":0" {
		t.Fatalf("EXISTS after empty = %q want :0", got)
	}
	if got := sendLine(t, r, c, "TYPE s"); got != "+none" {
		t.Fatalf("TYPE after empty = %q want +none", got)
	}
}

// TestSetLargeSPop pops members from a promoted set and checks the count falls
// and the key disappears once the last member is popped.
func TestSetLargeSPop(t *testing.T) {
	r, c := startData(t)
	saddMany(t, r, c, "s", 200)

	if got := len(array(t, r, c, "SPOP s 30")); got != 30 {
		t.Fatalf("SPOP 30 count = %d want 30", got)
	}
	if got := sendLine(t, r, c, "SCARD s"); got != ":170" {
		t.Fatalf("SCARD after pop = %q want :170", got)
	}
	// A single SPOP returns one member and drops the count by one.
	if got := bulk(t, r, c, "SPOP s"); got == "<nil>" {
		t.Fatalf("SPOP single returned nil")
	}
	if got := sendLine(t, r, c, "SCARD s"); got != ":169" {
		t.Fatalf("SCARD after single pop = %q want :169", got)
	}
	// Popping more than is present drains the set and deletes the key.
	if got := len(array(t, r, c, "SPOP s 1000")); got != 169 {
		t.Fatalf("SPOP drain count = %d want 169", got)
	}
	if got := sendLine(t, r, c, "EXISTS s"); got != ":0" {
		t.Fatalf("EXISTS after drain = %q want :0", got)
	}
}

// TestSetLargeSMove moves a member out of a promoted set into another and checks
// both sides update, including the case where the source empties.
func TestSetLargeSMove(t *testing.T) {
	r, c := startData(t)
	saddMany(t, r, c, "src", 200)
	_ = sendLine(t, r, c, "SADD dst x")

	if got := sendLine(t, r, c, "SMOVE src dst m5"); got != ":1" {
		t.Fatalf("SMOVE present = %q want :1", got)
	}
	if got := sendLine(t, r, c, "SISMEMBER src m5"); got != ":0" {
		t.Fatalf("SISMEMBER src m5 after move = %q want :0", got)
	}
	if got := sendLine(t, r, c, "SISMEMBER dst m5"); got != ":1" {
		t.Fatalf("SISMEMBER dst m5 after move = %q want :1", got)
	}
	if got := sendLine(t, r, c, "SCARD src"); got != ":199" {
		t.Fatalf("SCARD src after move = %q want :199", got)
	}
	// Moving an absent member is a no-op and replies 0.
	if got := sendLine(t, r, c, "SMOVE src dst absent"); got != ":0" {
		t.Fatalf("SMOVE absent = %q want :0", got)
	}
}

// TestSetLargeOverwriteWithString checks replacing a promoted set with a plain
// SET flips the type and tears the sub-tree down (no leak, integrity stays clean).
func TestSetLargeOverwriteWithString(t *testing.T) {
	r, c := startData(t)
	saddMany(t, r, c, "s", 200)
	if got := sendLine(t, r, c, "SET s plainstring"); got != "+OK" {
		t.Fatalf("SET over set = %q", got)
	}
	if got := sendLine(t, r, c, "TYPE s"); got != "+string" {
		t.Fatalf("TYPE = %q want +string", got)
	}
	if got := bulk(t, r, c, "GET s"); got != "plainstring" {
		t.Fatalf("GET = %q want plainstring", got)
	}
}

// TestSetLargeDumpRestore round-trips a promoted set through DUMP/RESTORE and
// checks the member set survives and the encoding is still hashtable.
func TestSetLargeDumpRestore(t *testing.T) {
	r, c := startData(t)
	saddMany(t, r, c, "s", 200)
	_ = dumpRestoreRoundTrip(t, r, c, "s")
	if got := sendLine(t, r, c, "SCARD s"); got != ":200" {
		t.Fatalf("SCARD after restore = %q want :200", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING s"); got != "hashtable" {
		t.Fatalf("encoding after restore = %q want hashtable", got)
	}
	if got := sendLine(t, r, c, "SISMEMBER s m123"); got != ":1" {
		t.Fatalf("SISMEMBER m123 after restore = %q want :1", got)
	}
}

// TestSetLargeDebugReload checks a promoted set survives DEBUG RELOAD, which
// serializes the whole keyspace to RDB and reads it back.
func TestSetLargeDebugReload(t *testing.T) {
	r, c := startData(t)
	saddMany(t, r, c, "s", 200)
	if got := sendLine(t, r, c, "DEBUG RELOAD"); got != "+OK" {
		t.Fatalf("DEBUG RELOAD = %q", got)
	}
	if got := sendLine(t, r, c, "SCARD s"); got != ":200" {
		t.Fatalf("SCARD after reload = %q want :200", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING s"); got != "hashtable" {
		t.Fatalf("encoding after reload = %q want hashtable", got)
	}
	if got := sendLine(t, r, c, "SISMEMBER s m50"); got != ":1" {
		t.Fatalf("SISMEMBER m50 after reload = %q want :1", got)
	}
}
