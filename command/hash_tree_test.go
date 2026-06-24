package command

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
)

// hsetMany fills key with n fields f0..f(n-1) -> v0..v(n-1) in batches, so the
// hash crosses the listpack threshold and promotes to the btree-backed form.
func hsetMany(t *testing.T, r *bufio.Reader, c net.Conn, key string, n int) {
	t.Helper()
	const batch = 50
	for start := 0; start < n; start += batch {
		var b strings.Builder
		b.WriteString("HSET " + key)
		for i := start; i < start+batch && i < n; i++ {
			fmt.Fprintf(&b, " f%d v%d", i, i)
		}
		_ = sendLine(t, r, c, b.String())
	}
}

// TestHashPromotesToHashtable checks a small hash reports listpack and a large
// one flips to hashtable, the externally visible signal that it is btree-backed.
func TestHashPromotesToHashtable(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET small a 1 b 2")
	if got := bulk(t, r, c, "OBJECT ENCODING small"); got != "listpack" {
		t.Fatalf("small encoding = %q want listpack", got)
	}
	hsetMany(t, r, c, "big", 200)
	if got := bulk(t, r, c, "OBJECT ENCODING big"); got != "hashtable" {
		t.Fatalf("big encoding = %q want hashtable", got)
	}
	if got := sendLine(t, r, c, "HLEN big"); got != ":200" {
		t.Fatalf("HLEN big = %q want :200", got)
	}
}

// TestHashLargePointOps exercises every point and bulk read against a promoted
// hash so the btree-backed read paths are covered end to end.
func TestHashLargePointOps(t *testing.T) {
	r, c := startData(t)
	hsetMany(t, r, c, "h", 200)

	if got := bulk(t, r, c, "HGET h f7"); got != "v7" {
		t.Fatalf("HGET f7 = %q want v7", got)
	}
	if got := bulk(t, r, c, "HGET h missing"); got != "<nil>" {
		t.Fatalf("HGET missing = %q want nil", got)
	}
	if got := sendLine(t, r, c, "HEXISTS h f100"); got != ":1" {
		t.Fatalf("HEXISTS f100 = %q want :1", got)
	}
	if got := sendLine(t, r, c, "HEXISTS h nope"); got != ":0" {
		t.Fatalf("HEXISTS nope = %q want :0", got)
	}
	if got := sendLine(t, r, c, "HSTRLEN h f199"); got != ":4" { // "v199"
		t.Fatalf("HSTRLEN f199 = %q want :4", got)
	}
	got := array(t, r, c, "HMGET h f0 absent f199")
	if !equalSlice(got, []string{"v0", "<nil>", "v199"}) {
		t.Fatalf("HMGET = %v", got)
	}

	// Updating an existing field adds 0; adding a new field grows the count.
	if got := sendLine(t, r, c, "HSET h f0 changed f200 v200"); got != ":1" {
		t.Fatalf("HSET update+add = %q want :1", got)
	}
	if got := bulk(t, r, c, "HGET h f0"); got != "changed" {
		t.Fatalf("HGET f0 = %q want changed", got)
	}
	if got := sendLine(t, r, c, "HLEN h"); got != ":201" {
		t.Fatalf("HLEN = %q want :201", got)
	}

	// HSETNX is a no-op on a present field and stays hashtable.
	if got := sendLine(t, r, c, "HSETNX h f0 nope"); got != ":0" {
		t.Fatalf("HSETNX present = %q want :0", got)
	}
	if got := sendLine(t, r, c, "HSETNX h fresh yes"); got != ":1" {
		t.Fatalf("HSETNX new = %q want :1", got)
	}

	// HKEYS / HVALS / HGETALL return the full set.
	if got := len(array(t, r, c, "HKEYS h")); got != 202 {
		t.Fatalf("HKEYS count = %d want 202", got)
	}
	if got := len(array(t, r, c, "HVALS h")); got != 202 {
		t.Fatalf("HVALS count = %d want 202", got)
	}
	if got := len(array(t, r, c, "HGETALL h")); got != 404 {
		t.Fatalf("HGETALL flat len = %d want 404", got)
	}
}

// TestHashLargeHDelEmptiesKey checks HDEL on a promoted hash removes fields and
// deletes the key when the last field goes, the same contract as a small hash.
func TestHashLargeHDelEmptiesKey(t *testing.T) {
	r, c := startData(t)
	hsetMany(t, r, c, "h", 130)

	if got := sendLine(t, r, c, "HDEL h f0 f1 absent"); got != ":2" {
		t.Fatalf("HDEL = %q want :2", got)
	}
	if got := sendLine(t, r, c, "HLEN h"); got != ":128" {
		t.Fatalf("HLEN = %q want :128", got)
	}

	// Remove every remaining field; the key must disappear.
	var b strings.Builder
	b.WriteString("HDEL h")
	for i := 2; i < 130; i++ {
		fmt.Fprintf(&b, " f%d", i)
	}
	if got := sendLine(t, r, c, b.String()); got != ":128" {
		t.Fatalf("HDEL rest = %q want :128", got)
	}
	if got := sendLine(t, r, c, "EXISTS h"); got != ":0" {
		t.Fatalf("EXISTS after empty = %q want :0", got)
	}
	if got := sendLine(t, r, c, "TYPE h"); got != "+none" {
		t.Fatalf("TYPE after empty = %q want +none", got)
	}
}

// TestHashLargeOverwriteWithString checks replacing a promoted hash with a plain
// SET flips the type and tears the sub-tree down (no leak, integrity stays clean).
func TestHashLargeOverwriteWithString(t *testing.T) {
	r, c := startData(t)
	hsetMany(t, r, c, "h", 200)
	if got := sendLine(t, r, c, "SET h plainstring"); got != "+OK" {
		t.Fatalf("SET over hash = %q", got)
	}
	if got := sendLine(t, r, c, "TYPE h"); got != "+string" {
		t.Fatalf("TYPE = %q want +string", got)
	}
	if got := bulk(t, r, c, "GET h"); got != "plainstring" {
		t.Fatalf("GET = %q want plainstring", got)
	}
}

// TestHashLargeDumpRestore round-trips a promoted hash through DUMP/RESTORE and
// checks the field set survives and the encoding is still hashtable.
func TestHashLargeDumpRestore(t *testing.T) {
	r, c := startData(t)
	hsetMany(t, r, c, "h", 200)
	_ = dumpRestoreRoundTrip(t, r, c, "h")
	if got := sendLine(t, r, c, "HLEN h"); got != ":200" {
		t.Fatalf("HLEN after restore = %q want :200", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING h"); got != "hashtable" {
		t.Fatalf("encoding after restore = %q want hashtable", got)
	}
	if got := bulk(t, r, c, "HGET h f123"); got != "v123" {
		t.Fatalf("HGET f123 after restore = %q want v123", got)
	}
}

// TestHashLargeDebugReload checks a promoted hash survives DEBUG RELOAD, which
// serializes the whole keyspace to RDB and reads it back.
func TestHashLargeDebugReload(t *testing.T) {
	r, c := startData(t)
	hsetMany(t, r, c, "h", 200)
	if got := sendLine(t, r, c, "DEBUG RELOAD"); got != "+OK" {
		t.Fatalf("DEBUG RELOAD = %q", got)
	}
	if got := sendLine(t, r, c, "HLEN h"); got != ":200" {
		t.Fatalf("HLEN after reload = %q want :200", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING h"); got != "hashtable" {
		t.Fatalf("encoding after reload = %q want hashtable", got)
	}
	if got := bulk(t, r, c, "HGET h f50"); got != "v50" {
		t.Fatalf("HGET f50 after reload = %q want v50", got)
	}
}
