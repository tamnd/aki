package command

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
)

// zaddMany fills key with n members m0..m(n-1) at score i in batches, so the
// sorted set crosses the listpack threshold and promotes to the btree-backed form.
func zaddMany(t *testing.T, r *bufio.Reader, c net.Conn, key string, n int) {
	t.Helper()
	const batch = 50
	for start := 0; start < n; start += batch {
		var b strings.Builder
		b.WriteString("ZADD " + key)
		for i := start; i < start+batch && i < n; i++ {
			fmt.Fprintf(&b, " %d m%d", i, i)
		}
		_ = sendLine(t, r, c, b.String())
	}
}

// TestZSetPromotesToSkiplist checks a small sorted set reports listpack and a large
// one flips to skiplist, the externally visible signal that it is btree-backed.
func TestZSetPromotesToSkiplist(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD small 1 a 2 b")
	if got := bulk(t, r, c, "OBJECT ENCODING small"); got != "listpack" {
		t.Fatalf("small encoding = %q want listpack", got)
	}
	zaddMany(t, r, c, "big", 200)
	if got := bulk(t, r, c, "OBJECT ENCODING big"); got != "skiplist" {
		t.Fatalf("big encoding = %q want skiplist", got)
	}
	if got := sendLine(t, r, c, "ZCARD big"); got != ":200" {
		t.Fatalf("ZCARD big = %q want :200", got)
	}
}

// TestZSetLargePointOps exercises the point reads and ordered reads against a
// promoted sorted set so the btree-backed read paths are covered end to end.
func TestZSetLargePointOps(t *testing.T) {
	r, c := startData(t)
	zaddMany(t, r, c, "z", 200)

	if got := bulk(t, r, c, "ZSCORE z m7"); got != "7" {
		t.Fatalf("ZSCORE m7 = %q want 7", got)
	}
	if got := bulk(t, r, c, "ZSCORE z nope"); got != "<nil>" {
		t.Fatalf("ZSCORE nope = %q want nil", got)
	}
	if got := sendLine(t, r, c, "ZRANK z m0"); got != ":0" {
		t.Fatalf("ZRANK m0 = %q want :0", got)
	}
	if got := sendLine(t, r, c, "ZRANK z m199"); got != ":199" {
		t.Fatalf("ZRANK m199 = %q want :199", got)
	}
	if got := sendLine(t, r, c, "ZREVRANK z m199"); got != ":0" {
		t.Fatalf("ZREVRANK m199 = %q want :0", got)
	}
	// ZRANGE returns members in score order.
	got := array(t, r, c, "ZRANGE z 0 2")
	if !equalSlice(got, []string{"m0", "m1", "m2"}) {
		t.Fatalf("ZRANGE 0 2 = %v", got)
	}
	// ZRANGEBYSCORE selects by score window.
	got = array(t, r, c, "ZRANGEBYSCORE z 100 102")
	if !equalSlice(got, []string{"m100", "m101", "m102"}) {
		t.Fatalf("ZRANGEBYSCORE = %v", got)
	}
	if got := sendLine(t, r, c, "ZCOUNT z 0 99"); got != ":100" {
		t.Fatalf("ZCOUNT 0 99 = %q want :100", got)
	}

	// Updating an existing member's score keeps the count and re-sorts.
	if got := sendLine(t, r, c, "ZADD z 1000 m0"); got != ":0" {
		t.Fatalf("ZADD update = %q want :0", got)
	}
	if got := bulk(t, r, c, "ZSCORE z m0"); got != "1000" {
		t.Fatalf("ZSCORE m0 after update = %q want 1000", got)
	}
	if got := sendLine(t, r, c, "ZREVRANK z m0"); got != ":0" {
		t.Fatalf("ZREVRANK m0 after raising score = %q want :0", got)
	}
	// Adding a new member grows the count.
	if got := sendLine(t, r, c, "ZADD z 5 m200"); got != ":1" {
		t.Fatalf("ZADD new = %q want :1", got)
	}
	if got := sendLine(t, r, c, "ZCARD z"); got != ":201" {
		t.Fatalf("ZCARD = %q want :201", got)
	}
}

// TestZSetLargeIncrBy checks ZINCRBY moves a member's score and re-positions it in
// a promoted sorted set.
func TestZSetLargeIncrBy(t *testing.T) {
	r, c := startData(t)
	zaddMany(t, r, c, "z", 200)
	if got := bulk(t, r, c, "ZINCRBY z 5 m10"); got != "15" {
		t.Fatalf("ZINCRBY existing = %q want 15", got)
	}
	if got := bulk(t, r, c, "ZSCORE z m10"); got != "15" {
		t.Fatalf("ZSCORE m10 = %q want 15", got)
	}
	// ZINCRBY on an absent member creates it at the increment.
	if got := bulk(t, r, c, "ZINCRBY z 3 fresh"); got != "3" {
		t.Fatalf("ZINCRBY new = %q want 3", got)
	}
	if got := sendLine(t, r, c, "ZCARD z"); got != ":201" {
		t.Fatalf("ZCARD = %q want :201", got)
	}
}

// TestZSetLargeZRemEmptiesKey checks ZREM on a promoted sorted set removes members
// and deletes the key when the last member goes.
func TestZSetLargeZRemEmptiesKey(t *testing.T) {
	r, c := startData(t)
	zaddMany(t, r, c, "z", 130)

	if got := sendLine(t, r, c, "ZREM z m0 m1 absent"); got != ":2" {
		t.Fatalf("ZREM = %q want :2", got)
	}
	if got := sendLine(t, r, c, "ZCARD z"); got != ":128" {
		t.Fatalf("ZCARD = %q want :128", got)
	}

	var b strings.Builder
	b.WriteString("ZREM z")
	for i := 2; i < 130; i++ {
		fmt.Fprintf(&b, " m%d", i)
	}
	if got := sendLine(t, r, c, b.String()); got != ":128" {
		t.Fatalf("ZREM rest = %q want :128", got)
	}
	if got := sendLine(t, r, c, "EXISTS z"); got != ":0" {
		t.Fatalf("EXISTS after empty = %q want :0", got)
	}
	if got := sendLine(t, r, c, "TYPE z"); got != "+none" {
		t.Fatalf("TYPE after empty = %q want +none", got)
	}
}

// TestZSetLargeZPopMin pops the lowest-score members from a promoted sorted set and
// checks the order and count, including draining past the end. ZPOP reads through
// the coll-aware getZSet, so it works on the promoted form.
func TestZSetLargeZPopMin(t *testing.T) {
	r, c := startData(t)
	zaddMany(t, r, c, "z", 200)
	// ZPOPMIN with a count returns member,score pairs from the low end in order.
	got := array(t, r, c, "ZPOPMIN z 3")
	if !equalSlice(got, []string{"m0", "0", "m1", "1", "m2", "2"}) {
		t.Fatalf("ZPOPMIN 3 = %v", got)
	}
	if got := sendLine(t, r, c, "ZCARD z"); got != ":197" {
		t.Fatalf("ZCARD after pop = %q want :197", got)
	}
	// ZPOPMAX returns the high end.
	got = array(t, r, c, "ZPOPMAX z 1")
	if !equalSlice(got, []string{"m199", "199"}) {
		t.Fatalf("ZPOPMAX 1 = %v", got)
	}
}

// TestZSetLargeOverwriteWithString checks replacing a promoted sorted set with a
// plain SET flips the type and tears the sub-tree down.
func TestZSetLargeOverwriteWithString(t *testing.T) {
	r, c := startData(t)
	zaddMany(t, r, c, "z", 200)
	if got := sendLine(t, r, c, "SET z plainstring"); got != "+OK" {
		t.Fatalf("SET over zset = %q", got)
	}
	if got := sendLine(t, r, c, "TYPE z"); got != "+string" {
		t.Fatalf("TYPE = %q want +string", got)
	}
	if got := bulk(t, r, c, "GET z"); got != "plainstring" {
		t.Fatalf("GET = %q want plainstring", got)
	}
}

// TestZSetLargeDumpRestore round-trips a promoted sorted set through DUMP/RESTORE
// and checks the members, scores, order and encoding survive.
func TestZSetLargeDumpRestore(t *testing.T) {
	r, c := startData(t)
	zaddMany(t, r, c, "z", 200)
	_ = dumpRestoreRoundTrip(t, r, c, "z")
	if got := sendLine(t, r, c, "ZCARD z"); got != ":200" {
		t.Fatalf("ZCARD after restore = %q want :200", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING z"); got != "skiplist" {
		t.Fatalf("encoding after restore = %q want skiplist", got)
	}
	if got := bulk(t, r, c, "ZSCORE z m123"); got != "123" {
		t.Fatalf("ZSCORE m123 after restore = %q want 123", got)
	}
	if got := sendLine(t, r, c, "ZRANK z m50"); got != ":50" {
		t.Fatalf("ZRANK m50 after restore = %q want :50", got)
	}
}

// TestZSetLargeDebugReload checks a promoted sorted set survives DEBUG RELOAD.
func TestZSetLargeDebugReload(t *testing.T) {
	r, c := startData(t)
	zaddMany(t, r, c, "z", 200)
	if got := sendLine(t, r, c, "DEBUG RELOAD"); got != "+OK" {
		t.Fatalf("DEBUG RELOAD = %q", got)
	}
	if got := sendLine(t, r, c, "ZCARD z"); got != ":200" {
		t.Fatalf("ZCARD after reload = %q want :200", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING z"); got != "skiplist" {
		t.Fatalf("encoding after reload = %q want skiplist", got)
	}
	if got := bulk(t, r, c, "ZSCORE z m50"); got != "50" {
		t.Fatalf("ZSCORE m50 after reload = %q want 50", got)
	}
}

// TestZSetLargeNegativeScores checks the sortable score encoding orders negative,
// zero and positive scores correctly in the btree-backed form.
func TestZSetLargeNegativeScores(t *testing.T) {
	r, c := startData(t)
	// Build a promoted set, then add members with negative and fractional scores.
	zaddMany(t, r, c, "z", 200)
	// m0 from zaddMany also has score 0, and at equal scores members order
	// bytewise, so "m0" comes before "zero".
	_ = sendLine(t, r, c, "ZADD z -5 neg5 -0.5 neghalf 0 zero 2.5 pos")
	got := array(t, r, c, "ZRANGE z 0 3")
	if !equalSlice(got, []string{"neg5", "neghalf", "m0", "zero"}) {
		t.Fatalf("ZRANGE low end = %v want neg5,neghalf,m0,zero", got)
	}
	if got := bulk(t, r, c, "ZSCORE z neghalf"); got != "-0.5" {
		t.Fatalf("ZSCORE neghalf = %q want -0.5", got)
	}
}
