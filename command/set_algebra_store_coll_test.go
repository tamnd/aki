package command

import (
	"fmt"
	"sort"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestSetStoreCollIsBounded guards the STORE forms against the loadSets materialize
// trap on coll-form sources. The old path cloned every member of every source onto
// the heap, then built the whole result before a single byte reached the
// destination, so SINTERSTORE or SDIFFSTORE of a small set against a set far larger
// than RAM dragged the huge set through memory in full.
//
// The witness keeps the driver small and the other source huge with a small result,
// the OOM shape: the huge source is the one that must not be cloned, and a small
// result stays a buffered blob so the write itself stays cheap too. The bounded
// path point-probes the huge source, so the allocation count tracks the small
// driver, not the huge probe target.
func TestSetStoreCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const big = 4000
	const small = 40
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	member := func(i int) []byte {
		return []byte(fmt.Sprintf("m:%08d", i) + string(pad))
	}
	for i := range big {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SADD"), []byte("big"), member(i)})
		if i < small {
			conn.ResetOut()
			d.Handle(conn, [][]byte{[]byte("SADD"), []byte("small"), member(i)})
		}
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("big")})
	if got := string(conn.OutBytes()); got != "$9\r\nhashtable\r\n" {
		t.Fatalf("big not in coll form: OBJECT ENCODING = %q", got)
	}

	// SINTERSTORE drives the smaller source and point-probes big; the result is
	// small (= small) and stays a buffered blob. SDIFFSTORE small\big is empty and
	// just deletes the destination. Both must track the small driver, never clone big.
	for _, cmd := range [][][]byte{
		{[]byte("SINTERSTORE"), []byte("dst"), []byte("small"), []byte("big")},
		{[]byte("SDIFFSTORE"), []byte("dst"), []byte("small"), []byte("big")},
	} {
		c := cmd
		allocs := testing.AllocsPerRun(10, func() {
			conn.ResetOut()
			d.Handle(conn, c)
		})
		if allocs > 700 {
			t.Fatalf("%s over a %d-member coll source allocated %.0f objects per run; "+
				"the bounded path should track the small driver, not clone the huge source", c[0], big, allocs)
		}
	}
}

// TestSetStoreCollMatchesNaive checks the streamed STORE results match a naive
// in-memory computation over coll-form sources, including the cardinality reply, the
// stored members, the empty-result delete, and the WRONGTYPE source.
func TestSetStoreCollMatchesNaive(t *testing.T) {
	r, c := startData(t)

	build := func(name string, lo, hi int) map[string]bool {
		set := map[string]bool{}
		for i := lo; i < hi; i++ {
			m := fmt.Sprintf("m:%06d", i)
			set[m] = true
			_ = sendLine(t, r, c, fmt.Sprintf("SADD %s %s", name, m))
		}
		if enc := bulk(t, r, c, "OBJECT ENCODING "+name); enc != "hashtable" {
			t.Fatalf("set %s encoding = %q want hashtable", name, enc)
		}
		return set
	}
	sa := build("a", 0, 600)
	sb := build("b", 300, 900)
	sc := build("c", 500, 1100)

	sortedKeys := func(m map[string]bool) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	// store runs the STORE command, checks the cardinality reply, then reads the
	// destination back with SMEMBERS and checks it equals want.
	store := func(cmd, dst string, want map[string]bool) {
		got := sendLine(t, r, c, cmd)
		if got != fmt.Sprintf(":%d", len(want)) {
			t.Fatalf("%s cardinality = %q want :%d", cmd, got, len(want))
		}
		members := readArray(t, r, c, "SMEMBERS "+dst)
		sort.Strings(members)
		w := sortedKeys(want)
		if len(members) != len(w) {
			t.Fatalf("%s stored %d members want %d", cmd, len(members), len(w))
		}
		for i := range w {
			if members[i] != w[i] {
				t.Fatalf("%s stored member %d = %q want %q", cmd, i, members[i], w[i])
			}
		}
	}

	inter := map[string]bool{}
	for m := range sa {
		if sb[m] && sc[m] {
			inter[m] = true
		}
	}
	store("SINTERSTORE d_inter a b c", "d_inter", inter)

	union := map[string]bool{}
	for _, s := range []map[string]bool{sa, sb, sc} {
		for m := range s {
			union[m] = true
		}
	}
	store("SUNIONSTORE d_union a b c", "d_union", union)

	diff := map[string]bool{}
	for m := range sa {
		if !sb[m] && !sc[m] {
			diff[m] = true
		}
	}
	store("SDIFFSTORE d_diff a b c", "d_diff", diff)

	// An empty result deletes the destination. Seed it first so the delete is real.
	_ = sendLine(t, r, c, "SADD d_empty seed")
	if got := sendLine(t, r, c, "SINTERSTORE d_empty a nokey"); got != ":0" {
		t.Fatalf("SINTERSTORE with missing source = %q want :0", got)
	}
	if got := sendLine(t, r, c, "EXISTS d_empty"); got != ":0" {
		t.Fatalf("empty SINTERSTORE left the destination: EXISTS = %q", got)
	}

	// A wrong-type source reports WRONGTYPE and leaves the destination untouched.
	_ = sendLine(t, r, c, "SET str hello")
	for _, cmd := range []string{"SINTERSTORE d_wt a str", "SUNIONSTORE d_wt a str", "SDIFFSTORE d_wt a str"} {
		if reply := sendLine(t, r, c, cmd); reply[:1] != "-" {
			t.Fatalf("%s = %q want WRONGTYPE error", cmd, reply)
		}
	}
	if got := sendLine(t, r, c, "EXISTS d_wt"); got != ":0" {
		t.Fatalf("WRONGTYPE STORE created the destination: EXISTS = %q", got)
	}
}

// TestSetStoreCollLargeResultSpills drives a STORE whose result crosses the
// hashtable threshold, so the destination spills into the coll form and every later
// member is written straight into the sub-tree. It checks the destination lands in
// coll form, holds the exact members, and that SUNIONSTORE deduplicates against the
// sub-tree (overlapping sources do not double-count).
func TestSetStoreCollLargeResultSpills(t *testing.T) {
	r, c := startData(t)
	const n = 1000
	pad := make([]byte, 80)
	for i := range pad {
		pad[i] = 'x'
	}
	want := map[string]bool{}
	for i := range n {
		m := fmt.Sprintf("m:%06d:%s", i, pad)
		want[m] = true
		_ = sendLine(t, r, c, fmt.Sprintf("SADD a %s", m))
		// b overlaps the back half of a, so the union must dedup the overlap.
		if i >= n/2 {
			_ = sendLine(t, r, c, fmt.Sprintf("SADD b %s", m))
		}
	}

	if got := sendLine(t, r, c, "SUNIONSTORE d a b"); got != fmt.Sprintf(":%d", n) {
		t.Fatalf("SUNIONSTORE cardinality = %q want :%d (overlap not deduplicated)", got, n)
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING d"); enc != "hashtable" {
		t.Fatalf("large SUNIONSTORE destination encoding = %q want hashtable", enc)
	}
	if got := sendLine(t, r, c, "SCARD d"); got != fmt.Sprintf(":%d", n) {
		t.Fatalf("SCARD d = %q want :%d", got, n)
	}
	members := readArray(t, r, c, "SMEMBERS d")
	if len(members) != n {
		t.Fatalf("SMEMBERS d returned %d members want %d", len(members), n)
	}
	for _, m := range members {
		if !want[m] {
			t.Fatalf("SMEMBERS d returned %q, not an expected member", m)
		}
	}
}

// TestSetStoreCollDestAliasesSource checks the fallback path: when the destination
// is also a source, the result must still be correct even though writing into the
// destination would otherwise mutate a source mid-walk.
func TestSetStoreCollDestAliasesSource(t *testing.T) {
	r, c := startData(t)
	build := func(name string, lo, hi int) map[string]bool {
		set := map[string]bool{}
		for i := lo; i < hi; i++ {
			m := fmt.Sprintf("m:%06d", i)
			set[m] = true
			_ = sendLine(t, r, c, fmt.Sprintf("SADD %s %s", name, m))
		}
		return set
	}
	sa := build("a", 0, 400)
	sb := build("b", 200, 600)

	// SINTERSTORE a a b: store a = a intersect b back into a.
	inter := map[string]bool{}
	for m := range sa {
		if sb[m] {
			inter[m] = true
		}
	}
	if got := sendLine(t, r, c, "SINTERSTORE a a b"); got != fmt.Sprintf(":%d", len(inter)) {
		t.Fatalf("SINTERSTORE a a b cardinality = %q want :%d", got, len(inter))
	}
	members := readArray(t, r, c, "SMEMBERS a")
	if len(members) != len(inter) {
		t.Fatalf("aliased SINTERSTORE stored %d members want %d", len(members), len(inter))
	}
	for _, m := range members {
		if !inter[m] {
			t.Fatalf("aliased SINTERSTORE stored %q, not in the intersection", m)
		}
	}
}

// TestSetStoreEncodingParity checks the buffered small-result path reports the same
// OBJECT ENCODING Redis would: an all-integer result under the intset cap is intset,
// a short-string result under the listpack cap is listpack.
func TestSetStoreEncodingParity(t *testing.T) {
	r, c := startData(t)
	for i := range 10 {
		_ = sendLine(t, r, c, fmt.Sprintf("SADD ints %d", i))
		_ = sendLine(t, r, c, fmt.Sprintf("SADD strs s%d", i))
	}
	if got := sendLine(t, r, c, "SUNIONSTORE d_ints ints"); got != ":10" {
		t.Fatalf("SUNIONSTORE d_ints = %q want :10", got)
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING d_ints"); enc != "intset" {
		t.Fatalf("all-integer STORE result encoding = %q want intset", enc)
	}
	if got := sendLine(t, r, c, "SUNIONSTORE d_strs strs"); got != ":10" {
		t.Fatalf("SUNIONSTORE d_strs = %q want :10", got)
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING d_strs"); enc != "listpack" {
		t.Fatalf("short-string STORE result encoding = %q want listpack", enc)
	}
}
