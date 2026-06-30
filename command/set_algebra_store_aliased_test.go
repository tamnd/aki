package command

import (
	"fmt"
	"sort"
	"testing"

	"github.com/tamnd/aki/networking"
)

// The aliased STORE path (destination is also a source) used to fall back to the
// loadSets materialize, which cloned every member of every source onto the heap
// before a byte reached the destination, so an aliased store off a source far larger
// than RAM OOM-killed. The new path streams the result into a scratch key with the
// same bounded sink (point-probes, batched walk, disk spill) and installs the scratch
// onto the destination: a spilled coll result is re-pointed in O(1), a small blob is
// copied. These tests pin the new path's correctness and its boundedness.

// TestSetStoreAliasedMatchesNaive checks the aliased STORE results match a naive
// in-memory computation over coll-form sources for all three operations, including a
// small (buffered-blob) result, a large (spilled, installed via CollMove) result, and
// the empty-result delete. The destination aliases a source in every case.
func TestSetStoreAliasedMatchesNaive(t *testing.T) {
	r, c := startData(t)

	add := func(name string, lo, hi int) map[string]bool {
		set := map[string]bool{}
		for i := lo; i < hi; i++ {
			m := fmt.Sprintf("m:%06d", i)
			set[m] = true
			_ = sendLine(t, r, c, fmt.Sprintf("SADD %s %s", name, m))
		}
		return set
	}
	build := func(name string, lo, hi int) map[string]bool {
		set := add(name, lo, hi)
		if enc := bulk(t, r, c, "OBJECT ENCODING "+name); enc != "hashtable" {
			t.Fatalf("set %s encoding = %q want hashtable (coll form)", name, enc)
		}
		return set
	}

	// readBack reads the destination with SMEMBERS and checks it equals want.
	readBack := func(cmd, dst string, want map[string]bool) {
		got := sendLine(t, r, c, cmd)
		if got != fmt.Sprintf(":%d", len(want)) {
			t.Fatalf("%s cardinality = %q want :%d", cmd, got, len(want))
		}
		if len(want) == 0 {
			if ex := sendLine(t, r, c, "EXISTS "+dst); ex != ":0" {
				t.Fatalf("empty %s left destination existing", cmd)
			}
			return
		}
		members := readArray(t, r, c, "SMEMBERS "+dst)
		sort.Strings(members)
		w := make([]string, 0, len(want))
		for k := range want {
			w = append(w, k)
		}
		sort.Strings(w)
		if len(members) != len(w) {
			t.Fatalf("%s stored %d members want %d", cmd, len(members), len(w))
		}
		for i := range w {
			if members[i] != w[i] {
				t.Fatalf("%s member %d = %q want %q", cmd, i, members[i], w[i])
			}
		}
	}

	// Large aliased intersection: a aliases the destination, the result is big enough
	// to spill, so install goes through CollMove (the O(1) sub-tree re-point).
	a := build("a", 0, 800)
	b := build("b", 200, 1200)
	interAB := map[string]bool{}
	for m := range a {
		if b[m] {
			interAB[m] = true
		}
	}
	readBack("SINTERSTORE a a b", "a", interAB)
	if enc := bulk(t, r, c, "OBJECT ENCODING a"); enc != "hashtable" {
		t.Fatalf("large aliased intersection encoding = %q want hashtable (spilled, CollMove install)", enc)
	}

	// Large aliased union: u aliases the destination, the union spills.
	u := build("u", 0, 700)
	v := build("v", 500, 1300)
	unionUV := map[string]bool{}
	for _, s := range []map[string]bool{u, v} {
		for m := range s {
			unionUV[m] = true
		}
	}
	readBack("SUNIONSTORE u u v", "u", unionUV)
	if enc := bulk(t, r, c, "OBJECT ENCODING u"); enc != "hashtable" {
		t.Fatalf("large aliased union encoding = %q want hashtable (spilled, CollMove install)", enc)
	}

	// Large aliased difference: d aliases the destination, the result spills.
	d := build("d", 0, 900)
	e := build("e", 600, 1000)
	diffDE := map[string]bool{}
	for m := range d {
		if !e[m] {
			diffDE[m] = true
		}
	}
	readBack("SDIFFSTORE d d e", "d", diffDE)

	// Small aliased result: stays a buffered blob, installed by copy (not CollMove).
	// s is coll form, the overlap source is small so the intersection stays under the
	// listpack threshold.
	s := build("s", 0, 300)
	small := add("small", 0, 20)
	interSmall := map[string]bool{}
	for m := range s {
		if small[m] {
			interSmall[m] = true
		}
	}
	readBack("SINTERSTORE s s small", "s", interSmall)
	if enc := bulk(t, r, c, "OBJECT ENCODING s"); enc != "listpack" {
		t.Fatalf("small aliased result encoding = %q want listpack (blob install)", enc)
	}

	// Empty aliased result deletes the destination even though it aliases a source.
	p := build("p", 0, 200)
	_ = p
	if got := sendLine(t, r, c, "SINTERSTORE p p nokey"); got != ":0" {
		t.Fatalf("aliased SINTERSTORE with missing source = %q want :0", got)
	}
	if got := sendLine(t, r, c, "EXISTS p"); got != ":0" {
		t.Fatalf("empty aliased SINTERSTORE left destination: EXISTS = %q", got)
	}
}

// TestSetStoreAliasedIsBounded is the OOM witness for the aliased path: when the
// aliased destination is the small driver and the other source is huge, the store
// must point-probe the huge source instead of cloning it, so allocations track the
// small driver and the per-batch probes, not the huge source. The old loadSets cloned
// the huge source in full before computing anything.
//
// The shape keeps the witness stable across AllocsPerRun's repeated calls: the aliased
// key z holds the small driver and a subset of big, so the intersection stored back is
// z itself (z stays small every run), while big is never written and stays huge.
func TestSetStoreAliasedIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	member := func(i int) []byte { return []byte(fmt.Sprintf("m:%08d", i) + string(pad)) }

	const small = 40
	build := func(bigSize int) (*Dispatcher, *networking.Conn) {
		d := newFuzzDispatcher(t)
		conn := networking.NewOfflineConn()
		apply := func(args [][]byte) { conn.ResetOut(); d.Handle(conn, args) }
		for i := range bigSize {
			apply([][]byte{[]byte("SADD"), []byte("big"), member(i)})
			if i < small {
				apply([][]byte{[]byte("SADD"), []byte("z"), member(i)})
			}
		}
		apply([][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("big")})
		if got := string(conn.OutBytes()); got != "$9\r\nhashtable\r\n" {
			t.Fatalf("big not coll form: OBJECT ENCODING = %q", got)
		}
		return d, conn
	}

	// SINTERSTORE z z big: z aliases the destination and is the small driver, big is
	// point-probed. The result is z's small membership (z subset of big), so z stays
	// small and big stays huge across every measured run.
	storeArgs := [][]byte{[]byte("SINTERSTORE"), []byte("z"), []byte("z"), []byte("big")}
	measure := func(d *Dispatcher, conn *networking.Conn) float64 {
		return testing.AllocsPerRun(10, func() { conn.ResetOut(); d.Handle(conn, storeArgs) })
	}

	dSmall, cSmall := build(4000)
	smallAllocs := measure(dSmall, cSmall)
	dLarge, cLarge := build(8000)
	largeAllocs := measure(dLarge, cLarge)

	if largeAllocs > smallAllocs+300 {
		t.Fatalf("aliased SINTERSTORE allocated %.0f over an 8000-member source vs %.0f over 4000; "+
			"the cost must track the small aliased driver, not clone the huge source", largeAllocs, smallAllocs)
	}
}
