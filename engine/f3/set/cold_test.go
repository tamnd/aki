package set

import "testing"

// The set demotion pack (spec 2064/f3/06 sections 6 and 7). Demoting a set walks a
// native sub-table in member-hash order, packs its members into cold chunks, adds a
// directory descriptor per chunk, and retiers every packed record in place. These
// tests hold the retier contract: after a demote the members have left the native
// slab into chunks the directory covers, every member still reads back through the
// unchanged table probe, and the running resident total drops by the freed slab and
// stays exact.

// residentOf is the walked footprint of the one set under test, the ground truth
// the running total tracks.
func residentOf(g *reg, key string) uint64 { return g.m[key].residentBytes() }

// TestSetDemoteNativePacksAndReadsBack demotes a whole native hashtable set: the
// pass packs every member into cold chunks, frees the slab, and the directory
// covers the full cardinality, yet every member still reads back and a stranger
// still misses. The pack lands more than one chunk (the packing factor is real, not
// one giant frame), and the running total drops by the freed slab and stays exact.
func TestSetDemoteNativePacksAndReadsBack(t *testing.T) {
	cx, g := coldCtx(t)

	members := gen("m", 0, 1000, 12)
	addKey(g, "k", members...)
	if enc := g.m["k"].enc; enc != encHashtable {
		t.Fatalf("1000-member set enc %v, want hashtable", enc)
	}
	wantExact(t, g)
	before := residentOf(g, "k")

	n := g.demote(cx, []byte("k"))
	if n != len(members) {
		t.Fatalf("demoted %d members, want %d", n, len(members))
	}

	s := g.m["k"]
	if s.ht.slab != nil {
		t.Fatalf("slab held %d bytes after demoting every member, want released", len(s.ht.slab))
	}
	if s.cold == nil {
		t.Fatal("cold state nil after a demote")
	}
	if got := s.cold.dir.Total(); got != uint64(len(members)) {
		t.Fatalf("directory total %d, want %d", got, len(members))
	}
	if s.cold.dir.Len() < 2 {
		t.Fatalf("directory holds %d chunks, want the pack to split into several", s.cold.dir.Len())
	}

	// Every demoted member reads back through the ordinary membership probe, which
	// now confirms against the chunk instead of the slab.
	for _, m := range members {
		if !s.has([]byte(m)) {
			t.Fatalf("member %q lost after demotion", m)
		}
	}
	if s.has([]byte("stranger")) {
		t.Fatal("a never-added member read back present from the cold tier")
	}
	if got := s.card(); got != len(members) {
		t.Fatalf("cardinality %d after demotion, want %d", got, len(members))
	}

	// The freed slab dropped the footprint, and the running total still equals the
	// walked sum (demote reconciles through note).
	if after := residentOf(g, "k"); after >= before {
		t.Fatalf("footprint %d did not drop below the pre-demote %d", after, before)
	}
	wantExact(t, g)
}

// TestSetDemotePartitionSweep demotes a partitioned set one partition per quantum:
// repeated demote calls each drain one whole partition until the set is fully cold,
// the counts sum to the cardinality, the directory spans every partition's members
// in one ordered array, and every member across every partition still reads back.
func TestSetDemotePartitionSweep(t *testing.T) {
	withThreshold(t, 512)
	cx, g := coldCtx(t)

	members := gen("p", 0, 2000, 10)
	addKey(g, "k", members...)
	if enc := g.m["k"].enc; enc != encPartitioned {
		t.Fatalf("2000-member set enc %v, want partitioned", enc)
	}
	wantExact(t, g)

	s := g.m["k"]
	nparts := len(s.part.parts)

	total, calls := 0, 0
	for {
		n := g.demote(cx, []byte("k"))
		if n == 0 {
			break
		}
		total += n
		calls++
		if calls > nparts {
			t.Fatalf("demote made %d draining calls over %d partitions, expected one per partition", calls, nparts)
		}
	}
	if total != len(members) {
		t.Fatalf("swept %d members across the partitions, want %d", total, len(members))
	}
	if got := s.cold.dir.Total(); got != uint64(len(members)) {
		t.Fatalf("directory total %d after the sweep, want %d", got, len(members))
	}

	// Every sub-table freed its slab, and every member reads back through its own
	// partition's probe against the shared cold directory.
	for _, h := range s.part.parts {
		if h.slab != nil {
			t.Fatalf("a partition kept %d slab bytes after the sweep, want released", len(h.slab))
		}
	}
	for _, m := range members {
		if !s.has([]byte(m)) {
			t.Fatalf("member %q lost after the partition sweep", m)
		}
	}
	wantExact(t, g)
}

// TestSetDemoteInlineStaysResident holds that the inline bands do not demote: an
// intset and a listpack are each below one chunk's worth, so a demote call is a
// no-op that leaves the set resident with no cold state.
func TestSetDemoteInlineStaysResident(t *testing.T) {
	cx, g := coldCtx(t)

	addKey(g, "ints", intGen(0, 8)...)
	addKey(g, "words", "alpha", "beta", "gamma")
	if enc := g.m["ints"].enc; enc != encIntset {
		t.Fatalf("small integer set enc %v, want intset", enc)
	}
	if enc := g.m["words"].enc; enc != encListpack {
		t.Fatalf("small word set enc %v, want listpack", enc)
	}

	if n := g.demote(cx, []byte("ints")); n != 0 {
		t.Fatalf("intset demoted %d members, want 0", n)
	}
	if n := g.demote(cx, []byte("words")); n != 0 {
		t.Fatalf("listpack demoted %d members, want 0", n)
	}
	if g.m["ints"].cold != nil || g.m["words"].cold != nil {
		t.Fatal("an inline set built cold state on a no-op demote")
	}
	wantExact(t, g)
}
