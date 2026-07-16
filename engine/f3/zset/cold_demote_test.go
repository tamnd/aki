package zset

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/engine/f3/store"
)

// The zset demote pass (spec 2064/f3/06 sections 6 and 7,
// milestones/M7-slice-cold-chunk-zset-plan.md, PR D2 second half). The pass gathers
// a contiguous rank window from the coldest (low-rank) end, packs it into cold chunks
// in score order, and retiers the records to chunk locators. These tests drive the
// pass over a native band and prove it packs the rank window it claims, keeps the
// score and rank resident, reads every member back through the ordered walk, drains
// successive quanta without re-demoting, and reclaims the slab on the churn rebuild.

// coldTiers reports how many of the store's live records carry the cold bit.
func coldTiers(n *nativeStore) int {
	c := 0
	n.tree.Each(func(_ uint64, ref uint32) bool {
		if n.recs[ref].loc&tierCold != 0 {
			c++
		}
		return true
	})
	return c
}

// TestNativeDemotePacksRankWindow demotes a quantum off the low-rank end and proves
// the pack: exactly the quantum lowest-rank members turn cold, the directory covers
// them, their slab bytes go dead, and the score, the rank, and the full ordered walk
// still answer correctly with the cold members preadd.
func TestNativeDemotePacksRankWindow(t *testing.T) {
	st := coldStore(t)
	n, members := buildColdNative(100)
	deadBefore := n.deadBytes

	const quantum = 40
	got := n.demote(st, []byte("z"), quantum)
	if got != quantum {
		t.Fatalf("demote packed %d members, want %d", got, quantum)
	}
	if n.cold == nil {
		t.Fatal("demote left no cold state")
	}
	if c := coldTiers(n); c != quantum {
		t.Fatalf("%d records turned cold, want %d", c, quantum)
	}
	if total := n.cold.dir.Total(); total != quantum {
		t.Fatalf("directory total %d, want %d", total, quantum)
	}
	if n.cold.dir.Len() == 0 {
		t.Fatal("directory holds no chunk after a demote")
	}
	// The demoted members' bytes are now dead in the slab, waiting for the rebuild.
	if n.deadBytes <= deadBefore {
		t.Fatalf("deadBytes %d did not grow past %d", n.deadBytes, deadBefore)
	}

	// The low-rank window is exactly what turned cold: ranks 0..quantum-1 are cold,
	// the rest resident. score and rank stay correct for a cold member.
	for i, m := range members {
		ord, ok := n.tbl.Find(store.Hash(m), m, n)
		if !ok {
			t.Fatalf("member %d absent after demote", i)
		}
		cold := n.recs[ord].loc&tierCold != 0
		if i < quantum && !cold {
			t.Fatalf("rank %d should be cold", i)
		}
		if i >= quantum && cold {
			t.Fatalf("rank %d should be resident", i)
		}
		sc, ok := n.score(m)
		if !ok || sc != float64(i) {
			t.Fatalf("score of member %d = %v,%v want %d", i, sc, ok, i)
		}
		r, rsc, ok := n.rank(m)
		if !ok || r != i || rsc != float64(i) {
			t.Fatalf("rank of member %d = %d,%v,%v want %d", i, r, rsc, ok, i)
		}
	}

	// The full ordered walk streams all hundred in order, cold members preadd.
	ms, scs := walkAll(n)
	if len(ms) != 100 {
		t.Fatalf("each streamed %d, want 100", len(ms))
	}
	for i := range ms {
		if !bytes.Equal(ms[i], members[i]) || scs[i] != float64(i) {
			t.Fatalf("rank %d walk = %q/%v, want %q", i, ms[i], scs[i], members[i])
		}
	}
}

// TestNativeDemoteSuccessiveQuantaDrain runs the pass twice and proves the rank
// cursor drains the low-rank end without re-demoting: the second quantum takes the
// next resident members, so after two passes ranks 0..2q-1 are cold and no member is
// packed twice.
func TestNativeDemoteSuccessiveQuantaDrain(t *testing.T) {
	st := coldStore(t)
	n, members := buildColdNative(100)

	const quantum = 30
	if got := n.demote(st, []byte("z"), quantum); got != quantum {
		t.Fatalf("first demote %d, want %d", got, quantum)
	}
	if got := n.demote(st, []byte("z"), quantum); got != quantum {
		t.Fatalf("second demote %d, want %d", got, quantum)
	}
	if c := coldTiers(n); c != 2*quantum {
		t.Fatalf("%d cold after two quanta, want %d", c, 2*quantum)
	}
	if total := n.cold.dir.Total(); total != 2*quantum {
		t.Fatalf("directory total %d, want %d", total, 2*quantum)
	}
	// Ranks 0..2q-1 cold, the rest resident: the two quanta covered a single
	// contiguous low-rank band.
	for i, m := range members {
		ord, _ := n.tbl.Find(store.Hash(m), m, n)
		cold := n.recs[ord].loc&tierCold != 0
		if want := i < 2*quantum; cold != want {
			t.Fatalf("rank %d cold=%v, want %v", i, cold, want)
		}
	}
	// Every member still reads back in order.
	ms, _ := walkAll(n)
	for i := range ms {
		if !bytes.Equal(ms[i], members[i]) {
			t.Fatalf("rank %d walk = %q, want %q", i, ms[i], members[i])
		}
	}
}

// TestNativeDemoteRebuildReclaimsSlab demotes past the rebuild threshold and proves
// the freed slab bytes are reclaimed: the slab capacity falls, the cold members stay
// cold across the reclaim, and every member still reads back in order.
func TestNativeDemoteRebuildReclaimsSlab(t *testing.T) {
	st := coldStore(t)
	// Wide members so demoting most of them puts more than the rebuild floor of dead
	// bytes behind more than half the slab, firing the reclaim inside demote.
	n := newNativeStore(500)
	raw := gen(0, 500, 24)
	members := make([][]byte, len(raw))
	for i, m := range raw {
		members[i] = []byte(m)
		n.insert(members[i], float64(i))
	}
	slabBefore := cap(n.slab)

	demoted := n.demote(st, []byte("z"), 450)
	if demoted != 450 {
		t.Fatalf("demote packed %d, want 450", demoted)
	}
	if cap(n.slab) >= slabBefore {
		t.Fatalf("slab cap %d did not fall from %d, rebuild did not reclaim", cap(n.slab), slabBefore)
	}
	if c := coldTiers(n); c != 450 {
		t.Fatalf("%d cold after the reclaim, want 450", c)
	}
	ms, scs := walkAll(n)
	if len(ms) != 500 {
		t.Fatalf("each streamed %d after reclaim, want 500", len(ms))
	}
	for i := range ms {
		if !bytes.Equal(ms[i], members[i]) || scs[i] != float64(i) {
			t.Fatalf("rank %d after reclaim = %q/%v, want %q", i, ms[i], scs[i], members[i])
		}
	}
}

// TestRegDemoteReconcilesResident drives the pass through the registry and proves it
// reconciles the footprint: the running total stays the walked sum, and a demote that
// fires the rebuild leaves the zset's resident figure below where it started (the
// freed slab outweighs the small resident directory the chunks add).
func TestRegDemoteReconcilesResident(t *testing.T) {
	cx, g := coldCtx(t)
	// Enough wide members to cross into the native band and give the demote enough
	// slab to reclaim past the directory it adds.
	addKey(g, "k", gen(0, 600, 24)...)
	if enc := g.m["k"].enc; enc != encSkiplist {
		t.Fatalf("zset enc %v, want skiplist", enc)
	}
	wantExact(t, g)
	before := g.resident

	n := g.demote(cx, []byte("k"), 500)
	if n != 500 {
		t.Fatalf("registry demote %d, want 500", n)
	}
	wantExact(t, g)
	if g.resident >= before {
		t.Fatalf("resident %d did not fall from %d after demote", g.resident, before)
	}

	// A demote of a missing key and of a listpack zset are both no-ops that leave the
	// total untouched.
	addKey(g, "small", gen(600, 8, 12)...)
	wantExact(t, g)
	afterSmall := g.resident
	if n := g.demote(cx, []byte("small"), 8); n != 0 {
		t.Fatalf("demote of a listpack zset packed %d, want 0", n)
	}
	if n := g.demote(cx, []byte("absent"), 8); n != 0 {
		t.Fatalf("demote of a missing key packed %d, want 0", n)
	}
	if g.resident != afterSmall {
		t.Fatalf("resident moved on no-op demotes: %d, want %d", g.resident, afterSmall)
	}
}
