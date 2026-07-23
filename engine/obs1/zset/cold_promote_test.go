package zset

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The zset cold-tier promotion (spec 2064/f3/06 sections 6.5 and 7.3,
// milestones/M7-slice-cold-chunk-zset-plan.md, PR E). A write that confirms a cold
// member preads its chunk, which signals the chunk is hot, so the whole chunk lands
// resident at once. The reads themselves are already transparent (the retier-free
// demote keeps every record on its table probe and tree ref, so ZRANGE, ZSCORE, and
// the pops read cold members straight through, proven by the D2 tests); these tests
// drive the one new mechanism, whole-chunk bring-up, at the native band and through
// the ZADD update path.

// TestNativeColdPromoteBringsChunkResident demotes a band into more than one chunk,
// promotes a cold member's chunk, and proves the bring-up: exactly that chunk's
// members turn resident, its directory descriptor is gone, the other chunks stay
// cold, and every member still reads back in order.
func TestNativeColdPromoteBringsChunkResident(t *testing.T) {
	st := coldStore(t)
	// Wide members so the pack spans more than one chunk, which proves the Floor
	// lands on the promoted member's own chunk and leaves the others cold.
	n := newNativeStore(500)
	raw := gen(0, 500, 96)
	members := make([][]byte, len(raw))
	for i, m := range raw {
		members[i] = []byte(m)
		n.insert(members[i], float64(i))
	}

	const demoted = 300
	if got := n.demote(st, []byte("z"), demoted); got != demoted {
		t.Fatalf("demote %d, want %d", got, demoted)
	}
	if n.cold.dir.Len() < 2 {
		t.Fatalf("demote made %d chunks, want the pack to span at least two", n.cold.dir.Len())
	}
	lenBefore := n.cold.dir.Len()
	totalBefore := n.cold.dir.Total()

	// A low-rank member is cold; record its chunk's slot and the members that share it.
	ord, ok := n.tbl.Find(store.Hash(members[10]), members[10], n)
	if !ok || n.recs[ord].loc&tierCold == 0 {
		t.Fatal("member 10 is not cold after the demote")
	}
	slot := locSlot(n.recs[ord].loc)
	var mates []uint32
	n.tree.Each(func(_ uint64, ref uint32) bool {
		r := &n.recs[ref]
		if r.loc&tierCold != 0 && locSlot(r.loc) == slot {
			mates = append(mates, ref)
		}
		return true
	})
	if len(mates) == 0 {
		t.Fatal("the chunk holds no live members")
	}

	// Promoting a resident record is a no-op that reports nothing brought up.
	if hot, _ := n.tbl.Find(store.Hash(members[400]), members[400], n); n.promote(hot) {
		t.Fatal("promote of a resident record reported a chunk brought up")
	}

	if !n.promote(ord) {
		t.Fatal("promote reported no chunk brought resident")
	}
	// The chunk's descriptor is gone: one fewer chunk, its members off the total.
	if got := n.cold.dir.Len(); got != lenBefore-1 {
		t.Fatalf("directory len %d after promote, want %d", got, lenBefore-1)
	}
	if got, want := n.cold.dir.Total(), totalBefore-uint64(len(mates)); got != want {
		t.Fatalf("directory total %d after promote, want %d", got, want)
	}
	// Every mate is resident now; the rest of the cold band is untouched.
	for _, ref := range mates {
		if n.recs[ref].loc&tierCold != 0 {
			t.Fatal("a promoted member stayed cold")
		}
	}
	if c := coldTiers(n); c != demoted-len(mates) {
		t.Fatalf("%d cold after promote, want %d", c, demoted-len(mates))
	}
	// Everything still reads back in order: the promoted chunk from the slab, the
	// rest of the band still preadd.
	ms, scs := walkAll(n)
	if len(ms) != 500 {
		t.Fatalf("each streamed %d after promote, want 500", len(ms))
	}
	for i := range ms {
		if !bytes.Equal(ms[i], members[i]) || scs[i] != float64(i) {
			t.Fatalf("rank %d after promote = %q/%v, want %q", i, ms[i], scs[i], members[i])
		}
	}
}

// TestUpdatePromotesColdMember drives the promotion through the ZADD update path: a
// re-add of a cold member confirms it, which brings its whole chunk resident. The
// footprint reconciles and the band still reads back in order.
func TestUpdatePromotesColdMember(t *testing.T) {
	cx, g := coldCtx(t)
	raw := gen(0, 600, 24)
	strs := make([]string, len(raw))
	copy(strs, raw)
	addKey(g, "k", strs...)
	z := g.m["k"]
	if z.enc != encSkiplist {
		t.Fatalf("zset enc %v, want skiplist", z.enc)
	}
	if n := g.demote(cx, []byte("k"), 500); n != 500 {
		t.Fatalf("demote %d, want 500", n)
	}
	nat := z.nat

	// A low-rank member is cold after the demote.
	cold := []byte(raw[10])
	ord, ok := nat.tbl.Find(store.Hash(cold), cold, nat)
	if !ok || nat.recs[ord].loc&tierCold == 0 {
		t.Fatal("member 10 is not cold after the demote")
	}
	lenBefore := nat.cold.dir.Len()

	// An idempotent re-add (ZADD of the member at its own score) still confirms it,
	// which promotes its whole chunk; then reconcile as the handler does.
	z.update(cold, 10, flags{})
	g.note(z)

	if nat.recs[ord].loc&tierCold != 0 {
		t.Fatal("re-added cold member stayed cold")
	}
	if got := nat.cold.dir.Len(); got >= lenBefore {
		t.Fatalf("directory len %d did not fall from %d after the promoting write", got, lenBefore)
	}
	wantExact(t, g)

	// Every member still reads back in order.
	ms, scs := walkAll(nat)
	if len(ms) != 600 {
		t.Fatalf("each streamed %d, want 600", len(ms))
	}
	for i := range ms {
		if !bytes.Equal(ms[i], []byte(raw[i])) || scs[i] != float64(i) {
			t.Fatalf("rank %d = %q/%v, want %q", i, ms[i], scs[i], raw[i])
		}
	}
}
