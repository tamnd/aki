package main

import "testing"

// These guard the layout the lab froze: the three arms are behaviorally identical
// on the pending-set operations, so the choice between them is purely the memory
// and speed the sweep measured, and the tree-only arm the PEL ships needs no hash
// to serve point ops. The resident-byte and ns/op figures live in the README; what
// CI holds here is the equivalence that makes dropping the hash safe.

func sampleIDs(n int) []id { return denseIDs(n, 1000) }

// TestArmsAgreeOnAck: every arm retires the same IDs to the same owners and reports
// a second ack as a miss, so nothing behavioral rides on the index geometry.
func TestArmsAgreeOnAck(t *testing.T) {
	ids := sampleIDs(2000)
	a, b, c := buildA(ids), buildB(ids), buildC(ids)
	for i, e := range ids {
		want := uint32(i % consumers)
		if got, ok := a.ack(e); !ok || got != want {
			t.Fatalf("A.ack(%v) = %d,%v; want %d,true", e, got, ok, want)
		}
		if got, ok := b.ack(e); !ok || got != want {
			t.Fatalf("B.ack(%v) = %d,%v; want %d,true", e, got, ok, want)
		}
		if ok := c.ack(e, want); !ok {
			t.Fatalf("C.ack(%v) missed, want hit", e)
		}
	}
	// The whole set is retired; a re-ack of any ID misses in every arm.
	if _, ok := a.ack(ids[0]); ok {
		t.Fatal("A re-ack hit, want miss")
	}
	if _, ok := b.ack(ids[0]); ok {
		t.Fatal("B re-ack hit, want miss")
	}
	if ok := c.ack(ids[0], 0); ok {
		t.Fatal("C re-ack hit, want miss")
	}
}

// TestTreeOnlyServesPointAck: the shipped arm B has no hash, and its tree delete
// still retires an arbitrary interior ID and hands back the right owner. This is
// the property that lets the PEL drop the spec's hash.
func TestTreeOnlyServesPointAck(t *testing.T) {
	ids := sampleIDs(1000)
	b := buildB(ids)
	mid := ids[500]
	if got, ok := b.ack(mid); !ok || got != uint32(500%consumers) {
		t.Fatalf("B.ack(mid) = %d,%v; want %d,true", got, ok, uint32(500%consumers))
	}
	if b.tree.Len() != len(ids)-1 {
		t.Fatalf("tree len = %d, want %d after one ack", b.tree.Len(), len(ids)-1)
	}
}

// TestWalkOwnerMatches: the shared-tree owner-filtered walk (arm A/B) and the
// per-consumer tree walk (arm C) yield the same owned IDs in the same ID order, so
// C's only win is speed, not a different answer.
func TestWalkOwnerMatches(t *testing.T) {
	ids := sampleIDs(2000)
	a, c := buildA(ids), buildC(ids)
	for ord := uint32(0); ord < consumers; ord++ {
		var fromA, fromC []id
		a.walkOwner(id{}, ord, func(e *pelEntry) { fromA = append(fromA, e.id) })
		c.walkOwner(id{}, ord, func(e *pelEntry) { fromC = append(fromC, e.id) })
		if len(fromA) != len(fromC) {
			t.Fatalf("ord %d: A saw %d, C saw %d", ord, len(fromA), len(fromC))
		}
		for i := range fromA {
			if fromA[i] != fromC[i] {
				t.Fatalf("ord %d entry %d: A %v, C %v", ord, i, fromA[i], fromC[i])
			}
		}
	}
}

// TestSlabReuse: an ack frees a slab ordinal the next insert reuses, so a steady
// deliver-then-ack cycle does not grow the arena, the same free-list the real PEL
// carries.
func TestSlabReuse(t *testing.T) {
	b := newPELB()
	b.insert(id{ms: 1}, 1, 0)
	b.ack(id{ms: 1})
	if len(b.free) != 1 {
		t.Fatalf("free list = %d, want 1 after an ack", len(b.free))
	}
	b.insert(id{ms: 2}, 2, 0)
	if len(b.slabs) != 1 {
		t.Fatalf("arena grew to %d, want 1 (freed slot reused)", len(b.slabs))
	}
}
