package main

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/engine/f3/store"
)

// TestSchemesFindAndReject checks that all three probe schemes are correct with
// variable-length field names: every inserted name is found and every disjoint
// miss name is rejected, at a high load where the probe walks are longest and
// across the three length classes so the variable-length confirm is exercised.
func TestSchemesFindAndReject(t *testing.T) {
	const capPow2 = 1 << 12
	fields := int(0.875 * capPow2)
	for _, size := range []int{8, 24, 64} {
		mem := make([][]byte, fields)
		for i := range mem {
			mem[i] = makeName(uint64(i), size)
		}
		miss := make([][]byte, 4096)
		for i := range miss {
			miss[i] = makeName((uint64(1)<<40)+uint64(i), size)
		}
		for _, sch := range []scheme{schLinear, schTriangular, schGroup} {
			tab := newTable(sch, capPow2, fields)
			for _, k := range mem {
				tab.insert(k)
			}
			if int(tab.n) != fields {
				t.Fatalf("size %d %s: inserted %d, want %d", size, sch, tab.n, fields)
			}
			for _, k := range mem {
				if !tab.lookup(k) {
					t.Fatalf("size %d %s: member %q not found", size, sch, k)
				}
			}
			for _, k := range miss {
				if tab.lookup(k) {
					t.Fatalf("size %d %s: miss name %q falsely found", size, sch, k)
				}
			}
		}
	}
}

// TestSwarMatch checks the two properties lookup relies on from the portable
// SWAR group match: it never misses a real tag match (no false negatives, so a
// member is always found), and it never flags an empty control byte (0x80), so
// an empty slot never gets confirmed as a candidate. It may over-report a full
// byte as a candidate; lookup catches that with the name comparison, so the
// match only has to be a safe superset.
func TestSwarMatch(t *testing.T) {
	words := []uint64{
		0x8080808080808080, // all empty
		0x0102030405060780, // seven tags plus one empty
		0x7f7f7f7f7f7f7f7f, // all tag 0x7f
		0x807f00017e2a5501, // mixed tags plus one empty
	}
	for _, w := range words {
		for tag := 0; tag < 128; tag++ {
			got := swarMatch(w, byte(tag))
			if got&^uint64(hi) != 0 {
				t.Fatalf("swarMatch(%016x, %d) = %016x set a non-0x80 bit", w, tag, got)
			}
			for i := 0; i < 8; i++ {
				b := byte(w >> (8 * i))
				bit := uint64(0x80) << (8 * i)
				if b == byte(tag) && got&bit == 0 {
					t.Fatalf("swarMatch(%016x, %d) missed real match at byte %d", w, tag, i)
				}
				if b == ctrlEmpty && got&bit != 0 {
					t.Fatalf("swarMatch(%016x, %d) flagged empty at byte %d", w, tag, i)
				}
			}
		}
	}
}

// TestConfirmIsRealHasher pins the two fidelity choices that make the confirm
// cost trustworthy: the confirm is a bytes.Equal over the stored name (not the
// M1 8-byte word compare), and the hash is the engine's store.Hash, so a name's
// tag and home group are the ones field.go computes.
func TestConfirmIsRealHasher(t *testing.T) {
	tab := newTable(schGroup, 1<<8, 16)
	name := makeName(7, 24)
	tab.insert(name)
	if !bytes.Equal(tab.slab[tab.foff[0]:tab.foff[0]+uint32(tab.flen[0])], name) {
		t.Fatal("stored name does not round-trip through the slab")
	}
	if tagOf(store.Hash(name)) != tab.ctrl[firstFullSlot(tab)] {
		t.Fatal("stored tag does not match store.Hash of the name")
	}
}

func firstFullSlot(t *table) uint32 {
	for i, c := range t.ctrl {
		if c != ctrlEmpty {
			return uint32(i)
		}
	}
	return 0
}

// TestQuickSweep runs the smoke path: the full sweep body at the -quick op
// budget must complete and produce a cell per axis point without panicking, and
// every cell must carry finite, non-negative numbers. This is the -quick check
// the README's method paragraph names.
func TestQuickSweep(t *testing.T) {
	cells := sweep(quickLookOps)
	// 2 cardinalities x 3 length classes x 6 loads x 3 schemes.
	if want := 2 * 3 * 6 * 3; len(cells) != want {
		t.Fatalf("sweep produced %d cells, want %d", len(cells), want)
	}
	for _, c := range cells {
		if c.bytesPerField <= 0 || c.hitNs <= 0 || c.missNs <= 0 || c.mixNs <= 0 {
			t.Fatalf("cell %s/%s/%s load %.3f has a non-positive reading: %+v",
				c.card, c.length, c.sch, c.load, c)
		}
		if c.hitPr < 1 || c.missPr < 1 {
			t.Fatalf("cell %s/%s/%s load %.3f has a probe count below 1: hit %.2f miss %.2f",
				c.card, c.length, c.sch, c.load, c.hitPr, c.missPr)
		}
	}
}
