package main

import "testing"

// TestSchemesFindAndReject checks that all three probe schemes are correct:
// every inserted member is found and every disjoint miss key is rejected, at a
// high load where the probe walks are longest.
func TestSchemesFindAndReject(t *testing.T) {
	const capPow2 = 1 << 12
	members := int(0.875 * capPow2)
	mem := make([]uint64, members)
	rng := xorshift(0x1234567)
	for i := range mem {
		mem[i] = rng.next() | 1 // odd
	}
	miss := make([]uint64, 4096)
	for i := range miss {
		miss[i] = rng.next() &^ 1 // even, disjoint from mem
	}
	for _, sch := range []scheme{schLinear, schTriangular, schGroup} {
		tab := newTable(sch, capPow2, members)
		for _, k := range mem {
			tab.insert(k)
		}
		if int(tab.n) != members {
			t.Fatalf("%s: inserted %d, want %d", sch, tab.n, members)
		}
		for _, k := range mem {
			if !tab.lookup(k) {
				t.Fatalf("%s: member %d not found", sch, k)
			}
		}
		for _, k := range miss {
			if tab.lookup(k) {
				t.Fatalf("%s: miss key %d falsely found", sch, k)
			}
		}
	}
}

// TestSwarMatch checks the two properties lookup relies on from the portable
// SWAR group match (the abseil GroupPortable behaviour): it never misses a real
// tag match (no false negatives, so a member is always found), and it never
// flags an empty control byte (0x80), so an empty slot never gets confirmed as a
// candidate. It may over-report a full byte as a candidate; lookup catches that
// with the record key comparison, so the match only has to be a safe superset.
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
			if got&^hi != 0 {
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
