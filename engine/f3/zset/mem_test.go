package zset

import (
	"math/rand/v2"
	"testing"
)

// The F14 memory report for the native band. The convention follows the tree
// lab: data is the member bytes plus the 8-byte score every engine must hold,
// and everything else is overhead. Two figures are reported per size:
//
//   - the tree side alone, Bytes()/n minus the 16-byte (key, ref, count share)
//     payload, against the 2-3 B/entry bulk bar from doc 12 section 9; the
//     milestone blocks a bulk build over 5 B/entry;
//   - the full dual structure (tree + member table + record cells + slab
//     slack), which doc 12 section 9 expects around 34-36 B/entry, the member
//     hash slot dominating.
//
// Bulk builds go through appendSorted + seal, the promotion path, landing at
// the right-edge 0.9 fill; random builds go through insert and pay the split
// fill instead.
func TestNativeBytesPerEntry(t *testing.T) {
	sizes := []int{10_000, 1_000_000}
	if testing.Short() {
		sizes = sizes[:1]
	}
	for _, size := range sizes {
		t.Run("bulk/"+itoa(size), func(t *testing.T) {
			n := newNativeStore(size)
			memberBytes := 0
			for i := 0; i < size; i++ {
				m := "member:" + pad(i)
				n.appendSorted([]byte(m), float64(i))
				memberBytes += len(m)
			}
			n.seal()
			report(t, n, size, memberBytes, true)
		})
		t.Run("random/"+itoa(size), func(t *testing.T) {
			n := newNativeStore(size)
			rng := rand.New(rand.NewPCG(9, uint64(size)))
			memberBytes := 0
			for i := 0; i < size; i++ {
				m := "member:" + pad(i)
				n.insert([]byte(m), rng.Float64()*1e6)
				memberBytes += len(m)
			}
			report(t, n, size, memberBytes, false)
		})
	}
}

func report(t *testing.T, n *nativeStore, size, memberBytes int, bulk bool) {
	t.Helper()
	if n.card() != size {
		t.Fatalf("card = %d, want %d", n.card(), size)
	}
	treeSide := float64(n.tree.Bytes())/float64(size) - 16
	full := float64(n.bytes()-memberBytes-8*size) / float64(size)
	t.Logf("n=%d tree side %.2f B/entry, full structure %.2f B/entry over member+score", size, treeSide, full)
	if bulk && treeSide > 5 {
		t.Fatalf("bulk tree side %.2f B/entry over the 5 B milestone block (bar 2-3 B)", treeSide)
	}
}

// pad renders i at a fixed width so every member is the same length and the
// bulk build's input arrives in both member and score order.
func pad(i int) string {
	s := itoa(i)
	const w = 7
	if len(s) >= w {
		return s
	}
	return "0000000"[:w-len(s)] + s
}
