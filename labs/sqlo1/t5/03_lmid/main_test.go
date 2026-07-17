package main

import (
	"bytes"
	"math/rand"
	"testing"
)

// checkInvariants holds the model to the reference slice and the
// structural rules: fence partition, both caps, exact encoded sizes,
// and no empty nodes.
func checkInvariants(t *testing.T, l *list, ref [][]byte) {
	t.Helper()
	if l.count != len(ref) {
		t.Fatalf("count %d want %d", l.count, len(ref))
	}
	i := 0
	l.walk(func(e []byte) {
		if i < len(ref) && !bytes.Equal(e, ref[i]) {
			t.Fatalf("walk[%d] mismatch", i)
		}
		i++
	})
	if i != len(ref) {
		t.Fatalf("walk yielded %d want %d", i, len(ref))
	}
	total := 0
	for _, n := range l.nodes {
		if len(n.elems) == 0 {
			t.Fatalf("empty node %d survives", n.id)
		}
		if len(n.elems) > l.ecap {
			t.Fatalf("node %d over ecap: %d", n.id, len(n.elems))
		}
		want := nodeHdrBytes
		for _, e := range n.elems {
			want += elemSize(len(e))
		}
		if n.bytes != want {
			t.Fatalf("node %d bytes %d want %d", n.id, n.bytes, want)
		}
		if n.bytes > l.nodeMax {
			t.Fatalf("node %d over nodeMax: %d", n.id, n.bytes)
		}
		total += len(n.elems)
	}
	if total != l.count {
		t.Fatalf("fence partition sums %d want %d", total, l.count)
	}
}

// TestMiddleOracle drives insertAt and removeAt against a reference
// slice at tiny thresholds, with merge off and at two thresholds, so
// splits, drops, and merges all fire thousands of times.
func TestMiddleOracle(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mergeMax int
	}{
		{"nomerge", 0},
		{"mergehalf", 64},
		{"mergefull", 128},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(7))
			l := newList(128, 8, tc.mergeMax)
			var ref [][]byte
			for op := range 8000 {
				if rng.Intn(10) < 6 || len(ref) == 0 {
					e := elemBytes(rng, 1+rng.Intn(12))
					i := rng.Intn(len(ref) + 1)
					l.insertAt(i, e)
					ref = append(ref, nil)
					copy(ref[i+1:], ref[i:])
					ref[i] = e
				} else {
					i := rng.Intn(len(ref))
					got := l.removeAt(i)
					if !bytes.Equal(got, ref[i]) {
						t.Fatalf("op %d removeAt(%d) mismatch", op, i)
					}
					ref = append(ref[:i], ref[i+1:]...)
				}
				if op%500 == 499 {
					checkInvariants(t, l, ref)
				}
			}
			checkInvariants(t, l, ref)
			if l.splits == 0 {
				t.Fatal("splits never fired")
			}
			if tc.mergeMax > 0 && l.merges == 0 {
				t.Fatal("merges never fired")
			}
			if tc.mergeMax == 0 && l.merges != 0 {
				t.Fatalf("merge disabled but fired %d times", l.merges)
			}
		})
	}
}

// TestChurnBoundedWithMerge is the kill-table check in miniature: a
// steady-length insert-remove churn must keep the node count bounded
// with merge on, and the no-merge arm must show the erosion that
// makes the counterweight necessary.
func TestChurnBoundedWithMerge(t *testing.T) {
	run := func(mergeMax int) (nodes0, nodesEnd int) {
		rng := rand.New(rand.NewSource(21))
		l := newList(256, 16, mergeMax)
		for range 2000 {
			l.insertAt(l.count, elemBytes(rng, 12))
		}
		nodes0 = len(l.nodes)
		for range 30000 {
			l.insertAt(rng.Intn(l.count+1), elemBytes(rng, 12))
			l.removeAt(rng.Intn(l.count))
		}
		return nodes0, len(l.nodes)
	}
	n0, merged := run(128)
	_, unmerged := run(0)
	if merged > n0*2 {
		t.Fatalf("merge on: nodes grew %d -> %d, not bounded", n0, merged)
	}
	if unmerged <= merged {
		t.Fatalf("no-merge arm (%d nodes) not worse than merged (%d); the counterweight shows no signal", unmerged, merged)
	}
}
