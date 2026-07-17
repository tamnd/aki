package main

import (
	"bytes"
	"math/rand"
	"testing"
)

// ref is the oracle: a plain slice in list order.
type ref struct{ elems [][]byte }

func (r *ref) push(left bool, e []byte) {
	if left {
		r.elems = append([][]byte{e}, r.elems...)
	} else {
		r.elems = append(r.elems, e)
	}
}

func (r *ref) pop(left bool) ([]byte, bool) {
	if len(r.elems) == 0 {
		return nil, false
	}
	var e []byte
	if left {
		e = r.elems[0]
		r.elems = r.elems[1:]
	} else {
		e = r.elems[len(r.elems)-1]
		r.elems = r.elems[:len(r.elems)-1]
	}
	return e, true
}

func (r *ref) ltrimHead(keep int) {
	if len(r.elems) > keep {
		r.elems = r.elems[:keep]
	}
}

// checkAgainst holds the model to the reference: count, walk order,
// every index, sampled ranges, and the structural invariants (fence
// partition L-I6, node caps, encoded sizes).
func checkAgainst(t *testing.T, l *list, r *ref) {
	t.Helper()
	if l.count != len(r.elems) {
		t.Fatalf("count %d want %d", l.count, len(r.elems))
	}
	i := 0
	l.walk(func(e []byte) {
		if i < len(r.elems) && !bytes.Equal(e, r.elems[i]) {
			t.Fatalf("walk[%d] = %q want %q", i, e, r.elems[i])
		}
		i++
	})
	if i != len(r.elems) {
		t.Fatalf("walk yielded %d want %d", i, len(r.elems))
	}
	for j := 0; j < l.count; j++ {
		if !bytes.Equal(l.lindex(j), r.elems[j]) {
			t.Fatalf("lindex(%d) mismatch", j)
		}
	}
	fenceCount := 0
	for _, n := range l.nodes {
		if len(n.elems) == 0 {
			t.Fatalf("empty node %d survives in fence", n.id)
		}
		if len(n.elems) > l.ecap {
			t.Fatalf("node %d has %d elems over ecap %d", n.id, len(n.elems), l.ecap)
		}
		want := nodeHdrBytes
		for _, e := range n.elems {
			want += elemSize(len(e))
		}
		if n.bytes != want {
			t.Fatalf("node %d bytes %d want %d", n.id, n.bytes, want)
		}
		fenceCount += len(n.elems)
	}
	if fenceCount != l.count {
		t.Fatalf("fence partition sums %d want %d", fenceCount, l.count)
	}
}

func checkRange(t *testing.T, l *list, r *ref, off, count int) {
	t.Helper()
	var got [][]byte
	l.lrange(off, count, func(e []byte) { got = append(got, e) })
	want := [][]byte{}
	if off < len(r.elems) {
		hi := min(off+count, len(r.elems))
		want = r.elems[off:hi]
	}
	if len(got) != len(want) {
		t.Fatalf("lrange(%d,%d) yielded %d want %d", off, count, len(got), len(want))
	}
	for i := range got {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("lrange(%d,%d)[%d] mismatch", off, count, i)
		}
	}
}

// TestListModelOracle drives the model against the reference through
// thousands of ops at tiny thresholds so cuts, drops, trims, and both
// fence paging transitions all fire many times.
func TestListModelOracle(t *testing.T) {
	for _, tc := range []struct {
		name    string
		nodeMax int
		ecap    int
		elen    int
	}{
		{"bytebound", 64, 128, 9}, // node_max binds, ~4 elems per node
		{"capbound", 4032, 3, 9},  // ecap binds
		{"single", 24, 1, 9},      // every element its own node
		{"pagingcusp", 40, 2, 9},  // long fence, crosses inline cap both ways
	} {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(7))
			l := newList(tc.nodeMax, tc.ecap)
			r := &ref{}
			for op := range 6000 {
				switch k := rng.Intn(10); {
				case k < 4: // push, both ends
					e := elemBytes(rng, 1+rng.Intn(tc.elen))
					left := rng.Intn(2) == 0
					l.push(left, e)
					r.push(left, e)
				case k < 7: // pop, both ends
					left := rng.Intn(2) == 0
					ge, gok := l.pop(left)
					we, wok := r.pop(left)
					if gok != wok || (gok && !bytes.Equal(ge, we)) {
						t.Fatalf("op %d pop(%v) = %q,%v want %q,%v", op, left, ge, gok, we, wok)
					}
				case k < 8: // trim to a random cap
					if l.count > 0 {
						keep := rng.Intn(l.count + 1)
						l.ltrimHead(keep)
						r.ltrimHead(keep)
					}
				default: // read window
					checkRange(t, l, r, rng.Intn(l.count+3), 1+rng.Intn(20))
				}
				if op%500 == 499 {
					checkAgainst(t, l, r)
				}
			}
			checkAgainst(t, l, r)
			if tc.name == "pagingcusp" {
				// Drive both fence paging transitions deterministically:
				// grow well past the inline cap, verify, trim back
				// under it, verify again.
				for range 900 {
					e := elemBytes(rng, 1+rng.Intn(tc.elen))
					l.push(false, e)
					r.push(false, e)
				}
				if !l.paged() {
					t.Fatalf("fence with %d nodes still inline after growth", len(l.nodes))
				}
				checkAgainst(t, l, r)
				l.ltrimHead(40)
				r.ltrimHead(40)
				if l.paged() {
					t.Fatalf("fence with %d nodes still paged after trim", len(l.nodes))
				}
				checkAgainst(t, l, r)
			}
		})
	}
}

// TestCappedFeedShape pins the doc 07 LTRIM bound on the feed shape:
// each trim rewrites at most one edge node, and whole-node drops
// amortize to one per node-fill of pushes.
func TestCappedFeedShape(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	l := newList(512, 128)
	r := &ref{}
	for range 400 {
		e := elemBytes(rng, 60)
		l.push(true, e)
		r.push(true, e)
	}
	l.drops, l.edgeRewr = 0, 0
	const trims = 3000
	for range trims {
		e := elemBytes(rng, 60)
		l.push(true, e)
		r.push(true, e)
		l.ltrimHead(400)
		r.ltrimHead(400)
	}
	checkAgainst(t, l, r)
	if l.drops == 0 {
		t.Fatal("feed never dropped a whole node")
	}
	if l.edgeRewr > trims {
		t.Fatalf("edge rewrites %d exceed one per trim (%d); trim is not O(edges)", l.edgeRewr, trims)
	}
	// 60 B elements pack 7 to a 512 B node, so ~trims/7 whole-node
	// drops; hold the amortization to a loose band.
	if lo, hi := int64(trims/14), int64(trims/3); l.drops < lo || l.drops > hi {
		t.Fatalf("drops %d outside amortized band [%d,%d]", l.drops, lo, hi)
	}
}

// TestSeekPagedAgreement holds the two-level paged seek to the flat
// answer across a fence well past the inline cap.
func TestSeekPagedAgreement(t *testing.T) {
	rng := rand.New(rand.NewSource(13))
	l := newList(48, 2)
	for range 5000 {
		l.push(false, elemBytes(rng, 8))
	}
	if !l.paged() {
		t.Fatalf("fence with %d nodes still inline", len(l.nodes))
	}
	for range 2000 {
		i := rng.Intn(l.count)
		ni, off, _ := l.seek(i)
		// Flat recount.
		j := i
		fni := 0
		for ; j >= len(l.nodes[fni].elems); fni++ {
			j -= len(l.nodes[fni].elems)
		}
		if ni != fni || off != j {
			t.Fatalf("seek(%d) = node %d off %d, flat says node %d off %d", i, ni, off, fni, j)
		}
	}
}
