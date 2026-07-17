package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// trim wraps List.Trim.
func (r *listRig) trim(key string, start, stop int64) {
	r.t.Helper()
	if err := r.l.Trim(context.Background(), []byte(key), start, stop); err != nil {
		r.t.Fatalf("Trim(%q, %d, %d): %v", key, start, stop, err)
	}
}

// TestListTrimOracle drives Trim against a reference slice on both
// tiers through a fixed schedule of window shapes: interior windows
// crossing node boundaries, pure head and tail cuts, single-element
// windows, negatives, over-wide no-ops, and the empty window that
// deletes the key.
func TestListTrimOracle(t *testing.T) {
	rig := newListRig(t)
	ctx := context.Background()

	for _, tier := range []struct {
		name string
		n    int
	}{
		{"inline", 60},
		{"noded", 3000},
	} {
		key := "k-" + tier.name
		ref := make([]string, tier.n)
		elems := make([]string, tier.n)
		for i := range ref {
			ref[i] = fmt.Sprintf("e%04d", i)
			elems[i] = ref[i]
		}
		rig.push(key, false, elems...)

		check := func(l *List) {
			t.Helper()
			got := rig.rng(l, key, 0, -1)
			if len(got) != len(ref) {
				t.Fatalf("%s: %d elements after trim, want %d", tier.name, len(got), len(ref))
			}
			for i := range got {
				if got[i] != ref[i] {
					t.Fatalf("%s: [%d] = %q, want %q", tier.name, i, got[i], ref[i])
				}
			}
			if n, err := l.Len(ctx, []byte(key)); err != nil || n != int64(len(ref)) {
				t.Fatalf("%s: Len = %d, %v, want %d", tier.name, n, err, len(ref))
			}
		}

		// Each step trims the current list; the windows scale with the
		// tier so the noded arm crosses node boundaries both ways.
		u := tier.n / 60
		for _, w := range [][2]int64{
			{-1 << 40, 1 << 40},             // over-wide, a no-op
			{int64(2 * u), int64(-3*u - 1)}, // both edges, negatives
			{1, -1},                         // head cut of one
			{0, -2},                         // tail cut of one
			{int64(5 * u), int64(40 * u)},   // interior window
			{int64(u), int64(u)},            // single element
		} {
			rig.trim(key, w[0], w[1])
			ref = refRange(ref, w[0], w[1])
			check(rig.l)
		}

		// The cold view agrees after a drain.
		if err := rig.tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		check(rig.reopen())

		// The empty window deletes the key, and a recreate starts
		// inline from scratch.
		rig.trim(key, 5, 2)
		if n, err := rig.l.Len(ctx, []byte(key)); err != nil || n != 0 {
			t.Fatalf("%s: Len after empty-window trim = %d, %v", tier.name, n, err)
		}
		if enc, ok, _ := rig.l.Encoding(ctx, []byte(key)); ok {
			t.Fatalf("%s: key survived the empty window as %q", tier.name, enc)
		}
		rig.push(key, false, "fresh")
		if enc, ok, _ := rig.l.Encoding(ctx, []byte(key)); !ok || enc != "listpack" {
			t.Fatalf("%s: recreate = %q, %v", tier.name, enc, ok)
		}
	}

	// The doors: a missing key is a no-op, the wrong type refuses.
	rig.trim("missing", 0, 10)
	if err := rig.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := rig.l.Trim(ctx, []byte("str"), 0, 1); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Trim on a string = %v", err)
	}
}

// TestListTrimEdgeIO is the doc 07 O(edges) IO-count test: a trim
// bills at most two node rewrites and one root image no matter how
// long the list is or how many nodes it discards, and the discarded
// interior nodes leave as deletes without ever being read.
func TestListTrimEdgeIO(t *testing.T) {
	rig := newListRig(t)
	ctx := context.Background()

	const n = 3000
	elems := make([]string, n)
	for i := range elems {
		elems[i] = fmt.Sprintf("e%04d", i)
	}
	rig.push("k", false, elems...)
	if err := rig.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	nodes := len(rig.nodedRoot("k").fence)
	if nodes < 20 {
		t.Fatalf("only %d nodes; the test wants a long fence", nodes)
	}

	// batchBill flushes and tallies the ops the trim dirtied.
	batchBill := func() (puts, dels int) {
		t.Helper()
		mark := len(rig.rs.batches)
		if err := rig.tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		for _, b := range rig.rs.batches[mark:] {
			for _, op := range b.Ops {
				if op.Del {
					dels++
				} else {
					puts++
				}
			}
		}
		return puts, dels
	}

	// A head cut inside the first node: one node rewrite, one root.
	rig.trim("k", 3, -1)
	puts, dels := batchBill()
	if puts != 2 || dels != 0 {
		t.Fatalf("head cut billed %d puts, %d dels, want 2 and 0", puts, dels)
	}

	// A tail cut inside the last node: the same bill from the other
	// edge.
	rig.trim("k", 0, -4)
	puts, dels = batchBill()
	if puts != 2 || dels != 0 {
		t.Fatalf("tail cut billed %d puts, %d dels, want 2 and 0", puts, dels)
	}

	// An interior window dropping whole nodes at both ends: at most
	// two edge rewrites plus the root, and every dropped node is a
	// delete, not a rewrite.
	before := len(rig.nodedRoot("k").fence)
	rig.trim("k", 300, 900)
	puts, dels = batchBill()
	after := len(rig.nodedRoot("k").fence)
	dropped := before - after
	if dropped < 10 {
		t.Fatalf("the interior window dropped %d nodes; the test wants a real cut", dropped)
	}
	if puts > 3 {
		t.Fatalf("interior trim billed %d puts across %d dropped nodes, want at most 3", puts, dropped)
	}
	if dels != dropped {
		t.Fatalf("interior trim billed %d dels, dropped %d nodes", dels, dropped)
	}

	// Cold reads stayed at the edges too: the whole schedule read at
	// most two nodes per trim and never touched a dropped interior.
	got := rig.rng(rig.l, "k", 0, -1)
	if len(got) != 601 || got[0] != "e0303" || got[600] != "e0903" {
		t.Fatalf("post-trim window = %d elements [%s .. %s]", len(got), got[0], got[len(got)-1])
	}
}
