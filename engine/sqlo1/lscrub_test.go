package sqlo1

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// lscrubFill pushes n elements of width bytes onto key, right end, so
// element i sits at position i.
func lscrubFill(t *testing.T, l *List, key string, n, width int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		e := []byte(fmt.Sprintf("e%05d-%s", i, strings.Repeat("x", width)))
		if _, err := l.Push(ctx, []byte(key), false, false, e); err != nil {
			t.Fatalf("Push(%q, %d): %v", key, i, err)
		}
	}
}

// TestListVerifyHealthy drives the scrub over the trivially consistent
// tiers and a noded list, hot and again cold after a Flush and reopen.
func TestListVerifyHealthy(t *testing.T) {
	r := newListRig(t)
	ctx := context.Background()

	if err := r.l.Verify(ctx, []byte("absent")); err != nil {
		t.Fatalf("Verify(absent) = %v", err)
	}
	r.push("small", false, "a", "b", "c")
	lscrubFill(t, r.l, "big", 400, 16)
	if enc, _, err := r.l.Encoding(ctx, []byte("big")); err != nil || enc != "quicklist" {
		t.Fatalf("big encoding = (%q, %v), want noded", enc, err)
	}
	for _, k := range []string{"small", "big"} {
		if err := r.l.Verify(ctx, []byte(k)); err != nil {
			t.Fatalf("Verify(%s) hot = %v", k, err)
		}
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	l2 := r.reopen()
	for _, k := range []string{"absent", "small", "big"} {
		if err := l2.Verify(ctx, []byte(k)); err != nil {
			t.Fatalf("Verify(%s) cold = %v", k, err)
		}
	}
}

// TestListVerifyPaged runs the scrub over a paged fence under the
// shrunken fanouts, so the walk crosses page loads and the parent
// total cross-checks.
func TestListVerifyPaged(t *testing.T) {
	defer SetListFenceCapsForTest(4, 3, 8)()
	r := newListRig(t)
	ctx := context.Background()
	key := []byte("paged")
	lscrubFill(t, r.l, "paged", 1200, 16)
	if _, _, _, err := r.l.stateOf(ctx, key); err != nil {
		t.Fatalf("stateOf: %v", err)
	}
	if !r.l.nodeRoot.paged {
		t.Fatal("fence still flat under the 4-node cap, the paged case is not being tested")
	}
	if err := r.l.Verify(ctx, key); err != nil {
		t.Fatalf("Verify paged hot = %v", err)
	}
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := r.reopen().Verify(ctx, key); err != nil {
		t.Fatalf("Verify paged cold = %v", err)
	}
}

// TestListVerifyCatchesDivergence injects each fence-to-node
// disagreement through the layer's own record writers and pins that
// the scrub names it: a node header disagreeing with its fence count,
// a fence entry naming a missing node, an aliased fence naming one
// node twice, and the same header disagreement behind a fence page.
func TestListVerifyCatchesDivergence(t *testing.T) {
	ctx := context.Background()

	fail := func(t *testing.T, l *List, key []byte, want string) {
		t.Helper()
		err := l.Verify(ctx, key)
		if err == nil {
			t.Fatalf("Verify(%q) passed a diverged list", key)
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Verify(%q) = %q, want an error mentioning %q", key, err, want)
		}
	}

	// load classifies key and leaves the noded root state resident, so
	// the injections below can drive the layer's own record writers.
	load := func(t *testing.T, l *List, key []byte) {
		t.Helper()
		st, _, _, err := l.stateOf(ctx, key)
		if err != nil || st != listNodedState {
			t.Fatalf("stateOf = (%v, %v), want noded", st, err)
		}
	}

	t.Run("count mismatch", func(t *testing.T) {
		r := newListRig(t)
		key := []byte("l")
		lscrubFill(t, r.l, "l", 400, 16)
		load(t, r.l, key)
		ent := r.l.fence[1]
		node, err := r.l.readNode(ctx, ent.segid)
		if err != nil {
			t.Fatalf("readNode: %v", err)
		}
		// Rewrite the node with its last element dropped: the region
		// stays well formed, the header count stays honest, and only
		// the fence disagrees.
		it := listElemIter{p: node.elems}
		keep := 0
		for i := 0; i < node.n-1; i++ {
			e, _ := it.next()
			keep += listElemHdrLen + len(e)
		}
		buf := make([]byte, listNodeHdrLen, listNodeHdrLen+keep)
		buf = append(buf, node.elems[:keep]...)
		putListNodeHdr(buf, node.n-1)
		if err := r.l.writeNode(ctx, ent.segid, buf); err != nil {
			t.Fatalf("writeNode: %v", err)
		}
		fail(t, r.l, key, "node header says")
		if err := r.tr.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		fail(t, r.reopen(), key, "node header says")
	})

	t.Run("missing node", func(t *testing.T) {
		r := newListRig(t)
		key := []byte("l")
		lscrubFill(t, r.l, "l", 400, 16)
		load(t, r.l, key)
		if err := r.l.delNode(ctx, r.l.fence[2].segid); err != nil {
			t.Fatalf("delNode: %v", err)
		}
		fail(t, r.l, key, "is missing")
	})

	t.Run("aliased fence", func(t *testing.T) {
		r := newListRig(t)
		key := []byte("l")
		lscrubFill(t, r.l, "l", 400, 16)
		load(t, r.l, key)
		// Point entry 1 at entry 0's node. The counts are untouched, so
		// the decode's sum check still passes and the alias is the only
		// divergence; the scrub's no-aliasing rule fires before any
		// count comparison could.
		r.l.nodeRoot.fence[1].segid = r.l.nodeRoot.fence[0].segid
		if err := r.l.writeNodeRoot(ctx, key); err != nil {
			t.Fatalf("writeNodeRoot: %v", err)
		}
		fail(t, r.l, key, "twice")
	})

	t.Run("paged count mismatch", func(t *testing.T) {
		defer SetListFenceCapsForTest(4, 3, 8)()
		r := newListRig(t)
		key := []byte("l")
		lscrubFill(t, r.l, "l", 1200, 16)
		load(t, r.l, key)
		if !r.l.nodeRoot.paged {
			t.Fatal("fence still flat under the shrunken caps")
		}
		if err := r.l.loadPage(ctx, 1); err != nil {
			t.Fatalf("loadPage: %v", err)
		}
		ent := r.l.fence[0]
		node, err := r.l.readNode(ctx, ent.segid)
		if err != nil {
			t.Fatalf("readNode: %v", err)
		}
		it := listElemIter{p: node.elems}
		keep := 0
		for i := 0; i < node.n-1; i++ {
			e, _ := it.next()
			keep += listElemHdrLen + len(e)
		}
		buf := make([]byte, listNodeHdrLen, listNodeHdrLen+keep)
		buf = append(buf, node.elems[:keep]...)
		putListNodeHdr(buf, node.n-1)
		if err := r.l.writeNode(ctx, ent.segid, buf); err != nil {
			t.Fatalf("writeNode: %v", err)
		}
		fail(t, r.l, key, "node header says")
	})
}

// TestListVerifySample pins the deterministic draw: the same seed
// walks the same keys, a corrupt key inside the sample surfaces, and a
// too-large n clamps to the list.
func TestListVerifySample(t *testing.T) {
	r := newListRig(t)
	ctx := context.Background()

	var keys [][]byte
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		lscrubFill(t, r.l, k, 200, 16)
		keys = append(keys, []byte(k))
	}
	if err := r.l.VerifySample(ctx, keys, 0, 1); err != nil {
		t.Fatalf("n=0 sample = %v", err)
	}
	if err := r.l.VerifySample(ctx, keys, 100, 1); err != nil {
		t.Fatalf("healthy sample = %v", err)
	}
	st, _, _, err := r.l.stateOf(ctx, keys[3])
	if err != nil || st != listNodedState {
		t.Fatalf("stateOf = (%v, %v), want noded", st, err)
	}
	if err := r.l.delNode(ctx, r.l.fence[1].segid); err != nil {
		t.Fatalf("delNode: %v", err)
	}
	err1 := r.l.VerifySample(ctx, keys, 100, 7)
	if err1 == nil {
		t.Fatal("full sample missed the corrupt key")
	}
	err2 := r.l.VerifySample(ctx, keys, 100, 7)
	if err2 == nil || err1.Error() != err2.Error() {
		t.Fatalf("same seed diverged: %v vs %v", err1, err2)
	}
}
