package btree

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

// pad is a value body big enough that a 4 KiB page holds only a handful of rows,
// so a few hundred keys build a multi-level tree with interior nodes. The backward
// walk has to climb those interior nodes, which is the whole point of the test.
var pad = bytes.Repeat([]byte("v"), 200)

// forwardKeys walks the tree front to back and returns every key in order.
func forwardKeys(t *testing.T, tr *Tree) [][]byte {
	t.Helper()
	var out [][]byte
	c := tr.Cursor()
	if err := c.First(); err != nil {
		t.Fatalf("First: %v", err)
	}
	for c.Valid() {
		out = append(out, append([]byte(nil), c.Key()...))
		if err := c.Next(); err != nil {
			t.Fatalf("Next: %v", err)
		}
	}
	return out
}

// backwardKeys walks the tree back to front with Last+Prev and returns the keys
// in the order visited (largest first).
func backwardKeys(t *testing.T, tr *Tree) [][]byte {
	t.Helper()
	var out [][]byte
	c := tr.Cursor()
	if err := c.Last(); err != nil {
		t.Fatalf("Last: %v", err)
	}
	for c.Valid() {
		out = append(out, append([]byte(nil), c.Key()...))
		if err := c.Prev(); err != nil {
			t.Fatalf("Prev: %v", err)
		}
	}
	return out
}

// TestCursorBackwardMatchesForward checks Last+Prev visits exactly the reverse of
// First+Next across a multi-level tree, so the path-climbing backward walk agrees
// with the sibling-link forward walk.
func TestCursorBackwardMatchesForward(t *testing.T) {
	tr, _ := newTree(t, 4096)
	const n = 600
	for i := range n {
		if err := tr.Put(fmt.Appendf(nil, "k%05d", i), pad); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	fwd := forwardKeys(t, tr)
	if len(fwd) != n {
		t.Fatalf("forward visited %d keys, want %d", len(fwd), n)
	}
	bwd := backwardKeys(t, tr)
	if len(bwd) != n {
		t.Fatalf("backward visited %d keys, want %d", len(bwd), n)
	}
	for i := range n {
		if !bytes.Equal(fwd[i], bwd[n-1-i]) {
			t.Fatalf("at %d forward=%q backward(reversed)=%q", i, fwd[i], bwd[n-1-i])
		}
	}
}

// TestCursorBackwardAfterDeletes deletes a contiguous run (which empties leaves the
// page format never merges away) plus scattered keys, and checks the backward walk
// still equals the reverse of the forward walk. This is the case the path-climbing
// Prev exists for: an empty leaf has no left-sibling link to follow.
func TestCursorBackwardAfterDeletes(t *testing.T) {
	tr, _ := newTree(t, 4096)
	const n = 800
	for i := range n {
		if err := tr.Put(fmt.Appendf(nil, "k%05d", i), pad); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	for i := 200; i < 360; i++ {
		if _, err := tr.Delete(fmt.Appendf(nil, "k%05d", i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}
	for i := 0; i < n; i += 7 {
		if i >= 200 && i < 360 {
			continue
		}
		if _, err := tr.Delete(fmt.Appendf(nil, "k%05d", i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}
	fwd := forwardKeys(t, tr)
	bwd := backwardKeys(t, tr)
	if len(fwd) != len(bwd) {
		t.Fatalf("forward %d keys, backward %d keys", len(fwd), len(bwd))
	}
	for i := range fwd {
		if !bytes.Equal(fwd[i], bwd[len(bwd)-1-i]) {
			t.Fatalf("at %d forward=%q backward(reversed)=%q", i, fwd[i], bwd[len(bwd)-1-i])
		}
	}
}

// TestCursorSeekForPrev checks SeekForPrev lands on the largest key <= the target
// for present, absent, below-all, and above-all targets, and that Prev continues
// the descending walk from there.
func TestCursorSeekForPrev(t *testing.T) {
	tr, _ := newTree(t, 4096)
	var keys [][]byte
	for i := 0; i < 500; i += 2 { // even keys only, so odd targets are absent
		k := fmt.Appendf(nil, "k%05d", i)
		keys = append(keys, k)
		if err := tr.Put(k, pad); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	sort.Slice(keys, func(a, b int) bool { return bytes.Compare(keys[a], keys[b]) < 0 })

	// largestLE returns the largest stored key <= target, or nil when none.
	largestLE := func(target []byte) []byte {
		var got []byte
		for _, k := range keys {
			if bytes.Compare(k, target) <= 0 {
				got = k
			}
		}
		return got
	}

	cases := [][]byte{
		[]byte("k00100"), // present (even)
		[]byte("k00101"), // absent (odd), lands on k00100
		[]byte("k00099"), // absent, lands on k00098
		[]byte("k99999"), // above all, lands on the max
		[]byte("k00000"), // present minimum
		[]byte("j99999"), // below all, no predecessor
	}
	for _, target := range cases {
		c := tr.Cursor()
		if err := c.SeekForPrev(target); err != nil {
			t.Fatalf("SeekForPrev %q: %v", target, err)
		}
		want := largestLE(target)
		if want == nil {
			if c.Valid() {
				t.Fatalf("SeekForPrev %q valid, want exhausted (key=%q)", target, c.Key())
			}
			continue
		}
		if !c.Valid() {
			t.Fatalf("SeekForPrev %q exhausted, want %q", target, want)
		}
		if !bytes.Equal(c.Key(), want) {
			t.Fatalf("SeekForPrev %q = %q, want %q", target, c.Key(), want)
		}
		// One Prev step should land on the next-smaller stored key.
		idx := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], want) >= 0 })
		if err := c.Prev(); err != nil {
			t.Fatalf("Prev after SeekForPrev %q: %v", target, err)
		}
		if idx == 0 {
			if c.Valid() {
				t.Fatalf("Prev past minimum still valid (key=%q)", c.Key())
			}
			continue
		}
		if !c.Valid() || !bytes.Equal(c.Key(), keys[idx-1]) {
			t.Fatalf("Prev after %q landed wrong: valid=%v key=%q want %q", target, c.Valid(), safeKey(c), keys[idx-1])
		}
	}
}

func safeKey(c *Cursor) []byte {
	if c.Valid() {
		return c.Key()
	}
	return nil
}
