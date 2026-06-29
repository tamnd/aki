package btree

import (
	"bytes"
	"fmt"
	"testing"
)

// TestShortestSep checks the promoted separator routes correctly: it must be
// strictly greater than the left page's last key and no greater than the right
// page's first key, and it must be a prefix of the right first key.
func TestShortestSep(t *testing.T) {
	cases := []struct {
		lo, hi []byte
		want   []byte
	}{
		{[]byte("a"), []byte("b"), []byte("b")},                // differ at 0
		{[]byte("app"), []byte("apq"), []byte("apq")},          // differ at 2
		{[]byte("app"), []byte("application"), []byte("appl")}, // lo is a prefix of hi
		{[]byte("field:000000000123"), []byte("field:000000000124"), []byte("field:000000000124")},
		{bytes.Repeat([]byte{0x00}, 255), append(bytes.Repeat([]byte{0x00}, 255), 0x01), append(bytes.Repeat([]byte{0x00}, 255), 0x01)},
		{[]byte{0x10}, []byte{0x20, 0x30, 0x40}, []byte{0x20}}, // big right key collapses to one byte
	}
	for _, c := range cases {
		got := shortestSep(c.lo, c.hi)
		if !bytes.Equal(got, c.want) {
			t.Errorf("shortestSep(%q,%q)=%q want %q", c.lo, c.hi, got, c.want)
		}
		if bytes.Compare(got, c.lo) <= 0 {
			t.Errorf("shortestSep(%q,%q)=%q not > lo", c.lo, c.hi, got)
		}
		if bytes.Compare(got, c.hi) > 0 {
			t.Errorf("shortestSep(%q,%q)=%q > hi", c.lo, c.hi, got)
		}
		if !bytes.HasPrefix(c.hi, got) {
			t.Errorf("shortestSep(%q,%q)=%q not a prefix of hi", c.lo, c.hi, got)
		}
	}
}

// TestSuffixTruncationLargeKeys builds a multi-level tree from large random keys,
// the shape a set of long members produces, and checks that every key round-trips
// and the structural invariants hold. Without suffix truncation the interior pages
// would carry full-length separators; this exercises the truncated path under real
// splits, deletes, and reopen-free reads.
func TestSuffixTruncationLargeKeys(t *testing.T) {
	tr, _ := newTree(t, 4096)
	// Deterministic pseudo-random 200-byte keys; large enough that a full-key
	// separator would dominate an interior page, so truncation is what keeps the
	// tree shallow.
	const n = 4000
	keys := make([][]byte, n)
	seed := uint64(0x9E3779B97F4A7C15)
	for i := range n {
		k := make([]byte, 200)
		for j := range k {
			seed = seed*6364136223846793005 + 1442695040888963407
			k[j] = byte(seed >> 33)
		}
		// Prefix with the index so keys are distinct even if randomness collides.
		copy(k, fmt.Appendf(nil, "m%08d:", i))
		keys[i] = k
		if err := tr.Put(k, fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := CheckInvariants(tr); err != nil {
		t.Fatalf("invariants after load: %v", err)
	}
	for i, k := range keys {
		v, ok, err := tr.Get(k)
		if err != nil || !ok {
			t.Fatalf("get key %d: ok=%v err=%v", i, ok, err)
		}
		if string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("key %d value mismatch: %q", i, v)
		}
	}
	// Delete a third, in a scattered order, and re-verify the rest plus invariants.
	for i := 0; i < n; i += 3 {
		if _, err := tr.Delete(keys[i]); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	if err := CheckInvariants(tr); err != nil {
		t.Fatalf("invariants after deletes: %v", err)
	}
	for i, k := range keys {
		v, ok, _ := tr.Get(k)
		if i%3 == 0 {
			if ok {
				t.Fatalf("deleted key %d still present", i)
			}
			continue
		}
		if !ok || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("survivor key %d wrong: ok=%v v=%q", i, ok, v)
		}
	}
}
