package f1srv

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
)

// TestOrderKeyEndOrdering checks that the 8-byte end encoding is order preserving across the whole
// signed range the two end sequences use: a head seq driven below zero must sort before a tail seq
// above it, and stepping a seq by one must step the key to the adjacent key with no gap or overlap.
func TestOrderKeyEndOrdering(t *testing.T) {
	seqs := []int64{
		-1 << 62, -1 << 40, -1_000_000, -256, -1, 0, 1, 255, 256, 1_000_000, 1 << 40, 1<<62 - 1,
	}
	for i := 1; i < len(seqs); i++ {
		lo := orderKeyEnd(seqs[i-1])
		hi := orderKeyEnd(seqs[i])
		if bytes.Compare(lo, hi) >= 0 {
			t.Fatalf("seq %d key %x not below seq %d key %x", seqs[i-1], lo, seqs[i], hi)
		}
		if len(lo) != 8 || len(hi) != 8 {
			t.Fatalf("end keys must be 8 bytes, got %d and %d", len(lo), len(hi))
		}
	}
	// Adjacent seqs must produce keys with nothing representable between them at the backbone width,
	// which is what makes an interior insert land as backbone-prefix plus a fractional suffix.
	a := orderKeyEnd(41)
	b := orderKeyEnd(42)
	mid := orderKeyBetween(a, b)
	if bytes.Compare(a, mid) >= 0 || bytes.Compare(mid, b) >= 0 {
		t.Fatalf("between adjacent end keys not strictly ordered: %x < %x < %x", a, mid, b)
	}
	if len(mid) <= 8 {
		t.Fatalf("between adjacent 8-byte keys must extend past 8 bytes, got %d", len(mid))
	}
}

// assertBetween is the core invariant: k sits strictly between lo and hi and never ends in 0x00.
func assertBetween(t *testing.T, lo, hi, k []byte) {
	t.Helper()
	if lo != nil && bytes.Compare(lo, k) >= 0 {
		t.Fatalf("key %x not above lo %x", k, lo)
	}
	if hi != nil && bytes.Compare(k, hi) >= 0 {
		t.Fatalf("key %x not below hi %x", k, hi)
	}
	if len(k) == 0 {
		t.Fatalf("between returned empty key for lo=%x hi=%x", lo, hi)
	}
	if k[len(k)-1] == 0 {
		t.Fatalf("key %x ends in 0x00 (breaks the strict-between invariant)", k)
	}
}

// TestOrderKeyBetweenOpenBounds covers the head-insert (nil lo) and tail-insert (nil hi) cases and
// a plain gap, adjacency, and prefix pair.
func TestOrderKeyBetweenOpenBounds(t *testing.T) {
	cases := []struct{ lo, hi []byte }{
		{nil, nil},
		{nil, []byte{0x80}},
		{[]byte{0x80}, nil},
		{[]byte{10}, []byte{12}},       // gap
		{[]byte{10}, []byte{11}},       // adjacent
		{[]byte{10}, []byte{10, 0x80}}, // lo is a prefix of hi
		{[]byte{10, 0x80}, []byte{11}}, // fractional lo
		{[]byte{10}, []byte{10, 1}},    // lo prefix, adjacent low suffix digit
	}
	for _, c := range cases {
		k := orderKeyBetween(c.lo, c.hi)
		assertBetween(t, c.lo, c.hi, k)
	}
}

// TestOrderKeyBetweenExhaustiveShort walks every ordered pair of short strings over a small
// alphabet and asserts the invariant, so the digit-level branches (gap, adjacent, shared prefix,
// exhaustion) are all hit with concrete bytes rather than by argument. Bounds that end in 0x00 are
// excluded because they are exactly the keys the allocator's invariant forbids: no live key ever
// ends in 0x00, so no real bound does. A zero-terminated bound is also genuinely unsatisfiable in
// the open-lo case (nothing sorts strictly before the minimum key 0x00), which is the contract
// hole the exclusion documents, not a bug in the descent.
func TestOrderKeyBetweenExhaustiveShort(t *testing.T) {
	alphabet := []byte{0, 1, 2, 254, 255}
	var strs [][]byte
	strs = append(strs, nil)
	for _, a := range alphabet {
		if a != 0 {
			strs = append(strs, []byte{a})
		}
		for _, b := range alphabet {
			if b != 0 { // a bound must not end in 0x00 (the live-key invariant)
				strs = append(strs, []byte{a, b})
			}
		}
	}
	for _, lo := range strs {
		for _, hi := range strs {
			// Only meaningful when lo sorts strictly below hi, treating nil lo as -inf and nil hi
			// as +inf; a nil-nil pair is the open-open case, always valid.
			if lo != nil && hi != nil && bytes.Compare(lo, hi) >= 0 {
				continue
			}
			k := orderKeyBetween(lo, hi)
			assertBetween(t, lo, hi, k)
		}
	}
}

// TestOrderKeyBetweenFromBackbone drives the real LINSERT scenario: start from two adjacent
// backbone end keys (the fixed 8-byte encoding a fresh RPUSH pair produces) and repeatedly insert
// between real neighbours, so every bound the allocator sees is either an 8-byte backbone key or a
// fractional key it produced earlier, never a synthetic zero-terminated string. This is the faithful
// proof that the invariant holds along the path a live list actually walks.
func TestOrderKeyBetweenFromBackbone(t *testing.T) {
	rng := rand.New(rand.NewSource(0xB0B))
	o := &orderedInserter{keys: [][]byte{orderKeyEnd(100), orderKeyEnd(101), orderKeyEnd(102)}}
	for i := 0; i < 4000; i++ {
		// Insert only at interior gaps (1..len-1), the LINSERT-before/after case between two real
		// neighbours, so both bounds are always present live keys.
		gap := 1 + rng.Intn(len(o.keys)-1)
		k := o.insertAt(gap)
		assertBetween(t, o.keys[gap-1], nil, k) // above the left neighbour
		if gap+1 < len(o.keys) {
			assertBetween(t, nil, o.keys[gap+1], k) // below the right neighbour
		}
		for j := 1; j < len(o.keys); j++ {
			if bytes.Compare(o.keys[j-1], o.keys[j]) >= 0 {
				t.Fatalf("step %d unsorted at %d", i, j)
			}
		}
	}
}

// TestOrderKeyBetweenHammerSpot drives the adversarial case scheme A cannot survive: repeatedly
// insert at the exact same spot, each new key between the fixed left neighbour and the current
// closest right neighbour. A float-midpoint scheme wedges after about 52 such inserts when the
// mantissa runs out; the byte-string scheme instead subdivides forever, growing the key about one
// byte per eight inserts. The test proves it never wedges over many thousands of inserts and
// records the growth so the bounded-rebalance slice (section 13.2) has a real number to target.
func TestOrderKeyBetweenHammerSpot(t *testing.T) {
	left := orderKeyEnd(0)
	right := orderKeyEnd(1)
	closest := right
	const n = 20000
	maxLen := 0
	for i := 0; i < n; i++ {
		k := orderKeyBetween(left, closest)
		assertBetween(t, left, closest, k)
		closest = k
		if len(k) > maxLen {
			maxLen = len(k)
		}
	}
	// One byte buys eight halvings, so n inserts should stay well under n bytes and nowhere near
	// the 64KiB key ceiling; the point is that it is bounded and finite, and that a rebalance would
	// reset it. This asserts the growth is the expected roughly n/8 shape, not runaway.
	if maxLen > n/4 {
		t.Fatalf("hammered key grew to %d bytes over %d inserts, faster than the expected ~n/8", maxLen, n)
	}
	t.Logf("hammered %d inserts at one spot, deepest key %d bytes (~%.1f inserts per byte)", n, maxLen, float64(n)/float64(maxLen))
}

// orderedInserter is a reference list built purely from order keys: it keeps its element keys in a
// sorted slice and inserts a new element by allocating a key between the two neighbours at a chosen
// gap. It mirrors exactly what the order-statistic list does, minus the index, so a random sequence
// of inserts against it exercises the allocator the way LPUSH, RPUSH, and LINSERT will.
type orderedInserter struct {
	keys [][]byte
}

func (o *orderedInserter) insertAt(gap int) []byte {
	var lo, hi []byte
	if gap > 0 {
		lo = o.keys[gap-1]
	}
	if gap < len(o.keys) {
		hi = o.keys[gap]
	}
	k := orderKeyBetween(lo, hi)
	o.keys = append(o.keys, nil)
	copy(o.keys[gap+1:], o.keys[gap:])
	o.keys[gap] = k
	return k
}

// TestOrderKeyRandomInsertSequence builds a list through a long random sequence of inserts at
// arbitrary gaps (the general LINSERT workload plus both ends) and checks after every step that the
// key slice is still strictly sorted and every key honours the invariant, cross-checked against an
// independent sort. This is the property test that a mistake in the descent would trip.
func TestOrderKeyRandomInsertSequence(t *testing.T) {
	rng := rand.New(rand.NewSource(0xA11CE))
	o := &orderedInserter{}
	const n = 5000
	for i := 0; i < n; i++ {
		gap := rng.Intn(len(o.keys) + 1)
		k := o.insertAt(gap)
		if len(k) == 0 || k[len(k)-1] == 0 {
			t.Fatalf("step %d produced invalid key %x", i, k)
		}
		// The slice must stay strictly increasing after the splice.
		for j := 1; j < len(o.keys); j++ {
			if bytes.Compare(o.keys[j-1], o.keys[j]) >= 0 {
				t.Fatalf("step %d left keys unsorted at %d: %x >= %x", i, j, o.keys[j-1], o.keys[j])
			}
		}
	}
	// Independent confirmation: an external sort of the keys must equal their in-list order, so the
	// order key really does carry list order and nothing has drifted.
	sorted := make([][]byte, len(o.keys))
	copy(sorted, o.keys)
	sort.Slice(sorted, func(a, b int) bool { return bytes.Compare(sorted[a], sorted[b]) < 0 })
	for i := range sorted {
		if !bytes.Equal(sorted[i], o.keys[i]) {
			t.Fatalf("independent sort disagrees with list order at %d", i)
		}
	}
}

// TestOrderKeyEndInterleave mixes the two end sequences the way a push-only workload does (RPUSH
// stepping tail up, LPUSH stepping head down) and confirms the head keys all sort below the tail
// keys with the seed at the boundary, so a fresh list's two ends never cross.
func TestOrderKeyEndInterleave(t *testing.T) {
	var keys [][]byte
	// Tail pushes: seqs 0,1,2,... Head pushes: seqs -1,-2,-3,... The list order is head keys in
	// reverse allocation order, then the seed, then tail keys in allocation order.
	head := int64(0)
	tail := int64(0)
	var order [][]byte
	for i := 0; i < 50; i++ {
		head--
		hk := orderKeyEnd(head)
		order = append([][]byte{hk}, order...)
		keys = append(keys, hk)
		tk := orderKeyEnd(tail)
		tail++
		order = append(order, tk)
		keys = append(keys, tk)
	}
	for j := 1; j < len(order); j++ {
		if bytes.Compare(order[j-1], order[j]) >= 0 {
			t.Fatalf("interleaved end keys out of order at %d: %x >= %x", j, order[j-1], order[j])
		}
	}
}
