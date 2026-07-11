package list

import (
	"bytes"
	"testing"
)

// Table tests for the bounded interior surgery (spec 2064/f3/13 sections 5.6 to
// 5.8): LINSERT as a pivot scan plus in-chunk repack or split, LREM on a
// count-signed bounded scan, LTRIM as a chunk-range delete, and the LSET
// length-change that shares the LINSERT machinery. Each case applies the op to
// the real deque and to a listModel oracle, then asserts the whole structure:
// the element order matches, count equals the sum of live chunk counts, bytes
// equals the sum of element lengths, every dense index resolves through the real
// locate, and the flat scan and the Fenwick descent still agree at every index.
// That last check is the load-bearing one: a stale or wrong directory after a
// split or an unlink is the main risk this slice carries.

// checkNative asserts every structural invariant of nt against the want oracle.
func checkNative(t *testing.T, nt *native, want [][]byte, label string) {
	t.Helper()
	if nt.count != len(want) {
		t.Fatalf("%s: count %d, want %d", label, nt.count, len(want))
	}
	sum := 0
	for i := 0; i < nt.ring.n; i++ {
		sum += nt.ring.at(i).count()
	}
	if sum != nt.count {
		t.Fatalf("%s: sum of chunk counts %d, count %d", label, sum, nt.count)
	}
	wantBytes := 0
	for _, v := range want {
		wantBytes += len(v)
	}
	if nt.bytes != wantBytes {
		t.Fatalf("%s: bytes %d, want %d", label, nt.bytes, wantBytes)
	}
	// each() order.
	var got [][]byte
	nt.each(func(v []byte) { got = append(got, append([]byte(nil), v...)) })
	assertSeq(t, got, want, label+" each")
	// toSlice() order, exercising the materializer that backs the empty-trim path.
	assertSeq(t, nt.toSlice(), want, label+" toSlice")
	// Every dense index through the real locate (flat at or below flatMax, Fenwick
	// above), so a positional read after surgery lands on the model's element.
	for k := range want {
		if !bytes.Equal(nt.at(k), want[k]) {
			t.Fatalf("%s: at(%d) mismatch", label, k)
		}
	}
	if nt.count > 0 {
		checkLocateAgree(t, nt, label)
	}
}

func assertSeq(t *testing.T, got, want [][]byte, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: len %d, want %d", label, len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("%s: elem %d = %q, want %q", label, i, got[i], want[i])
		}
	}
}

// vseq builds n distinct values of the given byte size, tagged by index so a
// pivot or a positional read is unambiguous.
func vseq(n, size int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = sized(size, i)
	}
	return out
}

func TestInsertSurgery(t *testing.T) {
	// pivot at head, middle, and tail, before and after, on a small multi-value
	// list that stays inside one chunk.
	base := func() [][]byte { return bb("a", "b", "c", "d", "e") }
	cases := []struct {
		name   string
		before bool
		pivot  string
		v      string
		want   []string
	}{
		{"before-head", true, "a", "X", []string{"X", "a", "b", "c", "d", "e"}},
		{"after-head", false, "a", "X", []string{"a", "X", "b", "c", "d", "e"}},
		{"before-mid", true, "c", "X", []string{"a", "b", "X", "c", "d", "e"}},
		{"after-mid", false, "c", "X", []string{"a", "b", "c", "X", "d", "e"}},
		{"before-tail", true, "e", "X", []string{"a", "b", "c", "d", "X", "e"}},
		{"after-tail", false, "e", "X", []string{"a", "b", "c", "d", "e", "X"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nt := buildNativeVals(base())
			if !nt.insert(tc.before, []byte(tc.pivot), []byte(tc.v)) {
				t.Fatal("pivot reported missing")
			}
			checkNative(t, nt, bb(tc.want...), tc.name)
		})
	}

	t.Run("missing-pivot-noop", func(t *testing.T) {
		nt := buildNativeVals(base())
		if nt.insert(true, []byte("zzz"), []byte("X")) {
			t.Fatal("insert claimed a match for a missing pivot")
		}
		checkNative(t, nt, base(), "missing-pivot")
	})

	// An insertion that overflows the pivot's chunk must split it: 24 values of
	// ~200 bytes pack a chunk to the byte budget, so inserting one more forces a
	// split and a middle-of-ring link.
	t.Run("chunk-split", func(t *testing.T) {
		vals := vseq(24, 200)
		nt := buildNativeVals(vals)
		before := nt.ring.n
		pivot := append([]byte(nil), vals[10]...)
		if !nt.insert(true, pivot, sized(200, 999)) {
			t.Fatal("pivot reported missing")
		}
		if nt.ring.n <= before {
			t.Fatalf("insert did not split: chunks %d -> %d", before, nt.ring.n)
		}
		want := make([][]byte, 0, 25)
		want = append(want, vals[:10]...)
		want = append(want, sized(200, 999))
		want = append(want, vals[10:]...)
		checkNative(t, nt, want, "chunk-split")
	})

	// An insert past the crossover so the Fenwick descent resolves the edited ring.
	t.Run("past-crossover", func(t *testing.T) {
		vals := vseq(130*chunkElemCap, 4) // 130 full chunks of 4-byte values
		nt := buildNativeVals(vals)
		if nt.ring.n <= flatMax {
			t.Fatalf("setup only built %d chunks, need past %d", nt.ring.n, flatMax)
		}
		pivot := append([]byte(nil), vals[5000]...)
		if !nt.insert(false, pivot, sized(4, 424242)) {
			t.Fatal("pivot reported missing")
		}
		want := make([][]byte, 0, len(vals)+1)
		want = append(want, vals[:5001]...)
		want = append(want, sized(4, 424242))
		want = append(want, vals[5001:]...)
		checkNative(t, nt, want, "past-crossover")
	})
}

func TestRemoveSurgery(t *testing.T) {
	// matches at head, middle, and tail; count sign controls direction and cap.
	base := func() [][]byte { return bb("a", "x", "b", "x", "c", "x", "d") }
	cases := []struct {
		name    string
		count   int
		v       string
		want    []string
		removed int
	}{
		{"all", 0, "x", []string{"a", "b", "c", "d"}, 3},
		{"first-two", 2, "x", []string{"a", "b", "c", "x", "d"}, 2},
		{"last-one", -1, "x", []string{"a", "x", "b", "x", "c", "d"}, 1},
		{"last-two", -2, "x", []string{"a", "x", "b", "c", "d"}, 2},
		{"over-cap", 9, "x", []string{"a", "b", "c", "d"}, 3},
		{"no-match", 3, "zz", []string{"a", "x", "b", "x", "c", "x", "d"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nt := buildNativeVals(base())
			got := nt.remove(tc.count, []byte(tc.v))
			if got != tc.removed {
				t.Fatalf("removed %d, want %d", got, tc.removed)
			}
			checkNative(t, nt, bb(tc.want...), tc.name)
		})
	}

	// A whole chunk made of the target value is unlinked by the removal, not
	// renumbered: chunk 0 is keepA, chunk 1 is all DEL, chunk 2 is keepB.
	t.Run("chunk-unlinked", func(t *testing.T) {
		vals := make([][]byte, 0, 3*chunkElemCap)
		for i := 0; i < chunkElemCap; i++ {
			vals = append(vals, sized(6, i)) // distinct, chunk 0
		}
		for i := 0; i < chunkElemCap; i++ {
			vals = append(vals, []byte("DEL")) // chunk 1, all victims
		}
		for i := 0; i < chunkElemCap; i++ {
			vals = append(vals, sized(7, i)) // distinct, chunk 2
		}
		nt := buildNativeVals(vals)
		before := nt.ring.n
		got := nt.remove(0, []byte("DEL"))
		if got != chunkElemCap {
			t.Fatalf("removed %d, want %d", got, chunkElemCap)
		}
		if nt.ring.n != before-1 {
			t.Fatalf("emptied chunk not unlinked: chunks %d -> %d", before, nt.ring.n)
		}
		want := make([][]byte, 0, 2*chunkElemCap)
		want = append(want, vals[:chunkElemCap]...)
		want = append(want, vals[2*chunkElemCap:]...)
		checkNative(t, nt, want, "chunk-unlinked")
	})

	// Removal past the crossover keeps the Fenwick consistent after unlinks.
	t.Run("past-crossover", func(t *testing.T) {
		vals := make([][]byte, 0, 131*chunkElemCap)
		for i := 0; i < 130*chunkElemCap; i++ {
			vals = append(vals, sized(4, i))
		}
		// Scatter a repeated victim across the tail so several removals land.
		for i := 0; i < chunkElemCap; i++ {
			vals = append(vals, []byte("VV"))
		}
		nt := buildNativeVals(vals)
		got := nt.remove(0, []byte("VV"))
		if got != chunkElemCap {
			t.Fatalf("removed %d, want %d", got, chunkElemCap)
		}
		checkNative(t, nt, vals[:130*chunkElemCap], "past-crossover")
	})
}

func TestTrimSurgery(t *testing.T) {
	base := func() [][]byte { return bb("a", "b", "c", "d", "e", "f", "g") }
	cases := []struct {
		name        string
		start, stop int
		want        []string
	}{
		{"middle", 2, 4, []string{"c", "d", "e"}},
		{"whole", 0, 6, []string{"a", "b", "c", "d", "e", "f", "g"}},
		{"single", 3, 3, []string{"d"}},
		{"head", 0, 2, []string{"a", "b", "c"}},
		{"tail", 4, 6, []string{"e", "f", "g"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nt := buildNativeVals(base())
			nt.trim(tc.start, tc.stop)
			checkNative(t, nt, bb(tc.want...), tc.name)
		})
	}

	t.Run("empty", func(t *testing.T) {
		nt := buildNativeVals(base())
		nt.trim(1, 0) // the caller's empty-range signal
		checkNative(t, nt, nil, "empty")
	})

	// A trim whose window sits inside one chunk vs one that spans chunks, both on
	// a multi-chunk list, so the same-chunk and boundary-pair paths are covered.
	t.Run("same-chunk", func(t *testing.T) {
		vals := vseq(5*chunkElemCap, 4)
		nt := buildNativeVals(vals)
		lo, hi := chunkElemCap+10, chunkElemCap+40 // both inside chunk 1
		nt.trim(lo, hi)
		checkNative(t, nt, vals[lo:hi+1], "same-chunk")
	})
	t.Run("spanning", func(t *testing.T) {
		vals := vseq(5*chunkElemCap, 4)
		nt := buildNativeVals(vals)
		lo, hi := chunkElemCap+10, 3*chunkElemCap+40 // chunk 1 into chunk 3
		nt.trim(lo, hi)
		checkNative(t, nt, vals[lo:hi+1], "spanning")
	})

	// A trim past the crossover to a small window: drops most chunks, keeps a
	// boundary-trimmed pair, and the Fenwick must still resolve the survivors.
	t.Run("past-crossover", func(t *testing.T) {
		vals := vseq(200*chunkElemCap, 4)
		nt := buildNativeVals(vals)
		lo, hi := 50*chunkElemCap+7, 50*chunkElemCap+300
		nt.trim(lo, hi)
		checkNative(t, nt, vals[lo:hi+1], "past-crossover")
	})
}

// TestSetLengthChangeSurgery covers LSET's growing and shrinking value paths,
// including a grow that overflows the chunk and splits it, all through the shared
// spliceChunk machinery.
func TestSetLengthChangeSurgery(t *testing.T) {
	t.Run("grow-in-place", func(t *testing.T) {
		nt := buildNativeVals(bb("a", "b", "c"))
		nt.setAt(1, []byte("bbbbbb"))
		checkNative(t, nt, bb("a", "bbbbbb", "c"), "grow-in-place")
	})
	t.Run("shrink-in-place", func(t *testing.T) {
		nt := buildNativeVals(bb("aaaa", "bbbb", "cccc"))
		nt.setAt(1, []byte("b"))
		checkNative(t, nt, bb("aaaa", "b", "cccc"), "shrink-in-place")
	})
	t.Run("grow-splits-chunk", func(t *testing.T) {
		vals := vseq(24, 200) // one chunk near the byte budget
		nt := buildNativeVals(vals)
		before := nt.ring.n
		nt.setAt(12, sized(400, 777)) // a much larger value overflows the chunk
		if nt.ring.n <= before {
			t.Fatalf("length-change set did not split: chunks %d -> %d", before, nt.ring.n)
		}
		want := append([][]byte(nil), vals...)
		want[12] = sized(400, 777)
		checkNative(t, nt, want, "grow-splits-chunk")
	})
}

// TestInsertOversizedValue inserts a value wider than the blob budget, which must
// land in its own oversized chunk between the split halves.
func TestInsertOversizedValue(t *testing.T) {
	vals := vseq(10, 200)
	nt := buildNativeVals(vals)
	big := sized(5000, 321) // beyond chunkBlobCap, a lone oversized frame
	pivot := append([]byte(nil), vals[5]...)
	if !nt.insert(false, pivot, big) {
		t.Fatal("pivot reported missing")
	}
	want := make([][]byte, 0, 11)
	want = append(want, vals[:6]...)
	want = append(want, big)
	want = append(want, vals[6:]...)
	checkNative(t, nt, want, "oversized")
}

// TestRingInsertRemoveAt exercises the middle-of-ring handle splice and unlink
// directly across ring sizes, since insertAt/removeAt back every interior split
// and empty-chunk drop and their modulo bookkeeping is easy to get wrong.
func TestRingInsertRemoveAt(t *testing.T) {
	for _, n := range []int{1, 2, 3, 8, 33} {
		mk := func() *chunkRing {
			r := &chunkRing{}
			for i := 0; i < n; i++ {
				r.pushTail(&chunk{bytesUsed: i}) // tag by bytesUsed to track identity
			}
			return r
		}
		for at := 0; at <= n; at++ {
			r := mk()
			mark := &chunk{bytesUsed: -1}
			r.insertAt(at, mark)
			if r.n != n+1 {
				t.Fatalf("n=%d insertAt(%d): size %d", n, at, r.n)
			}
			if r.at(at) != mark {
				t.Fatalf("n=%d insertAt(%d): mark not at position", n, at)
			}
			// The original tags stay in order around the insertion.
			seq := make([]int, 0, r.n)
			for i := 0; i < r.n; i++ {
				seq = append(seq, r.at(i).bytesUsed)
			}
			expect := make([]int, 0, r.n)
			for i := 0; i < at; i++ {
				expect = append(expect, i)
			}
			expect = append(expect, -1)
			for i := at; i < n; i++ {
				expect = append(expect, i)
			}
			for i := range expect {
				if seq[i] != expect[i] {
					t.Fatalf("n=%d insertAt(%d): seq %v want %v", n, at, seq, expect)
				}
			}
		}
		for at := 0; at < n; at++ {
			r := mk()
			r.removeAt(at)
			if r.n != n-1 {
				t.Fatalf("n=%d removeAt(%d): size %d", n, at, r.n)
			}
			seq := make([]int, 0, r.n)
			for i := 0; i < r.n; i++ {
				seq = append(seq, r.at(i).bytesUsed)
			}
			expect := make([]int, 0, r.n)
			for i := 0; i < n; i++ {
				if i != at {
					expect = append(expect, i)
				}
			}
			for i := range expect {
				if seq[i] != expect[i] {
					t.Fatalf("n=%d removeAt(%d): seq %v want %v", n, at, seq, expect)
				}
			}
		}
	}
}

// TestSurgerySequenceKeepsFenwick drives a mixed head/tail push, interior insert,
// remove, length-change set, and trim stream on a list that starts past the
// crossover and rechecks the full structure after every step, so the directory
// is proven through repeated splits and unlinks rather than one static shape.
func TestSurgerySequenceKeepsFenwick(t *testing.T) {
	vals := vseq(140*chunkElemCap, 4)
	nt := buildNativeVals(vals)
	m := &listModel{}
	for _, v := range vals {
		m.pushBack(v)
	}
	step := func(desc string) { checkNative(t, nt, m.e, desc) }

	// Insert a repeated token so later removes and pivots hit.
	for i := 0; i < 40; i++ {
		idx := i * 300
		pivot := append([]byte(nil), m.e[idx]...)
		nt.insert(i%2 == 0, pivot, []byte("TOK"))
		m.insert(i%2 == 0, pivot, []byte("TOK"))
	}
	step("after inserts")

	nt.remove(0, []byte("TOK"))
	m.remove(0, []byte("TOK"))
	step("after remove-all")

	for i := 0; i < 20; i++ {
		idx := i * 500
		nt.setAt(idx, sized(1200, i)) // length-changing sets that split chunks
		m.setAt(idx, sized(1200, i))
	}
	step("after grows")

	nt.trim(1000, 9000)
	m.trim(1000, 9000)
	step("after trim")

	nt.remove(-3, []byte("TOK"))
	m.remove(-3, []byte("TOK"))
	step("after backward remove")
}
