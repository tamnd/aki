package list

import (
	"bytes"
	"strconv"
	"testing"
)

// The native LPOS scan (native.lpos) walks the chunk frames contiguously instead
// of resolving each index through the directory. This differential pins that the
// contiguous walk returns the identical result to a flat linear reference over
// the same elements, across the full RANK/COUNT/MAXLEN matrix, on lists sized to
// span many chunks and on a small native list, and with the target landing on
// chunk boundaries. The reference is a plain [][]byte scan implementing the same
// rule bookkeeping, so it is an independent oracle for the traversal, not a copy
// of the scanner under test.

// refLpos is the flat oracle: it applies the RANK direction, the RANK-1 skip, the
// COUNT limit, and the MAXLEN compared-count bound to a slice, the same rules
// lposScan and native.lpos implement, walking element by element with no chunk
// geometry.
func refLpos(elems [][]byte, target []byte, rank, limit, maxlen int) []int {
	forward := rank > 0
	skip := rank
	if skip < 0 {
		skip = -skip
	}
	skip--
	var out []int
	compared := 0
	visit := func(i int) bool {
		if maxlen > 0 && compared >= maxlen {
			return false
		}
		compared++
		if !bytes.Equal(elems[i], target) {
			return true
		}
		if skip > 0 {
			skip--
			return true
		}
		out = append(out, i)
		return limit <= 0 || len(out) < limit
	}
	if forward {
		for i := 0; i < len(elems); i++ {
			if !visit(i) {
				break
			}
		}
	} else {
		for i := len(elems) - 1; i >= 0; i-- {
			if !visit(i) {
				break
			}
		}
	}
	return out
}

// lposElems builds n filler elements with the target token injected at the
// positions want reports true for, so a test can place the token once, many
// times, or never, and at known chunk offsets. Fillers are 6 bytes so they never
// equal the 3-byte target.
func lposElems(n int, want func(i int) bool, target []byte) [][]byte {
	elems := make([][]byte, n)
	for i := range elems {
		if want(i) {
			elems[i] = append([]byte(nil), target...)
			continue
		}
		elems[i] = sized(6, i)
	}
	return elems
}

// checkLposMatrix runs the full RANK/COUNT/MAXLEN matrix over elems, comparing the
// native contiguous scan to the flat reference for every combination.
func checkLposMatrix(t *testing.T, tag string, elems [][]byte, target []byte) {
	t.Helper()
	nt := buildNativeVals(elems)
	l := &list{nat: nt, everLarge: true}
	if l.length() != len(elems) {
		t.Fatalf("%s: native length %d, want %d", tag, l.length(), len(elems))
	}
	ranks := []int{1, 2, 3, -1, -2, -3}
	counts := []int{-1, 0, 1, 2, 100}
	maxlens := []int{0, 1, 5, 1000}
	for _, rank := range ranks {
		for _, count := range counts {
			for _, maxlen := range maxlens {
				got := lposScan(l, target, rank, count, maxlen)
				want := refLpos(elems, target, rank, count, maxlen)
				if !equalInts(got, want) {
					t.Fatalf("%s: rank=%d count=%d maxlen=%d\n got=%v\nwant=%v",
						tag, rank, count, maxlen, got, want)
				}
			}
		}
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestNativeLposMatrix(t *testing.T) {
	target := []byte("TGT")

	// Small native list: a few chunks (300 elems is ~3 chunks at the 128-element
	// cap), so the walk crosses chunk boundaries but stays under the flat/Fenwick
	// crossover.
	t.Run("small", func(t *testing.T) {
		checkLposMatrix(t, "small/multi",
			lposElems(300, func(i int) bool { return i%37 == 0 }, target), target)
		checkLposMatrix(t, "small/once",
			lposElems(300, func(i int) bool { return i == 150 }, target), target)
		checkLposMatrix(t, "small/absent",
			lposElems(300, func(i int) bool { return false }, target), target)
	})

	// Large native list: past 128 chunks (20000 elems is ~157 chunks), exercising
	// the deep-ring geometry the per-index path paid a Fenwick descent per element
	// to walk.
	t.Run("large", func(t *testing.T) {
		checkLposMatrix(t, "large/multi",
			lposElems(20000, func(i int) bool { return i%137 == 0 }, target), target)
		checkLposMatrix(t, "large/once",
			lposElems(20000, func(i int) bool { return i == 12345 }, target), target)
		checkLposMatrix(t, "large/absent",
			lposElems(20000, func(i int) bool { return false }, target), target)
	})
}

// TestNativeLposChunkBoundary places the target as the first and last element of
// interior chunks. buildNativeVals fills chunks to chunkElemCap before sealing, so
// element chunkElemCap is the first of chunk 1 and chunkElemCap*2-1 is the last of
// chunk 1; both boundary offsets must resolve to the same positions the flat
// reference reports.
func TestNativeLposChunkBoundary(t *testing.T) {
	target := []byte("TGT")
	firstOfChunk := chunkElemCap      // first live frame of chunk 1
	lastOfChunk := chunkElemCap*2 - 1 // last live frame of chunk 1
	elems := lposElems(chunkElemCap*3, func(i int) bool {
		return i == firstOfChunk || i == lastOfChunk
	}, target)
	// Sanity: the two boundary indices are exactly where the reference finds them.
	want := refLpos(elems, target, 1, 0, 0)
	if !equalInts(want, []int{firstOfChunk, lastOfChunk}) {
		t.Fatalf("reference boundary positions = %v, want [%d %d]", want, firstOfChunk, lastOfChunk)
	}
	checkLposMatrix(t, "boundary", elems, target)
}

// TestNativeLposMixedSizes runs the matrix over a list whose values span the size
// bands (so chunks hold different element counts and the ring geometry is
// irregular), with the target sprinkled through, to catch any dependence on a
// uniform chunk fill.
func TestNativeLposMixedSizes(t *testing.T) {
	target := []byte("TGT")
	const n = 4000
	elems := make([][]byte, n)
	for i := range elems {
		switch {
		case i%50 == 0:
			elems[i] = append([]byte(nil), target...)
		case i%3 == 0:
			elems[i] = sized(64, i)
		case i%3 == 1:
			elems[i] = sized(1024, i)
		default:
			elems[i] = sized(6, i)
		}
	}
	checkLposMatrix(t, "mixed/"+strconv.Itoa(n), elems, target)
}
