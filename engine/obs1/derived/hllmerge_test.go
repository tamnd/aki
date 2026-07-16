package derived

import "testing"

// foldedEstimate folds a set of sketches through foldInto and estimates the
// union straight from the scratch, the multi-key PFCOUNT read path.
func foldedEstimate(t *testing.T, blobs ...[]byte) uint64 {
	t.Helper()
	acc := make([]byte, hllRegisters)
	tmp := make([]byte, hllRegisters)
	for _, b := range blobs {
		if !foldInto(acc, tmp, b) {
			t.Fatal("foldInto reported corruption on a valid sketch")
		}
	}
	var histo [64]int
	scratchHisto(acc, &histo)
	return estimateHisto(&histo)
}

func TestSwarMaxInto(t *testing.T) {
	// Every register-value pair 0..63 must fold to the larger byte.
	dst := make([]byte, 64*64)
	src := make([]byte, len(dst))
	k := 0
	for a := 0; a <= hllRegMax; a++ {
		for b := 0; b <= hllRegMax; b++ {
			dst[k], src[k] = byte(a), byte(b)
			k++
		}
	}
	swarMaxInto(dst, src)
	k = 0
	for a := 0; a <= hllRegMax; a++ {
		for b := 0; b <= hllRegMax; b++ {
			want := byte(a)
			if b > a {
				want = byte(b)
			}
			if dst[k] != want {
				t.Fatalf("max(%d,%d) = %d, want %d", a, b, dst[k], want)
			}
			k++
		}
	}
}

// TestFoldIsExactUnion pins HLL's exact mergeability: the sketch of a union of
// element sets equals the register-max fold of their separate sketches, so the
// folded estimate matches a sketch built from all the elements at once, and the
// repacked dense registers are byte-identical.
func TestFoldIsExactUnion(t *testing.T) {
	a := addAllRange(t, createSparse(), 0, 3000)
	b := addAllRange(t, createSparse(), 2000, 6000) // overlaps a on 2000..3000
	whole := addAllRange(t, createSparse(), 0, 6000)

	acc := make([]byte, hllRegisters)
	tmp := make([]byte, hllRegisters)
	if !foldInto(acc, tmp, a) || !foldInto(acc, tmp, b) {
		t.Fatal("foldInto reported corruption")
	}
	merged := denseFromScratch(acc)

	wholeDense, ok := sparseToDense(whole)
	if !ok {
		// whole may already be dense after 6000 adds; use it directly.
		wholeDense = whole
	}
	// Registers must match byte-for-byte (skip the 16-byte header, whose cache
	// bytes differ by construction).
	if string(merged[hllHdrSize:]) != string(wholeDense[hllHdrSize:]) {
		t.Fatal("folded registers differ from the all-at-once union sketch")
	}
	if foldedEstimate(t, a, b) != foldedEstimate(t, whole) {
		t.Fatal("folded union estimate differs from the whole-set estimate")
	}
}

// TestFoldOrderIndependent checks the fold is commutative: any visitation order
// of the sources yields the same registers.
func TestFoldOrderIndependent(t *testing.T) {
	s := []([]byte){
		addAllRange(t, createSparse(), 0, 1000),
		addAllRange(t, createSparse(), 500, 2500),
		addAllRange(t, createSparse(), 2000, 4000),
	}
	fold := func(order []int) []byte {
		acc := make([]byte, hllRegisters)
		tmp := make([]byte, hllRegisters)
		for _, i := range order {
			foldInto(acc, tmp, s[i])
		}
		return denseFromScratch(acc)
	}
	base := fold([]int{0, 1, 2})
	for _, order := range [][]int{{2, 1, 0}, {1, 0, 2}, {2, 0, 1}} {
		if string(fold(order)[hllHdrSize:]) != string(base[hllHdrSize:]) {
			t.Fatalf("fold order %v changed the registers", order)
		}
	}
}

// TestFoldSparseAndDenseAgree folds a sparse sketch and its densified twin and
// confirms they contribute the identical registers.
func TestFoldSparseAndDenseAgree(t *testing.T) {
	sparse := addAllRange(t, createSparse(), 0, 500)
	if sparse[4] != hllSparse {
		t.Fatalf("500 elements already dense (enc=%d)", sparse[4])
	}
	dense, ok := sparseToDense(sparse)
	if !ok {
		t.Fatal("sparseToDense reported corruption")
	}
	accS := make([]byte, hllRegisters)
	accD := make([]byte, hllRegisters)
	tmp := make([]byte, hllRegisters)
	foldInto(accS, tmp, sparse)
	foldInto(accD, tmp, dense)
	if string(accS) != string(accD) {
		t.Fatal("sparse and dense folds of the same sketch differ")
	}
}

// TestDenseFromScratchRoundtrip confirms unpack and repack are inverses on a
// filled dense sketch.
func TestDenseFromScratchRoundtrip(t *testing.T) {
	whole := addAllRange(t, createSparse(), 0, 40000) // dense by now
	if whole[4] != hllDense {
		t.Fatalf("40000 elements not dense (enc=%d)", whole[4])
	}
	tmp := make([]byte, hllRegisters)
	unpackDenseInto(whole[hllHdrSize:], tmp)
	back := denseFromScratch(tmp)
	if string(back[hllHdrSize:]) != string(whole[hllHdrSize:]) {
		t.Fatal("unpack then repack changed the dense registers")
	}
}
