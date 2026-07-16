package derived

import (
	"fmt"
	"math"
	"testing"
)

// addAll applies a run of elements to a sketch through the same hllAdd path
// PFADD walks, returning the final (possibly promoted) blob. It is the codec
// pipeline under test with the Ctx wrapper peeled off.
func addAll(t *testing.T, blob []byte, eles ...[]byte) []byte {
	t.Helper()
	for _, e := range eles {
		nb, ret := hllAdd(blob, e)
		if ret < 0 {
			t.Fatalf("hllAdd(%q) reported corruption", e)
		}
		blob = nb
	}
	return blob
}

// ele renders a distinct element for cardinality i, the Redis test's ele-%d
// shape so the spread across registers matches a real workload.
func ele(i int) []byte { return []byte(fmt.Sprintf("ele-%d", i)) }

func TestMurmurDeterministic(t *testing.T) {
	a := murmurHash64A([]byte("hello world"), hllHashSeed)
	b := murmurHash64A([]byte("hello world"), hllHashSeed)
	if a != b {
		t.Fatalf("hash not deterministic: %#x vs %#x", a, b)
	}
	if murmurHash64A([]byte("hello worlx"), hllHashSeed) == a {
		t.Fatal("distinct inputs collided on a one-byte change")
	}
	// Every tail length 1..7 must exercise the fallthrough switch without panic
	// and stay stable across calls.
	for n := 1; n <= 15; n++ {
		buf := make([]byte, n)
		for i := range buf {
			buf[i] = byte('a' + i)
		}
		cp := append([]byte(nil), buf...)
		if murmurHash64A(buf, hllHashSeed) != murmurHash64A(cp, hllHashSeed) {
			t.Fatalf("len %d not deterministic", n)
		}
	}
}

func TestPatLenBounds(t *testing.T) {
	for i := 0; i < 100000; i++ {
		index, count := hllPatLen(ele(i))
		if index < 0 || index >= hllRegisters {
			t.Fatalf("index %d out of range for %q", index, ele(i))
		}
		if count < 1 || int(count) > hllQ+1 {
			t.Fatalf("count %d out of range for %q", count, ele(i))
		}
	}
}

func TestIsHLL(t *testing.T) {
	if !isHLL(createSparse()) {
		t.Fatal("fresh sparse sketch failed validation")
	}
	dense := make([]byte, hllDenseSize)
	dense[0], dense[1], dense[2], dense[3] = 'H', 'Y', 'L', 'L'
	dense[4] = hllDense
	if !isHLL(dense) {
		t.Fatal("well-formed dense sketch failed validation")
	}
	// Bad magic, unknown encoding, and a short dense record are all rejected.
	bad := createSparse()
	bad[0] = 'X'
	if isHLL(bad) {
		t.Fatal("bad magic accepted")
	}
	enc := createSparse()
	enc[4] = 9
	if isHLL(enc) {
		t.Fatal("unknown encoding accepted")
	}
	short := dense[:hllDenseSize-1]
	if isHLL(short) {
		t.Fatal("short dense record accepted")
	}
	if isHLL([]byte("HY")) {
		t.Fatal("sub-header blob accepted")
	}
}

func TestCreateSparseIsSingleXZero(t *testing.T) {
	blob := createSparse()
	if len(blob) != hllHdrSize+2 {
		t.Fatalf("fresh sparse len = %d; want %d", len(blob), hllHdrSize+2)
	}
	// XZERO(16384) packs to {0x7f, 0xff}; the whole register file is one run.
	if blob[hllHdrSize] != 0x7f || blob[hllHdrSize+1] != 0xff {
		t.Fatalf("fresh XZERO = %#x %#x; want 0x7f 0xff", blob[hllHdrSize], blob[hllHdrSize+1])
	}
	if c, ok := hllCount(blob); !ok || c != 0 {
		t.Fatalf("empty sketch count = %d, ok=%v; want 0, true", c, ok)
	}
}

func TestSparseAddGrows(t *testing.T) {
	blob := createSparse()
	blob = addAll(t, blob, ele(1))
	if blob[4] != hllSparse {
		t.Fatalf("one element promoted to dense prematurely (enc=%d)", blob[4])
	}
	if c, _ := hllCount(blob); c != 1 {
		t.Fatalf("count after one distinct add = %d; want 1", c)
	}
	// The same element again does not change the sketch bytes.
	before := string(blob)
	nb, ret := hllAdd(blob, ele(1))
	if ret != 0 {
		t.Fatalf("re-adding a present element grew the sketch (ret=%d)", ret)
	}
	if string(nb) != before {
		t.Fatal("re-adding a present element changed the bytes")
	}
}

func TestSparseDenseAgree(t *testing.T) {
	// A few hundred elements keep it sparse; the sparse and densified histograms
	// must estimate the same cardinality.
	sparse := addAllRange(t, createSparse(), 0, 400)
	if sparse[4] != hllSparse {
		t.Fatalf("400 elements already dense (enc=%d)", sparse[4])
	}
	dense, ok := sparseToDense(sparse)
	if !ok {
		t.Fatal("sparseToDense reported corruption on a valid sketch")
	}
	cs, _ := hllCount(sparse)
	cd, _ := hllCount(dense)
	if cs != cd {
		t.Fatalf("sparse count %d != dense count %d", cs, cd)
	}
}

func TestPromotionToDense(t *testing.T) {
	// Enough distinct elements drive the sparse form past its byte budget and it
	// promotes to dense; the count keeps tracking across the transition.
	blob := createSparse()
	promoted := false
	for i := 0; i < 20000; i++ {
		blob = addAll(t, blob, ele(i))
		if blob[4] == hllDense {
			promoted = true
			break
		}
	}
	if !promoted {
		t.Fatal("20000 distinct elements never promoted to dense")
	}
	if len(blob) != hllDenseSize {
		t.Fatalf("promoted blob len = %d; want %d", len(blob), hllDenseSize)
	}
}

func TestEstimateAccuracy(t *testing.T) {
	// The HLL standard error is 1.04/sqrt(m) ~= 0.81%; allow a comfortable 4x
	// band so the test pins the estimator's constants without being flaky.
	const slack = 0.033
	for _, n := range []int{10, 100, 1000, 5000, 20000, 100000} {
		blob := addAllRange(t, createSparse(), 0, n)
		got, ok := hllCount(blob)
		if !ok {
			t.Fatalf("n=%d: count reported corruption", n)
		}
		rel := math.Abs(float64(got)-float64(n)) / float64(n)
		if rel > slack {
			t.Fatalf("n=%d: estimate %d, relative error %.4f > %.4f", n, got, rel, slack)
		}
	}
}

// addAllRange adds ele(lo)..ele(hi-1) to blob, promoting as needed.
func addAllRange(t *testing.T, blob []byte, lo, hi int) []byte {
	t.Helper()
	for i := lo; i < hi; i++ {
		nb, ret := hllAdd(blob, ele(i))
		if ret < 0 {
			t.Fatalf("hllAdd(%q) reported corruption", ele(i))
		}
		blob = nb
	}
	return blob
}
