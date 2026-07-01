package f1srv

import (
	"fmt"
	"strconv"
	"testing"
)

// PFADD creates a sketch and reports whether a register changed; PFCOUNT reads the estimate. For
// small exact sets the estimate is exact, so adding three distinct members yields a count of 3.
func TestPfAddCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// First add creates the key and changes registers -> 1.
	cmd(t, rw, "PFADD", "hll", "a", "b", "c")
	expect(t, rw, ":1")
	cmd(t, rw, "PFCOUNT", "hll")
	expect(t, rw, ":3")

	// Re-adding the same members changes nothing -> 0, and the count is unchanged.
	cmd(t, rw, "PFADD", "hll", "a", "b", "c")
	expect(t, rw, ":0")
	cmd(t, rw, "PFCOUNT", "hll")
	expect(t, rw, ":3")

	// A brand new member moves a register -> 1 and bumps the count.
	cmd(t, rw, "PFADD", "hll", "d")
	expect(t, rw, ":1")
	cmd(t, rw, "PFCOUNT", "hll")
	expect(t, rw, ":4")

	// PFADD with no members on a missing key creates it and returns 1; on an existing key returns 0.
	cmd(t, rw, "PFADD", "empty")
	expect(t, rw, ":1")
	cmd(t, rw, "PFCOUNT", "empty")
	expect(t, rw, ":0")
	cmd(t, rw, "PFADD", "hll")
	expect(t, rw, ":0")

	// A missing key counts as zero.
	cmd(t, rw, "PFCOUNT", "nope")
	expect(t, rw, ":0")

	// TYPE of an HLL is string.
	cmd(t, rw, "TYPE", "hll")
	expect(t, rw, "+string")
}

// A larger insert still lands within HLL's standard-error envelope; at p=14 the error is ~0.8%.
func TestPfCountApproxLarge(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	const n = 20000
	batch := make([]string, 0, 3)
	for i := 0; i < n; i++ {
		batch = append(batch[:0], "PFADD", "big", "elem:"+strconv.Itoa(i))
		cmd(t, rw, batch...)
		readReply(t, rw) // :0 or :1, don't care which
	}
	cmd(t, rw, "PFCOUNT", "big")
	got := readReply(t, rw)
	if got == "" || got[0] != ':' {
		t.Fatalf("PFCOUNT reply = %q, want integer", got)
	}
	est, err := strconv.Atoi(got[1:])
	if err != nil {
		t.Fatalf("bad integer %q: %v", got, err)
	}
	// 3% band leaves margin for the probabilistic tail while still catching a broken estimator.
	lo, hi := int(float64(n)*0.97), int(float64(n)*1.03)
	if est < lo || est > hi {
		t.Fatalf("PFCOUNT = %d, want within [%d, %d]", est, lo, hi)
	}
}

// PFMERGE unions sources into a destination; the merged count is the cardinality of the union, and
// disjoint sources add up while overlaps do not double-count.
func TestPfMerge(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "PFADD", "s1", "a", "b", "c")
	expect(t, rw, ":1")
	cmd(t, rw, "PFADD", "s2", "c", "d", "e")
	expect(t, rw, ":1")

	// Union of {a,b,c} and {c,d,e} is {a,b,c,d,e} -> 5.
	cmd(t, rw, "PFMERGE", "dst", "s1", "s2")
	expect(t, rw, "+OK")
	cmd(t, rw, "PFCOUNT", "dst")
	expect(t, rw, ":5")

	// Multi-key PFCOUNT computes the same union without persisting and without touching the sources.
	cmd(t, rw, "PFCOUNT", "s1", "s2")
	expect(t, rw, ":5")
	cmd(t, rw, "PFCOUNT", "s1")
	expect(t, rw, ":3")

	// Merging into an existing destination folds it in too (dst already holds the union of 5).
	cmd(t, rw, "PFADD", "s3", "f")
	expect(t, rw, ":1")
	cmd(t, rw, "PFMERGE", "dst", "s3")
	expect(t, rw, "+OK")
	cmd(t, rw, "PFCOUNT", "dst")
	expect(t, rw, ":6")
}

// A non-string collection under an HLL key is a WRONGTYPE; a plain string that is not a valid HLL is
// the HLL-specific not-valid error.
func TestPfErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "x")
	expect(t, rw, ":1")
	cmd(t, rw, "PFADD", "l", "a")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "PFCOUNT", "l")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "PFMERGE", "dst", "l")
	expect(t, rw, "-"+wrongType)

	// A string that is not a HYLL blob.
	cmd(t, rw, "SET", "str", "not an hll")
	expect(t, rw, "+OK")
	cmd(t, rw, "PFADD", "str", "a")
	expect(t, rw, "-"+hllNotValidErr)
	cmd(t, rw, "PFCOUNT", "str")
	expect(t, rw, "-"+hllNotValidErr)
}

// PFDEBUG ENCODING starts sparse and flips to dense once the sketch is forced dense, and GETREG
// returns 16384 registers whose set count matches what PFADD wrote.
func TestPfDebug(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "PFADD", "h", "a", "b", "c")
	expect(t, rw, ":1")
	cmd(t, rw, "PFDEBUG", "ENCODING", "h")
	expect(t, rw, "+sparse")

	cmd(t, rw, "PFDEBUG", "TODENSE", "h")
	expect(t, rw, ":1")
	cmd(t, rw, "PFDEBUG", "ENCODING", "h")
	expect(t, rw, "+dense")
	// TODENSE on an already-dense sketch reports no change.
	cmd(t, rw, "PFDEBUG", "TODENSE", "h")
	expect(t, rw, ":0")

	cmd(t, rw, "PFDEBUG", "GETREG", "h")
	got := readReply(t, rw)
	if got != fmt.Sprintf("*%d", hllRegisters) {
		t.Fatalf("GETREG header = %q, want *%d", got, hllRegisters)
	}
	nonzero := 0
	for i := 0; i < hllRegisters; i++ {
		r := readReply(t, rw)
		if r == "" || r[0] != ':' {
			t.Fatalf("GETREG element %d = %q, want integer", i, r)
		}
		if r != ":0" {
			nonzero++
		}
	}
	// Three distinct members set at most three registers (a collision would set fewer).
	if nonzero < 1 || nonzero > 3 {
		t.Fatalf("GETREG nonzero registers = %d, want 1..3", nonzero)
	}
}

// PFSELFTEST exercises the estimator across the cardinality range and the sparse/dense round trip.
func TestPfSelfTest(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "PFSELFTEST")
	expect(t, rw, "+OK")
}

// The dense expansion of a sparse sketch must reproduce identical registers, checked directly at the
// unit level so a splice or promotion bug surfaces without the server round trip.
func TestHllSparseDenseAgree(t *testing.T) {
	sparse := hllCreate()
	dense := hllCreate()
	d, ok := hllSparseToDense(dense)
	if !ok {
		t.Fatal("initial sparse->dense failed")
	}
	dense = d
	for i := 0; i < 5000; i++ {
		ele := []byte("member:" + strconv.Itoa(i))
		sparse, _ = hllAdd(sparse, ele)
		dense, _ = hllAdd(dense, ele)
	}
	fromSparse, ok := hllSparseToDense(sparse)
	if !ok {
		t.Fatal("sparse->dense failed after inserts")
	}
	var hA, hB [64]int
	hllRegHisto(fromSparse, &hA)
	hllRegHisto(dense, &hB)
	if hA != hB {
		t.Fatalf("sparse and dense register histograms diverged:\n%v\n%v", hA, hB)
	}
	cs, _ := hllCount(sparse)
	cd, _ := hllCount(dense)
	if cs != cd {
		t.Fatalf("sparse count %d != dense count %d", cs, cd)
	}
}
