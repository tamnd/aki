package command

import (
	"strconv"
	"testing"
)

func TestPFAddReturn(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "PFADD hll a b c d"); got != ":1" {
		t.Fatalf("first PFADD = %q want :1", got)
	}
	if got := sendLine(t, r, c, "PFADD hll a b c d"); got != ":0" {
		t.Fatalf("re-add PFADD = %q want :0", got)
	}
	if got := sendLine(t, r, c, "PFADD hll e f g"); got != ":1" {
		t.Fatalf("new element PFADD = %q want :1", got)
	}
}

func TestPFAddNoElements(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "PFADD fresh"); got != ":1" {
		t.Fatalf("create-only PFADD = %q want :1", got)
	}
	if got := sendLine(t, r, c, "PFADD fresh"); got != ":0" {
		t.Fatalf("no-op PFADD = %q want :0", got)
	}
}

func TestPFCountSmallExact(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "PFADD hll a b c")
	if got := sendLine(t, r, c, "PFCOUNT hll"); got != ":3" {
		t.Fatalf("PFCOUNT = %q want :3", got)
	}
}

func TestPFCountMissing(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "PFCOUNT nope"); got != ":0" {
		t.Fatalf("PFCOUNT missing = %q want :0", got)
	}
}

func TestPFCountApprox(t *testing.T) {
	r, c := startData(t)
	const n = 10000
	for i := range n {
		_ = sendLine(t, r, c, "PFADD big elem:"+strconv.Itoa(i))
	}
	got := sendLine(t, r, c, "PFCOUNT big")
	est, err := strconv.Atoi(got[1:])
	if err != nil {
		t.Fatalf("PFCOUNT reply = %q", got)
	}
	// 0.81% standard error, allow a generous 3% band.
	if est < n*97/100 || est > n*103/100 {
		t.Fatalf("PFCOUNT estimate %d off from %d", est, n)
	}
}

func TestPFMergeUnion(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "PFADD a x y z")
	_ = sendLine(t, r, c, "PFADD b z w")
	if got := sendLine(t, r, c, "PFMERGE m a b"); got != "+OK" {
		t.Fatalf("PFMERGE = %q want +OK", got)
	}
	if got := sendLine(t, r, c, "PFCOUNT m"); got != ":4" {
		t.Fatalf("PFCOUNT merged = %q want :4", got)
	}
}

func TestPFCountMultiKey(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "PFADD a x y z")
	_ = sendLine(t, r, c, "PFADD b z w")
	if got := sendLine(t, r, c, "PFCOUNT a b"); got != ":4" {
		t.Fatalf("PFCOUNT a b = %q want :4", got)
	}
}

func TestPFMergeNoSources(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "PFMERGE dest"); got != "+OK" {
		t.Fatalf("PFMERGE dest = %q want +OK", got)
	}
	if got := sendLine(t, r, c, "PFCOUNT dest"); got != ":0" {
		t.Fatalf("PFCOUNT empty dest = %q want :0", got)
	}
}

func TestPFAddWrongTypeNonString(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH lst a")
	if got := sendLine(t, r, c, "PFADD lst x"); got != "-"+wrongTypeError {
		t.Fatalf("PFADD on list = %q", got)
	}
}

func TestPFAddNotValidHLL(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s plainstring")
	if got := sendLine(t, r, c, "PFADD s x"); got != "-"+hllNotValidError {
		t.Fatalf("PFADD on plain string = %q", got)
	}
}

func TestPFDebugEncodingAndToDense(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "PFADD hll a b c")
	if got := sendLine(t, r, c, "PFDEBUG ENCODING hll"); got != "+sparse" {
		t.Fatalf("PFDEBUG ENCODING = %q want +sparse", got)
	}
	if got := sendLine(t, r, c, "PFDEBUG TODENSE hll"); got != "+OK" {
		t.Fatalf("PFDEBUG TODENSE = %q want +OK", got)
	}
	if got := sendLine(t, r, c, "PFDEBUG ENCODING hll"); got != "+dense" {
		t.Fatalf("PFDEBUG ENCODING after TODENSE = %q want +dense", got)
	}
	// Count is unchanged by the dense conversion.
	if got := sendLine(t, r, c, "PFCOUNT hll"); got != ":3" {
		t.Fatalf("PFCOUNT after TODENSE = %q want :3", got)
	}
}

func TestPFDebugGetReg(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "PFADD hll a b c")
	if _, err := c.Write([]byte("PFDEBUG GETREG hll\r\n")); err != nil {
		t.Fatal(err)
	}
	got := readResp(t, r)
	// One header line plus 16384 register integers.
	if got[0] != "*16384" {
		t.Fatalf("GETREG header = %q", got[0])
	}
	if len(got) != 1+hllRegisters {
		t.Fatalf("GETREG element count = %d want %d", len(got)-1, hllRegisters)
	}
}

func TestPFDebugMissing(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "PFDEBUG ENCODING nope"); got != "-"+hllNoKeyError {
		t.Fatalf("PFDEBUG ENCODING missing = %q", got)
	}
}

func TestPFSelfTest(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "PFSELFTEST"); got != "+OK" {
		t.Fatalf("PFSELFTEST = %q want +OK", got)
	}
}

func TestPFAddPromotesToDense(t *testing.T) {
	r, c := startData(t)
	// Enough distinct elements push the sparse body past the threshold and
	// promote the key to dense.
	for i := range 5000 {
		_ = sendLine(t, r, c, "PFADD grow e:"+strconv.Itoa(i))
	}
	if got := sendLine(t, r, c, "PFDEBUG ENCODING grow"); got != "+dense" {
		t.Fatalf("encoding after many adds = %q want +dense", got)
	}
}

func TestHLLObjectEncoding(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "PFADD hll a b c")
	if got := bulk(t, r, c, "OBJECT ENCODING hll"); got != "raw" {
		t.Fatalf("OBJECT ENCODING hll = %q want raw", got)
	}
}
