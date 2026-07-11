package zset

import (
	"strconv"
	"testing"
)

// The inline read path must not allocate (F7, spec 2064/f3/12 section 4):
// ZSCORE scans the packed blob in place and ZCARD reads a counter.
// testing.AllocsPerRun rounds to whole allocations, so anything above zero is a
// real escape, not noise.

func TestZeroAllocInline(t *testing.T) {
	z := newZset()
	for i := 0; i < 64; i++ {
		z.update([]byte("m"+strconv.Itoa(i)), float64(i), flags{})
	}
	if z.enc != encListpack {
		t.Fatalf("enc = %s, want listpack", z.enc)
	}
	hit := []byte("m40")
	miss := []byte("absent")

	checkZero(t, "inline score hit", func() { _, sinkBool = z.score(hit) })
	checkZero(t, "inline score miss", func() { _, sinkBool = z.score(miss) })
	checkZero(t, "inline card", func() { sinkInt = z.card() })
}

// The native band holds the same bar: ZSCORE is one member-hash probe reading
// the record's raw bits, ZCARD reads the table's counter, neither allocates.
func TestZeroAllocNative(t *testing.T) {
	z := newZset()
	for i := 0; i < maxListpackEntries+64; i++ {
		z.update([]byte("m"+strconv.Itoa(i)), float64(i), flags{})
	}
	if z.enc != encSkiplist {
		t.Fatalf("enc = %s, want skiplist", z.enc)
	}
	hit := []byte("m140")
	miss := []byte("absent")

	checkZero(t, "native score hit", func() { _, sinkBool = z.score(hit) })
	checkZero(t, "native score miss", func() { _, sinkBool = z.score(miss) })
	checkZero(t, "native card", func() { sinkInt = z.card() })
}

var (
	sinkBool bool
	sinkInt  int
)

func checkZero(t *testing.T, name string, fn func()) {
	t.Helper()
	if n := testing.AllocsPerRun(200, fn); n != 0 {
		t.Errorf("%s allocated %v times per run, want 0", name, n)
	}
}
