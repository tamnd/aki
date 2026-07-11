package set

import (
	"strconv"
	"testing"
)

// The inline point ops must not allocate (F7, doc 11 section 3.4): membership,
// cardinality, and a no-op add all run over the packed representation in place.
// testing.AllocsPerRun rounds to whole allocations, so anything above zero is a
// real escape, not noise.

func TestZeroAllocIntset(t *testing.T) {
	s := newSet([]byte("0"))
	for i := 0; i < 64; i++ {
		s.add([]byte(strconv.Itoa(i)))
	}
	hit := []byte("40")
	miss := []byte("9999")
	dup := []byte("40")

	checkZero(t, "intset has hit", func() { sink = s.has(hit) })
	checkZero(t, "intset has miss", func() { sink = s.has(miss) })
	checkZero(t, "intset card", func() { sinkInt = s.card() })
	checkZero(t, "intset add existing", func() { sink = s.add(dup) })
}

func TestZeroAllocListpack(t *testing.T) {
	s := newSet([]byte("seed"))
	for i := 0; i < 64; i++ {
		s.add([]byte("m" + strconv.Itoa(i)))
	}
	hit := []byte("m40")
	miss := []byte("absent")
	dup := []byte("m40")

	checkZero(t, "listpack has hit", func() { sink = s.has(hit) })
	checkZero(t, "listpack has miss", func() { sink = s.has(miss) })
	checkZero(t, "listpack card", func() { sinkInt = s.card() })
	checkZero(t, "listpack add existing", func() { sink = s.add(dup) })
}

var (
	sink    bool
	sinkInt int
)

func checkZero(t *testing.T, name string, fn func()) {
	t.Helper()
	if n := testing.AllocsPerRun(200, fn); n != 0 {
		t.Errorf("%s allocated %v times per run, want 0", name, n)
	}
}
