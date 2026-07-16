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

// The native member table must hold the same zero-allocation bar as the inline
// bands (F7, doc 11 section 2): SISMEMBER, SCARD, a duplicate SADD, and SREM all
// run over the table, records, slab, and draw vector in place, with only genuine
// growth (a new member outgrowing the slab, record slab, vector, or table)
// escaping, which these cases avoid by touching members already resident.
func TestZeroAllocHashtable(t *testing.T) {
	s := newSet([]byte("0"))
	for i := 0; i < 700; i++ { // past the 512 intset cap, so enc is hashtable
		s.add([]byte(strconv.Itoa(i)))
	}
	if s.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", s.enc)
	}
	hit := []byte("400")
	miss := []byte("nope")
	dup := []byte("400")

	checkZero(t, "hashtable has hit", func() { sink = s.has(hit) })
	checkZero(t, "hashtable has miss", func() { sink = s.has(miss) })
	checkZero(t, "hashtable card", func() { sinkInt = s.card() })
	checkZero(t, "hashtable add existing", func() { sink = s.add(dup) })

	// SREM is zero-alloc too: the free-list append and swap-remove run over
	// preallocated capacity. Removing the same member is a no-op after the first
	// run, which AllocsPerRun measures past its warm-up call.
	gone := []byte("399")
	checkZero(t, "hashtable rem", func() { sink = s.rem(gone) })
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
