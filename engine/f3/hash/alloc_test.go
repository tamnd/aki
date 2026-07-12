package hash

import (
	"strconv"
	"testing"
)

// The point path must not allocate (F7, spec 2064/f3/10 sections 3.5 and 4.2):
// HGET, HEXISTS, and HSTRLEN on either band, plus a no-op HSET (overwrite of an
// existing field with an equal-length value), all run over the live
// representation in place. testing.AllocsPerRun rounds to whole allocations, so
// anything above zero is a real escape, not noise. Inserts that grow the blob,
// slab, vector, or table may allocate; those cases are excluded here.

func TestZeroAllocInline(t *testing.T) {
	h := newHash()
	for i := 0; i < 64; i++ {
		h.set([]byte("f"+strconv.Itoa(i)), []byte("v"+strconv.Itoa(i)))
	}
	if h.enc != encListpack {
		t.Fatalf("enc = %s, want listpack", h.enc)
	}
	hit := []byte("f40")
	miss := []byte("absent")
	val := []byte("v40") // same length as the resident value: an in-place overwrite

	checkZero(t, "inline get hit", func() { sinkVal, sinkOk = h.get(hit) })
	checkZero(t, "inline get miss", func() { sinkVal, sinkOk = h.get(miss) })
	checkZero(t, "inline has hit", func() { sinkOk = h.has(hit) })
	checkZero(t, "inline strlen", func() { sinkInt = h.strlen(hit) })
	checkZero(t, "inline card", func() { sinkInt = h.card() })
	checkZero(t, "inline set no-op", func() { sinkOk = h.set(hit, val) })
}

func TestZeroAllocNative(t *testing.T) {
	h := forceNative(newHash())
	for i := 0; i < 300; i++ {
		h.set([]byte("f"+strconv.Itoa(i)), []byte("v"+strconv.Itoa(i)))
	}
	if h.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", h.enc)
	}
	hit := []byte("f200")
	miss := []byte("nope")
	val := []byte("v200") // equal-length overwrite: in place, no slab growth

	checkZero(t, "native get hit", func() { sinkVal, sinkOk = h.get(hit) })
	checkZero(t, "native get miss", func() { sinkVal, sinkOk = h.get(miss) })
	checkZero(t, "native has hit", func() { sinkOk = h.has(hit) })
	checkZero(t, "native strlen", func() { sinkInt = h.strlen(hit) })
	checkZero(t, "native card", func() { sinkInt = h.card() })
	checkZero(t, "native set no-op", func() { sinkOk = h.set(hit, val) })
}

var (
	sinkVal []byte
	sinkOk  bool
	sinkInt int
)

func checkZero(t *testing.T, name string, fn func()) {
	t.Helper()
	if n := testing.AllocsPerRun(200, fn); n != 0 {
		t.Errorf("%s allocated %v times per run, want 0", name, n)
	}
}
