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

// TestZeroAllocNativeMultiGet extends the single-op coverage to the multi-field
// probe shape (HMGET, spec 2064/f3/10 section 7.2): the command loops the same
// ftable.get over a slice of fields, so the whole batch must stay off the heap at
// the band level, not just one probe. A mix of present and absent fields exercises
// both the confirm-on-hit and the miss-terminates paths inside one measured
// closure. Only the read path is claimed here: HDEL appends to a free list and a
// slab-growing insert appends to the slab, so neither is zero-alloc and neither is
// asserted.
func TestZeroAllocNativeMultiGet(t *testing.T) {
	h := forceNative(newHash())
	for i := 0; i < 300; i++ {
		h.set([]byte("f"+strconv.Itoa(i)), []byte("v"+strconv.Itoa(i)))
	}
	if h.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", h.enc)
	}
	// Sixteen fields, alternating present and absent, built once outside the
	// measured closure so the range loop below allocates nothing.
	fields := [][]byte{
		[]byte("f0"), []byte("absent0"),
		[]byte("f37"), []byte("absent1"),
		[]byte("f88"), []byte("absent2"),
		[]byte("f150"), []byte("absent3"),
		[]byte("f199"), []byte("absent4"),
		[]byte("f240"), []byte("absent5"),
		[]byte("f275"), []byte("absent6"),
		[]byte("f299"), []byte("absent7"),
	}
	checkZero(t, "native multi-get", func() {
		for _, f := range fields {
			sinkVal, sinkOk = h.get(f)
		}
	})
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
