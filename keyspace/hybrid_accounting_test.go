package keyspace

import "testing"

// TestHybridUsedMemoryDelta checks the hybrid engine keeps used_memory exact across
// the three shapes the btree path handles: a fresh key adds its entry cost, an
// overwrite swaps the old body's cost for the new one's (via the displaced length
// SetWithPrev surfaces), and a delete frees the whole entry back to the baseline.
func TestHybridUsedMemoryDelta(t *testing.T) {
	db := openHL(t)
	key := []byte("mem")
	base := db.ks.UsedMemory()

	want := func(bodyLen int) int64 {
		return base + hlEntryBytes(len(key), bodyLen)
	}

	if err := db.Set(key, make([]byte, 100), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("set 100: %v", err)
	}
	if got := db.ks.UsedMemory(); got != want(100) {
		t.Fatalf("after set 100: used_memory %d, want %d", got, want(100))
	}

	// Overwrite with a shorter body: the delta must subtract the old 100-byte entry
	// and add the new 10-byte one, not stack a second entry.
	if err := db.Set(key, make([]byte, 10), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("set 10: %v", err)
	}
	if got := db.ks.UsedMemory(); got != want(10) {
		t.Fatalf("after overwrite 10: used_memory %d, want %d", got, want(10))
	}

	// Overwrite with a longer body the other direction.
	if err := db.Set(key, make([]byte, 500), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("set 500: %v", err)
	}
	if got := db.ks.UsedMemory(); got != want(500) {
		t.Fatalf("after overwrite 500: used_memory %d, want %d", got, want(500))
	}

	if ok, err := db.Delete(key); err != nil || !ok {
		t.Fatalf("delete: ok=%v err=%v", ok, err)
	}
	if got := db.ks.UsedMemory(); got != base {
		t.Fatalf("after delete: used_memory %d, want base %d", got, base)
	}
}

// TestHybridFreqOnSetAndGet checks the hybrid read and write paths feed the LFU
// bookkeeping: a Set seeds the counter, and a run of Gets at log factor 0 climbs it
// one per read, the OBJECT FREQ contract on the hybrid engine.
func TestHybridFreqOnSetAndGet(t *testing.T) {
	db := openHL(t)
	db.ks.SetLFUParams(0, 1) // factor 0 bumps on every access
	key := []byte("hot")

	if err := db.Set(key, []byte("v"), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got := db.Freq(key); got != lfuInitVal {
		t.Fatalf("seeded freq = %d, want %d", got, lfuInitVal)
	}
	for range 10 {
		if _, _, found, err := db.Get(key); err != nil || !found {
			t.Fatalf("get: found=%v err=%v", found, err)
		}
	}
	if got := db.Freq(key); got != lfuInitVal+10 {
		t.Fatalf("freq after 10 gets = %d, want %d", got, lfuInitVal+10)
	}

	// A delete forgets the bookkeeping so a later re-create seeds fresh.
	if ok, err := db.Delete(key); err != nil || !ok {
		t.Fatalf("delete: ok=%v err=%v", ok, err)
	}
	if got := db.Freq(key); got != 0 {
		t.Fatalf("freq after delete = %d, want 0", got)
	}
}
