package keyspace

import "testing"

// TestLFUStaysOnCoarseClock reproduces the clock-domain bug behind a flaky
// OBJECT FREQ. The hot path stamps a key's LFU decr field with the coarse
// (cron-cached) clock, so the seeders and the readers must use the same coarse
// clock. When a background server stops it freezes the coarse cache minutes in the
// past while the real clock moves on, so a stamp on the coarse clock compared
// against the real clock looks like minutes of elapsed decay and drops the counter.
//
// Here the coarse cache is pinned two minutes behind the real clock, the way a
// stopped background server leaves it for the rest of a test binary. With factor 0
// the counter must climb one per access from the seed of 5 to 15 and Freq must read
// back 15, with no spurious decay from the clock mismatch.
func TestLFUStaysOnCoarseClock(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	ks.SetLFUParams(0, 1) // factor 0 climbs every access, decay 1 minute

	prevActive := coarseActive.Load()
	prevMillis := coarseMillis.Load()
	t.Cleanup(func() {
		coarseActive.Store(prevActive)
		coarseMillis.Store(prevMillis)
	})
	// Freeze the coarse cache two minutes behind the real wall clock.
	coarseMillis.Store(nowMillis() - 2*60_000)
	coarseActive.Store(true)

	key := []byte("k")
	db.recordAccess(key, true) // first write seeds the counter at lfuInitVal (5)
	if got := db.Freq(key); got != lfuInitVal {
		t.Fatalf("seeded freq = %d, want %d", got, lfuInitVal)
	}
	for range 10 {
		db.recordAccess(key, false)
	}
	if got := db.Freq(key); got != 15 {
		t.Fatalf("freq after 10 reads at factor 0 = %d, want 15 (coarse clock must be used end to end)", got)
	}
}
