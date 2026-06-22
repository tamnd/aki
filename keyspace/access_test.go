package keyspace

import "testing"

// TestIdleTimeTracksAccess checks that Idle reports the seconds since the last
// read or write and that an access resets it.
func TestIdleTimeTracksAccess(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	clock := int64(1_000_000) // 1000s
	old := nowMillis
	nowMillis = func() int64 { return clock }
	defer func() { nowMillis = old }()

	if err := db.Set([]byte("k"), []byte("v"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	if got := db.Idle([]byte("k")); got != 0 {
		t.Fatalf("idle right after set = %d want 0", got)
	}
	clock += 10_000 // +10s
	if got := db.Idle([]byte("k")); got != 10 {
		t.Fatalf("idle after 10s = %d want 10", got)
	}
	// Reading the key is an access, so idle drops back to zero.
	if _, _, found, _ := db.Get([]byte("k")); !found {
		t.Fatal("key should be live")
	}
	if got := db.Idle([]byte("k")); got != 0 {
		t.Fatalf("idle after read = %d want 0", got)
	}
	// Peek does not count as an access, so idle keeps growing.
	clock += 5_000
	if _, _, found, _ := db.Peek([]byte("k")); !found {
		t.Fatal("key should be live on peek")
	}
	if got := db.Idle([]byte("k")); got != 5 {
		t.Fatalf("idle after peek = %d want 5", got)
	}
}

// TestFreqSeedAndDecay checks that a new key starts at the LFU init value and
// that the counter decays to zero after a long idle stretch.
func TestFreqSeedAndDecay(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	clock := int64(60_000) // minute 1
	old := nowMillis
	nowMillis = func() int64 { return clock }
	defer func() { nowMillis = old }()

	if err := db.Set([]byte("k"), []byte("v"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	if got := db.Freq([]byte("k")); got != lfuInitVal {
		t.Fatalf("fresh key freq = %d want %d", got, lfuInitVal)
	}
	// After more idle minutes than the init value, the counter decays to zero.
	clock += int64(60_000) * (lfuInitVal + 10)
	if got := db.Freq([]byte("k")); got != 0 {
		t.Fatalf("freq after long idle = %d want 0", got)
	}
}

// TestLFUDecayDisabled checks that lfu-decay-time 0, set through SetLFUParams,
// turns decay off so the counter holds across a long idle stretch instead of
// falling to zero. It also exercises the divide-by-zero guard.
func TestLFUDecayDisabled(t *testing.T) {
	ks, _, _ := newKS(t)
	ks.SetLFUParams(10, 0)
	db := mustDB(t, ks, 0)

	clock := int64(60_000) // minute 1
	old := nowMillis
	nowMillis = func() int64 { return clock }
	defer func() { nowMillis = old }()

	if err := db.Set([]byte("k"), []byte("v"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	if got := db.Freq([]byte("k")); got != lfuInitVal {
		t.Fatalf("fresh key freq = %d want %d", got, lfuInitVal)
	}
	// A long idle stretch that would decay the counter to zero under the default.
	clock += int64(60_000) * (lfuInitVal + 10)
	if got := db.Freq([]byte("k")); got != lfuInitVal {
		t.Fatalf("freq with decay off = %d want %d", got, lfuInitVal)
	}
}

// TestLFUParamsClamp checks SetLFUParams clamps negative inputs to zero so a bad
// config value cannot make the counter math go wrong.
func TestLFUParamsClamp(t *testing.T) {
	ks, _, _ := newKS(t)
	ks.SetLFUParams(-3, -7)
	if ks.lfuLogFactor != 0 || ks.lfuDecayTime != 0 {
		t.Fatalf("clamped params = %d / %d want 0 / 0", ks.lfuLogFactor, ks.lfuDecayTime)
	}
}

// TestSampleMetrics checks that the eviction sample carries each key's access
// time and frequency so the lru and lfu policies have something to sort on.
func TestSampleMetrics(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	clock := int64(1_000_000)
	old := nowMillis
	nowMillis = func() int64 { return clock }
	defer func() { nowMillis = old }()

	if err := db.Set([]byte("old"), []byte("v"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	clock += 100_000 // 100s later
	if err := db.Set([]byte("new"), []byte("v"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}

	cands := ks.SampleForEviction(10, false)
	if len(cands) != 2 {
		t.Fatalf("sampled %d candidates want 2", len(cands))
	}
	byKey := map[string]EvictionCandidate{}
	for _, c := range cands {
		byKey[string(c.Key)] = c
	}
	if byKey["old"].Atime >= byKey["new"].Atime {
		t.Fatalf("old atime %d should be less than new atime %d",
			byKey["old"].Atime, byKey["new"].Atime)
	}
	// The just-set key still carries the seed value; the older one has decayed a
	// little over the gap, so it is never higher.
	if byKey["new"].Freq != lfuInitVal {
		t.Fatalf("just-set key freq = %d want %d", byKey["new"].Freq, lfuInitVal)
	}
	if byKey["old"].Freq > byKey["new"].Freq {
		t.Fatalf("older key freq %d should not exceed newer %d",
			byKey["old"].Freq, byKey["new"].Freq)
	}
}
