package sqlo1

import (
	"context"
	"testing"
)

// keyspaceRig builds the same tier spread TestScanStepMerge uses:
// dirty-only, resident, cold-only, hot tombstone over cold, hot
// expired, cold expired, so the point probes are pinned against every
// state a key can be in.
func keyspaceRig(t *testing.T) (*Tiered, *Str, *int64) {
	t.Helper()
	ctx := context.Background()
	now := new(int64)
	*now = 1_000_000
	tr := NewTiered(NewMemStore(), TieredConfig{
		Budget: Budget{Entries: 256, Arenas: 64 << 20},
		Seed:   11,
		NowMs:  func() int64 { return *now },
	})
	str, err := NewStr(tr, StrConfig{})
	if err != nil {
		t.Fatal(err)
	}
	hsh, err := NewHash(tr, HashConfig{})
	if err != nil {
		t.Fatal(err)
	}
	set := func(k string) {
		t.Helper()
		if err := str.Set(ctx, []byte(k), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	// Cold phase: drained and evicted.
	set("coldstr")
	set("gone")
	set("cexp")
	if _, err := hsh.HSet(ctx, []byte("coldhash"), []byte("f"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	tr.ht.setExpireMs([]byte("cexp"), *now+500)
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	tr.EvictAllForTest()

	// Resident: drained but not evicted.
	set("res")
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Hot phase: a dirty key, a dirty hash, a tombstone over the cold
	// copy, and a hot key past its deadline.
	set("dirty")
	if _, err := hsh.HSet(ctx, []byte("dirtyhash"), []byte("f"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := str.Del(ctx, []byte("gone")); err != nil {
		t.Fatal(err)
	}
	set("hexp")
	tr.ht.setExpireMs([]byte("hexp"), *now+100)
	*now += 1_000 // past both deadlines

	return tr, str, now
}

// TestProbeBatchTiers pins existence per tier state, the one-BatchGet
// coalescing rule, and the no-promotion discipline: probing a cold key
// must not pull it hot or move the promotion counters.
func TestProbeBatchTiers(t *testing.T) {
	ctx := context.Background()
	tr, _, _ := keyspaceRig(t)

	keys := [][]byte{
		[]byte("dirty"),   // hot live
		[]byte("res"),     // resident
		[]byte("coldstr"), // cold only
		[]byte("gone"),    // hot tombstone over cold
		[]byte("hexp"),    // hot expired
		[]byte("cexp"),    // cold expired
		[]byte("nosuch"),  // absent
		[]byte("dirty"),   // duplicate mention counts again
	}
	want := []bool{true, true, true, false, false, false, false, true}

	before := tr.Stats()
	hits, err := tr.ProbeBatch(ctx, keys, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != len(want) {
		t.Fatalf("hits = %v, want %v", hits, want)
	}
	for i, w := range want {
		if hits[i] != w {
			t.Fatalf("key %q: hit = %v, want %v", keys[i], hits[i], w)
		}
	}
	after := tr.Stats()
	// coldstr, cexp, and nosuch coalesce into one BatchGet; the rest
	// are answered hot. The probe never promotes, so the read-path
	// counters stay still.
	if got := after.BatchReads - before.BatchReads; got != 1 {
		t.Fatalf("BatchReads moved by %d, want 1", got)
	}
	if after.ColdHits != before.ColdHits || after.HotHits != before.HotHits {
		t.Fatalf("promotion counters moved: %+v -> %+v", before, after)
	}
	if _, hot := tr.ht.peek([]byte("coldstr")); hot {
		t.Fatal("probe promoted a cold key")
	}

	// A second probe over the same cold key pays another BatchGet,
	// which is the no-promotion property observed from the outside.
	if _, err := tr.ProbeBatch(ctx, [][]byte{[]byte("coldstr")}, false, hits); err != nil {
		t.Fatal(err)
	}
	if got := tr.Stats().BatchReads - after.BatchReads; got != 1 {
		t.Fatalf("second cold probe moved BatchReads by %d, want 1", got)
	}

	// An all-hot probe touches the store not at all.
	mid := tr.Stats()
	if _, err := tr.ProbeBatch(ctx, [][]byte{[]byte("dirty"), []byte("res")}, false, hits); err != nil {
		t.Fatal(err)
	}
	if got := tr.Stats().BatchReads - mid.BatchReads; got != 0 {
		t.Fatalf("all-hot probe moved BatchReads by %d, want 0", got)
	}
}

// TestProbeBatchTouch pins the touch flag: TOUCH refreshes the read
// stamp of a hot live hit, a plain probe leaves it alone, and a cold
// key is counted without being warmed.
func TestProbeBatchTouch(t *testing.T) {
	ctx := context.Background()
	tr, _, now := keyspaceRig(t)

	hd, hot := tr.ht.peek([]byte("dirty"))
	if !hot {
		t.Fatal("dirty is not hot")
	}
	stamp := hd.lastRead

	// Move the tick so a touch is observable, then probe without the
	// flag: the stamp must not move.
	*now += 10_000
	tr.Now()
	if _, err := tr.ProbeBatch(ctx, [][]byte{[]byte("dirty")}, false, nil); err != nil {
		t.Fatal(err)
	}
	if hd.lastRead != stamp {
		t.Fatal("EXISTS-style probe touched the read stamp")
	}

	// The same probe with the flag refreshes it.
	if _, err := tr.ProbeBatch(ctx, [][]byte{[]byte("dirty")}, true, nil); err != nil {
		t.Fatal(err)
	}
	if hd.lastRead == stamp {
		t.Fatal("TOUCH-style probe left the read stamp stale")
	}

	// TOUCH on a cold key counts it but does not warm it.
	hits, err := tr.ProbeBatch(ctx, [][]byte{[]byte("coldstr")}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hits[0] {
		t.Fatal("cold key not counted by TOUCH")
	}
	if _, hot := tr.ht.peek([]byte("coldstr")); hot {
		t.Fatal("TOUCH warmed a cold key")
	}
}

// TestTypeTagTiers pins TYPE's tag source per tier state: the hot
// header answers directly, a cold root sniffs, a cold plain record is
// a string, and dead keys in every form answer none.
func TestTypeTagTiers(t *testing.T) {
	ctx := context.Background()
	tr, _, _ := keyspaceRig(t)

	cases := []struct {
		key string
		tag uint8
		ok  bool
	}{
		{"dirty", TagString, true},
		{"dirtyhash", TagHash, true},
		{"res", TagString, true},
		{"coldstr", TagString, true},
		{"coldhash", TagHash, true},
		{"gone", 0, false},
		{"hexp", 0, false},
		{"cexp", 0, false},
		{"nosuch", 0, false},
	}
	before := tr.Stats()
	for _, c := range cases {
		tag, ok, err := tr.TypeTag(ctx, []byte(c.key))
		if err != nil {
			t.Fatalf("%q: %v", c.key, err)
		}
		if ok != c.ok || tag != c.tag {
			t.Fatalf("%q: (%d, %v), want (%d, %v)", c.key, tag, ok, c.tag, c.ok)
		}
	}
	after := tr.Stats()
	if after.ColdHits != before.ColdHits || after.HotHits != before.HotHits {
		t.Fatalf("TYPE moved promotion counters: %+v -> %+v", before, after)
	}
	if _, hot := tr.ht.peek([]byte("coldhash")); hot {
		t.Fatal("TYPE promoted a cold key")
	}
}
