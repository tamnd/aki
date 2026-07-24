package sqlo1

import (
	"context"
	"errors"
	"testing"
)

// scanAllSteps drives ScanStep to completion, recording every emitted
// key with its tag and counting deliveries. It also checks the cursor
// grammar: hot-phase cursors carry the phase bit, cold-phase cursors
// do not, and the hot phase never restarts once the walk left it.
func scanAllSteps(t *testing.T, tr *Tiered, budget int) (map[string]uint8, map[string]int) {
	t.Helper()
	ctx := context.Background()
	tags := map[string]uint8{}
	count := map[string]int{}
	cur := uint64(0)
	inCold := false
	for step := 0; ; step++ {
		if step > 100_000 {
			t.Fatal("ScanStep does not terminate")
		}
		next, err := tr.ScanStep(ctx, cur, budget, func(key []byte, tag uint8) {
			tags[string(key)] = tag
			count[string(key)]++
		})
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		if next == 0 {
			return tags, count
		}
		if next&scanHotBit != 0 {
			if inCold {
				t.Fatalf("step %d: hot-phase cursor %#x after the cold phase began", step, next)
			}
		} else {
			inCold = true
		}
		cur = next
	}
}

// TestScanStepMerge pins the two-phase merge across every tier state
// a key can be in: dirty-only, resident over cold, cold-only, hot
// tombstone over cold, hot-expired, cold-expired, and the
// blind-overwrite window, plus the tag sources (shadow tag, hot tag
// on the cold walk, sniffed root tag).
func TestScanStepMerge(t *testing.T) {
	ctx := context.Background()
	now := int64(1_000_000)
	tr := NewTiered(NewMemStore(), TieredConfig{
		Budget: Budget{Entries: 256, Arenas: 64 << 20},
		Seed:   11,
		NowMs:  func() int64 { return now },
	})
	str, err := NewStr(tr, StrConfig{})
	if err != nil {
		t.Fatal(err)
	}
	hsh, err := NewHash(tr, HashConfig{})
	if err != nil {
		t.Fatal(err)
	}
	set := func(k, v string) {
		t.Helper()
		if err := str.Set(ctx, []byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	// Phase one: keys that will live cold. cexp carries a deadline the
	// clock will pass before the scan.
	set("coldstr", "v")
	set("redirty", "v")
	set("morph", "v")
	set("gone", "v")
	set("cexp", "v")
	if _, err := hsh.HSet(ctx, []byte("coldhash"), []byte("f"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	tr.ht.setExpireMs([]byte("cexp"), now+500)
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	tr.EvictAllForTest()

	// Phase two: a resident key, drained but not evicted.
	set("res", "v")
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Phase three: hot-side states over the cold keyspace. redirty and
	// morph are the blind-overwrite window (evicted, then rewritten
	// with vptr 0); morph also changes type, so the hot tag must win
	// over the cold record's. gone dies behind a hot tombstone, hexp
	// behind its deadline.
	set("dirty", "v")
	if _, err := hsh.HSet(ctx, []byte("dirtyhash"), []byte("f"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	set("redirty", "v2")
	if !tr.ht.Put([]byte("morph"), []byte{inlineSubBase | TagHash}, TagHash|TagRoot) {
		t.Fatal("morph rewrite refused")
	}
	if _, err := str.Del(ctx, []byte("gone")); err != nil {
		t.Fatal(err)
	}
	set("hexp", "v")
	tr.ht.setExpireMs([]byte("hexp"), now+100)
	now += 1_000 // past both deadlines

	wantTags := map[string]uint8{
		"dirty":     TagString,
		"dirtyhash": TagHash,
		"res":       TagString,
		"coldstr":   TagString,
		"coldhash":  TagHash,
		"redirty":   TagString,
		"morph":     TagHash,
	}
	// The blind-overwrite pair may arrive in both phases; everyone
	// else exactly once.
	dupOK := map[string]bool{"redirty": true, "morph": true}

	for _, budget := range []int{3, 1 << 20} {
		tags, count := scanAllSteps(t, tr, budget)
		if len(tags) != len(wantTags) {
			t.Fatalf("budget %d: keys = %v, want %v", budget, tags, wantTags)
		}
		for k, wt := range wantTags {
			if tags[k] != wt {
				t.Fatalf("budget %d: tag of %q = %d, want %d", budget, k, tags[k], wt)
			}
			if n := count[k]; n != 1 && !(dupOK[k] && n == 2) {
				t.Fatalf("budget %d: key %q delivered %d times", budget, k, n)
			}
		}
	}
}

// noScanStore strips the KeyScanner capability off MemStore, so the
// unsupported door is testable.
type noScanStore struct {
	ms *MemStore
}

func (n *noScanStore) Get(ctx context.Context, key []byte) (Record, error) {
	return n.ms.Get(ctx, key)
}
func (n *noScanStore) BatchGet(ctx context.Context, keys [][]byte) ([]Record, error) {
	return n.ms.BatchGet(ctx, keys)
}
func (n *noScanStore) ApplyBatch(ctx context.Context, b *DrainBatch) error {
	return n.ms.ApplyBatch(ctx, b)
}
func (n *noScanStore) Scan(ctx context.Context, cur Cursor, fn func(Record) bool) (Cursor, error) {
	return n.ms.Scan(ctx, cur, fn)
}
func (n *noScanStore) Stats() StoreStats { return n.ms.Stats() }

func TestScanStepUnsupportedStore(t *testing.T) {
	tr := NewTiered(&noScanStore{ms: NewMemStore()}, TieredConfig{
		Budget: Budget{Entries: 64, Arenas: 1 << 20},
	})
	_, err := tr.ScanStep(context.Background(), 0, 1<<20, func([]byte, uint8) {})
	if !errors.Is(err, ErrScanUnsupported) {
		t.Fatalf("err = %v, want ErrScanUnsupported", err)
	}
}
