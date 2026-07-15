package main

import "testing"

// Test configuration: small enough for the race runner, large enough
// that the structural verdicts are not noise.
const (
	tKeys  = 200_000
	tChurn = 80_000
)

// TestEveryKeyReapsExactlyOnce is the wheel correctness pin: every
// strategy and every width reaps the full key population, none early
// (an early reap would zero the authoritative expiry and the later
// bucket visit would then count the entry stale, leaving reaped short).
func TestEveryKeyReapsExactlyOnce(t *testing.T) {
	for _, eager := range []bool{false, true} {
		r := runConfig(tKeys, tChurn, 256, 8, eager, 3)
		if r.reaped != tKeys {
			t.Fatalf("eager=%v: reaped %d keys, want %d", eager, r.reaped, tKeys)
		}
	}
	for _, cfg := range []struct {
		width uint32
		shift uint
	}{{64, 6}, {128, 7}, {512, 9}} {
		r := runConfig(tKeys, tChurn, cfg.width, cfg.shift, false, 3)
		if r.reaped != tKeys {
			t.Fatalf("width %d: reaped %d keys, want %d", cfg.width, r.reaped, tKeys)
		}
	}
}

// TestReapTickMatchesExpiry pins reap timing exactly on a tiny wheel:
// a key's authoritative expiry is zeroed at its expiry tick and not one
// tick before, across all three levels and after GT and LT rewrites.
func TestReapTickMatchesExpiry(t *testing.T) {
	exps := []uint32{1, 7, 8, 63, 64, 65, 300, 511}
	w := newWheel(8, 3, len(exps), false)
	for k, exp := range exps {
		w.expiry[k] = exp
		w.file(int32(k), exp)
	}
	w.rewrite(3, 200) // GT: 63 -> 200, leaves a stale entry in a level-1 bucket
	w.rewrite(6, 20)  // LT: 300 -> 20, leaves a stale entry in a level-2 bucket
	exps[3], exps[6] = 200, 20

	for w.now < 511 {
		tick := w.now + 1
		for k, exp := range exps {
			if exp == tick && w.expiry[k] == 0 {
				t.Fatalf("key %d reaped before its expiry tick %d", k, exp)
			}
		}
		w.advance()
		for k, exp := range exps {
			if exp <= w.now && w.expiry[k] != 0 {
				t.Fatalf("key %d (exp %d) not reaped by tick %d", k, exp, w.now)
			}
		}
	}
	if w.reaped != len(exps) {
		t.Fatalf("reaped %d, want %d", w.reaped, len(exps))
	}
	if w.stale != 2 {
		t.Fatalf("stale %d, want exactly the 2 rewritten-over entries", w.stale)
	}
}

// TestLazyBloatIsChurnProportional pins the lazy strategy's cost model:
// entries after churn sit at keys plus one stale entry per effective
// rewrite, never more, and the stale fraction accounts for all of them.
func TestLazyBloatIsChurnProportional(t *testing.T) {
	r := runConfig(tKeys, tChurn, 256, 8, false, 3)
	if r.entriesAfter > tKeys+tChurn {
		t.Fatalf("lazy entries %d exceed keys+churn %d", r.entriesAfter, tKeys+tChurn)
	}
	if r.entriesAfter < tKeys+(tChurn*9)/10 {
		t.Fatalf("lazy entries %d, want near keys+churn %d (only no-op rewrites may skip)",
			r.entriesAfter, tKeys+tChurn)
	}
	wantStale := float64(r.entriesAfter-tKeys) / float64(r.entriesAfter)
	if diff := r.staleFrac - wantStale; diff < -0.001 || diff > 0.001 {
		t.Fatalf("stale frac %.4f, want %.4f (every non-live entry filtered exactly once)",
			r.staleFrac, wantStale)
	}
}

// TestEagerStaysExact pins the eager strategy's structural claim: the
// backpointer removal keeps the entry count at exactly the key count
// through arbitrary churn, with nothing stale left to filter.
func TestEagerStaysExact(t *testing.T) {
	r := runConfig(tKeys, tChurn, 256, 8, true, 3)
	if r.entriesAfter != tKeys {
		t.Fatalf("eager entries %d, want exactly %d", r.entriesAfter, tKeys)
	}
	if r.staleFrac != 0 {
		t.Fatalf("eager stale frac %.4f, want 0", r.staleFrac)
	}
}

// TestStaleEntriesNeverCascade pins the reason lazy bloat is cheap:
// stale entries drop at their first bucket visit instead of riding the
// cascade, so live cascade traffic matches the eager wheel's.
func TestStaleEntriesNeverCascade(t *testing.T) {
	lazy := runConfig(tKeys, tChurn, 256, 8, false, 3)
	eager := runConfig(tKeys, tChurn, 256, 8, true, 3)
	diff := lazy.cascadeMove - eager.cascadeMove
	if diff < 0 {
		diff = -diff
	}
	if diff*1000 > eager.cascadeMove {
		t.Fatalf("cascade moves lazy %d vs eager %d differ by more than 0.1%%",
			lazy.cascadeMove, eager.cascadeMove)
	}
}

// TestWiderWheelCascadesLess pins the width lever: when a wider wheel
// covers the same TTL population with fewer levels, cascade traffic
// drops (width 512 reaches the whole test horizon in two levels).
func TestWiderWheelCascadesLess(t *testing.T) {
	narrow := runConfig(tKeys, tChurn, 64, 6, false, 3)
	wide := runConfig(tKeys, tChurn, 512, 9, false, 3)
	if wide.cascadeMove >= narrow.cascadeMove {
		t.Fatalf("cascade moves width 512 %d, want below width 64 %d",
			wide.cascadeMove, narrow.cascadeMove)
	}
}
