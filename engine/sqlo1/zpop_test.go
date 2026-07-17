package sqlo1

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// popZ pops through ZPopCount and copies what it emits; begin must
// run exactly once, first, with the exact emit count.
func popZ(t *testing.T, z *ZSet, key string, count int64, maxSide bool) []zrEnt {
	t.Helper()
	var out []zrEnt
	began := int64(-1)
	err := z.ZPopCount(context.Background(), []byte(key), count, maxSide, func(n int64) {
		if began != -1 {
			t.Fatalf("ZPopCount(%q, %d) ran begin twice", key, count)
		}
		began = n
	}, func(score float64, member []byte) {
		out = append(out, zrEnt{s: zScoreSortable(score), m: string(member)})
	})
	if err != nil {
		t.Fatalf("ZPopCount(%q, %d, max=%v): %v", key, count, maxSide, err)
	}
	if began != int64(len(out)) {
		t.Fatalf("ZPopCount(%q, %d) began %d but emitted %d", key, count, began, len(out))
	}
	return out
}

// checkPopCadence pops the built key down against the model with an
// alternating min-max cadence, reopens cold partway to prove the
// removals persisted, and finishes with an over-count pop that must
// drain the key and delete it.
func checkPopCadence(t *testing.T, r *zsetRig, key string, model []zrEnt) {
	t.Helper()
	ctx := context.Background()
	cadence := []struct {
		count int64
		max   bool
	}{{1, false}, {1, true}, {3, false}, {5, true}, {2, false}, {7, true}, {12, false}}
	for _, c := range cadence {
		k := int(c.count)
		var want []zrEnt
		if c.max {
			tail := model[len(model)-k:]
			for i := len(tail) - 1; i >= 0; i-- {
				want = append(want, tail[i])
			}
			model = model[:len(model)-k]
		} else {
			want = append(want, model[:k]...)
			model = model[k:]
		}
		got := popZ(t, r.z, key, c.count, c.max)
		if !zrEqual(got, want) {
			t.Fatalf("pop(count=%d, max=%v) = %v, want %v", c.count, c.max, got, want)
		}
		// The popped end's member is gone from the member family too.
		if _, ok := r.memscore(key, got[0].m); ok {
			t.Fatalf("popped member %q still scores on the member side", got[0].m)
		}
	}
	if card := r.zcard(key); card != int64(len(model)) {
		t.Fatalf("after cadence ZCard = %d, want %d", card, len(model))
	}

	// The cold view a restart would see; the commit tails ride the
	// next flush, as they do under the server.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	z2 := r.reopen()
	if got := collectZ(t, z2, key, 0, int64(len(model))+8); !zrEqual(got, model) {
		t.Fatalf("cold survivors disagree with the model (%d vs %d entries)", len(got), len(model))
	}

	// Over-count pop drains everything and the key dies.
	var want []zrEnt
	want = append(want, model...)
	got := popZ(t, r.z, key, int64(len(model))+50, false)
	if !zrEqual(got, want) {
		t.Fatalf("draining pop emitted %d entries, want %d", len(got), len(want))
	}
	if card := r.zcard(key); card != 0 {
		t.Fatalf("drained key still cards %d", card)
	}
	st, _, _, err := r.z.h.stateOf(ctx, []byte(key))
	if err != nil || st != hashAbsent {
		t.Fatalf("drained key state = (%v, %v), want absent", st, err)
	}
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	z3 := r.reopen()
	if got := collectZ(t, z3, key, 0, 8); len(got) != 0 {
		t.Fatalf("drained key still walks %d entries cold", len(got))
	}

	// Popping the corpse and popping nothing are both begin(0).
	if got := popZ(t, r.z, key, 3, true); len(got) != 0 {
		t.Fatalf("pop on a dead key emitted %d entries", len(got))
	}
	if got := popZ(t, r.z, "never-was", 1, false); len(got) != 0 {
		t.Fatalf("pop on an absent key emitted %d entries", len(got))
	}
}

// TestZPopCountRungs runs the pop cadence oracle on all three fence
// rungs: inline root, segmented with a flat fence, and segmented with
// fence pages under shrunk caps.
func TestZPopCountRungs(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		r := newZsetRig(t)
		rng := rand.New(rand.NewSource(51))
		pairs := map[string]float64{}
		for i := range 60 {
			m := fmt.Sprintf("m%02d", i)
			sc := float64(rng.Intn(9)) - 3.5
			r.zadd("z", m, sc, ZAddFlags{})
			pairs[m] = sc
		}
		checkPopCadence(t, r, "z", zrModel(pairs))
	})
	t.Run("flat", func(t *testing.T) {
		r := newZsetRig(t)
		rng := rand.New(rand.NewSource(52))
		pairs := map[string]float64{}
		for i := range 400 {
			m := fmt.Sprintf("m%03d-%s", i, strings.Repeat("x", 24))
			sc := float64(rng.Intn(23)) * 1.5
			r.zadd("z", m, sc, ZAddFlags{})
			pairs[m] = sc
		}
		checkPopCadence(t, r, "z", zrModel(pairs))
	})
	t.Run("paged", func(t *testing.T) {
		defer SetZFenceCapsForTest(3, 5, 3, 4)()
		r := newZsetRig(t)
		rng := rand.New(rand.NewSource(53))
		pairs := map[string]float64{}
		for i := range 700 {
			m := fmt.Sprintf("m%03d-%s", i, strings.Repeat("y", 24))
			sc := float64(rng.Intn(31)) - 8
			r.zadd("z", m, sc, ZAddFlags{})
			pairs[m] = sc
		}
		if paged, err := r.z.FencePagedForTest(context.Background(), []byte("z")); err != nil || !paged {
			t.Fatalf("fence paged = (%v, %v), the rung under test needs pages", paged, err)
		}
		checkPopCadence(t, r, "z", zrModel(pairs))
	})
}

// TestZPopInlineExactSpan pins the inline split arithmetic: a min pop
// takes exactly the ordered region's head, a max pop its tail, and a
// full pop deletes the key with no survivors to rebuild.
func TestZPopInlineExactSpan(t *testing.T) {
	r := newZsetRig(t)
	for i := range 7 {
		r.zadd("z", fmt.Sprintf("e%d", i), float64(i), ZAddFlags{})
	}
	got := popZ(t, r.z, "z", 2, false)
	if len(got) != 2 || got[0].m != "e0" || got[1].m != "e1" {
		t.Fatalf("min pop 2 = %v, want e0 then e1", got)
	}
	got = popZ(t, r.z, "z", 2, true)
	if len(got) != 2 || got[0].m != "e6" || got[1].m != "e5" {
		t.Fatalf("max pop 2 = %v, want e6 then e5", got)
	}
	if card := r.zcard("z"); card != 3 {
		t.Fatalf("card = %d, want 3", card)
	}
	got = popZ(t, r.z, "z", 3, false)
	if len(got) != 3 || got[0].m != "e2" || got[2].m != "e4" {
		t.Fatalf("draining pop = %v, want e2 e3 e4", got)
	}
	st, _, _, err := r.z.h.stateOf(context.Background(), []byte("z"))
	if err != nil || st != hashAbsent {
		t.Fatalf("drained inline key state = (%v, %v), want absent", st, err)
	}
}

// randZ samples through ZRandMemberCount and copies what it emits
// (the emitted bytes alias run reads and die with the emit).
func randZ(t *testing.T, z *ZSet, key string, count int64, withReplacement bool) []zrEnt {
	t.Helper()
	var out []zrEnt
	began := int64(-1)
	err := z.ZRandMemberCount(context.Background(), []byte(key), count, withReplacement, func(n int64) {
		if began != -1 {
			t.Fatalf("ZRandMemberCount(%q, %d) ran begin twice", key, count)
		}
		began = n
	}, func(score float64, member []byte) {
		out = append(out, zrEnt{s: zScoreSortable(score), m: string(member)})
	})
	if err != nil {
		t.Fatalf("ZRandMemberCount(%q, %d, repl=%v): %v", key, count, withReplacement, err)
	}
	if began != int64(len(out)) {
		t.Fatalf("ZRandMemberCount(%q, %d) began %d but emitted %d", key, count, began, len(out))
	}
	return out
}

// checkRandSampling validates every draw against the model: real
// members, real scores, distinct mode never repeats and caps at the
// cardinality, replacement mode draws exactly count times.
func checkRandSampling(t *testing.T, r *zsetRig, key string, pairs map[string]float64) {
	t.Helper()
	card := int64(len(pairs))
	valid := func(got []zrEnt) {
		t.Helper()
		for _, e := range got {
			sc, ok := pairs[e.m]
			if !ok {
				t.Fatalf("sampled %q, a member never added", e.m)
			}
			if zScoreSortable(sc) != e.s {
				t.Fatalf("sampled %q at %#x, model holds %g", e.m, e.s, sc)
			}
		}
	}
	distinct := func(got []zrEnt) {
		t.Helper()
		seen := map[string]bool{}
		for _, e := range got {
			if seen[e.m] {
				t.Fatalf("distinct draw repeated %q", e.m)
			}
			seen[e.m] = true
		}
	}

	got := randZ(t, r.z, key, card/3, false)
	if int64(len(got)) != card/3 {
		t.Fatalf("distinct %d drew %d", card/3, len(got))
	}
	valid(got)
	distinct(got)

	// Asking for more than the cardinality caps at all of it.
	got = randZ(t, r.z, key, card+25, false)
	if int64(len(got)) != card {
		t.Fatalf("distinct over-count drew %d, want %d", len(got), card)
	}
	valid(got)
	distinct(got)

	got = randZ(t, r.z, key, card+13, true)
	if int64(len(got)) != card+13 {
		t.Fatalf("replacement drew %d, want %d", len(got), card+13)
	}
	valid(got)

	if got := randZ(t, r.z, key, 0, false); len(got) != 0 {
		t.Fatalf("count 0 drew %d", len(got))
	}
	if got := randZ(t, r.z, "never-was", 4, true); len(got) != 0 {
		t.Fatalf("absent key drew %d", len(got))
	}
}

// TestZRandMemberRungs runs the sampling oracle on the three rungs.
func TestZRandMemberRungs(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		r := newZsetRig(t)
		pairs := map[string]float64{}
		for i := range 30 {
			m := fmt.Sprintf("m%02d", i)
			r.zadd("z", m, float64(i%7)-2, ZAddFlags{})
			pairs[m] = float64(i%7) - 2
		}
		checkRandSampling(t, r, "z", pairs)
	})
	t.Run("flat", func(t *testing.T) {
		r := newZsetRig(t)
		rng := rand.New(rand.NewSource(54))
		pairs := map[string]float64{}
		for i := range 300 {
			m := fmt.Sprintf("m%03d-%s", i, strings.Repeat("x", 24))
			sc := float64(rng.Intn(17)) * 0.75
			r.zadd("z", m, sc, ZAddFlags{})
			pairs[m] = sc
		}
		checkRandSampling(t, r, "z", pairs)
	})
	t.Run("paged", func(t *testing.T) {
		defer SetZFenceCapsForTest(3, 5, 3, 4)()
		r := newZsetRig(t)
		rng := rand.New(rand.NewSource(55))
		pairs := map[string]float64{}
		for i := range 500 {
			m := fmt.Sprintf("m%03d-%s", i, strings.Repeat("y", 24))
			sc := float64(rng.Intn(29)) - 6
			r.zadd("z", m, sc, ZAddFlags{})
			pairs[m] = sc
		}
		if paged, err := r.z.FencePagedForTest(context.Background(), []byte("z")); err != nil || !paged {
			t.Fatalf("fence paged = (%v, %v), the rung under test needs pages", paged, err)
		}
		checkRandSampling(t, r, "z", pairs)
	})
}
