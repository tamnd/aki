package sqlo1

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// remZ trims through ZRemRange and checks the count against what the
// model window holds.
func remZ(t *testing.T, z *ZSet, key string, lo, hi int64) int64 {
	t.Helper()
	n, err := z.ZRemRange(context.Background(), []byte(key), lo, hi)
	if err != nil {
		t.Fatalf("ZRemRange(%q, %d, %d): %v", key, lo, hi, err)
	}
	return n
}

// checkTrimCadence trims the built key down against the model with a
// cadence of head, tail, mid, and clamped windows, reopens cold
// partway to prove the removals persisted, and finishes with a
// full-window trim that must delete the key.
func checkTrimCadence(t *testing.T, r *zsetRig, key string, model []zrEnt) {
	t.Helper()
	ctx := context.Background()
	trim := func(lo, hi int64) {
		t.Helper()
		wlo, whi := lo, hi
		if wlo < 0 {
			wlo = 0
		}
		if whi > int64(len(model)) {
			whi = int64(len(model))
		}
		if whi < wlo {
			whi = wlo
		}
		removed := append([]zrEnt(nil), model[wlo:whi]...)
		got := remZ(t, r.z, key, lo, hi)
		if got != whi-wlo {
			t.Fatalf("ZRemRange(%d, %d) = %d, want %d", lo, hi, got, whi-wlo)
		}
		model = append(model[:wlo], model[whi:]...)
		for _, e := range removed {
			if _, ok := r.memscore(key, e.m); ok {
				t.Fatalf("trimmed member %q still scores on the member side", e.m)
			}
		}
		if card := r.zcard(key); card != int64(len(model)) {
			t.Fatalf("after trim(%d, %d) ZCard = %d, want %d", lo, hi, card, len(model))
		}
		if got := collectZ(t, r.z, key, 0, int64(len(model))+8); !zrEqual(got, model) {
			t.Fatalf("after trim(%d, %d) survivors disagree with the model (%d vs %d entries)", lo, hi, len(got), len(model))
		}
	}

	trim(0, 3)                                       // head window
	trim(int64(len(model))-4, int64(len(model))+10)  // tail window, hi clamps
	trim(5, 25)                                      // mid window across runs
	trim(7, 8)                                       // single entry
	trim(3, 3)                                       // empty window
	trim(-5, 2)                                      // lo clamps to 0
	trim(int64(len(model))/3, 2*int64(len(model))/3) // wide interior window

	// The cold view a restart would see; the commit tails ride the
	// next flush, as they do under the server.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	z2 := r.reopen()
	if got := collectZ(t, z2, key, 0, int64(len(model))+8); !zrEqual(got, model) {
		t.Fatalf("cold survivors disagree with the model (%d vs %d entries)", len(got), len(model))
	}

	// Full-window trim drains everything and the key dies.
	want := int64(len(model))
	if got := remZ(t, r.z, key, 0, want+50); got != want {
		t.Fatalf("draining trim removed %d, want %d", got, want)
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

	// Trimming the corpse and an absent key are both 0.
	if got := remZ(t, r.z, key, 0, 5); got != 0 {
		t.Fatalf("trim on a dead key removed %d", got)
	}
	if got := remZ(t, r.z, "never-was", 0, 5); got != 0 {
		t.Fatalf("trim on an absent key removed %d", got)
	}
}

// TestZRemRangeRungs runs the trim cadence oracle on all three fence
// rungs: inline root, segmented with a flat fence, and segmented with
// fence pages under shrunk caps, where interior windows kill whole
// runs, leaves, and uppers.
func TestZRemRangeRungs(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		r := newZsetRig(t)
		rng := rand.New(rand.NewSource(61))
		pairs := map[string]float64{}
		for i := range 60 {
			m := fmt.Sprintf("m%02d", i)
			sc := float64(rng.Intn(11)) - 4.5
			r.zadd("z", m, sc, ZAddFlags{})
			pairs[m] = sc
		}
		checkTrimCadence(t, r, "z", zrModel(pairs))
	})
	t.Run("flat", func(t *testing.T) {
		r := newZsetRig(t)
		rng := rand.New(rand.NewSource(62))
		pairs := map[string]float64{}
		for i := range 400 {
			m := fmt.Sprintf("m%03d-%s", i, strings.Repeat("x", 24))
			sc := float64(rng.Intn(19)) * 1.25
			r.zadd("z", m, sc, ZAddFlags{})
			pairs[m] = sc
		}
		checkTrimCadence(t, r, "z", zrModel(pairs))
	})
	t.Run("paged", func(t *testing.T) {
		defer SetZFenceCapsForTest(3, 5, 3, 4)()
		r := newZsetRig(t)
		rng := rand.New(rand.NewSource(63))
		pairs := map[string]float64{}
		for i := range 700 {
			m := fmt.Sprintf("m%03d-%s", i, strings.Repeat("y", 24))
			sc := float64(rng.Intn(37)) - 11
			r.zadd("z", m, sc, ZAddFlags{})
			pairs[m] = sc
		}
		if paged, err := r.z.FencePagedForTest(context.Background(), []byte("z")); err != nil || !paged {
			t.Fatalf("fence paged = (%v, %v), the rung under test needs pages", paged, err)
		}
		checkTrimCadence(t, r, "z", zrModel(pairs))
	})
}

// TestZRemRangeInlineExactSpan pins the inline cut arithmetic: the
// window is a contiguous byte span of the ordered region, survivors
// are head plus tail.
func TestZRemRangeInlineExactSpan(t *testing.T) {
	r := newZsetRig(t)
	for i := range 7 {
		r.zadd("z", fmt.Sprintf("e%d", i), float64(i), ZAddFlags{})
	}
	if got := remZ(t, r.z, "z", 2, 5); got != 3 {
		t.Fatalf("mid trim removed %d, want 3", got)
	}
	want := []zrEnt{
		{s: zScoreSortable(0), m: "e0"},
		{s: zScoreSortable(1), m: "e1"},
		{s: zScoreSortable(5), m: "e5"},
		{s: zScoreSortable(6), m: "e6"},
	}
	if got := collectZ(t, r.z, "z", 0, 8); !zrEqual(got, want) {
		t.Fatalf("mid trim survivors = %v, want e0 e1 e5 e6", got)
	}
	if got := remZ(t, r.z, "z", 0, 4); got != 4 {
		t.Fatalf("draining trim removed %d, want 4", got)
	}
	st, _, _, err := r.z.h.stateOf(context.Background(), []byte("z"))
	if err != nil || st != hashAbsent {
		t.Fatalf("drained inline key state = (%v, %v), want absent", st, err)
	}
}
