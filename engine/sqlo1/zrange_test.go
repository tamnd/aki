package sqlo1

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
)

// zrEnt is one brute-force model entry.
type zrEnt struct {
	s uint64
	m string
}

// zrModel builds the sorted (sortable, member) view of what a test
// inserted: the oracle every rank assertion compares against.
func zrModel(pairs map[string]float64) []zrEnt {
	model := make([]zrEnt, 0, len(pairs))
	for m, sc := range pairs {
		model = append(model, zrEnt{s: zScoreSortable(sc), m: m})
	}
	sort.Slice(model, func(i, j int) bool {
		if model[i].s != model[j].s {
			return model[i].s < model[j].s
		}
		return model[i].m < model[j].m
	})
	return model
}

// zrRankOf is the brute-force insertion rank: entries strictly below
// the pair.
func zrRankOf(model []zrEnt, s uint64, m string) int64 {
	n := int64(0)
	for _, e := range model {
		if e.s < s || (e.s == s && e.m < m) {
			n++
		}
	}
	return n
}

// collect walks the rank window and copies what it emits (the walk's
// bytes die as it advances).
func (r *zsetRig) collect(key string, lo, hi int64) []zrEnt {
	r.t.Helper()
	return collectZ(r.t, r.z, key, lo, hi)
}

func collectZ(t *testing.T, z *ZSet, key string, lo, hi int64) []zrEnt {
	t.Helper()
	var out []zrEnt
	err := z.zwalkRank(context.Background(), []byte(key), lo, hi, func(s uint64, m []byte) bool {
		out = append(out, zrEnt{s: s, m: string(m)})
		return true
	})
	if err != nil {
		t.Fatalf("zwalkRank(%q, %d, %d): %v", key, lo, hi, err)
	}
	return out
}

func zrEqual(a, b []zrEnt) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// checkRankPrimitives runs the seek and walk oracles over one built
// key: every model pair's insertion rank (exact, successor, and
// score-only forms), the walk over a grid of windows, and the early
// stop.
func checkRankPrimitives(t *testing.T, r *zsetRig, key string, model []zrEnt) {
	t.Helper()
	ctx := context.Background()
	card := int64(len(model))

	probe := func(s uint64, m string) {
		t.Helper()
		rank, c, err := r.z.zseekRank(ctx, []byte(key), s, []byte(m))
		if err != nil {
			t.Fatalf("zseekRank(%#x, %q): %v", s, m, err)
		}
		if want := zrRankOf(model, s, m); rank != want || c != card {
			t.Fatalf("zseekRank(%#x, %q) = (%d, %d), want (%d, %d)", s, m, rank, c, want, card)
		}
	}
	for i, e := range model {
		if i%7 == 0 || i == len(model)-1 {
			probe(e.s, e.m)
			probe(e.s, e.m+"\x00")
			probe(e.s, "")
			probe(e.s+1, "")
		}
	}
	probe(0, "")
	probe(^uint64(0), "")

	for _, lo := range []int64{0, 1, card / 3, card - 1, card, card + 5} {
		for _, hi := range []int64{lo, lo + 1, lo + 7, card, card + 9} {
			got := r.collect(key, lo, hi)
			wlo, whi := min(max(lo, 0), card), min(max(hi, lo), card)
			if want := model[wlo:whi]; !zrEqual(got, want) {
				t.Fatalf("walk [%d, %d) of %q: %d entries, want %d", lo, hi, key, len(got), len(want))
			}
		}
	}

	// Early stop: the walk honors a false emit mid-window.
	if card >= 3 {
		seen := 0
		err := r.z.zwalkRank(ctx, []byte(key), 0, card, func(uint64, []byte) bool {
			seen++
			return seen < 3
		})
		if err != nil || seen != 3 {
			t.Fatalf("early stop saw %d entries (err %v), want 3", seen, err)
		}
	}
}

// TestZRankPrimitives drives zseekRank and zwalkRank against the
// brute-force model on all three rungs: the inline root, the flat
// segmented fence, and the paged fence under shrunk caps.
func TestZRankPrimitives(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		r := newZsetRig(t)
		rng := rand.New(rand.NewSource(41))
		pairs := map[string]float64{}
		for i := range 40 {
			m := fmt.Sprintf("m%02d", i)
			sc := float64(rng.Intn(9)) - 3.5
			r.zadd("z", m, sc, ZAddFlags{})
			pairs[m] = sc
		}
		checkRankPrimitives(t, r, "z", zrModel(pairs))
	})
	t.Run("flat", func(t *testing.T) {
		r := newZsetRig(t)
		rng := rand.New(rand.NewSource(42))
		pairs := map[string]float64{}
		for i := range 400 {
			m := fmt.Sprintf("m%03d-%s", i, strings.Repeat("x", 24))
			sc := float64(rng.Intn(23)) * 1.5
			r.zadd("z", m, sc, ZAddFlags{})
			pairs[m] = sc
		}
		checkRankPrimitives(t, r, "z", zrModel(pairs))
	})
	t.Run("paged", func(t *testing.T) {
		defer SetZFenceCapsForTest(3, 5, 3, 4)()
		r := newZsetRig(t)
		rng := rand.New(rand.NewSource(43))
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
		checkRankPrimitives(t, r, "z", zrModel(pairs))
	})
}

// TestZRankLexEqualScore pins the lex seek shape: one shared score,
// member-only ordering, zfirstSortable as the pairing score, and the
// low-byte successor for the exclusive side.
func TestZRankLexEqualScore(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(44))
	pairs := map[string]float64{}
	for i := range 300 {
		m := fmt.Sprintf("w%c%c-%03d", 'a'+rng.Intn(26), 'a'+rng.Intn(26), i)
		r.zadd("zl", m, 7, ZAddFlags{})
		pairs[m] = 7
	}
	model := zrModel(pairs)

	s0, ok, err := r.z.zfirstSortable(ctx, []byte("zl"))
	if err != nil || !ok || s0 != zScoreSortable(7) {
		t.Fatalf("zfirstSortable = (%#x, %v, %v), want the shared score", s0, ok, err)
	}
	if _, ok, err := r.z.zfirstSortable(ctx, []byte("nope")); err != nil || ok {
		t.Fatalf("zfirstSortable on an absent key = (ok=%v, %v)", ok, err)
	}

	for i, e := range model {
		if i%13 != 0 {
			continue
		}
		// Inclusive start [m: rank of (s0, m); exclusive start (m:
		// rank of the successor.
		rank, _, err := r.z.zseekRank(ctx, []byte("zl"), s0, []byte(e.m))
		if err != nil || rank != int64(i) {
			t.Fatalf("lex seek [%q = (%d, %v), want %d", e.m, rank, err, i)
		}
		rank, _, err = r.z.zseekRank(ctx, []byte("zl"), s0, append([]byte(e.m), 0))
		if err != nil || rank != int64(i+1) {
			t.Fatalf("lex seek (%q = (%d, %v), want %d", e.m, rank, err, i+1)
		}
	}
}

// TestZRangeStoreShapes lands windows on every destination rung and
// checks the stored zset from both families, hot and cold.
func TestZRangeStoreShapes(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(45))
	pairs := map[string]float64{}
	for i := range 500 {
		m := fmt.Sprintf("s%03d-%s", i, strings.Repeat("x", 24))
		sc := float64(rng.Intn(19)) - 4.25
		r.zadd("src", m, sc, ZAddFlags{})
		pairs[m] = sc
	}
	model := zrModel(pairs)

	verify := func(dest string, want []zrEnt) {
		t.Helper()
		if got := r.collect(dest, 0, int64(len(want))+8); !zrEqual(got, want) {
			t.Fatalf("stored %q walk holds %d entries, want %d", dest, len(got), len(want))
		}
		if card := r.zcard(dest); card != int64(len(want)) {
			t.Fatalf("stored %q card = %d, want %d", dest, card, len(want))
		}
		for i := 0; i < len(want); i += 1 + len(want)/9 {
			e := want[i]
			sc, ok := r.memscore(dest, e.m)
			if !ok || zScoreSortable(sc) != e.s {
				t.Fatalf("stored %q member %q = (%g, %v), want sortable %#x", dest, e.m, sc, ok, e.s)
			}
		}
		// The cold view a restart would see; the commit's tail rides
		// the next flush, as it does under the server.
		if err := r.tr.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		z2 := r.reopen()
		if got := collectZ(t, z2, dest, 0, int64(len(want))+8); !zrEqual(got, want) {
			t.Fatalf("cold %q walk disagrees with the hot one", dest)
		}
	}
	store := func(dest string, lo, hi int64, wantN int64) {
		t.Helper()
		n, err := r.z.ZRangeStore(ctx, []byte(dest), []byte("src"), lo, hi)
		if err != nil || n != wantN {
			t.Fatalf("ZRangeStore(%q, src, %d, %d) = (%d, %v), want %d", dest, lo, hi, n, err, wantN)
		}
	}
	state := func(key string, want hashState) {
		t.Helper()
		st, _, _, err := r.z.h.stateOf(ctx, []byte(key))
		if err != nil || st != want {
			t.Fatalf("stateOf(%q) = (%v, %v), want %v", key, st, err, want)
		}
	}

	// A small window lands inline.
	store("dst", 7, 12, 5)
	state("dst", hashInlineState)
	verify("dst", model[7:12])

	// A wide window over the same destination upgrades it in place.
	store("dst", 0, 450, 450)
	state("dst", hashSegState)
	verify("dst", model[:450])

	// Clamped past the end; then an empty window deletes.
	store("dst2", 490, 600, 10)
	verify("dst2", model[490:])
	store("dst2", 900, 950, 0)
	state("dst2", hashAbsent)

	// The destination is the source: the window collects fully before
	// the build tears the old plane down.
	store("src", 100, 200, 100)
	verify("src", model[100:200])
}

// TestZRangeStorePagedDest shrinks the fence caps so a stored window
// pages its score fence in the one-pass bulk build.
func TestZRangeStorePagedDest(t *testing.T) {
	defer SetZFenceCapsForTest(2, 4, 2, 6)()
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(46))
	pairs := map[string]float64{}
	for i := range 350 {
		m := fmt.Sprintf("p%03d-%s", i, strings.Repeat("z", 24))
		sc := float64(rng.Intn(13)) * 2.5
		r.zadd("src", m, sc, ZAddFlags{})
		pairs[m] = sc
	}
	model := zrModel(pairs)

	n, err := r.z.ZRangeStore(ctx, []byte("dst"), []byte("src"), 0, 320)
	if err != nil || n != 320 {
		t.Fatalf("ZRangeStore = (%d, %v), want 320", n, err)
	}
	if paged, err := r.z.FencePagedForTest(ctx, []byte("dst")); err != nil || !paged {
		t.Fatalf("stored fence paged = (%v, %v), the shrunk caps demand pages", paged, err)
	}
	if got := r.collect("dst", 0, 400); !zrEqual(got, model[:320]) {
		t.Fatalf("paged store walk holds %d entries, want 320", len(got))
	}
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	z2 := r.reopen()
	if got := collectZ(t, z2, "dst", 0, 400); !zrEqual(got, model[:320]) {
		t.Fatal("cold paged store walk disagrees")
	}
}

// TestZRangeStoreDestDoors covers the destination doors: a wrong-type
// value is overwritten, a carried TTL drops, and an empty result
// deletes whatever stood there.
func TestZRangeStoreDestDoors(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	for i := range 10 {
		r.zadd("src", fmt.Sprintf("m%d", i), float64(i), ZAddFlags{})
	}

	// A string destination is overwritten whatever it held.
	if err := r.s.Set(ctx, []byte("dst"), []byte("plain")); err != nil {
		t.Fatal(err)
	}
	if n, err := r.z.ZRangeStore(ctx, []byte("dst"), []byte("src"), 0, 4); err != nil || n != 4 {
		t.Fatalf("store over a string = (%d, %v)", n, err)
	}
	if sc, ok := r.memscore("dst", "m2"); !ok || sc != 2 {
		t.Fatalf("stored member = (%g, %v)", sc, ok)
	}

	// A carried TTL drops: the stored value is a fresh object.
	if _, err := r.tr.ExpireAt(ctx, []byte("dst"), 1<<41+60_000); err != nil {
		t.Fatal(err)
	}
	if n, err := r.z.ZRangeStore(ctx, []byte("dst"), []byte("src"), 0, 3); err != nil || n != 3 {
		t.Fatalf("re-store = (%d, %v)", n, err)
	}
	if _, _, expMs, ok, err := r.tr.LookupEntry(ctx, []byte("dst")); err != nil || !ok || expMs != 0 {
		t.Fatalf("stored dest expiry = (%d, ok=%v, %v), want cleared", expMs, ok, err)
	}

	// An empty window deletes the destination, and an absent source
	// behaves as one.
	if n, err := r.z.ZRangeStore(ctx, []byte("dst"), []byte("src"), 50, 60); err != nil || n != 0 {
		t.Fatalf("empty store = (%d, %v)", n, err)
	}
	if _, _, _, ok, err := r.tr.LookupEntry(ctx, []byte("dst")); err != nil || ok {
		t.Fatalf("dest survived the empty store (ok=%v, %v)", ok, err)
	}
	if n, err := r.z.ZRangeStore(ctx, []byte("dst"), []byte("ghost"), 0, 5); err != nil || n != 0 {
		t.Fatalf("absent-source store = (%d, %v)", n, err)
	}
}
