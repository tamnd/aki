package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"
)

// zalgOracle folds source maps the way Redis 8.8 was measured to: in
// ascending cardinality order (ties in argument order), weights
// applied per contribution with the NaN-to-0 clamp, SUM clamping a
// NaN sum to 0. It returns the (sortable, member)-sorted result.
func zalgOracle(inter bool, srcs []map[string]float64, weights []float64, agg zaggMode) []zrEnt {
	order := make([]int, len(srcs))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return len(srcs[order[a]]) < len(srcs[order[b]])
	})
	acc := map[string]float64{}
	hits := map[string]int{}
	for _, i := range order {
		w := 1.0
		if weights != nil {
			w = weights[i]
		}
		for m, s := range srcs[i] {
			v := s * w
			if math.IsNaN(v) {
				v = 0
			}
			if n, seen := hits[m]; seen {
				acc[m] = zaggApply(acc[m], v, agg)
				hits[m] = n + 1
			} else {
				acc[m] = v
				hits[m] = 1
			}
		}
	}
	var out []zrEnt
	for m, s := range acc {
		if inter && hits[m] != len(srcs) {
			continue
		}
		out = append(out, zrEnt{s: zScoreSortable(s), m: m})
	}
	sortZrEnts(out)
	return out
}

// zdiffOracle keeps the first map's members that appear in none of
// the rest, raw scores.
func zdiffOracle(srcs []map[string]float64) []zrEnt {
	var out []zrEnt
	for m, s := range srcs[0] {
		in := false
		for _, o := range srcs[1:] {
			if _, ok := o[m]; ok {
				in = true
				break
			}
		}
		if !in {
			out = append(out, zrEnt{s: zScoreSortable(s), m: m})
		}
	}
	sortZrEnts(out)
	return out
}

func sortZrEnts(out []zrEnt) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].s != out[j].s {
			return out[i].s < out[j].s
		}
		return out[i].m < out[j].m
	})
}

// zpairEnts copies an algebra result out of its arena (the pairs die
// at the next algebra call).
func zpairEnts(pairs []zbuildPair, arena []byte) []zrEnt {
	out := make([]zrEnt, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, zrEnt{s: p.s, m: string(arena[p.off:p.end])})
	}
	return out
}

func (r *zsetRig) zunion(keys []string, weights []float64, agg zaggMode) []zrEnt {
	r.t.Helper()
	pairs, arena, err := r.z.ZUnion(context.Background(), keyBytes(keys), weights, agg)
	if err != nil {
		r.t.Fatalf("ZUnion(%v): %v", keys, err)
	}
	return zpairEnts(pairs, arena)
}

func (r *zsetRig) zinter(keys []string, weights []float64, agg zaggMode) []zrEnt {
	r.t.Helper()
	pairs, arena, err := r.z.ZInter(context.Background(), keyBytes(keys), weights, agg)
	if err != nil {
		r.t.Fatalf("ZInter(%v): %v", keys, err)
	}
	return zpairEnts(pairs, arena)
}

func (r *zsetRig) zdiff(keys ...string) []zrEnt {
	r.t.Helper()
	pairs, arena, err := r.z.ZDiff(context.Background(), keyBytes(keys))
	if err != nil {
		r.t.Fatalf("ZDiff(%v): %v", keys, err)
	}
	return zpairEnts(pairs, arena)
}

func wantZrEnts(t *testing.T, got, want []zrEnt) {
	t.Helper()
	if !zrEqual(got, want) {
		t.Fatalf("algebra result:\n got %v\nwant %v", got, want)
	}
}

// seedZ fills key with n members m<lo>..m<lo+n-1> scored by pick and
// returns the oracle map. Scores stay in exactly representable
// territory so SUM aggregation is fold-order-free.
func seedZ(t *testing.T, r *zsetRig, key string, lo, n int, pick func(i int) float64) map[string]float64 {
	t.Helper()
	want := map[string]float64{}
	for i := lo; i < lo+n; i++ {
		m := fmt.Sprintf("m%05d", i)
		sc := pick(i)
		r.zadd(key, m, sc, ZAddFlags{})
		want[m] = sc
	}
	return want
}

func TestZUnionInline(t *testing.T) {
	r := newZsetRig(t)
	za := map[string]float64{"a": 1, "b": 2, "c": 3}
	zb := map[string]float64{"b": 5, "d": 1.5}
	for m, s := range za {
		r.zadd("za", m, s, ZAddFlags{})
	}
	for m, s := range zb {
		r.zadd("zb", m, s, ZAddFlags{})
	}

	wantZrEnts(t, r.zunion([]string{"za", "zb"}, nil, zaggSum),
		zalgOracle(false, []map[string]float64{za, zb}, nil, zaggSum))
	wantZrEnts(t, r.zunion([]string{"za", "zb"}, []float64{2, 0.5}, zaggSum),
		zalgOracle(false, []map[string]float64{za, zb}, []float64{2, 0.5}, zaggSum))
	wantZrEnts(t, r.zunion([]string{"za", "zb"}, nil, zaggMin),
		zalgOracle(false, []map[string]float64{za, zb}, nil, zaggMin))
	wantZrEnts(t, r.zunion([]string{"za", "zb"}, nil, zaggMax),
		zalgOracle(false, []map[string]float64{za, zb}, nil, zaggMax))

	// One key copies, absent keys are empty sources, and a duplicated
	// key contributes twice (Redis's behavior, no key dedupe).
	wantZrEnts(t, r.zunion([]string{"za"}, nil, zaggSum),
		zalgOracle(false, []map[string]float64{za}, nil, zaggSum))
	wantZrEnts(t, r.zunion([]string{"ghost", "za", "ghost2"}, nil, zaggSum),
		zalgOracle(false, []map[string]float64{za}, nil, zaggSum))
	wantZrEnts(t, r.zunion([]string{"za", "za"}, nil, zaggSum),
		zalgOracle(false, []map[string]float64{za, za}, nil, zaggSum))
	if got := r.zunion([]string{"ghost", "ghost2"}, nil, zaggSum); len(got) != 0 {
		t.Fatalf("union of absents = %v, want empty", got)
	}
}

func TestZAlgebraDoors(t *testing.T) {
	r := newZsetRig(t)
	r.zadd("z", "a", 1, ZAddFlags{})
	if err := r.s.Set(context.Background(), []byte("str"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := r.h.HSet(context.Background(), []byte("hsh"), []byte("f"), []byte("v")); err != nil {
		t.Fatalf("HSet: %v", err)
	}

	ctx := context.Background()
	// Unlike the set family, an absent key never masks a wrong type:
	// Redis type-checks the whole zset source list up front.
	checks := []func(keys [][]byte) error{
		func(k [][]byte) error { _, _, err := r.z.ZUnion(ctx, k, nil, zaggSum); return err },
		func(k [][]byte) error { _, _, err := r.z.ZInter(ctx, k, nil, zaggSum); return err },
		func(k [][]byte) error { _, _, err := r.z.ZDiff(ctx, k); return err },
		func(k [][]byte) error { _, err := r.z.ZInterCard(ctx, k, 0); return err },
		func(k [][]byte) error { _, err := r.z.ZUnionStore(ctx, []byte("d"), k, nil, zaggSum); return err },
	}
	for i, walk := range checks {
		for _, keys := range [][]string{{"str", "z"}, {"ghost", "str"}, {"z", "ghost", "hsh"}} {
			if err := walk(keyBytes(keys)); !errors.Is(err, ErrWrongType) {
				t.Fatalf("op %d over %v = %v, want ErrWrongType", i, keys, err)
			}
		}
	}
}

// TestZAlgebraFoldOrder pins the measured Redis 8.8 aggregation
// order: ascending cardinality whatever the argument order, visible
// only when infinities meet the NaN-to-0 clamp.
func TestZAlgebraFoldOrder(t *testing.T) {
	r := newZsetRig(t)
	inf := math.Inf(1)
	r.zadd("f1", "p", inf, ZAddFlags{})
	r.zadd("f2", "p", inf, ZAddFlags{})
	r.zadd("f2", "q", 1, ZAddFlags{})
	r.zadd("f3", "p", -inf, ZAddFlags{})
	r.zadd("f3", "q", 1, ZAddFlags{})
	r.zadd("f3", "r", 1, ZAddFlags{})

	// Ascending fold: inf+inf = inf, then inf + -inf clamps to 0.
	// Argument-order fold from f3 would answer inf instead.
	for _, keys := range [][]string{{"f1", "f2", "f3"}, {"f3", "f1", "f2"}, {"f2", "f3", "f1"}} {
		got := r.zunion(keys, nil, zaggSum)
		want := []zrEnt{{s: zScoreSortable(0), m: "p"}, {s: zScoreSortable(1), m: "r"}, {s: zScoreSortable(2), m: "q"}}
		wantZrEnts(t, got, want)
		got = r.zinter(keys, nil, zaggSum)
		wantZrEnts(t, got, []zrEnt{{s: zScoreSortable(0), m: "p"}})
	}

	// Weight 0 times an infinity contributes 0, not NaN.
	got := r.zunion([]string{"f1"}, []float64{0}, zaggSum)
	wantZrEnts(t, got, []zrEnt{{s: zScoreSortable(0), m: "p"}})

	// MIN and MAX compare plainly, so -inf survives MIN.
	got = r.zunion([]string{"f1", "f3"}, nil, zaggMin)
	if got[0].m != "p" || got[0].s != zScoreSortable(-inf) {
		t.Fatalf("MIN fold = %v, want p at -inf first", got)
	}
}

func TestZUnionSegmented(t *testing.T) {
	r := newZsetRig(t)
	rng := rand.New(rand.NewSource(51))
	a := seedZ(t, r, "a", 0, 600, func(int) float64 { return float64(rng.Intn(41)) * 0.25 })
	b := seedZ(t, r, "b", 300, 700, func(int) float64 { return float64(rng.Intn(41)) - 20 })
	c := map[string]float64{"m00450": 100, "x": -3}
	for m, s := range c {
		r.zadd("c", m, s, ZAddFlags{})
	}
	srcs := []map[string]float64{a, b, c}
	weights := []float64{1, 2, 0.5}

	wantZrEnts(t, r.zunion([]string{"a", "b", "c"}, nil, zaggSum),
		zalgOracle(false, srcs, nil, zaggSum))
	wantZrEnts(t, r.zunion([]string{"a", "b", "c"}, weights, zaggSum),
		zalgOracle(false, srcs, weights, zaggSum))
	wantZrEnts(t, r.zunion([]string{"a", "b", "c"}, weights, zaggMax),
		zalgOracle(false, srcs, weights, zaggMax))

	// Cold: a fresh runtime over the same store folds identically.
	if err := r.tr.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	z := r.reopen()
	pairs, arena, err := z.ZUnion(context.Background(), keyBytes([]string{"a", "b", "c"}), weights, zaggSum)
	if err != nil {
		t.Fatalf("cold ZUnion: %v", err)
	}
	wantZrEnts(t, zpairEnts(pairs, arena), zalgOracle(false, srcs, weights, zaggSum))
}

func TestZInterSegmented(t *testing.T) {
	r := newZsetRig(t)
	rng := rand.New(rand.NewSource(52))
	a := seedZ(t, r, "a", 0, 800, func(int) float64 { return float64(rng.Intn(31)) * 0.5 })
	b := seedZ(t, r, "b", 400, 1200, func(int) float64 { return float64(rng.Intn(31)) * 1.5 })
	srcs := []map[string]float64{a, b}

	// Driver is the smaller source whichever side it is on.
	wantZrEnts(t, r.zinter([]string{"a", "b"}, nil, zaggSum),
		zalgOracle(true, srcs, nil, zaggSum))
	wantZrEnts(t, r.zinter([]string{"b", "a"}, nil, zaggSum),
		zalgOracle(true, []map[string]float64{b, a}, nil, zaggSum))
	weights := []float64{3, 0.25}
	wantZrEnts(t, r.zinter([]string{"a", "b"}, weights, zaggMin),
		zalgOracle(true, srcs, weights, zaggMin))

	// An inline driver against a segmented source routes through the
	// same window filter, and any absent source empties the result.
	tiny := map[string]float64{"m00500": 7, "m01100": -2, "nothere": 1}
	for m, s := range tiny {
		r.zadd("tiny", m, s, ZAddFlags{})
	}
	wantZrEnts(t, r.zinter([]string{"tiny", "b"}, nil, zaggSum),
		zalgOracle(true, []map[string]float64{tiny, b}, nil, zaggSum))
	if got := r.zinter([]string{"a", "ghost", "b"}, nil, zaggSum); len(got) != 0 {
		t.Fatalf("inter with absent = %v, want empty", got)
	}

	if err := r.tr.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	z := r.reopen()
	pairs, arena, err := z.ZInter(context.Background(), keyBytes([]string{"a", "b"}), nil, zaggSum)
	if err != nil {
		t.Fatalf("cold ZInter: %v", err)
	}
	wantZrEnts(t, zpairEnts(pairs, arena), zalgOracle(true, srcs, nil, zaggSum))
}

func TestZInterPaged(t *testing.T) {
	r := newZsetRig(t)
	// Long members push the member fence past its flat cap so the
	// root pages; the probe routing then crosses fence pages.
	long := func(i int) string {
		return fmt.Sprintf("f%05d-%s", i, strings.Repeat("x", 54))
	}
	big := map[string]float64{}
	for i := range 8000 {
		m := long(i)
		sc := float64(i % 97)
		r.zadd("big", m, sc, ZAddFlags{})
		big[m] = sc
	}
	if st, _, _, err := r.z.h.stateOf(context.Background(), []byte("big")); err != nil || st != hashSegState {
		t.Fatalf("stateOf(big) = %v, %v", st, err)
	}
	if !r.z.h.segRoot.paged {
		t.Fatal("big is not paged; the fixture lost its point")
	}

	probe := map[string]float64{}
	for i := 100; i < 8000; i += 800 {
		m := long(i)
		r.zadd("probe", m, 0.5, ZAddFlags{})
		probe[m] = 0.5
	}
	r.zadd("probe", "absent-member", 1, ZAddFlags{})
	probe["absent-member"] = 1

	// The small driver probes across page boundaries, and the paged
	// zset as the larger source stays the probe target whichever side
	// of the argument list it is on.
	want := zalgOracle(true, []map[string]float64{probe, big}, nil, zaggSum)
	wantZrEnts(t, r.zinter([]string{"probe", "big"}, nil, zaggSum), want)
	wantZrEnts(t, r.zinter([]string{"big", "probe"}, nil, zaggSum), want)

	// Union walks the paged source through the cursor's page loop.
	wantZrEnts(t, r.zunion([]string{"probe", "big"}, nil, zaggSum),
		zalgOracle(false, []map[string]float64{probe, big}, nil, zaggSum))

	if err := r.tr.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	z := r.reopen()
	pairs, arena, err := z.ZInter(context.Background(), keyBytes([]string{"probe", "big"}), nil, zaggSum)
	if err != nil {
		t.Fatalf("cold paged ZInter: %v", err)
	}
	wantZrEnts(t, zpairEnts(pairs, arena), want)
}

func TestZInterCard(t *testing.T) {
	r := newZsetRig(t)
	seedZ(t, r, "a", 0, 800, func(int) float64 { return 1 })
	seedZ(t, r, "b", 400, 1200, func(int) float64 { return 2 })

	card := func(limit int64, keys ...string) int64 {
		t.Helper()
		n, err := r.z.ZInterCard(context.Background(), keyBytes(keys), limit)
		if err != nil {
			t.Fatalf("ZInterCard(%v, %d): %v", keys, limit, err)
		}
		return n
	}
	if n := card(0, "a", "b"); n != 400 {
		t.Fatalf("unlimited card = %d, want 400", n)
	}
	if n := card(150, "a", "b"); n != 150 {
		t.Fatalf("limited card = %d, want 150", n)
	}
	if n := card(4000, "a", "b"); n != 400 {
		t.Fatalf("over-limit card = %d, want 400", n)
	}
	if n := card(0, "a", "ghost"); n != 0 {
		t.Fatalf("absent-key card = %d, want 0", n)
	}
	if n := card(0, "a"); n != 800 {
		t.Fatalf("single-key card = %d, want 800", n)
	}
	if n := card(1, "a", "a"); n != 1 {
		t.Fatalf("self card at limit 1 = %d, want 1", n)
	}
}

func TestZDiff(t *testing.T) {
	r := newZsetRig(t)
	rng := rand.New(rand.NewSource(53))
	a := seedZ(t, r, "a", 0, 800, func(int) float64 { return float64(rng.Intn(101)) - 50 })
	b := seedZ(t, r, "b", 400, 1200, func(int) float64 { return 999 })
	c := map[string]float64{"m00000": 1, "m00001": 2}
	for m, s := range c {
		r.zadd("c", m, s, ZAddFlags{})
	}

	// The first key drives whatever its size and keeps its raw
	// scores; rest scores never matter.
	wantZrEnts(t, r.zdiff("a", "b", "c"), zdiffOracle([]map[string]float64{a, b, c}))
	wantZrEnts(t, r.zdiff("c", "a"), zdiffOracle([]map[string]float64{c, a}))
	if got := r.zdiff("ghost", "a"); len(got) != 0 {
		t.Fatalf("diff from absent = %v, want empty", got)
	}
	wantZrEnts(t, r.zdiff("c", "ghost"), zdiffOracle([]map[string]float64{c}))
	wantZrEnts(t, r.zdiff("c"), zdiffOracle([]map[string]float64{c}))
	if got := r.zdiff("a", "a"); len(got) != 0 {
		t.Fatalf("self diff = %v, want empty", got)
	}

	if err := r.tr.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	z := r.reopen()
	pairs, arena, err := z.ZDiff(context.Background(), keyBytes([]string{"a", "b", "c"}))
	if err != nil {
		t.Fatalf("cold ZDiff: %v", err)
	}
	wantZrEnts(t, zpairEnts(pairs, arena), zdiffOracle([]map[string]float64{a, b, c}))
}

// TestZAlgebraSetSources pins Redis's cross-type rule: a plain set is
// a legal source anywhere in the family and every member scores 1,
// weights included.
func TestZAlgebraSetSources(t *testing.T) {
	r := newZsetRig(t)
	za := map[string]float64{"a": 1, "b": 2, "c": 3}
	for m, s := range za {
		r.zadd("za", m, s, ZAddFlags{})
	}
	st := map[string]float64{"b": 1, "x": 1}
	for m := range st {
		if _, err := r.se.SAdd(context.Background(), []byte("st"), []byte(m)); err != nil {
			t.Fatalf("SAdd: %v", err)
		}
	}

	// The measured Redis 8.8 answers: zunion za st = a 1, x 1, b 3,
	// c 3; zinter = b 3; zdiff = a 1, c 3.
	wantZrEnts(t, r.zunion([]string{"za", "st"}, nil, zaggSum),
		zalgOracle(false, []map[string]float64{za, st}, nil, zaggSum))
	wantZrEnts(t, r.zinter([]string{"za", "st"}, nil, zaggSum),
		[]zrEnt{{s: zScoreSortable(3), m: "b"}})
	wantZrEnts(t, r.zdiff("za", "st"),
		[]zrEnt{{s: zScoreSortable(1), m: "a"}, {s: zScoreSortable(3), m: "c"}})

	// Weights scale the set's 1.0 like any score, sets can drive, and
	// a pure-set operation still answers a zset.
	wantZrEnts(t, r.zunion([]string{"za", "st"}, []float64{1, 5}, zaggSum),
		zalgOracle(false, []map[string]float64{za, st}, []float64{1, 5}, zaggSum))
	wantZrEnts(t, r.zinter([]string{"st", "za"}, nil, zaggMax),
		zalgOracle(true, []map[string]float64{st, za}, nil, zaggMax))
	st2 := map[string]float64{"x": 1, "y": 1}
	for m := range st2 {
		if _, err := r.se.SAdd(context.Background(), []byte("st2"), []byte(m)); err != nil {
			t.Fatalf("SAdd: %v", err)
		}
	}
	wantZrEnts(t, r.zunion([]string{"st", "st2"}, nil, zaggSum),
		zalgOracle(false, []map[string]float64{st, st2}, nil, zaggSum))
	if n, err := r.z.ZInterCard(context.Background(), keyBytes([]string{"za", "st"}), 0); err != nil || n != 1 {
		t.Fatalf("ZInterCard(za, st) = %d, %v, want 1", n, err)
	}

	// A larger segmented set behind a zset driver probes as a set.
	bigset := map[string]float64{}
	for i := range 900 {
		m := fmt.Sprintf("m%05d", i)
		if _, err := r.se.SAdd(context.Background(), []byte("bigset"), []byte(m)); err != nil {
			t.Fatalf("SAdd: %v", err)
		}
		bigset[m] = 1
	}
	zw := seedZ(t, r, "zw", 850, 100, func(i int) float64 { return float64(i) })
	wantZrEnts(t, r.zinter([]string{"zw", "bigset"}, nil, zaggSum),
		zalgOracle(true, []map[string]float64{zw, bigset}, nil, zaggSum))
}

func TestZAlgebraStore(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(54))
	a := seedZ(t, r, "a", 0, 500, func(int) float64 { return float64(rng.Intn(41)) * 0.5 })
	b := seedZ(t, r, "b", 250, 500, func(int) float64 { return float64(rng.Intn(41)) - 10 })
	weights := []float64{2, 0.5}

	checkDest := func(want []zrEnt) {
		t.Helper()
		got := collectZ(t, r.z, "dest", 0, int64(len(want))+10)
		wantZrEnts(t, got, want)
		if n := r.zcard("dest"); n != int64(len(want)) {
			t.Fatalf("dest card = %d, want %d", n, len(want))
		}
	}

	n, err := r.z.ZUnionStore(ctx, []byte("dest"), keyBytes([]string{"a", "b"}), weights, zaggSum)
	if err != nil {
		t.Fatalf("ZUnionStore: %v", err)
	}
	want := zalgOracle(false, []map[string]float64{a, b}, weights, zaggSum)
	if n != int64(len(want)) {
		t.Fatalf("ZUnionStore = %d, want %d", n, len(want))
	}
	checkDest(want)

	// The store overwrites whatever dest held, any type.
	n, err = r.z.ZInterStore(ctx, []byte("dest"), keyBytes([]string{"a", "b"}), nil, zaggMax)
	if err != nil {
		t.Fatalf("ZInterStore: %v", err)
	}
	want = zalgOracle(true, []map[string]float64{a, b}, nil, zaggMax)
	if n != int64(len(want)) {
		t.Fatalf("ZInterStore = %d, want %d", n, len(want))
	}
	checkDest(want)

	if err := r.s.Set(ctx, []byte("strdest"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := r.z.ZDiffStore(ctx, []byte("strdest"), keyBytes([]string{"a", "b"})); err != nil {
		t.Fatalf("ZDiffStore over a string dest: %v", err)
	}
	wantDiff := zdiffOracle([]map[string]float64{a, b})
	got := collectZ(t, r.z, "strdest", 0, int64(len(wantDiff))+10)
	wantZrEnts(t, got, wantDiff)

	// An empty result deletes dest.
	n, err = r.z.ZInterStore(ctx, []byte("dest"), keyBytes([]string{"a", "ghost"}), nil, zaggSum)
	if err != nil || n != 0 {
		t.Fatalf("empty ZInterStore = %d, %v, want 0", n, err)
	}
	if st, _, _, err := r.z.h.stateOf(ctx, []byte("dest")); err != nil || st != hashAbsent {
		t.Fatalf("dest after empty store = %v, %v, want absent", st, err)
	}

	// Cold: the stored zset reads back whole on a fresh runtime.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	z := r.reopen()
	wantZrEnts(t, collectZ(t, z, "strdest", 0, int64(len(wantDiff))+10), wantDiff)
}

// TestZAlgebraStorePagedDest drives the bulk build into fence-paged
// score territory under test caps, ZRANGESTORE's paged-dest shape on
// the algebra path.
func TestZAlgebraStorePagedDest(t *testing.T) {
	defer SetZFenceCapsForTest(2, 4, 2, 6)()
	r := newZsetRig(t)
	ctx := context.Background()
	a := seedZ(t, r, "a", 0, 400, func(i int) float64 { return float64(i % 53) })
	b := seedZ(t, r, "b", 200, 400, func(i int) float64 { return float64(i % 37) })

	n, err := r.z.ZUnionStore(ctx, []byte("dest"), keyBytes([]string{"a", "b"}), nil, zaggSum)
	if err != nil {
		t.Fatalf("ZUnionStore: %v", err)
	}
	want := zalgOracle(false, []map[string]float64{a, b}, nil, zaggSum)
	if n != int64(len(want)) {
		t.Fatalf("ZUnionStore = %d, want %d", n, len(want))
	}
	if paged, err := r.z.FencePagedForTest(ctx, []byte("dest")); err != nil || !paged {
		t.Fatalf("dest fence paged = (%v, %v), the shape under test needs pages", paged, err)
	}
	wantZrEnts(t, collectZ(t, r.z, "dest", 0, int64(len(want))+10), want)

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	z := r.reopen()
	wantZrEnts(t, collectZ(t, z, "dest", 0, int64(len(want))+10), want)
}
