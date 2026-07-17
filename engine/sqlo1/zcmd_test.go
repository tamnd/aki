package sqlo1

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func (r *zsetRig) zadd(key, member string, score float64, f ZAddFlags) (bool, bool, float64, bool) {
	r.t.Helper()
	added, changed, out, outOK, err := r.z.ZAdd(context.Background(), []byte(key), []byte(member), score, f)
	if err != nil {
		r.t.Fatalf("ZAdd(%q, %q, %g, %+v): %v", key, member, score, f, err)
	}
	return added, changed, out, outOK
}

func (r *zsetRig) zincr(key string, incr float64, member string) float64 {
	r.t.Helper()
	out, err := r.z.ZIncrBy(context.Background(), []byte(key), incr, []byte(member))
	if err != nil {
		r.t.Fatalf("ZIncrBy(%q, %g, %q): %v", key, incr, member, err)
	}
	return out
}

func (r *zsetRig) zrem(key, member string) bool {
	r.t.Helper()
	removed, err := r.z.ZRem(context.Background(), []byte(key), []byte(member))
	if err != nil {
		r.t.Fatalf("ZRem(%q, %q): %v", key, member, err)
	}
	return removed
}

// score asserts member's stored score.
func (r *zsetRig) score(key, member string, want float64) {
	r.t.Helper()
	got, ok := r.memscore(key, member)
	if !ok || got != want {
		r.t.Fatalf("score(%q, %q) = (%g, %v), want (%g, true)", key, member, got, ok, want)
	}
}

// checkInlineOrder asserts the inline root region holds its entries in
// (score, member) order, the invariant zupgrade's straight run cut
// rests on.
func (r *zsetRig) checkInlineOrder(key string) {
	r.t.Helper()
	st, hi, _, err := r.z.h.stateOf(context.Background(), []byte(key))
	if err != nil || st != hashInlineState {
		r.t.Fatalf("stateOf(%q) = (%v, %v), want inline", key, st, err)
	}
	it := hashEntryIter{p: hi.entries, enc: r.z.h.enc}
	var pv, pf []byte
	for {
		f, v, _, ok, err := it.next()
		if err != nil {
			r.t.Fatalf("inline walk of %q: %v", key, err)
		}
		if !ok {
			return
		}
		if pv != nil {
			if c := bytes.Compare(pv, v); c > 0 || (c == 0 && bytes.Compare(pf, f) >= 0) {
				r.t.Fatalf("inline region of %q out of order at member %q", key, f)
			}
		}
		pv, pf = append(pv[:0], v...), append(pf[:0], f...)
	}
}

// TestZAddInline drives the flag surface on the inline rung: create,
// update, the four condition flags, INCR, the NaN door, and the kept
// (score, member) region order.
func TestZAddInline(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()

	if a, c, out, ok := r.zadd("z", "b", 2, ZAddFlags{}); !a || c || out != 2 || !ok {
		t.Fatalf("create = (%v, %v, %g, %v)", a, c, out, ok)
	}
	r.zadd("z", "a", 1, ZAddFlags{})
	r.zadd("z", "c", 3, ZAddFlags{})
	r.checkInlineOrder("z")

	// A moved score answers changed, an equal one answers neither.
	if a, c, out, ok := r.zadd("z", "b", 5, ZAddFlags{}); a || !c || out != 5 || !ok {
		t.Fatalf("update = (%v, %v, %g, %v)", a, c, out, ok)
	}
	if a, c, out, ok := r.zadd("z", "b", 5, ZAddFlags{}); a || c || out != 5 || !ok {
		t.Fatalf("equal-score rewrite = (%v, %v, %g, %v)", a, c, out, ok)
	}
	r.checkInlineOrder("z")

	// NX writes only creates, XX only updates.
	if a, c, _, ok := r.zadd("z", "b", 9, ZAddFlags{NX: true}); a || c || ok {
		t.Fatalf("NX on a live member = (%v, %v, ok=%v)", a, c, ok)
	}
	r.score("z", "b", 5)
	if a, _, _, _ := r.zadd("z", "d", 4, ZAddFlags{NX: true}); !a {
		t.Fatal("NX did not create the new member")
	}
	if a, c, _, ok := r.zadd("z", "nope", 7, ZAddFlags{XX: true}); a || c || ok {
		t.Fatalf("XX on a new member = (%v, %v, ok=%v)", a, c, ok)
	}
	if _, ok := r.memscore("z", "nope"); ok {
		t.Fatal("XX created a member")
	}
	if _, c, _, _ := r.zadd("z", "d", 6, ZAddFlags{XX: true}); !c {
		t.Fatal("XX did not update the live member")
	}

	// GT and LT gate on the stored score but never block a create.
	if _, _, _, ok := r.zadd("z", "d", 2, ZAddFlags{GT: true}); ok {
		t.Fatal("GT let a lower score through")
	}
	r.score("z", "d", 6)
	if _, c, _, _ := r.zadd("z", "d", 8, ZAddFlags{GT: true}); !c {
		t.Fatal("GT blocked a higher score")
	}
	if _, _, _, ok := r.zadd("z", "d", 9, ZAddFlags{LT: true}); ok {
		t.Fatal("LT let a higher score through")
	}
	if _, c, _, _ := r.zadd("z", "d", 1, ZAddFlags{LT: true}); !c {
		t.Fatal("LT blocked a lower score")
	}
	if a, _, _, _ := r.zadd("z", "gnew", 5, ZAddFlags{GT: true}); !a {
		t.Fatal("GT blocked a create")
	}

	// An absent key under XX stays absent.
	if a, c, _, ok := r.zadd("ghost", "m", 1, ZAddFlags{XX: true}); a || c || ok {
		t.Fatalf("XX on an absent key = (%v, %v, ok=%v)", a, c, ok)
	}
	if _, _, _, err := r.z.h.stateOf(ctx, []byte("ghost")); err != nil {
		t.Fatal(err)
	} else if st, _, _, _ := r.z.h.stateOf(ctx, []byte("ghost")); st != hashAbsent {
		t.Fatalf("XX left %v behind", st)
	}

	// INCR accumulates, vetoes answer no score, and NaN refuses.
	if got := r.zincr("z", 2.5, "b"); got != 7.5 {
		t.Fatalf("ZIncrBy = %g, want 7.5", got)
	}
	if got := r.zincr("z", -7.5, "b"); got != 0 {
		t.Fatalf("ZIncrBy back = %g, want 0", got)
	}
	if _, _, _, ok := r.zadd("z", "b", -1, ZAddFlags{Incr: true, GT: true}); ok {
		t.Fatal("GT INCR let a lowering increment through")
	}
	r.score("z", "b", 0)
	r.zincr("z", math.Inf(1), "b")
	if _, _, _, _, err := r.z.ZAdd(ctx, []byte("z"), []byte("b"), math.Inf(-1), ZAddFlags{Incr: true}); !errors.Is(err, ErrZSetNaN) {
		t.Fatalf("inf + -inf error = %v, want ErrZSetNaN", err)
	}
	r.score("z", "b", math.Inf(1))
	r.checkInlineOrder("z")

	if enc, ok, err := r.z.Encoding(ctx, []byte("z")); err != nil || !ok || enc != "listpack" {
		t.Fatalf("Encoding = (%q, %v, %v)", enc, ok, err)
	}
}

// TestZAddUpgradeDual crosses the inline thresholds through ZAdd and
// checks both families landed: every member score reads back, the run
// walk holds the exact (score, member) order, and the cold view after
// reopen agrees. The byte door gets its own key: one fat member that
// can never sit inline builds the segmented rung straight away.
func TestZAddUpgradeDual(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(21))

	const n = 200
	want := make([]zrunPair, 0, n)
	for i := range n {
		member := fmt.Sprintf("player:%04d", i)
		score := float64(rng.Intn(40)) // equal-score chains on purpose
		if a, _, _, _ := r.zadd("board", member, score, ZAddFlags{}); !a {
			t.Fatalf("ZAdd(%q) not created", member)
		}
		want = append(want, zrunPair{s: zScoreSortable(score), m: member})
	}
	sortZrunPairs(want)
	if enc, _, err := r.z.Encoding(ctx, []byte("board")); err != nil || enc != "skiplist" {
		t.Fatalf("Encoding = (%q, %v), want skiplist", enc, err)
	}
	if got := r.zcard("board"); got != n {
		t.Fatalf("ZCard = %d, want %d", got, n)
	}
	zrunCheck(t, r.z, "board", want)

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	cold := r.reopen()
	if got, err := cold.ZCard(ctx, []byte("board")); err != nil || got != n {
		t.Fatalf("cold ZCard = (%d, %v)", got, err)
	}
	zrunCheck(t, cold, "board", want)
	for i := 0; i < n; i += 37 {
		member := fmt.Sprintf("player:%04d", i)
		if _, ok, err := cold.memScore(ctx, []byte("board"), []byte(member)); err != nil || !ok {
			t.Fatalf("cold memScore(%q) = (%v, %v)", member, ok, err)
		}
	}

	fat := string(bytes.Repeat([]byte("f"), hashInlineMax))
	if a, _, _, _ := r.zadd("fat", fat, 1, ZAddFlags{}); !a {
		t.Fatal("fat create failed")
	}
	if enc, _, err := r.z.Encoding(ctx, []byte("fat")); err != nil || enc != "skiplist" {
		t.Fatalf("fat Encoding = (%q, %v), want skiplist", enc, err)
	}
	zrunCheck(t, r.z, "fat", []zrunPair{{s: zScoreSortable(1), m: fat}})
}

// TestZDualMutationLadder is the big random ladder: thousands of adds,
// score moves, increments, and removes against a shadow map, with the
// run walk asserting Z-I1 exactly (the two families are the same set)
// hot and cold.
func TestZDualMutationLadder(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(33))

	shadow := map[string]float64{}
	member := func() string { return fmt.Sprintf("m%04d", rng.Intn(900)) }
	for range 4000 {
		m := member()
		switch op := rng.Intn(10); {
		case op < 5: // add or move, quantized so equal chains form
			score := float64(rng.Intn(60)) / 4
			r.zadd("L", m, score, ZAddFlags{})
			shadow[m] = score
		case op < 7:
			out := r.zincr("L", float64(rng.Intn(9))-4, m)
			shadow[m] = out
		case op < 9:
			removed := r.zrem("L", m)
			_, held := shadow[m]
			if removed != held {
				t.Fatalf("ZRem(%q) = %v, shadow holds %v", m, removed, held)
			}
			delete(shadow, m)
		default: // the no-write door: a same-score rewrite
			if s, held := shadow[m]; held {
				if a, c, _, _ := r.zadd("L", m, s, ZAddFlags{}); a || c {
					t.Fatalf("same-score ZAdd(%q) = (%v, %v)", m, a, c)
				}
			}
		}
	}

	if got := r.zcard("L"); got != int64(len(shadow)) {
		t.Fatalf("ZCard = %d, shadow holds %d", got, len(shadow))
	}
	want := make([]zrunPair, 0, len(shadow))
	for m, s := range shadow {
		r.score("L", m, s)
		want = append(want, zrunPair{s: zScoreSortable(s), m: m})
	}
	sortZrunPairs(want)
	zrunCheck(t, r.z, "L", want)

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	cold := r.reopen()
	if got, err := cold.ZCard(ctx, []byte("L")); err != nil || got != int64(len(shadow)) {
		t.Fatalf("cold ZCard = (%d, %v), want %d", got, err, len(shadow))
	}
	zrunCheck(t, cold, "L", want)
}

// TestZRemDoors walks the removal shapes: inline, segmented dual, the
// missing member, the absent key, and the last member killing the key
// through the plane retire.
func TestZRemDoors(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()

	// Inline: HDel's rebuild keeps the order.
	r.zadd("zi", "a", 1, ZAddFlags{})
	r.zadd("zi", "b", 2, ZAddFlags{})
	if !r.zrem("zi", "a") || r.zrem("zi", "a") {
		t.Fatal("inline ZRem doors")
	}
	r.checkInlineOrder("zi")
	if r.zrem("ghost", "m") {
		t.Fatal("ZRem on an absent key answered true")
	}
	if !r.zrem("zi", "b") {
		t.Fatal("last inline member did not remove")
	}
	if st, _, _, err := r.z.h.stateOf(ctx, []byte("zi")); err != nil || st != hashAbsent {
		t.Fatalf("emptied inline key = (%v, %v), want absent", st, err)
	}

	// Segmented: every member out one by one, the dual delete all the
	// way down, the last one retiring the plane.
	const n = 160
	want := make([]zrunPair, 0, n)
	for i := range n {
		m := fmt.Sprintf("s%04d", i)
		r.zadd("zs", m, float64(i%9), ZAddFlags{})
		want = append(want, zrunPair{s: zScoreSortable(float64(i % 9)), m: m})
	}
	sortZrunPairs(want)
	zrunCheck(t, r.z, "zs", want)
	if r.zrem("zs", "never") {
		t.Fatal("segmented ZRem of a missing member answered true")
	}
	for i := range n {
		m := fmt.Sprintf("s%04d", i)
		if !r.zrem("zs", m) {
			t.Fatalf("ZRem(%q) answered false", m)
		}
	}
	if st, _, _, err := r.z.h.stateOf(ctx, []byte("zs")); err != nil || st != hashAbsent {
		t.Fatalf("emptied segmented key = (%v, %v), want absent", st, err)
	}
	if _, ok, err := r.z.Encoding(ctx, []byte("zs")); err != nil || ok {
		t.Fatalf("Encoding of the dead key = (ok=%v, %v)", ok, err)
	}
	if got := r.zcard("zs"); got != 0 {
		t.Fatalf("ZCard of the dead key = %d", got)
	}

	// The key comes back clean under a fresh plane.
	r.zadd("zs", "reborn", 7, ZAddFlags{})
	r.score("zs", "reborn", 7)
}

// TestZAddSegFlags reruns the condition flags on the segmented rung,
// where a veto must leave both families untouched.
func TestZAddSegFlags(t *testing.T) {
	r := newZsetRig(t)

	const n = 150
	want := make([]zrunPair, 0, n)
	for i := range n {
		m := fmt.Sprintf("p%04d", i)
		r.zadd("zf", m, float64(i), ZAddFlags{})
		want = append(want, zrunPair{s: zScoreSortable(float64(i)), m: m})
	}
	if a, c, _, ok := r.zadd("zf", "p0010", 99, ZAddFlags{NX: true}); a || c || ok {
		t.Fatal("segmented NX wrote over a live member")
	}
	if _, _, _, ok := r.zadd("zf", "p0010", 5, ZAddFlags{GT: true}); ok {
		t.Fatal("segmented GT let a lower score through")
	}
	if _, _, _, ok := r.zadd("zf", "p0010", 50, ZAddFlags{LT: true}); ok {
		t.Fatal("segmented LT let a higher score through")
	}
	r.score("zf", "p0010", 10)
	if a, c, _, ok := r.zadd("zf", "nope", 3, ZAddFlags{XX: true}); a || c || ok {
		t.Fatal("segmented XX created a member")
	}
	sortZrunPairs(want)
	zrunCheck(t, r.z, "zf", want)

	// A move on the segmented rung is one dual command: the old run
	// entry dies, the new one lands, both families agree.
	if _, c, _, _ := r.zadd("zf", "p0010", 200.5, ZAddFlags{}); !c {
		t.Fatal("segmented move did not answer changed")
	}
	for i := range want {
		if want[i].m == "p0010" {
			want[i].s = zScoreSortable(200.5)
		}
	}
	sortZrunPairs(want)
	zrunCheck(t, r.z, "zf", want)
	if got := r.zincr("zf", -0.5, "p0010"); got != 200 {
		t.Fatalf("segmented ZIncrBy = %g, want 200", got)
	}
	for i := range want {
		if want[i].m == "p0010" {
			want[i].s = zScoreSortable(200)
		}
	}
	sortZrunPairs(want)
	zrunCheck(t, r.z, "zf", want)
}

// TestZAddWrongType pins the write surface's cross-type doors.
func TestZAddWrongType(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()

	if err := r.s.Set(ctx, []byte("str"), []byte("plain")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := r.z.ZAdd(ctx, []byte("str"), []byte("m"), 1, ZAddFlags{}); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ZAdd error = %v, want ErrWrongType", err)
	}
	if _, err := r.z.ZIncrBy(ctx, []byte("str"), 1, []byte("m")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ZIncrBy error = %v, want ErrWrongType", err)
	}
	if _, err := r.z.ZRem(ctx, []byte("str"), []byte("m")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ZRem error = %v, want ErrWrongType", err)
	}
}

// TestZScoreImageStability pins that the member side's stored image is
// exactly the big-endian sortable the run fence routes on, across both
// rungs, so the two families can never disagree on a score's bytes.
func TestZScoreImageStability(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()

	scores := []float64{0, -0.0, 1.5, -2.25, math.Inf(1), math.Inf(-1), 1e-300, -1e300}
	for i, sc := range scores {
		r.zadd("zi", fmt.Sprintf("m%d", i), sc, ZAddFlags{})
	}
	st, hi, _, err := r.z.h.stateOf(ctx, []byte("zi"))
	if err != nil || st != hashInlineState {
		t.Fatalf("stateOf = (%v, %v)", st, err)
	}
	seen := 0
	it := hashEntryIter{p: hi.entries, enc: r.z.h.enc}
	for {
		_, v, _, ok, err := it.next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		s := binary.BigEndian.Uint64(v)
		if got := zScoreSortable(zScoreFromSortable(s)); got != s {
			t.Fatalf("stored sortable %#x does not round-trip", s)
		}
		seen++
	}
	if seen != len(scores) {
		t.Fatalf("inline holds %d members, want %d", seen, len(scores))
	}
	// -0 folds into +0 at the codec, so m1's stored score is plus zero.
	if got, ok := r.memscore("zi", "m1"); !ok || got != 0 || math.Signbit(got) {
		t.Fatalf("-0 read back as %g (signbit %v), want +0", got, math.Signbit(got))
	}
}
