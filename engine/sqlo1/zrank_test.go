package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
)

// checkZRanks holds every member's ZRank, ZRevRank, and carried score
// against a sorted reference of the expected pairs, then sprays a few
// absent members.
func checkZRanks(t *testing.T, z *ZSet, key string, scores map[string]float64) {
	t.Helper()
	ctx := context.Background()
	type pair struct {
		s uint64
		m string
	}
	want := make([]pair, 0, len(scores))
	for m, sc := range scores {
		want = append(want, pair{zScoreSortable(sc), m})
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].s != want[j].s {
			return want[i].s < want[j].s
		}
		return want[i].m < want[j].m
	})
	for i, p := range want {
		rank, score, ok, err := z.ZRank(ctx, []byte(key), []byte(p.m))
		if err != nil {
			t.Fatalf("ZRank(%q): %v", p.m, err)
		}
		if !ok || rank != int64(i) || zScoreSortable(score) != p.s {
			t.Fatalf("ZRank(%q) = (%d, %g, %v), want (%d, %g, true)", p.m, rank, score, ok, i, scores[p.m])
		}
		rev, score, ok, err := z.ZRevRank(ctx, []byte(key), []byte(p.m))
		if err != nil {
			t.Fatalf("ZRevRank(%q): %v", p.m, err)
		}
		if wantRev := int64(len(want) - 1 - i); !ok || rev != wantRev || zScoreSortable(score) != p.s {
			t.Fatalf("ZRevRank(%q) = (%d, %g, %v), want (%d, %g, true)", p.m, rev, score, ok, wantRev, scores[p.m])
		}
	}
	for _, m := range []string{"nobody", "zzzz", ""} {
		if _, ok := scores[m]; ok {
			continue
		}
		if _, _, ok, err := z.ZRank(ctx, []byte(key), []byte(m)); err != nil || ok {
			t.Fatalf("ZRank(absent %q) = (ok=%v, err=%v), want a miss", m, ok, err)
		}
		if _, _, ok, err := z.ZRevRank(ctx, []byte(key), []byte(m)); err != nil || ok {
			t.Fatalf("ZRevRank(absent %q) = (ok=%v, err=%v), want a miss", m, ok, err)
		}
	}
}

// TestZRankLadder drives the rank walk over the whole representation
// ladder: a churned segmented board (creates, score moves, removes,
// splits and merges included), the reference check at every member,
// then the same board reopened cold.
func TestZRankLadder(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(31))

	scores := make(map[string]float64)
	members := make([]string, 0, 1200)
	for i := 0; i < 2400; i++ {
		m := fmt.Sprintf("player:%05d", rng.Intn(1500))
		sc := math.Round(rng.NormFloat64()*8000) / 8
		r.zadd("board", m, sc, ZAddFlags{})
		if _, seen := scores[m]; !seen {
			members = append(members, m)
		}
		scores[m] = sc
		if i%9 == 0 && len(members) > 10 {
			j := rng.Intn(len(members))
			if r.zrem("board", members[j]) {
				delete(scores, members[j])
				members[j] = members[len(members)-1]
				members = members[:len(members)-1]
			}
		}
	}
	if enc, ok, err := r.z.Encoding(ctx, []byte("board")); err != nil || !ok || enc != "skiplist" {
		t.Fatalf("board encoding = (%q, %v, %v), want segmented", enc, ok, err)
	}
	if got := r.zcard("board"); got != int64(len(scores)) {
		t.Fatalf("ZCard = %d, want %d", got, len(scores))
	}
	checkZRanks(t, r.z, "board", scores)

	if _, _, ok, err := r.z.ZRank(ctx, []byte("nokey"), []byte("a")); err != nil || ok {
		t.Fatalf("ZRank on an absent key = (ok=%v, err=%v)", ok, err)
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cold := r.reopen()
	checkZRanks(t, cold, "board", scores)
}

// TestZRankEqualScoreChain pins the rank walk on the lex shape: every
// member shares one score, the fence is a chain of equal separators,
// and the rank must still land through the first-entry routing.
func TestZRankEqualScoreChain(t *testing.T) {
	r := newZsetRig(t)
	scores := make(map[string]float64)
	for i := range 900 {
		m := fmt.Sprintf("word:%06d", i*7919%1000000)
		r.zadd("lex", m, 3.25, ZAddFlags{})
		scores[m] = 3.25
	}
	if len(r.z.zfence) < 3 {
		t.Fatalf("lex fence has %d runs, the chain needs several", len(r.z.zfence))
	}
	checkZRanks(t, r.z, "lex", scores)
}

// TestZRankInline pins the inline rung: the region's sort order is
// the rank order, no runs anywhere.
func TestZRankInline(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	scores := map[string]float64{}
	for m, sc := range map[string]float64{"a": 2, "b": -1, "c": 2, "d": 0.5} {
		r.zadd("zi", m, sc, ZAddFlags{})
		scores[m] = sc
	}
	if enc, ok, err := r.z.Encoding(ctx, []byte("zi")); err != nil || !ok || enc != "listpack" {
		t.Fatalf("zi encoding = (%q, %v, %v), want inline", enc, ok, err)
	}
	checkZRanks(t, r.z, "zi", scores)
}

// TestZScoreSurface pins ZScore and ZMScore: hits, misses, argument
// order, and the wrong-type door.
func TestZScoreSurface(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	r.zadd("z", "a", 1.5, ZAddFlags{})
	r.zadd("z", "b", -7, ZAddFlags{})

	if got, ok, err := r.z.ZScore(ctx, []byte("z"), []byte("a")); err != nil || !ok || got != 1.5 {
		t.Fatalf("ZScore(a) = (%g, %v, %v)", got, ok, err)
	}
	if _, ok, err := r.z.ZScore(ctx, []byte("z"), []byte("nope")); err != nil || ok {
		t.Fatalf("ZScore(nope) = (ok=%v, err=%v)", ok, err)
	}

	var got []struct {
		v  float64
		ok bool
	}
	err := r.z.ZMScore(ctx, []byte("z"), [][]byte{[]byte("b"), []byte("nope"), []byte("a")}, func(v float64, ok bool) {
		got = append(got, struct {
			v  float64
			ok bool
		}{v, ok})
	})
	if err != nil {
		t.Fatalf("ZMScore: %v", err)
	}
	if len(got) != 3 || !got[0].ok || got[0].v != -7 || got[1].ok || !got[2].ok || got[2].v != 1.5 {
		t.Fatalf("ZMScore answered %+v", got)
	}

	if err := r.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, _, _, err := r.z.ZRank(ctx, []byte("str"), []byte("a")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ZRank on a string = %v, want ErrWrongType", err)
	}
	if _, _, err := r.z.ZScore(ctx, []byte("str"), []byte("a")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ZScore on a string = %v, want ErrWrongType", err)
	}
}
