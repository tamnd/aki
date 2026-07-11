package zset

import (
	"math/rand/v2"
	"os"
	"testing"

	"github.com/tamnd/aki/f3srv/resp"
)

// The ZREMRANGEBY* correctness bars (spec 2064/f3/12 section 6.9). Each command
// resolves its bounds to a rank window and z.removeRange deletes it as one bounded
// operation, keeping the dual structure consistent (tree entries gone and member
// records deleted) so the surviving order, cardinality, and ranks all still agree
// with a sorted model. The removal is inline, so the reply count is exactly the
// window size and the structure is whole the moment removeRange returns.

// removeRankWindow removes the sorted model entries at inclusive ranks [lo, hi],
// the byrank window, and returns the survivors as a fresh model map.
func removeRankWindow(sm []pairMS, lo, hi int) map[string]float64 {
	out := map[string]float64{}
	for i, p := range sm {
		if i >= lo && i <= hi {
			continue
		}
		out[p.m] = p.s
	}
	return out
}

// assertMatchesModel checks z agrees with the model on cardinality and full order.
func assertMatchesModel(t *testing.T, z *zset, model map[string]float64, ctx string) {
	t.Helper()
	if z.card() != len(model) {
		t.Fatalf("%s: card %d, model %d", ctx, z.card(), len(model))
	}
	want := sortedModel(model)
	ev := z.entries()
	if len(ev) != len(want) {
		t.Fatalf("%s: entries %d, model %d", ctx, len(ev), len(want))
	}
	for i := range want {
		if string(ev[i].member) != want[i].m || ev[i].score != want[i].s {
			t.Fatalf("%s: rank %d = (%q,%v), model (%q,%v)", ctx, i,
				ev[i].member, ev[i].score, want[i].m, want[i].s)
		}
	}
	// A spot rank probe confirms the hash and the tree still agree after surgery.
	for i, p := range want {
		r, _, ok := z.rank([]byte(p.m))
		if !ok || r != i {
			t.Fatalf("%s: rank(%q) = %d,%v, want %d,true", ctx, p.m, r, ok, i)
		}
	}
}

// TestRemRangeByRankOracle drives random ZADD churn and random ZREMRANGEBYRANK
// windows against a sorted model, checking the full order, cardinality, ranks,
// and removed count agree after every removal. Two member spaces keep one run
// inline and push the other through the promotion and past the reclaim rebuild
// thresholds, so the inline splice and the native tree-window delete are both
// covered, tied score bands included.
func TestRemRangeByRankOracle(t *testing.T) {
	for _, space := range []int{16, 400} {
		space := space
		t.Run(map[bool]string{true: "inline", false: "native"}[space <= 100], func(t *testing.T) {
			rng := rand.New(rand.NewPCG(0x616, uint64(space)))
			z := newZset()
			model := map[string]float64{}

			fill := func(n int) {
				for i := 0; i < n; i++ {
					m := "m" + itoa(rng.IntN(space))
					s := float64(rng.IntN(8) - 4)
					z.update([]byte(m), s, flags{})
					model[m] = s
				}
			}

			for round := 0; round < 60; round++ {
				fill(space)
				sm := sortedModel(model)
				card := len(sm)
				if card == 0 {
					continue
				}
				start := rng.IntN(2*card) - card
				stop := rng.IntN(2*card) - card
				lo, hi, empty := clampRange(start, stop, card)
				want := 0
				if !empty {
					want = hi - lo + 1
				}
				got := z.removeRange(lo, hiOf(lo, hi, empty))
				if got != want {
					t.Fatalf("round %d: removeRange(%d,%d) removed %d, want %d", round, start, stop, got, want)
				}
				if !empty {
					model = removeRankWindow(sm, lo, hi)
				}
				assertMatchesModel(t, z, model, "after byrank window")
			}
		})
	}
}

// hiOf turns the clamp result into the half-open window removeRange takes: an
// empty window is [lo, lo), a live window is [lo, hi+1).
func hiOf(lo, hi int, empty bool) int {
	if empty {
		return lo
	}
	return hi + 1
}

// TestRemRangeByRankBoundaries pins the byrank edge cases at the removeRange
// level: negative indices count from the end, an inverted or out-of-range window
// removes nothing, the whole-set window empties the set, and a native band stays
// consistent after each.
func TestRemRangeByRankBoundaries(t *testing.T) {
	build := func(n int) (*zset, map[string]float64) {
		z := newZset()
		model := map[string]float64{}
		for i := 0; i < n; i++ {
			m := "m" + pad(i)
			z.update([]byte(m), float64(i), flags{})
			model[m] = float64(i)
		}
		return z, model
	}

	// Inverted window: nothing removed.
	z, model := build(200)
	if got := z.removeRange(clampToHalfOpen(5, 2, z.card())); got != 0 {
		t.Fatalf("inverted window removed %d, want 0", got)
	}
	assertMatchesModel(t, z, model, "inverted")

	// Last two via negatives.
	z, model = build(200)
	sm := sortedModel(model)
	lo, hi, _ := clampRange(-2, -1, z.card())
	if got := z.removeRange(lo, hi+1); got != 2 {
		t.Fatalf("last-two removed %d, want 2", got)
	}
	model = removeRankWindow(sm, lo, hi)
	assertMatchesModel(t, z, model, "last two")

	// Whole set: emptied.
	z, _ = build(200)
	if got := z.removeRange(0, z.card()); got != 200 {
		t.Fatalf("whole-set removed %d, want 200", got)
	}
	if z.card() != 0 {
		t.Fatalf("whole-set left card %d, want 0", z.card())
	}
	if len(z.entries()) != 0 {
		t.Fatal("whole-set left entries behind")
	}
}

// clampToHalfOpen resolves a byrank window to the half-open form removeRange takes.
func clampToHalfOpen(start, stop, card int) (int, int) {
	lo, hi, empty := clampRange(start, stop, card)
	return lo, hiOf(lo, hi, empty)
}

// TestRemRangeByScore checks ZREMRANGEBYSCORE over inclusive, exclusive, and
// infinite bounds removes exactly the score band and leaves the rest, on both
// bands. The expected band is computed by the bound predicates directly, not by
// the window resolver under test.
func TestRemRangeByScore(t *testing.T) {
	for _, space := range []int{16, 400} {
		bounds := []string{"-inf", "+inf", "-3", "(-3", "0", "(0", "3", "(3"}
		for _, loArg := range bounds {
			for _, hiArg := range bounds {
				z, model := seedScored(space)
				min, _ := parseScoreBound([]byte(loArg))
				max, _ := parseScoreBound([]byte(hiArg))
				lo, hiExcl := z.scoreWindow(min, max)
				want := 0
				surv := map[string]float64{}
				for m, s := range model {
					if !scoreBelowLow(s, min) && scoreWithinHigh(s, max) {
						want++
						continue
					}
					surv[m] = s
				}
				got := z.removeRange(lo, hiExcl)
				if got != want {
					t.Fatalf("space %d BYSCORE %s %s removed %d, want %d", space, loArg, hiArg, got, want)
				}
				assertMatchesModel(t, z, surv, "byscore "+loArg+" "+hiArg)
			}
		}
	}
}

// TestRemRangeByLex checks ZREMRANGEBYLEX over a single tied score band removes
// exactly the lex band, on both bands.
func TestRemRangeByLex(t *testing.T) {
	for _, space := range []int{16, 400} {
		bounds := []string{"-", "+", "[k1", "(k1", "[k2", "(k2", "[k", "(k9"}
		for _, loArg := range bounds {
			for _, hiArg := range bounds {
				z, model := seedLex(space)
				min, _ := parseLexBound([]byte(loArg))
				max, _ := parseLexBound([]byte(hiArg))
				lo, hiExcl := z.lexWindow(min, max)
				want := hiExcl - lo
				sm := sortedModel(model)
				surv := removeRankWindow(sm, lo, hiExcl-1)
				got := z.removeRange(lo, hiExcl)
				if got != want {
					t.Fatalf("space %d BYLEX %s %s removed %d, want %d", space, loArg, hiArg, got, want)
				}
				assertMatchesModel(t, z, surv, "bylex "+loArg+" "+hiArg)
			}
		}
	}
}

// seedScored builds a zset and a matching model over a small score space so bands
// are tied; space over the cap forces the native band.
func seedScored(space int) (*zset, map[string]float64) {
	z := newZset()
	model := map[string]float64{}
	rng := rand.New(rand.NewPCG(31, uint64(space)))
	for i := 0; i < space; i++ {
		m := "m" + pad(i)
		s := float64(rng.IntN(8) - 4)
		z.update([]byte(m), s, flags{})
		model[m] = s
	}
	return z, model
}

// seedLex builds a zset all at score 0 (the defined ZREMRANGEBYLEX shape).
func seedLex(space int) (*zset, map[string]float64) {
	z := newZset()
	model := map[string]float64{}
	for i := 0; i < space; i++ {
		m := "k" + pad(i)
		z.update([]byte(m), 0, flags{})
		model[m] = 0
	}
	return z, model
}

// TestRemRangeNativeLarge removes a 10k window from the middle of a 100k native
// zset and checks the removed count and the surviving boundary ranks, the shape
// the ns/element bench prices. The full-order model check would dominate the run,
// so this asserts the count and the seams; the oracle test carries the exhaustive
// order comparison.
func TestRemRangeNativeLarge(t *testing.T) {
	const n, winLo, winHi = 100_000, 45_000, 55_000
	z := buildNative(n)
	before := z.card()
	// The member at the rank just below the window and just at the window's new
	// position bracket the splice.
	belowM, _ := z.nat.at(winLo - 1)
	below := string(belowM)
	afterM, _ := z.nat.at(winHi)
	after := string(afterM)

	got := z.removeRange(winLo, winHi)
	if got != winHi-winLo {
		t.Fatalf("removed %d, want %d", got, winHi-winLo)
	}
	if z.card() != before-(winHi-winLo) {
		t.Fatalf("card %d, want %d", z.card(), before-(winHi-winLo))
	}
	// The entry that was just below the window keeps its rank; the entry that was
	// at the window's high edge now sits right after it.
	if r, _, ok := z.rank([]byte(below)); !ok || r != winLo-1 {
		t.Fatalf("rank(%q) = %d,%v, want %d", below, r, ok, winLo-1)
	}
	if r, _, ok := z.rank([]byte(after)); !ok || r != winLo {
		t.Fatalf("rank(%q) = %d,%v, want %d", after, r, ok, winLo)
	}
}

// TestRemRangeAgainstRedis replays a churned zset against a live Redis and checks
// ZREMRANGEBYRANK, ZREMRANGEBYSCORE, and ZREMRANGEBYLEX agree on the removed count
// and the resulting members, across both bands. Skipped unless AKI_REDIS_ADDR is
// set.
func TestRemRangeAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay ZREMRANGEBY* against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	seed := func(key string, space int, lex bool) (*zset, map[string]float64) {
		c.cmd("DEL", key)
		z := newZset()
		model := map[string]float64{}
		rng := rand.New(rand.NewPCG(97, uint64(space)))
		for i := 0; i < space; i++ {
			m := "m" + pad(i)
			s := 0.0
			if !lex {
				s = float64(rng.IntN(8) - 4)
			}
			z.update([]byte(m), s, flags{})
			model[m] = s
			c.cmd("ZADD", key, string(resp.FormatScore(nil, s)), m)
		}
		return z, model
	}

	for _, space := range []int{20, 400} {
		// BYRANK.
		key := "aki:zremrank:" + itoa(space)
		z, _ := seed(key, space, false)
		lo, hi, _ := clampRange(2, space/2, z.card())
		rn, err := c.cmd("ZREMRANGEBYRANK", key, "2", itoa(space/2))
		if err != nil {
			t.Fatalf("ZREMRANGEBYRANK: %v", err)
		}
		got := z.removeRange(lo, hi+1)
		if rn != itoa(got) {
			t.Fatalf("BYRANK removed: redis %q, zset %d", rn, got)
		}
		assertMembersAgree(t, c, key, mapOf(z))
		c.cmd("DEL", key)

		// BYSCORE.
		key = "aki:zremscore:" + itoa(space)
		z, _ = seed(key, space, false)
		rn, err = c.cmd("ZREMRANGEBYSCORE", key, "(-2", "3")
		if err != nil {
			t.Fatalf("ZREMRANGEBYSCORE: %v", err)
		}
		min, _ := parseScoreBound([]byte("(-2"))
		max, _ := parseScoreBound([]byte("3"))
		wlo, whi := z.scoreWindow(min, max)
		got = z.removeRange(wlo, whi)
		if rn != itoa(got) {
			t.Fatalf("BYSCORE removed: redis %q, zset %d", rn, got)
		}
		assertMembersAgree(t, c, key, mapOf(z))
		c.cmd("DEL", key)

		// BYLEX.
		key = "aki:zremlex:" + itoa(space)
		z, _ = seed(key, space, true)
		rn, err = c.cmd("ZREMRANGEBYLEX", key, "[m0000010", "(m0000050")
		if err != nil {
			t.Fatalf("ZREMRANGEBYLEX: %v", err)
		}
		min2, _ := parseLexBound([]byte("[m0000010"))
		max2, _ := parseLexBound([]byte("(m0000050"))
		wlo, whi = z.lexWindow(min2, max2)
		got = z.removeRange(wlo, whi)
		if rn != itoa(got) {
			t.Fatalf("BYLEX removed: redis %q, zset %d", rn, got)
		}
		assertMembersAgree(t, c, key, mapOf(z))
		c.cmd("DEL", key)
	}
}

// assertMembersAgree checks the live key's full ZRANGE matches the local model.
func assertMembersAgree(t *testing.T, c *redisConn, key string, model map[string]float64) {
	t.Helper()
	rElems, _, err := c.cmdArray("ZRANGE", key, "0", "-1", "WITHSCORES")
	if err != nil {
		t.Fatalf("ZRANGE: %v", err)
	}
	sm := sortedModel(model)
	var want []string
	for _, p := range sm {
		want = append(want, p.m, string(resp.FormatScore(nil, p.s)))
	}
	if !eqStrings(rElems, want) {
		t.Fatalf("members disagree:\n redis %v\n model %v", rElems, want)
	}
}
