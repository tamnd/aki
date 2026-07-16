package zset

import (
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/tamnd/aki/obs1srv/resp"
)

// TestPointOpsAgainstRedis replays random point-op churn against a live Redis
// and checks that ZSCORE and ZRANK agree byte for byte, across the inline band
// and the conversion into the native band. Skipped unless AKI_REDIS_ADDR points
// at a server, so it is the confirmation lever, not a required gate.
func TestPointOpsAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay point ops against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	for _, space := range []int{16, 200} {
		key := "aki:zpts:" + itoa(space)
		c.cmd("DEL", key)
		z := newZset()
		rng := rand.New(rand.NewPCG(7, uint64(space)))

		for step := 0; step < 600; step++ {
			m := "m" + itoa(rng.IntN(space))
			s := float64(rng.IntN(20) - 10)
			ss := string(resp.FormatScore(nil, s))
			z.update([]byte(m), s, flags{})
			if _, err := c.cmd("ZADD", key, ss, m); err != nil {
				t.Fatalf("ZADD: %v", err)
			}
		}

		// Encoding parity after the churn.
		enc, err := c.cmd("OBJECT", "ENCODING", key)
		if err != nil {
			t.Fatalf("OBJECT ENCODING: %v", err)
		}
		if enc != z.enc.String() {
			t.Fatalf("space %d: Redis encoding %q, zset %q", space, enc, z.enc.String())
		}

		// ZSCORE and ZRANK agree for a sample of members.
		for i := 0; i < space; i++ {
			m := "m" + itoa(i)
			rScore, _ := c.cmd("ZSCORE", key, m)
			zs, ok := z.score([]byte(m))
			zScore := ""
			if ok {
				zScore = string(resp.FormatScore(nil, zs))
			}
			if rScore != zScore {
				t.Fatalf("ZSCORE %q: Redis %q, zset %q", m, rScore, zScore)
			}
			rRank, _ := c.cmd("ZRANK", key, m)
			zr, _, zok := z.rank([]byte(m))
			zRank := ""
			if zok {
				zRank = itoa(zr)
			}
			if rRank != zRank {
				t.Fatalf("ZRANK %q: Redis %q, zset %q", m, rRank, zRank)
			}
		}
		c.cmd("DEL", key)
	}
}

// TestRankRangeAgainstRedis replays random churn against a live Redis and checks
// the rank-and-range surface agrees byte for byte: ZRANK and ZREVRANK with and
// without WITHSCORE (including the null-array absent form), ZRANGE by index in
// both directions with and without WITHSCORES, and the ZREVRANGE alias. Two
// member spaces keep one run inline and push the other into the native tree, so
// the streamed seek-and-walk is confirmed against the reference implementation.
func TestRankRangeAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay rank and range against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	for _, space := range []int{20, 400} {
		key := "aki:zrange:" + itoa(space)
		c.cmd("DEL", key)
		z := newZset()
		rng := rand.New(rand.NewPCG(13, uint64(space)))
		for step := 0; step < 1200; step++ {
			m := "m" + itoa(rng.IntN(space))
			s := float64(rng.IntN(12) - 6) // small score space forces tied bands
			z.update([]byte(m), s, flags{})
			if _, err := c.cmd("ZADD", key, string(resp.FormatScore(nil, s)), m); err != nil {
				t.Fatalf("ZADD: %v", err)
			}
		}

		card := z.card()

		// ZRANK / ZREVRANK, plain and WITHSCORE, over present and absent members.
		for i := -2; i < space; i++ {
			m := "m" + strconv.Itoa(i)
			for _, rev := range []bool{false, true} {
				verb := "ZRANK"
				if rev {
					verb = "ZREVRANK"
				}
				rElems, rNotNull, err := c.cmdArray(verb, key, m, "WITHSCORE")
				if err != nil {
					t.Fatalf("%s WITHSCORE %q: %v", verb, m, err)
				}
				zr, zsc, zok := z.rank([]byte(m))
				if rev && zok {
					zr = card - 1 - zr
				}
				if rNotNull != zok {
					t.Fatalf("%s WITHSCORE %q: redis notNull=%v, zset present=%v", verb, m, rNotNull, zok)
				}
				if zok {
					want := []string{itoa(zr), string(resp.FormatScore(nil, zsc))}
					if len(rElems) != 2 || rElems[0] != want[0] || rElems[1] != want[1] {
						t.Fatalf("%s WITHSCORE %q: redis %v, zset %v", verb, m, rElems, want)
					}
				}
			}
		}

		// ZRANGE / ZREVRANGE index windows, both directions, with and without
		// scores, over a spread of bounds including negatives and overflow.
		bounds := [][2]int{{0, -1}, {0, 0}, {-3, -1}, {2, card + 5}, {-card - 5, card + 5}, {5, 2}}
		for _, w := range bounds {
			for _, rev := range []bool{false, true} {
				for _, ws := range []bool{false, true} {
					args := []string{"ZRANGE", key, strconv.Itoa(w[0]), strconv.Itoa(w[1])}
					if rev {
						args = append(args, "REV")
					}
					if ws {
						args = append(args, "WITHSCORES")
					}
					rElems, _, err := c.cmdArray(args...)
					if err != nil {
						t.Fatalf("%v: %v", args, err)
					}
					want := localRange(t, z, w[0], w[1], rev, ws)
					if !eqStrings(rElems, want) {
						t.Fatalf("%v:\n redis %v\n zset  %v", args, rElems, want)
					}
					// The ZREVRANGE alias must equal ZRANGE ... REV.
					if rev {
						aliasArgs := []string{"ZREVRANGE", key, strconv.Itoa(w[0]), strconv.Itoa(w[1])}
						if ws {
							aliasArgs = append(aliasArgs, "WITHSCORES")
						}
						aElems, _, err := c.cmdArray(aliasArgs...)
						if err != nil {
							t.Fatalf("%v: %v", aliasArgs, err)
						}
						if !eqStrings(aElems, want) {
							t.Fatalf("%v:\n redis %v\n zset  %v", aliasArgs, aElems, want)
						}
					}
				}
			}
		}
		c.cmd("DEL", key)
	}
}

// localRange builds the element strings z would stream for the window, the
// reference the live differential compares Redis against.
func localRange(t *testing.T, z *zset, start, stop int, rev, ws bool) []string {
	t.Helper()
	lo, hi, empty := clampRange(start, stop, z.card())
	if empty {
		return nil
	}
	return decodeBulks(t, z.rangeByIndex(nil, lo, hi, rev, ws))
}

// TestFlagMatrixAgainstRedis replays randomized ZADD flag combinations (NX, XX,
// GT, LT, CH, INCR in every legal pairing) against a live Redis and checks the
// reply and the resulting score agree byte for byte. Two member spaces keep one
// run inline and push the other through the conversion.
func TestFlagMatrixAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay the flag matrix against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	for _, space := range []int{20, 200} {
		key := "aki:zflags:" + itoa(space)
		c.cmd("DEL", key)
		z := newZset()
		rng := rand.New(rand.NewPCG(11, uint64(space)))

		for step := 0; step < 1500; step++ {
			m := "m" + itoa(rng.IntN(space))
			s := float64(rng.IntN(40)-20) / 2
			var fl flags
			// One of the existence gates, or neither.
			switch rng.IntN(3) {
			case 0:
				fl.nx = true
			case 1:
				fl.xx = true
			}
			// GT and LT are mutually exclusive and both illegal with NX.
			if !fl.nx {
				switch rng.IntN(3) {
				case 0:
					fl.gt = true
				case 1:
					fl.lt = true
				}
			}
			fl.ch = rng.IntN(2) == 0
			fl.incr = rng.IntN(3) == 0

			args := []string{"ZADD", key}
			if fl.nx {
				args = append(args, "NX")
			}
			if fl.xx {
				args = append(args, "XX")
			}
			if fl.gt {
				args = append(args, "GT")
			}
			if fl.lt {
				args = append(args, "LT")
			}
			if fl.ch {
				args = append(args, "CH")
			}
			if fl.incr {
				args = append(args, "INCR")
			}
			args = append(args, string(resp.FormatScore(nil, s)), m)

			got, err := c.cmd(args...)
			if err != nil {
				t.Fatalf("step %d: %v: %v", step, args, err)
			}
			added, changed, ns, applied, nan := z.update([]byte(m), s, fl)
			if nan {
				t.Fatalf("step %d: unexpected NaN", step)
			}
			var want string
			if fl.incr {
				if applied {
					want = string(resp.FormatScore(nil, ns))
				} // else nil bulk, which cmd reads as ""
			} else {
				count := 0
				if added {
					count++
				}
				if fl.ch && changed {
					count++
				}
				want = itoa(count)
			}
			if got != want {
				t.Fatalf("step %d: %v: Redis %q, zset %q", step, args, got, want)
			}

			rScore, _ := c.cmd("ZSCORE", key, m)
			zScore := ""
			if zs, ok := z.score([]byte(m)); ok {
				zScore = string(resp.FormatScore(nil, zs))
			}
			if rScore != zScore {
				t.Fatalf("step %d after %v: ZSCORE Redis %q, zset %q", step, args, rScore, zScore)
			}
		}

		enc, _ := c.cmd("OBJECT", "ENCODING", key)
		if enc != z.enc.String() {
			t.Fatalf("space %d: Redis encoding %q, zset %q", space, enc, z.enc.String())
		}
		c.cmd("DEL", key)
	}
}

// TestRangeByBoundAgainstRedis replays random churn against a live Redis and
// checks the whole by-bound range surface agrees byte for byte: ZRANGEBYSCORE
// and ZREVRANGEBYSCORE over every combination of inclusive/exclusive/infinite
// bounds, with and without WITHSCORES and LIMIT; ZCOUNT; the ZRANGE BYSCORE
// form with REV; and the same for the lex family over a single tied band, which
// is the only shape ZRANGEBYLEX is defined for. Two member spaces keep one run
// inline and push the other into the native tree.
func TestRangeByBoundAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay by-bound ranges against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	// Score bands over a churned zset with a small score space (tied bands).
	for _, space := range []int{20, 400} {
		key := "aki:zbyscore:" + itoa(space)
		c.cmd("DEL", key)
		z := newZset()
		rng := rand.New(rand.NewPCG(31, uint64(space)))
		for step := 0; step < 1500; step++ {
			m := "m" + itoa(rng.IntN(space))
			s := float64(rng.IntN(12) - 6)
			z.update([]byte(m), s, flags{})
			if _, err := c.cmd("ZADD", key, string(resp.FormatScore(nil, s)), m); err != nil {
				t.Fatalf("ZADD: %v", err)
			}
		}

		// "(" and "" are the strtod empty-remainder bounds: exclusive zero and
		// inclusive zero, accepted by Redis, so they belong in the matrix.
		scoreArgs := []string{"-inf", "+inf", "-3", "(-3", "0", "(2", "5", "(5", "(", ""}
		for _, lo := range scoreArgs {
			for _, hi := range scoreArgs {
				for _, ws := range []bool{false, true} {
					for _, lim := range [][2]int{{-1, -1}, {0, 3}, {1, 2}, {2, -1}} {
						checkRangeCmd(t, c, z, "ZRANGEBYSCORE", key, lo, hi, false, ws, lim)
						checkRangeCmd(t, c, z, "ZREVRANGEBYSCORE", key, hi, lo, true, ws, lim)
					}
				}
				// ZCOUNT agreement.
				rc, err := c.cmd("ZCOUNT", key, lo, hi)
				if err != nil {
					t.Fatalf("ZCOUNT %s %s: %v", lo, hi, err)
				}
				mn, _ := parseScoreBound([]byte(lo))
				mx, _ := parseScoreBound([]byte(hi))
				wlo, whi := z.scoreWindow(mn, mx)
				if rc != itoa(whi-wlo) {
					t.Fatalf("ZCOUNT %s %s: redis %q, zset %d", lo, hi, rc, whi-wlo)
				}
			}
		}
		c.cmd("DEL", key)
	}

	// Lex bands over a single tied score, the defined ZRANGEBYLEX shape.
	for _, space := range []int{20, 400} {
		key := "aki:zbylex:" + itoa(space)
		c.cmd("DEL", key)
		z := newZset()
		rng := rand.New(rand.NewPCG(33, uint64(space)))
		for step := 0; step < 1500; step++ {
			m := "k" + itoa(rng.IntN(space))
			z.update([]byte(m), 0, flags{})
			if _, err := c.cmd("ZADD", key, "0", m); err != nil {
				t.Fatalf("ZADD: %v", err)
			}
		}
		lexArgs := []string{"-", "+", "[k1", "(k1", "[k2", "(k2", "[k", "(k9"}
		for _, lo := range lexArgs {
			for _, hi := range lexArgs {
				for _, lim := range [][2]int{{-1, -1}, {0, 3}, {1, 2}} {
					checkLexCmd(t, c, z, "ZRANGEBYLEX", key, lo, hi, false, lim)
					checkLexCmd(t, c, z, "ZREVRANGEBYLEX", key, hi, lo, true, lim)
				}
				rc, err := c.cmd("ZLEXCOUNT", key, lo, hi)
				if err != nil {
					t.Fatalf("ZLEXCOUNT %s %s: %v", lo, hi, err)
				}
				mn, _ := parseLexBound([]byte(lo))
				mx, _ := parseLexBound([]byte(hi))
				wlo, whi := z.lexWindow(mn, mx)
				if rc != itoa(whi-wlo) {
					t.Fatalf("ZLEXCOUNT %s %s: redis %q, zset %d", lo, hi, rc, whi-wlo)
				}
			}
		}
		c.cmd("DEL", key)
	}
}

// checkRangeCmd runs one score-range command against Redis and against the local
// model and asserts the elements match. loArg and hiArg are the bounds in the
// command's argument order (max first for the reverse form).
func checkRangeCmd(t *testing.T, c *redisConn, z *zset, verb, key, loArg, hiArg string, rev, ws bool, lim [2]int) {
	t.Helper()
	args := []string{verb, key, loArg, hiArg}
	if ws {
		args = append(args, "WITHSCORES")
	}
	limit := lim[0] >= 0
	if limit {
		args = append(args, "LIMIT", itoa(lim[0]), itoa(lim[1]))
	}
	rElems, _, err := c.cmdArray(args...)
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	min, _ := parseScoreBound([]byte(minOf(loArg, hiArg, rev)))
	max, _ := parseScoreBound([]byte(maxOf(loArg, hiArg, rev)))
	want := modelByScore(sortedModel(mapOf(z)), min, max, rev, ws, limit, lim[0], lim[1])
	if !eqStrings(rElems, want) {
		t.Fatalf("%v:\n redis %v\n model %v", args, rElems, want)
	}
}

func checkLexCmd(t *testing.T, c *redisConn, z *zset, verb, key, loArg, hiArg string, rev bool, lim [2]int) {
	t.Helper()
	args := []string{verb, key, loArg, hiArg}
	limit := lim[0] >= 0
	if limit {
		args = append(args, "LIMIT", itoa(lim[0]), itoa(lim[1]))
	}
	rElems, _, err := c.cmdArray(args...)
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	minArg, maxArg := loArg, hiArg
	if rev {
		minArg, maxArg = hiArg, loArg
	}
	min, _ := parseLexBound([]byte(minArg))
	max, _ := parseLexBound([]byte(maxArg))
	want := modelByLex(sortedModel(mapOf(z)), min, max, rev, limit, lim[0], lim[1])
	if !eqStrings(rElems, want) {
		t.Fatalf("%v:\n redis %v\n model %v", args, rElems, want)
	}
}

// minOf and maxOf pick the low and high score bound out of the command's
// argument order: forward is (min, max), reverse is (max, min).
func minOf(a, b string, rev bool) string {
	if rev {
		return b
	}
	return a
}
func maxOf(a, b string, rev bool) string {
	if rev {
		return a
	}
	return b
}

// mapOf drains a zset into a model map for the reference range.
func mapOf(z *zset) map[string]float64 {
	m := map[string]float64{}
	for _, e := range z.entries() {
		m[string(e.member)] = e.score
	}
	return m
}

// TestRangeErrorsAgainstRedis pins the exact error strings the by-bound ranges
// reject with against a live Redis: bad score and lex bounds, and the illegal
// option combinations. Redis clients match on these strings verbatim.
func TestRangeErrorsAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to check range error texts against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()
	key := "aki:zerr"
	c.cmd("DEL", key)
	c.cmd("ZADD", key, "1", "a", "2", "b", "3", "c")
	defer c.cmd("DEL", key)

	cases := []struct {
		args []string
		want string // the exact error text this package emits, checked against Redis
	}{
		// A bare "(" is not an error: strtod reads the empty remainder as 0, so
		// it parses as exclusive zero and lives in the bound matrix above.
		{[]string{"ZRANGEBYSCORE", key, "notafloat", "2"}, errScoreBound},
		{[]string{"ZRANGEBYSCORE", key, "(x", "2"}, errScoreBound},
		{[]string{"ZRANGEBYSCORE", key, "1", "nan"}, errScoreBound},
		{[]string{"ZCOUNT", key, "x", "2"}, errScoreBound},
		{[]string{"ZRANGEBYLEX", key, "a", "[b"}, errLexBound},
		{[]string{"ZRANGEBYLEX", key, "[a", "b"}, errLexBound},
		{[]string{"ZRANGEBYLEX", key, "[a", "[b", "WITHSCORES"}, errLexScores},
		{[]string{"ZLEXCOUNT", key, "+x", "-"}, errLexBound},
		{[]string{"ZRANGE", key, "0", "-1", "LIMIT", "0", "5"}, errLimitOnly},
		{[]string{"ZRANGE", key, "(1", "3", "BYSCORE", "BYLEX"}, "ERR syntax error"},
		{[]string{"ZRANGE", key, "[a", "[b", "BYLEX", "WITHSCORES"}, errLexScores},
		{[]string{"ZRANGEBYSCORE", key, "1", "2", "LIMIT", "0"}, "ERR syntax error"},
	}
	for _, tc := range cases {
		_, rErr := c.cmd(tc.args...)
		if rErr == nil {
			t.Fatalf("%v: redis accepted, want error %q", tc.args, tc.want)
		}
		// c.cmd wraps the reply as "redis: <body>"; the body is Redis's exact
		// error line, which must equal the string this package emits.
		got := strings.TrimPrefix(rErr.Error(), "redis: ")
		if got != tc.want {
			t.Fatalf("%v: redis %q, zset emits %q", tc.args, got, tc.want)
		}
	}
}

// TestPopsAgainstRedis replays random churn against a live Redis and drains it
// through ZPOPMIN and ZPOPMAX, checking the flat [m, s, ...] reply agrees byte
// for byte with the local zset draining in lockstep, across both bands. It also
// pins the two edge forms: an absent key answers the empty array, and the
// no-count form answers a two-element array. The ZMPOP nested reply is checked
// separately over the multi-key selection.
func TestPopsAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay pops against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	// Absent key: the empty array in both the plain and count forms.
	for _, args := range [][]string{{"ZPOPMIN", "aki:zpop:absent"}, {"ZPOPMAX", "aki:zpop:absent", "4"}} {
		el, notNull, err := c.cmdArray(args...)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if !notNull || len(el) != 0 {
			t.Fatalf("%v on absent key: got %v notNull=%v, want empty array", args, el, notNull)
		}
	}

	for _, space := range []int{20, 400} {
		key := "aki:zpop:" + itoa(space)
		c.cmd("DEL", key)
		z := newZset()
		rng := rand.New(rand.NewPCG(41, uint64(space)))
		for step := 0; step < 1500; step++ {
			m := "m" + itoa(rng.IntN(space))
			s := float64(rng.IntN(12) - 6) // small score space forces ties
			z.update([]byte(m), s, flags{})
			if _, err := c.cmd("ZADD", key, string(resp.FormatScore(nil, s)), m); err != nil {
				t.Fatalf("ZADD: %v", err)
			}
		}

		// No-count form: a single [m, s] pair off each end.
		for _, verb := range []string{"ZPOPMIN", "ZPOPMAX"} {
			rElems, _, err := c.cmdArray(verb, key)
			if err != nil {
				t.Fatalf("%s: %v", verb, err)
			}
			want := popStrings(z, verb == "ZPOPMIN", 1)
			if !eqStrings(rElems, want) {
				t.Fatalf("%s no-count:\n redis %v\n zset  %v", verb, rElems, want)
			}
		}

		// Drain both sides in lockstep with random ends and counts.
		for z.card() > 0 {
			min := rng.IntN(2) == 0
			count := 1 + rng.IntN(31)
			verb := "ZPOPMIN"
			if !min {
				verb = "ZPOPMAX"
			}
			rElems, _, err := c.cmdArray(verb, key, itoa(count))
			if err != nil {
				t.Fatalf("%s %d: %v", verb, count, err)
			}
			want := popStrings(z, min, count)
			if !eqStrings(rElems, want) {
				t.Fatalf("%s %d:\n redis %v\n zset  %v", verb, count, rElems, want)
			}
		}
		// Drained on both sides: the empty array.
		rElems, _, err := c.cmdArray("ZPOPMIN", key)
		if err != nil {
			t.Fatalf("ZPOPMIN drained: %v", err)
		}
		if len(rElems) != 0 {
			t.Fatalf("ZPOPMIN on drained key: %v, want empty", rElems)
		}
		c.cmd("DEL", key)
	}
}

// popStrings pops count members off z at the named end and renders the flat
// [member, score, ...] element strings the wire reply carries, the reference the
// live drain compares Redis against.
func popStrings(z *zset, min bool, count int) []string {
	var out []string
	z.pop(min, count, func(m []byte, s float64) {
		out = append(out, string(m), string(resp.FormatScore(nil, s)))
	})
	return out
}

// TestZmpopAgainstRedis pins the ZMPOP nested reply against a live Redis: it
// skips the empty and absent keys and pops from the first key with members,
// answering [keyname, [[member, score], ...]], and answers a null array when
// every listed key is empty.
func TestZmpopAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay ZMPOP against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	empty := "aki:zmpop:empty"
	full := "aki:zmpop:full"
	absent := "aki:zmpop:absent"
	c.cmd("DEL", empty, full, absent)

	// Every key empty or absent: a null array.
	rep, err := c.cmdReply("ZMPOP", "2", empty, absent, "MIN")
	if err != nil {
		t.Fatalf("ZMPOP all-empty: %v", err)
	}
	if rep != nil {
		t.Fatalf("ZMPOP over empty keys = %v, want null array", rep)
	}

	z := newZset()
	rng := rand.New(rand.NewPCG(43, 7))
	for step := 0; step < 800; step++ {
		m := "m" + itoa(rng.IntN(300))
		s := float64(rng.IntN(20) - 10)
		z.update([]byte(m), s, flags{})
		if _, err := c.cmd("ZADD", full, string(resp.FormatScore(nil, s)), m); err != nil {
			t.Fatalf("ZADD: %v", err)
		}
	}

	for z.card() > 0 {
		min := rng.IntN(2) == 0
		count := 1 + rng.IntN(9)
		end := "MIN"
		if !min {
			end = "MAX"
		}
		// The empty and absent keys are skipped; full is the first with members.
		rep, err := c.cmdReply("ZMPOP", "3", empty, absent, full, end, "COUNT", itoa(count))
		if err != nil {
			t.Fatalf("ZMPOP: %v", err)
		}
		want := zmpopModel(z, full, min, count)
		if !zmpopEqual(rep, want) {
			t.Fatalf("ZMPOP %s %d:\n redis %v\n want  %v", end, count, rep, want)
		}
	}
	c.cmd("DEL", empty, full, absent)
}

// zmpopModel builds the [keyname, [[m, s], ...]] shape from popping z, the
// reference the live ZMPOP compares against.
func zmpopModel(z *zset, key string, min bool, count int) []any {
	var pairs []any
	z.pop(min, count, func(m []byte, s float64) {
		pairs = append(pairs, []any{string(m), string(resp.FormatScore(nil, s))})
	})
	return []any{key, pairs}
}

// zmpopEqual compares a recursive Redis reply against the modeled ZMPOP reply.
func zmpopEqual(got any, want []any) bool {
	ga, ok := got.([]any)
	if !ok || len(ga) != 2 {
		return false
	}
	if ga[0] != want[0] {
		return false
	}
	gp, ok := ga[1].([]any)
	wp := want[1].([]any)
	if !ok || len(gp) != len(wp) {
		return false
	}
	for i := range gp {
		gpair, ok := gp[i].([]any)
		wpair := wp[i].([]any)
		if !ok || len(gpair) != 2 || gpair[0] != wpair[0] || gpair[1] != wpair[1] {
			return false
		}
	}
	return true
}

// TestRandmemberAgainstRedis checks ZRANDMEMBER against a live Redis on the
// properties its reply must hold, since the member order is unspecified: the
// no-count form answers one member of the set (nil when absent); a positive
// count answers that many distinct members, capped at the cardinality; a
// negative count answers exactly that many with repetition allowed; and every
// returned member carries its true score under WITHSCORES. Both bands are run.
func TestRandmemberAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay ZRANDMEMBER against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	// Absent key: nil for the no-count form, empty array for the count form.
	if v, err := c.cmd("ZRANDMEMBER", "aki:zrand:absent"); err != nil || v != "" {
		t.Fatalf("ZRANDMEMBER absent no-count = %q,%v, want nil", v, err)
	}
	if el, _, err := c.cmdArray("ZRANDMEMBER", "aki:zrand:absent", "3"); err != nil || len(el) != 0 {
		t.Fatalf("ZRANDMEMBER absent count = %v,%v, want empty array", el, err)
	}

	for _, space := range []int{20, 400} {
		key := "aki:zrand:" + itoa(space)
		c.cmd("DEL", key)
		z := newZset()
		scores := map[string]float64{}
		rng := rand.New(rand.NewPCG(47, uint64(space)))
		for i := 0; i < space; i++ {
			m := "m" + itoa(i)
			s := float64(rng.IntN(40)-20) / 2
			z.update([]byte(m), s, flags{})
			scores[m] = s
			if _, err := c.cmd("ZADD", key, string(resp.FormatScore(nil, s)), m); err != nil {
				t.Fatalf("ZADD: %v", err)
			}
		}
		card := z.card()

		// No-count: one member of the set.
		v, err := c.cmd("ZRANDMEMBER", key)
		if err != nil {
			t.Fatalf("ZRANDMEMBER: %v", err)
		}
		if _, ok := scores[v]; !ok {
			t.Fatalf("ZRANDMEMBER returned %q, not a member", v)
		}

		for _, count := range []int{1, 5, card, card + 10, -1, -7, -(card + 5)} {
			for _, ws := range []bool{false, true} {
				args := []string{"ZRANDMEMBER", key, itoa(count)}
				if ws {
					args = append(args, "WITHSCORES")
				}
				el, _, err := c.cmdArray(args...)
				if err != nil {
					t.Fatalf("%v: %v", args, err)
				}
				members := checkRandScores(t, args, el, scores, ws)
				if count >= 0 {
					// Distinct, capped at the cardinality.
					exp := count
					if exp > card {
						exp = card
					}
					if len(members) != exp {
						t.Fatalf("%v: got %d members, want %d", args, len(members), exp)
					}
					if distinctCount(members) != len(members) {
						t.Fatalf("%v: positive count returned a repeat: %v", args, members)
					}
				} else if len(members) != -count {
					// With replacement: exactly -count members.
					t.Fatalf("%v: got %d members, want %d", args, len(members), -count)
				}
			}
		}
		c.cmd("DEL", key)
	}
}

// checkRandScores validates a ZRANDMEMBER array: every element is a member of
// the set, and under WITHSCORES each member is followed by its true score. It
// returns the member list for the caller's count and distinctness checks.
func checkRandScores(t *testing.T, args, el []string, scores map[string]float64, ws bool) []string {
	t.Helper()
	var members []string
	step := 1
	if ws {
		step = 2
	}
	for i := 0; i < len(el); i += step {
		m := el[i]
		s, ok := scores[m]
		if !ok {
			t.Fatalf("%v: %q is not a member", args, m)
		}
		if ws {
			want := string(resp.FormatScore(nil, s))
			if el[i+1] != want {
				t.Fatalf("%v: score for %q = %q, want %q", args, m, el[i+1], want)
			}
		}
		members = append(members, m)
	}
	return members
}

func distinctCount(xs []string) int {
	seen := map[string]bool{}
	for _, x := range xs {
		seen[x] = true
	}
	return len(seen)
}

// TestScoreFormattingAgainstRedis pins the zero and infinity reply forms
// against a live Redis, with both sides parsing the same wire bytes. The
// interesting split: a -0 score collapses to "0" in the listpack band (Redis
// integer-encodes it and loses the sign) but answers "-0" in the native band
// (the skiplist keeps the double), and both bands must agree with the server
// byte for byte. The infinities print inf and -inf everywhere.
func TestScoreFormattingAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to check score formatting against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	for _, native := range []bool{false, true} {
		key := "aki:zfmt:" + map[bool]string{false: "inline", true: "native"}[native]
		c.cmd("DEL", key)
		z := newZset()
		if native {
			// Seed both sides past the caps so the checks run on the native band.
			for i := 0; i <= maxListpackEntries; i++ {
				m := "seed" + itoa(i)
				z.update([]byte(m), 1e9+float64(i), flags{})
				c.cmd("ZADD", key, string(resp.FormatScore(nil, 1e9+float64(i))), m)
			}
		}
		cases := []struct {
			m  string
			ss string
		}{
			{"negzero", "-0"},
			{"negzerodot", "-0.0"},
			{"poszero", "0"},
			{"posinf", "inf"},
			{"neginf", "-inf"},
		}
		for _, cs := range cases {
			if _, err := c.cmd("ZADD", key, cs.ss, cs.m); err != nil {
				t.Fatalf("ZADD %s %s: %v", cs.ss, cs.m, err)
			}
			s, ok := parseScore([]byte(cs.ss))
			if !ok {
				t.Fatalf("parseScore(%q) rejected a score Redis accepted", cs.ss)
			}
			z.update([]byte(cs.m), s, flags{})
			rScore, _ := c.cmd("ZSCORE", key, cs.m)
			zs, ok := z.score([]byte(cs.m))
			if !ok {
				t.Fatalf("%s absent locally", cs.m)
			}
			if got := string(resp.FormatScore(nil, zs)); got != rScore {
				t.Fatalf("ZSCORE %s: Redis %q, zset %q", cs.m, rScore, got)
			}
		}
		// The zeros tie on score and order by member bytes; the ranks must agree.
		for _, m := range []string{"negzero", "negzerodot", "poszero"} {
			rRank, _ := c.cmd("ZRANK", key, m)
			zr, _, _ := z.rank([]byte(m))
			if rRank != itoa(zr) {
				t.Fatalf("ZRANK %s: Redis %q, zset %d", m, rRank, zr)
			}
		}
		// INCR to NaN errors on both sides and changes nothing.
		if _, err := c.cmd("ZINCRBY", key, "-inf", "posinf"); err == nil {
			t.Fatal("Redis accepted an inf + -inf increment")
		}
		if _, _, _, _, nan := z.update([]byte("posinf"), math.Inf(-1), flags{incr: true}); !nan {
			t.Fatal("zset accepted an inf + -inf increment")
		}
		rScore, _ := c.cmd("ZSCORE", key, "posinf")
		zs, _ := z.score([]byte("posinf"))
		if got := string(resp.FormatScore(nil, zs)); got != rScore {
			t.Fatalf("after NaN rejection: Redis %q, zset %q", rScore, got)
		}
		c.cmd("DEL", key)
	}
}
