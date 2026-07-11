package zset

import (
	"math"
	"os"
	"strings"
	"testing"

	"github.com/tamnd/aki/f3srv/resp"
)

// The algebra live-replay tests. They load the same operands into a live Redis
// and into local zsets, run ZUNION/ZINTER/ZDIFF/ZINTERCARD (and their STORE
// forms via their read counterparts on the destination) on the server, run the
// matching kernel locally, and check the score-ordered results and the exact
// error texts agree byte for byte. Skipped unless AKI_REDIS_ADDR is set, so this
// is the confirmation lever the M2 gate uses, not a required unit gate.

// kernelFlat renders a kernel result as the flat wire elements ZUNION-family
// replies carry: members alone, or member then score under WITHSCORES.
func kernelFlat(pairs []scoredMember, ws bool) []string {
	var out []string
	for _, p := range pairs {
		out = append(out, string(p.member))
		if ws {
			out = append(out, string(resp.FormatScore(nil, p.score)))
		}
	}
	return out
}

// loadZ deletes key on the server and reloads it with the pairs, and returns the
// matching local zset. inf scores go over the wire as inf/-inf, the form Redis
// parses.
func loadZ(t *testing.T, c *redisConn, key string, pairs []msPair) *zset {
	t.Helper()
	c.cmd("DEL", key)
	z := newZset()
	for _, p := range pairs {
		z.update([]byte(p.m), p.s, flags{})
		if _, err := c.cmd("ZADD", key, string(resp.FormatScore(nil, p.s)), p.m); err != nil {
			t.Fatalf("ZADD %s: %v", key, err)
		}
	}
	return z
}

// TestAlgebraAgainstRedis replays the multi-key read forms against a live Redis
// across disjoint, overlapping, and fan-in shapes, both bands, every aggregate
// mode, a spread of weights (including the 0-weight-times-infinity quirk), and
// with and without WITHSCORES.
func TestAlgebraAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay zset algebra against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	inf := math.Inf(1)
	shapes := [][][]msPair{
		{{{"a", 1}, {"b", 2}}, {{"c", 3}, {"d", 4}}},
		{{{"a", 1}, {"b", 2}, {"c", 3}}, {{"a", 10}, {"b", 20}, {"c", 30}}},
		{{{"a", 1}, {"b", 2}, {"c", 3}}, {{"b", 5}, {"c", 6}, {"d", 7}}},
		{{{"a", 1}, {"x", 2}}, {{"a", 3}, {"y", 4}}, {{"a", 5}, {"z", 6}}},
		{{{"a", inf}, {"b", 2}}, {{"a", 4}, {"b", 5}}},
	}
	weightsets := [][]float64{nil, {2, 3, 4}, {0, 1, 1}, {-1, 2, 1}}
	modeArg := map[aggMode]string{aggSum: "SUM", aggMin: "MIN", aggMax: "MAX"}

	for si, srcs := range shapes {
		for _, native := range []bool{false, true} {
			keys := make([]string, len(srcs))
			ops := make([]operand, len(srcs))
			for i, s := range srcs {
				keys[i] = "aki:zalg:" + itoa(si) + ":" + itoa(i)
				if native {
					keys[i] += "n"
				}
				z := loadZ(t, c, keys[i], s)
				if native {
					// Push both sides past the entry cap so the kernel runs native.
					z = padNative(t, c, keys[i], z)
				}
				ops[i] = operand{z: z, weight: 1}
			}

			for _, ws := range weightsets {
				for _, mode := range []aggMode{aggSum, aggMin, aggMax} {
					for i := range ops {
						ops[i].weight = 1
						if ws != nil {
							ops[i].weight = ws[i]
						}
					}
					for _, withScores := range []bool{false, true} {
						// ZUNION and ZINTER carry WEIGHTS and AGGREGATE.
						base := []string{itoa(len(keys))}
						base = append(base, keys...)
						if ws != nil {
							base = append(base, "WEIGHTS")
							for _, w := range ws[:len(keys)] {
								base = append(base, string(resp.FormatScore(nil, w)))
							}
						}
						base = append(base, "AGGREGATE", modeArg[mode])
						tail := base
						if withScores {
							tail = append([]string{}, base...)
							tail = append(tail, "WITHSCORES")
						}
						checkAlgebra(t, c, "ZUNION", tail, kernelFlat(union(ops, mode), withScores))
						checkAlgebra(t, c, "ZINTER", tail, kernelFlat(intersect(ops, mode), withScores))
					}
				}
			}

			// ZDIFF takes neither weights nor aggregate; check plain and WITHSCORES.
			for _, withScores := range []bool{false, true} {
				dargs := append([]string{itoa(len(keys))}, keys...)
				if withScores {
					dargs = append(dargs, "WITHSCORES")
				}
				diffOps := make([]operand, len(ops))
				copy(diffOps, ops)
				checkAlgebra(t, c, "ZDIFF", dargs, kernelFlat(diff(diffOps), withScores))
			}

			// ZINTERCARD, unlimited and with a LIMIT smaller than the intersection.
			for _, limit := range []int{0, 1, 2} {
				cargs := append([]string{itoa(len(keys))}, keys...)
				if limit > 0 {
					cargs = append(cargs, "LIMIT", itoa(limit))
				}
				rv, err := c.cmd(append([]string{"ZINTERCARD"}, cargs...)...)
				if err != nil {
					t.Fatalf("ZINTERCARD %v: %v", cargs, err)
				}
				if want := itoa(intercard(ops, limit)); rv != want {
					t.Fatalf("ZINTERCARD %v: redis %q, kernel %q", cargs, rv, want)
				}
			}

			for _, k := range keys {
				c.cmd("DEL", k)
			}
		}
	}
}

// checkAlgebra runs one read-form command against Redis and asserts the flat
// reply equals the kernel's rendered result.
func checkAlgebra(t *testing.T, c *redisConn, verb string, tail, want []string) {
	t.Helper()
	args := append([]string{verb}, tail...)
	got, _, err := c.cmdArray(args...)
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	if !eqStrings(got, want) {
		t.Fatalf("%v:\n redis  %v\n kernel %v", args, got, want)
	}
}

// padNative seeds both the server key and the local zset past the entry cap with
// far-away members so the kernel runs over the native band. The padding members
// are disjoint from every other operand, so they never join an intersection and
// only ever appear in a union or diff as themselves, which the differential still
// checks against Redis.
func padNative(t *testing.T, c *redisConn, key string, z *zset) *zset {
	t.Helper()
	for i := 0; i <= maxListpackEntries; i++ {
		m := "pad" + itoa(i) + ":" + key
		s := 1e6 + float64(i)
		z.update([]byte(m), s, flags{})
		c.cmd("ZADD", key, string(resp.FormatScore(nil, s)), m)
	}
	if z.enc != encSkiplist {
		t.Fatalf("padNative did not promote %s", key)
	}
	return z
}

// TestAlgebraStoreAgainstRedis replays the STORE forms against a live Redis: it
// runs ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE on the server, then reads the
// destination back with ZRANGE WITHSCORES and checks it equals the kernel's
// stored result, including the empty-result destination delete.
func TestAlgebraStoreAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay the STORE forms against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	a := loadZ(t, c, "aki:zst:a", []msPair{{"a", 1}, {"b", 2}, {"c", 3}})
	b := loadZ(t, c, "aki:zst:b", []msPair{{"b", 5}, {"c", 6}, {"d", 7}})
	dest := "aki:zst:dest"
	defer c.cmd("DEL", "aki:zst:a", "aki:zst:b", dest)

	cases := []struct {
		verb   string
		result []scoredMember
	}{
		{"ZUNIONSTORE", union([]operand{{a, 1}, {b, 1}}, aggSum)},
		{"ZINTERSTORE", intersect([]operand{{a, 1}, {b, 1}}, aggSum)},
		{"ZDIFFSTORE", diff([]operand{{a, 1}, {b, 1}})},
	}
	for _, tc := range cases {
		args := []string{tc.verb, dest, "2", "aki:zst:a", "aki:zst:b"}
		rn, err := c.cmd(args...)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if want := itoa(len(tc.result)); rn != want {
			t.Fatalf("%v: redis count %q, kernel %q", args, rn, want)
		}
		got, _, err := c.cmdArray("ZRANGE", dest, "0", "-1", "WITHSCORES")
		if err != nil {
			t.Fatalf("ZRANGE dest: %v", err)
		}
		if !eqStrings(got, kernelFlat(tc.result, true)) {
			t.Fatalf("%v stored:\n redis  %v\n kernel %v", args, got, kernelFlat(tc.result, true))
		}
	}

	// An empty intersection deletes the destination (ZDIFFSTORE of a key with
	// itself is empty here: a\a).
	c.cmd("SET", "aki:zst:probe", "x")
	// Seed the destination so we can see it get deleted.
	c.cmd("ZADD", dest, "1", "seed")
	empty := loadZ(t, c, "aki:zst:e1", nil)
	_ = empty
	rn, err := c.cmd("ZINTERSTORE", dest, "2", "aki:zst:a", "aki:zst:e1")
	if err != nil {
		t.Fatalf("ZINTERSTORE empty: %v", err)
	}
	if rn != "0" {
		t.Fatalf("ZINTERSTORE empty count = %q, want 0", rn)
	}
	if ex, _ := c.cmd("EXISTS", dest); ex != "0" {
		t.Fatalf("empty ZINTERSTORE left destination existing (EXISTS=%q)", ex)
	}
	c.cmd("DEL", "aki:zst:e1", "aki:zst:probe")
}

// TestAlgebraErrorsAgainstRedis pins the exact error texts the algebra surface
// rejects with against a live Redis. Redis clients match on these strings.
func TestAlgebraErrorsAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to check algebra error texts against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()
	c.cmd("DEL", "aki:zerr:a", "aki:zerr:b")
	c.cmd("ZADD", "aki:zerr:a", "1", "x", "2", "y")
	c.cmd("ZADD", "aki:zerr:b", "3", "y", "4", "z")
	defer c.cmd("DEL", "aki:zerr:a", "aki:zerr:b")

	cases := []struct {
		args []string
		want string
	}{
		{[]string{"ZUNION", "0", "aki:zerr:a"}, "ERR at least 1 input key is needed for 'zunion' command"},
		{[]string{"ZINTER", "0", "aki:zerr:a"}, "ERR at least 1 input key is needed for 'zinter' command"},
		{[]string{"ZDIFF", "0", "aki:zerr:a"}, "ERR at least 1 input key is needed for 'zdiff' command"},
		{[]string{"ZINTERCARD", "0", "aki:zerr:a"}, "ERR at least 1 input key is needed for 'zintercard' command"},
		{[]string{"ZUNIONSTORE", "aki:zerr:d", "0", "aki:zerr:a"}, "ERR at least 1 input key is needed for 'zunionstore' command"},
		{[]string{"ZUNION", "notanint", "aki:zerr:a"}, errNotInt},
		{[]string{"ZUNION", "3", "aki:zerr:a", "aki:zerr:b"}, "ERR syntax error"},
		{[]string{"ZUNION", "2", "aki:zerr:a", "aki:zerr:b", "WEIGHTS", "1"}, "ERR syntax error"},
		{[]string{"ZUNION", "2", "aki:zerr:a", "aki:zerr:b", "WEIGHTS", "x", "y"}, "ERR weight value is not a float"},
		{[]string{"ZUNION", "2", "aki:zerr:a", "aki:zerr:b", "AGGREGATE", "FOO"}, "ERR syntax error"},
		{[]string{"ZDIFF", "2", "aki:zerr:a", "aki:zerr:b", "WEIGHTS", "1", "1"}, "ERR syntax error"},
		{[]string{"ZINTERCARD", "2", "aki:zerr:a", "aki:zerr:b", "LIMIT", "-1"}, "ERR LIMIT can't be negative"},
		{[]string{"ZINTERCARD", "1", "aki:zerr:a", "LIMIT", "x"}, "ERR LIMIT can't be negative"},
	}
	for _, tc := range cases {
		_, rErr := c.cmd(tc.args...)
		if rErr == nil {
			t.Fatalf("%v: redis accepted, want error %q", tc.args, tc.want)
		}
		got := strings.TrimPrefix(rErr.Error(), "redis: ")
		if got != tc.want {
			t.Fatalf("%v: redis %q, this package emits %q", tc.args, got, tc.want)
		}
	}

	// WRONGTYPE: a string operand is rejected before any write.
	c.cmd("SET", "aki:zerr:str", "v")
	defer c.cmd("DEL", "aki:zerr:str")
	if _, rErr := c.cmd("ZUNION", "2", "aki:zerr:a", "aki:zerr:str"); rErr == nil ||
		!strings.HasPrefix(strings.TrimPrefix(rErr.Error(), "redis: "), "WRONGTYPE") {
		t.Fatalf("ZUNION over a string operand: got %v, want WRONGTYPE", rErr)
	}
}
