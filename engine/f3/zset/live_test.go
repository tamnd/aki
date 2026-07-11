package zset

import (
	"math"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/tamnd/aki/f3srv/resp"
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
