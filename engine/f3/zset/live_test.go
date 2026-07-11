package zset

import (
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
