package command

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"net"
	"strings"
	"testing"
	"time"
)

// TestLTMZRankVsZScoreRatio isolates the algorithmic cost of coll-form ZRANK from
// the cap-faulting effect the larger-than-memory matrix measures. It builds one
// coll-form zset in RAM, then times the same count of random ZRANK and ZSCORE
// probes over the same loopback socket. The socket cost is identical for both, so
// the ratio is the command-internal difference. If order-stat is engaged, ZRANK is
// two O(log n) descents against ZSCORE's one, so the ratio should sit near 2x. A
// ratio near 10x would mean ZRANK is taking the O(rank) count walk instead, which
// is what the LTM matrix slowdown would imply if the fast path were not wired.
//
// It is skipped in -short because it builds 100k members and runs 40k probes.
func TestLTMZRankVsZScoreRatio(t *testing.T) {
	if testing.Short() {
		t.Skip("LTM ratio probe builds 100k members; skipped in -short")
	}
	r, c := startData(t)
	const (
		n      = 100000
		probes = 20000
	)
	pad := strings.Repeat("x", 200)
	member := func(i int) string { return fmt.Sprintf("%012d", i) + pad }

	// Pipelined build: stream all ZADDs, then drain the replies, so the load is not
	// bounded by one round trip per member.
	pipelineBuild(t, c, r, n, member)

	if got := sendLine(t, r, c, "OBJECT ENCODING z"); got != "$8" {
		t.Fatalf("zset not coll form: header %q", got)
	}
	if got := sendLine(t, r, c, ""); got != "skiplist" {
		t.Fatalf("zset not coll form: %q", got)
	}

	rng := rand.NewPCG(1, 2)
	gen := rand.New(rng)
	idx := make([]int, probes)
	for i := range idx {
		idx[i] = gen.IntN(n)
	}

	zscoreNS := timeProbes(t, c, r, idx, member, "ZSCORE")
	zrankNS := timeProbes(t, c, r, idx, member, "ZRANK")

	zscoreRPS := float64(probes) / (float64(zscoreNS) / 1e9)
	zrankRPS := float64(probes) / (float64(zrankNS) / 1e9)
	ratio := zscoreRPS / zrankRPS
	t.Logf("in-RAM coll-form: ZSCORE=%.0f rps  ZRANK=%.0f rps  ZSCORE/ZRANK=%.2fx", zscoreRPS, zrankRPS, ratio)
	if ratio > 4 {
		t.Errorf("ZRANK is %.2fx slower than ZSCORE in RAM: the order-stat fast path is not paying off, "+
			"ZRANK is likely on the O(rank) count walk, not the O(log n) Rank descent", ratio)
	}
}

func pipelineBuild(t *testing.T, c net.Conn, r *bufio.Reader, n int, member func(int) string) {
	t.Helper()
	_ = c.SetWriteDeadline(time.Now().Add(60 * time.Second))
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "ZADD z %d %s\r\n", i, member(i))
		if b.Len() > 1<<20 {
			if _, err := c.Write([]byte(b.String())); err != nil {
				t.Fatal(err)
			}
			b.Reset()
		}
	}
	if b.Len() > 0 {
		if _, err := c.Write([]byte(b.String())); err != nil {
			t.Fatal(err)
		}
	}
	_ = c.SetReadDeadline(time.Now().Add(60 * time.Second))
	for i := 0; i < n; i++ {
		if _, err := r.ReadString('\n'); err != nil {
			t.Fatalf("drain reply %d: %v", i, err)
		}
	}
}

func timeProbes(t *testing.T, c net.Conn, r *bufio.Reader, idx []int, member func(int) string, cmd string) int64 {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(120 * time.Second))
	_ = c.SetWriteDeadline(time.Now().Add(120 * time.Second))
	start := time.Now()
	var b strings.Builder
	for _, i := range idx {
		fmt.Fprintf(&b, "%s z %s\r\n", cmd, member(i))
	}
	if _, err := c.Write([]byte(b.String())); err != nil {
		t.Fatal(err)
	}
	for range idx {
		if _, err := r.ReadString('\n'); err != nil {
			t.Fatalf("%s reply: %v", cmd, err)
		}
	}
	return time.Since(start).Nanoseconds()
}
