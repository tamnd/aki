package set

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// The donation oracles: every reply a donated fan-out produces must be
// byte-identical to the same command on a one-worker runtime, where FanOut has
// no pool and runs its tasks inline on the owner. The inline arm is the serial
// execution of the same decomposition, so agreement across seeds and shapes is
// the section-6.5 correctness bar, and the -race build makes it double as the
// memory-model check for the coordinator reading donated task output.

// The oracle handler table: the real set commands at fixed ops, plus two test
// seams, one to seed the shard registry's PCG so both arms draw the same
// indices and one to engage F13 draw escalation (EscalateHotDraws).
const (
	orSadd byte = iota + 1
	orSinter
	orSunion
	orSdiff
	orSintercard
	orSpop
	orSrand
	orSeed
	orEsc
)

func oracleHandlers() []shard.Handler {
	h := make([]shard.Handler, orEsc+1)
	h[orSadd] = Sadd
	h[orSinter] = Sinter
	h[orSunion] = Sunion
	h[orSdiff] = Sdiff
	h[orSintercard] = Sintercard
	h[orSpop] = Spop
	h[orSrand] = Srandmember
	h[orSeed] = func(cx *shard.Ctx, args [][]byte, r shard.Reply) {
		s1, _ := strconv.ParseUint(string(args[1]), 10, 64)
		s2, _ := strconv.ParseUint(string(args[2]), 10, 64)
		registry(cx).rng = *rand.NewPCG(s1, s2)
		r.Int(1)
	}
	h[orEsc] = func(cx *shard.Ctx, args [][]byte, r shard.Reply) {
		if EscalateHotDraws(cx, args[0], 8) {
			r.Int(1)
		} else {
			r.Int(0)
		}
	}
	return h
}

func oracleRuntime(t *testing.T, shards int) *shard.Runtime {
	t.Helper()
	rt := shard.New(shards, 8<<20, 1<<18)
	rt.Use(oracleHandlers())
	rt.Start()
	t.Cleanup(rt.Stop)
	return rt
}

// do sends one keyed command and returns its whole reply.
func do(t *testing.T, c *shard.Conn, op byte, keyIdx int, a ...string) []byte {
	t.Helper()
	args := make([][]byte, len(a))
	for i := range a {
		args[i] = []byte(a[i])
	}
	if err := c.DoAt(op, keyIdx, args); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	var rep []byte
	deadline := time.Now().Add(10 * time.Second)
	for rep == nil {
		c.DrainReplies(func(b []byte) { rep = append([]byte(nil), b...) })
		if rep == nil {
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for a reply")
			}
			runtime.Gosched()
		}
	}
	return rep
}

// colocatedKeys returns count key names that all route to one shard of rt, so
// a multi-key command finds every operand in its owner's registry.
func colocatedKeys(t *testing.T, rt *shard.Runtime, count int) []string {
	t.Helper()
	want := rt.ShardOf([]byte("anchor"))
	keys := []string{"anchor"}
	for i := 0; len(keys) < count; i++ {
		k := fmt.Sprintf("key%d", i)
		if rt.ShardOf([]byte(k)) == want {
			keys = append(keys, k)
		}
		if i > 1_000_000 {
			t.Fatal("could not co-locate keys")
		}
	}
	return keys
}

// fill adds members lo..hi (as m<i>) to key in fixed batches, the same
// insertion sequence on every arm so the set layouts match byte for byte.
func fill(t *testing.T, c *shard.Conn, op byte, key string, lo, hi int) {
	t.Helper()
	const batch = 200 // spanCap bounds one command's argument count
	for lo < hi {
		n := min(batch, hi-lo)
		a := make([]string, 0, n+1)
		a = append(a, key)
		for i := 0; i < n; i++ {
			a = append(a, "m"+strconv.Itoa(lo+i))
		}
		lo += n
		do(t, c, op, 0, a...)
	}
}

// TestAlgebraDonationOracle holds SINTER, SUNION, SDIFF, and SINTERCARD over a
// partitioned pair above fanoutFloor byte-identical between the one-worker
// inline arm and the eight-worker donated arm. The lowered partition threshold
// makes the operands partitioned (P16) without the production 262144-member
// build; fanoutFloor itself is a const, so the pair still carries 96k real
// members.
func TestAlgebraDonationOracle(t *testing.T) {
	if testing.Short() {
		t.Skip("96k-member operand build")
	}
	defer SetAlgebraMaintain(SetAlgebraMaintain(true))
	withThreshold(t, 4096)

	run := func(shards int) map[string][]byte {
		rt := oracleRuntime(t, shards)
		c := rt.NewConn()
		keys := colocatedKeys(t, rt, 2)
		a, b := keys[0], keys[1]
		fill(t, c, orSadd, a, 0, 48000)
		fill(t, c, orSadd, b, 24000, 72000)
		return map[string][]byte{
			"sinter":     do(t, c, orSinter, 0, a, b),
			"sunion":     do(t, c, orSunion, 0, a, b),
			"sdiff":      do(t, c, orSdiff, 0, a, b),
			"sintercard": do(t, c, orSintercard, 1, "2", a, b),
			"limited":    do(t, c, orSintercard, 1, "2", a, b, "LIMIT", "1000"),
		}
	}

	serial := run(1)
	donated := run(8)
	for name, want := range serial {
		if got := string(donated[name]); got != string(want) {
			t.Errorf("%s: donated reply diverges from the inline arm (%d vs %d bytes)", name, len(got), len(want))
		}
	}
	if len(serial["sinter"]) < 24000 {
		t.Fatalf("sinter reply implausibly small (%d bytes); the merge path did not engage", len(serial["sinter"]))
	}
}

// TestDrawDonationOracle holds the escalated count-form draws (SPOP count,
// SRANDMEMBER count, SRANDMEMBER -count) byte-identical between the inline and
// donated arms across seeds. The indices are drawn serially on the owner's PCG
// in both arms (drawfan.go), so any divergence is the fanned resolve reading
// or ordering wrong.
func TestDrawDonationOracle(t *testing.T) {
	withThreshold(t, 512)

	seeds := [][2]uint64{{2064, 17}, {1, 1}, {987654321, 42}}
	run := func(shards int) [][]byte {
		rt := oracleRuntime(t, shards)
		c := rt.NewConn()
		var out [][]byte
		for i, seed := range seeds {
			key := "hot" + strconv.Itoa(i)
			fill(t, c, orSadd, key, 0, 16384)
			if esc := do(t, c, orEsc, 0, key); string(esc) != ":1\r\n" {
				t.Fatalf("escalation did not engage: %q", esc)
			}
			s1 := strconv.FormatUint(seed[0], 10)
			s2 := strconv.FormatUint(seed[1], 10)
			do(t, c, orSeed, 0, key, s1, s2)
			out = append(out,
				do(t, c, orSrand, 0, key, "3000"),
				do(t, c, orSrand, 0, key, "-3000"),
				do(t, c, orSpop, 0, key, "3000"),
			)
		}
		return out
	}

	serial := run(1)
	donated := run(8)
	if len(serial) != len(donated) {
		t.Fatalf("arm reply counts differ: %d vs %d", len(serial), len(donated))
	}
	names := []string{"srandmember 3000", "srandmember -3000", "spop 3000"}
	for i := range serial {
		if string(serial[i]) != string(donated[i]) {
			t.Errorf("seed %d %s: donated reply diverges from the inline arm", i/3, names[i%3])
		}
	}
}
