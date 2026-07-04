package f1srv

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// Slice 3 routes SADD/SREM/SISMEMBER/SMISMEMBER through partitions (spec 2064/f1_rewrite_ltm/19).
// Two things must hold before the routed path is trusted: it produces byte-identical replies to the
// unpartitioned path across every legal partition count, and splitting one hot key's single stripe
// lock into P partition locks lets single-key write throughput rise with P. The correctness test
// pins the first, the contention microbenchmark the second.

// newPartServer builds a bare server with the partition count forced to p and no network listener,
// so a test drives the routed set commands in-process without a socket. forceP is the slice-3 test
// hook: production leaves it 0 so every set is P=1, and setting it here drives the four routed
// commands through P>1 exactly as the adaptive engage will in slice 6.
func newPartServer(t testing.TB, p int) *Server {
	t.Helper()
	cfg := Config{Addr: "127.0.0.1:0", IndexBuckets: 1 << 12, ArenaBytes: 1 << 20, ReadBufSize: 4 << 10, IncrStripes: 64}
	srv := New(cfg)
	srv.forceP.Store(int64(p))
	return srv
}

// bareConn builds a connState bound to srv with no socket, so a test or benchmark calls the command
// methods directly and reads the reply bytes out of c.out. It is the goroutine-driver shape the real
// per-connection path uses, minus the conn, which the routed set commands never touch.
func bareConn(srv *Server) *connState {
	return &connState{srv: srv, out: make([]byte, 0, 4096)}
}

// call runs one command through the connState and returns the reply bytes it appended, resetting the
// reply buffer first so each call's reply stands alone. argv[0] is the command name, matching the
// dispatch contract the wire path feeds these methods.
func call(c *connState, dispatch func(*connState, [][]byte), args ...string) string {
	c.out = c.out[:0]
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	dispatch(c, argv)
	return string(c.out)
}

// TestSetPartitionRoutingIdentical runs one scripted sequence of the four routed set commands plus
// SCARD against P=1, 2, 4, and 8 and asserts every reply is byte-identical across all four. P=1 is
// the unpartitioned path, so matching it proves the routed P>1 path returns identical results: the
// slice-3 DoD "correctness tests confirm identical results to the unpartitioned path across P=1, 2,
// 4, 8". The sequence adds many members (spreading them across every partition), removes some,
// re-adds, and probes present, absent, and mixed members so the membership, cardinality, and
// multi-member reply shapes are all exercised under routing.
func TestSetPartitionRoutingIdentical(t *testing.T) {
	// A scripted run of commands, each a dispatch func plus its args, replayed against every P.
	type step struct {
		name string
		fn   func(*connState, [][]byte)
		args []string
	}
	sadd := func(c *connState, a [][]byte) { c.cmdSAdd(a) }
	srem := func(c *connState, a [][]byte) { c.cmdSRem(a) }
	sism := func(c *connState, a [][]byte) { c.cmdSIsMember(a) }
	smis := func(c *connState, a [][]byte) { c.cmdSMIsMember(a) }
	scard := func(c *connState, a [][]byte) { c.cmdSCard(a) }

	var steps []step
	// Add 200 members in three batches so members land across every partition of P=8.
	for b := 0; b < 3; b++ {
		args := []string{"SADD", "hot"}
		for i := b * 70; i < b*70+70; i++ {
			args = append(args, fmt.Sprintf("m%04d", i))
		}
		steps = append(steps, step{"sadd", sadd, args})
	}
	// Re-add an overlapping batch: every member already present, so the added count is 0.
	steps = append(steps, step{"sadd-dup", sadd, append([]string{"SADD", "hot"}, "m0000", "m0069", "m0139")})
	// Remove a scattered set, including a member never added so the removed count excludes it.
	steps = append(steps, step{"srem", srem, []string{"SREM", "hot", "m0000", "m0100", "m9999", "m0150"}})
	// Re-add two of the removed members so they reappear once.
	steps = append(steps, step{"re-add", sadd, []string{"SADD", "hot", "m0000", "m0150"}})
	// Probe present, absent, and a member on a missing key.
	steps = append(steps, step{"sism-present", sism, []string{"SISMEMBER", "hot", "m0050"}})
	steps = append(steps, step{"sism-absent", sism, []string{"SISMEMBER", "hot", "m0100"}})
	steps = append(steps, step{"sism-missing", sism, []string{"SISMEMBER", "cold", "m0050"}})
	// A mixed multi-member probe: present, absent, present, absent.
	steps = append(steps, step{"smis", smis, []string{"SMISMEMBER", "hot", "m0001", "m0100", "m0209", "m9999"}})
	steps = append(steps, step{"scard", scard, []string{"SCARD", "hot"}})
	// Remove every remaining member so the set drains to empty and the header retires.
	drain := []string{"SREM", "hot"}
	for i := 0; i < 210; i++ {
		drain = append(drain, fmt.Sprintf("m%04d", i))
	}
	steps = append(steps, step{"drain", srem, drain})
	steps = append(steps, step{"scard-empty", scard, []string{"SCARD", "hot"}})

	// Replay the whole sequence per P and record each reply.
	replies := map[int][]string{}
	for _, p := range []int{1, 2, 4, 8} {
		srv := newPartServer(t, p)
		c := bareConn(srv)
		got := make([]string, len(steps))
		for i, s := range steps {
			got[i] = call(c, s.fn, s.args...)
		}
		replies[p] = got
		srv.Close()
	}

	// Every P must match the unpartitioned P=1 reply byte for byte, step for step.
	ref := replies[1]
	for _, p := range []int{2, 4, 8} {
		got := replies[p]
		for i, s := range steps {
			if got[i] != ref[i] {
				t.Fatalf("P=%d step %q (%v): reply %q, want %q (P=1)", p, s.name, s.args, got[i], ref[i])
			}
		}
	}
}

// TestSetPartitionStripeSpread checks stripePart spreads one hot key's partitions across many
// distinct stripes rather than piling them onto one, which is what turns the single-stripe write
// wall into P independent locks. It does not require any partition stripe to differ from the
// whole-key stripe: single-member ops hold one lock at a time and the header create nests no lock,
// so a coincidence is harmless (see stripePart). It only requires the spread to be wide, so with a
// 64-stripe table and 16 partitions the routed writers land on a healthy number of distinct
// stripes instead of serializing.
func TestSetPartitionStripeSpread(t *testing.T) {
	const p = 16
	srv := newPartServer(t, p)
	defer srv.Close()
	key := []byte("hot")
	seen := map[uint32]bool{}
	for part := 0; part < p; part++ {
		seen[srv.stripePart(key, part)] = true
	}
	// A perfect hash would give 16 distinct stripes; allow a little collision slack but insist the
	// spread is wide enough that partitioning buys real parallelism.
	if len(seen) < p*3/4 {
		t.Fatalf("%d partitions landed on only %d distinct stripes, want a wide spread", p, len(seen))
	}
	// The same (key, partition) must always map to the same stripe so a routed write and a later
	// write to the same partition serialize correctly. Capture once and re-derive to compare, so
	// the check reads a stable value rather than two calls the compiler folds together.
	first := make([]uint32, p)
	for part := 0; part < p; part++ {
		first[part] = srv.stripePart(key, part)
	}
	for part := 0; part < p; part++ {
		if got := srv.stripePart(key, part); got != first[part] {
			t.Fatalf("stripePart is not stable for partition %d: %d then %d", part, first[part], got)
		}
	}
}

// BenchmarkSetPartitionContention hammers one hot key with concurrent SADD/SREM from many goroutines
// and reports throughput as P rises. At P=1 every writer serializes on the set's single stripe lock;
// at P>1 writers whose members route to different partitions take different partition locks and run
// on different cores, so single-key write throughput should climb with P. This is the slice-3 DoD
// "a contention microbenchmark shows single-key SADD/SREM throughput rising with P". Members are
// precomputed per goroutine so the hot loop allocates nothing and measures lock contention, not
// formatting.
func BenchmarkSetPartitionContention(b *testing.B) {
	for _, p := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("P=%d", p), func(b *testing.B) {
			srv := newPartServer(b, p)
			defer srv.Close()
			key := "hot"
			var gid int64
			b.RunParallel(func(pb *testing.PB) {
				// Each goroutine owns a disjoint member range so its writes spread across partitions
				// and never collide with another goroutine's, isolating the partition-lock split.
				base := int(atomic.AddInt64(&gid, 1)) * 4096
				members := make([][]byte, 256)
				addArgs := make([][][]byte, 256)
				remArgs := make([][][]byte, 256)
				for i := range members {
					members[i] = []byte(fmt.Sprintf("m%08d", base+i))
					addArgs[i] = [][]byte{[]byte("SADD"), []byte(key), members[i]}
					remArgs[i] = [][]byte{[]byte("SREM"), []byte(key), members[i]}
				}
				c := bareConn(srv)
				i := 0
				for pb.Next() {
					j := i & 255
					c.out = c.out[:0]
					c.cmdSAdd(addArgs[j])
					c.out = c.out[:0]
					c.cmdSRem(remArgs[j])
					i++
				}
			})
		})
	}
}
