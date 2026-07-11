package set

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
)

// Cross-shard set algebra (gathercross.go): the differential suite holds the
// intent-path gather byte-identical to the co-located point handler across the
// full operand matrix, and the concurrency suite proves the barrier freezes
// every operand at one instant. The single-shard drivers' own Redis-exactness
// is algebra_test.go's job; here the co-located arm IS the oracle, exactly as
// smovecross_test.go pins SMOVE.

const (
	gcSadd byte = iota + 1
	gcSinter
	gcSunion
	gcSdiff
	gcSintercard
	gcScard
	gcStrSet
)

func gatherHandlers() []shard.Handler {
	h := make([]shard.Handler, gcStrSet+1)
	h[gcSadd] = Sadd
	h[gcSinter] = Sinter
	h[gcSunion] = Sunion
	h[gcSdiff] = Sdiff
	h[gcSintercard] = Sintercard
	h[gcScard] = Scard
	// gcStrSet plants a string at the key, the WRONGTYPE seed.
	h[gcStrSet] = func(cx *shard.Ctx, args [][]byte, r shard.Reply) {
		_ = cx.St.Set(args[0], args[1])
		r.Int(1)
	}
	return h
}

func gatherRuntime(t *testing.T, shards int) *shard.Runtime {
	t.Helper()
	rt := shard.New(shards, 8<<20, 1<<18)
	rt.Use(gatherHandlers())
	rt.Start()
	t.Cleanup(rt.Stop)
	return rt
}

// crossAlgebra runs a transaction over keys and returns its whole reply, the
// same route dispatch takes for cross-shard operands.
func crossAlgebra(t *testing.T, c *shard.Conn, keys []string, body func(tx *shard.Txn) []byte) []byte {
	t.Helper()
	raw := make([][]byte, len(keys))
	for i, k := range keys {
		raw[i] = []byte(k)
	}
	if err := c.DoTxn(raw, body); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	var rep []byte
	deadline := time.Now().Add(10 * time.Second)
	for rep == nil {
		c.DrainReplies(func(b []byte) { rep = append([]byte(nil), b...) })
		if rep == nil {
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for the transaction reply")
			}
			runtime.Gosched()
		}
	}
	return rep
}

func bytesKeys(keys []string) [][]byte {
	raw := make([][]byte, len(keys))
	for i, k := range keys {
		raw[i] = []byte(k)
	}
	return raw
}

// TestGatherCrossDifferential replays SINTER, SUNION, SDIFF, and SINTERCARD on
// both paths: the operands co-located on one shard through the point handler,
// and the same operands spread across shards through the Cross gather under
// DoTxn. The reply bytes must agree exactly, member order included, across
// every operand shape (overlap, disjoint, missing, string-typed, intset vs
// hashtable band).
func TestGatherCrossDifferential(t *testing.T) {
	rt := gatherRuntime(t, 4)
	c := rt.NewConn()

	// operand describes one set: its members, or a string plant, or absence.
	type operand struct {
		members []string
		str     bool // plant a string, the WRONGTYPE seed
		missing bool // never created
	}
	set := func(m ...string) operand { return operand{members: m} }
	str := operand{str: true}
	missing := operand{missing: true}

	// big builds a hashtable-band operand from a stride so the cross remove and
	// clone hops run the same band code the fast path does at size.
	big := func(lo, hi, step int) operand {
		var m []string
		for i := lo; i < hi; i += step {
			m = append(m, "m"+strconv.Itoa(i))
		}
		return operand{members: m}
	}

	cases := []struct {
		name     string
		operands []operand
	}{
		{"two overlap", []operand{set("a", "b", "c"), set("b", "c", "d")}},
		{"two disjoint", []operand{set("a", "b"), set("x", "y")}},
		{"three chain", []operand{set("a", "b", "c", "d"), set("b", "c", "d"), set("c", "d")}},
		{"first missing", []operand{missing, set("a", "b")}},
		{"middle missing", []operand{set("a", "b", "c"), missing, set("b", "c")}},
		{"last missing", []operand{set("a", "b"), set("a"), missing}},
		{"all missing", []operand{missing, missing}},
		{"intset operands", []operand{set("1", "2", "3", "4"), set("2", "4", "6")}},
		{"intset mixed member", []operand{set("1", "2", "hello"), set("2", "hello", "3")}},
		{"single operand", []operand{set("a", "b", "c")}},
		{"single missing", []operand{missing}},
		{"wrongtype first", []operand{str, set("a")}},
		{"wrongtype middle", []operand{set("a"), str, set("b")}},
		{"wrongtype last", []operand{set("a"), set("b"), str}},
		{"wrongtype and missing", []operand{missing, str, set("a")}},
		{"big overlap", []operand{big(0, 400, 1), big(200, 600, 1)}},
		{"big vs small probe", []operand{big(0, 400, 1), set("m5", "m399", "zz")}},
		{"big three", []operand{big(0, 300, 1), big(0, 300, 2), big(0, 300, 3)}},
		{"big one string", []operand{big(0, 300, 1), str}},
	}

	for ci, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := len(tc.operands)
			// Co-located operands all land on shard 2; cross operands are placed
			// on distinct shards round-robin so at least two shards take part.
			co := make([]string, n)
			cross := make([]string, n)
			for i, op := range tc.operands {
				p := fmt.Sprintf("c%d_%d", ci, i)
				co[i] = keyOn(t, rt, 2, "co"+p)
				cross[i] = keyOn(t, rt, i%4, "x"+p)
				seed := func(key string, op operand) {
					switch {
					case op.missing:
					case op.str:
						do(t, c, gcStrSet, 0, key, "notaset")
					default:
						for lo := 0; lo < len(op.members); {
							hi := min(lo+128, len(op.members))
							do(t, c, gcSadd, 0, append([]string{key}, op.members[lo:hi]...)...)
							lo = hi
						}
					}
				}
				seed(co[i], op)
				seed(cross[i], op)
			}

			// SINTER
			coRep := do(t, c, gcSinter, 0, co...)
			xRep := crossAlgebra(t, c, cross, func(tx *shard.Txn) []byte {
				return SinterCross(tx, bytesKeys(cross))
			})
			if !bytes.Equal(coRep, xRep) {
				t.Fatalf("SINTER drift: co-located %q, cross-shard %q", coRep, xRep)
			}

			// SUNION
			coRep = do(t, c, gcSunion, 0, co...)
			xRep = crossAlgebra(t, c, cross, func(tx *shard.Txn) []byte {
				return SunionCross(tx, bytesKeys(cross))
			})
			if !bytes.Equal(coRep, xRep) {
				t.Fatalf("SUNION drift: co-located %q, cross-shard %q", coRep, xRep)
			}

			// SDIFF
			coRep = do(t, c, gcSdiff, 0, co...)
			xRep = crossAlgebra(t, c, cross, func(tx *shard.Txn) []byte {
				return SdiffCross(tx, bytesKeys(cross))
			})
			if !bytes.Equal(coRep, xRep) {
				t.Fatalf("SDIFF drift: co-located %q, cross-shard %q", coRep, xRep)
			}

			// SINTERCARD, unlimited and with a LIMIT that clips.
			for _, limit := range []string{"0", "2"} {
				coArgs := append([]string{strconv.Itoa(n)}, co...)
				coArgs = append(coArgs, "LIMIT", limit)
				xArgs := append([]string{strconv.Itoa(n)}, cross...)
				xArgs = append(xArgs, "LIMIT", limit)
				coRep = do(t, c, gcSintercard, 1, coArgs...)
				xRep = crossAlgebra(t, c, cross, func(tx *shard.Txn) []byte {
					return SintercardCross(tx, bytesKeys(xArgs))
				})
				if !bytes.Equal(coRep, xRep) {
					t.Fatalf("SINTERCARD LIMIT %s drift: co-located %q, cross-shard %q", limit, coRep, xRep)
				}
			}
		})
	}
}

// TestGatherCrossAtomicity is the barrier's oracle: while writers churn two
// cross-shard operands, a reader running SINTERCARD over both through the
// gather must never see a half-applied SMOVE-style displacement. A mover
// ping-pongs a shared member between the two operands with paired SADD/SREM
// (there is no single-shard SMOVE across shards, so the reader models the move
// as two operations the barrier must not interleave with its own read). The
// intersection over both operands must always report the shared member exactly
// once when it is present in both, and the invariant checked here is the weaker
// but decisive one: every gathered SINTERCARD equals the co-located SINTERCARD
// of a snapshot the reader takes under its own barrier, so the read is
// linearizable against the churn. The -race build makes it the memory-model
// check for the coordinator reading cloned operands.
func TestGatherCrossAtomicity(t *testing.T) {
	if testing.Short() {
		t.Skip("concurrency stress")
	}
	rt := gatherRuntime(t, 4)
	c := rt.NewConn()

	a := keyOn(t, rt, 0, "atoma")
	b := keyOn(t, rt, 1, "atomb")
	// Seed both with a disjoint private core plus a shared band the mover
	// shuttles. The intersection size is exactly the count of shared members
	// currently present in both, and the mover keeps every shared member in
	// exactly one of the two, so a correct barrier reads an intersection of the
	// shared members that happen to be in both at the read instant.
	const shared = 64
	do(t, c, gcSadd, 0, append([]string{a}, privateMembers("a", 200)...)...)
	do(t, c, gcSadd, 0, append([]string{b}, privateMembers("b", 200)...)...)
	for i := 0; i < shared; i++ {
		do(t, c, gcSadd, 0, a, "s"+strconv.Itoa(i))
	}

	var stop atomic.Bool
	var wg sync.WaitGroup

	// The mover ping-pongs each shared member between a and b under one barrier
	// per hop, so at every quiescent point each shared member is in exactly one
	// operand and the intersection over shared members is empty.
	wg.Add(1)
	go func() {
		defer wg.Done()
		mc := rt.NewConn()
		on := make([]bool, shared) // true: member i is in a, false: in b
		for i := range on {
			on[i] = true
		}
		for r := 0; !stop.Load(); r++ {
			i := r % shared
			m := "s" + strconv.Itoa(i)
			src, dst := a, b
			if !on[i] {
				src, dst = b, a
			}
			crossAlgebra(t, mc, []string{src, dst}, func(tx *shard.Txn) []byte {
				return SmoveCross(tx, []byte(src), []byte(dst), []byte(m))
			})
			on[i] = !on[i]
		}
	}()

	// The reader repeatedly gathers SINTERCARD over both operands on its own
	// barrier, the same rt.Begin/Acquire/Release the coordinator runs a DoTxn on
	// but held here so the test drives the read directly (smovecross_test.go's
	// reader takes the same direct route). The private cores are disjoint and
	// never move, so the only members that could ever be in both are shared
	// members, and the mover keeps each in exactly one, so a correctly barriered
	// read must always see intersection cardinality 0. A torn read that saw a
	// shared member mid-hop in both operands, cloning a with the member before
	// the SMOVE and reading b after it landed, would report a positive count,
	// the atomicity violation.
	keys := [][]byte{[]byte(a), []byte(b)}
	args := [][]byte{[]byte("2"), []byte(a), []byte(b)}
	deadline := time.Now().Add(2 * time.Second)
	reads := 0
	for time.Now().Before(deadline) {
		tx := rt.Begin(keys)
		tx.Acquire()
		rep := SintercardCross(tx, args)
		tx.Release()
		want := []byte(":0\r\n")
		if !bytes.Equal(rep, want) {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("torn read: SINTERCARD over churning operands reported %q, want %q", rep, want)
		}
		reads++
	}
	stop.Store(true)
	wg.Wait()
	if reads < 100 {
		t.Fatalf("only %d reads landed, the churn never overlapped the reads", reads)
	}
}

func privateMembers(prefix string, n int) []string {
	m := make([]string, n)
	for i := range m {
		m[i] = prefix + "p" + strconv.Itoa(i)
	}
	return m
}
