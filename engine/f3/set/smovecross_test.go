package set

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
)

// Cross-shard SMOVE (smovecross.go): the differential suite holds the intent
// path byte-identical to the single-shard fast path across the full case
// matrix, and the concurrency suite proves the atomicity the barrier buys.
// The single-shard core's own Redis-exactness is smove_test.go's job; here
// the co-located arm IS the oracle.

const (
	csSadd byte = iota + 1
	csSrem
	csSismember
	csScard
	csSmove
	csStrSet
)

func crossHandlers() []shard.Handler {
	h := make([]shard.Handler, csStrSet+1)
	h[csSadd] = Sadd
	h[csSrem] = Srem
	h[csSismember] = Sismember
	h[csScard] = Scard
	h[csSmove] = Smove
	// csStrSet plants a string at the key, the WRONGTYPE seed.
	h[csStrSet] = func(cx *shard.Ctx, args [][]byte, r shard.Reply) {
		_ = cx.St.Set(args[0], args[1])
		r.Int(1)
	}
	return h
}

func crossRuntime(t *testing.T, shards int) *shard.Runtime {
	t.Helper()
	rt := shard.New(shards, 8<<20, 1<<18)
	rt.Use(crossHandlers())
	rt.Start()
	t.Cleanup(rt.Stop)
	return rt
}

// keyOn returns a key with the given prefix routed to shard sh.
func keyOn(t *testing.T, rt *shard.Runtime, sh int, prefix string) string {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		k := prefix + strconv.Itoa(i)
		if rt.ShardOf([]byte(k)) == sh {
			return k
		}
	}
	t.Fatalf("no key with prefix %q on shard %d", prefix, sh)
	return ""
}

// crossSmove runs SMOVE on the intent path and returns the reply, the same
// route dispatch takes for a cross-shard pair.
func crossSmove(t *testing.T, c *shard.Conn, src, dst, member string) []byte {
	t.Helper()
	err := c.DoTxn([][]byte{[]byte(src), []byte(dst)}, func(tx *shard.Txn) []byte {
		return SmoveCross(tx, []byte(src), []byte(dst), []byte(member))
	})
	if err != nil {
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

// TestSmoveCrossDifferential replays every SMOVE shape on both paths: the
// co-located pair through the Smove point handler and a cross-shard pair
// through SmoveCross under DoTxn. Reply bytes and the post-state probes
// (SCARD and per-member SISMEMBER on both keys) must agree exactly.
func TestSmoveCrossDifferential(t *testing.T) {
	rt := crossRuntime(t, 4)
	c := rt.NewConn()

	cases := []struct {
		name           string
		src, dst       []string // nil is a missing key
		srcStr, dstStr bool     // plant a string instead, the WRONGTYPE seed
		member         string
	}{
		{name: "present into existing", src: []string{"a", "b", "c"}, dst: []string{"x"}, member: "b"},
		{name: "present into missing", src: []string{"a", "b"}, dst: nil, member: "a"},
		{name: "present in both", src: []string{"a", "b"}, dst: []string{"b", "x"}, member: "b"},
		{name: "absent member", src: []string{"a", "b"}, dst: []string{"x"}, member: "zz"},
		{name: "missing src", src: nil, dst: []string{"x"}, member: "a"},
		{name: "both missing", src: nil, dst: nil, member: "a"},
		{name: "last member drops src", src: []string{"only"}, dst: []string{"x"}, member: "only"},
		{name: "intset member", src: []string{"1", "2", "3"}, dst: []string{"9"}, member: "2"},
		{name: "wrongtype src", srcStr: true, dst: []string{"x"}, member: "a"},
		{name: "wrongtype dst", src: []string{"a"}, dstStr: true, member: "a"},
		{name: "wrongtype both", srcStr: true, dstStr: true, member: "a"},
		{name: "wrongtype dst absent member", src: []string{"a"}, dstStr: true, member: "zz"},
	}
	for ci, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := fmt.Sprintf("c%d", ci)
			coSrc := keyOn(t, rt, 2, p+"cosrc")
			coDst := keyOn(t, rt, 2, p+"codst")
			xSrc := keyOn(t, rt, 0, p+"xsrc")
			xDst := keyOn(t, rt, 1, p+"xdst")

			seed := func(key string, members []string, str bool) {
				if str {
					do(t, c, csStrSet, 0, key, "notaset")
					return
				}
				if members == nil {
					return
				}
				do(t, c, csSadd, 0, append([]string{key}, members...)...)
			}
			seed(coSrc, tc.src, tc.srcStr)
			seed(coDst, tc.dst, tc.dstStr)
			seed(xSrc, tc.src, tc.srcStr)
			seed(xDst, tc.dst, tc.dstStr)

			coRep := do(t, c, csSmove, 0, coSrc, coDst, tc.member)
			xRep := crossSmove(t, c, xSrc, xDst, tc.member)
			if !bytes.Equal(coRep, xRep) {
				t.Fatalf("reply drift: co-located %q, cross-shard %q", coRep, xRep)
			}

			probes := append([]string{tc.member}, tc.src...)
			probes = append(probes, tc.dst...)
			check := func(co, x string) {
				coCard := do(t, c, csScard, 0, co)
				xCard := do(t, c, csScard, 0, x)
				if !bytes.Equal(coCard, xCard) {
					t.Fatalf("SCARD drift on %s/%s: co-located %q, cross-shard %q", co, x, coCard, xCard)
				}
				for _, m := range probes {
					coIs := do(t, c, csSismember, 0, co, m)
					xIs := do(t, c, csSismember, 0, x, m)
					if !bytes.Equal(coIs, xIs) {
						t.Fatalf("SISMEMBER %q drift on %s/%s: co-located %q, cross-shard %q", m, co, x, coIs, xIs)
					}
				}
			}
			check(coSrc, xSrc)
			check(coDst, xDst)
		})
	}
}

// TestSmoveCrossDifferentialHashtable is the big-band arm of the differential:
// hashtable-band operands, so the cross-shard remove and insert hops run the
// same band code the fast path does at size.
func TestSmoveCrossDifferentialHashtable(t *testing.T) {
	rt := crossRuntime(t, 4)
	c := rt.NewConn()
	coSrc := keyOn(t, rt, 2, "hcosrc")
	coDst := keyOn(t, rt, 2, "hcodst")
	xSrc := keyOn(t, rt, 0, "hxsrc")
	xDst := keyOn(t, rt, 1, "hxdst")
	for _, k := range []string{coSrc, xSrc} {
		fill(t, c, csSadd, k, 0, 300)
	}
	for _, k := range []string{coDst, xDst} {
		fill(t, c, csSadd, k, 150, 450)
	}
	for _, m := range []string{"m5", "m200", "m999"} {
		coRep := do(t, c, csSmove, 0, coSrc, coDst, m)
		xRep := crossSmove(t, c, xSrc, xDst, m)
		if !bytes.Equal(coRep, xRep) {
			t.Fatalf("reply drift on %q: co-located %q, cross-shard %q", m, coRep, xRep)
		}
	}
	for _, pair := range [][2]string{{coSrc, xSrc}, {coDst, xDst}} {
		coCard := do(t, c, csScard, 0, pair[0])
		xCard := do(t, c, csScard, 0, pair[1])
		if !bytes.Equal(coCard, xCard) {
			t.Fatalf("SCARD drift on %s/%s: %q vs %q", pair[0], pair[1], coCard, xCard)
		}
		for i := 0; i < 450; i += 7 {
			m := "m" + strconv.Itoa(i)
			coIs := do(t, c, csSismember, 0, pair[0], m)
			xIs := do(t, c, csSismember, 0, pair[1], m)
			if !bytes.Equal(coIs, xIs) {
				t.Fatalf("SISMEMBER %q drift on %s/%s", m, pair[0], pair[1])
			}
		}
	}
}

// TestSmoveCrossAtomicity is the coordinator's oracle: while cross-shard SMOVE
// ping-pongs one member between two keys on different shards, a reader holding
// its own barrier over both keys must see the member in exactly one set, never
// both and never neither, at every observation. SADD/SREM churn on private
// members runs on both keys throughout, and each churner's final member state
// must match its last operation, the per-member linearizability witness. The
// move counters give the SMOVE witness: successes in the two directions differ
// by the member's net displacement, so a lost or doubled move is arithmetic.
// The whole test reruns across seeds and the -race build makes it double as
// the memory-model check.
func TestSmoveCrossAtomicity(t *testing.T) {
	for seed := 0; seed < 4; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Parallel()
			rt := crossRuntime(t, 4)
			pfx := fmt.Sprintf("s%d", seed)
			src := keyOn(t, rt, 0, pfx+"a")
			dst := keyOn(t, rt, 1, pfx+"b")
			member := "ball"

			seedConn := rt.NewConn()
			do(t, seedConn, csSadd, 0, src, member, "srcpad")
			do(t, seedConn, csSadd, 0, dst, "dstpad")

			const rounds = 150
			var fwd, back int // successful moves in each direction
			var workers, readers sync.WaitGroup
			stop := make(chan struct{})

			mover := func(a, b string, hits *int) {
				defer workers.Done()
				c := rt.NewConn()
				for i := 0; i < rounds; i++ {
					rep := crossSmove(t, c, a, b, member)
					if bytes.Equal(rep, []byte(":1\r\n")) {
						*hits++
					}
				}
			}
			churn := func(key, mine string) {
				defer workers.Done()
				c := rt.NewConn()
				for i := 0; i < rounds; i++ {
					do(t, c, csSadd, 0, key, mine)
					do(t, c, csSrem, 0, key, mine)
				}
				do(t, c, csSadd, 0, key, mine)
			}
			reader := func() {
				defer readers.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					var inSrc, inDst bool
					tx := rt.Begin([][]byte{[]byte(src), []byte(dst)})
					tx.Acquire()
					tx.Do([]byte(src), func(cx *shard.Ctx) {
						s, _ := registry(cx).lookup(cx, []byte(src))
						inSrc = s != nil && s.has([]byte(member))
					})
					tx.Do([]byte(dst), func(cx *shard.Ctx) {
						s, _ := registry(cx).lookup(cx, []byte(dst))
						inDst = s != nil && s.has([]byte(member))
					})
					tx.Release()
					if inSrc == inDst {
						t.Errorf("atomicity broken: inSrc=%v inDst=%v", inSrc, inDst)
						return
					}
				}
			}

			workers.Add(4)
			readers.Add(1)
			go mover(src, dst, &fwd)
			go mover(dst, src, &back)
			go churn(src, "churnsrc")
			go churn(dst, "churndst")
			go reader()
			workers.Wait()
			close(stop)
			readers.Wait()

			// Final state: the member sits where the move arithmetic says.
			c := rt.NewConn()
			inSrc := bytes.Equal(do(t, c, csSismember, 0, src, member), []byte(":1\r\n"))
			inDst := bytes.Equal(do(t, c, csSismember, 0, dst, member), []byte(":1\r\n"))
			if inSrc == inDst {
				t.Fatalf("final placement broken: inSrc=%v inDst=%v", inSrc, inDst)
			}
			netForward := fwd - back
			if inDst && netForward != 1 {
				t.Fatalf("move arithmetic: member at dst but fwd-back=%d", netForward)
			}
			if inSrc && netForward != 0 {
				t.Fatalf("move arithmetic: member at src but fwd-back=%d", netForward)
			}
			// The churners' last op was SADD: both private members present.
			if !bytes.Equal(do(t, c, csSismember, 0, src, "churnsrc"), []byte(":1\r\n")) {
				t.Fatal("src churner's final SADD lost")
			}
			if !bytes.Equal(do(t, c, csSismember, 0, dst, "churndst"), []byte(":1\r\n")) {
				t.Fatal("dst churner's final SADD lost")
			}
		})
	}
}
