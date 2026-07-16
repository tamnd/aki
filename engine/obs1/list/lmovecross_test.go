package list

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// Cross-shard LMOVE and RPOPLPUSH (lmovecross.go): the differential suite holds
// the intent path byte-identical to the co-located fast path across the full
// case matrix, and the concurrency suite proves the atomicity the barrier buys.
// The co-located core's own Redis-exactness is lmove_test.go's job; here the
// co-located arm IS the oracle. The whole point of the cross path is the
// cross-hop element capture, so every case seeds a co-located pair on one shard
// and a cross-shard pair on two shards from the same data and demands the same
// reply bytes and the same post-move LRANGE on both keys.

const (
	xcRpush byte = iota + 1
	xcLmove
	xcRpoplpush
	xcLrange
	xcSet
	xcLast
)

func crossMoveHandlers() []shard.Handler {
	h := make([]shard.Handler, xcLast)
	h[xcRpush] = Rpush
	h[xcLmove] = Lmove
	h[xcRpoplpush] = Rpoplpush
	h[xcLrange] = Lrange
	// xcSet plants a string at the key, the WRONGTYPE seed.
	h[xcSet] = func(cx *shard.Ctx, args [][]byte, r shard.Reply) {
		if err := cx.St.Set(args[0], args[1]); err != nil {
			r.Err("ERR " + err.Error())
			return
		}
		r.Status("OK")
	}
	return h
}

func crossMoveRuntime(t *testing.T, shards int) *shard.Runtime {
	t.Helper()
	rt := shard.New(shards, 8<<20, 1<<18)
	rt.Use(crossMoveHandlers())
	rt.Start()
	t.Cleanup(rt.Stop)
	return rt
}

// keyOnShard returns a key with the given prefix routed to shard sh.
func keyOnShard(t *testing.T, rt *shard.Runtime, sh int, prefix string) string {
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

// crossMove runs LMOVE on the intent path and returns the reply, the same route
// dispatch takes for a cross-shard pair.
func crossMove(t *testing.T, c *shard.Conn, src, dst string, srcLeft, dstLeft bool) []byte {
	t.Helper()
	return runTxnReply(t, c, src, dst, func(tx *shard.Txn) []byte {
		return LmoveCross(tx, []byte(src), []byte(dst), srcLeft, dstLeft)
	})
}

// crossRpoplpush runs RPOPLPUSH on the intent path.
func crossRpoplpush(t *testing.T, c *shard.Conn, src, dst string) []byte {
	t.Helper()
	return runTxnReply(t, c, src, dst, func(tx *shard.Txn) []byte {
		return RpoplpushCross(tx, []byte(src), []byte(dst))
	})
}

func runTxnReply(t *testing.T, c *shard.Conn, src, dst string, body func(tx *shard.Txn) []byte) []byte {
	t.Helper()
	err := c.DoTxn([][]byte{[]byte(src), []byte(dst)}, body)
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

// TestLmoveCrossDifferential replays every LMOVE shape on both paths across all
// four directions: the co-located pair through the Lmove point handler and a
// cross-shard pair through LmoveCross under DoTxn. Reply bytes and the post-move
// LRANGE of both keys must agree exactly, so the cross path is proven identical
// to the co-located move a client cannot tell apart.
func TestLmoveCrossDifferential(t *testing.T) {
	rt := crossMoveRuntime(t, 4)
	c := rt.NewConn()

	dirs := []struct {
		name             string
		from, to         string
		srcLeft, dstLeft bool
	}{
		{"LEFT LEFT", "LEFT", "LEFT", true, true},
		{"LEFT RIGHT", "LEFT", "RIGHT", true, false},
		{"RIGHT LEFT", "RIGHT", "LEFT", false, true}, // the RPOPLPUSH move
		{"RIGHT RIGHT", "RIGHT", "RIGHT", false, false},
	}
	cases := []struct {
		name           string
		src, dst       []string // nil is a missing key
		srcStr, dstStr bool     // plant a string instead, the WRONGTYPE seed
	}{
		{name: "present into existing", src: []string{"a", "b", "c"}, dst: []string{"x", "y"}},
		{name: "present into missing", src: []string{"a", "b"}, dst: nil},
		{name: "drains source", src: []string{"solo"}, dst: []string{"x"}},
		{name: "missing src", src: nil, dst: []string{"x"}},
		{name: "both missing", src: nil, dst: nil},
		{name: "wrongtype src", srcStr: true, dst: []string{"x"}},
		{name: "wrongtype dst present src", src: []string{"a"}, dstStr: true},
		{name: "wrongtype dst missing src", src: nil, dstStr: true},
		{name: "wrongtype both", srcStr: true, dstStr: true},
		{name: "native band", src: bigVals("s", 200), dst: bigVals("d", 200)},
	}
	for ci, tc := range cases {
		for _, dir := range dirs {
			t.Run(tc.name+" "+dir.name, func(t *testing.T) {
				p := fmt.Sprintf("c%d%s", ci, dir.from[:1]+dir.to[:1])
				coSrc := keyOnShard(t, rt, 2, p+"cosrc")
				coDst := keyOnShard(t, rt, 2, p+"codst")
				xSrc := keyOnShard(t, rt, 0, p+"xsrc")
				xDst := keyOnShard(t, rt, 1, p+"xdst")

				seed := func(key string, vals []string, str bool) {
					if str {
						do(t, c, xcSet, key, "notalist")
						return
					}
					for _, v := range vals {
						do(t, c, xcRpush, key, v)
					}
				}
				seed(coSrc, tc.src, tc.srcStr)
				seed(coDst, tc.dst, tc.dstStr)
				seed(xSrc, tc.src, tc.srcStr)
				seed(xDst, tc.dst, tc.dstStr)

				coRep := do(t, c, xcLmove, coSrc, coDst, dir.from, dir.to)
				xRep := crossMove(t, c, xSrc, xDst, dir.srcLeft, dir.dstLeft)
				if !bytes.Equal(coRep, xRep) {
					t.Fatalf("reply drift: co-located %q, cross-shard %q", coRep, xRep)
				}
				eqRange(t, c, "source", coSrc, xSrc)
				eqRange(t, c, "destination", coDst, xDst)
			})
		}
	}
}

// TestRpoplpushCrossDifferential is the RPOPLPUSH arm: RpoplpushCross must match
// the co-located Rpoplpush handler (LMOVE source destination RIGHT LEFT) reply
// and post-move state across the same case matrix.
func TestRpoplpushCrossDifferential(t *testing.T) {
	rt := crossMoveRuntime(t, 4)
	c := rt.NewConn()

	cases := []struct {
		name           string
		src, dst       []string
		srcStr, dstStr bool
	}{
		{name: "present into existing", src: []string{"a", "b", "c"}, dst: []string{"x", "y"}},
		{name: "present into missing", src: []string{"a", "b"}, dst: nil},
		{name: "drains source", src: []string{"solo"}, dst: []string{"x"}},
		{name: "missing src", src: nil, dst: []string{"x"}},
		{name: "wrongtype src", srcStr: true, dst: []string{"x"}},
		{name: "wrongtype dst present src", src: []string{"a"}, dstStr: true},
		{name: "native band", src: bigVals("s", 200), dst: bigVals("d", 200)},
	}
	for ci, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := fmt.Sprintf("r%d", ci)
			coSrc := keyOnShard(t, rt, 2, p+"cosrc")
			coDst := keyOnShard(t, rt, 2, p+"codst")
			xSrc := keyOnShard(t, rt, 0, p+"xsrc")
			xDst := keyOnShard(t, rt, 1, p+"xdst")

			seed := func(key string, vals []string, str bool) {
				if str {
					do(t, c, xcSet, key, "notalist")
					return
				}
				for _, v := range vals {
					do(t, c, xcRpush, key, v)
				}
			}
			seed(coSrc, tc.src, tc.srcStr)
			seed(coDst, tc.dst, tc.dstStr)
			seed(xSrc, tc.src, tc.srcStr)
			seed(xDst, tc.dst, tc.dstStr)

			coRep := do(t, c, xcRpoplpush, coSrc, coDst)
			xRep := crossRpoplpush(t, c, xSrc, xDst)
			if !bytes.Equal(coRep, xRep) {
				t.Fatalf("reply drift: co-located %q, cross-shard %q", coRep, xRep)
			}
			eqRange(t, c, "source", coSrc, xSrc)
			eqRange(t, c, "destination", coDst, xDst)
		})
	}
}

// eqRange fails unless the two keys hold the same list, read through LRANGE 0 -1,
// so the post-move state is compared byte-exact and not just the reply.
func eqRange(t *testing.T, c *shard.Conn, what, co, x string) {
	t.Helper()
	coR := do(t, c, xcLrange, co, "0", "-1")
	xR := do(t, c, xcLrange, x, "0", "-1")
	if !bytes.Equal(coR, xR) {
		t.Fatalf("%s LRANGE drift on %s/%s: co-located %q, cross-shard %q", what, co, x, coR, xR)
	}
}

// TestLmoveCrossAtomicity is the coordinator's oracle: while two movers ping-pong
// end elements between two lists on different shards, a reader holding its own
// barrier over both keys must always see the full element set spread across the
// two lists with nothing duplicated and nothing lost. The move publishes the
// element onto the destination before removing it from the source, so the only
// transient a non-atomic path could leak is element-in-both, which the reader
// would catch as a count of 2N+1. Under the barrier the reader never sees it: the
// total is exactly 2N at every observation and the union is the seeded set. The
// test reruns across seeds and the -race build makes it double as the memory
// model check.
func TestLmoveCrossAtomicity(t *testing.T) {
	for seed := 0; seed < 4; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Parallel()
			rt := crossMoveRuntime(t, 4)
			pfx := fmt.Sprintf("m%d", seed)
			a := keyOnShard(t, rt, 0, pfx+"a")
			b := keyOnShard(t, rt, 1, pfx+"b")

			const n = 8
			aVals := make([]string, n)
			bVals := make([]string, n)
			seedConn := rt.NewConn()
			for i := 0; i < n; i++ {
				aVals[i] = fmt.Sprintf("%s-a-%d", pfx, i)
				bVals[i] = fmt.Sprintf("%s-b-%d", pfx, i)
				do(t, seedConn, xcRpush, a, aVals[i])
				do(t, seedConn, xcRpush, b, bVals[i])
			}
			want := multiset(aVals, bVals)

			const rounds = 200
			var workers, readers sync.WaitGroup
			stop := make(chan struct{})

			// mover pops one end and pushes the other, ping-ponging elements across
			// the shard boundary. A drained source replies null and moves nothing,
			// so a mover that outran the other simply idles until it is fed again.
			mover := func(src, dst string, srcLeft, dstLeft bool) {
				defer workers.Done()
				c := rt.NewConn()
				for i := 0; i < rounds; i++ {
					crossMove(t, c, src, dst, srcLeft, dstLeft)
				}
			}
			reader := func() {
				defer readers.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					got := make(map[string]int)
					total := 0
					tx := rt.Begin([][]byte{[]byte(a), []byte(b)})
					tx.Acquire()
					collect := func(cx *shard.Ctx, key string) {
						if l := registry(cx).m[key]; l != nil {
							for _, v := range decode(l) {
								got[v]++
								total++
							}
						}
					}
					tx.Do([]byte(a), func(cx *shard.Ctx) { collect(cx, a) })
					tx.Do([]byte(b), func(cx *shard.Ctx) { collect(cx, b) })
					tx.Release()
					if total != 2*n {
						t.Errorf("atomicity broken: saw %d elements across both lists, want %d", total, 2*n)
						return
					}
					for v, cnt := range got {
						if want[v] != cnt {
							t.Errorf("atomicity broken: element %q seen %d times, want %d", v, cnt, want[v])
							return
						}
					}
				}
			}

			workers.Add(2)
			readers.Add(1)
			go mover(a, b, false, true) // RIGHT LEFT: tail of a to head of b
			go mover(b, a, false, true) // RIGHT LEFT: tail of b to head of a
			go reader()
			workers.Wait()
			close(stop)
			readers.Wait()

			// Final state: the seeded set is conserved across the two lists, nothing
			// duplicated and nothing dropped by any move.
			c := rt.NewConn()
			final := multiset(listAll(t, c, a), listAll(t, c, b))
			if len(final) != len(want) {
				t.Fatalf("final multiset has %d distinct elements, want %d", len(final), len(want))
			}
			for v, cnt := range want {
				if final[v] != cnt {
					t.Fatalf("final element %q count %d, want %d", v, final[v], cnt)
				}
			}
		})
	}
}

// listAll reads a key's whole list through LRANGE 0 -1 into a []string.
func listAll(t *testing.T, c *shard.Conn, key string) []string {
	t.Helper()
	got := decodeReply(t, do(t, c, xcLrange, key, "0", "-1"))
	arr, ok := got.([]any)
	if !ok {
		return nil
	}
	out := make([]string, len(arr))
	for i := range arr {
		out[i], _ = arr[i].(string)
	}
	return out
}

// --- live redis parity ----------------------------------------------------

// crossMoveDiffer pairs a multi-shard move runtime with a live redis. The cross
// keys are forced onto different shards, so the move really rides the intent
// path, and every reply and post-move LRANGE is compared byte-exact against
// redis-server 8.8.
type crossMoveDiffer struct {
	t  *testing.T
	rt *shard.Runtime
	c  *shard.Conn
	r  *redisConn
}

func newCrossMoveDiffer(t *testing.T) *crossMoveDiffer {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay the cross-shard move commands against a live Redis")
	}
	rc, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(rc.close)
	rt := crossMoveRuntime(t, 4)
	return &crossMoveDiffer{t: t, rt: rt, c: rt.NewConn(), r: rc}
}

// pair returns a source on shard 0 and a destination on shard 1, freshly deleted
// on the redis side, so the two keys genuinely span shards on the aki side.
func (d *crossMoveDiffer) pair(name string) (string, string) {
	src := keyOnShard(d.t, d.rt, 0, "akixmv:"+name+":src")
	dst := keyOnShard(d.t, d.rt, 1, "akixmv:"+name+":dst")
	d.r.cmd("DEL", src)
	d.r.cmd("DEL", dst)
	return src, dst
}

// agreeRpush replays RPUSH on both backends (a single-key command, routed
// normally on the aki side).
func (d *crossMoveDiffer) agreeRpush(key string, vals ...string) {
	d.t.Helper()
	mine := decodeReply(d.t, do(d.t, d.c, xcRpush, append([]string{key}, vals...)...))
	theirs, err := d.r.cmdReply(append([]string{"RPUSH", key}, vals...)...)
	if err != nil {
		d.t.Fatalf("RPUSH redis transport error: %v", err)
	}
	if !equalReply(mine, theirs) {
		d.t.Fatalf("RPUSH %s: aki %v, redis %v", key, render(mine), render(theirs))
	}
}

// agreeSet plants a string on both backends, the WRONGTYPE seed.
func (d *crossMoveDiffer) agreeSet(key, val string) {
	d.t.Helper()
	do(d.t, d.c, xcSet, key, val)
	if _, err := d.r.cmdReply("SET", key, val); err != nil {
		d.t.Fatalf("SET %s: redis transport error: %v", key, err)
	}
}

// agreeMove runs LMOVE on the cross path here and on redis, then compares both
// keys through LRANGE, so the reply and the post-move state are byte-exact.
func (d *crossMoveDiffer) agreeMove(src, dst, from, to string, srcLeft, dstLeft bool) {
	d.t.Helper()
	mine := decodeReply(d.t, crossMove(d.t, d.c, src, dst, srcLeft, dstLeft))
	theirs, err := d.r.cmdReply("LMOVE", src, dst, from, to)
	if err != nil {
		d.t.Fatalf("LMOVE %s %s %s %s: redis transport error: %v", src, dst, from, to, err)
	}
	if !equalReply(mine, theirs) {
		d.t.Fatalf("LMOVE %s %s %s %s: aki %v, redis %v", src, dst, from, to, render(mine), render(theirs))
	}
	d.agreeRange(src)
	d.agreeRange(dst)
}

// agreeRpoplpush runs RPOPLPUSH on the cross path here and on redis, then
// compares both keys.
func (d *crossMoveDiffer) agreeRpoplpush(src, dst string) {
	d.t.Helper()
	mine := decodeReply(d.t, crossRpoplpush(d.t, d.c, src, dst))
	theirs, err := d.r.cmdReply("RPOPLPUSH", src, dst)
	if err != nil {
		d.t.Fatalf("RPOPLPUSH %s %s: redis transport error: %v", src, dst, err)
	}
	if !equalReply(mine, theirs) {
		d.t.Fatalf("RPOPLPUSH %s %s: aki %v, redis %v", src, dst, render(mine), render(theirs))
	}
	d.agreeRange(src)
	d.agreeRange(dst)
}

func (d *crossMoveDiffer) agreeRange(key string) {
	d.t.Helper()
	mine := decodeReply(d.t, do(d.t, d.c, xcLrange, key, "0", "-1"))
	theirs, err := d.r.cmdReply("LRANGE", key, "0", "-1")
	if err != nil {
		d.t.Fatalf("LRANGE %s: redis transport error: %v", key, err)
	}
	if !equalReply(mine, theirs) {
		d.t.Fatalf("LRANGE %s: aki %v, redis %v", key, render(mine), render(theirs))
	}
}

func TestLMoveCrossAgainstRedis(t *testing.T) {
	d := newCrossMoveDiffer(t)
	dirs := []struct {
		from, to         string
		srcLeft, dstLeft bool
	}{
		{"LEFT", "LEFT", true, true},
		{"LEFT", "RIGHT", true, false},
		{"RIGHT", "LEFT", false, true},
		{"RIGHT", "RIGHT", false, false},
	}
	for _, dir := range dirs {
		src, dst := d.pair("dir" + dir.from + dir.to)
		d.agreeRpush(src, "a", "b", "c", "d")
		d.agreeMove(src, dst, dir.from, dir.to, dir.srcLeft, dir.dstLeft)
		d.agreeMove(src, dst, dir.from, dir.to, dir.srcLeft, dir.dstLeft)
	}

	// Missing source: a null bulk, no side effect on the destination.
	src, dst := d.pair("missing")
	d.agreeMove(src, dst, "LEFT", "RIGHT", true, false)

	// Draining a source to empty deletes it: the second move finds it gone.
	one, oneDst := d.pair("one")
	d.agreeRpush(one, "solo")
	d.agreeMove(one, oneDst, "LEFT", "LEFT", true, true)
	d.agreeMove(one, oneDst, "LEFT", "LEFT", true, true)

	// WRONGTYPE on the source and on the destination (present source).
	strSrc, listDst := d.pair("wrongsrc")
	d.agreeSet(strSrc, "v")
	d.agreeRpush(listDst, "a")
	d.agreeMove(strSrc, listDst, "LEFT", "RIGHT", true, false)
	listSrc, strDst := d.pair("wrongdst")
	d.agreeRpush(listSrc, "a")
	d.agreeSet(strDst, "v")
	d.agreeMove(listSrc, strDst, "LEFT", "RIGHT", true, false)

	// A native-band, many-chunk move across the shard boundary.
	big, bigDst := d.pair("big")
	block := strings.Repeat("q", 100)
	for i := 0; i < 300; i++ {
		d.agreeRpush(big, fmt.Sprintf("%04d:", i)+block)
	}
	for i := 0; i < 50; i++ {
		d.agreeMove(big, bigDst, "RIGHT", "LEFT", false, true)
	}
}

func TestRpoplpushCrossAgainstRedis(t *testing.T) {
	d := newCrossMoveDiffer(t)

	src, dst := d.pair("basic")
	d.agreeRpush(src, "a", "b", "c")
	d.agreeRpoplpush(src, dst)
	d.agreeRpoplpush(src, dst)

	// Missing source: a null bulk, no side effect.
	absent, nullDst := d.pair("absent")
	d.agreeRpoplpush(absent, nullDst)

	// WRONGTYPE on the source and on the destination.
	strSrc, listDst := d.pair("wrongsrc")
	d.agreeSet(strSrc, "v")
	d.agreeRpush(listDst, "a")
	d.agreeRpoplpush(strSrc, listDst)
	listSrc, strDst := d.pair("wrongdst")
	d.agreeRpush(listSrc, "a")
	d.agreeSet(strDst, "v")
	d.agreeRpoplpush(listSrc, strDst)

	// A native-band, many-chunk move across the shard boundary.
	big, bigDst := d.pair("big")
	block := strings.Repeat("q", 100)
	for i := 0; i < 300; i++ {
		d.agreeRpush(big, fmt.Sprintf("%04d:", i)+block)
	}
	for i := 0; i < 50; i++ {
		d.agreeRpoplpush(big, bigDst)
	}
}
