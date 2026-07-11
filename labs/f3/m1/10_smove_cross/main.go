// Lab: the cross-shard SMOVE tax (spec 2064/f3 doc 03 section 6.7, doc 11
// section 9.2, M1 lab 10).
//
// Slice 3 routes SMOVE onto the F17 intent path when source and destination
// live on different shards: two arm sub-commands fence the transaction into
// the pipeline, the coordinator acquires both keys' intents, three barrier
// hops do the type checks and the move, and a loopback node carries the reply
// back at the command's sequence. The co-located pair never pays any of that;
// dispatch keeps it on the single-shard point path #602 shipped. This lab
// prices the difference, because the divert is only defensible if the
// co-located path stays untouched (the DrainExecute gate reads that) and the
// cross-shard path costs what a three-hop barrier plausibly should.
//
// Method: one runtime, one synchronous connection, a src/dst pair either
// co-located on one shard or split across two, both sets prefilled to the
// swept cardinality so the moved member's band work is identical on both
// arms. The measured unit is the ping-pong pair (move there, move back), so
// the keys' state is byte-identical at every rep boundary and both arms move
// the same member through the same bands forever. Cardinality sweeps the
// bands (listpack, small hashtable, large hashtable); the shard count sweeps
// the runtime the barrier competes in. A pipelined arm (depth 16) reads the
// throughput cost separately from the latency cost, since the cross-shard
// path spends one goroutine handoff per command that pipelining can hide.
package main

import (
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/tamnd/aki/engine/f3/set"
	"github.com/tamnd/aki/engine/f3/shard"
)

const (
	opSadd byte = iota + 1
	opSmove
)

func handlers() []shard.Handler {
	h := make([]shard.Handler, opSmove+1)
	h[opSadd] = set.Sadd
	h[opSmove] = set.Smove
	return h
}

// bench is one runtime under measurement plus a synchronous connection.
type bench struct {
	rt *shard.Runtime
	c  *shard.Conn
}

func newBench(shards int) *bench {
	rt := shard.New(shards, 8<<20, 1<<18)
	rt.Use(handlers())
	rt.Start()
	return &bench{rt: rt, c: rt.NewConn()}
}

func (b *bench) stop() { b.rt.Stop() }

// do sends one keyed command and spins out its whole reply.
func (b *bench) do(op byte, keyIdx int, a ...string) []byte {
	args := make([][]byte, len(a))
	for i := range a {
		args[i] = []byte(a[i])
	}
	if err := b.c.DoAt(op, keyIdx, args); err != nil {
		panic(err)
	}
	b.c.Flush()
	var rep []byte
	for rep == nil {
		b.c.DrainReplies(func(r []byte) { rep = append([]byte(nil), r...) })
	}
	return rep
}

// keyOn returns a key with the given prefix routed to shard sh.
func (b *bench) keyOn(sh int, prefix string) string {
	for i := 0; ; i++ {
		k := prefix + strconv.Itoa(i)
		if b.rt.ShardOf([]byte(k)) == sh {
			return k
		}
	}
}

// pair returns a src/dst key pair, co-located on shard 0 or split across
// shards 0 and 1.
func (b *bench) pair(cross bool) (string, string) {
	src := b.keyOn(0, "src")
	if cross {
		return src, b.keyOn(1, "dst")
	}
	return src, b.keyOn(0, "dst")
}

// fill loads key with n padding members plus, when withBall, the moved member.
func (b *bench) fill(key string, tag byte, n int, withBall bool) {
	const batch = 200
	a := []string{key}
	if withBall {
		a = append(a, "ball")
	}
	for i := 0; i < n; i++ {
		a = append(a, fmt.Sprintf("%c%015d", tag, i))
		if len(a) > batch {
			b.do(opSadd, 0, a...)
			a = a[:1]
		}
	}
	if len(a) > 1 {
		b.do(opSadd, 0, a...)
	}
}

// smoveCross runs one SMOVE through the intent path, the same route
// dispatchCross takes, and spins out the loopback reply.
func (b *bench) smoveCross(src, dst string) []byte {
	err := b.c.DoTxn([][]byte{[]byte(src), []byte(dst)}, func(t *shard.Txn) []byte {
		return set.SmoveCross(t, []byte(src), []byte(dst), []byte("ball"))
	})
	if err != nil {
		panic(err)
	}
	b.c.Flush()
	var rep []byte
	for rep == nil {
		b.c.DrainReplies(func(r []byte) { rep = append([]byte(nil), r...) })
	}
	return rep
}

// timeCmd runs fn until the floor duration and returns ns per call.
func timeCmd(fn func()) float64 {
	const minDur = 300 * time.Millisecond
	reps := 0
	start := time.Now()
	for time.Since(start) < minDur {
		fn()
		reps++
	}
	return float64(time.Since(start).Nanoseconds()) / float64(reps)
}

// mustMoved asserts the :1 reply, the per-rep correctness cross-check: a :0
// would mean the ping-pong lost the ball and the arms stopped measuring the
// same work.
func mustMoved(rep []byte) {
	if string(rep) != ":1\r\n" {
		panic(fmt.Sprintf("reply %q, want :1", rep))
	}
}

func main() {
	quick := flag.Bool("quick", false, "smaller cardinalities for a fast check")
	flag.Parse()

	cards := []int{8, 1024, 65536}
	shardCounts := []int{2, 8}
	const depth = 16
	if *quick {
		cards = []int{8}
		shardCounts = []int{2}
	}

	fmt.Println("cross-shard SMOVE tax: ping-pong pair (there and back), us per SMOVE")
	fmt.Println("| per-set n | S | co-located us | cross-shard us | tax |")
	fmt.Println("|---|---|---|---|---|")
	for _, s := range shardCounts {
		for _, n := range cards {
			b := newBench(s)
			coSrc, coDst := b.pair(false)
			xSrc, xDst := b.pair(true)
			b.fill(coSrc, 'a', n, true)
			b.fill(coDst, 'b', n, false)
			b.fill(xSrc, 'a', n, true)
			b.fill(xDst, 'b', n, false)

			co := timeCmd(func() {
				mustMoved(b.do(opSmove, 0, coSrc, coDst, "ball"))
				mustMoved(b.do(opSmove, 0, coDst, coSrc, "ball"))
			}) / 2
			cross := timeCmd(func() {
				mustMoved(b.smoveCross(xSrc, xDst))
				mustMoved(b.smoveCross(xDst, xSrc))
			}) / 2
			fmt.Printf("| %d | %d | %.2f | %.2f | %.1fx |\n", n, s, co/1e3, cross/1e3, cross/co)
			b.stop()
		}
	}

	// The pipelined arm: depth commands in flight before the first drain, so
	// the coordinator handoffs overlap. Only the small-set shape; the band
	// work is the same term as above.
	fmt.Println()
	fmt.Printf("pipelined arm, depth %d, per-set n 8: us per SMOVE\n", depth)
	fmt.Println("| S | co-located us | cross-shard us | tax |")
	fmt.Println("|---|---|---|---|")
	for _, s := range shardCounts {
		b := newBench(s)
		coSrc, coDst := b.pair(false)
		xSrc, xDst := b.pair(true)
		b.fill(coSrc, 'a', 8, true)
		b.fill(coDst, 'b', 8, false)
		b.fill(xSrc, 'a', 8, true)
		b.fill(xDst, 'b', 8, false)

		drain := func(want int) {
			got := 0
			for got < want {
				got += b.c.DrainReplies(func(r []byte) { mustMoved(r) })
			}
		}
		co := timeCmd(func() {
			for i := 0; i < depth/2; i++ {
				must(b.c.DoAt(opSmove, 0, args(coSrc, coDst, "ball")))
				must(b.c.DoAt(opSmove, 0, args(coDst, coSrc, "ball")))
			}
			b.c.Flush()
			drain(depth)
		}) / depth
		cross := timeCmd(func() {
			for i := 0; i < depth/2; i++ {
				must(b.c.DoTxn(args(xSrc, xDst), func(t *shard.Txn) []byte {
					return set.SmoveCross(t, []byte(xSrc), []byte(xDst), []byte("ball"))
				}))
				must(b.c.DoTxn(args(xDst, xSrc), func(t *shard.Txn) []byte {
					return set.SmoveCross(t, []byte(xDst), []byte(xSrc), []byte("ball"))
				}))
			}
			b.c.Flush()
			drain(depth)
		}) / depth
		fmt.Printf("| %d | %.2f | %.2f | %.1fx |\n", s, co/1e3, cross/1e3, cross/co)
		b.stop()
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func args(a ...string) [][]byte {
	out := make([][]byte, len(a))
	for i := range a {
		out[i] = []byte(a[i])
	}
	return out
}
