// Lab: the cross-shard blocking serve cost (spec 2064/f3 doc 13 M3 slice 8, PR 6,
// issue #545).
//
// PR 6 lets a blocking list verb block across shards. BLPOP, BRPOP, and BLMPOP
// park a waiter on several owners at once and let whichever owner's push or
// timeout wins a shared atomic claim serve it, then cancel the losers. BLMOVE and
// BRPOPLPUSH block on the one source but reach a destination on another shard: the
// serving push cannot push there itself, so it spawns a coordinator that acquires
// both keys and runs the move under a fresh barrier. The co-located forms never do
// either; a push serves them inline on the one owner. This lab prices the two new
// serve paths.
//
// The immediate-serve cost, when a blocking verb finds an element already there
// and never parks, is the ordered scan under the DoBlockCross barrier, which is
// the same barrier-and-hops tax lab 04 already priced for the non-blocking cross
// move. What PR 6 adds and no earlier lab covers is the parked serve: the claim CAS
// plus the cancel fan-out for a pop, and the spawned two-owner coordinator for a
// move. So this lab measures the parked path: park a blocker across shards, then
// wake it with a push on a far owner, and time the wake-to-reply per served waiter,
// against the co-located inline serve.
//
// Method: one runtime, the list handlers. A burst of blockers park on empty keys,
// each on its own connection, then the lab sleeps briefly so every one is
// registered before any push lands (so this measures a real park-then-wake, never
// an accidental immediate serve). A pusher then feeds the source one element at a
// time; each push serves the FIFO-head blocker, and the lab drains that blocker's
// reply before the next push. The pusher round trip is identical on both arms, so
// the co-located-to-cross delta is the cross serve overhead alone. A second table
// sweeps the pop across key counts to show the cancel fan-out cost, the one term
// that grows with the number of owners a waiter parked on.
//
// macOS has no raw futex, so a cross serve that hops to another owner rides a Go
// channel wake; the absolute cell is higher than the gate box will show (a Linux
// futex wake is a ~1-2us syscall path), but the co-to-cross delta this lab freezes
// is a difference measured on the same transport, so it carries. See README.md for
// the numbers and the frozen verdict.
package main

import (
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/tamnd/aki/engine/f3/list"
	"github.com/tamnd/aki/engine/f3/shard"
)

const (
	opRpush byte = iota + 1
	opLrange
	opBlpop
	opBlmove
	opBrpoplpush
)

func handlers() []shard.Handler {
	h := make([]shard.Handler, opBrpoplpush+1)
	h[opRpush] = list.Rpush
	h[opLrange] = list.Lrange
	h[opBlpop] = list.Blpop
	h[opBlmove] = list.Blmove
	h[opBrpoplpush] = list.Brpoplpush
	return h
}

// bench is one runtime under measurement.
type bench struct {
	rt     *shard.Runtime
	shards int
}

func newBench(shards int) *bench {
	rt := shard.New(shards, 8<<20, 1<<18)
	rt.Use(handlers())
	rt.Start()
	return &bench{rt: rt, shards: shards}
}

func (b *bench) stop() { b.rt.Stop() }

func bytesOf(a []string) [][]byte {
	out := make([][]byte, len(a))
	for i := range a {
		out[i] = []byte(a[i])
	}
	return out
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

// push sends one RPUSH and drains its own reply, so the serve it triggers has run
// by the time it returns.
func (b *bench) push(c *shard.Conn, key, val string) {
	if err := c.DoAt(opRpush, 0, bytesOf([]string{key, val})); err != nil {
		panic(err)
	}
	c.Flush()
	b.drain(c)
}

// drain spins one whole reply off the connection.
func (b *bench) drain(c *shard.Conn) {
	for {
		got := false
		c.DrainReplies(func([]byte) { got = true })
		if got {
			return
		}
	}
}

// parkPopCross parks one cross-shard BLPOP on keys (all empty) through
// DoBlockCross, the dispatch route, and returns without waiting.
func (b *bench) parkPopCross(c *shard.Conn, keys []string) {
	tail := bytesOf(append(append([]string{}, keys...), "0"))
	err := c.DoBlockCross(bytesOf(keys), func(tx *shard.Txn, conn *shard.Conn, seq uint32) []byte {
		return list.BlpopCross(tx, conn, seq, tail)
	})
	if err != nil {
		panic(err)
	}
	c.ArmBlock()
	c.Flush()
}

// parkPopCo parks one co-located BLPOP on keys through the point handler.
func (b *bench) parkPopCo(c *shard.Conn, keys []string) {
	tail := bytesOf(append(append([]string{}, keys...), "0"))
	if err := c.DoAt(opBlpop, 0, tail); err != nil {
		panic(err)
	}
	c.Flush()
}

// parkMoveCross parks one cross-shard BLMOVE (or BRPOPLPUSH) on src, destination
// dst on another shard, through DoBlockCross.
func (b *bench) parkMoveCross(c *shard.Conn, src, dst string, rpoplpush bool) {
	if rpoplpush {
		tail := bytesOf([]string{src, dst, "0"})
		must(c.DoBlockCross(bytesOf([]string{src, dst}), func(tx *shard.Txn, conn *shard.Conn, seq uint32) []byte {
			return list.BrpoplpushCross(tx, conn, seq, tail)
		}))
	} else {
		tail := bytesOf([]string{src, dst, "LEFT", "RIGHT", "0"})
		must(c.DoBlockCross(bytesOf([]string{src, dst}), func(tx *shard.Txn, conn *shard.Conn, seq uint32) []byte {
			return list.BlmoveCross(tx, conn, seq, tail)
		}))
	}
	c.ArmBlock()
	c.Flush()
}

// parkMoveCo parks one co-located BLMOVE (or BRPOPLPUSH) through the point handler,
// src and dst on the one shard.
func (b *bench) parkMoveCo(c *shard.Conn, src, dst string, rpoplpush bool) {
	var tail [][]byte
	op := byte(opBlmove)
	if rpoplpush {
		op = opBrpoplpush
		tail = bytesOf([]string{src, dst, "0"})
	} else {
		tail = bytesOf([]string{src, dst, "LEFT", "RIGHT", "0"})
	}
	if err := c.DoAt(op, 0, tail); err != nil {
		panic(err)
	}
	c.Flush()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// settle is the pause after the last park, so every blocker is registered on its
// owner before the first push lands and the measurement is a real park-then-wake.
const settle = 30 * time.Millisecond

// servePop parks n cross or co-located BLPOP blockers on nkeys keys, then times
// serving them one push at a time and returns ns per served waiter.
func servePop(shards, n, nkeys int, cross bool) float64 {
	b := newBench(shards)
	defer b.stop()
	keys := make([]string, nkeys)
	for j := range keys {
		sh := 0
		if cross {
			sh = j % shards
		}
		keys[j] = b.keyOn(sh, fmt.Sprintf("pk%d_%d_", nkeys, j))
	}
	conns := make([]*shard.Conn, n)
	for i := range conns {
		conns[i] = b.rt.NewConn()
		if cross {
			b.parkPopCross(conns[i], keys)
		} else {
			b.parkPopCo(conns[i], keys)
		}
	}
	time.Sleep(settle)
	pusher := b.rt.NewConn()
	start := time.Now()
	for i := 0; i < n; i++ {
		b.push(pusher, keys[0], "v")
		b.drain(conns[i])
	}
	return float64(time.Since(start).Nanoseconds()) / float64(n)
}

// serveMove parks n cross or co-located BLMOVE (or BRPOPLPUSH) blockers on one
// source, each with its own destination, then times serving them one push at a
// time and returns ns per served waiter.
func serveMove(shards, n int, cross, rpoplpush bool) float64 {
	b := newBench(shards)
	defer b.stop()
	src := b.keyOn(0, "src_")
	conns := make([]*shard.Conn, n)
	dsts := make([]string, n)
	for i := range conns {
		conns[i] = b.rt.NewConn()
		dstShard := 0
		if cross {
			dstShard = 1
		}
		dsts[i] = b.keyOn(dstShard, fmt.Sprintf("dst%d_", i))
		if cross {
			b.parkMoveCross(conns[i], src, dsts[i], rpoplpush)
		} else {
			b.parkMoveCo(conns[i], src, dsts[i], rpoplpush)
		}
	}
	time.Sleep(settle)
	pusher := b.rt.NewConn()
	start := time.Now()
	for i := 0; i < n; i++ {
		b.push(pusher, src, "v")
		b.drain(conns[i])
	}
	return float64(time.Since(start).Nanoseconds()) / float64(n)
}

// minRuns returns the smallest result of f over reps runs. A parked serve rides a
// Go channel wake, and macOS scheduling adds a few microseconds of jitter that only
// ever inflates a sample, so the minimum is the cleanest estimate of the real serve
// floor and is stable run to run where a single sample swings by more than the tax
// it is trying to show.
func minRuns(reps int, f func() float64) float64 {
	best := f()
	for i := 1; i < reps; i++ {
		if v := f(); v < best {
			best = v
		}
	}
	return best
}

func main() {
	quick := flag.Bool("quick", false, "smaller burst for a fast check")
	flag.Parse()

	const shards = 8
	n, reps := 2000, 7
	if *quick {
		n, reps = 200, 3
	}

	fmt.Println("cross-shard blocking serve cost: park a blocker, wake it with a push, us per served waiter (min of", reps, "runs)")
	fmt.Println("| verb | co-located us | cross-shard us | serve tax us |")
	fmt.Println("|---|---|---|---|")
	rows := []struct {
		name string
		fn   func() (co, cross float64)
	}{
		{"BLPOP (2 keys)", func() (float64, float64) {
			return minRuns(reps, func() float64 { return servePop(shards, n, 2, false) }) / 1e3,
				minRuns(reps, func() float64 { return servePop(shards, n, 2, true) }) / 1e3
		}},
		{"BLMOVE", func() (float64, float64) {
			return minRuns(reps, func() float64 { return serveMove(shards, n, false, false) }) / 1e3,
				minRuns(reps, func() float64 { return serveMove(shards, n, true, false) }) / 1e3
		}},
		{"BRPOPLPUSH", func() (float64, float64) {
			return minRuns(reps, func() float64 { return serveMove(shards, n, false, true) }) / 1e3,
				minRuns(reps, func() float64 { return serveMove(shards, n, true, true) }) / 1e3
		}},
	}
	for _, r := range rows {
		co, cross := r.fn()
		fmt.Printf("| %s | %.2f | %.2f | %.2f |\n", r.name, co, cross, cross-co)
	}

	fmt.Println()
	fmt.Println("cross-shard BLPOP serve by key count: the cancel fan-out over the owners a waiter parked on (min of", reps, "runs)")
	fmt.Println("| keys | cross-shard us | over 2-key us |")
	fmt.Println("|---|---|---|")
	base := minRuns(reps, func() float64 { return servePop(shards, n, 2, true) }) / 1e3
	for _, nk := range []int{2, 4, 8} {
		us := minRuns(reps, func() float64 { return servePop(shards, n, nk, true) }) / 1e3
		fmt.Printf("| %d | %.2f | %+.2f |\n", nk, us, us-base)
	}
}
