// Lab: the cross-shard set-algebra gather tax (spec 2064/f3 doc 03 section
// 6.7, doc 11 section 6, M1 lab 11).
//
// Slice 3's second half routes SINTER, SUNION, SDIFF, and SINTERCARD onto the
// F17 intent path when their operands span shards. The transaction holds an
// intent on every operand, then one hop per remote shard clones that shard's
// operands into plain heap sets and one hop on the first key's owner resolves
// its own operands zero-copy and runs the same driver the co-located handler
// runs. The co-located case never comes near the gather; dispatch keeps it on
// the owner-local point path #620 shipped. This lab prices the difference.
//
// Where lab 10 found SMOVE's cross-shard tax flat in the data (a two-key move
// touches a bounded amount of member data no matter the set size), the gather
// is the opposite by construction: cloning a remote operand copies every one
// of its members. So the question is not whether the tax is flat, it is how
// steep the per-remote-member slope is, and whether the fixed barrier cost
// still dominates at the small sizes the operands usually are.
//
// Method: one runtime, one synchronous connection. The co-located arm places
// K operands on one shard and runs the Sinter point handler; the cross arm
// places each operand on its own shard and runs SinterCross under DoTxn, the
// exact route dispatch takes. Every operand is filled with the same members,
// so the intersection is the whole set: the driver does its maximum probe
// work and the clone copies its maximum member count, the worst case for the
// gather and the cleanest read of the clone slope. Operand cardinality sweeps
// the bands; the operand count K sweeps the number of remote hops the gather
// pays. A second table pins n and sweeps K alone, isolating the per-remote-
// operand hop cost. SINTERCARD with an early LIMIT reads how much of the tax
// is the clone versus the compute.
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
	opSinter
	opSintercard
)

func handlers() []shard.Handler {
	h := make([]shard.Handler, opSintercard+1)
	h[opSadd] = set.Sadd
	h[opSinter] = set.Sinter
	h[opSintercard] = set.Sintercard
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

// txn runs body under DoTxn over keys and spins out the loopback reply.
func (b *bench) txn(keys []string, body func(t *shard.Txn) []byte) []byte {
	if err := b.c.DoTxn(bytesOf(keys), body); err != nil {
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

// operands returns k operand keys. Co-located ones all land on shard 0; cross
// ones land on shards 0, 1, 2, ... one per operand, so the gather pays one
// remote hop per operand past the first.
func (b *bench) operands(k int, cross bool) []string {
	keys := make([]string, k)
	for i := 0; i < k; i++ {
		sh := 0
		if cross {
			sh = i
		}
		keys[i] = b.keyOn(sh, fmt.Sprintf("op%d_", i))
	}
	return keys
}

// fill loads key with members m0..m(n-1). Every operand gets the same members,
// so the intersection is the whole set and both the driver probe and the clone
// copy run at full width.
func (b *bench) fill(key string, n int) {
	const batch = 200
	a := []string{key}
	for i := 0; i < n; i++ {
		a = append(a, "m"+strconv.Itoa(i))
		if len(a) > batch {
			b.do(opSadd, 0, a...)
			a = a[:1]
		}
	}
	if len(a) > 1 {
		b.do(opSadd, 0, a...)
	}
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

func main() {
	quick := flag.Bool("quick", false, "smaller sweep for a fast check")
	flag.Parse()

	cards := []int{8, 1024, 16384}
	operandCounts := []int{2, 4}
	if *quick {
		cards = []int{8}
		operandCounts = []int{2}
	}

	fmt.Println("cross-shard SINTER gather tax: full-overlap operands, us per SINTER")
	fmt.Println("| K operands | per-operand n | co-located us | cross-shard us | tax |")
	fmt.Println("|---|---|---|---|---|")
	for _, k := range operandCounts {
		for _, n := range cards {
			b := newBench(8)
			co := b.operands(k, false)
			cross := b.operands(k, true)
			for _, key := range append(append([]string{}, co...), cross...) {
				b.fill(key, n)
			}
			coNs := timeCmd(func() { b.do(opSinter, 0, co...) })
			crossNs := timeCmd(func() {
				b.txn(cross, func(t *shard.Txn) []byte { return set.SinterCross(t, bytesOf(cross)) })
			})
			fmt.Printf("| %d | %d | %.2f | %.2f | %.1fx |\n", k, n, coNs/1e3, crossNs/1e3, crossNs/coNs)
			b.stop()
		}
	}

	// Per-remote-operand hop: hold n small so the clone copy is cheap and the
	// tax is dominated by the fixed per-remote-shard hop, then sweep K. A flat
	// per-operand increment here is the barrier hop; the slope over the table
	// above minus this is the clone copy.
	fmt.Println()
	fmt.Println("per-remote-operand hop, per-operand n 8, us per SINTER")
	fmt.Println("| K operands | co-located us | cross-shard us | tax |")
	fmt.Println("|---|---|---|---|")
	kSweep := []int{2, 3, 4, 6, 8}
	if *quick {
		kSweep = []int{2, 4}
	}
	for _, k := range kSweep {
		b := newBench(8)
		co := b.operands(k, false)
		cross := b.operands(k, true)
		for _, key := range append(append([]string{}, co...), cross...) {
			b.fill(key, 8)
		}
		coNs := timeCmd(func() { b.do(opSinter, 0, co...) })
		crossNs := timeCmd(func() {
			b.txn(cross, func(t *shard.Txn) []byte { return set.SinterCross(t, bytesOf(cross)) })
		})
		fmt.Printf("| %d | %.2f | %.2f | %.1fx |\n", k, coNs/1e3, crossNs/1e3, crossNs/coNs)
		b.stop()
	}

	// Clone versus compute: SINTERCARD LIMIT 1 stops the driver after one
	// member, so the compute term collapses while the clone still copies every
	// remote operand in full. What is left of the cross tax at large n is the
	// clone, not the intersection walk.
	fmt.Println()
	fmt.Println("SINTERCARD LIMIT 1, K 4, us per call (compute collapsed, clone full)")
	fmt.Println("| per-operand n | co-located us | cross-shard us | tax |")
	fmt.Println("|---|---|---|---|")
	for _, n := range cards {
		b := newBench(8)
		co := b.operands(4, false)
		cross := b.operands(4, true)
		for _, key := range append(append([]string{}, co...), cross...) {
			b.fill(key, n)
		}
		coArgs := append([]string{"4"}, co...)
		coArgs = append(coArgs, "LIMIT", "1")
		xArgs := append([]string{"4"}, cross...)
		xArgs = append(xArgs, "LIMIT", "1")
		coNs := timeCmd(func() { b.do(opSintercard, 1, coArgs...) })
		crossNs := timeCmd(func() {
			b.txn(cross, func(t *shard.Txn) []byte { return set.SintercardCross(t, bytesOf(xArgs)) })
		})
		fmt.Printf("| %d | %.2f | %.2f | %.1fx |\n", n, coNs/1e3, crossNs/1e3, crossNs/coNs)
		b.stop()
	}
}

func bytesOf(a []string) [][]byte {
	out := make([][]byte, len(a))
	for i := range a {
		out[i] = []byte(a[i])
	}
	return out
}
