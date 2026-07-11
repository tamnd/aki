// Lab: worker donation, live (spec 2064/f3 doc 11 sections 5.4 and 6.5, M1
// lab 09).
//
// Lab 08 priced the partition-parallel merge fan-out with lab-local executors
// and froze fanoutFloor = 65536; the engine now donates the group loop to idle
// workers for real (engine/f3/shard/donate.go). This lab reads the same
// questions through the shipped path: Runtime, Conn, the real set handlers,
// the real member tables, the real donation barrier. Two sweeps.
//
// First, live k-way scaling: SINTERCARD over a partitioned 1M-by-1M pair (P4)
// and a 2M-by-2M pair (P8) across shard counts 1, 2, 4, 8. The one-shard arm
// is the serial oracle: FanOut has no pool there and runs the same group
// tasks inline on the owner, so the row pair is lab 08's seq-versus-pool read
// with the engine's actual barrier in the middle. The donation cap is half
// the pool, so S=8 donates to at most 4 workers plus the coordinating owner.
//
// Second, the escalated draw floor: SRANDMEMBER count over a 2M-member
// escalated set (P8, F13 engaged through the real EscalateHotDraws seam),
// count swept 512 to 32768, one-shard against eight-shard. drawfan.go gates
// the fanned resolve at drawFanFloor = 2048; the sweep reads whether the
// donated arm already pays at the floor (freeze it) or not (raise it). Below
// the floor both arms run the flat serial sample, so those rows should read
// 1.0x and any drift there is measurement noise, not donation cost.
//
// SINTERCARD rather than SINTER carries the merge metric: the count reply
// isolates the merge from the reply encode, which is owner-side buffer work
// donation does not touch. The operands are production-shaped: the real
// 262144-member engagement threshold, so P comes from deriveP, not a lowered
// test seam. Wall-clock per command over a fixed floor duration, counts
// cross-checked against the built overlap every rep.
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
	opSrand
	opEsc
)

func handlers() []shard.Handler {
	h := make([]shard.Handler, opEsc+1)
	h[opSadd] = set.Sadd
	h[opSinter] = set.Sinter
	h[opSintercard] = set.Sintercard
	h[opSrand] = set.Srandmember
	h[opEsc] = func(cx *shard.Ctx, args [][]byte, r shard.Reply) {
		if set.EscalateHotDraws(cx, args[0], 8) {
			r.Int(1)
		} else {
			r.Int(0)
		}
	}
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

// do sends one command keyed at keyIdx and spins out its whole reply, the
// synchronous round trip the sweeps time.
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

// colocated returns count key names routing to one shard, so every operand of
// a multi-key command lives in that owner's registry (the co-location the
// current router assumes, algebra_commands.go).
func (b *bench) colocated(count int) []string {
	want := b.rt.ShardOf([]byte("anchor"))
	keys := []string{"anchor"}
	for i := 0; len(keys) < count; i++ {
		k := fmt.Sprintf("key%d", i)
		if b.rt.ShardOf([]byte(k)) == want {
			keys = append(keys, k)
		}
	}
	return keys
}

// fill loads a set of n fixed 16-byte members: shared members s%015d first
// (identical across operands), then tag-prefixed distinct ones, the lab 08
// member shape pushed through real SADDs in spanCap-safe batches.
func (b *bench) fill(key string, tag byte, shared, n int) {
	const batch = 200
	a := make([]string, 0, batch+1)
	for lo := 0; lo < n; {
		a = append(a[:0], key)
		for len(a) < batch+1 && lo < n {
			if lo < shared {
				a = append(a, fmt.Sprintf("s%015d", lo))
			} else {
				a = append(a, fmt.Sprintf("%c%015d", tag, lo))
			}
			lo++
		}
		b.do(opSadd, 0, a...)
	}
}

// timeCmd runs fn until the floor duration and returns ns per command.
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

// mustInt asserts an integer reply's value, the per-rep correctness
// cross-check.
func mustInt(rep []byte, want int) {
	if string(rep) != fmt.Sprintf(":%d\r\n", want) {
		panic(fmt.Sprintf("reply %q, want :%d", rep, want))
	}
}

func main() {
	quick := flag.Bool("quick", false, "smaller cardinalities for a fast check")
	flag.Parse()
	set.SetAlgebraMaintain(true)

	interNs := []int{1 << 20, 2 << 20}
	drawCard := 2 << 20
	counts := []int{512, 1024, 2048, 4096, 8192, 16384, 32768}
	if *quick {
		interNs = []int{512 << 10}
		drawCard = 0 // the escalated shape needs P8, which needs 2M members
		counts = nil
	}

	fmt.Println("live k-way scaling: SINTERCARD, overlap 0.5, production threshold")
	fmt.Println("| per-side n | S | ms/cmd | vs S=1 |")
	fmt.Println("|---|---|---|---|")
	for _, n := range interNs {
		shared := n / 2
		var base float64
		for _, s := range []int{1, 2, 4, 8} {
			b := newBench(s)
			keys := b.colocated(2)
			a, bb := keys[0], keys[1]
			b.fill(a, 'a', shared, n)
			b.fill(bb, 'b', shared, n)
			ns := timeCmd(func() {
				mustInt(b.do(opSintercard, 1, "2", a, bb), shared)
			})
			if s == 1 {
				base = ns
			}
			fmt.Printf("| %d | %d | %.2f | %.2fx |\n", n, s, ns/1e6, base/ns)
			b.stop()
		}
	}

	if drawCard == 0 {
		return
	}
	fmt.Println()
	fmt.Println("escalated draw floor: SRANDMEMBER count, 2M-member set, P8, F13 engaged")
	fmt.Println("| count | inline us (S=1) | donated us (S=8) | donated/inline |")
	fmt.Println("|---|---|---|---|")
	arms := make(map[int]map[int]float64) // shards -> count -> ns
	for _, s := range []int{1, 8} {
		b := newBench(s)
		key := b.colocated(1)[0]
		b.fill(key, 'a', 0, drawCard)
		mustInt(b.do(opEsc, 0, key), 1)
		arms[s] = make(map[int]float64)
		for _, want := range counts {
			w := strconv.Itoa(want)
			arms[s][want] = timeCmd(func() {
				rep := b.do(opSrand, 0, key, w)
				if len(rep) < want {
					panic("short draw reply")
				}
			})
		}
		b.stop()
	}
	for _, want := range counts {
		in, dn := arms[1][want], arms[8][want]
		fmt.Printf("| %d | %.1f | %.1f | %.2f |\n", want, in/1e3, dn/1e3, dn/in)
	}
}
