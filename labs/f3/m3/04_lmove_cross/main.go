// Lab: the cross-shard LMOVE hop cost (spec 2064/f3 doc 03 section 6.7, doc 13
// section 5, M3 lab 04).
//
// Slice 7 routes LMOVE and RPOPLPUSH onto the F17 intent path when the source
// and destination keys span shards. The transaction holds an intent on both
// keys, then the move runs as owner hops under the barrier: the source hop peeks
// its end element and clones it, the destination hop pushes the clone, and a
// final source hop removes the moved element and drops an emptied source. The
// co-located case never comes near this path; dispatch keeps it on the
// owner-local point handler slice 6 shipped. This lab prices the difference.
//
// Where the gather (lab 11) copies every member of every remote operand, so its
// tax rises with the data, the move is the opposite by construction: an LMOVE
// touches exactly one element no matter how deep the two lists are. So the
// question is whether the cross tax is flat in the data, the way lab 10 found
// the cross-shard SMOVE flat, and what the fixed price of the three-hop remote
// path is in absolute us. That price is the whole cost the design adds, so name
// it honestly.
//
// Method: one runtime, one synchronous connection. The co-located arm places
// both keys on one shard and runs the Lmove point handler; the cross arm places
// each key on its own shard and runs LmoveCross under DoTxn, the exact route
// dispatch takes. Each timed call moves one element and the next call moves it
// back, so both lists hold their depth across the whole run and never drain, and
// the number measured is the steady per-move cost. The element size and the list
// depth sweep the bands (inline listpack and native chunked deque); a second
// table sweeps the four LMOVE directions at one band to show the hop cost does
// not depend on which ends the move touches.
package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/engine/f3/list"
	"github.com/tamnd/aki/engine/f3/shard"
)

const (
	opRpush byte = iota + 1
	opLmove
)

func handlers() []shard.Handler {
	h := make([]shard.Handler, opLmove+1)
	h[opRpush] = list.Rpush
	h[opLmove] = list.Lmove
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

// fill loads key with depth elements of the given byte size, so the co-located
// and cross-shard arms move over lists of the same shape.
func (b *bench) fill(key string, depth, elemBytes int) {
	body := strings.Repeat("x", elemBytes)
	for i := 0; i < depth; i++ {
		b.do(opRpush, 0, key, fmt.Sprintf("%06d:", i)+body)
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

// coMove runs one co-located LMOVE src dst RIGHT LEFT through the point handler.
func (b *bench) coMove(src, dst string) {
	b.do(opLmove, 0, src, dst, "RIGHT", "LEFT")
}

// crossMove runs one cross-shard LMOVE src dst under DoTxn, the dispatch route.
func (b *bench) crossMove(src, dst string, srcLeft, dstLeft bool) {
	b.txn([]string{src, dst}, func(t *shard.Txn) []byte {
		return list.LmoveCross(t, []byte(src), []byte(dst), srcLeft, dstLeft)
	})
}

func main() {
	quick := flag.Bool("quick", false, "smaller sweep for a fast check")
	flag.Parse()

	bands := []struct {
		name      string
		elemBytes int
		depth     int
	}{
		{"inline small", 8, 16},
		{"inline wide", 200, 16},
		{"native", 100, 400},
	}
	if *quick {
		bands = bands[:1]
	}

	fmt.Println("cross-shard LMOVE hop cost: one element moved per call, us per move")
	fmt.Println("| band | element bytes | depth | co-located us | cross-shard us | tax |")
	fmt.Println("|---|---|---|---|---|---|")
	for _, bd := range bands {
		b := newBench(8)
		coSrc := b.keyOn(0, "cosrc_")
		coDst := b.keyOn(0, "codst_") // co arm: both on shard 0
		xSrc := b.keyOn(0, "xsrc_")
		xDst := b.keyOn(1, "xdst_") // cross arm: shard 0 and shard 1
		b.fill(coSrc, bd.depth, bd.elemBytes)
		b.fill(coDst, bd.depth, bd.elemBytes)
		b.fill(xSrc, bd.depth, bd.elemBytes)
		b.fill(xDst, bd.depth, bd.elemBytes)

		coFwd := true
		coNs := timeCmd(func() {
			if coFwd {
				b.coMove(coSrc, coDst)
			} else {
				b.coMove(coDst, coSrc)
			}
			coFwd = !coFwd
		})
		xFwd := true
		crossNs := timeCmd(func() {
			if xFwd {
				b.crossMove(xSrc, xDst, false, true)
			} else {
				b.crossMove(xDst, xSrc, false, true)
			}
			xFwd = !xFwd
		})
		fmt.Printf("| %s | %d | %d | %.2f | %.2f | %.1fx |\n", bd.name, bd.elemBytes, bd.depth, coNs/1e3, crossNs/1e3, crossNs/coNs)
		b.stop()
	}

	// The four LMOVE directions at one band: the move touches a different pair of
	// ends each time, but the hop count is the same three, so the cross cost should
	// not move with the direction. A flat column here is the read that the tax is
	// the barrier and the hops, not the end work.
	fmt.Println()
	fmt.Println("cross-shard LMOVE by direction, inline band (8B x 16), us per move")
	fmt.Println("| direction | co-located us | cross-shard us | tax |")
	fmt.Println("|---|---|---|---|")
	dirs := []struct {
		name             string
		from, to         string
		srcLeft, dstLeft bool
	}{
		{"LEFT LEFT", "LEFT", "LEFT", true, true},
		{"LEFT RIGHT", "LEFT", "RIGHT", true, false},
		{"RIGHT LEFT", "RIGHT", "LEFT", false, true},
		{"RIGHT RIGHT", "RIGHT", "RIGHT", false, false},
	}
	for _, dir := range dirs {
		b := newBench(8)
		coSrc := b.keyOn(0, "dcosrc_")
		coDst := b.keyOn(0, "dcodst_")
		xSrc := b.keyOn(0, "dxsrc_")
		xDst := b.keyOn(1, "dxdst_")
		for _, k := range []string{coSrc, coDst, xSrc, xDst} {
			b.fill(k, 16, 8)
		}
		coFwd := true
		coNs := timeCmd(func() {
			if coFwd {
				b.do(opLmove, 0, coSrc, coDst, dir.from, dir.to)
			} else {
				b.do(opLmove, 0, coDst, coSrc, dir.from, dir.to)
			}
			coFwd = !coFwd
		})
		xFwd := true
		crossNs := timeCmd(func() {
			if xFwd {
				b.crossMove(xSrc, xDst, dir.srcLeft, dir.dstLeft)
			} else {
				b.crossMove(xDst, xSrc, dir.srcLeft, dir.dstLeft)
			}
			xFwd = !xFwd
		})
		fmt.Printf("| %s | %.2f | %.2f | %.1fx |\n", dir.name, coNs/1e3, crossNs/1e3, crossNs/coNs)
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
