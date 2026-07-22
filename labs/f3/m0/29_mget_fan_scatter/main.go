// Lab: MGET fan-out scatter allocation elision (spec 2064/f3 doc 03 section 6.2,
// M0 lab 29).
//
// The question: a multi-key command (MGET, MSET) is a tier-one fan-out. The
// reader routes each key to its owning shard and splits the command into
// per-shard sub-commands that all carry one reply sequence. The original
// scatter (engine/f3/shard/fan.go DoFan) built that split by allocating a fresh
// routing slice, a fresh sub-command list, and a fresh argv plus positions
// buffer for every shard, every command. On the M0-G10 gate cell MGET lands at
// 1.93x redis, a near-miss, and under the gate's GOGC=20 those per-command
// allocations are exactly the kind of churn that keeps a throughput row under
// 2x: an MGET of 16 keys across 8 shards allocated on the order of twenty small
// objects the single-threaded rival never pays.
//
// The elision: route once into a reader-owned scratch slice, count the
// sub-commands in a cheap allocation-free pass so the coordinator countdown is
// still final before the first enqueue, then scatter out of one reused argv and
// one reused positions buffer. enqueueFan's node builder copies every argument's
// bytes into its own span table synchronously, so both scratch buffers are safe
// to reset and reuse the moment the enqueue returns. After a warmup the scatter
// allocates nothing per command.
//
// This lab prices the two scatter strategies against each other: it replays the
// exact shard grouping and per-sub cap chunking of DoFan over a synthetic key
// set and reports allocations per command and nanoseconds per command for each,
// so the churn the elision removes is visible as both. It also asserts the two
// strategies emit an identical flat sub-command sequence (same shard, same keys
// in argument order, same MGET position blob), since a scatter change is only
// safe if every owner still receives the same slice of the command.
//
// Method: in-process, no server, no wire, no engine import. The two scatter
// shapes are reproduced verbatim from fan.go (the make-per-command build and the
// reused-scratch build), and the enqueue is modelled by the same byte copy
// b.add does into a node's span table (a reused sink buffer), so the consumer
// side allocates nothing and only the scatter's own churn is measured.
//
// Axes: keys per command {2, 8, 16, 64} (the MGET batch the gate rides is 16),
// shards {8} (the gate shard count), kind {MGET, MSET}. Read: allocs/command and
// ns/command for both strategies and the elision's win. See README.md for the
// sweep and the frozen verdict.
package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"testing"
)

// maxFanKeys and the per-sub byte cap mirror fan.go: a sub-command's argument
// run must fit a node's span table, so a shard with more than the caps allow
// gets several sub-commands.
const maxFanKeys = 100

// shardOf routes a key the way the runtime does for the purpose of this lab: a
// stable hash folded onto the shard count. The exact hash does not matter, only
// that keys spread across the shards the way the gate's 1M-key MGET does.
func shardOf(key []byte, shards int) int {
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(shards))
}

// sink models the node span table enqueueFan's b.add copies each argument into:
// the bytes are copied out synchronously, which is what makes the reader's argv
// and pos scratch safe to reuse right after the enqueue. Reset per command so it
// never grows unbounded and never allocates on the steady state.
type sink struct {
	data  []byte
	spans int
}

func (s *sink) reset() { s.data = s.data[:0]; s.spans = 0 }
func (s *sink) add(argv [][]byte) {
	for _, a := range argv {
		s.data = append(s.data, a...)
		s.spans++
	}
}

// scatterOld is the original fan.go scatter: a fresh order slice, a fresh subs
// list, and fresh per-shard argv and pos buffers, every command. dataCap is the
// per-sub byte cap.
func scatterOld(dst *sink, keys, vals [][]byte, shards, dataCap int, mget bool) {
	order := make([]int, len(keys))
	for i, k := range keys {
		order[i] = shardOf(k, shards)
	}
	type fanSub struct {
		sh   int
		argv [][]byte
	}
	var subs []fanSub
	for sh := range shards {
		var argv [][]byte
		var pos []byte
		kn := 0
		bytes := 0
		flushSub := func() {
			if kn == 0 {
				return
			}
			if mget {
				argv = append(argv, pos)
			}
			subs = append(subs, fanSub{sh: sh, argv: argv})
			argv = nil
			pos = nil
			kn = 0
			bytes = 0
		}
		for i := range keys {
			if order[i] != sh {
				continue
			}
			if kn > 0 && (kn >= maxFanKeys || bytes > dataCap) {
				flushSub()
			}
			argv = append(argv, keys[i])
			bytes += len(keys[i])
			if vals != nil {
				argv = append(argv, vals[i])
				bytes += len(vals[i])
			}
			if mget {
				pos = binary.LittleEndian.AppendUint16(pos, uint16(i))
				bytes += 2
			}
			kn++
		}
		flushSub()
	}
	for _, sub := range subs {
		dst.add(sub.argv)
	}
}

// scatterer holds the reader-owned scratch the elided scatter reuses across
// commands: the routing slice, the argv buffer, and the MGET positions buffer.
type scatterer struct {
	order []int
	argv  [][]byte
	pos   []byte
}

// countSubs replays the scatter's grouping and chunking without allocating, so
// the coordinator countdown is final before the first enqueue.
func countSubs(keys, vals [][]byte, order []int, shards, dataCap int, mget bool) int {
	subs := 0
	for sh := range shards {
		kn := 0
		bytes := 0
		for i := range keys {
			if order[i] != sh {
				continue
			}
			if kn > 0 && (kn >= maxFanKeys || bytes > dataCap) {
				subs++
				kn, bytes = 0, 0
			}
			bytes += len(keys[i])
			if vals != nil {
				bytes += len(vals[i])
			}
			if mget {
				bytes += 2
			}
			kn++
		}
		if kn > 0 {
			subs++
		}
	}
	return subs
}

// scatterNew is the elided scatter: route once into reused scratch, count the
// subs allocation-free, then enqueue each sub out of one reused argv and one
// reused pos buffer. Returns the sub count so a caller can check it matches.
func (s *scatterer) scatterNew(dst *sink, keys, vals [][]byte, shards, dataCap int, mget bool) int {
	order := s.order[:0]
	for _, k := range keys {
		order = append(order, shardOf(k, shards))
	}
	s.order = order

	pending := countSubs(keys, vals, order, shards, dataCap, mget)

	argv := s.argv[:0]
	pos := s.pos[:0]
	for sh := range shards {
		kn := 0
		bytes := 0
		for i := range keys {
			if order[i] != sh {
				continue
			}
			if kn > 0 && (kn >= maxFanKeys || bytes > dataCap) {
				// Append the position blob in this scope so a growth of the
				// reused argv backing persists across subs instead of
				// reallocating every flush, matching fan.go's inlined scatter.
				if mget {
					argv = append(argv, pos)
				}
				dst.add(argv)
				argv, pos = argv[:0], pos[:0]
				kn, bytes = 0, 0
			}
			argv = append(argv, keys[i])
			bytes += len(keys[i])
			if vals != nil {
				argv = append(argv, vals[i])
				bytes += len(vals[i])
			}
			if mget {
				pos = binary.LittleEndian.AppendUint16(pos, uint16(i))
				bytes += 2
			}
			kn++
		}
		if kn > 0 {
			if mget {
				argv = append(argv, pos)
			}
			dst.add(argv)
			argv, pos = argv[:0], pos[:0]
		}
	}
	s.argv, s.pos = argv, pos
	return pending
}

// recSink records the exact per-sub sequence each scatter emits: one framed
// entry per enqueued sub-command, holding the sub's arguments in order. Two
// scatters are equivalent only when they hand every owner the same slice of the
// command, so a byte-identical recording is the safety check.
type recSink struct {
	buf []byte
}

func (r *recSink) add(argv [][]byte) {
	r.buf = append(r.buf, '[')
	for _, a := range argv {
		r.buf = binary.LittleEndian.AppendUint32(r.buf, uint32(len(a)))
		r.buf = append(r.buf, a...)
	}
	r.buf = append(r.buf, ']')
}

// verify asserts the reused-scratch scatter emits the identical flat sub-command
// sequence the make-per-command scatter does, over the same key set.
func verify(keys, vals [][]byte, shards int, mget bool) error {
	var oldRec, newRec recSink
	scatterOldRec(&oldRec, keys, vals, shards, perSubCap, mget)
	scatterNewRec(&newRec, keys, vals, shards, perSubCap, mget)
	if string(oldRec.buf) != string(newRec.buf) {
		return fmt.Errorf("scatter mismatch (keys=%d mget=%v): reused scratch diverged from make-per-command", len(keys), mget)
	}
	return nil
}

// scatterOldRec and scatterNewRec mirror the two scatters but push each sub into
// a recSink instead of the byte sink, so the sub sequences can be compared.
func scatterOldRec(dst *recSink, keys, vals [][]byte, shards, dataCap int, mget bool) {
	order := make([]int, len(keys))
	for i, k := range keys {
		order[i] = shardOf(k, shards)
	}
	for sh := range shards {
		var argv [][]byte
		var pos []byte
		kn, bytes := 0, 0
		flushSub := func() {
			if kn == 0 {
				return
			}
			if mget {
				argv = append(argv, pos)
			}
			dst.add(argv)
			argv, pos, kn, bytes = nil, nil, 0, 0
		}
		for i := range keys {
			if order[i] != sh {
				continue
			}
			if kn > 0 && (kn >= maxFanKeys || bytes > dataCap) {
				flushSub()
			}
			argv = append(argv, keys[i])
			bytes += len(keys[i])
			if vals != nil {
				argv = append(argv, vals[i])
				bytes += len(vals[i])
			}
			if mget {
				pos = binary.LittleEndian.AppendUint16(pos, uint16(i))
				bytes += 2
			}
			kn++
		}
		flushSub()
	}
}

func scatterNewRec(dst *recSink, keys, vals [][]byte, shards, dataCap int, mget bool) {
	sc := &scatterer{}
	order := sc.order[:0]
	for _, k := range keys {
		order = append(order, shardOf(k, shards))
	}
	argv := sc.argv[:0]
	pos := sc.pos[:0]
	for sh := range shards {
		kn, bytes := 0, 0
		for i := range keys {
			if order[i] != sh {
				continue
			}
			if kn > 0 && (kn >= maxFanKeys || bytes > dataCap) {
				flushRec(dst, argv, pos, mget)
				argv, pos, kn, bytes = argv[:0], pos[:0], 0, 0
			}
			argv = append(argv, keys[i])
			bytes += len(keys[i])
			if vals != nil {
				argv = append(argv, vals[i])
				bytes += len(vals[i])
			}
			if mget {
				pos = binary.LittleEndian.AppendUint16(pos, uint16(i))
				bytes += 2
			}
			kn++
		}
		if kn > 0 {
			flushRec(dst, argv, pos, mget)
			argv, pos = argv[:0], pos[:0]
		}
	}
}

func flushRec(dst *recSink, argv [][]byte, pos []byte, mget bool) {
	if mget {
		argv = append(argv, pos)
	}
	dst.add(argv)
}

// synthKeys builds n distinct keys shaped like the gate's key space.
func synthKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key:%012d", i)
	}
	return keys
}

func synthVals(n, size int) [][]byte {
	vals := make([][]byte, n)
	v := make([]byte, size)
	for i := range v {
		v[i] = 'v'
	}
	for i := range vals {
		vals[i] = v
	}
	return vals
}

// perSubCap is the per-sub byte cap; the gate runs batch-data-cap 1024.
const perSubCap = 1024

func main() {
	fmt.Println("MGET/MSET fan-out scatter: per-command allocation elision")
	fmt.Printf("%-6s %-5s %8s  %14s %14s   %10s %10s\n",
		"kind", "keys", "shards", "old alloc/op", "new alloc/op", "old ns/op", "new ns/op")

	type row struct {
		kind string
		mget bool
		keys int
	}
	rows := []row{
		{"MGET", true, 2}, {"MGET", true, 8}, {"MGET", true, 16}, {"MGET", true, 64},
		{"MSET", false, 2}, {"MSET", false, 8}, {"MSET", false, 16}, {"MSET", false, 64},
	}
	const shards = 8
	for _, r := range rows {
		keys := synthKeys(r.keys)
		var vals [][]byte
		if !r.mget {
			vals = synthVals(r.keys, 64)
		}

		var dst sink
		oldRes := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				dst.reset()
				scatterOld(&dst, keys, vals, shards, perSubCap, r.mget)
			}
		})

		sc := &scatterer{}
		newRes := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				dst.reset()
				sc.scatterNew(&dst, keys, vals, shards, perSubCap, r.mget)
			}
		})

		if err := verify(keys, vals, shards, r.mget); err != nil {
			panic(err)
		}

		fmt.Printf("%-6s %-5d %8d  %14d %14d   %10d %10d\n",
			r.kind, r.keys, shards,
			oldRes.AllocsPerOp(), newRes.AllocsPerOp(),
			oldRes.NsPerOp(), newRes.NsPerOp())
	}
}
