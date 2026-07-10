// Lab: value copies on the GET reply path (spec 2064/f3, M0 gate follow-up,
// lab 9).
//
// The question: the gate profile put readSep plus the reply-arena append at
// about 10 percent of CPU at 4KiB values (9.24s + 3.38s of 121.6s). The GET
// handler read every band through the shard scratch (GetString appends the
// arena bytes into cx.Val) and then Reply.Bulk copied that scratch into the
// reply arena, so a resident value crossed memory twice where redis crosses
// once. The handler runs on the single owner inside the epoch bracket and
// nothing moves arena bytes mid-command, so the resident bands can hand the
// arena view straight to the reply builder. What does dropping the scratch
// copy buy, per value size?
//
// Method: in-process, no server, no wire: the cost under test is exactly
// store read plus resp.AppendBulk into a reused reply buffer, the two calls
// the owner runs per GET. Per value size {64B, 512B, 1KiB, 4KiB}: fill a
// store (1M keys, 256K at 4KiB), then run rounds of 16 appends into a 64KiB
// reply buffer (the P16 batch shape) over uniform-random keys for 2 seconds
// per lane. Lane copy is GetString through a reused scratch; lane view is
// GetView. Cells alternate lanes to keep thermal drift out of the ratio.
//
// See README.md for the numbers and the verdict.
package main

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

const (
	pipeline = 16
	cellDur  = 2 * time.Second
)

// xorshift is the key picker: cheap, stateful, uniform enough for a cache
// benchmark, and identical across lanes.
type xorshift uint64

func (x *xorshift) next() uint64 {
	v := *x
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = v
	return uint64(v)
}

func makeKey(buf []byte, n uint64) []byte {
	binary.LittleEndian.PutUint64(buf[0:8], n)
	binary.LittleEndian.PutUint64(buf[8:16], n*0x9e3779b97f4a7c15)
	return buf[:16]
}

func fill(keys, valLen int) *store.Store {
	rec := 96 + valLen
	s := store.New(keys*rec+keys*rec/4+(16<<20), 0)
	val := make([]byte, valLen)
	for i := range val {
		val[i] = 'a' + byte(i%26)
	}
	var kb [16]byte
	for i := 0; i < keys; i++ {
		if err := s.SetString(makeKey(kb[:], uint64(i)), val, 0, 0, false); err != nil {
			panic(err)
		}
	}
	return s
}

// lane runs GET rounds for cellDur and returns ops/sec. Each round is
// pipeline appends into the reply buffer, then a reset, the hop-batch shape.
func lane(s *store.Store, keys int, view bool) float64 {
	var kb [16]byte
	var scratch []byte
	rep := make([]byte, 0, 64<<10)
	rng := xorshift(0x9e3779b97f4a7c15)
	var ops int64
	start := time.Now()
	for time.Since(start) < cellDur {
		rep = rep[:0]
		for i := 0; i < pipeline; i++ {
			k := makeKey(kb[:], rng.next()%uint64(keys))
			var v []byte
			var ok bool
			if view {
				v, ok = s.GetView(k, 0)
			} else {
				v, ok = s.GetString(k, 0, scratch)
				scratch = v
			}
			if !ok {
				panic("miss")
			}
			rep = resp.AppendBulk(rep, v)
		}
		ops += pipeline
	}
	return float64(ops) / time.Since(start).Seconds()
}

func main() {
	fmt.Println("lab 09: GET reply build, copy-through-scratch vs arena view")
	fmt.Printf("%-8s %14s %14s %8s\n", "value", "copy ops/s", "view ops/s", "view/copy")
	for _, sz := range []int{64, 512, 1024, 4096} {
		keys := 1 << 20
		if sz == 4096 {
			keys = 1 << 18
		}
		s := fill(keys, sz)
		// Warm both lanes once, then measure alternating so drift is shared.
		lane(s, keys, false)
		lane(s, keys, true)
		var copyOps, viewOps float64
		const reps = 3
		for r := 0; r < reps; r++ {
			copyOps += lane(s, keys, false)
			viewOps += lane(s, keys, true)
		}
		copyOps /= reps
		viewOps /= reps
		fmt.Printf("%-8d %14.0f %14.0f %7.2fx\n", sz, copyOps, viewOps, viewOps/copyOps)
	}
}
