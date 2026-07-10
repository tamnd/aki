// Lab: inline value threshold (spec 2064/f3/09 section 2, M0 lab 5).
//
// The question: at what value size does storing the value inline in the
// record stop beating an out-of-record pointer? Doc 09's band ladder embeds
// values up to str_inline_max (default 1024B) and moves bigger ones behind a
// 16-byte value pointer; this lab measures both shapes on the small end of
// the ladder, where the choice is live, by sweeping value sizes 8B to 512B.
//
// Method: two variants over the real engine/f3/store. The inline variant is
// the store as shipped: Set embeds the value bytes in the record, Get copies
// them out. The out-of-line variant keeps the value bytes in a lab-side value
// log (one bump-allocated slab, standing in for the separated-run arena the
// value-band slice will add) and stores a 16-byte pointer (offset u64, length
// u32, pad) as the record's value; a Get probes the store, decodes the
// pointer, and copies from the slab, and a Set probes for the pointer and
// rewrites the run in place, which is doc 09's mutable-region fast path and
// the fairest shape for a same-size overwrite. The engine is not modified.
//
// Per (variant, size) cell: fill 1M keys (16B keys), then time 4M uniform
// pre-shuffled GETs and 1M uniform SETs, and read bytes-per-key off the arena
// (plus the slab's live run bytes for the out-of-line variant).
//
// See README.md for the numbers and the verdict.
package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

const (
	nKeys = 1 << 20 // 1M keys
	nGets = 4 << 20
	nSets = 1 << 20
	ptrSz = 16
)

func keyBytes(dst []byte, i uint64) []byte {
	x := i + 0x9e3779b97f4a7c15
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	dst = dst[:0]
	for j := 0; j < 16; j++ {
		dst = append(dst, "0123456789abcdef"[(x>>(j*4))&15])
	}
	return dst
}

func align8(n int) int { return (n + 7) &^ 7 }

// slab is the lab's stand-in for the separated-value arena: bump allocation,
// no free, run addresses are plain offsets.
type slab struct {
	buf  []byte
	next int
}

func (s *slab) alloc(n int) int {
	off := s.next
	s.next += align8(n)
	return off
}

type cell struct {
	getNs, setNs   float64
	bytesPerKey    float64
	arenaBytesUsed uint64
}

func newOrder(n, keys int, seed int64) []uint32 {
	rng := rand.New(rand.NewSource(seed))
	ord := make([]uint32, n)
	for i := range ord {
		ord[i] = uint32(rng.Intn(keys))
	}
	return ord
}

func runInline(valSize int) cell {
	arenaBytes := nKeys*(32+align8(valSize)+16) + 64<<20
	s := store.New(arenaBytes, 0)
	val := make([]byte, valSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	key := make([]byte, 0, 16)
	for i := 0; i < nKeys; i++ {
		if err := s.Set(keyBytes(key, uint64(i)), val); err != nil {
			panic(err)
		}
	}
	used, _ := s.ArenaBytes()

	getOrd := newOrder(nGets, nKeys, 1)
	dst := make([]byte, 0, valSize)
	t0 := time.Now()
	for _, k := range getOrd {
		if _, ok := s.Get(keyBytes(key, uint64(k)), dst); !ok {
			panic("miss")
		}
	}
	getNs := float64(time.Since(t0).Nanoseconds()) / nGets

	setOrd := newOrder(nSets, nKeys, 2)
	t0 = time.Now()
	for _, k := range setOrd {
		if err := s.Set(keyBytes(key, uint64(k)), val); err != nil {
			panic(err)
		}
	}
	setNs := float64(time.Since(t0).Nanoseconds()) / nSets

	return cell{getNs, setNs, float64(used) / nKeys, used}
}

func runOutOfLine(valSize int) cell {
	arenaBytes := nKeys*(32+ptrSz+16) + 64<<20
	s := store.New(arenaBytes, 0)
	vlog := &slab{buf: make([]byte, nKeys*align8(valSize)+1<<20)}
	val := make([]byte, valSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	ptr := make([]byte, ptrSz)
	key := make([]byte, 0, 16)
	for i := 0; i < nKeys; i++ {
		off := vlog.alloc(valSize)
		copy(vlog.buf[off:], val)
		binary.LittleEndian.PutUint64(ptr, uint64(off))
		binary.LittleEndian.PutUint32(ptr[8:], uint32(valSize))
		if err := s.Set(keyBytes(key, uint64(i)), ptr); err != nil {
			panic(err)
		}
	}
	used, _ := s.ArenaBytes()
	slabLive := uint64(vlog.next)

	getOrd := newOrder(nGets, nKeys, 1)
	dst := make([]byte, 0, valSize)
	pbuf := make([]byte, 0, ptrSz)
	t0 := time.Now()
	for _, k := range getOrd {
		p, ok := s.Get(keyBytes(key, uint64(k)), pbuf)
		if !ok {
			panic("miss")
		}
		off := binary.LittleEndian.Uint64(p)
		n := binary.LittleEndian.Uint32(p[8:])
		dst = append(dst[:0], vlog.buf[off:off+uint64(n)]...)
	}
	getNs := float64(time.Since(t0).Nanoseconds()) / nGets
	if len(dst) != valSize {
		panic("bad read")
	}

	// Same-size overwrite: fetch the pointer, rewrite the run in place (the
	// doc 09 mutable-region path). No republish, no fresh run.
	setOrd := newOrder(nSets, nKeys, 2)
	t0 = time.Now()
	for _, k := range setOrd {
		p, ok := s.Get(keyBytes(key, uint64(k)), pbuf)
		if !ok {
			panic("miss")
		}
		off := binary.LittleEndian.Uint64(p)
		copy(vlog.buf[off:off+uint64(valSize)], val)
	}
	setNs := float64(time.Since(t0).Nanoseconds()) / nSets

	return cell{getNs, setNs, float64(used+slabLive) / nKeys, used}
}

func main() {
	sizes := []int{8, 16, 32, 64, 128, 256, 512}
	fmt.Printf("keys=%d gets=%d sets=%d key=16B uniform\n\n", nKeys, nGets, nSets)
	fmt.Println("| value | inline GET ns | oob GET ns | inline SET ns | oob SET ns | inline B/key | oob B/key |")
	fmt.Println("|---|---|---|---|---|---|---|")
	for _, sz := range sizes {
		in := runInline(sz)
		oo := runOutOfLine(sz)
		fmt.Printf("| %dB | %.1f | %.1f | %.1f | %.1f | %.0f | %.0f |\n",
			sz, in.getNs, oo.getNs, in.setNs, oo.setNs, in.bytesPerKey, oo.bytesPerKey)
		_ = os.Stdout.Sync()
	}
}
