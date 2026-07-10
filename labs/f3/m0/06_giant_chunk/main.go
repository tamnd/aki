// Lab: giant-value chunk threshold and chunk size (spec 2064/f3/09 sections
// 2 and 5, M0 lab 6).
//
// The question: doc 09 chunks values at and above str_chunk_min = 64KiB into
// str_chunk_size = 64KiB chunks behind a 16B-per-chunk directory, so that a
// windowed write (SETRANGE, APPEND) costs the touched chunks, never the value
// (the L11 bound). Chunking is not free: every full read and write pays a
// directory walk and per-chunk dispatch, and the chunk size sets the price of
// one windowed write. Where do those costs actually land, per value size and
// chunk size, on this substrate?
//
// Method: values 64KiB to 8MiB, chunk sizes 16KiB to 1MiB (chunk <= value),
// plus a whole-run row per value size (one unchunked slab run, the shape a
// higher chunk threshold would keep). Chunk bytes live in a lab-side bump
// slab standing in for the value log; the real engine/f3/store holds one
// directory record per key (16 bytes per chunk: run offset u64, length u32,
// CRC slot u32). The engine is not modified. Per cell, over ~256MiB of
// values: a full write pass (chunk copies in, directory built, one Set), a
// full read pass in shuffled key order (Get directory, then stream every
// chunk through one reusable chunk-sized buffer, the F19 shape that never
// assembles the value), and 256 SETRANGE-shaped 100B windowed writes at
// random offsets (copy-on-write of the touched chunk plus a directory
// republish; the whole-run row rewrites the full value, which is the point).
// Go heap allocations across the read pass are read from runtime.MemStats.
//
// See README.md for the numbers and the verdict.
package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

const (
	totalBytes = 256 << 20
	nWindows   = 256
	windowLen  = 100
	entrySize  = 16
)

func align8(n int) int { return (n + 7) &^ 7 }

type slab struct {
	buf  []byte
	next int
}

func (s *slab) alloc(n int) int {
	off := s.next
	s.next += align8(n)
	if s.next > len(s.buf) {
		panic("slab full")
	}
	return off
}

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

type result struct {
	writeGBs, readGBs float64
	windowUs          float64
	dirBytes          int
	readAllocs        uint64
}

// runCell measures one (value size, chunk size) cell. chunk == val is the
// whole-run shape: one run, a one-entry directory, and a windowed write that
// has no choice but to rewrite the full value.
func runCell(valSize, chunkSize int) result {
	nVals := totalBytes / valSize
	if nVals < 4 {
		nVals = 4
	}
	nChunks := (valSize + chunkSize - 1) / chunkSize
	dirSize := nChunks * entrySize

	// Cap the windowed-write count so its COW churn stays bounded at the big
	// chunk sizes (a whole-run 8MiB row would otherwise churn 2GB of slab).
	nW := nWindows
	if nW*chunkSize > totalBytes {
		nW = totalBytes / chunkSize
	}

	vlog := &slab{buf: make([]byte, nVals*valSize+nW*(chunkSize+16)+(16<<20))}
	s := store.New(nVals*(64+dirSize)+nW*(dirSize+64)+(64<<20), 0)

	// Pre-fault the slab so page zeroing is not charged to whichever pass
	// touches a page first; the cells stay comparable.
	for i := 0; i < len(vlog.buf); i += 4096 {
		vlog.buf[i] = 1
	}

	src := make([]byte, valSize)
	for i := range src {
		src[i] = byte(i)
	}
	dir := make([]byte, 0, dirSize)
	key := make([]byte, 0, 16)

	// Write pass: chunks into the slab, directory into the store.
	t0 := time.Now()
	for v := 0; v < nVals; v++ {
		dir = dir[:0]
		for c := 0; c < nChunks; c++ {
			lo := c * chunkSize
			hi := min(lo+chunkSize, valSize)
			off := vlog.alloc(hi - lo)
			copy(vlog.buf[off:], src[lo:hi])
			var e [entrySize]byte
			binary.LittleEndian.PutUint64(e[0:], uint64(off))
			binary.LittleEndian.PutUint32(e[8:], uint32(hi-lo))
			dir = append(dir, e[:]...)
		}
		if err := s.Set(keyBytes(key, uint64(v)), dir); err != nil {
			panic(err)
		}
	}
	writeSec := time.Since(t0).Seconds()

	// Read pass: shuffled key order, every chunk streamed through one
	// reusable buffer, never the whole value in one allocation.
	order := rand.New(rand.NewSource(1)).Perm(nVals)
	chunkBuf := make([]byte, chunkSize)
	dbuf := make([]byte, 0, dirSize)
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	t0 = time.Now()
	var sink byte
	for _, v := range order {
		d, ok := s.Get(keyBytes(key, uint64(v)), dbuf)
		if !ok {
			panic("miss")
		}
		for e := 0; e < len(d); e += entrySize {
			off := binary.LittleEndian.Uint64(d[e:])
			n := binary.LittleEndian.Uint32(d[e+8:])
			copy(chunkBuf, vlog.buf[off:off+uint64(n)])
			sink ^= chunkBuf[0]
		}
	}
	readSec := time.Since(t0).Seconds()
	runtime.ReadMemStats(&m1)

	// Windowed writes: 100B at a random offset. COW the touched chunk (or
	// chunks, when the window straddles a boundary), republish the directory.
	rng := rand.New(rand.NewSource(2))
	t0 = time.Now()
	for w := 0; w < nW; w++ {
		v := rng.Intn(nVals)
		wOff := rng.Intn(valSize - windowLen)
		d, ok := s.Get(keyBytes(key, uint64(v)), dbuf)
		if !ok {
			panic("miss")
		}
		dbuf = d
		for c := wOff / chunkSize; c <= (wOff+windowLen-1)/chunkSize; c++ {
			e := c * entrySize
			old := binary.LittleEndian.Uint64(dbuf[e:])
			n := int(binary.LittleEndian.Uint32(dbuf[e+8:]))
			fresh := vlog.alloc(n)
			copy(vlog.buf[fresh:fresh+n], vlog.buf[old:old+uint64(n)])
			lo := max(wOff-c*chunkSize, 0)
			hi := min(lo+windowLen, n)
			for b := lo; b < hi; b++ {
				vlog.buf[fresh+b] = 0x5a
			}
			binary.LittleEndian.PutUint64(dbuf[e:], uint64(fresh))
		}
		if err := s.Set(keyBytes(key, uint64(v)), dbuf); err != nil {
			panic(err)
		}
	}
	windowSec := time.Since(t0).Seconds()
	_ = sink

	bytes := float64(nVals) * float64(valSize)
	return result{
		writeGBs:   bytes / writeSec / (1 << 30),
		readGBs:    bytes / readSec / (1 << 30),
		windowUs:   windowSec / float64(nW) * 1e6,
		dirBytes:   dirSize,
		readAllocs: m1.Mallocs - m0.Mallocs,
	}
}

func human(n int) string {
	if n >= 1<<20 {
		return fmt.Sprintf("%dMiB", n>>20)
	}
	return fmt.Sprintf("%dKiB", n>>10)
}

func main() {
	valSizes := []int{64 << 10, 256 << 10, 1 << 20, 4 << 20, 8 << 20}
	chunkSizes := []int{16 << 10, 64 << 10, 256 << 10, 1 << 20}
	fmt.Printf("total=%dMiB per cell, windows=%d x %dB, key=16B\n\n",
		totalBytes>>20, nWindows, windowLen)
	fmt.Println("| value | chunk | write GB/s | read GB/s | 100B window us | dir B/value | read allocs |")
	fmt.Println("|---|---|---|---|---|---|---|")
	for _, v := range valSizes {
		for _, c := range chunkSizes {
			if c >= v {
				break
			}
			r := runCell(v, c)
			fmt.Printf("| %s | %s | %.1f | %.1f | %.1f | %d | %d |\n",
				human(v), human(c), r.writeGBs, r.readGBs, r.windowUs, r.dirBytes, r.readAllocs)
			_ = os.Stdout.Sync()
		}
		r := runCell(v, v)
		fmt.Printf("| %s | whole | %.1f | %.1f | %.1f | %d | %d |\n",
			human(v), r.writeGBs, r.readGBs, r.windowUs, r.dirBytes, r.readAllocs)
		_ = os.Stdout.Sync()
	}
}
