// Lab: prefetch depth (spec 2064/f3/03 section 3.4 and 04 section 2, M0 lab 4).
//
// The question: how far ahead should the batch drain prefetch index buckets?
// Doc 03's drain is two-stage (prefetch all buckets, then complete probes
// while prefetching record lines) and the survey's convergent answer is
// depth 16 (Redis 8.4's lookahead default); doc 04 pre-registers <= 8ns
// amortized probes under depth-16 batches (LAB-2). This lab sweeps the
// prefetch distance over 0, 2, 4, 8, 16 on a stream of random probes and
// measures amortized ns/probe at 1M and 10M keys.
//
// Method: the probe target is a lab-local clone of the ported index bucket
// shape, one flat power-of-two array of the exact 64-byte bucket (7
// tag-plus-address entry words and a link word, tags from the same bit range
// store uses) over records in a flat arena (16B header, 16B key, 64B value),
// keyed by the ported store.Hash. The clone exists because the sweep needs
// raw bucket and record addresses to feed the prefetch instruction, which
// the store rightly does not export; the layout constants match
// engine/f3/store/index.go and a store.Get baseline row anchors the clone
// against the real ported engine on the same keys. The probe loop is a
// software pipeline at distance d: hash key i and prefetch its bucket,
// complete the bucket probe for key i-d and prefetch its record lines, and
// finish the record read (key compare plus value touch) for key i-2d, so
// both dependent misses of a point probe overlap across d in-flight probes.
// d=0 is the fully serial baseline. Prefetch is a real PRFM/PREFETCHT0 via
// a one-instruction asm shim.
//
// See README.md for the numbers and the verdict.
package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"time"
	"unsafe"

	"github.com/tamnd/aki/engine/f3/store"
)

const (
	valBytes = 64
	keyBytes = 16
	recSize  = 16 + keyBytes + valBytes // header + key + value, 96B
	nProbes  = 1 << 23

	slotsPerBucket = 7
	tagShift       = 52 // entries are tag<<52 | arena offset, as in the ported index
)

type bucket struct {
	slots [slotsPerBucket]uint64
	link  uint64 // overflow slab index + 1, 0 = none
}

// table is the lab clone of the index: flat buckets, flat overflow slab,
// records appended to a flat arena.
type table struct {
	buckets  []bucket
	overflow []bucket
	arena    []byte
	mask     uint64
}

func keyOf(dst []byte, i uint64) []byte {
	x := i + 0x9e3779b97f4a7c15
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	dst = dst[:0]
	for j := 0; j < keyBytes; j++ {
		dst = append(dst, "0123456789abcdef"[(x>>(j*4))&15])
	}
	return dst
}

func tagOf(h uint64) uint64 { return (h >> tagShift) | 1 }

func newTable(nKeys int) *table {
	nb := uint64(1)
	for nb*slotsPerBucket < uint64(nKeys)*4/3 { // target load under 75 percent
		nb <<= 1
	}
	t := &table{
		buckets: make([]bucket, nb),
		arena:   make([]byte, 0, nKeys*recSize),
		mask:    nb - 1,
	}
	key := make([]byte, 0, keyBytes)
	val := make([]byte, valBytes)
	for i := 0; i < nKeys; i++ {
		k := keyOf(key, uint64(i))
		off := uint64(len(t.arena))
		var hdr [16]byte
		hdr[0] = keyBytes
		hdr[1] = valBytes
		t.arena = append(t.arena, hdr[:]...)
		t.arena = append(t.arena, k...)
		t.arena = append(t.arena, val...)
		t.insert(store.Hash(k), off)
	}
	return t
}

func (t *table) insert(h, off uint64) {
	word := tagOf(h)<<tagShift | off
	b := &t.buckets[h&t.mask]
	for {
		for i := range b.slots {
			if b.slots[i] == 0 {
				b.slots[i] = word
				return
			}
		}
		if b.link == 0 {
			t.overflow = append(t.overflow, bucket{})
			b.link = uint64(len(t.overflow))
		}
		b = &t.overflow[b.link-1]
	}
}

// probeBucket resolves hash h to a record offset using only bucket lines,
// returning ^uint64(0) on a clean miss. Tag-only resolution: a 12-bit tag hit
// is almost always the record, and the pipelined caller verifies the key
// bytes at record stage, exactly the two-phase split of the drain; the rare
// tag collision falls back to probeFull.
func (t *table) probeBucket(h uint64) uint64 {
	tag := tagOf(h)
	b := &t.buckets[h&t.mask]
	for {
		for i := range b.slots {
			w := b.slots[i]
			if w != 0 && w>>tagShift == tag {
				return w & (1<<tagShift - 1)
			}
		}
		if b.link == 0 {
			return ^uint64(0)
		}
		b = &t.overflow[b.link-1]
	}
}

// probeFull is the collision fallback: resolve with the key compare inline,
// as store.findEntry does, scanning past same-tag impostors.
func (t *table) probeFull(h uint64, key []byte) uint64 {
	tag := tagOf(h)
	b := &t.buckets[h&t.mask]
	for {
		for i := range b.slots {
			w := b.slots[i]
			if w != 0 && w>>tagShift == tag && t.keyAt(w&(1<<tagShift-1), key) {
				return w & (1<<tagShift - 1)
			}
		}
		if b.link == 0 {
			panic("probe miss")
		}
		b = &t.overflow[b.link-1]
	}
}

func (t *table) keyAt(off uint64, key []byte) bool {
	rec := t.arena[off:]
	for i := 0; i < keyBytes; i++ {
		if rec[16+i] != key[i] {
			return false
		}
	}
	return true
}

// readRecord verifies the key bytes and touches the value, the record-line
// half of the probe. A tag collision (wrong record behind a matching tag)
// re-resolves through probeFull, the same shape as the drain rechecking a
// prefetched probe.
func (t *table) readRecord(off uint64, h uint64, key []byte) byte {
	if !t.keyAt(off, key) {
		off = t.probeFull(h, key)
	}
	return t.arena[off+16+keyBytes]
}

// sweep runs nProbes random probes at pipeline distance d and returns
// amortized ns/probe. d=0 is serial: hash, probe, read, one probe at a time.
func (t *table) sweep(ids []uint32, d int) float64 {
	const ringSize = 64 // > 2*max distance, power of two
	var hRing [ringSize]uint64
	var offRing [ringSize]uint64
	var keyRing [ringSize][keyBytes]byte
	var sink byte

	t0 := time.Now()
	if d == 0 {
		key := make([]byte, 0, keyBytes)
		for _, id := range ids {
			k := keyOf(key, uint64(id))
			h := store.Hash(k)
			off := t.probeBucket(h)
			sink ^= t.readRecord(off, h, k)
		}
	} else {
		n := len(ids)
		for i := 0; i < n+2*d; i++ {
			if i < n {
				k := keyRing[i%ringSize][:0]
				k = keyOf(k, uint64(ids[i]))
				h := store.Hash(k)
				hRing[i%ringSize] = h
				prefetch(unsafe.Pointer(&t.buckets[h&t.mask]))
			}
			if j := i - d; j >= 0 && j < n {
				off := t.probeBucket(hRing[j%ringSize])
				offRing[j%ringSize] = off
				prefetch(unsafe.Pointer(&t.arena[off]))
				prefetch(unsafe.Pointer(&t.arena[off+64]))
			}
			if k := i - 2*d; k >= 0 && k < n {
				sink ^= t.readRecord(offRing[k%ringSize], hRing[k%ringSize], keyRing[k%ringSize][:])
			}
		}
	}
	el := time.Since(t0)
	runtime.KeepAlive(sink)
	return float64(el.Nanoseconds()) / float64(len(ids))
}

// storeBaseline measures the ported store's serial Get on the same keys, the
// anchor row proving the clone probes at engine speed.
func storeBaseline(nKeys int, ids []uint32) float64 {
	s := store.New(nKeys*160+64<<20, 0)
	key := make([]byte, 0, keyBytes)
	val := make([]byte, valBytes)
	for i := 0; i < nKeys; i++ {
		if err := s.Set(keyOf(key, uint64(i)), val); err != nil {
			panic(err)
		}
	}
	dst := make([]byte, 0, valBytes)
	t0 := time.Now()
	for _, id := range ids {
		var ok bool
		dst, ok = s.Get(keyOf(key, uint64(id)), dst)
		if !ok {
			panic("miss")
		}
	}
	return float64(time.Since(t0).Nanoseconds()) / float64(len(ids))
}

func main() {
	fmt.Printf("cores=%d probes=%d key=%dB val=%dB rec=%dB\n",
		runtime.NumCPU(), nProbes, keyBytes, valBytes, recSize)
	for _, nKeys := range []int{1 << 20, 10 << 20} {
		rng := rand.New(rand.NewSource(int64(nKeys)))
		ids := make([]uint32, nProbes)
		for i := range ids {
			ids[i] = uint32(rng.Intn(nKeys))
		}
		t := newTable(nKeys)
		load := float64(nKeys) / float64(len(t.buckets)*slotsPerBucket)
		fmt.Printf("\nkeys=%d buckets=%d load=%.2f overflow=%d\n\n",
			nKeys, len(t.buckets), load, len(t.overflow))
		fmt.Println("| distance | ns/probe | vs serial |")
		fmt.Println("|---|---|---|")
		var serial float64
		for _, d := range []int{0, 2, 4, 8, 16} {
			ns := t.sweep(ids, d)
			if d == 0 {
				serial = ns
			}
			fmt.Printf("| %d | %.1f | %.2fx |\n", d, ns, serial/ns)
		}
		fmt.Printf("| store.Get serial | %.1f | anchor |\n", storeBaseline(nKeys, ids))
	}
}
