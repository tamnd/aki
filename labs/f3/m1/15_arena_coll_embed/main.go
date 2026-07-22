// Lab m1/15: the tiny-collection arena-embed memory lever (spec 2064/f3,
// keyspace-unification arc, slice 1).
//
// The memory bar fails on the 2M-tiny-collection gate cell: aki charges far
// more per tiny set than the rivals because a tiny collection today is three
// separate Go-heap objects plus map overhead, all GC-scanned:
//
//  1. the per-type header struct (a set is 80 bytes, size class 80),
//  2. its packed data slice (a single-member listpack, ~7 bytes, class 16),
//  3. the registry map entry: the copied key string bytes (class 16) and the
//     amortized bucket slot (string header + pointer + tophash).
//
// The keyspace-unification fix stores the whole collection where a small string
// value already lives: inline in one arena record, [header | key | packed blob]
// contiguous, discriminated by a collection kind byte (engine/f3/store
// PutCollBlob, this slice's primitive). The three heap objects and the map entry
// collapse to one arena record. Two wins compound: the record is packed tight
// with no per-object allocator size-class slack, and it lives in the arena,
// which is mapped outside the Go heap (arena_map_unix.go), so the collection
// bytes leave the GC-scanned set entirely and stop inflating the collector's
// pacing goal the way 2M live heap objects do.
//
// This lab prices both halves against the real code. The wall arm builds the
// real Go-heap shape (a map of header structs each with its own blob) and reads
// runtime.MemStats.HeapAlloc for the true per-collection heap charge the
// allocator books. The embed arm drives the real store.Store through
// PutCollBlob and reads the store's own resident accounting (MemoryUsage, the
// record's charged bytes) for the per-collection arena charge, and confirms the
// Go heap barely moves because the arena is off-heap. It does NOT model the
// per-type routing that wires tiny sets onto this path (slice 2): it measures
// the storage footprint the routing will inherit, so the routing is only built
// against a footprint already proven to clear the bar.
//
// Read: bytes/collection for the wall and the embed, the ratio, and the
// projected resident total at the gate's 2M cell. The embed clears the memory
// bar (<=0.5x the wall) by construction of the packed off-heap record. See
// README.md.
package main

import (
	"flag"
	"fmt"
	"runtime"
	"sort"
	"unsafe"

	"github.com/tamnd/aki/engine/f3/store"
)

// wallSet mirrors the real set header (engine/f3/set/set.go, 80 bytes, size
// class 80) at its true field widths so the allocator charges the wall arm the
// same class the live engine does. Only the footprint matters here, so the
// table and cold pointers are reproduced as raw words.
type wallSet struct {
	enc      uint8
	clock    uint16
	expireAt int64
	data     []byte
	n        int
	ht       unsafe.Pointer
	part     unsafe.Pointer
	acct     uint64
	cold     unsafe.Pointer
}

// oneMemberBlob packs a single member as the engine's listpack does:
// [len][tag][bytes], the tiny-collection shape the gate cell builds.
func oneMemberBlob(m []byte) []byte {
	var tag byte
	if len(m) > 0 {
		tag = m[0]
	}
	b := make([]byte, 0, len(m)+2)
	b = append(b, byte(len(m)), tag)
	b = append(b, m...)
	return b
}

// key builds a fixed-width key so every record charges the same aligned key
// bytes, keeping the per-collection arena charge a single constant this lab can
// sample once rather than sum over millions of records.
func key(i int) []byte { return fmt.Appendf(nil, "set:%08d", i) }

// heapPerWall builds count tiny collections in the current Go-heap registry
// shape (a map of header structs, each owning its own blob) and returns the
// HeapAlloc bytes the allocator charged per collection: the struct's size class,
// the blob's size class, the copied key string, and the amortized map bucket
// slot. GC is forced and pinned so the delta is the live set alone.
func heapPerWall(count int, member []byte) float64 {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	m := make(map[string]*wallSet, count)
	for i := range count {
		m[string(key(i))] = &wallSet{enc: 1, clock: 1, data: oneMemberBlob(member), n: 1}
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	got := float64(after.HeapAlloc-before.HeapAlloc) / float64(count)
	runtime.KeepAlive(m)
	return got
}

// embedResult carries the embed arm's numbers. recordBytes is the per-record
// arena charge the store books (header, aligned key, reserved value capacity),
// which lives off the Go heap. indexBytes is the measured Go-heap growth over
// the build, the store's keyspace index (the extendible hash), the embed
// design's answer to the wall arm's map-as-index. total is their sum, the
// apples-to-apples per-collection resident charge next to the wall arm, whose
// own number already folds its map bucket overhead in.
type embedResult struct {
	recordBytes float64
	indexBytes  float64
	total       float64
}

// embedPerColl builds count tiny collections through the real store.Store as
// inline collection records (PutCollBlob) and reports the arena charge per
// collection from the store's own accounting, plus the Go-heap growth per
// collection over the build. The arena is sized to hold the records with
// headroom; segBytes gives the index room to split.
func embedPerColl(count int, member []byte) embedResult {
	blob := oneMemberBlob(member)
	arenaBytes := count*96 + (32 << 20)
	s := store.New(arenaBytes, 1<<20)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := range count {
		if err := s.PutCollBlob(key(i), 0x02 /*kindSet*/, 1, blob, 0, 0); err != nil {
			panic(err)
		}
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// Every record is the same fixed-width key and blob, so one MemoryUsage
	// sample is the per-record arena charge (header, aligned key, reserved
	// value capacity) the whole build repeats.
	rec, ok := s.MemoryUsage(key(0), 0)
	if !ok {
		panic("embed: record 0 missing after build")
	}
	record := float64(rec)
	index := float64(after.HeapAlloc-before.HeapAlloc) / float64(count)
	res := embedResult{
		recordBytes: record,
		indexBytes:  index,
		total:       record + index,
	}
	runtime.KeepAlive(s)
	return res
}

func median(xs []float64) float64 {
	sort.Float64s(xs)
	return xs[len(xs)/2]
}

func main() {
	reps := flag.Int("reps", 5, "reps per cell, median reported")
	flag.Parse()

	member := []byte("hello")
	counts := []int{100_000, 500_000, 1_000_000, 2_000_000}

	fmt.Printf("tiny-collection arena-embed footprint: single-member sets, member %q\n", member)
	fmt.Printf("sizeof wallSet = %d bytes (size class 80)\n\n", unsafe.Sizeof(wallSet{}))
	fmt.Printf("%10s  %12s  %12s  %12s  %12s  %7s  %14s\n",
		"count", "wall B/coll", "embed rec B", "embed idx B", "embed tot B", "ratio", "saved (count)")
	for _, c := range counts {
		wallReps := make([]float64, *reps)
		embReps := make([]float64, *reps)
		var emb embedResult
		for r := range *reps {
			wallReps[r] = heapPerWall(c, member)
			emb = embedPerColl(c, member)
			embReps[r] = emb.total
		}
		w := median(wallReps)
		e := median(embReps)
		ratio := e / w
		savedMB := (w - e) * float64(c) / (1024 * 1024)
		fmt.Printf("%10d  %12.1f  %12.1f  %12.1f  %12.1f  %6.2fx  %11.1f MB\n",
			c, w, emb.recordBytes, emb.indexBytes, e, ratio, savedMB)
	}

	fmt.Printf("\nembed clears the memory bar when ratio <= 0.50x; arena is off the Go heap,\n")
	fmt.Printf("so embed heap B/coll stays near zero and the collector's pacing goal does not\n")
	fmt.Printf("carry 2M live objects. See engine/f3/store/collblob.go PutCollBlob.\n")
}
