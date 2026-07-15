// Lab: resident-footprint bound of the whole-record cold migrator, small keys
// past a resident cap versus a rival that holds every key in RAM (spec
// 2064/f3/06 sections 2.4 and 8.1, M7 slice 1 lab 01).
//
// The question: the residency hand (resid.go) bounds the separated band by
// spilling a cold value's run to the value log while its record stays resident.
// It cannot touch the int and embedded bands, whose value bytes live inside the
// record. A workload of many small keys therefore pins the whole record set in
// the arena, and once admission parks the fill at the cap the store cannot take
// a new key. The whole-record migrator (migrate.go) is the missing valve: it
// demotes the coldest whole records to the shard's cold region on disk, so the
// arena fill tracks the cap while the index still names every key. Redis and
// Valkey have no such tier: every key sits in RAM with its dict entry, its
// object header, and its SDS strings for as long as it is live.
//
// This lab prices the resulting memory bar. aki's resident cost is the index (an
// entry per key, resident or cold) plus a bounded arena (the working set that
// fits the cap); the cold values live on disk and cost no RAM. The rival's cost
// is every key, whole, in RAM. The claim the slice bakes in is that past the cap
// aki's RAM tracks the index plus the cap, not the dataset, so a small-key set
// far past the cap uses far less memory, flooring at the index share. It also
// makes the dataset admissible at all: without the migrator the arena alone
// ErrFulls once the fill reaches the cap.
//
// Method: in-process, no server, no wire, no engine import, the lab-local model
// the other f3 labs use. The record geometry (16-byte header, 8-byte alignment,
// the int cell and embedded value bands) and the cold-frame header match the
// store, so aki's resident figures are the store's. The index share is charged
// conservatively high (a full 8-byte slot at half load) and the rival's per-key
// overhead conservatively low (a bare dict entry, object header, and SDS
// header), so every rounding is against aki's win, not for it.
package main

import (
	"flag"
	"fmt"
	"time"
)

const (
	hdrSize = 16 // the record header: ver, vlen, klen, vcap, kind, flags
	intCell = 8  // the V_INT value cell
	coldHdr = 12 // the cold frame header: total, kind, flags, klen, vlen

	// indexBytesPerKey charges the extendible-hash index a full 8-byte entry
	// word at a half-full load factor, so a cold key still costs its slot. This
	// overstates the index (real load runs denser), which only ever adds to
	// aki's side of the ledger.
	indexBytesPerKey = 16

	// rivalPerKey is the rival's fixed per-key overhead: a dict entry (key
	// pointer, value pointer, chain next), a value object header, and one SDS
	// string header, without the allocator size-class rounding a real redis
	// pays. It understates the rival, so aki's win here is a floor.
	rivalPerKey = 48
)

func align8(n int) int { return (n + 7) &^ 7 }

// recordBytes is one resident record's arena charge: header, 8-aligned key, and
// the value cell chosen by band (the 8-byte int cell, or the 8-aligned embedded
// value).
func recordBytes(klen, vlen int, isInt bool) int {
	vc := intCell
	if !isInt {
		vc = align8(vlen)
	}
	return hdrSize + align8(klen) + vc
}

// coldFrameBytes is one demoted record's cold-region frame: header, key, value.
// These bytes live on disk, not in RAM, which is the whole point of the tier.
func coldFrameBytes(klen, vlen int) int {
	return coldHdr + klen + vlen
}

// footprint is a workload's memory split across the two engines.
type footprint struct {
	residentRecs int
	coldRecs     int
	akiRAM       int64 // index (all keys) plus the bounded arena (resident recs)
	akiDisk      int64 // cold frames, on disk, no RAM cost
	rivalRAM     int64 // every key, whole, in RAM
	admissible   bool  // does the dataset fit without the migrator (arena <= cap)
}

// model computes the footprint of n keys of the given key and value size under a
// resident cap. The migrator keeps the arena fill at the cap: the resident
// record count is what fits, the rest are cold on disk. Without the migrator the
// arena would have to hold every record, so admissibility is whether that fits
// the cap.
func model(n, klen, vlen int, isInt bool, cap int64) footprint {
	rec := int64(recordBytes(klen, vlen, isInt))
	residentRecs := int(cap / rec)
	if residentRecs > n {
		residentRecs = n
	}
	coldRecs := n - residentRecs
	akiRAM := int64(n)*indexBytesPerKey + int64(residentRecs)*rec
	akiDisk := int64(coldRecs) * int64(coldFrameBytes(klen, vlen))
	rivalRAM := int64(n) * int64(rivalPerKey+klen+vlen)
	return footprint{
		residentRecs: residentRecs,
		coldRecs:     coldRecs,
		akiRAM:       akiRAM,
		akiDisk:      akiDisk,
		rivalRAM:     rivalRAM,
		admissible:   int64(n)*rec <= cap,
	}
}

// floor is the ratio aki/rival converges to as n grows without bound: the index
// share dominates aki (the bounded arena is a constant), so the limit is the
// per-key index cost over the rival's per-key cost.
func floor(klen, vlen int) float64 {
	return float64(indexBytesPerKey) / float64(rivalPerKey+klen+vlen)
}

func main() {
	quick := flag.Bool("quick", false, "smaller counts for a fast check")
	flag.Parse()

	fmt.Printf("cold-migrator resident footprint, small keys past a cap vs an all-RAM rival, %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("record header %d B, int cell %d B, cold frame header %d B, index %d B/key, rival %d B/key overhead\n",
		hdrSize, intCell, coldHdr, indexBytesPerKey, rivalPerKey)

	// Sweep A: a fixed 64 MiB cap and a small embedded value, rising key count.
	// While the whole set fits the cap the two track (aki also holds it all in
	// the arena); once the set crosses the cap the migrator moves the overflow to
	// disk and aki's RAM tracks the index plus the cap while the rival keeps
	// growing. The "noMig" column is where the arena alone would ErrFull.
	fmt.Println()
	fmt.Println("Sweep A: 64 MiB cap, 16-byte key, 8-byte embedded value, rising key count")
	fmt.Printf("%-12s %10s %10s %12s %12s %12s %10s %7s\n", "keys", "resident", "cold", "akiRAM", "akiDisk", "rivalRAM", "aki/rival", "noMig")
	cap := int64(64) << 20
	counts := []int{100_000, 1_000_000, 4_000_000, 16_000_000, 64_000_000}
	if *quick {
		counts = []int{100_000, 4_000_000, 64_000_000}
	}
	for _, n := range counts {
		f := model(n, 16, 8, false, cap)
		mig := "ok"
		if !f.admissible {
			mig = "ErrFull"
		}
		fmt.Printf("%-12d %10d %10d %12s %12s %12s %10.4f %7s\n",
			n, f.residentRecs, f.coldRecs, human(f.akiRAM), human(f.akiDisk), human(f.rivalRAM), ratio(f.akiRAM, f.rivalRAM), mig)
	}

	// Sweep B: fixed 16M keys and a 64 MiB cap, rising value size. A larger value
	// pushes more bytes to disk on demotion and costs the rival more RAM per key,
	// so the win deepens with value size until the working set of resident records
	// shrinks to a handful.
	fmt.Println()
	fmt.Println("Sweep B: 16M keys, 64 MiB cap, 16-byte key, rising value size")
	fmt.Printf("%-10s %10s %12s %12s %10s %10s\n", "valueLen", "resident", "akiRAM", "rivalRAM", "aki/rival", "floor")
	for _, vl := range []int{8, 32, 128, 512} {
		n := 16_000_000
		f := model(n, 16, vl, false, cap)
		fmt.Printf("%-10d %10d %12s %12s %10.4f %10.4f\n",
			vl, f.residentRecs, human(f.akiRAM), human(f.rivalRAM), ratio(f.akiRAM, f.rivalRAM), floor(16, vl))
	}

	// Sweep C: fixed 16M keys and value, rising cap. A larger working set is
	// resident, so aki's RAM rises toward the rival, but every byte of value the
	// cap cannot hold is a byte the rival keeps and aki does not. The cap is the
	// RAM knob.
	fmt.Println()
	fmt.Println("Sweep C: 16M keys, 16-byte key, 8-byte value, rising cap")
	fmt.Printf("%-10s %10s %10s %12s %10s\n", "cap", "resident", "coldFrac", "akiRAM", "aki/rival")
	for _, c := range []int64{16 << 20, 64 << 20, 256 << 20, 1 << 30} {
		n := 16_000_000
		f := model(n, 16, 8, false, c)
		coldFrac := float64(f.coldRecs) / float64(n)
		fmt.Printf("%-10s %10d %9.1f%% %12s %10.4f\n", human(c), f.residentRecs, coldFrac*100, human(f.akiRAM), ratio(f.akiRAM, f.rivalRAM))
	}

	fmt.Println()
	fmt.Printf("Floor: at a 16-byte key and 8-byte value aki converges to %.4f of the rival's RAM (%.1fx less) as the set grows past the cap.\n",
		floor(16, 8), 1/floor(16, 8))
}

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func human(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2fGiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2fMiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2fKiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}
