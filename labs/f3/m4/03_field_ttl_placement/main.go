// Lab: field-TTL memory placement on the two hash bands (spec 2064/f3 doc 10
// section 6, M4 lab 03, issue #546).
//
// The question: where do the field-TTL bytes live, and what does a hash that
// never sets one pay? Field TTL is the HEXPIRE family. Redis stores a listpack
// hash with any TTL as listpackex, an eight-byte expiry slot after every entry
// in the whole blob, and a hashtable hash with a TTL through an ebuckets index.
// The memory bar for f3 is to use less RAM than the rival for the same data, so
// the placement has to cost nothing until a field actually takes a TTL and then
// cost only the eight bytes the expiry needs, with no per-hash TTL index.
//
// aki's placement (engine/f3/hash/field.go and hash.go):
//
//   - native band: a lazily allocated exp column, []uint64 indexed by record
//     ordinal, nil until the first HEXPIRE-family setter. A native hash with no
//     field TTL carries no expiry bytes at all. On the first TTL the column
//     allocates one slot per record, so the whole hash then pays eight bytes a
//     field, matching what the rival's per-entry expiry costs, with the column
//     header the only fixed add.
//   - inline band: a sticky listpackex flag in the blob header. Before the first
//     TTL the blob is the plain listpack, no expiry bytes. On the first TTL the
//     blob re-encodes with an eight-byte slot trailing every entry and the flag
//     flips, so from then on the blob pays eight bytes a field, byte for byte the
//     rival's listpackex.
//   - per hash: one uint64 next-expire hint, always present, so the machinery
//     that gates the lazy reap costs one word a hash and one comparison a command
//     whether or not any field has a TTL.
//
// This lab models the two bands' TTL-bearing memory exactly as the real structs
// lay it out (the same modeling discipline lab 02 uses for the field slab) and
// measures the heap bytes each placement costs with no TTL, with every field
// TTL'd, and with half the fields TTL'd. The half arm is the honest nuance: both
// bands pay the eight bytes for every field the moment any one field takes a TTL
// (the column and the sticky slot are whole-container, not per-field), the same
// all-or-nothing the rival's listpackex pays, so half costs the same as all.
//
// Swept over field count m in {8, 64, 512, 4096, 65536} by value width in {8, 64}
// bytes. Read: the heap bytes the band allocates with no TTL against with all
// TTL'd, the delta, and the delta per field, which is exactly eight on both bands
// once a TTL lands and zero until then. See README.md for the table and the
// frozen verdict. The rival RSS comparison this feeds is PRED-F3-M4-HASHMEM at
// the M4 gate run, not here.
package main

import (
	"flag"
	"fmt"
	"runtime"
	"strconv"
	"unsafe"
)

// fentry mirrors engine/f3/hash/field.go's record exactly, so the native model's
// per-record footprint matches the real band and the exp column is the only thing
// the TTL arm adds.
type fentry struct {
	foff  uint32
	voff  uint32
	vslot uint32
	flen  uint16
	vlen  uint32
}

// nativeBand models the ftable memory field TTL touches: the record vector, the
// draw vector, the field/value slab, and the lazily allocated exp column. ttl is
// the number of fields carrying a TTL; the column is allocated (and so charges the
// whole hash) the moment ttl is above zero, exactly as setFieldExp does.
func nativeBand(m, valWidth, ttl int) (ents []fentry, vec []uint32, slab []byte, exp []uint64) {
	ents = make([]fentry, m)
	vec = make([]uint32, m)
	for i := 0; i < m; i++ {
		field := "f" + strconv.Itoa(i)
		foff := uint32(len(slab))
		slab = append(slab, field...)
		voff := uint32(len(slab))
		for j := 0; j < valWidth; j++ {
			slab = append(slab, byte('a'+(i+j)%26))
		}
		ents[i] = fentry{foff, voff, uint32(i), uint16(len(field)), uint32(valWidth)}
		vec[i] = uint32(i)
	}
	if ttl > 0 {
		// The first TTL allocates the column for the whole record vector; a later
		// TTL writes a slot that already exists. So any ttl > 0 costs len(ents)*8.
		exp = make([]uint64, m)
		for i := 0; i < ttl; i++ {
			exp[i] = 1 << 40
		}
	}
	return
}

// inlineBlob models the listpack (no TTL) and listpackex (any TTL) blob layouts
// from engine/f3/hash/hash.go: a three-byte header then, per entry,
// [flen][field][vlen][value] and, once sticky, a trailing eight-byte expiry slot.
// ttl > 0 flips the sticky bit, which re-encodes every entry with the slot, so the
// blob grows by eight a field for the whole container, not per TTL'd field.
func inlineBlob(m, valWidth, ttl int) []byte {
	sticky := ttl > 0
	blob := []byte{0, 0, 0} // count:uint16-le, flags:uint8
	for i := 0; i < m; i++ {
		field := "f" + strconv.Itoa(i)
		blob = append(blob, byte(len(field)))
		blob = append(blob, field...)
		blob = append(blob, byte(valWidth))
		for j := 0; j < valWidth; j++ {
			blob = append(blob, byte('a'+(i+j)%26))
		}
		if sticky {
			blob = append(blob, 0, 0, 0, 0, 0, 0, 0, 0)
		}
	}
	return blob
}

// sizeFentry is the per-record footprint of the native band, taken off the model
// struct so it tracks the real fentry width (five fields, aligned).
var sizeFentry = int(unsafe.Sizeof(fentry{}))

// nativeBytes is the resident structural footprint of the native band: the record
// vector, the draw vector, the field/value slab, and the exp column when present.
// Structural bytes, not size-class-rounded heap, so the TTL delta is exact and
// comparable to the rival's byte layout rather than to Go's allocator buckets.
func nativeBytes(ents []fentry, vec []uint32, slab []byte, exp []uint64) int {
	return len(ents)*sizeFentry + len(vec)*4 + len(slab) + len(exp)*8
}

// sink defeats dead-code elimination for the measured allocator-rounding note.
var sink int

// heapBytes reports the mean heap bytes build allocates per call, the size-class
// rounded figure the allocator actually reserves (the measure lab 02 uses). Shown
// once as a note so the reader sees the allocator rounds the structural bytes up.
func heapBytes(reps int, build func() int) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for r := 0; r < reps; r++ {
		sink += build()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(reps)
}

func main() {
	flag.Bool("quick", false, "smaller sweep (kept for the shared lab runner; the table is deterministic)")
	flag.Parse()

	ms := []int{8, 64, 512, 4096, 65536}
	widths := []int{8, 64}

	fmt.Printf("M4 lab 03: field-TTL memory placement, native and inline bands\n")
	fmt.Printf("fentry %d bytes/record; per-hash next-expire hint 8 bytes (always present)\n\n", sizeFentry)

	// Native band: the exp column is the whole TTL cost, nil until the first TTL.
	fmt.Printf("native band (exp column, []uint64 indexed by ordinal)\n")
	fmt.Printf("%8s %6s %12s %12s %12s %11s\n",
		"fields", "valW", "noTTL_B", "allTTL_B", "delta_B", "delta/f")
	for _, w := range widths {
		for _, m := range ms {
			en, vn, sn, xn := nativeBand(m, w, 0)
			ea, va, sa, xa := nativeBand(m, w, m)
			no := nativeBytes(en, vn, sn, xn)
			all := nativeBytes(ea, va, sa, xa)
			fmt.Printf("%8d %6d %12d %12d %12d %11.2f\n",
				m, w, no, all, all-no, float64(all-no)/float64(m))
		}
	}

	// Inline band: the sticky listpackex slot, zero until the first TTL.
	fmt.Printf("\ninline band (listpack -> listpackex, 8B slot per entry)\n")
	fmt.Printf("%8s %6s %12s %12s %12s %11s\n",
		"fields", "valW", "noTTL_B", "allTTL_B", "delta_B", "delta/f")
	for _, w := range widths {
		for _, m := range ms {
			if m > maxInline {
				continue // the inline band tops out at hash-max-listpack-entries
			}
			no := len(inlineBlob(m, w, 0))
			all := len(inlineBlob(m, w, m))
			fmt.Printf("%8d %6d %12d %12d %12d %11.2f\n",
				m, w, no, all, all-no, float64(all-no)/float64(m))
		}
	}

	// The half arm: any TTL charges the whole container, so half costs the same as
	// all. This is the all-or-nothing the rival's listpackex pays too.
	fmt.Printf("\nhalf-TTL arm (any TTL charges the whole container)\n")
	fmt.Printf("%8s %6s %8s %12s %12s\n", "fields", "valW", "band", "halfTTL_B", "allTTL_B")
	for _, m := range []int{64, 512} {
		_, _, _, xh := nativeBand(m, 64, m/2)
		_, _, _, xa := nativeBand(m, 64, m)
		fmt.Printf("%8d %6d %8s %12d %12d\n", m, 64, "native", len(xh)*8, len(xa)*8)
		fmt.Printf("%8d %6d %8s %12d %12d\n", m, 64, "inline",
			len(inlineBlob(m, 64, m/2)), len(inlineBlob(m, 64, m)))
	}

	// One measured note: the resident heap is larger than the structural bytes
	// because each make grows to a size class and the slab is built with append,
	// which over-reserves as it doubles. The real band presizes the slab and table
	// on promotion (newFtable takes a hint), so it pays less of the append tax than
	// this build loop; either way the rounding is allocator overhead the rival pays
	// too, so it cancels in the gate-run RSS comparison. The TTL delta above is the
	// exact structural cost, which is what the memory bar compares.
	e, v, s, x := nativeBand(512, 64, 512)
	structural := nativeBytes(e, v, s, x)
	measured := heapBytes(5000, func() int {
		en, vn, sn, xn := nativeBand(512, 64, 512)
		return len(en) + len(vn) + len(sn) + len(xn)
	})
	fmt.Printf("\nallocator note (512 fields, 64B, all-TTL native): structural %d B, heap %d B/build\n",
		structural, measured)

	fmt.Printf("\nsink=%d\n", sink)
}

// maxInline is hash-max-listpack-entries, the inline-band ceiling (hash.go).
const maxInline = 512
