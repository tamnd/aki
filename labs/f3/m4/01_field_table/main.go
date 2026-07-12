// Lab: field-table load factor and confirm cost by field-name length (spec
// 2064/f3 doc 10 section 3.4-3.6, M4 lab 01).
//
// The question: M1's lab 01 (labs/f3/m1/01_member_table) froze the SHARED
// struct.Table kernel at 7/8 maximum load with an eight-wide SWAR group probe,
// triangular group stepping, and a 7-bit H2 tag, and it settled that on set
// members that are fixed 8-byte words, so the confirm on a tag hit was one
// 8-byte compare. M4's field table (engine/f3/hash/field.go) reuses that exact
// kernel, but its confirm (ftable.Match) is a variable-length bytes.Equal over
// field-name bytes in a slab, and hash field names are variable-length strings,
// not fixed 8-byte words. So the question this lab answers: does M1's 7/8 plus
// Swiss-group verdict still hold once the confirm is a variable-length
// bytes.Equal across realistic field-name lengths, or does the confirm cost
// refute the inheritance? The milestone says this lab inherits M1's verdict
// unless refuted, so the job is to price the new axis (confirm cost by field
// length) and either confirm the inheritance or lay out the refutation with
// numbers.
//
// Method: in-process, no server, no wire. The tables here are lab-local code
// that models the design points (this is NOT the engine table; field.go is
// that), built so the three schemes compare on equal terms. A slot is one
// control byte plus a four-byte record ordinal, bytesPerSlot 5, the same layout
// M1 priced and the same layout the field table's struct.Table carries, so the
// bucket bytes per field is exactly bytesPerSlot / load and reproduces M1's
// curve by construction. Field names live in a record slab addressed by
// ordinal via an offset and a length, exactly like ftable, so a tag hit pays
// one variable-length bytes.Equal to confirm, the field table's real confirm.
// The hash is store.Hash (wyhash), the engine's own hasher, so the confirm cost
// and the probe distribution are the true ones and not a lab surrogate (the M1
// lab rolled its own splitmix; using the real hasher here is strictly better
// and it is what field.go calls).
//
// Axes: load {0.50, 0.60, 0.70, 0.80, 0.875, 0.90}, scheme {linear per-slot,
// triangular per-slot, group (8-wide SWAR, triangular group stepping, H2 tag)},
// field-name length class {short ~8B, medium ~24B, long ~64B}, mix {all hit,
// all miss, 50/50}, cardinality {10k, 1M}.
//
// Read per cell: HSET-shaped insert ns/op (probe then append the record),
// HGET/HEXISTS-shaped lookup ns/op per mix, mean probes per lookup
// (deterministic given load, scheme, and the hashes, the noise-free mechanism
// metric), and bucket bytes per field (analytic, 5/load, identical to M1). See
// README.md for the sweep and the frozen verdict.
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math/bits"
	"sort"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

const (
	ctrlEmpty    = 0x80 // high bit set: empty slot; full slots hold a 7-bit tag
	groupW       = 8    // one 64-bit SWAR word per group
	bytesPerSlot = 5    // 1 control byte + 4-byte record ordinal (field.go slot)

	lo = 0x0101010101010101
	hi = 0x8080808080808080
)

// tagOf and groupBits mirror struct.Table exactly: the low 7 bits are the H2
// tag in the control byte, the bits above the tag pick the home group, so the
// two are drawn from disjoint parts of the hash.
func tagOf(h uint64) byte       { return byte(h) & 0x7f }
func groupBits(h uint64) uint64 { return h >> 7 }

type scheme int

const (
	schLinear scheme = iota
	schTriangular
	schGroup
)

func (s scheme) String() string {
	switch s {
	case schLinear:
		return "linear"
	case schTriangular:
		return "triangular"
	default:
		return "group"
	}
}

// table is a lab-local open-addressed field table. It carries only what the
// three schemes need to be compared on equal terms: a control-byte array, a
// per-slot record ordinal, and a record slab of field-name bytes addressed by
// offset and length, the ftable layout minus the value (the value never touches
// the probe or the confirm). The confirm is bytes.Equal over the name slice,
// which is the field table's real Match and the whole point of this lab.
type table struct {
	sch  scheme
	mask uint32   // cap - 1
	ctrl []byte   // one control byte per slot, cap long, padded to a group
	ord  []uint32 // record ordinal per slot
	slab []byte   // record slab: field-name bytes, appended
	foff []uint32 // per-ordinal slab offset of the name
	flen []uint16 // per-ordinal name length
	n    uint32
}

func newTable(sch scheme, capPow2 uint32, maxFields int) *table {
	return &table{
		sch:  sch,
		mask: capPow2 - 1,
		ctrl: bytesFilled(int(capPow2), ctrlEmpty),
		ord:  make([]uint32, capPow2),
		foff: make([]uint32, 0, maxFields),
		flen: make([]uint16, 0, maxFields),
	}
}

func bytesFilled(n int, b byte) []byte {
	s := make([]byte, n)
	for i := range s {
		s[i] = b
	}
	return s
}

// swarMatch returns a mask with 0x80 set in every byte of word equal to tag,
// the portable abseil group match (exact for any target byte).
func swarMatch(word uint64, tag byte) uint64 {
	cmp := word ^ (lo * uint64(tag))
	return (cmp - lo) &^ cmp & hi
}

// match is the field table's confirm: the name stored at ord must equal key, a
// variable-length bytes.Equal over the slab slice. It runs only on a tag hit.
func (t *table) match(ord uint32, key []byte) bool {
	off := t.foff[ord]
	return bytes.Equal(t.slab[off:off+uint32(t.flen[ord])], key)
}

// insert probes for name and places it at the first empty slot the probe
// passed, the HSET miss path (probe then append the record). Distinct names are
// inserted, so it never reports a duplicate.
func (t *table) insert(name []byte) {
	h := store.Hash(name)
	tag := tagOf(h)
	ord := uint32(len(t.foff))
	t.foff = append(t.foff, uint32(len(t.slab)))
	t.flen = append(t.flen, uint16(len(name)))
	t.slab = append(t.slab, name...)
	switch t.sch {
	case schGroup:
		numG := (t.mask + 1) / groupW
		g := uint32(groupBits(h)) & (numG - 1)
		step := uint32(1)
		for {
			base := g * groupW
			word := binary.LittleEndian.Uint64(t.ctrl[base:])
			empt := word & hi
			if empt != 0 {
				slot := base + uint32(idx(empt))
				t.place(slot, tag, ord)
				return
			}
			g = (g + step) & (numG - 1)
			step++
		}
	default:
		slot := uint32(groupBits(h)) & t.mask
		step := uint32(1)
		for {
			if t.ctrl[slot] == ctrlEmpty {
				t.place(slot, tag, ord)
				return
			}
			if t.sch == schLinear {
				slot = (slot + 1) & t.mask
			} else {
				slot = (slot + step) & t.mask
				step++
			}
		}
	}
}

func (t *table) place(slot uint32, tag byte, ord uint32) {
	t.ctrl[slot] = tag
	t.ord[slot] = ord
	t.n++
}

// idx returns the byte index (0..7) of the lowest set 0x80 bit in mask.
func idx(mask uint64) int {
	return bits.TrailingZeros64(mask) >> 3
}

// lookup is the HGET probe: find name, confirm bytes on a tag match.
func (t *table) lookup(name []byte) bool {
	h := store.Hash(name)
	tag := tagOf(h)
	switch t.sch {
	case schGroup:
		numG := (t.mask + 1) / groupW
		g := uint32(groupBits(h)) & (numG - 1)
		step := uint32(1)
		for {
			base := g * groupW
			word := binary.LittleEndian.Uint64(t.ctrl[base:])
			m := swarMatch(word, tag)
			for m != 0 {
				slot := base + uint32(idx(m))
				if t.match(t.ord[slot], name) {
					return true
				}
				m &= m - 1
			}
			if word&hi != 0 {
				return false
			}
			g = (g + step) & (numG - 1)
			step++
		}
	default:
		slot := uint32(groupBits(h)) & t.mask
		step := uint32(1)
		for {
			c := t.ctrl[slot]
			if c == ctrlEmpty {
				return false
			}
			if c == tag && t.match(t.ord[slot], name) {
				return true
			}
			if t.sch == schLinear {
				slot = (slot + 1) & t.mask
			} else {
				slot = (slot + step) & t.mask
				step++
			}
		}
	}
}

// probes counts slots (linear/triangular) or groups (group) examined for one
// lookup, a mechanism metric that explains the ns/op curve. It runs the same
// confirm the timed lookup does, so a tag collision costs a probe just as it
// does in flight.
func (t *table) probes(name []byte) int {
	h := store.Hash(name)
	tag := tagOf(h)
	n := 0
	switch t.sch {
	case schGroup:
		numG := (t.mask + 1) / groupW
		g := uint32(groupBits(h)) & (numG - 1)
		step := uint32(1)
		for {
			n++
			base := g * groupW
			word := binary.LittleEndian.Uint64(t.ctrl[base:])
			m := swarMatch(word, tag)
			for m != 0 {
				slot := base + uint32(idx(m))
				if t.match(t.ord[slot], name) {
					return n
				}
				m &= m - 1
			}
			if word&hi != 0 {
				return n
			}
			g = (g + step) & (numG - 1)
			step++
		}
	default:
		slot := uint32(groupBits(h)) & t.mask
		step := uint32(1)
		for {
			n++
			c := t.ctrl[slot]
			if c == ctrlEmpty {
				return n
			}
			if c == tag && t.match(t.ord[slot], name) {
				return n
			}
			if t.sch == schLinear {
				slot = (slot + 1) & t.mask
			} else {
				slot = (slot + step) & t.mask
				step++
			}
		}
	}
}

type card struct {
	name string
	pow2 uint32 // table capacity in slots
}

type lenClass struct {
	name string
	size int // field-name length in bytes
}

type cell struct {
	card          string
	length        string
	sch           scheme
	load          float64
	field         int
	bytesPerField float64
	insNs         float64
	hitNs         float64
	missNs        float64
	mixNs         float64
	hitPr         float64
	missPr        float64
}

// makeName writes a distinct field name of the given length. The first eight
// bytes are the id counter little-endian, the tail is a filler pattern derived
// from the id, so names of a class are the full length and a hit confirm walks
// every byte (bytes.Equal on equal slices reads all of them). Disjoint id
// ranges give disjoint names with no collision check needed.
func makeName(id uint64, length int) []byte {
	b := make([]byte, length)
	binary.LittleEndian.PutUint64(b, id)
	for i := 8; i < length; i++ {
		b[i] = byte(id>>uint(i&7)) | 0x40 // a non-zero, printable-ish tail
	}
	return b
}

// Op budgets. Lookups dominate the signal so they get the most ops; the length
// axis triples the cell count against M1, so the full per-run budget is trimmed
// to keep the whole sweep to a few minutes without softening the ordering,
// which is wide. -quick shrinks it for the smoke test.
const (
	fullLookOps  = 5_000_000
	quickLookOps = 300_000
)

// fullCards is the cardinality axis the README reports: a cache-resident 10k
// cell and a DRAM-bound 1M cell. The 1M cell builds a ~944k-name table per
// length and scheme, which is seconds on a fast box but minutes on a loaded CI
// runner, so the smoke test drives the sweep over a small cardinality instead of
// this one (see main_test.go). The 1M arm is a memory-bandwidth measurement, not
// a correctness one; its build and probe code is the same the 10k arm exercises.
var fullCards = []card{
	{"10k", 1 << 14}, // up to ~14.7k fields at load 0.9
	{"1M", 1 << 20},  // up to ~944k fields at load 0.9
}

func main() {
	quick := flag.Bool("quick", false, "smaller op counts for a fast check")
	flag.Parse()

	lookOps := fullLookOps
	if *quick {
		lookOps = quickLookOps
	}
	report(sweep(lookOps, fullCards))
}

// sweep runs every axis point at the given lookup budget over the given
// cardinality cells and returns one cell per (cardinality, length class, load,
// scheme). It is the whole experiment, factored out so the smoke test can drive
// it at a small cardinality and budget.
func sweep(lookOps int, cards []card) []cell {
	loads := []float64{0.50, 0.60, 0.70, 0.80, 0.875, 0.90}
	schemes := []scheme{schLinear, schTriangular, schGroup}
	lengths := []lenClass{{"short", 8}, {"medium", 24}, {"long", 64}}

	// Disjoint id ranges keep the miss pool from ever colliding with a member.
	const missBase = uint64(1) << 40

	var cells []cell
	for _, cd := range cards {
		for _, lc := range lengths {
			for _, ld := range loads {
				fields := int(ld*float64(cd.pow2) + 0.5)
				mem := make([][]byte, fields)
				for i := range mem {
					mem[i] = makeName(uint64(i), lc.size)
				}
				missPool := make([][]byte, 1<<16)
				for i := range missPool {
					missPool[i] = makeName(missBase+uint64(i), lc.size)
				}
				warm := lookOps / 4

				for _, sch := range schemes {
					t := newTable(sch, cd.pow2, fields)
					// HSET-shaped: time the full build (probe-then-place per name).
					start := time.Now()
					for _, k := range mem {
						t.insert(k)
					}
					insEl := time.Since(start)

					pick := func(r *xorshift, hit bool) []byte {
						if hit {
							return mem[r.next()%uint64(len(mem))]
						}
						return missPool[r.next()&(uint64(len(missPool))-1)]
					}

					run := func(hitFrac int) float64 {
						r := xorshift(0xd1b54a32d192ed03)
						for i := 0; i < warm; i++ {
							_ = t.lookup(pick(&r, i%100 < hitFrac))
						}
						var sink int
						s := time.Now()
						for i := 0; i < lookOps; i++ {
							if t.lookup(pick(&r, i%100 < hitFrac)) {
								sink++
							}
						}
						el := time.Since(s)
						if sink < 0 {
							fmt.Println(sink)
						}
						return float64(el.Nanoseconds()) / float64(lookOps)
					}

					// Average probe length over a metrics pass (untimed).
					avgPr := func(hit bool) float64 {
						r := xorshift(0x2545f4914f6cdd1d)
						const samples = 200000
						tot := 0
						for i := 0; i < samples; i++ {
							tot += t.probes(pick(&r, hit))
						}
						return float64(tot) / samples
					}

					cells = append(cells, cell{
						card:          cd.name,
						length:        lc.name,
						sch:           sch,
						load:          float64(fields) / float64(cd.pow2),
						field:         fields,
						bytesPerField: float64(cd.pow2) * bytesPerSlot / float64(fields),
						insNs:         float64(insEl.Nanoseconds()) / float64(fields),
						hitNs:         run(100),
						missNs:        run(0),
						mixNs:         run(50),
						hitPr:         avgPr(true),
						missPr:        avgPr(false),
					})
				}
			}
		}
	}
	return cells
}

// xorshift is the shared PRNG for the lookup key stream, identical across cells.
type xorshift uint64

func (x *xorshift) next() uint64 {
	v := *x
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = v
	return uint64(v)
}

func report(cells []cell) {
	lenRank := map[string]int{"short": 0, "medium": 1, "long": 2}
	sort.SliceStable(cells, func(i, j int) bool {
		if cells[i].card != cells[j].card {
			return cells[i].card == "10k"
		}
		if cells[i].length != cells[j].length {
			return lenRank[cells[i].length] < lenRank[cells[j].length]
		}
		if cells[i].load != cells[j].load {
			return cells[i].load < cells[j].load
		}
		return cells[i].sch < cells[j].sch
	})
	fmt.Printf("field-table sweep, %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("%-4s %-6s %-11s %6s %6s %7s %7s %7s %7s %7s %7s\n",
		"card", "len", "scheme", "load", "B/f", "insNs", "hitNs", "missNs", "mixNs", "hitPr", "missPr")
	last := ""
	for _, c := range cells {
		key := c.card + c.length + fmt.Sprintf("%.3f", c.load)
		if key != last {
			fmt.Println()
			last = key
		}
		fmt.Printf("%-4s %-6s %-11s %6.3f %6.2f %7.1f %7.1f %7.1f %7.1f %7.2f %7.2f\n",
			c.card, c.length, c.sch.String(), c.load, c.bytesPerField,
			c.insNs, c.hitNs, c.missNs, c.mixNs, c.hitPr, c.missPr)
	}
}
