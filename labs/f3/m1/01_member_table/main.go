// Lab: member-table load factor and probe stepping (spec 2064/f3 doc 11
// section 2.1-2.5 and 11.1, M1 lab 01).
//
// The question: doc 11 bakes a Swiss-style open-addressed member table at 7/8
// maximum load with SWAR control bytes and 7-bit H2 tags (lines 144, 198, 656),
// and prices its bucket term at 5 x 8/7 ~= 5.7 bytes per member. Before the
// native member-table slice writes that constant into engine/f3/struct, this
// lab prices the two design points the doc fixes but does not sweep: the load
// factor and the probe stepping. It builds candidate table layouts as lab-local
// code (this is NOT the real engine table; the slice writes that) so we can put
// a number on what 7/8 costs in ns/op versus a looser table, and what a plain
// per-slot probe walk costs versus the grouped SWAR scan the doc names.
//
// Method: in-process, no server, no wire, no engine import, one process per
// nothing (the whole sweep runs in one binary because the tables are tiny
// relative to a 1M-value store and maxrss is not the axis here, the analytic
// bucket bytes are). Capacity is a fixed power of two per cardinality; the load
// factor is set by how many distinct keys we insert, so bucket bytes per member
// is exactly bytesPerSlot / load and the load sweep is exact rather than
// quantised by power-of-two rounding. Keys are 8-byte, stored in a record slab
// indexed by ordinal, so a tag hit pays one record read to confirm, the doc's
// "confirm bytes on tag match" (line 189). Three schemes: linear per-slot,
// triangular per-slot, and group (8-wide SWAR groups, triangular group
// stepping), the doc's Swiss shape. Axes: load {0.5 .. 0.9}, scheme, cardinality
// {10k, 1M}, mix {hit, miss, mixed}.
//
// Read: SADD-shaped insert ns/op, SISMEMBER-shaped lookup ns/op per mix, average
// probes per lookup, and bucket bytes per member. The bar is PRED-F3-M1-SETMEM:
// bytes per member at or under Valkey 8.1's 10-20B embedded-entry figure (doc 11
// line 649); this lab settles the table-bucket contribution to that ledger. See
// README.md for the sweep table and the frozen verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/bits"
	"sort"
	"time"
)

const (
	ctrlEmpty    = 0x80 // high bit set: empty slot; full slots hold a 7-bit tag
	groupW       = 8    // one 64-bit SWAR word per group
	bytesPerSlot = 5    // 1 control byte + 4-byte record ordinal (doc 11 line 656)

	lo = 0x0101010101010101
	hi = 0x8080808080808080
)

// splitmix64 finalizer: a cheap strong hash of a uint64 key.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// xorshift is the shared PRNG, identical across cells.
type xorshift uint64

func (x *xorshift) next() uint64 {
	v := *x
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = v
	return uint64(v)
}

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

// table is a lab-local open-addressed member table. It carries only what the
// three schemes need to be compared on equal terms: a control-byte array, a
// per-slot record ordinal, and a record slab of member keys. The real engine
// record is 16 bytes (doc 11 section 2.2); here the record is the 8-byte key,
// which is all the confirm step reads, so the table access pattern is faithful
// and the bucket accounting (control + ordinal) matches the doc's line 656.
type table struct {
	sch  scheme
	mask uint32   // cap - 1
	ctrl []byte   // one control byte per slot, cap long, padded to a group
	ord  []uint32 // record ordinal per slot
	keys []uint64 // record slab: keys[ord] is the member
	n    uint32
}

func newTable(sch scheme, capPow2 uint32, maxMembers int) *table {
	return &table{
		sch:  sch,
		mask: capPow2 - 1,
		ctrl: bytesFilled(int(capPow2), ctrlEmpty),
		ord:  make([]uint32, capPow2),
		keys: make([]uint64, 0, maxMembers),
	}
}

func bytesFilled(n int, b byte) []byte {
	s := make([]byte, n)
	for i := range s {
		s[i] = b
	}
	return s
}

func tagOf(h uint64) byte    { return byte(h) & 0x7f }
func slotOf(h uint64) uint64 { return h >> 7 }

// swarMatch returns a mask with 0x80 set in every byte of word equal to tag.
func swarMatch(word uint64, tag byte) uint64 {
	cmp := word ^ (lo * uint64(tag))
	return (cmp - lo) &^ cmp & hi
}

// insert probes for key, and if absent places it at the first empty slot the
// probe passed, which is exactly the SADD miss path (probe then append record).
// Distinct keys are inserted, so it never reports a duplicate.
func (t *table) insert(key uint64) {
	h := mix(key)
	tag := tagOf(h)
	ord := uint32(len(t.keys))
	switch t.sch {
	case schGroup:
		numG := (t.mask + 1) / groupW
		g := uint32(slotOf(h)) & (numG - 1)
		step := uint32(1)
		for {
			base := g * groupW
			word := binary.LittleEndian.Uint64(t.ctrl[base:])
			empt := word & hi
			if empt != 0 {
				slot := base + uint32(idx(empt))
				t.place(slot, tag, ord, key)
				return
			}
			g = (g + step) & (numG - 1)
			step++
		}
	default:
		slot := uint32(slotOf(h)) & t.mask
		step := uint32(1)
		for {
			if t.ctrl[slot] == ctrlEmpty {
				t.place(slot, tag, ord, key)
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

func (t *table) place(slot uint32, tag byte, ord uint32, key uint64) {
	t.ctrl[slot] = tag
	t.ord[slot] = ord
	t.keys = append(t.keys, key)
	t.n++
}

// idx returns the byte index (0..7) of the lowest set 0x80 bit in mask.
func idx(mask uint64) int {
	return bits.TrailingZeros64(mask) >> 3
}

// lookup is the SISMEMBER probe: find key, confirm bytes on tag match.
func (t *table) lookup(key uint64) bool {
	h := mix(key)
	tag := tagOf(h)
	switch t.sch {
	case schGroup:
		numG := (t.mask + 1) / groupW
		g := uint32(slotOf(h)) & (numG - 1)
		step := uint32(1)
		for {
			base := g * groupW
			word := binary.LittleEndian.Uint64(t.ctrl[base:])
			m := swarMatch(word, tag)
			for m != 0 {
				slot := base + uint32(idx(m))
				if t.keys[t.ord[slot]] == key {
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
		slot := uint32(slotOf(h)) & t.mask
		step := uint32(1)
		for {
			c := t.ctrl[slot]
			if c == ctrlEmpty {
				return false
			}
			if c == tag && t.keys[t.ord[slot]] == key {
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
// lookup, a mechanism metric that explains the ns/op curve.
func (t *table) probes(key uint64) int {
	h := mix(key)
	tag := tagOf(h)
	n := 0
	switch t.sch {
	case schGroup:
		numG := (t.mask + 1) / groupW
		g := uint32(slotOf(h)) & (numG - 1)
		step := uint32(1)
		for {
			n++
			base := g * groupW
			word := binary.LittleEndian.Uint64(t.ctrl[base:])
			m := swarMatch(word, tag)
			for m != 0 {
				slot := base + uint32(idx(m))
				if t.keys[t.ord[slot]] == key {
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
		slot := uint32(slotOf(h)) & t.mask
		step := uint32(1)
		for {
			n++
			c := t.ctrl[slot]
			if c == ctrlEmpty {
				return n
			}
			if c == tag && t.keys[t.ord[slot]] == key {
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

type cell struct {
	card           string
	sch            scheme
	load           float64
	member         int
	bytesPerMember float64
	insNs          float64
	hitNs          float64
	missNs         float64
	mixNs          float64
	hitPr          float64
	missPr         float64
}

func main() {
	quick := flag.Bool("quick", false, "smaller op counts for a fast check")
	flag.Parse()

	cards := []card{
		{"10k", 1 << 14}, // up to ~14.7k members at load 0.9
		{"1M", 1 << 20},  // up to ~944k members at load 0.9
	}
	loads := []float64{0.50, 0.60, 0.70, 0.80, 0.875, 0.90}
	schemes := []scheme{schLinear, schTriangular, schGroup}

	// Op budgets. Lookups dominate the signal so they get the most ops.
	lookOps := 20_000_000
	if *quick {
		lookOps = 2_000_000
	}

	var cells []cell
	for _, cd := range cards {
		for _, ld := range loads {
			members := int(ld*float64(cd.pow2) + 0.5)
			// Distinct member keys and a disjoint miss pool.
			mem := make([]uint64, members)
			seen := make(map[uint64]struct{}, members)
			rng := xorshift(0x9e3779b97f4a7c15 ^ uint64(cd.pow2) ^ uint64(members))
			for i := range mem {
				var k uint64
				for {
					k = rng.next() | 1 // keep odd so the miss pool (even) never collides
					if _, ok := seen[k]; !ok {
						seen[k] = struct{}{}
						break
					}
				}
				mem[i] = k
			}
			missPool := make([]uint64, 1<<16)
			for i := range missPool {
				missPool[i] = rng.next() &^ 1 // even keys, never inserted
			}
			warm := lookOps / 4

			for _, sch := range schemes {
				t := newTable(sch, cd.pow2, members)
				// SADD-shaped: time the full build (probe-then-place per key).
				start := time.Now()
				for _, k := range mem {
					t.insert(k)
				}
				insEl := time.Since(start)

				pick := func(r *xorshift, hit bool) uint64 {
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
					card:           cd.name,
					sch:            sch,
					load:           float64(members) / float64(cd.pow2),
					member:         members,
					bytesPerMember: float64(cd.pow2) * bytesPerSlot / float64(members),
					insNs:          float64(insEl.Nanoseconds()) / float64(members),
					hitNs:          run(100),
					missNs:         run(0),
					mixNs:          run(50),
					hitPr:          avgPr(true),
					missPr:         avgPr(false),
				})
			}
		}
	}

	report(cells)
}

func report(cells []cell) {
	sort.SliceStable(cells, func(i, j int) bool {
		if cells[i].card != cells[j].card {
			return cells[i].card == "10k"
		}
		if cells[i].load != cells[j].load {
			return cells[i].load < cells[j].load
		}
		return cells[i].sch < cells[j].sch
	})
	fmt.Printf("member-table sweep, %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("%-4s %-11s %6s %7s %7s %7s %7s %7s %7s %7s\n",
		"card", "scheme", "load", "B/mem", "insNs", "hitNs", "missNs", "mixNs", "hitPr", "missPr")
	last := ""
	for _, c := range cells {
		key := c.card + fmt.Sprintf("%.3f", c.load)
		if key != last {
			fmt.Println()
			last = key
		}
		fmt.Printf("%-4s %-11s %6.3f %7.2f %7.1f %7.1f %7.1f %7.1f %7.2f %7.2f\n",
			c.card, c.sch.String(), c.load, c.bytesPerMember,
			c.insNs, c.hitNs, c.missNs, c.mixNs, c.hitPr, c.missPr)
	}
}
