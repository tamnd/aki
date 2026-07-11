// Lab: the inline set threshold (spec 2064/f3 doc 11 section 3, M1 lab 02).
//
// The question, from doc 11 section 3: the inline band keeps a set as one
// packed blob (a sorted intset-class integer array or a listpack-class
// length-prefixed member blob) and answers membership by binary search or a
// linear scan, and it converts one way to the native member table (a hash
// probe) once it breaches a cap. Redis freezes those caps at
// set-max-intset-entries 512, set-max-listpack-entries 128, and
// set-max-listpack-value 64, and the slice bakes the same numbers so OBJECT
// ENCODING parity holds. This lab prices the two inline representations
// against a member-table stand-in across cardinality and member size, so the
// baked caps are a measured crossover and not an inherited guess: the packed
// blob may only stay inline while its point ops sit at or under the table it
// would convert to.
//
// The member table is the next slice (M1 lab 01 settles its load factor), so
// this lab uses a Go map[string]struct{} as the hash-probe stand-in: it is
// the pessimistic proxy (a real open-addressed tag-probed table is faster and
// allocation-free), which only strengthens the verdict, because if the packed
// blob beats even a map up to the cap it beats the native table too.
//
// Method: in-process, no server, no wire. For each (representation,
// cardinality, member size) cell the lab builds the structure, then times the
// adversarial membership op: a lookup of a member that is absent, which forces
// the listpack scan to walk every entry and the intset binary search to its
// full depth. The miss is the conservative price the conversion threshold has
// to cover; a hit averages half a scan. Reps are timed back to back and the
// median is reported to damp scheduler noise.
//
// Read: ns/op for intset binary search, listpack linear scan (with the
// one-byte tag fast-reject doc 11 section 3.1 specifies), and the map probe,
// across the cardinality sweep. The crossover cardinality is where the linear
// scan first costs more than the map; the frozen caps must sit at or below it.
// See README.md for the numbers and the frozen verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"sort"
	"time"
)

// Redis's frozen inline caps, the numbers under test.
const (
	maxIntsetEntries   = 512
	maxListpackEntries = 128
	maxListpackValue   = 64
)

// intsetProbe answers membership on a sorted []int64 by binary search, the
// intset-class point op.
func intsetProbe(a []int64, x int64) bool {
	lo, hi := 0, len(a)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if a[mid] < x {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(a) && a[lo] == x
}

// listpack is the packed-member blob: for each entry a one-byte length, a
// one-byte tag (the member's first byte, or 0 when empty), then the member
// bytes. Members are capped at maxListpackValue, so the length is one byte.
type listpack struct {
	buf []byte
}

func (lp *listpack) add(m []byte) {
	lp.buf = append(lp.buf, byte(len(m)))
	var tag byte
	if len(m) > 0 {
		tag = m[0]
	}
	lp.buf = append(lp.buf, tag)
	lp.buf = append(lp.buf, m...)
}

// has scans the blob with the tag fast-reject: an entry whose tag or length
// misses is skipped without a byte compare, which is the whole point of
// carrying the tag inline.
func (lp *listpack) has(m []byte) bool {
	var tag byte
	if len(m) > 0 {
		tag = m[0]
	}
	b := lp.buf
	for i := 0; i < len(b); {
		n := int(b[i])
		t := b[i+1]
		start := i + 2
		if t == tag && n == len(m) && string(b[start:start+n]) == string(m) {
			return true
		}
		i = start + n
	}
	return false
}

// memberBytes renders member index i at the given width as a fixed-size key,
// deterministic across representations.
func memberBytes(dst []byte, i, width int) []byte {
	dst = dst[:0]
	var head [8]byte
	binary.LittleEndian.PutUint64(head[:], uint64(i)*0x9e3779b97f4a7c15)
	for len(dst) < width {
		dst = append(dst, head[:]...)
	}
	return dst[:width]
}

// median returns the middle of a small timing slice.
func median(xs []float64) float64 {
	sort.Float64s(xs)
	return xs[len(xs)/2]
}

// timeOp runs op iters times, reps reps, and returns the median ns/op.
func timeOp(iters, reps int, op func(i int)) float64 {
	best := make([]float64, reps)
	for r := 0; r < reps; r++ {
		start := time.Now()
		for i := 0; i < iters; i++ {
			op(i)
		}
		best[r] = float64(time.Since(start).Nanoseconds()) / float64(iters)
	}
	return median(best)
}

var sink int

func main() {
	iters := flag.Int("iters", 4_000_000, "membership probes per rep")
	reps := flag.Int("reps", 5, "reps per cell, median reported")
	flag.Parse()

	cards := []int{1, 2, 4, 8, 16, 32, 64, 96, 128, 192, 256, 384, 512, 1024}
	widths := []int{8, 16, 32, 64}

	fmt.Printf("inline set threshold sweep: %d iters x %d reps, median ns/op (absent-member probe)\n\n", *iters, *reps)

	// Intset-class: sorted integers, binary search, no member width.
	fmt.Println("## intset-class (sorted int64, binary search)")
	fmt.Printf("%8s  %10s  %10s\n", "card", "intset", "map")
	for _, k := range cards {
		ints := make([]int64, k)
		mp := make(map[int64]struct{}, k)
		for i := 0; i < k; i++ {
			ints[i] = int64(i) * 2 // even members, odd probes always miss
			mp[int64(i)*2] = struct{}{}
		}
		sort.Slice(ints, func(a, b int) bool { return ints[a] < ints[b] })
		missBase := int64(k*2 + 1)
		bs := timeOp(*iters, *reps, func(i int) {
			if intsetProbe(ints, missBase+int64(i&1023)*2) {
				sink++
			}
		})
		mpt := timeOp(*iters, *reps, func(i int) {
			if _, ok := mp[missBase+int64(i&1023)*2]; ok {
				sink++
			}
		})
		note := ""
		if k > maxIntsetEntries {
			note = "  (past intset cap)"
		}
		fmt.Printf("%8d  %9.2f  %9.2f%s\n", k, bs, mpt, note)
	}

	// Listpack-class: packed member blob, linear scan with tag reject.
	for _, w := range widths {
		fmt.Printf("\n## listpack-class (packed blob, linear scan, %dB members)\n", w)
		fmt.Printf("%8s  %10s  %10s\n", "card", "listpack", "map")
		for _, k := range cards {
			var lp listpack
			mp := make(map[string]struct{}, k)
			var mb [64]byte
			for i := 0; i < k; i++ {
				m := memberBytes(mb[:], i, w)
				lp.add(m)
				mp[string(m)] = struct{}{}
			}
			var pb [64]byte
			scan := timeOp(*iters, *reps, func(i int) {
				m := memberBytes(pb[:], k+(i&1023), w) // always absent
				if lp.has(m) {
					sink++
				}
			})
			probe := timeOp(*iters, *reps, func(i int) {
				m := memberBytes(pb[:], k+(i&1023), w)
				if _, ok := mp[string(m)]; ok {
					sink++
				}
			})
			note := ""
			if k > maxListpackEntries {
				note = "  (past listpack cap)"
			}
			fmt.Printf("%8d  %9.2f  %9.2f%s\n", k, scan, probe, note)
		}
	}

	fmt.Printf("\ncaps under test: intset %d, listpack entries %d, listpack value %dB\n",
		maxIntsetEntries, maxListpackEntries, maxListpackValue)
	if sink < 0 {
		fmt.Println(sink)
	}
}
