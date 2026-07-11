// Lab: score codec (spec 2064/f3 doc 12 section 3, M2 lab 03).
//
// The question: the native-band tree keys on (sortable score, member) so that
// memcmp on the tree's byte keys equals the zset total order (doc 12 sections
// 2.3, 3.1). Raw IEEE-754 doubles do not byte-sort: the sign bit is high and
// negative magnitudes run backwards. The doc prescribes the standard
// order-preserving transform (flip the sign bit for non-negative, invert every
// bit for negative) so -inf is the smallest 8 bytes, +inf the largest, and
// ordinary doubles fall in numeric order between. This lab settles three things
// the tree slice and the dual-write slice bake in: whether the tree should key
// on the sortable u64 or compare raw floats in the node, what the transform
// costs to apply and invert, and whether the literal transform actually honors
// the total order at the edges the contract names (-0.0 versus +0.0, the
// infinities), plus the geo groundwork (52-bit geohash integers stored as
// float64) M6 rides.
//
// Method: in-process, no server, no wire, no engine import. Lab-local kernels
// model the tree's three candidate order-key forms. rawFloat keeps the score as
// a float64 in the node and compares with a float less. u64 keys on the
// order-preserving transform and compares the u64. byteComposite stores the
// 8-byte big-endian sortable prefix followed by the member bytes as one blob
// and compares with a single bytes.Compare, the form doc 12 section 2.3 draws
// its separator and leaf layouts as. All three break score ties by member
// bytes, the doc's (score, member) key. The descent kernel walks depth interior
// nodes of arity separators each, linear-scanning the separators the way an
// arity-16 B+ node does without SIMD, so the timed loop is comparison-bound.
// Two regimes: distinct scores (the common case, where the score compare
// decides and the member is never touched) and a fully tied band at score 0
// (the autocomplete and lex shape of section 3.2, where every compare falls
// through to the member bytes).
//
// Axes: cardinality {1e3, 1e4, 1e6} mapped to tree depth at arity 16; member
// size {8, 24} bytes, 24 carrying a shared prefix so the tied-band memcmp cost
// is visible; order-key form {rawFloat, u64, byteComposite}; regime {distinct,
// tied}. Read: ns per descent for each form (what the tree slice compares when
// it picks a node layout), ns per encode and per decode for the transform (what
// the dual-write slice pays per insert and what a range-bound decode costs),
// and the property findings the codec must guarantee. See README.md for the
// sweep and the frozen verdict.
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"
)

const (
	signBit = uint64(1) << 63
	arity   = 16 // doc 12 section 2.3: arity-16 nodes, 15 separators each
)

// scoreKey maps a float64 score to the order-preserving u64 the tree stores as
// its 8-byte big-endian separator and leaf prefix (doc 12 section 3.1). Signed
// zero is normalized to +0 before the transform so -0.0 and +0.0 collapse to
// one key: Redis orders the two zeros as equal, so a member at -0.0 and one at
// +0.0 must order by member bytes, not by the sign of a zero. The literal
// transform in the doc's code block does not do this on its own (see
// scoreKeyNaive), which is the codec's one real trap and the reason ZSCORE
// reads raw bits from the member hash rather than decoding the tree key.
func scoreKey(f float64) uint64 {
	if f == 0 { // true for both +0.0 and -0.0
		f = 0 // positive zero
	}
	b := math.Float64bits(f)
	if b&signBit == 0 {
		return b ^ signBit
	}
	return ^b
}

// scoreKeyNaive is the transform exactly as written in doc 12 section 3.1's
// code block, with no signed-zero normalization. It is kept to measure the
// transform in isolation and, in the tests, to demonstrate that it splits -0.0
// below +0.0 and so breaks the total order when both zeros are present.
func scoreKeyNaive(f float64) uint64 {
	b := math.Float64bits(f)
	if b&signBit == 0 {
		return b ^ signBit
	}
	return ^b
}

// scoreFromKey inverts scoreKeyNaive exactly for every non-NaN float64: it is
// the tree-side decode a range bound or a cold-chunk directory key uses when it
// must recover a score from a key. It is never on the ZSCORE path, which reads
// the raw double bits kept in the member-hash slot (doc 12 section 2.6). Note
// that scoreFromKey(scoreKey(-0.0)) is +0.0, not -0.0, because scoreKey
// normalized the sign away on purpose; the raw-bits copy in the hash is what
// carries the sign to ZSCORE.
func scoreFromKey(k uint64) float64 {
	if k&signBit != 0 { // high bit set: the score was non-negative
		return math.Float64frombits(k ^ signBit)
	}
	return math.Float64frombits(^k)
}

// zsetLess is the reference total order: score ascending with -0.0 equal to
// +0.0 (Go's float == already treats them equal, and != is false, so the
// comparison falls through to the member bytes), ties broken by member bytes,
// the infinities ordering as ordinary floats. NaN never reaches here; the
// parser rejects it at the door.
func zsetLess(sa float64, ma []byte, sb float64, mb []byte) bool {
	if sa != sb {
		return sa < sb
	}
	return bytes.Compare(ma, mb) < 0
}

// keyLess is the order the tree actually enforces: the sortable u64 compared as
// an integer, ties by member bytes. It must agree with zsetLess for every
// non-NaN score pair, which is the codec's total-order correctness property.
func keyLess(ka uint64, ma []byte, kb uint64, mb []byte) bool {
	if ka != kb {
		return ka < kb
	}
	return bytes.Compare(ma, mb) < 0
}

// node holds one interior node's separators in all three forms, built from the
// same underlying scores and members so the three descent kernels do identical
// comparison work and only the compared representation differs.
type node struct {
	scores  []float64
	keys    []uint64
	members [][]byte
	blobs   [][]byte // 8B big-endian key prefix followed by member bytes
}

// tree is depth interior nodes, the comparison-bound skeleton of a descent. The
// child pointers and subtree counts a real B+ tree carries are not modeled: the
// lab times the separator scan, which is where the order-key form shows up.
type tree struct {
	levels []node
}

func makeMember(r *rand.Rand, size int, shared bool) []byte {
	m := make([]byte, size)
	r.Read(m)
	if shared && size > 6 {
		// A shared leading prefix so a tied-band memcmp must walk past it,
		// modeling the URL-style members of doc 12 section 3.2.
		copy(m, "https:")
	}
	return m
}

func buildTree(r *rand.Rand, depth, memberSize int, tied, shared bool) tree {
	t := tree{levels: make([]node, depth)}
	for d := 0; d < depth; d++ {
		n := node{}
		for i := 0; i < arity-1; i++ {
			var sc float64
			if tied {
				sc = 0
			} else {
				sc = r.NormFloat64() * 1e6
			}
			m := makeMember(r, memberSize, shared)
			n.scores = append(n.scores, sc)
			n.keys = append(n.keys, scoreKey(sc))
			n.members = append(n.members, m)
			blob := make([]byte, 8+len(m))
			binary.BigEndian.PutUint64(blob, scoreKey(sc))
			copy(blob[8:], m)
			n.blobs = append(n.blobs, blob)
		}
		// Separators within a node are sorted, as a real node keeps them.
		idx := make([]int, arity-1)
		for i := range idx {
			idx[i] = i
		}
		sort.Slice(idx, func(a, b int) bool {
			return keyLess(n.keys[idx[a]], n.members[idx[a]], n.keys[idx[b]], n.members[idx[b]])
		})
		sn := node{}
		for _, j := range idx {
			sn.scores = append(sn.scores, n.scores[j])
			sn.keys = append(sn.keys, n.keys[j])
			sn.members = append(sn.members, n.members[j])
			sn.blobs = append(sn.blobs, n.blobs[j])
		}
		t.levels[d] = sn
	}
	return t
}

// descendRaw walks every level linear-scanning the float separators, returning
// the count of separators the target sorts at or after, summed over the path.
// The return value is only there to defeat dead-code elimination.
func descendRaw(t tree, sc float64, m []byte) int {
	acc := 0
	for d := range t.levels {
		n := t.levels[d]
		for i := range n.scores {
			if !zsetLess(sc, m, n.scores[i], n.members[i]) {
				acc++
			}
		}
	}
	return acc
}

func descendU64(t tree, k uint64, m []byte) int {
	acc := 0
	for d := range t.levels {
		n := t.levels[d]
		for i := range n.keys {
			if !keyLess(k, m, n.keys[i], n.members[i]) {
				acc++
			}
		}
	}
	return acc
}

func descendBlob(t tree, blob []byte) int {
	acc := 0
	for d := range t.levels {
		n := t.levels[d]
		for i := range n.blobs {
			if bytes.Compare(blob, n.blobs[i]) >= 0 {
				acc++
			}
		}
	}
	return acc
}

func depthFor(card int) int {
	// ceil(log_arity(card)), the interior levels an arity-16 tree descends.
	d := 1
	span := arity
	for span < card {
		span *= arity
		d++
	}
	return d
}

func median(xs []float64) float64 {
	sort.Float64s(xs)
	return xs[len(xs)/2]
}

func benchDescend(reps, descents, memberSize int, tied, shared bool) (raw, u64, blob float64) {
	r := rand.New(rand.NewSource(1))
	rawReps := make([]float64, reps)
	u64Reps := make([]float64, reps)
	blobReps := make([]float64, reps)
	for _, card := range descendCards {
		depth := depthFor(card)
		t := buildTree(r, depth, memberSize, tied, shared)
		// Pre-generate targets so the timed loop does no allocation.
		scs := make([]float64, descents)
		ms := make([][]byte, descents)
		ks := make([]uint64, descents)
		blobs := make([][]byte, descents)
		for i := 0; i < descents; i++ {
			if tied {
				scs[i] = 0
			} else {
				scs[i] = r.NormFloat64() * 1e6
			}
			ms[i] = makeMember(r, memberSize, shared)
			ks[i] = scoreKey(scs[i])
			b := make([]byte, 8+memberSize)
			binary.BigEndian.PutUint64(b, ks[i])
			copy(b[8:], ms[i])
			blobs[i] = b
		}
		for rep := 0; rep < reps; rep++ {
			sink := 0
			start := time.Now()
			for i := 0; i < descents; i++ {
				sink += descendRaw(t, scs[i], ms[i])
			}
			rawReps[rep] = float64(time.Since(start).Nanoseconds()) / float64(descents)

			sink = 0
			start = time.Now()
			for i := 0; i < descents; i++ {
				sink += descendU64(t, ks[i], ms[i])
			}
			u64Reps[rep] = float64(time.Since(start).Nanoseconds()) / float64(descents)

			sink = 0
			start = time.Now()
			for i := 0; i < descents; i++ {
				sink += descendBlob(t, blobs[i])
			}
			blobReps[rep] = float64(time.Since(start).Nanoseconds()) / float64(descents)
			_ = sink
		}
		regime := "distinct"
		if tied {
			regime = "tied"
		}
		fmt.Printf("  card %-8d depth %d  member %2dB %-8s  raw %6.1f  u64 %6.1f  blob %6.1f  ns/descent\n",
			card, depth, memberSize, regime, median(rawReps), median(u64Reps), median(blobReps))
		raw, u64, blob = median(rawReps), median(u64Reps), median(blobReps)
	}
	return raw, u64, blob
}

func benchTransform(reps, n int) (enc, dec float64) {
	r := rand.New(rand.NewSource(2))
	scores := make([]float64, n)
	keys := make([]uint64, n)
	for i := range scores {
		scores[i] = r.NormFloat64() * 1e6
		keys[i] = scoreKey(scores[i])
	}
	encReps := make([]float64, reps)
	decReps := make([]float64, reps)
	for rep := 0; rep < reps; rep++ {
		var ks uint64
		start := time.Now()
		for i := 0; i < n; i++ {
			ks += scoreKey(scores[i])
		}
		encReps[rep] = float64(time.Since(start).Nanoseconds()) / float64(n)

		var fs float64
		start = time.Now()
		for i := 0; i < n; i++ {
			fs += scoreFromKey(keys[i])
		}
		decReps[rep] = float64(time.Since(start).Nanoseconds()) / float64(n)
		_ = ks
		_ = fs
	}
	return median(encReps), median(decReps)
}

var descendCards = []int{1_000, 10_000, 1_000_000}

func main() {
	reps := flag.Int("reps", 7, "timed reps per cell, median reported")
	descents := flag.Int("descents", 2_000_000, "descents timed per rep")
	transformN := flag.Int("transform", 20_000_000, "encode/decode ops timed per rep")
	flag.Parse()

	fmt.Println("score codec lab, spec 2064/f3 doc 12 section 3")
	fmt.Println()

	fmt.Println("descent comparison cost, distinct scores:")
	benchDescend(*reps, *descents, 8, false, false)
	benchDescend(*reps, *descents, 24, false, false)
	fmt.Println()

	fmt.Println("descent comparison cost, fully tied band at score 0:")
	benchDescend(*reps, *descents, 8, true, false)
	benchDescend(*reps, *descents, 24, true, true)
	fmt.Println()

	enc, dec := benchTransform(*reps, *transformN)
	fmt.Printf("transform cost: encode %.2f ns/op  decode %.2f ns/op\n", enc, dec)
}
