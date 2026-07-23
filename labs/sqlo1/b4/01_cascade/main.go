// Lab: cascade encoder selection on Redis-shaped values (spec 2064/sqlo1
// doc 04 section 11, milestone B4 lab 01).
//
// The compression pipeline picks a scheme per compaction output group:
// integer-shaped groups try frame-of-reference bit-packing, string-shaped
// groups try dictionary and dictionary-plus-RLE, and anything where the
// lightweight winner saves less than the floor falls through to zstd
// against raw. Two constants get baked from this lab: the minimum-win
// floor (the spec carries 8 percent from BtrBlocks) and the sample rate
// the selector scores on (the spec carries 1 percent, right 77 percent
// of the time in their corpus). Both need our own curves on Redis-shaped
// data, not theirs: counter strings, timestamp strings, binary u64 runs,
// UUIDs, small JSON bodies, and the mixed bag a real keyspace is.
//
// The lab prices every encoder on every shape (ratio, encode ns per
// value, decode ns per value), then runs the selection rule over
// group-sized windows sweeping the floor, and finally scores the
// sampled selector against the full-group oracle. Decode cost is the
// number that matters most downstream: the read path pays it on every
// cold group, so a scheme that wins 2 percent of size and triples
// decode ns is a loss the floor exists to refuse.
//
// Encoders carry the spec's scheme ids: 0 raw, 1 dict, 2 dict+RLE,
// 3 FOR+pack (ascii decimal or 8-byte LE u64), 5 plain zstd
// (klauspost, SpeedDefault). Trained-dictionary zstd is lab 02.
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/klauspost/compress/zstd"
)

// ---- corpus generators ----

// genShape returns n values shaped like the named Redis population.
// Every generator is deterministic under the seed so encoder rows and
// selection rows price the same bytes.
func genShape(shape string, n int, rng *rand.Rand) [][]byte {
	vals := make([][]byte, 0, n)
	switch shape {
	case "counters":
		// Per-key INCR bodies: a hot set of small counters, decimal
		// ASCII, heavy repeats in the low range.
		hot := make([]uint64, 256)
		for i := 0; i < n; i++ {
			k := rng.Intn(len(hot))
			hot[k] += uint64(rng.Intn(3) + 1)
			vals = append(vals, []byte(fmt.Sprintf("%d", hot[k])))
		}
	case "timestamps":
		// Near-monotonic unix milliseconds, decimal ASCII, the
		// last-seen and rate-limit window population.
		ts := uint64(1750000000000)
		for i := 0; i < n; i++ {
			ts += uint64(rng.Intn(5000))
			vals = append(vals, []byte(fmt.Sprintf("%d", ts)))
		}
	case "u64s":
		// Binary 8-byte LE values, the zset score-run and stream
		// seq shape: near-sorted with local jitter.
		v := uint64(1 << 40)
		for i := 0; i < n; i++ {
			v += uint64(rng.Intn(1 << 16))
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], v)
			vals = append(vals, append([]byte(nil), b[:]...))
		}
	case "uuids":
		for i := 0; i < n; i++ {
			var u [16]byte
			rng.Read(u[:])
			vals = append(vals, []byte(fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])))
		}
	case "json":
		states := []string{"active", "pending", "closed", "archived", "flagged"}
		words := []string{"retry", "checkout", "batch", "sync", "flush", "verify", "resend", "expire"}
		for i := 0; i < n; i++ {
			note := words[rng.Intn(len(words))] + " " + words[rng.Intn(len(words))]
			v := fmt.Sprintf(`{"id":%d,"user":"user_%d","state":%q,"score":%d.%02d,"ts":%d,"tags":["a","b"],"note":%q}`,
				rng.Intn(1000000), rng.Intn(50000), states[rng.Intn(len(states))],
				rng.Intn(100), rng.Intn(100), 1750000000+rng.Intn(1000000), note)
			vals = append(vals, []byte(v))
		}
	case "mixed":
		// The keyspace blend: 40 percent counters, 20 each of
		// timestamps, uuids, and json, interleaved per record the
		// way a compaction stream would see a mixed extent.
		parts := [][][]byte{
			genShape("counters", n*4/10, rng),
			genShape("timestamps", n*2/10, rng),
			genShape("uuids", n*2/10, rng),
			genShape("json", n-n*4/10-2*(n*2/10), rng),
		}
		idx := make([]int, len(parts))
		for len(vals) < n {
			p := rng.Intn(len(parts))
			if idx[p] < len(parts[p]) {
				vals = append(vals, parts[p][idx[p]])
				idx[p]++
			}
		}
	default:
		panic("unknown shape " + shape)
	}
	return vals
}

// intShaped reports whether shape carries the integer tag the per-type
// docs would put on its groups; the selector uses it to pick which
// lightweight encoders to try, the doc 04 step 3 and 4 split.
func intShaped(shape string) bool {
	return shape == "counters" || shape == "timestamps" || shape == "u64s"
}

// ---- codecs ----

// rawEncode is scheme 0 and the framing every ratio is measured
// against: u32 count then varint length plus bytes per value.
func rawEncode(vals [][]byte) []byte {
	out := make([]byte, 4, 64)
	binary.LittleEndian.PutUint32(out, uint32(len(vals)))
	for _, v := range vals {
		out = binary.AppendUvarint(out, uint64(len(v)))
		out = append(out, v...)
	}
	return out
}

func rawDecode(b []byte) [][]byte {
	n := binary.LittleEndian.Uint32(b)
	b = b[4:]
	vals := make([][]byte, n)
	for i := range vals {
		l, k := binary.Uvarint(b)
		b = b[k:]
		vals[i] = b[:l:l]
		b = b[l:]
	}
	return vals
}

// dictEncode is scheme 1: first-appearance dictionary plus varint
// indexes. dictRLE is scheme 2: same dictionary with the index stream
// run-length coded, the win when repeats cluster.
func dictBuild(vals [][]byte) (dict [][]byte, idx []uint32) {
	seen := make(map[string]uint32, 64)
	idx = make([]uint32, len(vals))
	for i, v := range vals {
		id, ok := seen[string(v)]
		if !ok {
			id = uint32(len(dict))
			dict = append(dict, v)
			seen[string(v)] = id
		}
		idx[i] = id
	}
	return dict, idx
}

func dictHeader(dict [][]byte, nvals int) []byte {
	out := make([]byte, 0, 64)
	out = binary.AppendUvarint(out, uint64(len(dict)))
	for _, d := range dict {
		out = binary.AppendUvarint(out, uint64(len(d)))
		out = append(out, d...)
	}
	out = binary.AppendUvarint(out, uint64(nvals))
	return out
}

func dictEncode(vals [][]byte) []byte {
	dict, idx := dictBuild(vals)
	out := dictHeader(dict, len(vals))
	for _, id := range idx {
		out = binary.AppendUvarint(out, uint64(id))
	}
	return out
}

func dictDecodeHeader(b []byte) (dict [][]byte, nvals int, rest []byte) {
	nd, k := binary.Uvarint(b)
	b = b[k:]
	dict = make([][]byte, nd)
	for i := range dict {
		l, k := binary.Uvarint(b)
		b = b[k:]
		dict[i] = b[:l:l]
		b = b[l:]
	}
	nv, k := binary.Uvarint(b)
	return dict, int(nv), b[k:]
}

func dictDecode(b []byte) [][]byte {
	dict, nv, b := dictDecodeHeader(b)
	vals := make([][]byte, nv)
	for i := range vals {
		id, k := binary.Uvarint(b)
		b = b[k:]
		vals[i] = dict[id]
	}
	return vals
}

func dictRLEEncode(vals [][]byte) []byte {
	dict, idx := dictBuild(vals)
	out := dictHeader(dict, len(vals))
	for i := 0; i < len(idx); {
		j := i + 1
		for j < len(idx) && idx[j] == idx[i] {
			j++
		}
		out = binary.AppendUvarint(out, uint64(idx[i]))
		out = binary.AppendUvarint(out, uint64(j-i))
		i = j
	}
	return out
}

func dictRLEDecode(b []byte) [][]byte {
	dict, nv, b := dictDecodeHeader(b)
	vals := make([][]byte, 0, nv)
	for len(vals) < nv {
		id, k := binary.Uvarint(b)
		b = b[k:]
		run, k := binary.Uvarint(b)
		b = b[k:]
		for j := uint64(0); j < run; j++ {
			vals = append(vals, dict[id])
		}
	}
	return vals
}

// forPack is scheme 3: frame-of-reference plus bit-packing in 1024-value
// blocks, each block packed at its own width over its own base so the
// near-sorted shapes get narrow words. Values are canonical decimal
// ASCII (mode 0) or 8-byte LE u64 (mode 1); anything else is not
// integer-shaped and the encoder refuses.
const forBlock = 1024

func parseCanonicalDec(v []byte) (uint64, bool) {
	if len(v) == 0 || len(v) > 20 || (len(v) > 1 && v[0] == '0') {
		return 0, false
	}
	var x uint64
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if x > (1<<64-1-d)/10 {
			return 0, false
		}
		x = x*10 + d
	}
	return x, true
}

func forApplicable(vals [][]byte) (mode int, ok bool) {
	if len(vals) == 0 {
		return 0, false
	}
	bin := true
	for _, v := range vals {
		if len(v) != 8 {
			bin = false
			break
		}
	}
	if bin {
		return 1, true
	}
	for _, v := range vals {
		if _, ok := parseCanonicalDec(v); !ok {
			return 0, false
		}
	}
	return 0, true
}

func forInts(vals [][]byte, mode int) []uint64 {
	xs := make([]uint64, len(vals))
	for i, v := range vals {
		if mode == 1 {
			xs[i] = binary.LittleEndian.Uint64(v)
		} else {
			xs[i], _ = parseCanonicalDec(v)
		}
	}
	return xs
}

func forPackEncode(vals [][]byte) []byte {
	mode, ok := forApplicable(vals)
	if !ok {
		panic("forPack on non-integer values")
	}
	xs := forInts(vals, mode)
	out := make([]byte, 0, len(xs))
	out = append(out, byte(mode))
	out = binary.AppendUvarint(out, uint64(len(xs)))
	for b := 0; b < len(xs); b += forBlock {
		blk := xs[b:min(b+forBlock, len(xs))]
		base := blk[0]
		for _, x := range blk {
			if x < base {
				base = x
			}
		}
		width := 0
		for _, x := range blk {
			w := bitsLen(x - base)
			if w > width {
				width = w
			}
		}
		out = binary.AppendUvarint(out, base)
		out = append(out, byte(width))
		var word uint64
		fill := 0
		for _, x := range blk {
			d := x - base
			word |= d << fill
			fill += width
			if fill >= 64 {
				out = binary.LittleEndian.AppendUint64(out, word)
				fill -= 64
				if width > 0 && fill > 0 {
					word = d >> (width - fill)
				} else {
					word = 0
				}
			}
		}
		if fill > 0 {
			out = binary.LittleEndian.AppendUint64(out, word)
		}
	}
	return out
}

func forPackDecode(b []byte) [][]byte {
	mode := int(b[0])
	b = b[1:]
	n, k := binary.Uvarint(b)
	b = b[k:]
	xs := make([]uint64, 0, n)
	for uint64(len(xs)) < n {
		blkN := int(min(uint64(forBlock), n-uint64(len(xs))))
		base, k := binary.Uvarint(b)
		b = b[k:]
		width := int(b[0])
		b = b[1:]
		if width == 0 {
			for j := 0; j < blkN; j++ {
				xs = append(xs, base)
			}
			continue
		}
		mask := uint64(1)<<width - 1
		if width == 64 {
			mask = ^uint64(0)
		}
		words := (blkN*width + 63) / 64
		var word uint64
		fill := 0
		wi := 0
		for j := 0; j < blkN; j++ {
			if fill < width {
				next := binary.LittleEndian.Uint64(b[wi*8:])
				d := (word | next<<fill) & mask
				xs = append(xs, base+d)
				word = next >> (width - fill)
				fill += 64 - width
				wi++
			} else {
				xs = append(xs, base+(word&mask))
				word >>= width
				fill -= width
			}
		}
		b = b[words*8:]
	}
	vals := make([][]byte, len(xs))
	for i, x := range xs {
		if mode == 1 {
			var v [8]byte
			binary.LittleEndian.PutUint64(v[:], x)
			vals[i] = append([]byte(nil), v[:]...)
		} else {
			vals[i] = strconv.AppendUint(make([]byte, 0, 20), x, 10)
		}
	}
	return vals
}

func bitsLen(x uint64) int {
	n := 0
	for x > 0 {
		n++
		x >>= 1
	}
	return n
}

// zstd is scheme 5: the raw framing through a plain klauspost encoder
// at SpeedDefault, the fallback everything falls through to.
var (
	zEnc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	zDec, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
)

func zstdEncode(vals [][]byte) []byte {
	return zEnc.EncodeAll(rawEncode(vals), nil)
}

func zstdDecode(b []byte) [][]byte {
	raw, err := zDec.DecodeAll(b, nil)
	if err != nil {
		panic(err)
	}
	return rawDecode(raw)
}

// ---- encoder table ----

type encoder struct {
	name       string
	scheme     int
	applicable func([][]byte) bool
	encode     func([][]byte) []byte
	decode     func([]byte) [][]byte
}

var encoders = []encoder{
	{"raw", 0, func([][]byte) bool { return true }, rawEncode, rawDecode},
	{"dict", 1, func([][]byte) bool { return true }, dictEncode, dictDecode},
	{"dictrle", 2, func([][]byte) bool { return true }, dictRLEEncode, dictRLEDecode},
	{"forpack", 3, func(v [][]byte) bool { _, ok := forApplicable(v); return ok }, forPackEncode, forPackDecode},
	{"zstd", 5, func([][]byte) bool { return true }, zstdEncode, zstdDecode},
}

func valsEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// ---- selection ----

// selectScheme is the doc 04 step 3-5 rule over one group: try the
// shape's lightweight encoders, keep the best only if it saves at
// least floor (a fraction of raw) over raw, otherwise fall through to
// zstd versus raw. Returns the winning encoder index into encoders.
func selectScheme(vals [][]byte, integer bool, floor float64) int {
	rawLen := len(rawEncode(vals))
	best, bestLen := -1, rawLen
	var tries []int
	if integer {
		tries = []int{3}
	} else {
		tries = []int{1, 2}
	}
	for _, ei := range tries {
		if !encoders[ei].applicable(vals) {
			continue
		}
		l := len(encoders[ei].encode(vals))
		if l < bestLen {
			best, bestLen = ei, l
		}
	}
	if best >= 0 && float64(rawLen-bestLen) >= floor*float64(rawLen) {
		return best
	}
	if l := len(zstdEncode(vals)); l < rawLen {
		return 4
	}
	return 0
}

// oracleScheme is the full-knowledge pick: smallest encoding across
// every applicable encoder, ties to the cheaper decode (lower scheme).
func oracleScheme(vals [][]byte) int {
	best, bestLen := 0, len(rawEncode(vals))
	for ei := 1; ei < len(encoders); ei++ {
		if !encoders[ei].applicable(vals) {
			continue
		}
		if l := len(encoders[ei].encode(vals)); l < bestLen {
			best, bestLen = ei, l
		}
	}
	return best
}

func main() {
	shape := flag.String("shape", "mixed", "counters|timestamps|u64s|uuids|json|mixed")
	n := flag.Int("n", 100000, "values in the corpus")
	gsize := flag.Int("gsize", 256, "values per selection group")
	workload := flag.String("workload", "encoder", "encoder|select|sample")
	floor := flag.Float64("floor", 0.08, "minimum lightweight win as a fraction of raw")
	rate := flag.Float64("rate", 0.01, "sample rate for the sampled selector")
	seed := flag.Int64("seed", 1, "corpus seed")
	flag.Parse()

	rng := rand.New(rand.NewSource(*seed))
	vals := genShape(*shape, *n, rng)
	rawLen := len(rawEncode(vals))

	switch *workload {
	case "encoder":
		for _, e := range encoders {
			if !e.applicable(vals) {
				fmt.Printf("%s,%d,encoder,%s,,,,0,0\n", *shape, *n, e.name)
				continue
			}
			t0 := time.Now()
			enc := e.encode(vals)
			encNs := float64(time.Since(t0).Nanoseconds()) / float64(*n)
			var dec [][]byte
			best := int64(1 << 62)
			for r := 0; r < 5; r++ {
				t1 := time.Now()
				dec = e.decode(enc)
				if d := time.Since(t1).Nanoseconds(); d < best {
					best = d
				}
			}
			if !valsEqual(vals, dec) {
				fmt.Fprintf(os.Stderr, "%s: round trip failed on %s\n", e.name, *shape)
				os.Exit(1)
			}
			fmt.Printf("%s,%d,encoder,%s,%.4f,%.1f,%.1f,1,%d\n",
				*shape, *n, e.name, float64(len(enc))/float64(rawLen), encNs,
				float64(best)/float64(*n), len(enc))
		}
	case "select":
		integer := intShaped(*shape)
		total, chosen := 0, map[string]int{}
		decNs := int64(0)
		groups := 0
		for i := 0; i < len(vals); i += *gsize {
			g := vals[i:min(i+*gsize, len(vals))]
			ei := selectScheme(g, integer, *floor)
			enc := encoders[ei].encode(g)
			total += len(enc)
			chosen[encoders[ei].name]++
			t1 := time.Now()
			encoders[ei].decode(enc)
			decNs += time.Since(t1).Nanoseconds()
			groups++
		}
		light := chosen["dict"] + chosen["dictrle"] + chosen["forpack"]
		fmt.Printf("%s,%d,select,%.2f,%.4f,,%.1f,%.1f,%.1f\n",
			*shape, *n, *floor, float64(total)/float64(rawLen),
			float64(decNs)/float64(len(vals)),
			100*float64(light)/float64(groups), 100*float64(chosen["zstd"])/float64(groups))
	case "sample":
		integer := intShaped(*shape)
		match, groups := 0, 0
		sampledBytes, oracleBytes := 0, 0
		for i := 0; i < len(vals); i += *gsize {
			g := vals[i:min(i+*gsize, len(vals))]
			ns := int(float64(len(g)) * *rate)
			if ns < 8 {
				ns = 8
			}
			if ns > len(g) {
				ns = len(g)
			}
			stride := len(g) / ns
			samp := make([][]byte, 0, ns)
			for j := 0; j < len(g); j += stride {
				samp = append(samp, g[j])
			}
			si := selectScheme(samp, integer, *floor)
			oi := selectScheme(g, integer, *floor)
			if si == oi {
				match++
			}
			sampledBytes += len(encoders[si].encode(g))
			oracleBytes += len(encoders[oi].encode(g))
			groups++
		}
		fmt.Printf("%s,%d,sample,%.3f,%.4f,,,%.1f,%.2f\n",
			*shape, *n, *rate, float64(sampledBytes)/float64(rawLen),
			100*float64(match)/float64(groups),
			100*(float64(sampledBytes)/float64(oracleBytes)-1))
	default:
		fmt.Fprintln(os.Stderr, "unknown workload", *workload)
		os.Exit(1)
	}
}
