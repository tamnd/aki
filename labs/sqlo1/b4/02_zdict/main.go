// zdict prices the trained-dictionary zstd scheme (doc 04 section 11
// scheme 6) against plain zstd (scheme 5) on small Redis-shaped values.
//
// The doc 04 recipe trains a ~112 KiB dictionary on ~100x samples and
// replaces the incumbent only on a >=5% held-out win. This lab measures
// where the dictionary actually pays (value shape, group size), how the
// win scales with dictionary size and training volume, and how fast the
// win decays when the workload shifts, which is what arms the
// replacement trigger.
//
// Workloads (one CSV row per invocation, run.sh sweeps):
//
//	dictsize  arg = dictionary KiB; ratio = dict, x1 = plain, x2 = win pct
//	gsize     arg = values per group at 112 KiB dict; same columns
//	train     arg = training bytes multiple of the dict size; same columns
//	shift     arg = drift fraction p; ratio = incumbent on drifted data,
//	          x1 = plain, x2 = held-out win pct of a fresh candidate over
//	          the incumbent (>=5 means the doc 04 trigger fires)
//
// CSV: shape,n,workload,arg,ratio,enc_ns_val,dec_ns_val,x1,x2
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/klauspost/compress/dict"
	"github.com/klauspost/compress/zstd"
)

// genOne emits one value of the shape. set selects the template family
// so the shift workload can drift the workload without changing the
// shape's byte-size class.
func genOne(shape string, set int, rng *rand.Rand) []byte {
	switch shape {
	case "json":
		if set == 1 {
			evs := []string{"click", "view", "purchase", "signup", "logout"}
			return fmt.Appendf(nil, `{"id":%d,"user":"u%06d","ev":"%s","ts":%d,"ok":%t,"score":%d}`,
				rng.Intn(1<<30), rng.Intn(1000000), evs[rng.Intn(len(evs))],
				1750000000000+rng.Int63n(86400000), rng.Intn(2) == 0, rng.Intn(1000))
		}
		evs := []string{"stream.start", "stream.stop", "seek", "buffer", "quality.change"}
		return fmt.Appendf(nil, `{"session":"%08x%08x","event":"%s","at":%d,"device":"%s","region":"%s"}`,
			rng.Uint32(), rng.Uint32(), evs[rng.Intn(len(evs))],
			1750000000000+rng.Int63n(86400000),
			[]string{"ios", "android", "web", "tv"}[rng.Intn(4)],
			[]string{"us-east", "eu-west", "ap-south"}[rng.Intn(3)])
	case "sess":
		var tok [16]byte
		rng.Read(tok[:])
		scopes := []string{"read", "write", "admin", "billing"}
		if set != 1 {
			scopes = []string{"cart", "checkout", "wishlist", "support"}
		}
		return fmt.Appendf(nil, "sess:%x:uid=%d:exp=%d:scope=%s:ver=3",
			tok, rng.Intn(10000000), 1750000000+rng.Int63n(86400), scopes[rng.Intn(len(scopes))])
	case "user":
		names := []string{"alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"}
		cities := []string{"hanoi", "tokyo", "berlin", "austin", "lagos", "quito"}
		plans := []string{"free", "pro", "team"}
		if set != 1 {
			plans = []string{"starter", "growth", "scale"}
			cities = []string{"osaka", "porto", "denver", "nairobi"}
		}
		return fmt.Appendf(nil, "name=%s;city=%s;plan=%s;score=%d;flags=%03b",
			names[rng.Intn(len(names))], cities[rng.Intn(len(cities))],
			plans[rng.Intn(len(plans))], rng.Intn(100000), rng.Intn(8))
	case "rand":
		v := make([]byte, 64)
		rng.Read(v)
		return v
	}
	panic("unknown shape " + shape)
}

func genCorpus(shape string, n, set int, rng *rand.Rand) [][]byte {
	vals := make([][]byte, n)
	for i := range vals {
		vals[i] = genOne(shape, set, rng)
	}
	return vals
}

// genMix drifts the corpus: each value comes from template set 2 with
// probability p, else set 1.
func genMix(shape string, n int, p float64, rng *rand.Rand) [][]byte {
	vals := make([][]byte, n)
	for i := range vals {
		set := 1
		if rng.Float64() < p {
			set = 2
		}
		vals[i] = genOne(shape, set, rng)
	}
	return vals
}

// frame is the same raw group framing the cascade lab priced: u32 count
// then uvarint length plus bytes per value. Groups compress framed so
// the ratio baselines match across the two labs.
func frame(vals [][]byte) []byte {
	var tmp [binary.MaxVarintLen64]byte
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(vals)))
	for _, v := range vals {
		n := binary.PutUvarint(tmp[:], uint64(len(v)))
		b = append(b, tmp[:n]...)
		b = append(b, v...)
	}
	return b
}

func unframe(b []byte) [][]byte {
	n := binary.LittleEndian.Uint32(b[:4])
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

// trainDict builds a zstd dictionary of at most sizeB bytes from the
// samples. A nil return means training failed (degenerate corpus) and
// the caller falls back to plain zstd, which is also what the engine
// slice must do. BuildZstdDict panics rather than erroring on some
// degenerate corpora (v1.19.1, index out of range in the matcher), so
// the engine trainer needs this recover too.
func trainDict(samples [][]byte, sizeB int) (out []byte) {
	defer func() {
		if recover() != nil {
			out = nil
		}
	}()
	d, err := dict.BuildZstdDict(samples, dict.Options{
		MaxDictSize: sizeB,
		HashBytes:   6,
		ZstdDictID:  907,
		ZstdLevel:   zstd.SpeedDefault,
	})
	if err != nil {
		return nil
	}
	return d
}

// sampleBytes draws fresh values until the total reaches want bytes,
// the reservoir-free stand-in for the doc 04 trainer's sample stream.
func sampleBytes(shape string, set int, want int, rng *rand.Rand) [][]byte {
	var out [][]byte
	total := 0
	for total < want {
		v := genOne(shape, set, rng)
		out = append(out, v)
		total += len(v)
	}
	return out
}

func sampleMixBytes(shape string, p float64, want int, rng *rand.Rand) [][]byte {
	var out [][]byte
	total := 0
	for total < want {
		set := 1
		if rng.Float64() < p {
			set = 2
		}
		v := genOne(shape, set, rng)
		out = append(out, v)
		total += len(v)
	}
	return out
}

type codec struct {
	enc *zstd.Encoder
	dec *zstd.Decoder
}

func newCodec(d []byte) codec {
	eo := []zstd.EOption{zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1)}
	do := []zstd.DOption{zstd.WithDecoderConcurrency(1)}
	if d != nil {
		eo = append(eo, zstd.WithEncoderDict(d))
		do = append(do, zstd.WithDecoderDicts(d))
	}
	enc, err := zstd.NewWriter(nil, eo...)
	if err != nil {
		panic(err)
	}
	dec, err := zstd.NewReader(nil, do...)
	if err != nil {
		panic(err)
	}
	return codec{enc, dec}
}

// measure compresses the corpus in gsize-value groups and returns the
// total compressed to raw ratio plus encode and decode ns per value.
// Every group is round-trip verified once; decode timing is best of 5.
func measure(vals [][]byte, gsize int, c codec) (ratio, encNs, decNs float64) {
	var frames [][]byte
	for i := 0; i < len(vals); i += gsize {
		frames = append(frames, frame(vals[i:min(i+gsize, len(vals))]))
	}
	rawTotal, encTotal := 0, 0
	encoded := make([][]byte, len(frames))
	start := time.Now()
	for i, f := range frames {
		encoded[i] = c.enc.EncodeAll(f, nil)
	}
	encDur := time.Since(start)
	for i, f := range frames {
		rawTotal += len(f)
		encTotal += len(encoded[i])
		got, err := c.dec.DecodeAll(encoded[i], nil)
		if err != nil {
			panic(err)
		}
		if !bytes.Equal(got, f) {
			panic("round trip mismatch")
		}
		dec := unframe(got)
		if len(dec) != min(gsize, len(vals)-i*gsize) {
			panic("unframe count mismatch")
		}
	}
	best := time.Duration(1 << 62)
	for rep := 0; rep < 5; rep++ {
		start = time.Now()
		for i := range encoded {
			if _, err := c.dec.DecodeAll(encoded[i], nil); err != nil {
				panic(err)
			}
		}
		if d := time.Since(start); d < best {
			best = d
		}
	}
	n := float64(len(vals))
	return float64(encTotal) / float64(rawTotal), float64(encDur.Nanoseconds()) / n, float64(best.Nanoseconds()) / n
}

// groupBytes returns total compressed bytes for the corpus in
// gsize-value groups under the codec, the held-out comparison metric.
func groupBytes(vals [][]byte, gsize int, c codec) int {
	total := 0
	for i := 0; i < len(vals); i += gsize {
		total += len(c.enc.EncodeAll(frame(vals[i:min(i+gsize, len(vals))]), nil))
	}
	return total
}

func row(shape string, n int, workload, arg string, ratio, encNs, decNs, x1, x2 float64) {
	fmt.Printf("%s,%d,%s,%s,%.4f,%.1f,%.1f,%.4f,%.2f\n", shape, n, workload, arg, ratio, encNs, decNs, x1, x2)
}

func main() {
	shape := flag.String("shape", "json", "json | sess | user | rand")
	n := flag.Int("n", 50000, "eval corpus size in values")
	workload := flag.String("workload", "dictsize", "dictsize | gsize | train | shift")
	dictKiB := flag.Int("dict", 112, "dictionary size in KiB")
	gsize := flag.Int("gsize", 64, "values per compressed group")
	trainx := flag.Int("trainx", 100, "training bytes as a multiple of the dictionary size")
	drift := flag.Float64("drift", 0, "shift workload: fraction of values from template set 2")
	seed := flag.Int64("seed", 1, "rng seed")
	flag.Parse()

	evalRng := rand.New(rand.NewSource(*seed))
	trainRng := rand.New(rand.NewSource(*seed + 1000))
	dictB := *dictKiB * 1024

	switch *workload {
	case "dictsize", "gsize", "train":
		vals := genCorpus(*shape, *n, 1, evalRng)
		tstart := time.Now()
		d := trainDict(sampleBytes(*shape, 1, *trainx*dictB, trainRng), dictB)
		trainMs := time.Since(tstart).Milliseconds()
		plainRatio, _, _ := measure(vals, *gsize, newCodec(nil))
		ratio, encNs, decNs := plainRatio, 0.0, 0.0
		if d != nil {
			ratio, encNs, decNs = measure(vals, *gsize, newCodec(d))
		} else {
			fmt.Fprintf(os.Stderr, "train failed for %s, falling back to plain\n", *shape)
		}
		win := (plainRatio - ratio) / plainRatio * 100
		arg := fmt.Sprintf("%d", *dictKiB)
		if *workload == "gsize" {
			arg = fmt.Sprintf("%d", *gsize)
		} else if *workload == "train" {
			arg = fmt.Sprintf("%d", *trainx)
		}
		fmt.Fprintf(os.Stderr, "%s %s=%s: dict %d B trained in %d ms\n", *workload, *workload, arg, len(d), trainMs)
		row(*shape, *n, *workload, arg, ratio, encNs, decNs, plainRatio, win)
	case "shift":
		// The incumbent trains on the original workload; the corpus
		// then drifts and a fresh candidate trains on the drifted
		// stream. x2 is the candidate's held-out byte win over the
		// incumbent, the number the 5% replacement trigger reads.
		incumbent := trainDict(sampleBytes(*shape, 1, *trainx*dictB, trainRng), dictB)
		candidate := trainDict(sampleMixBytes(*shape, *drift, *trainx*dictB, trainRng), dictB)
		if incumbent == nil || candidate == nil {
			fmt.Fprintf(os.Stderr, "train failed for %s, no shift row\n", *shape)
			return
		}
		vals := genMix(*shape, *n, *drift, evalRng)
		heldout := genMix(*shape, *n, *drift, evalRng)
		ratio, _, _ := measure(vals, *gsize, newCodec(incumbent))
		plainRatio, _, _ := measure(vals, *gsize, newCodec(nil))
		incB := groupBytes(heldout, *gsize, newCodec(incumbent))
		candB := groupBytes(heldout, *gsize, newCodec(candidate))
		win := float64(incB-candB) / float64(incB) * 100
		row(*shape, *n, "shift", fmt.Sprintf("%.2f", *drift), ratio, 0, 0, plainRatio, win)
	default:
		fmt.Fprintf(os.Stderr, "unknown workload %s\n", *workload)
		os.Exit(2)
	}
}
