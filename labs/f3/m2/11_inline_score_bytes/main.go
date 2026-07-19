// Lab: inline-band score bytes (spec 2064/f3 doc 12 section 4, M2 lab 11).
//
// The question: the listpack-class inline band stores each entry as
// [len:u8][tag:u8][member][score]. The score was a flat 8-byte IEEE-754 double,
// so a tiny integer-scored zset (the common rank, leaderboard, and timestamp
// shape) paid 8 bytes per member for a score that fits in one. Redis's listpack
// integer-encodes a small score to one or two bytes and only spends the full
// float width on a fractional score. On the tiny-collection memory row (M2-G10)
// that fixed 8-byte score is the inline band's largest avoidable cost against
// the rival listpack. This lab settles two things the codec slice bakes in: how
// many bytes the class-tagged encoding actually saves across realistic score
// distributions, and what the tag-and-branch costs to encode and decode per
// entry versus the old fixed PutUint64 / Float64frombits pair.
//
// Method: in-process, no server, no wire, no engine import. Lab-local kernels
// mirror the engine codec exactly (codec.go): putScore writes a one-byte class
// tag (int8, int16, int32, or float64 fallback) followed by a width-matched
// payload, readScore decodes it. The blob model builds a whole tiny-zset inline
// blob both ways and reports total bytes, the figure that drives the VmHWM peak
// the memory row reads. Score distributions: small integers (0..255, ranks and
// small counters), timestamps (unix seconds, int32 band), large integers past
// int32 (float fallback, lossless), and fractional scores (float fallback).
//
// Axes: members per zset {4, 8, 16, 128}, member size {2, 8, 20} bytes, score
// distribution {smallint, timestamp, bigint, fraction}. Read: bytes per entry
// old vs new and the whole-blob delta for a tiny zset (what the memory row
// wins), plus ns per encode and per decode (what the write and read paths pay).
// See README.md for the sweep and the frozen verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"
)

const (
	clsF64 byte = 0
	clsI8  byte = 1
	clsI16 byte = 2
	clsI32 byte = 4
)

// intScore mirrors the engine: integer inside the int32 band, else float
// fallback.
func intScore(f float64) (int32, bool) {
	if f < math.MinInt32 || f > math.MaxInt32 {
		return 0, false
	}
	if f != math.Trunc(f) {
		return 0, false
	}
	return int32(f), true
}

func encWidth(score float64) int {
	iv, ok := intScore(score)
	if !ok {
		return 9
	}
	switch {
	case iv >= math.MinInt8 && iv <= math.MaxInt8:
		return 2
	case iv >= math.MinInt16 && iv <= math.MaxInt16:
		return 3
	default:
		return 5
	}
}

func putScore(b []byte, score float64) int {
	iv, ok := intScore(score)
	if !ok {
		b[0] = clsF64
		binary.BigEndian.PutUint64(b[1:], math.Float64bits(score))
		return 9
	}
	switch {
	case iv >= math.MinInt8 && iv <= math.MaxInt8:
		b[0] = clsI8
		b[1] = byte(int8(iv))
		return 2
	case iv >= math.MinInt16 && iv <= math.MaxInt16:
		b[0] = clsI16
		binary.BigEndian.PutUint16(b[1:], uint16(int16(iv)))
		return 3
	default:
		b[0] = clsI32
		binary.BigEndian.PutUint32(b[1:], uint32(iv))
		return 5
	}
}

func readScore(b []byte) (float64, int) {
	switch b[0] {
	case clsI8:
		return float64(int8(b[1])), 2
	case clsI16:
		return float64(int16(binary.BigEndian.Uint16(b[1:]))), 3
	case clsI32:
		return float64(int32(binary.BigEndian.Uint32(b[1:]))), 5
	default:
		return math.Float64frombits(binary.BigEndian.Uint64(b[1:])), 9
	}
}

// blobBytesOld models the pre-codec inline blob: 2-byte header plus member plus
// a flat 8-byte score per entry.
func blobBytesOld(members int, memberSize int) int {
	return members * (2 + memberSize + 8)
}

// blobBytesNew models the class-tagged inline blob over a score distribution.
func blobBytesNew(scores []float64, memberSize int) int {
	total := 0
	for _, s := range scores {
		total += 2 + memberSize + encWidth(s)
	}
	return total
}

func drawScores(r *rand.Rand, n int, dist string) []float64 {
	out := make([]float64, n)
	for i := range out {
		switch dist {
		case "smallint":
			out[i] = float64(r.Intn(256))
		case "timestamp":
			out[i] = float64(1_700_000_000 + r.Intn(1_000_000))
		case "bigint":
			out[i] = float64(int64(3_000_000_000) + int64(r.Intn(1_000_000)))
		default: // fraction
			out[i] = r.NormFloat64() * 100
		}
	}
	return out
}

func median(xs []float64) float64 {
	sort.Float64s(xs)
	return xs[len(xs)/2]
}

var (
	memberCounts = []int{4, 8, 16, 128}
	memberSizes  = []int{2, 8, 20}
	dists        = []string{"smallint", "timestamp", "bigint", "fraction"}
)

func benchBytes() {
	r := rand.New(rand.NewSource(1))
	fmt.Println("inline blob bytes, old (flat 8B score) vs new (class-tagged):")
	for _, dist := range dists {
		for _, ms := range memberSizes {
			for _, mc := range memberCounts {
				scores := drawScores(r, mc, dist)
				oldB := blobBytesOld(mc, ms)
				newB := blobBytesNew(scores, ms)
				fmt.Printf("  %-9s member %2dB x%-4d  old %6d  new %6d  ratio %.2f  save %d B/entry\n",
					dist, ms, mc, oldB, newB, float64(newB)/float64(oldB), (oldB-newB)/mc)
			}
		}
	}
}

func benchCodec(reps, n int) {
	r := rand.New(rand.NewSource(2))
	for _, dist := range dists {
		scores := drawScores(r, n, dist)
		buf := make([]byte, 9)
		encReps := make([]float64, reps)
		decReps := make([]float64, reps)
		for rep := 0; rep < reps; rep++ {
			var w int
			start := time.Now()
			for i := 0; i < n; i++ {
				w += putScore(buf, scores[i])
			}
			encReps[rep] = float64(time.Since(start).Nanoseconds()) / float64(n)

			// Pre-encode a mixed buffer for decode timing.
			putScore(buf, scores[rep%n])
			var acc float64
			start = time.Now()
			for i := 0; i < n; i++ {
				s, _ := readScore(buf)
				acc += s
			}
			decReps[rep] = float64(time.Since(start).Nanoseconds()) / float64(n)
			_ = w
			_ = acc
		}
		fmt.Printf("  %-9s encode %.2f ns/op  decode %.2f ns/op\n",
			dist, median(encReps), median(decReps))
	}
}

func main() {
	reps := flag.Int("reps", 7, "timed reps per cell, median reported")
	codecN := flag.Int("codec", 20_000_000, "encode/decode ops timed per rep")
	flag.Parse()

	fmt.Println("inline-band score bytes lab, spec 2064/f3 doc 12 section 4")
	fmt.Println()
	benchBytes()
	fmt.Println()
	fmt.Println("codec cost:")
	benchCodec(*reps, *codecN)
}
