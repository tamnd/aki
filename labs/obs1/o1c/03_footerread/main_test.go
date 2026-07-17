package main

import (
	"math"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

func TestDrawLatencyQuantiles(t *testing.T) {
	d := sim.S3Standard.Get
	if got := drawLatency(d, 0); got != d.P50 {
		t.Fatalf("z=0 draw = %v want %v", got, d.P50)
	}
	got := drawLatency(d, 2.3263)
	if diff := got - d.P99; diff < -time.Microsecond || diff > time.Microsecond {
		t.Fatalf("z=2.3263 draw = %v want %v", got, d.P99)
	}
}

func TestNormalMoments(t *testing.T) {
	p := &prng{s: 42}
	const n = 50000
	var sum, sumSq float64
	for range n {
		z := p.normal()
		sum += z
		sumSq += z * z
	}
	mean := sum / n
	variance := sumSq/n - mean*mean
	if math.Abs(mean) > 0.02 {
		t.Fatalf("mean = %f", mean)
	}
	if math.Abs(variance-1) > 0.05 {
		t.Fatalf("variance = %f", variance)
	}
}

func TestBuildOneDeterministic(t *testing.T) {
	sh := shape{recBytes: 200, recsChunk: 128, segMiB: 1}
	a := &prng{s: 7}
	b := &prng{s: 7}
	fa, oa, _, _, _ := buildOne(a, sh, 1)
	fb, ob, _, _, _ := buildOne(b, sh, 1)
	if fa != fb || oa != ob {
		t.Fatalf("same seed diverged: %d/%d vs %d/%d", fa, oa, fb, ob)
	}
}

// The lab's whole point is that the record-level bloom dominates the
// footer; pin the direction so a bloom sizing change resurfaces here.
func TestRecordBloomDominates(t *testing.T) {
	p := &prng{s: 9}
	fc, _, _, _, _ := buildOne(p, shape{recBytes: 200, recsChunk: 128, segMiB: 1}, 1)
	p = &prng{s: 9}
	fr, _, _, _, _ := buildOne(p, shape{recBytes: 200, recsChunk: 128, segMiB: 1, perRecord: true}, 1)
	if fr <= fc {
		t.Fatalf("record-bloom footer %d not bigger than chunk-bloom %d", fr, fc)
	}
	// 1 MiB of 200 B records is ~5000 keys, ~6.5 KiB of bloom delta.
	if fr-fc < 4<<10 {
		t.Fatalf("bloom delta only %d bytes", fr-fc)
	}
}

// The built object must be a real segment: round-trip through the engine
// decoder and check the totals the lab reports against the parsed footer.
func TestBuiltSegmentParses(t *testing.T) {
	p := &prng{s: 11}
	sh := shape{recBytes: 200, recsChunk: 128, segMiB: 1}
	var segChunks []obs1.SegmentChunk
	var keys [][]byte
	size := 0
	for size < sh.segMiB<<20 {
		rpc := sh.recsChunk/2 + p.intn(sh.recsChunk+1)
		key := make([]byte, 16)
		p.fill(key)
		data := make([]byte, 4+rpc*sh.recBytes)
		data[0] = byte(len(data))
		data[1] = byte(len(data) >> 8)
		data[2] = byte(len(data) >> 16)
		data[3] = byte(len(data) >> 24)
		p.fill(data[4:])
		segChunks = append(segChunks, obs1.SegmentChunk{
			Key: key, Kind: 1, FirstDisc: uint64(len(segChunks)),
			Count: uint16(rpc), LiveHint: uint16(rpc), Data: data,
		})
		keys = append(keys, key)
		size += len(data)
	}
	seg, err := obs1.BuildSegment(obs1.SegmentFooter{Group: 1, Epoch: 1, SegSeq: 1}, segChunks, keys, 0)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := obs1.AppendSegment(nil, 7, seg)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _, err := obs1.ParseSegment(obj)
	if err != nil {
		t.Fatal(err)
	}
	if int(parsed.Footer.SegSeq) != 1 || len(parsed.Footer.Chunks) != len(segChunks) {
		t.Fatalf("parsed footer: seq %d chunks %d want 1 %d", parsed.Footer.SegSeq, len(parsed.Footer.Chunks), len(segChunks))
	}
	last := parsed.Footer.Blocks[len(parsed.Footer.Blocks)-1]
	off, n := last.BlockSpan()
	if int(off+n) >= len(obj) {
		t.Fatalf("no footer bytes after last block: %d >= %d", off+n, len(obj))
	}
}

func TestQuantile(t *testing.T) {
	s := []float64{1, 2, 3, 4, 5}
	if quantile(s, 0.5) != 3 {
		t.Fatalf("p50 = %f", quantile(s, 0.5))
	}
	if quantile(s, 0.99) != 5 {
		t.Fatalf("p99 = %f", quantile(s, 0.99))
	}
	if quantile(nil, 0.5) != 0 {
		t.Fatal("empty quantile")
	}
}
