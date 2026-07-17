// Command foldthroughput measures what fold costs the owner (spec 2064/obs1
// doc 06 sections 1.4 and 8): fold is the direct descendant of f3 demotion,
// so its owner-side tax is the real StageColdDrain pass, the SIEVE hand walk
// plus framing, and this lab times it against pass budget and record size on
// a pressured store built through the public surface. The I/O-pool side is
// the segment build, timed through the real encoder. Together they answer
// whether the 8 MiB pass budget is a tolerable owner stall, whether the
// budget is a throughput knob or only a latency knob, and what fraction of
// an owner design ingest would eat.
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/store"
)

type prng struct{ s uint64 }

func (p *prng) next() uint64 {
	p.s += 0x9E3779B97F4A7C15
	z := p.s
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (p *prng) intn(n int) int { return int(p.next() % uint64(n)) }

func (p *prng) fill(b []byte) {
	for i := range b {
		b[i] = byte(p.next() >> 56)
	}
}

func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(math.Ceil(q*float64(len(sorted)-1)))]
}

type stageCfg struct {
	valBytes    int
	passBytes   int
	targetBytes int // bytes to stage across the whole cell
	arenaBytes  int
	capBytes    uint64
	marginBytes int // extra fill past the drain trigger before passes start
}

type stageOut struct {
	passes, dryPasses int
	stagedBytes       int
	stalls            []float64 // ms per pass, sorted
	stallSec          float64   // owner time inside StageColdDrain
	totalSec          float64   // whole loop: stalls, refills, cold writes
	ioSec             float64   // ColdWriteAt time
	refillBytes       int
	refillSec         float64
	setsPerSec        float64
}

// runStageCell drives the real two-phase drain the way the shard worker
// does: stage on the owner, write the buffer to the cold region, complete
// the flip, and refill through Set whenever pressure drops below the
// trigger. Every Set and every stage runs on this goroutine, which is
// exactly the owner model.
func runStageCell(cfg stageCfg) (stageOut, error) {
	var out stageOut
	dir, err := os.MkdirTemp("", "foldthru")
	if err != nil {
		return out, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	s, err := store.Open(store.Options{
		ArenaBytes:       cfg.arenaBytes,
		SegBytes:         256 << 10,
		VlogPath:         filepath.Join(dir, "vlog"),
		ColdPath:         filepath.Join(dir, "cold"),
		ResidentCapBytes: cfg.capBytes,
	})
	if err != nil {
		return out, err
	}
	defer func() { _ = s.Close() }()

	p := &prng{s: uint64(cfg.valBytes)*1e9 + uint64(cfg.passBytes)}
	val := make([]byte, cfg.valBytes)
	p.fill(val)
	nkeys := 0
	set := func() error {
		k := fmt.Appendf(nil, "k:%09d", nkeys)
		nkeys++
		return s.Set(k, val)
	}
	fill0 := time.Now()
	for !s.NeedsColdDrain() {
		if err := set(); err != nil {
			return out, err
		}
	}
	perKey := cfg.valBytes + 11
	for range cfg.marginBytes / perKey {
		if err := set(); err != nil {
			return out, err
		}
	}
	out.setsPerSec = float64(nkeys) / time.Since(fill0).Seconds()

	buf := make([]byte, 0, cfg.passBytes)
	dry := 0
	t0 := time.Now()
	for out.stagedBytes < cfg.targetBytes {
		if !s.NeedsColdDrain() {
			r0 := time.Now()
			for !s.NeedsColdDrain() {
				if err := set(); err != nil {
					return out, err
				}
				out.refillBytes += perKey
			}
			out.refillSec += time.Since(r0).Seconds()
			continue
		}
		st := time.Now()
		d := s.StageColdDrain(buf)
		stall := time.Since(st)
		if d == nil {
			return out, fmt.Errorf("cold region refused a reservation")
		}
		if len(d.Buf()) == 0 {
			// The hand's first revolution over a run of hot slots clears
			// heat and frames nothing; retry, but a store that never yields
			// a frame is a bug worth failing loudly on.
			buf = d.Buf()
			if dry++; dry > 10000 {
				return out, fmt.Errorf("10000 dry passes in a row")
			}
			out.dryPasses++
			continue
		}
		dry = 0
		out.passes++
		out.stagedBytes += len(d.Buf())
		out.stalls = append(out.stalls, float64(stall)/float64(time.Millisecond))
		out.stallSec += stall.Seconds()
		w0 := time.Now()
		if _, err := s.ColdWriteAt(d.Off(), d.Buf()); err != nil {
			return out, err
		}
		out.ioSec += time.Since(w0).Seconds()
		s.CompleteColdDrain(d, true)
		buf = d.Buf()
	}
	out.totalSec = time.Since(t0).Seconds()
	sort.Float64s(out.stalls)
	return out, nil
}

// runBuildCell times the I/O-pool half: packing chunk frames into blocks
// and the segment object through the real encoder, comp 0 (the zstd-worth
// lab priced comp 1 separately; its CLI-measured rate bounds this stage
// when compression is on).
func runBuildCell(recBytes, rpc, segBytes, segs int) (mibps float64, err error) {
	p := &prng{s: uint64(recBytes)}
	var built int
	var dur time.Duration
	for si := range segs {
		var chunks []obs1.SegmentChunk
		var keys [][]byte
		size := 0
		for size < segBytes {
			key := make([]byte, 16)
			p.fill(key)
			data := make([]byte, 4+rpc*recBytes)
			data[0] = byte(len(data))
			data[1] = byte(len(data) >> 8)
			data[2] = byte(len(data) >> 16)
			data[3] = byte(len(data) >> 24)
			p.fill(data[4:])
			chunks = append(chunks, obs1.SegmentChunk{
				Key: key, Kind: 1, FirstDisc: uint64(len(chunks)),
				Count: uint16(rpc), LiveHint: uint16(rpc), Data: data,
			})
			keys = append(keys, key)
			size += len(data)
		}
		t0 := time.Now()
		seg, err := obs1.BuildSegment(obs1.SegmentFooter{Group: 1, Epoch: 1, SegSeq: uint64(si + 1)}, chunks, keys, 0)
		if err != nil {
			return 0, err
		}
		obj, err := obs1.AppendSegment(nil, 7, seg)
		if err != nil {
			return 0, err
		}
		dur += time.Since(t0)
		built += len(obj)
	}
	return float64(built) / (1 << 20) / dur.Seconds(), nil
}

func main() {
	quick := flag.Bool("quick", false, "small cells, smoke only")
	flag.Parse()

	// The store separates values over 1 KiB into the vlog by construction,
	// and separated records never enter the cold-stage path (the scored run
	// proved it: 2 KiB cells dry-loop while NeedsColdDrain stays true), so
	// the stage cells stay inside the embedded band.
	valSizes := []int{16, 200, 1000}
	passSizes := []int{1 << 20, 4 << 20, 8 << 20, 16 << 20}
	cfg := stageCfg{targetBytes: 160 << 20, arenaBytes: 512 << 20, capBytes: 128 << 20, marginBytes: 48 << 20}
	buildSegs := 4
	if *quick {
		valSizes = []int{200}
		passSizes = []int{8 << 20}
		cfg = stageCfg{targetBytes: 16 << 20, arenaBytes: 128 << 20, capBytes: 32 << 20, marginBytes: 16 << 20}
		buildSegs = 1
	}

	fmt.Println("kind,valB,passMiB,recB,passes,dry,stagedMiB,stallP50ms,stallP99ms,stallMiBps,sustainedMiBps,ownerPctAt100,ioMiBps,setsPerSec,buildMiBps")
	for _, v := range valSizes {
		for _, pb := range passSizes {
			c := cfg
			c.valBytes, c.passBytes = v, pb
			out, err := runStageCell(c)
			if err != nil {
				fmt.Fprintf(os.Stderr, "stage cell val=%d pass=%d: %v\n", v, pb, err)
				os.Exit(1)
			}
			stagedMiB := float64(out.stagedBytes) / (1 << 20)
			stallRate := stagedMiB / out.stallSec
			fmt.Printf("stage,%d,%d,,%d,%d,%.1f,%.3f,%.3f,%.0f,%.0f,%.1f,%.0f,%.0f,\n",
				v, pb>>20, out.passes, out.dryPasses, stagedMiB,
				quantile(out.stalls, 0.5), quantile(out.stalls, 0.99),
				stallRate, stagedMiB/out.totalSec, 100*100/stallRate,
				stagedMiB/out.ioSec, out.setsPerSec)
		}
	}
	for _, rb := range []int{200, 2048} {
		mibps, err := runBuildCell(rb, 512, 64<<20, buildSegs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "build cell rec=%d: %v\n", rb, err)
			os.Exit(1)
		}
		fmt.Printf("build,,,%d,,,,,,,,,,,%.0f\n", rb, mibps)
	}
}
