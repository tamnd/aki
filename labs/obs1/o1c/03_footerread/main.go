// Command footerread prices opening a segment cold (spec 2064/obs1 doc 03
// section 12, doc 05 sections 2.3 and 5): the tail GET, the footer GET,
// and the first block GET, against footer size and object size, using the
// real segment encoder so footer sizes are measured facts rather than
// arithmetic. It drives whether the tail read merges with the footer read
// and at what speculative size, and it prices the bloom placement choice,
// since a member-level bloom at 10 bits per key can outweigh every other
// footer line put together.
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// prng is splitmix64, deterministic and import-free.
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

// normal draws a standard normal via Box-Muller.
func (p *prng) normal() float64 {
	u1 := (float64(p.next()>>11) + 1) / (1 << 53)
	u2 := float64(p.next()>>11) / (1 << 53)
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

// drawLatency maps a standard normal onto the sim Dist's lognormal, the
// same two-line map sim uses, pinned to its constants so the O5 E-cloud
// refit moves this lab automatically.
func drawLatency(d sim.Dist, z float64) time.Duration {
	sigma := math.Log(float64(d.P99)/float64(d.P50)) / 2.3263
	return time.Duration(float64(d.P50) * math.Exp(sigma*z))
}

type shape struct {
	recBytes  int
	recsChunk int // nominal records per chunk, jittered 0.5x to 1.5x
	segMiB    int // payload target
	perRecord bool
}

func (s shape) arm() string {
	if s.perRecord {
		return "record"
	}
	return "chunk"
}

// buildOne packs one segment of the shape through the real encoder and
// returns the measured facts: the byte count a speculative tail read must
// cover (footer plus object tail), and the object, block, chunk, and
// record totals.
func buildOne(p *prng, sh shape, seq uint64) (footTail, objBytes, blocks, chunks, records int) {
	var segChunks []obs1.SegmentChunk
	var memberKeys [][]byte
	var size, nrec int
	for size < sh.segMiB<<20 {
		rpc := max(sh.recsChunk/2+p.intn(sh.recsChunk+1), 1)
		key := make([]byte, 12+p.intn(13))
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
		if sh.perRecord {
			for range rpc {
				mk := make([]byte, 16)
				p.fill(mk)
				memberKeys = append(memberKeys, mk)
			}
		} else {
			memberKeys = append(memberKeys, key)
		}
		size += len(data)
		nrec += rpc
	}
	seg, err := obs1.BuildSegment(obs1.SegmentFooter{Group: 1, Epoch: 1, SegSeq: seq}, segChunks, memberKeys, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	obj, err := obs1.AppendSegment(nil, 7, seg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// Locate and decode the footer exactly the way a cold open does: the
	// 16-byte tail says where it lives, then the footer parses on its own.
	// The speculative tail read must cover footer plus tail, so F is
	// everything after the last block.
	footerOff, footerLen, err := obs1.ParseTail(obj[len(obj)-obs1.TailSize:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	f, err := obs1.ParseSegmentFooter(obj[footerOff : footerOff+uint64(footerLen)])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return len(obj) - int(footerOff), len(obj), len(f.Blocks), len(segChunks), nrec
}

func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(q * float64(len(sorted)-1)))
	return sorted[i]
}

func main() {
	quick := flag.Bool("quick", false, "fewer and smaller segments, smoke only")
	flag.Parse()

	segsPerCell, latSamples := 8, 20000
	targets := []int{16, 64, 128}
	if *quick {
		segsPerCell, latSamples = 2, 2000
		targets = []int{16}
	}
	specKiB := []int{16, 32, 64, 128, 256, 512}

	fmt.Println("kind,arm,recB,recsChunk,segMiB,specKiB,segs,chunksMean,blocksMean,footKiBp50,footKiBmax,hitPct,getsPerOpen,usdPerMopens,openP50ms,openP99ms,wastedKiBmean")

	p := &prng{s: 0xF007}
	for _, perRecord := range []bool{false, true} {
		for _, recB := range []int{200, 2048} {
			for _, rpc := range []int{128, 512, 2048} {
				for _, segMiB := range targets {
					sh := shape{recBytes: recB, recsChunk: rpc, segMiB: segMiB, perRecord: perRecord}
					var foots []float64
					var chunkSum, blockSum, objSum int
					for i := range segsPerCell {
						ft, ob, bl, ch, _ := buildOne(p, sh, uint64(i+1))
						foots = append(foots, float64(ft))
						chunkSum += ch
						blockSum += bl
						objSum += ob
					}
					sort.Float64s(foots)
					fmt.Printf("cell,%s,%d,%d,%d,0,%d,%.0f,%.0f,%.1f,%.1f,,,,,,\n",
						sh.arm(), recB, rpc, segMiB, segsPerCell,
						float64(chunkSum)/float64(segsPerCell), float64(blockSum)/float64(segsPerCell),
						quantile(foots, 0.5)/1024, foots[len(foots)-1]/1024)
					for _, sk := range specKiB {
						spec := float64(sk << 10)
						var hits int
						var wasted float64
						for _, f := range foots {
							if f <= spec {
								hits++
								wasted += spec - f
							}
						}
						hitRate := float64(hits) / float64(len(foots))
						var lat []float64
						for range latSamples {
							f := foots[p.intn(len(foots))]
							open := drawLatency(sim.S3Standard.Get, p.normal()) + drawLatency(sim.S3Standard.Get, p.normal())
							if f > spec {
								open += drawLatency(sim.S3Standard.Get, p.normal())
							}
							lat = append(lat, float64(open)/float64(time.Millisecond))
						}
						sort.Float64s(lat)
						gets := 3 - hitRate
						usd := sim.S3StandardPrices.Bill(sim.Usage{GetRequests: int64(gets * 1e6)}, 0).Gets
						wm := 0.0
						if hits > 0 {
							wm = wasted / float64(hits) / 1024
						}
						fmt.Printf("open,%s,%d,%d,%d,%d,%d,,,,,%.1f,%.3f,%.3f,%.1f,%.1f,%.1f\n",
							sh.arm(), recB, rpc, segMiB, sk, segsPerCell,
							hitRate*100, gets, usd, quantile(lat, 0.5), quantile(lat, 0.99), wm)
					}
				}
			}
		}
	}
}
