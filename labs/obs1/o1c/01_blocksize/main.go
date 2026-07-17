// blocksize sweeps the segment compression-block size (spec 2064/obs1
// doc 03 section 5, doc 05 sections 3-4) against the point-read GET
// bill and the scan fetch waste, and gates the 128 KiB default before
// the fold slice bakes it into the packer.
//
// The model is a block cache in front of a bucket, nothing else: keys
// of one fixed value size lie contiguous in fold order, chunks never
// span blocks, a point read fetches exactly one block on a miss, and a
// scan fetches the block roundup of each of its fragments with adjacent
// blocks coalesced into ranges at the AWS-guidance cap. Dollars go
// through sim.S3StandardPrices.Bill so the O5 E-cloud refit moves this
// lab automatically.
//
// Three read distributions bracket the deployment: uniform is the
// floor, raw Zipfian theta 0.99 is the skew the cache would see with no
// hot tier in front, and tail-Zipfian conditions the same draw on the
// hot tier having absorbed the top 10% of keys by rank, which is the
// doc 05 serving shape (tiers 1 and 2 answer before the block cache
// ever sees the read).
//
// Simplifications, disclosed: the cache is plain SIEVE over whole
// blocks with a byte budget, not the doc 05 two-touch doorkeeper (that
// admission policy lands with the async-cold-read slice and matters for
// scan pollution, which this lab keeps out by construction); blocks are
// counted at raw size because compression is the zstd-worth lab's
// business; the hot tier is idealized as the exact top ranks.
package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand/v2"

	"github.com/tamnd/aki/engine/obs1/sim"
)

const coalesceBytes = 16 << 20 // doc 05 section 3 scan range cap

// zetaSum is the truncated zeta the YCSB generator normalizes by.
func zetaSum(n uint64, theta float64) float64 {
	s := 0.0
	for i := uint64(1); i <= n; i++ {
		s += 1 / math.Pow(float64(i), theta)
	}
	return s
}

// zipf draws ranks 0..n-1 with the YCSB zipfian algorithm; rank 0 is
// the hottest key.
type zipf struct {
	n     uint64
	theta float64
	alpha float64
	zetan float64
	eta   float64
	r     *rand.Rand
}

func newZipf(r *rand.Rand, n uint64, theta, zetan float64) *zipf {
	z := &zipf{n: n, theta: theta, r: r, zetan: zetan}
	z.alpha = 1 / (1 - theta)
	z.eta = (1 - math.Pow(2/float64(n), 1-theta)) / (1 - zetaSum(2, theta)/zetan)
	return z
}

func (z *zipf) rank() uint64 {
	u := z.r.Float64()
	uz := u * z.zetan
	if uz < 1 {
		return 0
	}
	if uz < 1+math.Pow(0.5, z.theta) {
		return 1
	}
	rk := uint64(float64(z.n) * math.Pow(z.eta*u-z.eta+1, z.alpha))
	if rk >= z.n {
		rk = z.n - 1
	}
	return rk
}

// scramble maps a popularity rank to a key position bijectively inside
// a power-of-two keyspace (odd multiply mod 2^k, then xorshift, both
// invertible), so fold order is independent of popularity without a
// permutation table.
func scramble(x, mask uint64) uint64 {
	x = (x * 0x9E3779B97F4A7C15) & mask
	x ^= x >> 13
	x = (x * 0xBF58476D1CE4E5B9) & mask
	x ^= x >> 9
	return x
}

// sieve is a byte-budgeted SIEVE cache over block ids: insertion at the
// head, one visited bit per entry, the hand walks tail to head clearing
// visited bits and evicts the first unvisited entry.
type sieve struct {
	cap  int
	idx  map[uint64]int32
	key  []uint64
	vis  []bool
	prev []int32 // toward the head (newer)
	next []int32 // toward the tail (older)
	head int32
	tail int32
	hand int32
	free []int32
}

func newSieve(capBlocks int) *sieve {
	if capBlocks < 1 {
		capBlocks = 1
	}
	return &sieve{cap: capBlocks, idx: make(map[uint64]int32, capBlocks), head: -1, tail: -1, hand: -1}
}

// access probes the cache for a block, admitting it on a miss, and
// reports whether it hit.
func (s *sieve) access(b uint64) bool {
	if i, ok := s.idx[b]; ok {
		s.vis[i] = true
		return true
	}
	if len(s.idx) >= s.cap {
		s.evict()
	}
	var i int32
	if n := len(s.free); n > 0 {
		i = s.free[n-1]
		s.free = s.free[:n-1]
		s.key[i], s.vis[i] = b, false
	} else {
		i = int32(len(s.key))
		s.key = append(s.key, b)
		s.vis = append(s.vis, false)
		s.prev = append(s.prev, -1)
		s.next = append(s.next, -1)
	}
	s.prev[i], s.next[i] = -1, s.head
	if s.head != -1 {
		s.prev[s.head] = i
	}
	s.head = i
	if s.tail == -1 {
		s.tail = i
	}
	s.idx[b] = i
	return false
}

func (s *sieve) evict() {
	h := s.hand
	if h == -1 {
		h = s.tail
	}
	for s.vis[h] {
		s.vis[h] = false
		h = s.prev[h]
		if h == -1 {
			h = s.tail
		}
	}
	s.hand = s.prev[h]
	if p := s.prev[h]; p != -1 {
		s.next[p] = s.next[h]
	} else {
		s.head = s.next[h]
	}
	if n := s.next[h]; n != -1 {
		s.prev[n] = s.prev[h]
	} else {
		s.tail = s.prev[h]
	}
	delete(s.idx, s.key[h])
	s.free = append(s.free, h)
}

// drawer produces key positions for one read distribution.
type drawer interface{ next() uint64 }

type uniformDraw struct {
	n uint64
	r *rand.Rand
}

func (d *uniformDraw) next() uint64 { return d.r.Uint64N(d.n) }

type zipfDraw struct {
	z    *zipf
	mask uint64
	hotN uint64 // ranks below this are absorbed by the hot tier; 0 = raw
}

func (d *zipfDraw) next() uint64 {
	for {
		rk := d.z.rank()
		if rk >= d.hotN {
			return scramble(rk, d.mask)
		}
	}
}

type pointRes struct {
	hitPct     float64
	getsPer1k  float64
	mibPer1k   float64
	usdPerMops float64
}

// runPoint measures one point-read cell: warm ops fill the cache, meas
// ops score it, and each miss buys one GET of one whole block.
func runPoint(blockBytes, vsize, coldBytes int64, budget float64, d drawer, warm, meas int) pointRes {
	keysPerBlock := blockBytes / vsize
	capBlocks := int(float64(coldBytes) * budget / float64(blockBytes))
	c := newSieve(capBlocks)
	for range warm {
		c.access(d.next() / uint64(keysPerBlock))
	}
	misses := 0
	for range meas {
		if !c.access(d.next() / uint64(keysPerBlock)) {
			misses++
		}
	}
	missRate := float64(misses) / float64(meas)
	usd := sim.S3StandardPrices.Bill(sim.Usage{GetRequests: int64(missRate * 1e6)}, 0).Gets
	return pointRes{
		hitPct:     100 * (1 - missRate),
		getsPer1k:  missRate * 1000,
		mibPer1k:   missRate * 1000 * float64(blockBytes) / (1 << 20),
		usdPerMops: usd,
	}
}

// fetchedForFrag is the bytes a fragment costs: the block roundup of
// its span given its offset inside the first block.
func fetchedForFrag(startInBlock, fragBytes, blockBytes int64) int64 {
	return (startInBlock + fragBytes + blockBytes - 1) / blockBytes * blockBytes
}

type scanRes struct {
	reqsPerScan float64
	fetchRatio  float64
}

// runScan measures one scan cell: each scan reads span bytes in frags
// contiguous fragments at uniform block offsets, fragments cannot
// coalesce with each other, and each fragment's contiguous block range
// splits into GETs at the coalesce cap.
func runScan(blockBytes, span int64, frags, samples int, r *rand.Rand) scanRes {
	fragBytes := span / int64(frags)
	var reqs, fetched int64
	for range samples {
		for range frags {
			fb := fetchedForFrag(r.Int64N(blockBytes), fragBytes, blockBytes)
			fetched += fb
			reqs += (fb + coalesceBytes - 1) / coalesceBytes
		}
	}
	return scanRes{
		reqsPerScan: float64(reqs) / float64(samples),
		fetchRatio:  float64(fetched) / float64(int64(samples)*span),
	}
}

func main() {
	quick := flag.Bool("quick", false, "tiny keyspaces and op counts, smoke only")
	seed := flag.Uint64("seed", 20260717, "PCG seed")
	flag.Parse()

	blockKiB := []int64{32, 64, 128, 256, 512}
	type corpus struct {
		vsize   int64
		keyBits uint
	}
	corpora := []corpus{{200, 24}, {2048, 21}}
	warm, meas, scanN := 2_000_000, 8_000_000, 200_000
	if *quick {
		corpora = []corpus{{200, 18}, {2048, 15}}
		warm, meas, scanN = 100_000, 400_000, 10_000
	}
	const theta = 0.99

	fmt.Println("kind,dist,vsizeB,budgetPct,blockKiB,spanKiB,frags,hitPct,getsPer1k,mibPer1k,usdPerMops,reqsPerScan,fetchRatio")
	for _, co := range corpora {
		n := uint64(1) << co.keyBits
		coldBytes := int64(n) * co.vsize
		zetan := zetaSum(n, theta)
		for _, budget := range []float64{0.02, 0.10} {
			for _, dist := range []string{"uniform", "zipf", "tailzipf"} {
				for _, bk := range blockKiB {
					r := rand.New(rand.NewPCG(*seed, uint64(bk)<<32|uint64(co.vsize)))
					var d drawer
					switch dist {
					case "uniform":
						d = &uniformDraw{n: n, r: r}
					case "zipf":
						d = &zipfDraw{z: newZipf(r, n, theta, zetan), mask: n - 1}
					case "tailzipf":
						d = &zipfDraw{z: newZipf(r, n, theta, zetan), mask: n - 1, hotN: n / 10}
					}
					p := runPoint(bk<<10, co.vsize, coldBytes, budget, d, warm, meas)
					fmt.Printf("point,%s,%d,%g,%d,,,%.2f,%.2f,%.2f,%.4f,,\n",
						dist, co.vsize, budget*100, bk, p.hitPct, p.getsPer1k, p.mibPer1k, p.usdPerMops)
				}
			}
		}
	}
	for _, spanKiB := range []int64{64, 1024} {
		for _, frags := range []int{1, 4} {
			for _, bk := range blockKiB {
				r := rand.New(rand.NewPCG(*seed, uint64(spanKiB)<<16|uint64(frags)))
				s := runScan(bk<<10, spanKiB<<10, frags, scanN, r)
				fmt.Printf("scan,,,,%d,%d,%d,,,,,%.3f,%.3f\n", bk, spanKiB, frags, s.reqsPerScan, s.fetchRatio)
			}
		}
	}
}
