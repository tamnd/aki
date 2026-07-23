// scanplan gates the doc 05 section 3 scan planner choices before the
// scan slice lands: coalesced 8 to 16 MiB range GETs vs per-block reads
// on a real cold scan, the 1 GiB and 10 GiB plan arithmetic, the
// scan-fan default of 8 under a disclosed latency model, and the
// readahead admission exemption against a compact S3-FIFO cache.
//
// The scan transfers are real counting-sim traffic verified by checksum;
// the fan wall-clock is an analytic model over the measured plan and the
// cache is a lab-local S3-FIFO, both disclosed in the README. The scan
// slice and the O3 cache milestone replace the models with landed planes.
package main

import (
	"context"
	"flag"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/tamnd/aki/engine/obs1/sim"
)

const (
	blockBytes = 128 << 10
	// The doc 01 section 2.2 envelope, lab-local fit: first-byte p50 and
	// the AWS-guidance per-connection throughput, a 10 Gbps client NIC.
	// The E-sim refit at O5 replaces these numbers with measured ones.
	ttfbSec   = 0.020
	laneBps   = 100e6
	nicBps    = 1250e6
	ghostFrac = 2 // ghost holds main's entry count / ghostFrac
)

// planGets is the directory math: a scan of `bytes` in ranges of `rng`.
func planGets(bytes, rng int64) int64 { return (bytes + rng - 1) / rng }

// fanWall models the scan wall clock at fan F: ranges round-robin onto F
// lanes, each lane pays first-byte plus transfer per range, and the NIC
// divides across lanes once F lanes outrun it.
func fanWall(nRanges int64, rngBytes int64, f int) float64 {
	eff := min(laneBps, nicBps/float64(f))
	perLane := float64((nRanges + int64(f) - 1) / int64(f))
	return perLane * (ttfbSec + float64(rngBytes)/eff)
}

type labErr struct{ err error }

func die(format string, args ...any) { panic(labErr{fmt.Errorf(format, args...)}) }

// scanArm streams the whole object in ranges of `rng`, checksumming the
// stream; the caller bills the sim delta.
func scanArm(ctx context.Context, s *sim.Sim, key string, size, rng int64, wantCRC uint32) {
	crc := crc32.NewIEEE()
	for off := int64(0); off < size; off += rng {
		nb := min(rng, size-off)
		b, _, err := s.GetRange(ctx, key, off, nb)
		if err != nil {
			die("scan get: %v", err)
		}
		_, _ = crc.Write(b)
	}
	if crc.Sum32() != wantCRC {
		die("scan stream checksum mismatch")
	}
}

// s3fifo is the doc 05 section 4 policy in compact form: probationary
// small FIFO (10 percent), main FIFO, ghost of evicted ids at half of
// main's entry count; a small-FIFO victim with re-reference moves to
// main, a ghost hit on admission goes straight to main, and main evicts
// with one lazy-promotion second chance.
type s3fifo struct {
	capSmall, capMain, capGhost int
	freq                        map[int64]int
	inMain                      map[int64]bool
	small, main                 []int64
	ghost                       map[int64]bool
	ghostQ                      []int64
}

func newS3FIFO(slots int) *s3fifo {
	cs := max(1, slots/10)
	cm := slots - cs
	return &s3fifo{
		capSmall: cs, capMain: cm, capGhost: max(1, cm/ghostFrac),
		freq:   make(map[int64]int),
		inMain: make(map[int64]bool),
		ghost:  make(map[int64]bool),
	}
}

func (c *s3fifo) has(id int64) bool {
	if _, ok := c.freq[id]; ok {
		c.freq[id] = min(c.freq[id]+1, 3)
		return true
	}
	return false
}

func (c *s3fifo) toGhost(id int64) {
	if c.ghost[id] {
		return
	}
	c.ghost[id] = true
	c.ghostQ = append(c.ghostQ, id)
	for len(c.ghostQ) > c.capGhost {
		delete(c.ghost, c.ghostQ[0])
		c.ghostQ = c.ghostQ[1:]
	}
}

func (c *s3fifo) evictMain() {
	for len(c.main) > 0 {
		h := c.main[0]
		c.main = c.main[1:]
		if c.freq[h] > 0 {
			c.freq[h]--
			c.main = append(c.main, h)
			continue
		}
		delete(c.freq, h)
		delete(c.inMain, h)
		c.toGhost(h)
		return
	}
}

func (c *s3fifo) pushMain(id int64) {
	for len(c.main) >= c.capMain {
		c.evictMain()
	}
	c.main = append(c.main, id)
	c.inMain[id] = true
}

func (c *s3fifo) admit(id int64) {
	if c.ghost[id] {
		delete(c.ghost, id)
		c.freq[id] = 0
		c.pushMain(id)
		return
	}
	c.freq[id] = 0
	c.small = append(c.small, id)
	for len(c.small) > c.capSmall {
		h := c.small[0]
		c.small = c.small[1:]
		if c.freq[h] > 0 {
			c.freq[h] = 0
			c.pushMain(h)
			continue
		}
		delete(c.freq, h)
		c.toGhost(h)
	}
}

// lru is the naive reference arm: admit everything, evict oldest use.
// The order queue holds tick-stamped entries; a popped entry whose tick
// no longer matches the id's latest touch is stale and skipped.
type lruEnt struct {
	id   int64
	tick int64
}

type lru struct {
	cap   int
	stamp map[int64]int64
	order []lruEnt
	tick  int64
}

func newLRU(slots int) *lru { return &lru{cap: slots, stamp: make(map[int64]int64)} }

func (c *lru) touch(id int64) {
	c.tick++
	c.stamp[id] = c.tick
	c.order = append(c.order, lruEnt{id: id, tick: c.tick})
}

func (c *lru) has(id int64) bool {
	if _, ok := c.stamp[id]; ok {
		c.touch(id)
		return true
	}
	return false
}

func (c *lru) admit(id int64) {
	c.touch(id)
	for len(c.stamp) > c.cap {
		h := c.order[0]
		c.order = c.order[1:]
		if st, ok := c.stamp[h.id]; ok && st == h.tick {
			delete(c.stamp, h.id)
		}
	}
}

type blockCache interface {
	has(id int64) bool
	admit(id int64)
}

// admissionArm runs the doc 05 admission workload against one cache arm:
// warm point reads over a skewed space, then a full cold scan interleaved
// with point reads, and the score is the point hit rate during the scan
// window plus the GETs bought for point misses.
func admissionArm(ctx context.Context, s *sim.Sim, key string, c blockCache, exempt bool,
	blocks, hot, warmOps, scanBlocks, pointsPer int) (hitRate float64, gets int64) {
	r := uint64(0x9E3779B97F4A7C15)
	next := func() uint64 {
		r = r*6364136223846793005 + 1442695040888963407
		return r >> 11
	}
	pointID := func() int64 {
		if next()%10 < 9 {
			return int64(next() % uint64(hot))
		}
		return int64(next() % uint64(blocks))
	}
	before := s.Usage()
	touch := func(id int64, scan bool) bool {
		if c.has(id) {
			return true
		}
		off := (id % int64(blocks)) * blockBytes
		if _, _, err := s.GetRange(ctx, key, off, blockBytes); err != nil {
			die("admission get: %v", err)
		}
		if !scan || !exempt {
			c.admit(id)
		}
		return false
	}
	for range warmOps {
		touch(pointID(), false)
	}
	hits, points := 0, 0
	for sb := range scanBlocks {
		touch(int64(blocks+sb), true) // scan ids live outside the point space
		for range pointsPer {
			points++
			if touch(pointID(), false) {
				hits++
			}
		}
	}
	after := s.Usage()
	return float64(hits) / float64(points), after.GetRequests - before.GetRequests
}

// putScanObject builds and stores the deterministic scan object, letting
// the local buffer release before the arms run.
func putScanObject(ctx context.Context, s *sim.Sim, objBytes int64) uint32 {
	obj := make([]byte, objBytes)
	for i := range obj {
		obj[i] = byte(i*31 + 7)
	}
	if _, err := s.Put(ctx, "seg/scan", obj); err != nil {
		die("put: %v", err)
	}
	return crc32.ChecksumIEEE(obj)
}

type results struct {
	scan  []cell
	plans []cell
	fans  []cell
	adm   []cell
}

type cell struct {
	name  string
	gets  int64
	mib   float64
	extra string
}

func run(objBytes int64, cacheSlots, hot, warmOps, scanBlocks int) (res results, rerr error) {
	defer func() {
		if r := recover(); r != nil {
			le, ok := r.(labErr)
			if !ok {
				panic(r)
			}
			rerr = le.err
		}
	}()
	ctx := context.Background()
	s := sim.New(sim.Config{})
	wantCRC := putScanObject(ctx, s, objBytes)

	for _, arm := range []struct {
		name string
		rng  int64
	}{{"perblock", blockBytes}, {"coalesce8", 8 << 20}, {"coalesce16", 16 << 20}} {
		before := s.Usage()
		scanArm(ctx, s, "seg/scan", objBytes, arm.rng, wantCRC)
		after := s.Usage()
		res.scan = append(res.scan, cell{
			name: "scan_" + arm.name,
			gets: after.GetRequests - before.GetRequests,
			mib:  float64(after.BytesDown-before.BytesDown) / (1 << 20),
		})
	}
	for _, pl := range []struct {
		name  string
		bytes int64
		rng   int64
	}{
		{"plan_1gib_8mib", 1 << 30, 8 << 20},
		{"plan_1gib_16mib", 1 << 30, 16 << 20},
		{"plan_10gib_16mib", 10 << 30, 16 << 20},
	} {
		res.plans = append(res.plans, cell{name: pl.name, gets: planGets(pl.bytes, pl.rng)})
	}
	nR := planGets(10<<30, 16<<20)
	for _, f := range []int{1, 2, 4, 8, 16, 32} {
		res.fans = append(res.fans, cell{
			name: fmt.Sprintf("fan_%d", f),
			mib:  fanWall(nR, 16<<20, f),
			extra: fmt.Sprintf("throughput %.0f MB/s",
				float64(10<<30)/fanWall(nR, 16<<20, f)/1e6),
		})
	}

	blocks := int(objBytes / blockBytes)
	pointsPer := 16
	for _, arm := range []struct {
		name   string
		mk     func() blockCache
		exempt bool
	}{
		{"adm_s3fifo_exempt", func() blockCache { return newS3FIFO(cacheSlots) }, true},
		{"adm_s3fifo_admit", func() blockCache { return newS3FIFO(cacheSlots) }, false},
		{"adm_lru_admit", func() blockCache { return newLRU(cacheSlots) }, false},
	} {
		hr, gets := admissionArm(ctx, s, "seg/scan", arm.mk(), arm.exempt,
			blocks, hot, warmOps, scanBlocks, pointsPer)
		res.adm = append(res.adm, cell{
			name: arm.name, gets: gets,
			extra: fmt.Sprintf("point_hit %.4f", hr),
		})
	}
	return res, nil
}

func main() {
	quick := flag.Bool("quick", false, "smoke sizes")
	flag.Parse()
	objBytes, cacheSlots, hot, warmOps, scanBlocks := int64(512<<20), 512, 400, 20_000, 4096
	if *quick {
		objBytes, cacheSlots, hot, warmOps, scanBlocks = 32<<20, 64, 50, 2_000, 512
	}
	res, err := run(objBytes, cacheSlots, hot, warmOps, scanBlocks)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("cell,gets,mib_or_sec,extra")
	for _, c := range res.scan {
		fmt.Printf("%s,%d,%.2f,\n", c.name, c.gets, c.mib)
	}
	for _, c := range res.plans {
		fmt.Printf("%s,%d,,\n", c.name, c.gets)
	}
	for _, c := range res.fans {
		fmt.Printf("%s,,%.2f,%s\n", c.name, c.mib, c.extra)
	}
	for _, c := range res.adm {
		fmt.Printf("%s,%d,,%s\n", c.name, c.gets, c.extra)
	}
}
