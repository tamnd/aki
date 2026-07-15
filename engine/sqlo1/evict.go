package sqlo1

import (
	"math"
	"math/rand/v2"
)

// Eviction policy constants, doc 04 section 8: sample K resident headers
// uniformly, score them WATT-lite, evict the lowest-scoring 10 percent
// of the sample, writes weigh 2x reads. The hotclock lab found the
// scoring a wash against plain clock at the verdict promotion rate, so
// these are robustness defaults, not load-bearing tuning.
const (
	evictSampleK = 64
	evictWRead   = 1.0
	evictWWrite  = 2.0
)

// stampWorth is the lab's WATT access-rate estimate n/(now-oldest+1)
// over a two-stamp class. Stamp zero means unset, so the server clock
// starts at tick 1.
func stampWorth(last, prev, now uint32) float64 {
	switch {
	case last == 0:
		return 0
	case prev == 0:
		return 1 / float64(now-last+1)
	default:
		return 2 / float64(now-prev+1)
	}
}

type evictCand struct {
	slot  uint32
	score float64
}

// evictor is the WATT-lite sampled evictor. Clean-first is structural:
// only resident headers are candidates, so dirty records are never
// evicted (R-I3) and eviction never performs IO (R-I5); when residents
// run out the caller gets less than it asked for and the backpressure
// rung, not this code, forces a drain.
type evictor struct {
	ht    *HotTable
	rng   *rand.Rand
	cands []evictCand
	// evictions and evictedBytes count policy victims. Only clean
	// residents are candidates, so these double as the clean-first
	// measurement: every unit counted here was clean and cost no IO.
	evictions    int64
	evictedBytes int64
}

func newEvictor(ht *HotTable, seed uint64) *evictor {
	return &evictor{
		ht:    ht,
		rng:   rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		cands: make([]evictCand, 0, evictSampleK),
	}
}

func (e *evictor) score(hd *hdr) float64 {
	now := e.ht.tick
	return evictWRead*stampWorth(hd.lastRead, hd.prevRead, now) +
		evictWWrite*stampWorth(hd.lastWrite, hd.prevWrite, now)
}

// sample fills the candidate buffer with up to evictSampleK resident
// slots drawn uniformly from the header array, giving up after a bounded
// number of misses so a dirty-heavy table cannot spin it.
func (e *evictor) sample() int {
	e.cands = e.cands[:0]
	if len(e.ht.hdrs) == 0 {
		return 0
	}
	for attempts := 0; len(e.cands) < evictSampleK && attempts < evictSampleK*8; attempts++ {
		s := uint32(e.rng.IntN(len(e.ht.hdrs)))
		hd := &e.ht.hdrs[s]
		if hd.state != stateResident {
			continue
		}
		e.cands = append(e.cands, evictCand{slot: s, score: e.score(hd)})
	}
	return len(e.cands)
}

// evict frees at least need bytes of hot-tier payload (header plus key
// plus value bytes) and returns what it actually freed, which is less
// than need only when resident candidates ran out. Every victim keeps a
// ghost: the hotclock lab ghosted every eviction and its verdict is
// what makes the stingy promotion coin safe, and the direct-mapped ring
// forgets cold ghosts by replacement anyway.
func (e *evictor) evict(need int) int {
	freed := 0
	for freed < need {
		k := e.sample()
		if k == 0 {
			break
		}
		for range max(k/10, 1) {
			best := -1
			for i := range e.cands {
				if e.cands[i].score < math.MaxFloat64 && (best < 0 || e.cands[i].score < e.cands[best].score) {
					best = i
				}
			}
			if best < 0 {
				break
			}
			c := &e.cands[best]
			c.score = math.MaxFloat64 // spent
			hd := &e.ht.hdrs[c.slot]
			if hd.state != stateResident {
				continue // the sample drew this slot twice
			}
			n := hdrSize + int(hd.klen) + len(e.ht.vals.data(hd.valRef))
			freed += n
			e.evictions++
			e.evictedBytes += int64(n)
			e.ht.evict(c.slot, true)
		}
	}
	return freed
}
