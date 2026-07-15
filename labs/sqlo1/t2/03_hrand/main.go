// Lab: HRANDFIELD fence-weighted sampling (spec 2064/sqlo1 doc 06
// section 3 operator table, milestone T2 lab 03).
//
// HRANDFIELD picks a segment through the fence, weighted by the fill
// class the 16-bit fence-entry meta carries, then a uniform entry
// inside it; T2 slice 8 bakes the weighting rule and the fill-class
// bit width. Segments split at the entry median, so occupancy sits
// anywhere between half full and full and drifts lower under deletes
// (merges are lazy), which means unweighted segment picking is biased
// by construction; and rules W1 and W2 drain fill and segments in the
// same batch, so the drained fill is never stale, leaving quantization
// as the only approximation the design can actually ship.
//
// The lab builds a segmented hash through the real insert-split path,
// churns it with deletes and reinserts to widen the occupancy spread,
// then draws with-replacement samples (the negative-count contract)
// under four weightings: exact counts, the 15-bit capped fill class as
// shipped, a 4-bit quantized class, and the unweighted null that must
// fail. Chi-square against uniform over all live fields is the
// verdict; the segment-level worst deviation shows where the bias
// lives. No store underneath: the per-draw record cost is the operator
// table's O(samples) and hfence prices record reads, while this lab
// guards the distribution.
package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"slices"
	"sort"
	"time"
)

type config struct {
	fields   int
	maxEnts  int
	churnPct int
	samplesX int
	seed     int64
}

type ent struct {
	fh uint64
	id int32
}

type segment struct {
	lo   uint64
	ents []ent
}

// hash is the sampling model: a fence of segments built through the
// same insert-split path slice 2 ships, entries carrying field ids so
// draws can be tallied per field.
type hash struct {
	maxEnts int
	los     []uint64 // fence lo values, sorted
	segs    []*segment
	fhs     []uint64
	alive   []bool
	live    []int32
}

func newHash(maxEnts int) *hash {
	return &hash{
		maxEnts: maxEnts,
		los:     []uint64{0},
		segs:    []*segment{{lo: 0}},
	}
}

func (h *hash) segFor(f uint64) int {
	return sort.Search(len(h.los), func(i int) bool { return h.los[i] > f }) - 1
}

func (h *hash) insert(rng *rand.Rand) {
	f := rng.Uint64()
	id := int32(len(h.fhs))
	h.fhs = append(h.fhs, f)
	h.alive = append(h.alive, true)
	h.live = append(h.live, id)
	si := h.segFor(f)
	s := h.segs[si]
	s.ents = append(s.ents, ent{fh: f, id: id})
	if len(s.ents) <= h.maxEnts {
		return
	}
	// Split at the entry-median fh, the slice 2 rule; the sort is paid
	// only when a segment overflows.
	slices.SortFunc(s.ents, func(a, b ent) int {
		if a.fh < b.fh {
			return -1
		}
		if a.fh > b.fh {
			return 1
		}
		return 0
	})
	mid := len(s.ents) / 2
	newLo := s.ents[mid].fh
	for mid > 0 && s.ents[mid-1].fh == newLo {
		mid--
	}
	if mid == 0 {
		return
	}
	ns := &segment{lo: newLo, ents: slices.Clone(s.ents[mid:])}
	s.ents = s.ents[:mid]
	i := si + 1
	h.los = slices.Insert(h.los, i, newLo)
	h.segs = slices.Insert(h.segs, i, ns)
}

// remove kills one uniformly chosen live field, leaving its segment
// thinner; nothing merges, which is the doc 06 lazy-merge posture and
// exactly what widens the occupancy spread weighting must survive.
func (h *hash) remove(rng *rand.Rand) {
	li := rng.Intn(len(h.live))
	id := h.live[li]
	h.live[li] = h.live[len(h.live)-1]
	h.live = h.live[:len(h.live)-1]
	h.alive[id] = false
	s := h.segs[h.segFor(h.fhs[id])]
	for i := range s.ents {
		if s.ents[i].id == id {
			s.ents[i] = s.ents[len(s.ents)-1]
			s.ents = s.ents[:len(s.ents)-1]
			return
		}
	}
	panic("hrand: live field missing from its segment")
}

func build(cfg config, rng *rand.Rand) *hash {
	h := newHash(cfg.maxEnts)
	for range cfg.fields {
		h.insert(rng)
	}
	churn := cfg.fields * cfg.churnPct / 100
	for range churn {
		h.remove(rng)
	}
	for range churn {
		h.insert(rng)
	}
	return h
}

// weightsFor derives the per-segment weight an arm samples with. The
// fill15 arm is the shipped encoding: the entry count capped into the
// 15 bits the fence meta has left of its 16 after the has-TTL bit.
// The quant4 arm compresses to a 4-bit class, the floor if meta bits
// get taken later; empty classes clamp to 1 so no live field becomes
// unreachable.
func weightsFor(arm string, h *hash) ([]float64, error) {
	w := make([]float64, len(h.segs))
	for i, s := range h.segs {
		n := len(s.ents)
		switch arm {
		case "exact":
			w[i] = float64(n)
		case "fill15":
			w[i] = float64(min(n, 0x7fff))
		case "quant4":
			if n == 0 {
				w[i] = 0
			} else {
				w[i] = float64(max(1, (n*15+h.maxEnts/2)/h.maxEnts))
			}
		case "unweighted":
			// The null arm still skips emptied segments: a real
			// implementation without weights would redraw on them, and
			// giving them mass would only make the null look worse.
			if n == 0 {
				w[i] = 0
			} else {
				w[i] = 1
			}
		default:
			return nil, fmt.Errorf("unknown arm %q", arm)
		}
	}
	return w, nil
}

// sample draws with replacement: segment by cumulative-weight binary
// search, entry uniform within, which is the negative-count HRANDFIELD
// contract; positive counts sample distinct on the same per-draw law.
func sample(h *hash, w []float64, draws int, rng *rand.Rand, obs []int64) time.Duration {
	cum := make([]float64, len(w))
	total := 0.0
	for i, x := range w {
		total += x
		cum[i] = total
	}
	t0 := time.Now()
	for range draws {
		x := rng.Float64() * total
		si := sort.SearchFloat64s(cum, x)
		if si >= len(h.segs) {
			si = len(h.segs) - 1
		}
		s := h.segs[si]
		if len(s.ents) == 0 {
			continue // a zero-weight empty segment cannot be drawn; guard anyway
		}
		obs[s.ents[rng.Intn(len(s.ents))].id]++
	}
	return time.Since(t0)
}

type verdict struct {
	chi2PerDof float64
	z          float64
	maxSegDev  float64
	drawn      int64
}

// judge computes the chi-square of the draws against uniform over all
// live fields and the worst segment-level relative deviation, which is
// where weighting bias shows before per-field noise settles.
func judge(h *hash, obs []int64, draws int) verdict {
	nLive := len(h.live)
	exp := float64(draws) / float64(nLive)
	chi2 := 0.0
	var drawn int64
	for id, ok := range h.alive {
		if !ok {
			continue
		}
		d := float64(obs[id]) - exp
		chi2 += d * d / exp
		drawn += obs[id]
	}
	dof := float64(nLive - 1)
	maxDev := 0.0
	for _, s := range h.segs {
		if len(s.ents) == 0 {
			continue
		}
		segExp := exp * float64(len(s.ents))
		var segObs int64
		for _, e := range s.ents {
			segObs += obs[e.id]
		}
		if dev := math.Abs(float64(segObs)-segExp) / segExp * 100; dev > maxDev {
			maxDev = dev
		}
	}
	return verdict{
		chi2PerDof: chi2 / dof,
		z:          (chi2 - dof) / math.Sqrt(2*dof),
		maxSegDev:  maxDev,
		drawn:      drawn,
	}
}

var arms = []string{"exact", "fill15", "quant4", "unweighted"}

func runAll(cfg config, out io.Writer) error {
	rng := rand.New(rand.NewSource(cfg.seed))
	h := build(cfg, rng)
	occMin, occMax := h.maxEnts, 0
	for _, s := range h.segs {
		occMin, occMax = min(occMin, len(s.ents)), max(occMax, len(s.ents))
	}
	fmt.Fprintf(os.Stderr, "hrand: %d live fields, %d segments, occupancy %d..%d of %d\n",
		len(h.live), len(h.segs), occMin, occMax, h.maxEnts)

	draws := len(h.live) * cfg.samplesX
	for _, arm := range arms {
		w, err := weightsFor(arm, h)
		if err != nil {
			return err
		}
		obs := make([]int64, len(h.fhs))
		dur := sample(h, w, draws, rng, obs)
		v := judge(h, obs, draws)
		if v.drawn != int64(draws) {
			return fmt.Errorf("%s: %d draws tallied, want %d", arm, v.drawn, draws)
		}
		fmt.Fprintf(out, "%s,%d,%d,%d,%.3f,%.2f,%.2f,%.0f\n",
			arm, len(h.live), len(h.segs), draws,
			v.chi2PerDof, v.z, v.maxSegDev,
			float64(dur.Nanoseconds())/float64(draws))
	}
	return nil
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.IntVar(&cfg.fields, "fields", 200000, "fields inserted before churn")
	flag.IntVar(&cfg.maxEnts, "maxents", 71, "entries per segment before a split (doc 06 occupancy target)")
	flag.IntVar(&cfg.churnPct, "churn", 25, "percent of fields deleted and reinserted before sampling")
	flag.IntVar(&cfg.samplesX, "samplesx", 20, "draws per live field")
	flag.Int64Var(&cfg.seed, "seed", 71, "rng seed")
	flag.Parse()
	if *quick {
		cfg.fields, cfg.samplesX = 20000, 10
	}
	if err := runAll(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "hrand:", err)
		os.Exit(1)
	}
}
