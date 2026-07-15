package main

import (
	"bytes"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

// TestRunAllSmoke runs the quick shape and checks the CSV: one row per
// arm in order, eight columns, numerics parsing.
func TestRunAllSmoke(t *testing.T) {
	cfg := config{fields: 20000, maxEnts: 71, churnPct: 25, samplesX: 10, seed: 71}
	var out bytes.Buffer
	if err := runAll(cfg, &out); err != nil {
		t.Fatalf("runAll: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != len(arms) {
		t.Fatalf("got %d rows, want %d:\n%s", len(lines), len(arms), out.String())
	}
	for i, line := range lines {
		fields := strings.Split(line, ",")
		if len(fields) != 8 {
			t.Fatalf("row has %d fields, want 8: %q", len(fields), line)
		}
		if fields[0] != arms[i] {
			t.Fatalf("row %d arm %q, want %q", i, fields[0], arms[i])
		}
		for _, idx := range []int{1, 2, 3, 4, 5, 6, 7} {
			if _, err := strconv.ParseFloat(fields[idx], 64); err != nil {
				t.Fatalf("field %d not numeric in %q: %v", idx, line, err)
			}
		}
	}
}

// TestBuildInvariants checks the insert-split-churn model: live count
// conserved, no segment over the split threshold, fence sorted, every
// live field inside its fence range and findable, every dead one gone.
func TestBuildInvariants(t *testing.T) {
	cfg := config{fields: 30000, maxEnts: 31, churnPct: 40, samplesX: 1, seed: 3}
	rng := rand.New(rand.NewSource(cfg.seed))
	h := build(cfg, rng)

	if len(h.live) != cfg.fields {
		t.Fatalf("live count %d, want %d after balanced churn", len(h.live), cfg.fields)
	}
	total := 0
	for i, s := range h.segs {
		if len(s.ents) > h.maxEnts {
			t.Fatalf("segment %d holds %d entries, threshold %d", i, len(s.ents), h.maxEnts)
		}
		if s.lo != h.los[i] {
			t.Fatalf("segment %d lo %d disagrees with fence %d", i, s.lo, h.los[i])
		}
		if i > 0 && h.los[i] <= h.los[i-1] {
			t.Fatalf("fence not strictly sorted at %d", i)
		}
		hi := uint64(1<<64 - 1)
		if i+1 < len(h.los) {
			hi = h.los[i+1]
		}
		for _, e := range s.ents {
			if !h.alive[e.id] {
				t.Fatalf("dead field %d still in segment %d", e.id, i)
			}
			if e.fh != h.fhs[e.id] {
				t.Fatalf("field %d fh mismatch", e.id)
			}
			if e.fh < s.lo || (i+1 < len(h.los) && e.fh >= hi) {
				t.Fatalf("field %d fh %d outside segment %d range", e.id, e.fh, i)
			}
		}
		total += len(s.ents)
	}
	if total != len(h.live) {
		t.Fatalf("segments hold %d entries, live list has %d", total, len(h.live))
	}
}

// TestSamplerRatios hand-builds three segments with weights 1, 0, and
// 3 and checks the draw shares land on 25/0/75 within a percent.
func TestSamplerRatios(t *testing.T) {
	h := &hash{
		maxEnts: 8,
		los:     []uint64{0, 1 << 62, 1 << 63},
		segs:    []*segment{{lo: 0}, {lo: 1 << 62}, {lo: 1 << 63}},
	}
	addField := func(si int, f uint64) {
		id := int32(len(h.fhs))
		h.fhs = append(h.fhs, f)
		h.alive = append(h.alive, true)
		h.live = append(h.live, id)
		h.segs[si].ents = append(h.segs[si].ents, ent{fh: f, id: id})
	}
	addField(0, 1)
	addField(0, 2)
	for i := range 6 {
		addField(2, 1<<63+uint64(i))
	}

	const draws = 200000
	obs := make([]int64, len(h.fhs))
	rng := rand.New(rand.NewSource(9))
	sample(h, []float64{1, 0, 3}, draws, rng, obs)

	var per [3]int64
	for si, s := range h.segs {
		for _, e := range s.ents {
			per[si] += obs[e.id]
		}
	}
	if per[1] != 0 {
		t.Fatalf("zero-weight segment drew %d", per[1])
	}
	if got := float64(per[0]) / draws; math.Abs(got-0.25) > 0.01 {
		t.Fatalf("segment 0 share %.3f, want 0.25", got)
	}
	if got := float64(per[2]) / draws; math.Abs(got-0.75) > 0.01 {
		t.Fatalf("segment 2 share %.3f, want 0.75", got)
	}
}

// TestExactPassesNullFails is the oracle for the verdict rule: on a
// churned hash the exact weighting must sit inside the chi-square
// band while the unweighted null blows far out of it.
func TestExactPassesNullFails(t *testing.T) {
	cfg := config{fields: 5000, maxEnts: 31, churnPct: 40, samplesX: 50, seed: 17}
	rng := rand.New(rand.NewSource(cfg.seed))
	h := build(cfg, rng)
	draws := len(h.live) * cfg.samplesX

	for _, tc := range []struct {
		arm  string
		pass bool
	}{
		{"exact", true},
		{"fill15", true},
		{"unweighted", false},
	} {
		w, err := weightsFor(tc.arm, h)
		if err != nil {
			t.Fatal(err)
		}
		obs := make([]int64, len(h.fhs))
		sample(h, w, draws, rng, obs)
		v := judge(h, obs, draws)
		if tc.pass && math.Abs(v.z) > 5 {
			t.Fatalf("%s: |z| = %.1f, want < 5", tc.arm, v.z)
		}
		if !tc.pass && v.z < 10 {
			t.Fatalf("%s: z = %.1f, want the null to fail with z >= 10", tc.arm, v.z)
		}
	}
}
