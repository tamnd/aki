package main

import (
	"math/rand"
	"testing"
)

func testSet(t *testing.T, members int) *set {
	t.Helper()
	rng := rand.New(rand.NewSource(7))
	return build(config{members: members, maxEnts: 61, churnPct: 25}, rng)
}

// TestBuildInvariants pins the model: the fence partitions fh space,
// every entry sits in its fence range, splits respect the threshold,
// and churn conserves the member count.
func TestBuildInvariants(t *testing.T) {
	s := testSet(t, 30000)
	if s.live != 30000 {
		t.Fatalf("live = %d after churn, want 30000", s.live)
	}
	total := 0
	for i, sg := range s.segs {
		if sg.lo != s.los[i] {
			t.Fatalf("segment %d lo %d disagrees with fence %d", i, sg.lo, s.los[i])
		}
		if i > 0 && s.los[i] <= s.los[i-1] {
			t.Fatalf("fence not strictly increasing at %d", i)
		}
		hi := uint64(1<<64 - 1)
		if i+1 < len(s.los) {
			hi = s.los[i+1] - 1
		}
		for _, e := range sg.ents {
			if e.fh < sg.lo || e.fh > hi {
				t.Fatalf("entry fh %d outside segment %d range [%d, %d]", e.fh, i, sg.lo, hi)
			}
		}
		if len(sg.ents) > s.maxEnts {
			t.Fatalf("segment %d holds %d entries past the %d threshold", i, len(sg.ents), s.maxEnts)
		}
		total += len(sg.ents)
	}
	if total != s.live {
		t.Fatalf("segments hold %d entries, live says %d", total, s.live)
	}
}

// TestAllocate pins the allocator contract: takes sum to count, no
// take exceeds its segment, and the picked indexes are distinct and in
// range, which together make the draw a distinct uniform subset.
func TestAllocate(t *testing.T) {
	s := testSet(t, 20000)
	pre := prefix(s)
	rng := rand.New(rand.NewSource(9))
	for _, count := range []int{1, 100, s.live / 3, s.live} {
		takes, picks := allocate(pre, count, rng)
		sum := 0
		for i, take := range takes {
			if take > len(s.segs[i].ents) {
				t.Fatalf("count %d: segment %d take %d exceeds live %d", count, i, take, len(s.segs[i].ents))
			}
			sum += take
			seen := map[int]bool{}
			for _, p := range picks[i] {
				if p < 0 || p >= len(s.segs[i].ents) {
					t.Fatalf("count %d: pick %d out of range in segment %d", count, p, i)
				}
				if seen[p] {
					t.Fatalf("count %d: segment %d picked index %d twice", count, i, p)
				}
				seen[p] = true
			}
			if len(picks[i]) != take {
				t.Fatalf("count %d: segment %d has %d picks for take %d", count, i, len(picks[i]), take)
			}
		}
		if sum != count {
			t.Fatalf("takes sum to %d, want %d", sum, count)
		}
	}
}

// TestUniformityOracle is the pass/fail contract the sweep reads by:
// the position allocator sits inside |z| < 3 and the even-per-segment
// null fails loudly.
func TestUniformityOracle(t *testing.T) {
	s := testSet(t, 20000)
	pre := prefix(s)
	rng := rand.New(rand.NewSource(11))
	count := s.live / 20
	trials := 400
	for _, arm := range []string{"positions", "even"} {
		obs := make([]int64, s.live)
		for range trials {
			var picks [][]int
			if arm == "positions" {
				_, picks = allocate(pre, count, rng)
			} else {
				_, picks = allocateEven(pre, count, rng)
			}
			for i, ps := range picks {
				for _, p := range ps {
					obs[s.segs[i].ents[p].id]++
				}
			}
		}
		v := judge(obs, trials, count, s.live)
		if arm == "positions" && (v.z < -3 || v.z > 3) {
			t.Fatalf("position allocator z = %.2f, want |z| < 3", v.z)
		}
		if arm == "even" && v.z < 10 {
			t.Fatalf("even null z = %.2f, want a loud failure above 10", v.z)
		}
	}
}

// TestCostModel pins the pricing edges: popping everything is one
// delete frame per segment plus the root with no payload on the edit
// arm and a bare root delete on the rebuild arm, and a one-member pop
// touches one segment.
func TestCostModel(t *testing.T) {
	s := testSet(t, 20000)
	pre := prefix(s)
	rng := rand.New(rand.NewSource(13))

	takes, _ := allocate(pre, s.live, rng)
	edit, touched, emptied := editCost(s, takes)
	if touched != len(s.segs) || emptied != len(s.segs) {
		t.Fatalf("full pop touched %d emptied %d, want all %d segments", touched, emptied, len(s.segs))
	}
	if edit.frames != len(s.segs)+1 || edit.bytes != 64 {
		t.Fatalf("full pop edit cost = %+v, want %d delete frames plus the root", edit, len(s.segs)+1)
	}
	if reb := rebuildCost(s, s.live); reb.frames != 1 || reb.bytes != 0 {
		t.Fatalf("full pop rebuild cost = %+v, want a bare root delete", reb)
	}

	takes, _ = allocate(pre, 1, rng)
	edit, touched, _ = editCost(s, takes)
	if touched != 1 || edit.frames != 2 {
		t.Fatalf("one-member pop touched %d segments with %d frames, want 1 and 2", touched, edit.frames)
	}
}
