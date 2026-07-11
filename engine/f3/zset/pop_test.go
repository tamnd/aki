package zset

import (
	"math/rand/v2"
	"sort"
	"testing"
)

// popEmit collects the (member, score) sequence z.pop hands back, copying the
// member out of the aliased storage since the next step reslices over it.
func popEmit(z *zset, min bool, count int) []pairMS {
	var got []pairMS
	z.pop(min, count, func(m []byte, s float64) {
		got = append(got, pairMS{string(m), s})
	})
	return got
}

// TestPopModel drives random ZADD then random-count ZPOPMIN/ZPOPMAX against a
// sorted reference model, over both bands, and audits the counted tree after
// every native pop. The min band emits ascending from the low end, the max band
// emits descending from the high end, and the reply caps at the cardinality when
// the count runs past it, so every one of those rules is checked by construction.
func TestPopModel(t *testing.T) {
	for _, size := range []int{50, 2000} {
		size := size
		name := map[bool]string{true: "inline", false: "native"}[size <= maxListpackEntries]
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(9, uint64(size)))
			z := newZset()
			model := map[string]float64{}
			for i := 0; i < size; i++ {
				m := "m" + itoa(i)
				s := float64(rng.IntN(size)) // small score space so bands tie
				z.update([]byte(m), s, flags{})
				model[m] = s
			}

			for z.card() > 0 {
				min := rng.IntN(2) == 0
				count := 1 + rng.IntN(37) // spans single, cross-leaf, and past-card
				want := modelPop(model, min, count)
				got := popEmit(z, min, count)
				if !eqPairs(got, want) {
					t.Fatalf("size %d min=%v count=%d:\n got  %v\n want %v", size, min, count, got, want)
				}
				for _, p := range want {
					delete(model, p.m)
				}
				if z.card() != len(model) {
					t.Fatalf("card = %d, model %d after pop", z.card(), len(model))
				}
				if z.enc == encSkiplist {
					if err := z.nat.tree.Check(z.nat); err != nil {
						t.Fatalf("tree check after pop: %v", err)
					}
				}
			}
			if len(model) != 0 {
				t.Fatalf("model still holds %d after drain", len(model))
			}
			// A pop on the drained set yields nothing and drops no key underneath.
			if got := popEmit(z, true, 5); len(got) != 0 {
				t.Fatalf("pop on empty set emitted %v", got)
			}
		})
	}
}

// TestPopDropsEmptied confirms a native pop that takes the last member leaves the
// set empty and audits clean, the last-member-empties-key rule the handler keys
// its DEL-on-empty on.
func TestPopDropsEmptied(t *testing.T) {
	z := buildNative(300)
	got := popEmit(z, false, 1000) // more than the cardinality
	if len(got) != 300 {
		t.Fatalf("popped %d, want 300", len(got))
	}
	if z.card() != 0 {
		t.Fatalf("card = %d after draining every member", z.card())
	}
	// Descending from the top: member:0299 down to member:0000.
	if got[0].m != "member:"+pad(299) || got[299].m != "member:"+pad(0) {
		t.Fatalf("drain order wrong: first %q last %q", got[0].m, got[299].m)
	}
}

// modelPop is the reference pop: the sorted model's low end for a min pop, its
// high end in descending order for a max pop, capped at the cardinality.
func modelPop(model map[string]float64, min bool, count int) []pairMS {
	seq := sortedModel(model)
	if count > len(seq) {
		count = len(seq)
	}
	out := make([]pairMS, 0, count)
	if min {
		out = append(out, seq[:count]...)
		return out
	}
	for i := 0; i < count; i++ {
		out = append(out, seq[len(seq)-1-i])
	}
	return out
}

func eqPairs(a, b []pairMS) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].m != b[i].m || a[i].s != b[i].s {
			return false
		}
	}
	return true
}

// sanity: sortedModel keeps the total order the pop model leans on.
func TestSortedModelOrder(t *testing.T) {
	m := map[string]float64{"b": 1, "a": 1, "c": 0}
	got := sortedModel(m)
	want := []pairMS{{"c", 0}, {"a", 1}, {"b", 1}}
	if !sort.SliceIsSorted(got, func(i, j int) bool {
		return lessStr(got[i].s, got[i].m, got[j].s, got[j].m)
	}) || !eqPairs(got, want) {
		t.Fatalf("sortedModel = %v, want %v", got, want)
	}
}
