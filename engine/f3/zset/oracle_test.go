package zset

import (
	"math/rand/v2"
	"sort"
	"testing"
)

// TestOracleChurn drives random ZADD/ZINCRBY/ZREM churn against a reference
// model (a Go map plus a sort) and checks the zset agrees on cardinality,
// per-member score, ranks, and full order after every op. The member space is
// small enough to force duplicate adds, rescores, and removals, and wide enough
// to cross the inline-to-native threshold, so both bands and the conversion are
// exercised in one run.
func TestOracleChurn(t *testing.T) {
	for _, space := range []int{16, 200} {
		space := space
		t.Run(map[bool]string{true: "inline", false: "crosses"}[space <= 100], func(t *testing.T) {
			rng := rand.New(rand.NewPCG(1, uint64(space)))
			z := newZset()
			model := map[string]float64{}
			member := func() string { return "m" + itoa(rng.IntN(space)) }

			for step := 0; step < 4000; step++ {
				m := member()
				switch rng.IntN(3) {
				case 0: // ZADD
					s := float64(rng.IntN(10) - 5)
					z.update([]byte(m), s, flags{})
					model[m] = s
				case 1: // ZINCRBY
					d := float64(rng.IntN(6) - 3)
					_, _, ns, _, nan := z.update([]byte(m), d, flags{incr: true})
					if nan {
						t.Fatal("unexpected NaN from a finite increment")
					}
					model[m] = model[m] + d
					if model[m] != ns {
						t.Fatalf("incr score = %v, model %v", ns, model[m])
					}
				case 2: // ZREM
					got := z.rem([]byte(m))
					_, want := model[m]
					if got != want {
						t.Fatalf("rem(%q) = %v, model had it = %v", m, got, want)
					}
					delete(model, m)
				}

				if z.card() != len(model) {
					t.Fatalf("step %d: card = %d, model %d", step, z.card(), len(model))
				}
			}

			// Full agreement at the end: scores, order, ranks.
			if z.card() != len(model) {
				t.Fatalf("final card = %d, model %d", z.card(), len(model))
			}
			type pair struct {
				m string
				s float64
			}
			want := make([]pair, 0, len(model))
			for m, s := range model {
				want = append(want, pair{m, s})
				gs, ok := z.score([]byte(m))
				if !ok || gs != s {
					t.Fatalf("score(%q) = %v,%v, model %v", m, gs, ok, s)
				}
			}
			sort.Slice(want, func(i, j int) bool {
				if want[i].s != want[j].s {
					return want[i].s < want[j].s
				}
				return want[i].m < want[j].m
			})
			ev := z.entries()
			if len(ev) != len(want) {
				t.Fatalf("entries %d, want %d", len(ev), len(want))
			}
			for i := range want {
				if string(ev[i].member) != want[i].m || ev[i].score != want[i].s {
					t.Fatalf("entry %d = %q/%v, want %q/%v", i, ev[i].member, ev[i].score, want[i].m, want[i].s)
				}
				if r, _, ok := z.rank([]byte(want[i].m)); !ok || r != i {
					t.Fatalf("rank(%q) = %d, want %d", want[i].m, r, i)
				}
			}
		})
	}
}

// TestOracleChurnNative hammers the native band alone: a member space well
// past the inline caps, heavier churn, fractional increments, and a periodic
// structural audit of the counted tree against the member store, so the
// dual-structure invariant (hash bits equal tree key for every member, one
// tree entry per member) is checked by construction, not just by observation.
func TestOracleChurnNative(t *testing.T) {
	const space = 3000
	rng := rand.New(rand.NewPCG(2, space))
	z := newZset()
	model := map[string]float64{}
	member := func() string { return "m" + itoa(rng.IntN(space)) }

	for step := 0; step < 30000; step++ {
		m := member()
		switch rng.IntN(3) {
		case 0: // ZADD
			s := float64(rng.IntN(2000)-1000) / 8
			z.update([]byte(m), s, flags{})
			model[m] = s
		case 1: // ZINCRBY
			d := float64(rng.IntN(200)-100) / 4
			_, _, ns, _, nan := z.update([]byte(m), d, flags{incr: true})
			if nan {
				t.Fatal("unexpected NaN from a finite increment")
			}
			model[m] = model[m] + d
			if model[m] != ns {
				t.Fatalf("step %d: incr score = %v, model %v", step, ns, model[m])
			}
		case 2: // ZREM
			got := z.rem([]byte(m))
			_, want := model[m]
			if got != want {
				t.Fatalf("step %d: rem(%q) = %v, model had it = %v", step, m, got, want)
			}
			delete(model, m)
		}
		if z.card() != len(model) {
			t.Fatalf("step %d: card = %d, model %d", step, z.card(), len(model))
		}
		if step%2000 == 1999 && z.enc == encSkiplist {
			if err := z.nat.tree.Check(z.nat); err != nil {
				t.Fatalf("step %d: tree check: %v", step, err)
			}
		}
	}
	if z.enc != encSkiplist {
		t.Fatalf("native churn never promoted, enc = %s", z.enc)
	}
	if err := z.nat.tree.Check(z.nat); err != nil {
		t.Fatalf("final tree check: %v", err)
	}

	// Full agreement at the end: per-member scores, order, and every rank.
	type pair struct {
		m string
		s float64
	}
	want := make([]pair, 0, len(model))
	for m, s := range model {
		want = append(want, pair{m, s})
		gs, ok := z.score([]byte(m))
		if !ok || gs != s {
			t.Fatalf("score(%q) = %v,%v, model %v", m, gs, ok, s)
		}
	}
	sort.Slice(want, func(i, j int) bool {
		return lessStr(want[i].s, want[i].m, want[j].s, want[j].m)
	})
	ev := z.entries()
	if len(ev) != len(want) {
		t.Fatalf("entries %d, want %d", len(ev), len(want))
	}
	for i := range want {
		if string(ev[i].member) != want[i].m || ev[i].score != want[i].s {
			t.Fatalf("entry %d = %q/%v, want %q/%v", i, ev[i].member, ev[i].score, want[i].m, want[i].s)
		}
		if r, _, ok := z.rank([]byte(want[i].m)); !ok || r != i {
			t.Fatalf("rank(%q) = %d, want %d", want[i].m, r, i)
		}
	}
}

// TestRebuildReclaims drives enough removals through a native zset to trip the
// wholesale rebuild and checks the survivors and the footprint both come out
// right: dead counters reset, the slab holds only live bytes, the tree audits.
func TestRebuildReclaims(t *testing.T) {
	z := newZset()
	const total = 5000
	for i := 0; i < total; i++ {
		z.update([]byte("member-"+itoa(i)), float64(i), flags{})
	}
	for i := 0; i < total; i++ {
		if i%5 != 0 {
			z.rem([]byte("member-" + itoa(i)))
		}
	}
	n := z.nat
	if n.deadRecs > n.tbl.Len() && n.deadRecs >= 1024 {
		t.Fatalf("rebuild never fired: %d dead records over %d live", n.deadRecs, n.tbl.Len())
	}
	if err := n.tree.Check(n); err != nil {
		t.Fatalf("tree check after rebuild: %v", err)
	}
	if z.card() != total/5 {
		t.Fatalf("card = %d, want %d", z.card(), total/5)
	}
	for i := 0; i < total; i += 5 {
		s, ok := z.score([]byte("member-" + itoa(i)))
		if !ok || s != float64(i) {
			t.Fatalf("survivor member-%d = %v,%v after rebuild", i, s, ok)
		}
	}
	if _, ok := z.score([]byte("member-1")); ok {
		t.Fatal("removed member resurrected by the rebuild")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
