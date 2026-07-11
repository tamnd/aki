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
