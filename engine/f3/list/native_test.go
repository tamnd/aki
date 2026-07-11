package list

import (
	"bytes"
	"math/rand/v2"
	"strconv"
	"testing"
)

// The model-oracle differential for the chunked deque (spec 2064/f3/13 section
// 2). A long random stream of every list command runs against the real *list
// and against a dead-simple []string reference with the same semantics; after
// every step the deque's order, length, and positional reads must equal the
// model's, and the encoding must track (sticky, never quicklist->listpack). The
// value bands (tiny tokens, 64B, 1KiB, 4KiB, and a single oversized element)
// force the list across the inline budget and into many chunks, so the deque's
// split, recycle, and directory paths are all exercised. Seeded deterministically
// so a failure reproduces.

// listModel is the reference: an ordered slice of owned byte strings with the
// list command semantics implemented the obvious O(n) way, independent of the
// package's own removeMatches/lposScan so the differential is a real oracle.
type listModel struct{ e [][]byte }

func (m *listModel) len() int              { return len(m.e) }
func (m *listModel) pushBack(v []byte)     { m.e = append(m.e, clone(v)) }
func (m *listModel) pushFront(v []byte)    { m.e = append([][]byte{clone(v)}, m.e...) }
func (m *listModel) popFront()             { m.e = m.e[1:] }
func (m *listModel) popBack()              { m.e = m.e[:len(m.e)-1] }
func (m *listModel) setAt(i int, v []byte) { m.e[i] = clone(v) }

func (m *listModel) insert(before bool, pivot, v []byte) bool {
	for i, x := range m.e {
		if bytes.Equal(x, pivot) {
			at := i
			if !before {
				at++
			}
			m.e = append(m.e, nil)
			copy(m.e[at+1:], m.e[at:])
			m.e[at] = clone(v)
			return true
		}
	}
	return false
}

func (m *listModel) remove(count int, v []byte) int {
	out := m.e[:0:0]
	removed := 0
	if count >= 0 {
		lim := count
		for _, x := range m.e {
			if bytes.Equal(x, v) && (lim == 0 || removed < lim) {
				removed++
				continue
			}
			out = append(out, x)
		}
	} else {
		lim := -count
		// Mark from the tail, keep the rest in order.
		drop := make([]bool, len(m.e))
		for i := len(m.e) - 1; i >= 0 && removed < lim; i-- {
			if bytes.Equal(m.e[i], v) {
				drop[i] = true
				removed++
			}
		}
		for i, x := range m.e {
			if !drop[i] {
				out = append(out, x)
			}
		}
	}
	m.e = out
	return removed
}

func (m *listModel) trim(start, stop int) {
	if start > stop {
		m.e = nil
		return
	}
	kept := make([][]byte, stop-start+1)
	copy(kept, m.e[start:stop+1])
	m.e = kept
}

func clone(v []byte) []byte { c := make([]byte, len(v)); copy(c, v); return c }

// band returns a value from one of the size bands, tagged with a sequence number
// so distinct pushes are distinguishable, plus small repeated tokens so pivot,
// remove, and position scans actually hit.
func band(rng *rand.Rand, seq int) []byte {
	switch rng.IntN(6) {
	case 0, 1: // tiny tokens for match-bearing ops
		return []byte(string(rune('A' + rng.IntN(4))))
	case 2:
		return sized(64, seq)
	case 3:
		return sized(1024, seq)
	case 4:
		return sized(4096, seq)
	default:
		return sized(5000, seq) // oversized: a lone frame beyond the blob budget
	}
}

func sized(n, seq int) []byte {
	b := make([]byte, n)
	tag := strconv.Itoa(seq)
	copy(b, tag)
	for i := len(tag); i < n; i++ {
		b[i] = byte('a' + (i+seq)%26)
	}
	return b
}

func TestNativeAgainstModel(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3} {
		t.Run("seed"+strconv.FormatUint(seed, 10), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 0x9e3779b9))
			l := newList()
			m := &listModel{}
			seq := 0
			everQuick := false

			checkAll := func(stepDesc string) {
				t.Helper()
				if l.length() != m.len() {
					t.Fatalf("%s: length %d, model %d", stepDesc, l.length(), m.len())
				}
				got := decode(l)
				if len(got) != m.len() {
					t.Fatalf("%s: each len %d, model %d", stepDesc, len(got), m.len())
				}
				for i := range got {
					if got[i] != string(m.e[i]) {
						t.Fatalf("%s: elem %d differs", stepDesc, i)
					}
				}
				// Positional reads ride locate: sample a few indices.
				for r := 0; r < 4 && m.len() > 0; r++ {
					i := rng.IntN(m.len())
					if string(l.get(i)) != string(m.e[i]) {
						t.Fatalf("%s: get(%d) differs", stepDesc, i)
					}
				}
				// Encoding is sticky and consistent with the native band.
				if l.encoding() == encQuicklist {
					everQuick = true
					if l.nat == nil {
						t.Fatalf("%s: quicklist encoding but nat nil", stepDesc)
					}
				} else if everQuick {
					t.Fatalf("%s: encoding fell back to listpack", stepDesc)
				}
			}

			for step := 0; step < 4000; step++ {
				seq++
				op := rng.IntN(11)
				switch op {
				case 0:
					v := band(rng, seq)
					l.pushBack(v)
					m.pushBack(v)
				case 1:
					v := band(rng, seq)
					l.pushFront(v)
					m.pushFront(v)
				case 2:
					if m.len() > 0 {
						lv, mv := string(l.popFront()), string(m.e[0])
						m.popFront()
						if lv != mv {
							t.Fatalf("step %d popFront %q, model %q", step, lv, mv)
						}
					}
				case 3:
					if m.len() > 0 {
						lv, mv := string(l.popBack()), string(m.e[m.len()-1])
						m.popBack()
						if lv != mv {
							t.Fatalf("step %d popBack %q, model %q", step, lv, mv)
						}
					}
				case 4: // count-pop from the front
					k := rng.IntN(5)
					for j := 0; j < k && m.len() > 0; j++ {
						if string(l.popFront()) != string(m.e[0]) {
							t.Fatalf("step %d count-popFront differs", step)
						}
						m.popFront()
					}
				case 5: // count-pop from the back
					k := rng.IntN(5)
					for j := 0; j < k && m.len() > 0; j++ {
						if string(l.popBack()) != string(m.e[m.len()-1]) {
							t.Fatalf("step %d count-popBack differs", step)
						}
						m.popBack()
					}
				case 6:
					if m.len() > 0 {
						i := rng.IntN(m.len())
						v := band(rng, seq)
						l.setAt(i, v)
						m.setAt(i, v)
					}
				case 7:
					before := rng.IntN(2) == 0
					pivot := band(rng, seq)
					if m.len() > 0 && rng.IntN(2) == 0 {
						pivot = clone(m.e[rng.IntN(m.len())]) // bias toward a real pivot
					}
					v := band(rng, seq)
					gotOK := l.insert(before, pivot, v)
					wantOK := m.insert(before, pivot, v)
					if gotOK != wantOK {
						t.Fatalf("step %d insert found=%v, model=%v", step, gotOK, wantOK)
					}
				case 8:
					count := rng.IntN(7) - 3
					v := band(rng, seq)
					if m.len() > 0 && rng.IntN(2) == 0 {
						v = clone(m.e[rng.IntN(m.len())])
					}
					got := l.remove(count, v)
					want := m.remove(count, v)
					if got != want {
						t.Fatalf("step %d remove got=%d, model=%d", step, got, want)
					}
				case 9:
					if m.len() > 0 {
						a := rng.IntN(m.len())
						b := rng.IntN(m.len())
						lo, hi := a, b
						if lo > hi {
							lo, hi = hi, lo
						}
						l.trim(lo, hi)
						m.trim(lo, hi)
					}
				case 10: // LPOS parity through the command scanner
					if m.len() > 0 {
						target := clone(m.e[rng.IntN(m.len())])
						rank := rng.IntN(3) - 1
						if rank == 0 {
							rank = 1
						}
						cnt := rng.IntN(3)
						gotHits := lposScan(l, target, rank, cnt, 0)
						wantHits := modelLpos(m, target, rank, cnt)
						if len(gotHits) != len(wantHits) {
							t.Fatalf("step %d lpos len %d, model %d", step, len(gotHits), len(wantHits))
						}
						for i := range gotHits {
							if gotHits[i] != wantHits[i] {
								t.Fatalf("step %d lpos[%d] %d, model %d", step, i, gotHits[i], wantHits[i])
							}
						}
					}
				}
				checkAll("step " + strconv.Itoa(step))
			}
			if !everQuick {
				t.Fatalf("seed %d never promoted to the native deque", seed)
			}
		})
	}
}

// modelLpos is the independent LPOS reference: positions of target under the
// RANK direction and COUNT cap, no MAXLEN.
func modelLpos(m *listModel, target []byte, rank, limit int) []int {
	forward := rank > 0
	skip := rank
	if skip < 0 {
		skip = -skip
	}
	skip--
	var out []int
	step := func(i int) bool {
		if !bytes.Equal(m.e[i], target) {
			return true
		}
		if skip > 0 {
			skip--
			return true
		}
		out = append(out, i)
		return limit <= 0 || len(out) < limit
	}
	if forward {
		for i := 0; i < m.len(); i++ {
			if !step(i) {
				break
			}
		}
	} else {
		for i := m.len() - 1; i >= 0; i-- {
			if !step(i) {
				break
			}
		}
	}
	return out
}
