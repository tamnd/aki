package list

import (
	"bytes"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/tamnd/aki/f3srv/resp"
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

// forceFlatLocate resolves k by the flat linear scan regardless of ring size,
// the exact scan locate runs at or below flatMax. Tests and benchmarks use it to
// exercise the flat path above the crossover, where locate itself would pick the
// Fenwick descent.
func forceFlatLocate(nt *native, k int) (ci, ord int) {
	for i := 0; i < nt.ring.n; i++ {
		n := nt.ring.at(i).count()
		if k < n {
			return i, k
		}
		k -= n
	}
	return nt.ring.n - 1, k
}

// buildNativeVals pushes vals onto a fresh deque through the tail path, the same
// promotion order toNative uses, so the ring geometry matches a real list.
func buildNativeVals(vals [][]byte) *native {
	nt := &native{}
	for _, v := range vals {
		nt.pushBack(v)
	}
	return nt
}

// checkLocateAgree asserts that for every dense index k the flat scan and the
// Fenwick descent return the identical (chunk, ordinal) pair. It forces a fresh
// Fenwick build so the descent reads the current chunk geometry, mirroring lab
// 02's flat-versus-Fenwick equivalence assertion. This is the pre-registered
// correctness bar for the crossover: the switch at flatMax may never change an
// answer.
func checkLocateAgree(t *testing.T, nt *native, label string) {
	t.Helper()
	nt.dir.stale = true
	nt.dir.sync(&nt.ring)
	// Walk the chunks in order so the flat expectation is one pass, then compare
	// the Fenwick descent at every k against it.
	k := 0
	for ci := 0; ci < nt.ring.n; ci++ {
		n := nt.ring.at(ci).count()
		for ord := 0; ord < n; ord++ {
			bc, bo := nt.dir.rank(k)
			if bc != ci || bo != ord {
				t.Fatalf("%s: k=%d flat=(%d,%d) fenwick=(%d,%d) chunks=%d",
					label, k, ci, ord, bc, bo, nt.ring.n)
			}
			k++
		}
	}
}

// TestLocateFlatMatchesFenwick is the equivalence bar for the chunk directory
// (section 2.4, invariant 2). Across many ring geometries, including well past
// the 128-chunk crossover and past 512 chunks, and after sequences of mixed
// head/tail pushes, pops, inserts, removes, trims, and length-changing sets, the
// flat scan and the Fenwick descent must resolve every k identically. The churn
// drives the ring through chunk appends, edge removals at both ends, head-side
// renumbering, and interior rebuilds, so the directory is proven through
// structural change, not just a static build.
func TestLocateFlatMatchesFenwick(t *testing.T) {
	// Static geometries: chunk fills from a few elements to near-full, at chunk
	// counts straddling and far past the crossover. elemSize picks the fill: ~1200
	// bytes gives three to four per chunk, 64 bytes about sixty, 4 bytes the max.
	static := []struct {
		chunks   int
		elemSize int
	}{
		{4, 64}, {17, 1200}, {130, 4}, {130, 1200}, {200, 64}, {512, 4}, {600, 64},
	}
	for _, cfg := range static {
		// Enough elements to reach the target chunk count at this fill.
		perChunk := chunkElemCap
		if cfg.elemSize >= 1024 {
			perChunk = 3
		} else if cfg.elemSize >= 64 {
			perChunk = 60
		}
		vals := make([][]byte, cfg.chunks*perChunk)
		for i := range vals {
			vals[i] = sized(cfg.elemSize, i)
		}
		nt := buildNativeVals(vals)
		checkLocateAgree(t, nt, "static")
	}

	// Churn: start past the crossover, then drive a long mixed-op stream and
	// re-check the equivalence at every step, so the Fenwick is exercised through
	// appends, both-end pops, and interior rebuilds rather than one static build.
	rng := rand.New(rand.NewPCG(0x5eed, 0x1337))
	vals := make([][]byte, 150*60) // ~150 chunks of 64-byte values, past the crossover
	for i := range vals {
		vals[i] = sized(64, i)
	}
	nt := buildNativeVals(vals)
	for step := 0; step < 200; step++ {
		switch rng.IntN(8) {
		case 0:
			nt.pushBack(sized(64, step))
		case 1:
			nt.pushFront(sized(64, step))
		case 2:
			if nt.count > 0 {
				nt.popFront()
			}
		case 3:
			if nt.count > 0 {
				nt.popBack()
			}
		case 4:
			if nt.count > 0 {
				nt.setAt(rng.IntN(nt.count), sized(1200, step)) // length-changing set, rebuild
			}
		case 5:
			if nt.count > 0 {
				pivot := nt.at(rng.IntN(nt.count))
				nt.insert(rng.IntN(2) == 0, cloneBytes(pivot), sized(64, step))
			}
		case 6:
			if nt.count > 0 {
				nt.remove(rng.IntN(5)-2, nt.at(rng.IntN(nt.count)))
			}
		case 7:
			if nt.count > 4 {
				a, b := rng.IntN(nt.count), rng.IntN(nt.count)
				if a > b {
					a, b = b, a
				}
				nt.trim(a, b)
			}
		}
		if nt.count > 0 {
			checkLocateAgree(t, nt, "churn step "+strconv.Itoa(step))
		}
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

// TestLrangeCursorMatchesPerElement locks the LRANGE cursor walk (appendRange)
// to the per-element get path it replaced, across the Fenwick locate threshold
// (>flatMax*chunkElemCap elements), so the seek-once-then-advance walk never
// drifts from repeated index resolution. The RESP encodings must be byte-equal.
func TestLrangeCursorMatchesPerElement(t *testing.T) {
	l := newList()
	const n = 20000 // forces many chunks, above flatMax so locate rides the Fenwick descent
	for i := 0; i < n; i++ {
		l.pushBack([]byte(strconv.Itoa(i)))
	}
	if l.nat == nil {
		t.Fatal("expected the native band at this length")
	}
	for _, w := range [][2]int{
		{0, 9}, {0, n - 1}, {127, 129}, {128, 260},
		{n / 2, n/2 + 100}, {n - 50, n - 1}, {5000, 5000},
	} {
		lo, hi := w[0], w[1]
		got := l.appendRange(nil, lo, hi)
		var want []byte
		for i := lo; i <= hi; i++ {
			want = resp.AppendBulk(want, l.get(i))
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("window [%d,%d]: cursor walk output != per-element get output", lo, hi)
		}
	}
}

// locateFlat resolves k the way the flat scan does, reading the ring's live
// counts directly and never touching the Fenwick tree, so it is an independent
// oracle for the tree's answer.
func locateFlat(nt *native, k int) (ci, ord int) {
	for i := 0; i < nt.ring.n; i++ {
		n := nt.ring.at(i).count()
		if k < n {
			return i, k
		}
		k -= n
	}
	panic("out of range")
}

// TestLsetNoSplitKeepsDirectoryValid pins the setAt fix: a length-changing LSET
// that does not overflow its chunk leaves every chunk's element count unchanged,
// so the Fenwick directory stays valid and must NOT be marked stale. Above the
// crossover a spurious stale forced the next locate to rebuild the whole
// O(chunks) directory, the deep-LSET regression. The test proves the directory
// is left non-stale and still resolves every index against the flat oracle
// WITHOUT a rebuild, and that a genuine overflow split does grow the ring and is
// picked up.
func TestLsetNoSplitKeepsDirectoryValid(t *testing.T) {
	// Small elements so the 128-element cap fills a chunk long before the 4096-byte
	// cap: each chunk carries chunkElemCap frames of ~9 bytes (~1152B), leaving
	// ample blob headroom for a growth that does not split. ~200 chunks is well
	// past flatMax so locate rides the Fenwick descent.
	const chunks, perChunk, elemSize = 200, chunkElemCap, 8
	vals := make([][]byte, chunks*perChunk)
	for i := range vals {
		vals[i] = sized(elemSize, i)
	}
	nt := buildNativeVals(vals)
	if nt.ring.n <= flatMax {
		t.Fatalf("need a ring past flatMax, got %d chunks", nt.ring.n)
	}
	// Prime the directory to a fresh, non-stale build.
	nt.locate(0)
	if nt.dir.stale {
		t.Fatal("directory unexpectedly stale after a plain locate")
	}
	total := nt.count

	// Grow one element in each of several deep chunks by 16 bytes (frameLen 65 ->
	// 81), each staying under chunkBlobCap so no chunk splits.
	touched := []int{total / 3, total / 2, 2 * total / 3, total - 40}
	for step, idx := range touched {
		grown := sized(elemSize+16, 100000+step)
		ringBefore := nt.ring.n
		nt.setAt(idx, grown)
		if nt.ring.n != ringBefore {
			t.Fatalf("step %d: unexpected split, ring %d -> %d (raise the headroom)",
				step, ringBefore, nt.ring.n)
		}
		if nt.dir.stale {
			t.Fatalf("step %d: directory marked stale after a no-split LSET", step)
		}
		// The still-live (never rebuilt) Fenwick tree must resolve every index
		// exactly as the flat oracle does, and the grown value must read back.
		for k := 0; k < nt.count; k++ {
			gc, go_ := nt.dir.rank(k)
			fc, fo := locateFlat(nt, k)
			if gc != fc || go_ != fo {
				t.Fatalf("step %d k=%d: fenwick=(%d,%d) flat=(%d,%d) without rebuild",
					step, k, gc, go_, fc, fo)
			}
		}
		if got := nt.at(idx); !bytes.Equal(got, grown) {
			t.Fatalf("step %d: at(%d)=%q want %q", step, idx, got, grown)
		}
	}

	// A genuine overflow split (an oversized lone frame) must grow the ring; the
	// next locate then rebuilds off the length mismatch and still agrees.
	ringBefore := nt.ring.n
	nt.setAt(total/2, sized(5000, 7))
	if nt.ring.n <= ringBefore {
		t.Fatalf("oversized LSET did not split: ring %d -> %d", ringBefore, nt.ring.n)
	}
	checkLocateAgree(t, nt, "post-split")
}
