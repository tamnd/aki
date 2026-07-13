package structs

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// nilMembers is the callback for distinct-score tests: the comparisons short-
// circuit on the score, so the member bytes are never read and a nil is safe. It
// keeps the width and overflow tests off the heap at million-entry scale.
type nilMembers struct{}

func (nilMembers) Member(uint32) []byte { return nil }

// memStore is a member arena for the tie-break tests: it maps a member's bytes to
// a stable reference and back, the shape the zset seam's member hash provides.
type memStore struct {
	bytesByRef [][]byte
	refByKey   map[string]uint32
}

func newMemStore() *memStore { return &memStore{refByKey: map[string]uint32{}} }

func (m *memStore) Member(ref uint32) []byte { return m.bytesByRef[ref] }

func (m *memStore) ref(member string) uint32 {
	if r, ok := m.refByKey[member]; ok {
		return r
	}
	r := uint32(len(m.bytesByRef))
	m.bytesByRef = append(m.bytesByRef, []byte(member))
	m.refByKey[member] = r
	return r
}

// --- Frozen geometry: the lab-settled constants and the arity formula ---

func TestFrozenGeometry(t *testing.T) {
	if BranchSize != 256 || LeafSize != 512 || CountWidth != 4 {
		t.Fatalf("frozen geometry moved: branch %d leaf %d width %d", BranchSize, LeafSize, CountWidth)
	}
	tr := NewTree()
	if tr.Arity() != 16 {
		t.Fatalf("frozen arity %d, want 16", tr.Arity())
	}
	if tr.LeafCap() != 31 {
		t.Fatalf("frozen leaf capacity %d, want 31", tr.LeafCap())
	}
	if LeafCapFor(512) != 31 {
		t.Fatalf("LeafCapFor(512)=%d, want 31", LeafCapFor(512))
	}
}

// TestArityByWidth pins the interior arity as a pure function of the frozen branch
// size and the count width, lab 02's disqualifier context: a child costs an
// 8-byte separator, a 4-byte ordinal and a count of w bytes, so a wider count
// seats fewer children. 256-byte branch is 18 at u16, 16 at u32, 12 at u64.
func TestArityByWidth(t *testing.T) {
	cases := []struct {
		countW int
		arity  int
	}{{2, 18}, {4, 16}, {8, 12}}
	for _, c := range cases {
		if got := ArityFor(BranchSize, c.countW); got != c.arity {
			t.Fatalf("ArityFor(256,%d)=%d, want %d", c.countW, got, c.arity)
		}
		tr := newTreeSized(BranchSize, LeafSize, c.countW)
		if tr.Arity() != c.arity {
			t.Fatalf("countW %d: tree arity %d, want %d", c.countW, tr.Arity(), c.arity)
		}
	}
}

// TestWidthCeilings asserts on the count accessors directly: a count at the
// width's ceiling round-trips, and a count one past it truncates. The truncation
// is the silent corruption the u32 choice guards against.
func TestWidthCeilings(t *testing.T) {
	cases := []struct {
		countW int
		ceil   uint64
	}{{2, 1<<16 - 1}, {4, 1<<32 - 1}}
	for _, c := range cases {
		tr := newTreeSized(BranchSize, LeafSize, c.countW)
		o := tr.allocBranch()
		tr.bSetCount(o, 0, c.ceil)
		if got := tr.bCount(o, 0); got != c.ceil {
			t.Fatalf("countW %d: ceiling %d round-tripped as %d", c.countW, c.ceil, got)
		}
		tr.bSetCount(o, 0, c.ceil+1)
		if got := tr.bCount(o, 0); got == c.ceil+1 {
			t.Fatalf("countW %d: value past ceiling stored intact, want truncation", c.countW)
		}
	}
	// u32's ceiling near 4.29e9 covers the collection cap with headroom.
	tr := newTreeSized(BranchSize, LeafSize, 4)
	o := tr.allocBranch()
	const near = uint64(4_290_000_000)
	tr.bSetCount(o, 0, near)
	if got := tr.bCount(o, 0); got != near {
		t.Fatalf("u32: %d round-tripped as %d", near, got)
	}
}

// TestOverflowAtScale is the width disqualifier: a u16 tree large enough that a
// root-child subtree passes 65535 stores truncated counts, so the count audit
// reports the inconsistency, while a u32 tree of the same keys stays exact and
// ranks and selects correctly.
func TestOverflowAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("million-entry overflow sweep")
	}
	const n = 1_000_000
	scores := distinctScores(n, 0x51ab)

	u16 := buildDistinct(2, scores)
	maxCount, consistent := u16.auditCounts()
	if maxCount <= 1<<16-1 {
		t.Fatalf("test too small to overflow u16: maxCount %d", maxCount)
	}
	if consistent {
		t.Fatal("u16 counts consistent past the ceiling, truncation not detected")
	}

	u32 := buildDistinct(4, scores)
	maxCount, consistent = u32.auditCounts()
	if !consistent {
		t.Fatalf("u32 counts inconsistent at %d entries (maxCount %d)", n, maxCount)
	}
	if maxCount > 1<<32-1 {
		t.Fatalf("u32 maxCount %d over its ceiling", maxCount)
	}
	sorted := append([]uint64(nil), scores...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for _, i := range []int{0, 1, n / 3, n / 2, n - 1} {
		got, present := u32.Rank(sorted[i], nil, nilMembers{})
		if !present || got != uint64(i) {
			t.Fatalf("u32 rank(sorted[%d])=%d present=%v, want %d", i, got, present, i)
		}
		s, _, ok := u32.SelectAt(uint64(i))
		if !ok || s != sorted[i] {
			t.Fatalf("u32 select(%d)=%d ok=%v, want %d", i, s, ok, sorted[i])
		}
	}
}

// TestRankSelectPerWidth checks that below any width's ceiling, rank and select
// agree with a sorted-slice model for every width, so the width changes only the
// layout that stores the counts, never the order-statistic answers.
func TestRankSelectPerWidth(t *testing.T) {
	for _, w := range []int{2, 4, 8} {
		tr := newTreeSized(BranchSize, LeafSize, w)
		seen := map[uint64]struct{}{}
		rng := rand.New(rand.NewSource(int64(w) * 1009))
		for i := 0; i < 5000; i++ {
			k := rng.Uint64()
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			tr.Insert(k, nil, 0, nilMembers{})
		}
		if _, consistent := tr.auditCounts(); !consistent {
			t.Fatalf("countW %d: counts inconsistent below any ceiling", w)
		}
		keys := make([]uint64, 0, len(seen))
		for k := range seen {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		for probe := 0; probe < 300; probe++ {
			i := rng.Intn(len(keys))
			got, present := tr.Rank(keys[i], nil, nilMembers{})
			if !present || got != uint64(i) {
				t.Fatalf("countW %d: rank(%d)=%d present=%v, want %d", w, keys[i], got, present, i)
			}
			s, _, ok := tr.SelectAt(uint64(i))
			if !ok || s != keys[i] {
				t.Fatalf("countW %d: select(%d)=%d ok=%v, want %d", w, i, s, ok, keys[i])
			}
		}
	}
}

// TestWalkFromRankRevEdges pins the reverse walk's boundaries: an empty tree
// yields nothing, a start past the end clamps to the last entry, and a full
// descending scan visits every rank exactly once across many leaf boundaries.
func TestWalkFromRankRevEdges(t *testing.T) {
	empty := NewTree()
	empty.WalkFromRankRev(0, func(uint64, uint32) bool {
		t.Fatal("empty tree yielded an entry")
		return true
	})
	empty.WalkFromRankRev(100, func(uint64, uint32) bool {
		t.Fatal("empty tree yielded an entry for an out-of-range start")
		return true
	})

	const n = 4000
	tr := NewTree()
	for i := 0; i < n; i++ {
		tr.Insert(uint64(i)<<8, nil, uint32(i), nilMembers{})
	}
	// A start past the end clamps to rank n-1, so the full descending scan begins
	// at the top and lands exactly n entries.
	want := n - 1
	tr.WalkFromRankRev(uint64(n+50), func(sc uint64, _ uint32) bool {
		if sc != uint64(want)<<8 {
			t.Fatalf("reverse scan at %d saw score %d, want %d", want, sc, uint64(want)<<8)
		}
		want--
		return true
	})
	if want != -1 {
		t.Fatalf("reverse scan stopped with %d entries unseen", want+1)
	}
	// Early stop: a reverse walk that returns false after one entry visits only it.
	seen := 0
	tr.WalkFromRankRev(10, func(sc uint64, _ uint32) bool {
		if sc != uint64(10)<<8 {
			t.Fatalf("reverse walk from rank 10 started at score %d", sc)
		}
		seen++
		return false
	})
	if seen != 1 {
		t.Fatalf("early-stop reverse walk visited %d, want 1", seen)
	}
}

// --- Bulk load and the bytes-per-entry bar ---

// TestBulkLoadBytesPerEntry is the F14 memory bar (test e): a right-edge 0.9-fill
// bulk load produces the budgeted overhead, ~3.0B/entry at the frozen 512-byte
// leaf per lab 01, well inside the 5B block line, and the built tree passes every
// structural invariant.
func TestBulkLoadBytesPerEntry(t *testing.T) {
	for _, n := range []int{10_000, 100_000} {
		scores := distinctScores(n, uint64(n))
		sort.Slice(scores, func(i, j int) bool { return scores[i] < scores[j] })
		ents := make([]Entry, n)
		for i, s := range scores {
			ents[i] = Entry{Score: s, Ref: uint32(i)}
		}
		tr := BulkLoad(ents)
		if err := tr.Check(nilMembers{}); err != nil {
			t.Fatalf("n=%d bulk-built tree invalid: %v", n, err)
		}
		if tr.Len() != n {
			t.Fatalf("n=%d bulk Len %d", n, tr.Len())
		}
		bpe := float64(tr.Bytes())/float64(n) - 16
		// Lab 01 froze the score-only 256b/512l arm at 3.0B bulk; the exact tie-break
		// separator refs this tree adds cost ~0.15B/entry more, so the total sits near
		// 3.15B, inside the 2-3B target's rounding and far under the 5B block line the
		// milestone blocks at.
		if bpe > 3.5 {
			t.Fatalf("n=%d bulk bytes/entry %.2f over budget", n, bpe)
		}
		t.Logf("n=%d bulk bytes/entry %.2f (leaf 512B, arity %d)", n, bpe, tr.Arity())
		// Order-statistic answers match the model after a bulk build.
		for _, i := range []int{0, n / 2, n - 1} {
			got, present := tr.Rank(scores[i], nil, nilMembers{})
			if !present || got != uint64(i) {
				t.Fatalf("n=%d bulk rank(%d)=%d, want %d", n, scores[i], got, i)
			}
			s, _, _ := tr.SelectAt(uint64(i))
			if s != scores[i] {
				t.Fatalf("n=%d bulk select(%d)=%d, want %d", n, i, s, scores[i])
			}
		}
	}
}

// --- Tied bands: the lex path and the separator spill ---

// TestTiedBand forces one score across every member (the autocomplete/prefix
// shape, doc section 3.2) so the whole tree is one tied band spanning many nodes,
// then checks rank, select, walk and the invariants hold, which exercises the
// separator member tie-break routing.
func TestTiedBand(t *testing.T) {
	ms := newMemStore()
	tr := NewTree()
	const n = 4000
	members := make([]string, n)
	for i := range members {
		members[i] = fmt.Sprintf("member:%08d", i) // sorted by index
	}
	// Insert in shuffled order at one fixed score.
	order := rand.New(rand.NewSource(7)).Perm(n)
	for _, i := range order {
		tr.Insert(0, []byte(members[i]), ms.ref(members[i]), ms)
	}
	if err := tr.Check(ms); err != nil {
		t.Fatalf("tied-band tree invalid: %v", err)
	}
	if tr.Len() != n {
		t.Fatalf("tied-band Len %d, want %d", tr.Len(), n)
	}
	// members[i] sorts to rank i because the format zero-pads.
	for _, i := range []int{0, 1, n / 2, n - 2, n - 1} {
		got, present := tr.Rank(0, []byte(members[i]), ms)
		if !present || got != uint64(i) {
			t.Fatalf("tied rank(%q)=%d present=%v, want %d", members[i], got, present, i)
		}
		_, ref, ok := tr.SelectAt(uint64(i))
		if !ok || string(ms.Member(ref)) != members[i] {
			t.Fatalf("tied select(%d)=%q, want %q", i, ms.Member(ref), members[i])
		}
	}
	// A lex walk from a bound emits the contiguous suffix in member order.
	var walked []string
	tr.WalkFrom(0, []byte(members[100]), ms, func(_ uint64, ref uint32) bool {
		walked = append(walked, string(ms.Member(ref)))
		return len(walked) < 50
	})
	for j, mstr := range walked {
		if mstr != members[100+j] {
			t.Fatalf("lex walk[%d]=%q, want %q", j, mstr, members[100+j])
		}
	}
}

// --- The randomized grow/churn/drain property test (test d) ---

type key struct {
	score  uint64
	member string
}

func lessKey(a, b key) bool {
	if a.score != b.score {
		return a.score < b.score
	}
	return a.member < b.member
}

// TestPropertyGrowChurn drives insert and delete over tied and distinct scores
// with a fixed seed, running the invariant checker after every batch and matching
// rank, select and the range walk against a sorted-slice model across a grow, a
// churn and a full drain to empty. This is the fuzz exit lab 01 names.
func TestPropertyGrowChurn(t *testing.T) {
	rng := rand.New(rand.NewSource(0x2064f3))
	ms := newMemStore()
	tr := NewTree()
	present := map[key]struct{}{}

	randKey := func() key {
		// Small score range forces tied bands; short members force tie-break work.
		return key{score: uint64(rng.Intn(6)), member: fmt.Sprintf("m%d", rng.Intn(2000))}
	}

	apply := func(insertBias int) {
		for i := 0; i < 300; i++ {
			k := randKey()
			_, has := present[k]
			if rng.Intn(100) < insertBias || !has {
				// insert (or a delete miss folded into an insert when absent)
				added := tr.Insert(k.score, []byte(k.member), ms.ref(k.member), ms)
				if added == has {
					t.Fatalf("Insert added=%v but present=%v for %v", added, has, k)
				}
				present[k] = struct{}{}
			} else {
				_, removed := tr.Delete(k.score, []byte(k.member), ms)
				if !removed {
					t.Fatalf("Delete missed a present key %v", k)
				}
				delete(present, k)
			}
		}
	}

	verify := func(phase string) {
		if err := tr.Check(ms); err != nil {
			t.Fatalf("%s: invariant broken: %v", phase, err)
		}
		model := make([]key, 0, len(present))
		for k := range present {
			model = append(model, k)
		}
		sort.Slice(model, func(i, j int) bool { return lessKey(model[i], model[j]) })
		if tr.Len() != len(model) {
			t.Fatalf("%s: Len %d, model %d", phase, tr.Len(), len(model))
		}
		// Each yields the model order.
		idx := 0
		tr.Each(func(_ uint64, ref uint32) bool {
			if string(ms.Member(ref)) != model[idx].member {
				t.Fatalf("%s: Each[%d] member %q, want %q", phase, idx, ms.Member(ref), model[idx].member)
			}
			idx++
			return true
		})
		if idx != len(model) {
			t.Fatalf("%s: Each visited %d, want %d", phase, idx, len(model))
		}
		// Rank and select agree with the model on a sample.
		for s := 0; s < 40 && len(model) > 0; s++ {
			i := rng.Intn(len(model))
			got, ok := tr.Rank(model[i].score, []byte(model[i].member), ms)
			if !ok || got != uint64(i) {
				t.Fatalf("%s: Rank(%v)=%d ok=%v, want %d", phase, model[i], got, ok, i)
			}
			sc, ref, sok := tr.SelectAt(uint64(i))
			if !sok || sc != model[i].score || string(ms.Member(ref)) != model[i].member {
				t.Fatalf("%s: Select(%d)=(%d,%q), want %v", phase, i, sc, ms.Member(ref), model[i])
			}
		}
		// A range walk from a random rank emits the exact model window.
		if len(model) > 0 {
			start := rng.Intn(len(model))
			want := model[start:]
			if len(want) > 25 {
				want = want[:25]
			}
			j := 0
			tr.WalkFromRank(uint64(start), func(sc uint64, ref uint32) bool {
				if sc != want[j].score || string(ms.Member(ref)) != want[j].member {
					t.Fatalf("%s: WalkFromRank[%d]=(%d,%q), want %v", phase, j, sc, ms.Member(ref), want[j])
				}
				j++
				return j < len(want)
			})
			if j != len(want) {
				t.Fatalf("%s: walk emitted %d, want %d", phase, j, len(want))
			}
			// A reverse walk from a random rank emits the model window in
			// descending order, re-seeking across each leaf boundary it crosses.
			rstart := rng.Intn(len(model))
			lo := rstart - 25
			if lo < 0 {
				lo = 0
			}
			wantRev := model[lo : rstart+1]
			k := len(wantRev) - 1
			tr.WalkFromRankRev(uint64(rstart), func(sc uint64, ref uint32) bool {
				if sc != wantRev[k].score || string(ms.Member(ref)) != wantRev[k].member {
					t.Fatalf("%s: WalkFromRankRev[%d]=(%d,%q), want %v", phase, k, sc, ms.Member(ref), wantRev[k])
				}
				k--
				return k >= 0
			})
			if k != -1 {
				t.Fatalf("%s: reverse walk emitted %d short of the window", phase, k+1)
			}
		}
	}

	// Grow.
	for b := 0; b < 8; b++ {
		apply(90)
		verify(fmt.Sprintf("grow batch %d", b))
	}
	// Churn: balanced insert and delete.
	for b := 0; b < 12; b++ {
		apply(50)
		verify(fmt.Sprintf("churn batch %d", b))
	}
	// Drain to empty, one key at a time, checking the tree stays valid and
	// height honestly collapses.
	for len(present) > 0 {
		var k key
		for kk := range present {
			k = kk
			break
		}
		if _, removed := tr.Delete(k.score, []byte(k.member), ms); !removed {
			t.Fatalf("drain: Delete missed %v", k)
		}
		delete(present, k)
		if len(present)%137 == 0 {
			if err := tr.Check(ms); err != nil {
				t.Fatalf("drain at %d: invariant broken: %v", len(present), err)
			}
		}
	}
	if err := tr.Check(ms); err != nil {
		t.Fatalf("drained-empty tree invalid: %v", err)
	}
	if tr.Len() != 0 {
		t.Fatalf("drained tree Len %d, want 0", tr.Len())
	}
	if tr.height != 1 {
		t.Fatalf("drained tree height %d, want 1", tr.height)
	}
}

// TestAppendRightEdge checks the ordered append fast path builds a valid tree that
// matches the model, the band-promotion path without a full sort search.
func TestAppendRightEdge(t *testing.T) {
	tr := NewTree()
	const n = 3000
	scores := distinctScores(n, 99)
	sort.Slice(scores, func(i, j int) bool { return scores[i] < scores[j] })
	for i, s := range scores {
		tr.Append(s, uint32(i))
	}
	if err := tr.Check(nilMembers{}); err != nil {
		t.Fatalf("appended tree invalid: %v", err)
	}
	if tr.Len() != n {
		t.Fatalf("appended Len %d, want %d", tr.Len(), n)
	}
	for _, i := range []int{0, n / 2, n - 1} {
		got, present := tr.Rank(scores[i], nil, nilMembers{})
		if !present || got != uint64(i) {
			t.Fatalf("append rank(%d)=%d, want %d", scores[i], got, i)
		}
	}
}

// TestGeoGroundwork proves the tree carries geohash-encoded scores cleanly (the
// M6 groundwork, exit gate): a 52-bit interleaved geohash is an ordered u64, and
// feeding a range of them keeps the tree in geohash order, so a GEOSEARCH over a
// geohash band is a plain tree range. The order key is opaque here; the float
// score codec that maps a double geohash to this form is the dual-write slice's.
func TestGeoGroundwork(t *testing.T) {
	tr := NewTree()
	// Fabricate ordered 52-bit geohash cells, the width Redis packs into a score.
	const n = 2000
	geo := make([]uint64, n)
	rng := rand.New(rand.NewSource(0x6e0))
	seen := map[uint64]struct{}{}
	for i := range geo {
		var g uint64
		for {
			g = rng.Uint64() & ((1 << 52) - 1) // 52-bit geohash cell
			if _, ok := seen[g]; !ok {
				seen[g] = struct{}{}
				break
			}
		}
		geo[i] = g
	}
	for i, g := range geo {
		tr.Insert(g, nil, uint32(i), nilMembers{})
	}
	if err := tr.Check(nilMembers{}); err != nil {
		t.Fatalf("geo tree invalid: %v", err)
	}
	sorted := append([]uint64(nil), geo...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	// Select walks the cells in geohash order, which is the area order GEO rides.
	prev := uint64(0)
	tr.Each(func(g uint64, _ uint32) bool {
		if g < prev {
			t.Fatalf("geo order broken: %d after %d", g, prev)
		}
		prev = g
		return true
	})
	// A band [lo, hi) is a rank-arithmetic slice, the GEOSEARCH cell decomposition.
	lo, hi := sorted[500], sorted[1500]
	loRank, _ := tr.Rank(lo, nil, nilMembers{})
	hiRank, _ := tr.Rank(hi, nil, nilMembers{})
	if hiRank-loRank != 1000 {
		t.Fatalf("geo band count %d, want 1000", hiRank-loRank)
	}
}

// --- helpers ---

func distinctScores(n int, seed uint64) []uint64 {
	out := make([]uint64, n)
	x := seed ^ 0x9e3779b97f4a7c15
	seen := make(map[uint64]struct{}, n)
	for i := range out {
		var k uint64
		for {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			k = splitmix(x)
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				break
			}
		}
		out[i] = k
	}
	return out
}

func splitmix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func buildDistinct(countW int, scores []uint64) *Tree {
	tr := newTreeSized(BranchSize, LeafSize, countW)
	for i, s := range scores {
		tr.Insert(s, nil, uint32(i), nilMembers{})
	}
	return tr
}

// TestFindPointLookup pins the point lookup XCLAIM and XNACK ride: a hit returns
// the exact reference stored at the key, and same-score member ties resolve to the
// right reference through the Members callback (scores are taken mod 8 so many
// distinct members share a score). A miss, whether an absent member within a
// present score or an absent score, returns ok=false. It fills far past a single
// leaf so the descent crosses interior nodes, the path a range seek takes. Each
// member is unique and used as both the tie-break key and the ref payload, so the
// bytes Find compares match what Member returns, the invariant the PEL upholds.
func TestFindPointLookup(t *testing.T) {
	ms := newMemStore()
	tr := NewTree()
	model := map[key]uint32{}
	for i := 0; i < 2000; i++ {
		k := key{score: uint64(i % 8), member: fmt.Sprintf("m%d", i)}
		ref := ms.ref(k.member)
		tr.Insert(k.score, []byte(k.member), ref, ms)
		model[k] = ref
	}
	for k, want := range model {
		got, ok := tr.Find(k.score, []byte(k.member), ms)
		if !ok || got != want {
			t.Fatalf("Find(%d,%q)=(%d,%v), want (%d,true)", k.score, k.member, got, ok, want)
		}
	}
	// A member absent within a present score misses.
	if _, ok := tr.Find(3, []byte("nope"), ms); ok {
		t.Fatal("Find of an absent member within a present score reported present")
	}
	// A score no entry carries misses.
	if _, ok := tr.Find(999, []byte("m0"), ms); ok {
		t.Fatal("Find of an absent score reported present")
	}
	// The empty tree misses.
	if _, ok := NewTree().Find(1, []byte("m0"), ms); ok {
		t.Fatal("Find on an empty tree reported present")
	}
}
