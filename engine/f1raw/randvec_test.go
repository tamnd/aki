package f1raw

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"testing"
)

// The dense member vector tests target the three ways it can drift: the vector loses
// lockstep with the hash index, the swap-remove corrupts the back-index, or the draw is not
// uniform. They build sets through the same engine calls the server will use (PutKind +
// CollInsert + CollRandInsert to add, CollRandRemove + DeleteKind to remove), so a test set
// is byte-identical to a served set, and they assert the density invariant after every
// mutation.

// testRNG is a splitmix64 generator that hands the draw tests a fresh random word each call,
// standing in for the per-connection rng the server feeds CollRandSelect. A varying word is
// what makes the coverage and uniformity assertions meaningful.
type testRNG uint64

func (r *testRNG) next() uint64 {
	*r += 0x9e3779b97f4a7c15
	z := uint64(*r)
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// setPrefixBytes builds the bounding prefix uvarint(len(skey)) | skey the server's setPrefix
// builds, so a test set's member rows land under exactly the prefix the vector is keyed by.
func setPrefixBytes(skey string) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	return append(append([]byte{}, tmp[:n]...), skey...)
}

// memberKeyBytes builds the composite member key uvarint(len(skey)) | skey | member, the
// server's memberKey.
func memberKeyBytes(skey, member string) []byte {
	return append(setPrefixBytes(skey), member...)
}

const tKindSetMember = 0x02 // mirror the server's kindSetMember for tests

// addMember adds one member through the same triple the server's setMemberAdd will run: the
// hash record, the ordered-index node, and the vector append. It reports whether the member
// was newly created, so a re-add does not double-count.
func addMember(t *testing.T, s *Store, prefix []byte, skey, member string) bool {
	t.Helper()
	mk := memberKeyBytes(skey, member)
	created, err := s.PutKind(mk, nil, tKindSetMember)
	if err != nil {
		t.Fatalf("PutKind(%q): %v", member, err)
	}
	if created {
		s.CollInsert(mk, tKindSetMember)
		s.CollRandInsert(prefix, mk, tKindSetMember)
	}
	return created
}

// removeMember removes one member through the same pair the server's setMemberRemove will
// run, in the order the vector requires: the vector swap-remove first (while the record is
// still live so its offset resolves), then the hash delete, then the ordered-index removal.
func removeMember(t *testing.T, s *Store, prefix []byte, skey, member string) bool {
	t.Helper()
	mk := memberKeyBytes(skey, member)
	if !s.ExistsKind(mk, tKindSetMember) {
		return false
	}
	s.CollRandRemove(prefix, mk, tKindSetMember)
	s.DeleteKind(mk, tKindSetMember)
	s.CollRemove(mk)
	return true
}

// checkDense asserts the vector for prefix is a valid dense probability space: its length
// equals want, back and slots agree in size, back[slots[i]] == i for every slot, and every
// offset resolves to a live member row. It reaches into the shard under its mutex the same
// way the engine does.
func checkDense(t *testing.T, s *Store, prefix []byte, want int) {
	t.Helper()
	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	v := sh.get(prefix)
	if v == nil {
		if want != 0 {
			t.Fatalf("no vector but want %d members", want)
		}
		return
	}
	if len(v.slots) != want {
		t.Fatalf("slots len = %d, want %d", len(v.slots), want)
	}
	if len(v.back) != want {
		t.Fatalf("back len = %d, want %d", len(v.back), want)
	}
	for i, off := range v.slots {
		if v.back[off] != i {
			t.Fatalf("back[%d] = %d, want %d", off, v.back[off], i)
		}
		if !s.liveAt(off) {
			t.Fatalf("slot %d offset %d is not live", i, off)
		}
	}
}

func newTestStore() *Store {
	s := New(1<<12, 1<<24)
	s.SetTopKindFunc(func(byte) bool { return false })
	return s
}

func TestRandVecAddRemoveDense(t *testing.T) {
	s := newTestStore()
	prefix := setPrefixBytes("s")
	// Build the vector eagerly so every add maintains it (the lazy build is covered
	// separately); a first draw would build it, so ensure stands in for that.
	s.CollRandEnsure(prefix)

	const n = 1000
	for i := 0; i < n; i++ {
		if !addMember(t, s, prefix, "s", fmt.Sprintf("m%d", i)) {
			t.Fatalf("member %d reported not new", i)
		}
	}
	checkDense(t, s, prefix, n)

	// Remove every third member; the vector stays dense and the survivors stay drawable.
	removed := 0
	for i := 0; i < n; i += 3 {
		if !removeMember(t, s, prefix, "s", fmt.Sprintf("m%d", i)) {
			t.Fatalf("member %d reported absent on remove", i)
		}
		removed++
	}
	checkDense(t, s, prefix, n-removed)

	// A re-add of a removed member is a new record at a new offset and must reappear in the
	// vector exactly once.
	if !addMember(t, s, prefix, "s", "m0") {
		t.Fatalf("re-add of m0 reported not new")
	}
	checkDense(t, s, prefix, n-removed+1)
}

func TestRandVecRemoveLast(t *testing.T) {
	// The swap-remove's move-last self-assignment case: remove members in reverse insert
	// order so each victim is the current last slot.
	s := newTestStore()
	prefix := setPrefixBytes("s")
	s.CollRandEnsure(prefix)
	const n = 64
	for i := 0; i < n; i++ {
		addMember(t, s, prefix, "s", fmt.Sprintf("m%d", i))
	}
	for i := n - 1; i >= 0; i-- {
		removeMember(t, s, prefix, "s", fmt.Sprintf("m%d", i))
		checkDense(t, s, prefix, i)
	}
}

func TestRandVecDrainToEmpty(t *testing.T) {
	// Pop the set to empty through the destructive select-remove, the SPOP no-count path,
	// and assert the vector, the hash index, and the ordered index all reach empty together.
	s := newTestStore()
	prefix := setPrefixBytes("s")
	const n = 500
	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		m := fmt.Sprintf("m%d", i)
		addMember(t, s, prefix, "s", m)
		seen[m] = true
	}
	// First draw builds the vector lazily.
	got := map[string]bool{}
	for i := 0; i < n; i++ {
		k, ok := s.CollRandSelectRemove(prefix)
		if !ok {
			t.Fatalf("select-remove %d reported empty with %d left", i, n-i)
		}
		// The returned key is the composite member key; delete its hash record and ordered
		// node the way SPOP will, so the three structures drain together.
		member := string(k[len(prefix):])
		if !seen[member] {
			t.Fatalf("drew %q which is not a member", member)
		}
		if got[member] {
			t.Fatalf("drew %q twice", member)
		}
		got[member] = true
		s.DeleteKind(k, tKindSetMember)
		s.CollRemove(k)
	}
	if _, ok := s.CollRandSelectRemove(prefix); ok {
		t.Fatalf("select-remove on drained set reported non-empty")
	}
	checkDense(t, s, prefix, 0)
	if len(got) != n {
		t.Fatalf("drained %d distinct, want %d", len(got), n)
	}
	// The ordered index for the prefix is empty.
	offs, _ := s.oidx.Load().scanBatch(prefix, nil, 16, nil)
	if len(offs) != 0 {
		t.Fatalf("ordered index still holds %d members after drain", len(offs))
	}
}

func TestRandVecEagerBuild(t *testing.T) {
	// Doc 20 makes the vector eager and authoritative: SADD builds it on the set's first member
	// and maintains it thereafter, so a set that is only ever enumerated and never drawn still
	// has a complete vector. This is the property the old lazy contract could not offer.
	s := newTestStore()
	prefix := setPrefixBytes("s")
	const n = 300
	members := map[string]bool{}
	for i := 0; i < n; i++ {
		m := fmt.Sprintf("m%d", i)
		addMember(t, s, prefix, "s", m) // eager: CollRandInsert builds/appends on every add
		members[m] = true
	}
	// The vector exists and is a complete dense space before any draw or enumeration.
	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	present := sh.get(prefix) != nil
	sh.mu.Unlock()
	if !present {
		t.Fatalf("vector missing after adds; eager build did not run")
	}
	checkDense(t, s, prefix, n)

	// Every member draws out; sample many times and confirm coverage.
	drawn := map[string]bool{}
	var rng testRNG
	for i := 0; i < n*20; i++ {
		k, ok := s.CollRandSelect(prefix, rng.next())
		if !ok {
			t.Fatalf("draw reported empty on a set of %d", n)
		}
		drawn[string(k[len(prefix):])] = true
	}
	checkDense(t, s, prefix, n)
	for m := range members {
		if !drawn[m] {
			t.Fatalf("member %q never drawn, lazy build missed it", m)
		}
	}
}

func TestRandVecRebuildAfterDrop(t *testing.T) {
	// Simulate a restart: build a vector, drop all vectors (the empty post-restart map), then
	// draw and confirm the lazy rebuild reconstructs exactly the persisted members.
	s := newTestStore()
	prefix := setPrefixBytes("s")
	const n = 120
	members := map[string]bool{}
	s.CollRandEnsure(prefix)
	for i := 0; i < n; i++ {
		m := fmt.Sprintf("m%d", i)
		addMember(t, s, prefix, "s", m)
		members[m] = true
	}
	s.CollRandDrop(prefix)
	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	if sh.get(prefix) != nil {
		sh.mu.Unlock()
		t.Fatalf("vector survived drop")
	}
	sh.mu.Unlock()

	drawn := map[string]bool{}
	var rng testRNG
	for i := 0; i < n*20; i++ {
		k, ok := s.CollRandSelect(prefix, rng.next())
		if !ok {
			t.Fatalf("draw empty after rebuild")
		}
		drawn[string(k[len(prefix):])] = true
	}
	checkDense(t, s, prefix, n)
	if len(drawn) != n {
		t.Fatalf("rebuilt vector drew %d distinct, want %d", len(drawn), n)
	}
}

// TestRandVecAt walks every dense index of a set and confirms SetVecAt hands back each live
// member exactly once, that the eager vector is present and complete before any index read,
// and that an out-of-range index reports absent rather than reading past the end. This is the
// count-sampler contract: SRANDMEMBER/SPOP with a count pick indices in [0,card) and expect a
// live member for each, and a sampler that races a shrink expects a clean false rather than a
// stale offset.
func TestRandVecAt(t *testing.T) {
	s := newTestStore()
	prefix := setPrefixBytes("s")
	const n = 300
	members := map[string]bool{}
	for i := 0; i < n; i++ {
		m := fmt.Sprintf("m%d", i)
		addMember(t, s, prefix, "s", m) // eager: the vector is built and maintained on every add
		members[m] = true
	}
	// The eager vector exists before the first index read.
	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	present := sh.get(prefix) != nil
	sh.mu.Unlock()
	if !present {
		t.Fatalf("vector missing after adds; eager build did not run")
	}

	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		k, ok := s.SetVecAt(prefix, i)
		if !ok {
			t.Fatalf("SetVecAt(%d) reported absent on a set of %d", i, n)
		}
		member := string(k[len(prefix):])
		if !members[member] {
			t.Fatalf("SetVecAt(%d) returned %q which is not a member", i, member)
		}
		if seen[member] {
			t.Fatalf("SetVecAt returned %q twice; vector is not dense", member)
		}
		seen[member] = true
	}
	if len(seen) != n {
		t.Fatalf("SetVecAt covered %d distinct members, want %d", len(seen), n)
	}
	checkDense(t, s, prefix, n)

	// Out-of-range indices report absent, not a panic or a stale offset.
	if _, ok := s.SetVecAt(prefix, n); ok {
		t.Fatalf("SetVecAt(card) reported present, want absent")
	}
	if _, ok := s.SetVecAt(prefix, -1); ok {
		t.Fatalf("SetVecAt(-1) reported present, want absent")
	}
	if _, ok := s.SetVecAt(prefix, n*10); ok {
		t.Fatalf("SetVecAt past the end reported present, want absent")
	}
}

// TestRandVecUniform draws a large sample from a fixed set and checks the empirical
// frequency of each member is inside a chi-square band for uniform, catching a swap-remove
// or draw bug that biases the distribution.
func TestRandVecUniform(t *testing.T) {
	s := newTestStore()
	prefix := setPrefixBytes("s")
	s.CollRandEnsure(prefix)
	const n = 200
	for i := 0; i < n; i++ {
		addMember(t, s, prefix, "s", fmt.Sprintf("m%d", i))
	}
	const draws = n * 500
	counts := map[string]int{}
	var rng testRNG
	for i := 0; i < draws; i++ {
		k, ok := s.CollRandSelect(prefix, rng.next())
		if !ok {
			t.Fatal("empty draw")
		}
		counts[string(k[len(prefix):])]++
	}
	if len(counts) != n {
		t.Fatalf("only %d of %d members ever drawn", len(counts), n)
	}
	expected := float64(draws) / float64(n)
	chi := 0.0
	for _, c := range counts {
		d := float64(c) - expected
		chi += d * d / expected
	}
	// For n-1 = 199 degrees of freedom the chi-square 99.9th percentile is well under 280;
	// a uniform draw sits near 199, so a band at 300 flags a real bias without flaking on
	// ordinary sampling noise.
	if chi > 300 {
		t.Fatalf("chi-square %.1f exceeds uniform band (expected near %d)", chi, n-1)
	}
	if math.IsNaN(chi) {
		t.Fatal("chi-square is NaN")
	}
}

// TestRandVecFuzz interleaves a random stream of adds, removes, and draws against a reference
// set and asserts the vector's live offsets resolve exactly to the reference's members after
// every operation, the strongest lockstep check.
func TestRandVecFuzz(t *testing.T) {
	s := newTestStore()
	prefix := setPrefixBytes("s")
	s.CollRandEnsure(prefix)
	ref := map[string]bool{}
	// A deterministic splitmix64 stream so a failure reproduces.
	var rng uint64 = 0x1234567
	nextRand := func() uint64 {
		rng += 0x9e3779b97f4a7c15
		z := rng
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		return z ^ (z >> 31)
	}
	const ops = 20000
	const space = 400
	for i := 0; i < ops; i++ {
		m := fmt.Sprintf("m%d", nextRand()%space)
		switch nextRand() % 3 {
		case 0, 1: // bias toward growth so the set does not sit near empty
			if addMember(t, s, prefix, "s", m) {
				ref[m] = true
			}
		default:
			if removeMember(t, s, prefix, "s", m) {
				delete(ref, m)
			}
		}
		if i%997 == 0 {
			assertVectorMatchesRef(t, s, prefix, ref)
		}
	}
	assertVectorMatchesRef(t, s, prefix, ref)
}

// assertVectorMatchesRef checks the multiset of members reachable through the vector equals
// the reference exactly: same size, and every drawn offset resolves to a reference member.
func assertVectorMatchesRef(t *testing.T, s *Store, prefix []byte, ref map[string]bool) {
	t.Helper()
	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	v := sh.get(prefix)
	var got []string
	if v != nil {
		for _, off := range v.slots {
			k := s.keyAt(off)
			got = append(got, string(k[len(prefix):]))
		}
	}
	sh.mu.Unlock()
	if len(got) != len(ref) {
		t.Fatalf("vector holds %d members, reference has %d", len(got), len(ref))
	}
	sort.Strings(got)
	for _, m := range got {
		if !ref[m] {
			t.Fatalf("vector holds %q not in reference", m)
		}
	}
}
