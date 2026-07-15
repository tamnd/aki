package sqlo1b

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

func TestBucketOf(t *testing.T) {
	cases := []struct {
		placement uint64
		level     uint8
		split     uint64
		want      uint64
	}{
		{0, 0, 0, 0},
		{0xFFFF, 0, 0, 0},
		{5, 3, 2, 5},         // 5&7=5, not below split
		{9, 3, 2, 9},         // 9&7=1 below split 2, so 9&15=9
		{17, 3, 2, 1},        // 17&7=1 below split 2, 17&15=1
		{0b1011, 2, 0, 0b11}, // plain level mask
	}
	for _, tc := range cases {
		if got := BucketOf(tc.placement, tc.level, tc.split); got != tc.want {
			t.Errorf("BucketOf(%#x, %d, %d) = %d, want %d", tc.placement, tc.level, tc.split, got, tc.want)
		}
	}
}

func TestAdvanceSplit(t *testing.T) {
	if l, s := AdvanceSplit(0, 0); l != 1 || s != 0 {
		t.Errorf("advance from (0,0) gave (%d,%d)", l, s)
	}
	if l, s := AdvanceSplit(3, 6); l != 3 || s != 7 {
		t.Errorf("advance from (3,6) gave (%d,%d)", l, s)
	}
	if l, s := AdvanceSplit(3, 7); l != 4 || s != 0 {
		t.Errorf("advance from (3,7) gave (%d,%d)", l, s)
	}
	if n := NumBuckets(3, 6); n != 14 {
		t.Errorf("NumBuckets(3,6) = %d, want 14", n)
	}
}

func TestShouldSplitBoundary(t *testing.T) {
	// lf85 over one bucket of 42: 35 entries is 83.3%, 36 is 85.7%.
	if ShouldSplit(35, 1) {
		t.Error("split at 35/42")
	}
	if !ShouldSplit(36, 1) {
		t.Error("no split at 36/42")
	}
	// Exact 85% must not split: 357 entries over 10 buckets.
	if ShouldSplit(357, 10) {
		t.Error("split at exactly 85%")
	}
	if !ShouldSplit(358, 10) {
		t.Error("no split just past 85%")
	}
}

// hashFor builds a hash that places in bucket b at the given level
// with chosen higher placement bits and fingerprint.
func hashFor(b uint64, level uint8, hi uint64, fp uint16) uint64 {
	return uint64(fp)<<48 | (hi<<level|b)&placementMask
}

func metaFor(t *testing.T, h uint64, base uint8) uint16 {
	t.Helper()
	m, err := MakeEntryMeta(1, ExpClassNone, true)
	if err != nil {
		t.Fatal(err)
	}
	w, err := SplitWindow(h, base)
	if err != nil {
		t.Fatal(err)
	}
	m, err = MetaWithWindow(m, w)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestSplitBucketWindowPath(t *testing.T) {
	// Bucket 1 at level 1, base 0: windows cover levels 0..8, so the
	// split routes without a refresh and children inherit base 0.
	c := newChunk(t, 1, 0)
	hashes := []uint64{
		hashFor(1, 1, 0b0, 0x1111), // bit 1 = 0, stays
		hashFor(1, 1, 0b1, 0x2222), // bit 1 = 1, moves
		hashFor(1, 1, 0b10, 0x3333),
		hashFor(1, 1, 0b11, 0x4444),
	}
	for i, h := range hashes {
		if err := c.InsertEntry(Fingerprint(h), metaFor(t, h, 0), uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	left, right, err := SplitBucket([]*Chunk{c}, 1, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 || len(right) != 1 {
		t.Fatalf("split gave %d and %d chunks, want 1 and 1", len(left), len(right))
	}
	if left[0].ChunkNoLo() != 1 || right[0].ChunkNoLo() != 3 {
		t.Fatalf("children numbered %d and %d, want 1 and 3", left[0].ChunkNoLo(), right[0].ChunkNoLo())
	}
	if left[0].WindowBase() != 0 || right[0].WindowBase() != 0 {
		t.Fatal("window path must inherit the parent base")
	}
	if left[0].Count() != 2 || right[0].Count() != 2 {
		t.Fatalf("routed %d and %d entries, want 2 and 2", left[0].Count(), right[0].Count())
	}
	fp, meta, vptr := left[0].EntryAt(0)
	if fp != 0x1111 || vptr != 0 || meta != metaFor(t, hashes[0], 0) {
		t.Fatalf("left entry 0 came back fp %#x meta %#x vptr %d", fp, meta, vptr)
	}
	fp, _, _ = right[0].EntryAt(0)
	if fp != 0x2222 {
		t.Fatalf("right entry 0 fp %#x, want 0x2222", fp)
	}
}

func TestSplitBucketRefreshPath(t *testing.T) {
	// Base 0 covers levels 0..8, so a split at level 9 exhausts the
	// window: nil refresh gets the sentinel, a callback rebases the
	// children to base 9.
	const bucket = uint64(1)
	c := newChunk(t, bucket, 0)
	byVptr := map[uint64]uint64{}
	for i := range 6 {
		h := hashFor(bucket, 9, uint64(i), uint16(0x100+i))
		byVptr[uint64(i)] = h
		if err := c.InsertEntry(Fingerprint(h), metaFor(t, h, 0), uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := SplitBucket([]*Chunk{c}, bucket, 9, nil); !errors.Is(err, ErrWindowExhausted) {
		t.Fatalf("nil refresh gave %v, want ErrWindowExhausted", err)
	}
	calls := 0
	refresh := func(vptrs []uint64) ([]uint64, error) {
		calls++
		hs := make([]uint64, len(vptrs))
		for i, v := range vptrs {
			hs[i] = byVptr[v]
		}
		return hs, nil
	}
	left, right, err := SplitBucket([]*Chunk{c}, bucket, 9, refresh)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("refresh called %d times, want 1", calls)
	}
	if left[0].WindowBase() != 9 || right[0].WindowBase() != 9 {
		t.Fatal("refresh path must rebase the children to the level")
	}
	if right[0].ChunkNoLo() != uint32(bucket+1<<9) {
		t.Fatalf("right bucket numbered %d, want %d", right[0].ChunkNoLo(), bucket+1<<9)
	}
	// hi = i, so bit 9 of placement is i&1: evens stay, odds move.
	if left[0].Count() != 3 || right[0].Count() != 3 {
		t.Fatalf("routed %d and %d entries, want 3 and 3", left[0].Count(), right[0].Count())
	}
	for _, side := range [][]*Chunk{left, right} {
		for i := range side[0].Count() {
			fp, meta, vptr := side[0].EntryAt(i)
			h := byVptr[vptr]
			if Fingerprint(h) != fp {
				t.Fatalf("vptr %d fp %#x diverged", vptr, fp)
			}
			w, err := SplitWindow(h, 9)
			if err != nil {
				t.Fatal(err)
			}
			if MetaSplitWindow(meta) != w {
				t.Fatalf("vptr %d window %#x, want %#x rebased to 9", vptr, MetaSplitWindow(meta), w)
			}
		}
	}
}

func TestSplitBucketRefreshRejects(t *testing.T) {
	const bucket = uint64(0)
	h := hashFor(bucket, 9, 1, 0x700)
	build := func() []*Chunk {
		c := newChunk(t, bucket, 0)
		if err := c.InsertEntry(Fingerprint(h), metaFor(t, h, 0), 5); err != nil {
			t.Fatal(err)
		}
		return []*Chunk{c}
	}
	cases := []struct {
		name    string
		refresh RefreshFunc
	}{
		{"error passthrough", func([]uint64) ([]uint64, error) { return nil, errors.New("disk") }},
		{"wrong count", func(v []uint64) ([]uint64, error) { return nil, nil }},
		{"fingerprint mismatch", func(v []uint64) ([]uint64, error) { return []uint64{h ^ 1<<50}, nil }},
		{"wrong bucket", func(v []uint64) ([]uint64, error) { return []uint64{h ^ 1}, nil }},
	}
	for _, tc := range cases {
		if _, _, err := SplitBucket(build(), bucket, 9, tc.refresh); err == nil {
			t.Errorf("%s: split succeeded", tc.name)
		}
	}
}

func TestSplitBucketChainPacking(t *testing.T) {
	// 83 entries all landing on one side: that side packs as 41 in
	// the base chunk (chain slot reserved) plus 42 in the overflow,
	// and the empty side still gets its one empty chunk.
	const bucket = uint64(0)
	base := newChunk(t, bucket, 0)
	chain := newChunk(t, bucket, 0)
	byVptr := map[uint64]uint64{}
	insert := func(c *Chunk, i int) {
		h := hashFor(bucket, 0, uint64(i)<<1, uint16(i)) // bit 0 of hi is 0: all stay left at level 0
		byVptr[uint64(i)] = h
		if err := c.InsertEntry(Fingerprint(h), metaFor(t, h, 0), uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 41 {
		insert(base, i)
	}
	for i := 41; i < 83; i++ {
		insert(chain, i)
	}
	left, right, err := SplitBucket([]*Chunk{base, chain}, bucket, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 2 || left[0].Count() != 41 || left[1].Count() != 42 {
		t.Fatalf("left packed as %d chunks, counts %v", len(left), chunkCounts(left))
	}
	if left[0].Chained() || left[1].Chained() {
		t.Fatal("packBucket must not set chain pointers, linking is the store's business")
	}
	if len(right) != 1 || right[0].Count() != 0 {
		t.Fatalf("empty side came back as %d chunks, count %d", len(right), right[0].Count())
	}
	if right[0].ChunkNoLo() != 1 {
		t.Fatalf("right bucket numbered %d, want 1", right[0].ChunkNoLo())
	}
}

func chunkCounts(cs []*Chunk) []int {
	out := make([]int, len(cs))
	for i, c := range cs {
		out[i] = c.Count()
	}
	return out
}

func TestSplitBucketMixedBaseRefreshes(t *testing.T) {
	// Both bases cover level 5 on their own, but the children need
	// one base, so mixed bases must take the refresh path.
	a := newChunk(t, 0, 0)
	b := newChunk(t, 0, 3)
	if _, _, err := SplitBucket([]*Chunk{a, b}, 0, 5, nil); !errors.Is(err, ErrWindowExhausted) {
		t.Fatalf("mixed bases gave %v, want ErrWindowExhausted", err)
	}
}

func TestSplitBucketRejects(t *testing.T) {
	c := newChunk(t, 2, 0)
	if _, _, err := SplitBucket(nil, 0, 0, nil); err == nil {
		t.Error("empty bucket image split")
	}
	if _, _, err := SplitBucket([]*Chunk{c}, 2, 48, nil); err == nil {
		t.Error("level 48 split")
	}
	if _, _, err := SplitBucket([]*Chunk{c}, 2, 1, nil); err == nil {
		t.Error("bucket 2 split at level 1")
	}
	if _, _, err := SplitBucket([]*Chunk{c}, 3, 2, nil); err == nil {
		t.Error("chunk_no_lo mismatch accepted")
	}
}

// miniTable is the property-test store: buckets are chains of
// unlinked chunks, records are a vptr-to-key map, and every split
// goes through SplitBucket exactly as the real store will drive it.
type miniTable struct {
	t         *testing.T
	level     uint8
	split     uint64
	buckets   [][]*Chunk
	keys      map[uint64][]byte
	oracle    map[string]uint64
	entries   uint64
	splits    int
	refreshes int
}

func newMiniTable(t *testing.T) *miniTable {
	first, err := NewChunk(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return &miniTable{
		t:       t,
		buckets: [][]*Chunk{{first}},
		keys:    map[uint64][]byte{},
		oracle:  map[string]uint64{},
	}
}

func (m *miniTable) refresh(vptrs []uint64) ([]uint64, error) {
	m.refreshes++
	hs := make([]uint64, len(vptrs))
	for i, v := range vptrs {
		key, ok := m.keys[v]
		if !ok {
			return nil, fmt.Errorf("no record at vptr %d", v)
		}
		hs[i] = KeyHash(key)
	}
	return hs, nil
}

func (m *miniTable) insert(key []byte, vptr uint64) {
	h := KeyHash(key)
	b := BucketOf(PlacementBits(h), m.level, m.split)
	chain := m.buckets[b]
	last := chain[len(chain)-1]
	// The store chains at 41 to keep the pointer slot free; overflow
	// chunks share the base chunk's window base.
	if last.Count() >= ChunkChainCap {
		next, err := NewChunk(b, chain[0].WindowBase())
		if err != nil {
			m.t.Fatal(err)
		}
		m.buckets[b] = append(chain, next)
		last = next
	}
	if err := last.InsertEntry(Fingerprint(h), metaFor(m.t, h, last.WindowBase()), vptr); err != nil {
		m.t.Fatal(err)
	}
	m.keys[vptr] = key
	m.oracle[string(key)] = vptr
	m.entries++
	for ShouldSplit(m.entries, NumBuckets(m.level, m.split)) {
		m.splitOne()
	}
}

func (m *miniTable) splitOne() {
	s := m.split
	left, right, err := SplitBucket(m.buckets[s], s, m.level, m.refresh)
	if err != nil {
		m.t.Fatal(err)
	}
	if uint64(len(m.buckets)) != NumBuckets(m.level, m.split) {
		m.t.Fatalf("bucket slice length %d diverged from NumBuckets %d", len(m.buckets), NumBuckets(m.level, m.split))
	}
	m.buckets[s] = left
	m.buckets = append(m.buckets, right)
	m.level, m.split = AdvanceSplit(m.level, m.split)
	m.splits++
}

func (m *miniTable) remove(key []byte) {
	h := KeyHash(key)
	vptr, ok := m.oracle[string(key)]
	if !ok {
		m.t.Fatalf("remove of absent key %q", key)
	}
	b := BucketOf(PlacementBits(h), m.level, m.split)
	for _, c := range m.buckets[b] {
		for i := range c.Count() {
			fp, _, v := c.EntryAt(i)
			if fp == Fingerprint(h) && v == vptr {
				if err := c.RemoveEntry(i); err != nil {
					m.t.Fatal(err)
				}
				delete(m.oracle, string(key))
				delete(m.keys, vptr)
				m.entries--
				return
			}
		}
	}
	m.t.Fatalf("key %q not found in bucket %d", key, b)
}

func (m *miniTable) lookup(key []byte) (uint64, bool) {
	h := KeyHash(key)
	b := BucketOf(PlacementBits(h), m.level, m.split)
	var found uint64
	ok := false
	for _, c := range m.buckets[b] {
		c.Probe(Fingerprint(h), func(i int, meta uint16, vptr uint64) bool {
			if string(m.keys[vptr]) == string(key) {
				found, ok = vptr, true
				return false
			}
			return true
		})
		if ok {
			break
		}
	}
	return found, ok
}

func (m *miniTable) verify() {
	m.t.Helper()
	for key, want := range m.oracle {
		got, ok := m.lookup([]byte(key))
		if !ok || got != want {
			m.t.Fatalf("key %q lost: got %d ok %v, want %d (level %d split %d)", key, got, ok, want, m.level, m.split)
		}
	}
	var total uint64
	for b, chain := range m.buckets {
		for _, c := range chain {
			if c.ChunkNoLo() != uint32(b) {
				m.t.Fatalf("bucket %d holds chunk_no_lo %d", b, c.ChunkNoLo())
			}
			if c.WindowBase() != chain[0].WindowBase() {
				m.t.Fatalf("bucket %d has mixed window bases", b)
			}
			for i := range c.Count() {
				fp, meta, vptr := c.EntryAt(i)
				key, ok := m.keys[vptr]
				if !ok {
					m.t.Fatalf("bucket %d holds ghost vptr %d", b, vptr)
				}
				h := KeyHash(key)
				if Fingerprint(h) != fp {
					m.t.Fatalf("vptr %d fp %#x, hash says %#x", vptr, fp, Fingerprint(h))
				}
				if BucketOf(PlacementBits(h), m.level, m.split) != uint64(b) {
					m.t.Fatalf("vptr %d misrouted to bucket %d", vptr, b)
				}
				w, err := SplitWindow(h, c.WindowBase())
				if err != nil {
					m.t.Fatal(err)
				}
				if MetaSplitWindow(meta) != w {
					m.t.Fatalf("vptr %d window %#x, hash at base %d says %#x", vptr, MetaSplitWindow(meta), c.WindowBase(), w)
				}
				total++
			}
		}
	}
	if total != m.entries || total != uint64(len(m.oracle)) {
		m.t.Fatalf("table holds %d entries, counter %d, oracle %d", total, m.entries, len(m.oracle))
	}
}

// TestLinearHashingSplitDuringTraffic grows one bucket past level 10
// under mixed inserts and deletes against a map oracle. Ten levels
// forces the refresh path (a window is nine bits), and the window
// path keeps carrying splits after chunks rebase, so both routes
// run under load.
func TestLinearHashingSplitDuringTraffic(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	m := newMiniTable(t)
	var live []string
	nextVptr := uint64(1)
	const ops = 60_000
	for op := range ops {
		if len(live) > 0 && rng.Intn(100) < 15 {
			i := rng.Intn(len(live))
			m.remove([]byte(live[i]))
			live[i] = live[len(live)-1]
			live = live[:len(live)-1]
		} else {
			key := make([]byte, 8)
			binary.LittleEndian.PutUint64(key, rng.Uint64())
			if _, dup := m.oracle[string(key)]; dup {
				continue
			}
			m.insert(key, nextVptr)
			live = append(live, string(key))
			nextVptr++
		}
		if op%8000 == 7999 {
			m.verify()
		}
	}
	m.verify()
	if m.level < 10 {
		t.Fatalf("table only reached level %d, the test must cross 10 to force refreshes", m.level)
	}
	if m.refreshes == 0 {
		t.Fatal("no split took the refresh path")
	}
	if m.refreshes >= m.splits {
		t.Fatalf("all %d splits refreshed, the window path never carried one", m.splits)
	}
	t.Logf("level %d split %d, %d entries, %d splits, %d refreshes (%.1f%%)",
		m.level, m.split, m.entries, m.splits, m.refreshes, 100*float64(m.refreshes)/float64(m.splits))
}
