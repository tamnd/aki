package sqlo1

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

// zsetRig mirrors setRig: one Tiered over the recording store, the
// zset layer, and the string, hash, and set layers beside it for the
// cross-type doors.
type zsetRig struct {
	t  *testing.T
	rs *recordingStore
	tr *Tiered
	z  *ZSet
	h  *Hash
	se *Set
	s  *Str
}

func newZsetRig(t *testing.T) *zsetRig {
	t.Helper()
	rs := newRecordingStore()
	tr := NewTiered(rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     11,
		NowMs:    func() int64 { return 1 << 41 },
	})
	s, err := NewStr(tr, StrConfig{RopeMin: 8 << 10, Log2Chunk: 10})
	if err != nil {
		t.Fatalf("NewStr: %v", err)
	}
	h, err := NewHash(tr, HashConfig{})
	if err != nil {
		t.Fatalf("NewHash: %v", err)
	}
	se, err := NewSet(tr, HashConfig{})
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	z, err := NewZSet(tr, HashConfig{})
	if err != nil {
		t.Fatalf("NewZSet: %v", err)
	}
	return &zsetRig{t: t, rs: rs, tr: tr, z: z, h: h, se: se, s: s}
}

// reopen builds a fresh runtime over the same store, the cold view a
// restart would see.
func (r *zsetRig) reopen() *ZSet {
	r.t.Helper()
	tr := NewTiered(r.rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     12,
		NowMs:    func() int64 { return 1 << 41 },
	})
	z, err := NewZSet(tr, HashConfig{})
	if err != nil {
		r.t.Fatalf("NewZSet: %v", err)
	}
	return z
}

func (r *zsetRig) memset(key, member string, score float64) bool {
	r.t.Helper()
	created, err := r.z.memSet(context.Background(), []byte(key), []byte(member), score)
	if err != nil {
		r.t.Fatalf("memSet(%q, %q, %g): %v", key, member, score, err)
	}
	return created
}

func (r *zsetRig) memscore(key, member string) (float64, bool) {
	r.t.Helper()
	score, ok, err := r.z.memScore(context.Background(), []byte(key), []byte(member))
	if err != nil {
		r.t.Fatalf("memScore(%q, %q): %v", key, member, err)
	}
	return score, ok
}

func (r *zsetRig) zcard(key string) int64 {
	r.t.Helper()
	n, err := r.z.ZCard(context.Background(), []byte(key))
	if err != nil {
		r.t.Fatalf("ZCard(%q): %v", key, err)
	}
	return n
}

// TestZMemEntryCodec pins the member entry encoding: the 3-byte set
// header, member bytes, then exactly zmemScoreLen score bytes whose
// big-endian sortable image preserves score order under memcmp, which
// is the byte contract the score runs fence on.
func TestZMemEntryCodec(t *testing.T) {
	img := func(score float64) []byte {
		var b [zmemScoreLen]byte
		binary.BigEndian.PutUint64(b[:], zScoreSortable(score))
		return b[:]
	}

	var region []byte
	region = appendHashEntry(region, []byte("alice"), img(1.5), 0, encZMem)
	region = appendHashEntry(region, []byte(""), img(-2), 0, encZMem)
	region = appendHashEntry(region, []byte("bob"), img(0), 0, encZMem)
	if want := 3*(setEntryHdrLen+zmemScoreLen) + 5 + 0 + 3; len(region) != want {
		t.Fatalf("encoded region is %d bytes, want %d", len(region), want)
	}

	it := hashEntryIter{p: region, enc: encZMem}
	for _, want := range []struct {
		member string
		score  float64
	}{{"alice", 1.5}, {"", -2}, {"bob", 0}} {
		f, v, exp, ok, err := it.next()
		if err != nil || !ok {
			t.Fatalf("next: ok=%v err=%v", ok, err)
		}
		if string(f) != want.member || exp != 0 {
			t.Fatalf("entry = (%q, exp %d), want (%q, 0)", f, exp, want.member)
		}
		if len(v) != zmemScoreLen {
			t.Fatalf("score image of %q is %d bytes, want %d", f, len(v), zmemScoreLen)
		}
		if got := zScoreFromSortable(binary.BigEndian.Uint64(v)); got != want.score {
			t.Fatalf("score of %q = %g, want %g", f, got, want.score)
		}
	}
	if _, _, _, ok, err := it.next(); ok || err != nil {
		t.Fatalf("end of region: ok=%v err=%v", ok, err)
	}

	if got := hashEntrySize(5, zmemScoreLen, 0, encZMem); got != setEntryHdrLen+5+zmemScoreLen {
		t.Fatalf("hashEntrySize encZMem = %d, want %d", got, setEntryHdrLen+5+zmemScoreLen)
	}

	for _, pair := range [][2]float64{{-2, -1.5}, {-1, 0}, {0, 0.25}, {1.5, 1e9}} {
		if bytes.Compare(img(pair[0]), img(pair[1])) >= 0 {
			t.Fatalf("score images of %g and %g are not memcmp-ordered", pair[0], pair[1])
		}
	}

	bad := append([]byte{}, region...)
	bad[0] = 0x01
	it = hashEntryIter{p: bad, enc: encZMem}
	if _, _, _, _, err := it.next(); err == nil {
		t.Fatal("reserved eflags bits decoded without error")
	}

	short := region[:len(region)-1]
	it = hashEntryIter{p: short, enc: encZMem}
	var err error
	for err == nil {
		var ok bool
		_, _, _, ok, err = it.next()
		if !ok && err == nil {
			t.Fatal("truncated region walked off the end without error")
		}
	}
}

// TestZSetRootTail pins the root tail seam: a zset root round-trips
// an opaque tail through encode and decode, a hash or set root with
// trailing bytes fails loudly, and the decoded tail aliases nothing
// the encoder holds.
func TestZSetRootTail(t *testing.T) {
	tail := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01}
	r := hashSegRoot{
		sub:       zsetSubSeg,
		rootgen:   3,
		rooth:     0x1234,
		count:     2,
		nextSegid: 2,
		fence:     []hashFenceEnt{{lo: 0, segid: 1, meta: 5}},
		tail:      tail,
	}
	p := appendHashSegRoot(nil, &r)

	got, err := decodeHashSegRoot(p, nil, nil)
	if err != nil {
		t.Fatalf("decodeHashSegRoot: %v", err)
	}
	if !bytes.Equal(got.tail, tail) {
		t.Fatalf("decoded tail = %x, want %x", got.tail, tail)
	}
	if got.count != 2 || got.rooth != 0x1234 || len(got.fence) != 1 {
		t.Fatalf("decoded root = %+v", got)
	}

	bare := r
	bare.tail = nil
	pb := appendHashSegRoot(nil, &bare)
	gb, err := decodeHashSegRoot(pb, nil, nil)
	if err != nil {
		t.Fatalf("decodeHashSegRoot without tail: %v", err)
	}
	if len(gb.tail) != 0 {
		t.Fatalf("tailless zset root decoded a %d-byte tail", len(gb.tail))
	}

	for _, sub := range []uint8{hashSubSeg, setSubSeg} {
		hr := r
		hr.sub = sub
		hp := appendHashSegRoot(nil, &hr)
		if _, err := decodeHashSegRoot(hp, nil, nil); err == nil {
			t.Fatalf("sub %d root with trailing bytes decoded without error", sub)
		}
	}

	if _, err := decodeHashSegRoot(p[:len(p)-len(tail)-1], nil, nil); err == nil {
		t.Fatal("truncated zset root decoded without error")
	}
}

// TestZSetMemberLadder drives the member side through the whole
// representation ladder: 1200 members with random scores cross the
// inline thresholds and several segment splits, every score reads
// back exactly, deletes land, and the cold view after reopen agrees.
func TestZSetMemberLadder(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(7))

	const n = 1200
	want := make(map[string]float64, n)
	for i := range n {
		member := fmt.Sprintf("player:%06d", i)
		score := rng.NormFloat64() * 1e6
		if !r.memset("board", member, score) {
			t.Fatalf("memSet(%q) not created", member)
		}
		want[member] = score
	}
	if got := r.zcard("board"); got != n {
		t.Fatalf("ZCard = %d, want %d", got, n)
	}

	// Score updates are not-created and replace the stored image.
	for i := 0; i < n; i += 97 {
		member := fmt.Sprintf("player:%06d", i)
		score := rng.NormFloat64()
		if r.memset("board", member, score) {
			t.Fatalf("memSet(%q) update answered created", member)
		}
		want[member] = score
	}
	if got := r.zcard("board"); got != n {
		t.Fatalf("ZCard after updates = %d, want %d", got, n)
	}

	for member, score := range want {
		got, ok := r.memscore("board", member)
		if !ok || got != score {
			t.Fatalf("memScore(%q) = (%g, %v), want (%g, true)", member, got, ok, score)
		}
	}
	if _, ok := r.memscore("board", "nobody"); ok {
		t.Fatal("memScore of an absent member answered ok")
	}

	deleted := 0
	for i := 0; i < n; i += 13 {
		member := fmt.Sprintf("player:%06d", i)
		existed, err := r.z.memDel(ctx, []byte("board"), []byte(member))
		if err != nil || !existed {
			t.Fatalf("memDel(%q) = (%v, %v)", member, existed, err)
		}
		delete(want, member)
		deleted++
	}
	if got := r.zcard("board"); got != int64(n-deleted) {
		t.Fatalf("ZCard after deletes = %d, want %d", got, n-deleted)
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cold := r.reopen()
	for member, score := range want {
		got, ok, err := cold.memScore(ctx, []byte("board"), []byte(member))
		if err != nil || !ok || got != score {
			t.Fatalf("cold memScore(%q) = (%g, %v, %v), want (%g, true, nil)", member, got, ok, err, score)
		}
	}
	if got, err := cold.ZCard(ctx, []byte("board")); err != nil || got != int64(n-deleted) {
		t.Fatalf("cold ZCard = (%d, %v), want %d", got, err, n-deleted)
	}
}

// TestZSetOccupancy holds the segmented ladder to the doc 09 band:
// leaderboard-shaped members land segments in the 80-150 member range
// on average, and no segment strays past what mem_max 4032 admits.
func TestZSetOccupancy(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(9))

	const n = 3000
	for i := range n {
		r.memset("board", fmt.Sprintf("player:%06d", i), rng.NormFloat64()*1e6)
	}

	s, err := r.z.memStats(ctx, []byte("board"))
	if err != nil {
		t.Fatalf("memStats: %v", err)
	}
	if s.members != n {
		t.Fatalf("memStats members = %d, want %d", s.members, n)
	}
	if s.segs < 2 {
		t.Fatalf("memStats segs = %d, ladder never segmented", s.segs)
	}
	avg := float64(s.members) / float64(s.segs)
	if avg < 80 || avg > 150 {
		t.Fatalf("average occupancy = %.1f members per segment, want the doc 09 band [80, 150] (segs=%d min=%d max=%d)", avg, s.segs, s.minSeg, s.maxSeg)
	}
	perEntry := setEntryHdrLen + len("player:000000") + zmemScoreLen
	if lim := (hashSegMax - hashSegHdrLen) / perEntry; s.maxSeg > lim {
		t.Fatalf("largest segment holds %d members, mem_max admits %d", s.maxSeg, lim)
	}
	if s.minSeg <= 0 {
		t.Fatalf("smallest segment holds %d members", s.minSeg)
	}
	if s.bytes > s.segs*hashSegMax {
		t.Fatalf("payload bytes %d exceed %d segments at mem_max", s.bytes, s.segs)
	}
}

// TestZSetPagedMembers pushes the member side through the fence
// paging transition with fat members, the pageRigHash trick: ~800
// byte members put ~4 entries in a segment, so 600 members cross the
// 128-segment flat-fence limit, and the paged root still answers
// point reads, the count, and the telemetry walk, hot and cold.
func TestZSetPagedMembers(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()

	const n = 600
	member := func(i int) []byte {
		m := bytes.Repeat([]byte{'m'}, 800)
		copy(m, fmt.Sprintf("member:%06d:", i))
		return m
	}
	for i := range n {
		created, err := r.z.memSet(ctx, []byte("fat"), member(i), float64(i)/3)
		if err != nil || !created {
			t.Fatalf("memSet(%d) = (%v, %v)", i, created, err)
		}
	}
	if got := r.zcard("fat"); got != n {
		t.Fatalf("ZCard = %d, want %d", got, n)
	}

	st, _, _, err := r.z.h.stateOf(ctx, []byte("fat"))
	if err != nil || st != hashSegState {
		t.Fatalf("stateOf = (%v, %v), want hashSegState", st, err)
	}
	if !r.z.h.segRoot.paged {
		t.Fatalf("root not paged at %d fat members (%d segments)", n, len(r.z.h.segRoot.fence))
	}

	for i := 0; i < n; i += 41 {
		got, ok, err := r.z.memScore(ctx, []byte("fat"), member(i))
		if err != nil || !ok || got != float64(i)/3 {
			t.Fatalf("memScore(%d) = (%g, %v, %v)", i, got, ok, err)
		}
	}

	s, err := r.z.memStats(ctx, []byte("fat"))
	if err != nil {
		t.Fatalf("memStats: %v", err)
	}
	if s.members != n || s.segs <= hashFenceMaxSegs {
		t.Fatalf("memStats = %+v, want %d members over more than %d segments", s, n, hashFenceMaxSegs)
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cold := r.reopen()
	for i := 0; i < n; i += 89 {
		got, ok, err := cold.memScore(ctx, []byte("fat"), member(i))
		if err != nil || !ok || got != float64(i)/3 {
			t.Fatalf("cold memScore(%d) = (%g, %v, %v)", i, got, ok, err)
		}
	}
}

// TestZSetWrongType pins the cross-type doors: zset ops on string,
// hash, and set keys answer ErrWrongType, and the other layers answer
// it on a zset key.
func TestZSetWrongType(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()

	if err := r.s.Set(ctx, []byte("str"), []byte("plain")); err != nil {
		t.Fatalf("Str.Set: %v", err)
	}
	if _, err := r.h.HSet(ctx, []byte("hash"), []byte("f"), []byte("v")); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if _, err := r.se.SAdd(ctx, []byte("set"), []byte("m")); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	r.memset("z", "m", 1)

	for _, key := range []string{"str", "hash", "set"} {
		if _, err := r.z.memSet(ctx, []byte(key), []byte("m"), 1); !errors.Is(err, ErrWrongType) {
			t.Fatalf("memSet(%s) error = %v, want ErrWrongType", key, err)
		}
		if _, _, err := r.z.memScore(ctx, []byte(key), []byte("m")); !errors.Is(err, ErrWrongType) {
			t.Fatalf("memScore(%s) error = %v, want ErrWrongType", key, err)
		}
		if _, err := r.z.ZCard(ctx, []byte(key)); !errors.Is(err, ErrWrongType) {
			t.Fatalf("ZCard(%s) error = %v, want ErrWrongType", key, err)
		}
	}

	if _, err := r.h.HSet(ctx, []byte("z"), []byte("f"), []byte("v")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("HSet(z) error = %v, want ErrWrongType", err)
	}
	if _, err := r.se.SAdd(ctx, []byte("z"), []byte("m")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SAdd(z) error = %v, want ErrWrongType", err)
	}
	if _, _, err := r.s.Get(ctx, []byte("z")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Str.Get(z) error = %v, want ErrWrongType", err)
	}
}
