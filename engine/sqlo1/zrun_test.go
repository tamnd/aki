package sqlo1

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// zrunPair is one collected (sortable score, member) walk entry, with
// the member copied out of the aliased read.
type zrunPair struct {
	s uint64
	m string
}

// zrunCollect walks key's score side and copies every entry out.
func zrunCollect(t *testing.T, z *ZSet, key string) []zrunPair {
	t.Helper()
	var got []zrunPair
	err := z.zrunWalk(context.Background(), []byte(key), func(s uint64, m []byte) {
		got = append(got, zrunPair{s: s, m: string(m)})
	})
	if err != nil {
		t.Fatalf("zrunWalk(%q): %v", key, err)
	}
	return got
}

// zrunCheck asserts the walk of key matches want exactly, in (score,
// member) order, and that the fence counts sum to the same total.
func zrunCheck(t *testing.T, z *ZSet, key string, want []zrunPair) {
	t.Helper()
	got := zrunCollect(t, z, key)
	if len(got) != len(want) {
		t.Fatalf("walk of %q emitted %d entries, want %d", key, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("walk of %q entry %d = (%#x, %q), want (%#x, %q)", key, i, got[i].s, got[i].m, want[i].s, want[i].m)
		}
	}
	total := uint32(0)
	for _, e := range z.zfence {
		total += e.count
	}
	if int(total) != len(want) {
		t.Fatalf("fence counts of %q sum to %d, want %d", key, total, len(want))
	}
}

// sortZrunPairs orders a reference slice the way the runs do.
func sortZrunPairs(pairs []zrunPair) {
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].s != pairs[j].s {
			return pairs[i].s < pairs[j].s
		}
		return pairs[i].m < pairs[j].m
	})
}

// TestZTailCodec pins the root tail encoding: the fence round-trips
// with segid, meta, and count intact, an empty tail decodes to no
// runs, and every malformed shape fails loudly.
func TestZTailCodec(t *testing.T) {
	fence := []zFenceEnt{
		{lo: 0, segid: 7, meta: 0x0102, count: 3},
		{lo: 0x1122334455667788, segid: hashFenceSegidMax, count: 1},
		{lo: 0x1122334455667788, segid: 9, count: 200},
		{lo: 0xFFF0000000000000, segid: 12, meta: 0xFFFF, count: 1},
	}
	p := appendZTail(nil, fence)
	if want := zTailHdrLen + len(fence)*zFenceEntLen; len(p) != want {
		t.Fatalf("encoded tail is %d bytes, want %d", len(p), want)
	}
	got, err := decodeZTail(p, nil)
	if err != nil {
		t.Fatalf("decodeZTail: %v", err)
	}
	if len(got) != len(fence) {
		t.Fatalf("decoded %d fence entries, want %d", len(got), len(fence))
	}
	for i := range fence {
		if got[i] != fence[i] {
			t.Fatalf("fence entry %d = %+v, want %+v", i, got[i], fence[i])
		}
	}

	if got, err := decodeZTail(nil, got); err != nil || len(got) != 0 {
		t.Fatalf("empty tail = (%d runs, %v), want none", len(got), err)
	}

	sentinelEmpty := appendZTail(nil, []zFenceEnt{{lo: 0, segid: 3, count: 0}})
	if got, err := decodeZTail(sentinelEmpty, nil); err != nil || len(got) != 1 || got[0].count != 0 {
		t.Fatalf("emptied sentinel = (%+v, %v), want one zero-count run", got, err)
	}

	doors := []struct {
		name string
		p    []byte
	}{
		{"short header", p[:2]},
		{"paged flag", append([]byte{zflagFencePaged}, p[1:]...)},
		{"unknown flag", append([]byte{0x80}, p[1:]...)},
		{"nonzero byte 1", append([]byte{0, 1}, p[2:]...)},
		{"zero runs with header", []byte{0, 0, 0, 0}},
		{"truncated fence", p[:len(p)-1]},
		{"oversize fence", append(append([]byte{}, p...), 0)},
		{"missing sentinel", appendZTail(nil, []zFenceEnt{{lo: 5, segid: 1, count: 1}})},
		{"out of order", appendZTail(nil, []zFenceEnt{{lo: 0, segid: 1, count: 1}, {lo: 9, segid: 2, count: 1}, {lo: 4, segid: 3, count: 1}})},
		{"empty non-sentinel", appendZTail(nil, []zFenceEnt{{lo: 0, segid: 1, count: 1}, {lo: 9, segid: 2, count: 0}})},
	}
	for _, d := range doors {
		if _, err := decodeZTail(d.p, nil); err == nil {
			t.Fatalf("%s decoded without error", d.name)
		}
	}
}

// TestZRunEntryCodec pins the run image encoding: entries round-trip
// including the empty member, zrunPos answers found, insert-before,
// and append positions with count clipping, and truncated images fail.
func TestZRunEntryCodec(t *testing.T) {
	img := make([]byte, zRunHdrLen)
	img = appendZRunEnt(img, 5, []byte("apple"))
	img = appendZRunEnt(img, 5, []byte("cherry"))
	img = appendZRunEnt(img, 9, []byte(""))
	putZRunHdr(img, 3)

	off := zRunHdrLen
	for _, want := range []zrunPair{{5, "apple"}, {5, "cherry"}, {9, ""}} {
		s, m, next, err := zRunEntAt(img, off)
		if err != nil {
			t.Fatalf("zRunEntAt(%d): %v", off, err)
		}
		if s != want.s || string(m) != want.m {
			t.Fatalf("entry at %d = (%d, %q), want (%d, %q)", off, s, m, want.s, want.m)
		}
		off = next
	}
	if off != len(img) {
		t.Fatalf("iteration ended at %d, image is %d bytes", off, len(img))
	}

	applesEnd := zRunHdrLen + zRunEntHdrLen + len("apple")
	pos, found, liveEnd, err := zrunPos(img, 3, 5, []byte("cherry"))
	if err != nil || !found || pos != applesEnd || liveEnd != len(img) {
		t.Fatalf("zrunPos(cherry) = (%d, %v, %d, %v), want (%d, true, %d)", pos, found, liveEnd, err, applesEnd, len(img))
	}
	pos, found, _, err = zrunPos(img, 3, 5, []byte("banana"))
	if err != nil || found || pos != applesEnd {
		t.Fatalf("zrunPos(banana) = (%d, %v, %v), want insert at %d", pos, found, err, applesEnd)
	}
	pos, found, _, err = zrunPos(img, 3, 12, []byte("zz"))
	if err != nil || found || pos != len(img) {
		t.Fatalf("zrunPos(past end) = (%d, %v, %v), want append at %d", pos, found, err, len(img))
	}
	// The count is the live authority: clipped at 2, the third entry
	// is dead bytes and the append position is its start.
	cherryEnd := applesEnd + zRunEntHdrLen + len("cherry")
	pos, found, liveEnd, err = zrunPos(img, 2, 9, []byte(""))
	if err != nil || found || pos != cherryEnd || liveEnd != cherryEnd {
		t.Fatalf("clipped zrunPos = (%d, %v, %d, %v), want (%d, false, %d)", pos, found, liveEnd, err, cherryEnd, cherryEnd)
	}

	if _, _, _, err := zRunEntAt(img, len(img)-3); err == nil {
		t.Fatal("truncated entry header decoded without error")
	}
	short := img[:len(img)-1]
	putZRunHdr(short, 3)
	if _, _, _, err := zrunPos(short, 3, 99, nil); err == nil {
		t.Fatal("truncated member walked off the end without error")
	}
}

// TestZRunLadder drives the score side end to end: 1500 pairs land
// through the bootstrap, plain inserts, and several splits, the walk
// answers exact (score, member) order with fence counts summing to
// the total, duplicate adds and absent deletes answer false, a delete
// stride lands, every surviving entry cross-checks against the member
// side (the Z-I4 shape), and the cold reopen view agrees.
func TestZRunLadder(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(21))

	const n = 1500
	want := make([]zrunPair, 0, n)
	scores := make(map[string]float64, n)
	for i := range n {
		member := fmt.Sprintf("player:%06d", i)
		score := rng.NormFloat64() * 1e6
		r.memset("board", member, score)
		scores[member] = score
		want = append(want, zrunPair{s: zScoreSortable(score), m: member})
	}

	for i := range n {
		member := fmt.Sprintf("player:%06d", i)
		created, err := r.z.zrunAdd(ctx, []byte("board"), scores[member], []byte(member))
		if err != nil || !created {
			t.Fatalf("zrunAdd(%q) = (%v, %v)", member, created, err)
		}
	}
	sortZrunPairs(want)
	zrunCheck(t, r.z, "board", want)
	if len(r.z.zfence) < 4 {
		t.Fatalf("fence holds %d runs after %d inserts, splits never fired", len(r.z.zfence), n)
	}

	for i := 0; i < n; i += 211 {
		member := fmt.Sprintf("player:%06d", i)
		created, err := r.z.zrunAdd(ctx, []byte("board"), scores[member], []byte(member))
		if err != nil || created {
			t.Fatalf("duplicate zrunAdd(%q) = (%v, %v), want (false, nil)", member, created, err)
		}
	}
	if existed, err := r.z.zrunDel(ctx, []byte("board"), 1.5, []byte("nobody")); err != nil || existed {
		t.Fatalf("absent zrunDel = (%v, %v), want (false, nil)", existed, err)
	}
	if existed, err := r.z.zrunDel(ctx, []byte("board"), scores["player:000001"]+1, []byte("player:000001")); err != nil || existed {
		t.Fatalf("wrong-score zrunDel = (%v, %v), want (false, nil)", existed, err)
	}

	live := want[:0]
	for i := 0; i < n; i += 13 {
		member := fmt.Sprintf("player:%06d", i)
		existed, err := r.z.zrunDel(ctx, []byte("board"), scores[member], []byte(member))
		if err != nil || !existed {
			t.Fatalf("zrunDel(%q) = (%v, %v)", member, existed, err)
		}
		delete(scores, member)
	}
	for _, p := range want {
		if _, ok := scores[p.m]; ok {
			live = append(live, p)
		}
	}
	zrunCheck(t, r.z, "board", live)

	for _, p := range zrunCollect(t, r.z, "board") {
		got, ok := r.memscore("board", p.m)
		if !ok || zScoreSortable(got) != p.s {
			t.Fatalf("member side of %q = (%g, %v), score side holds %#x", p.m, got, ok, p.s)
		}
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cold := r.reopen()
	zrunCheck(t, cold, "board", live)
}

// TestZRunEqualScoreChain forces the separator chain: every member
// shares one score, so splits stamp equal separators and routing must
// disambiguate by first entries. The walk is member order, duplicate
// adds and point deletes land across the chain, an interleaved delete
// pass shrinks runs into merges, deleting everything leaves only the
// zero-count sentinel, and the emptied zset accepts fresh inserts.
func TestZRunEqualScoreChain(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(5))

	const n = 900
	const score = 42.0
	member := func(i int) string { return fmt.Sprintf("m:%05d", i) }
	for i := range n {
		r.memset("lex", member(i), score)
	}

	want := make([]zrunPair, 0, n)
	for _, i := range rng.Perm(n) {
		created, err := r.z.zrunAdd(ctx, []byte("lex"), score, []byte(member(i)))
		if err != nil || !created {
			t.Fatalf("zrunAdd(%q) = (%v, %v)", member(i), created, err)
		}
	}
	for i := range n {
		want = append(want, zrunPair{s: zScoreSortable(score), m: member(i)})
	}
	zrunCheck(t, r.z, "lex", want)
	if len(r.z.zfence) < 4 {
		t.Fatalf("fence holds %d runs, the chain never formed", len(r.z.zfence))
	}
	for i, e := range r.z.zfence[2:] {
		if e.lo != zScoreSortable(score) {
			t.Fatalf("fence entry %d separator = %#x, want the shared score", i+2, e.lo)
		}
	}

	for i := 0; i < n; i += 89 {
		created, err := r.z.zrunAdd(ctx, []byte("lex"), score, []byte(member(i)))
		if err != nil || created {
			t.Fatalf("duplicate zrunAdd(%q) = (%v, %v), want (false, nil)", member(i), created, err)
		}
	}

	for i := 0; i < n; i += 71 {
		existed, err := r.z.zrunDel(ctx, []byte("lex"), score, []byte(member(i)))
		if err != nil || !existed {
			t.Fatalf("zrunDel(%q) = (%v, %v)", member(i), existed, err)
		}
		if existed, err := r.z.zrunDel(ctx, []byte("lex"), score, []byte(member(i))); err != nil || existed {
			t.Fatalf("re-delete of %q = (%v, %v), want (false, nil)", member(i), existed, err)
		}
		created, err := r.z.zrunAdd(ctx, []byte("lex"), score, []byte(member(i)))
		if err != nil || !created {
			t.Fatalf("re-add of %q = (%v, %v)", member(i), created, err)
		}
	}
	zrunCheck(t, r.z, "lex", want)

	// Merges are lazy and fire only when the merged image stays under
	// zRunMin, so runs must shrink to a few hundred bytes first: keep
	// every sixteenth member and the survivors fold together.
	runsBefore := len(r.z.zfence)
	for i := range n {
		if i%16 == 0 {
			continue
		}
		if existed, err := r.z.zrunDel(ctx, []byte("lex"), score, []byte(member(i))); err != nil || !existed {
			t.Fatalf("thinning zrunDel(%q) = (%v, %v)", member(i), existed, err)
		}
	}
	kept := want[:0]
	for i := 0; i < n; i += 16 {
		kept = append(kept, zrunPair{s: zScoreSortable(score), m: member(i)})
	}
	zrunCheck(t, r.z, "lex", kept)
	if len(r.z.zfence) >= runsBefore {
		t.Fatalf("fence holds %d runs after thinning, %d before, merges never fired", len(r.z.zfence), runsBefore)
	}

	for i := 0; i < n; i += 16 {
		if existed, err := r.z.zrunDel(ctx, []byte("lex"), score, []byte(member(i))); err != nil || !existed {
			t.Fatalf("final zrunDel(%q) = (%v, %v)", member(i), existed, err)
		}
	}
	zrunCheck(t, r.z, "lex", nil)
	if len(r.z.zfence) != 1 || r.z.zfence[0].lo != 0 || r.z.zfence[0].count != 0 {
		t.Fatalf("emptied fence = %+v, want the zero-count sentinel alone", r.z.zfence)
	}

	for i := range 5 {
		created, err := r.z.zrunAdd(ctx, []byte("lex"), float64(i), []byte(member(i)))
		if err != nil || !created {
			t.Fatalf("rebirth zrunAdd(%q) = (%v, %v)", member(i), created, err)
		}
	}
	reborn := make([]zrunPair, 0, 5)
	for i := range 5 {
		reborn = append(reborn, zrunPair{s: zScoreSortable(float64(i)), m: member(i)})
	}
	zrunCheck(t, r.z, "lex", reborn)
}

// TestZRunFenceCap drives the flat fence past its run limit with fat
// members: the split past zFenceMaxRuns is the paging transition, so
// every insert lands, the fence comes out paged, and the walk still
// streams the whole board in order, hot and cold.
func TestZRunFenceCap(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()

	fat := func(i int) []byte {
		m := bytes.Repeat([]byte{'f'}, 800)
		copy(m, fmt.Sprintf("fat:%04d:", i))
		return m
	}
	for i := range 4 {
		if _, err := r.z.memSet(ctx, []byte("cap"), fat(i), float64(i+1)); err != nil {
			t.Fatalf("memSet(%d): %v", i, err)
		}
	}

	const n = 600
	for i := 0; i < n; i++ {
		created, err := r.z.zrunAdd(ctx, []byte("cap"), float64(i+1), fat(i))
		if err != nil || !created {
			t.Fatalf("zrunAdd(%d) = (%v, %v)", i, created, err)
		}
	}
	if _, err := r.z.zscoreState(ctx, []byte("cap")); err != nil {
		t.Fatalf("zscoreState: %v", err)
	}
	if !r.z.zpaged {
		t.Fatalf("fence still flat after %d fat adds, the transition never fired", n)
	}
	if len(r.z.zridx) != 1 {
		t.Fatalf("root index holds %d entries, want 1 upper at this size", len(r.z.zridx))
	}
	runs, count := zidxSum(r.z.zridx)
	if runs <= zFenceMaxRuns || count != n {
		t.Fatalf("root index sums (%d runs, %d members), want past the %d flat cap and %d members", runs, count, zFenceMaxRuns, n)
	}

	got := zrunCollect(t, r.z, "cap")
	if len(got) != n {
		t.Fatalf("walk emits %d entries, %d adds landed", len(got), n)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].s > got[i].s || (got[i-1].s == got[i].s && got[i-1].m >= got[i].m) {
			t.Fatalf("walk out of order at entry %d after the transition", i)
		}
	}

	// A splitless insert still lands paged: 0.5 routes below every
	// fat score into the sentinel run, which has room.
	created, err := r.z.zrunAdd(ctx, []byte("cap"), 0.5, []byte("tiny"))
	if err != nil || !created {
		t.Fatalf("paged zrunAdd = (%v, %v)", created, err)
	}
	if got := zrunCollect(t, r.z, "cap"); len(got) != n+1 || got[0].m != "tiny" {
		t.Fatalf("paged walk emits %d entries with head %q, want %d and tiny", len(got), got[0].m, n+1)
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cold := r.reopen()
	if got := zrunCollect(t, cold, "cap"); len(got) != n+1 || got[0].m != "tiny" {
		t.Fatalf("cold paged walk emits %d entries with head %q, want %d and tiny", len(got), got[0].m, n+1)
	}
}

// TestZRunInlineDoor pins the caller contract: score-run ops on an
// inline root are an error until slice 4's upgrade builds both
// families, and on an absent or wrong-typed key they fail the same
// state door.
func TestZRunInlineDoor(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()

	r.memset("small", "m", 1)
	if _, err := r.z.zrunAdd(ctx, []byte("small"), 1, []byte("m")); err == nil {
		t.Fatal("zrunAdd on an inline root landed without error")
	}
	if _, err := r.z.zrunDel(ctx, []byte("small"), 1, []byte("m")); err == nil {
		t.Fatal("zrunDel on an inline root landed without error")
	}
	if err := r.z.zrunWalk(ctx, []byte("small"), func(uint64, []byte) {}); err == nil {
		t.Fatal("zrunWalk on an inline root landed without error")
	}
	if _, err := r.z.zrunAdd(ctx, []byte("absent"), 1, []byte("m")); err == nil {
		t.Fatal("zrunAdd on an absent key landed without error")
	}
	if err := r.s.Set(ctx, []byte("str"), []byte("plain")); err != nil {
		t.Fatalf("Str.Set: %v", err)
	}
	if _, err := r.z.zrunAdd(ctx, []byte("str"), 1, []byte("m")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("zrunAdd(str) error = %v, want ErrWrongType", err)
	}
}
