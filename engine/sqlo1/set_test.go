package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// setRig mirrors hashRig: one Tiered over the recording store, the set
// layer, and the string and hash layers beside it for the cross-type
// doors.
type setRig struct {
	t  *testing.T
	rs *recordingStore
	tr *Tiered
	se *Set
	h  *Hash
	s  *Str
}

func newSetRig(t *testing.T) *setRig {
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
	return &setRig{t: t, rs: rs, tr: tr, se: se, h: h, s: s}
}

// reopen builds a fresh runtime over the same store, the cold view a
// restart would see.
func (r *setRig) reopen() *Set {
	r.t.Helper()
	tr := NewTiered(r.rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     12,
		NowMs:    func() int64 { return 1 << 41 },
	})
	se, err := NewSet(tr, HashConfig{})
	if err != nil {
		r.t.Fatalf("NewSet: %v", err)
	}
	return se
}

func (r *setRig) sadd(key, member string) bool {
	r.t.Helper()
	created, err := r.se.SAdd(context.Background(), []byte(key), []byte(member))
	if err != nil {
		r.t.Fatalf("SAdd(%q, %q): %v", key, member, err)
	}
	return created
}

func (r *setRig) sismember(key, member string) bool {
	r.t.Helper()
	ok, err := r.se.SIsMember(context.Background(), []byte(key), []byte(member))
	if err != nil {
		r.t.Fatalf("SIsMember(%q, %q): %v", key, member, err)
	}
	return ok
}

func (r *setRig) scard(key string) int64 {
	r.t.Helper()
	n, err := r.se.SCard(context.Background(), []byte(key))
	if err != nil {
		r.t.Fatalf("SCard(%q): %v", key, err)
	}
	return n
}

func (r *setRig) encoding(key string) string {
	r.t.Helper()
	enc, ok, err := r.se.Encoding(context.Background(), []byte(key))
	if err != nil {
		r.t.Fatalf("Encoding(%q): %v", key, err)
	}
	if !ok {
		return ""
	}
	return enc
}

// TestSetEntryCodec pins the valueless entry encoding: 3-byte header,
// member bytes, nothing else, and the reserved eflags byte fails
// loudly.
func TestSetEntryCodec(t *testing.T) {
	var region []byte
	region = appendHashEntry(region, []byte("alpha"), nil, 0, encSet)
	region = appendHashEntry(region, []byte(""), nil, 0, encSet)
	region = appendHashEntry(region, []byte("42"), nil, 0, encSet)
	if want := 3*setEntryHdrLen + 5 + 0 + 2; len(region) != want {
		t.Fatalf("encoded region is %d bytes, want %d", len(region), want)
	}

	it := hashEntryIter{p: region, enc: encSet}
	for _, want := range []string{"alpha", "", "42"} {
		f, v, exp, ok, err := it.next()
		if err != nil || !ok {
			t.Fatalf("next: ok=%v err=%v", ok, err)
		}
		if string(f) != want || v != nil || exp != 0 {
			t.Fatalf("entry = (%q, %v, %d), want (%q, nil, 0)", f, v, exp, want)
		}
	}
	if _, _, _, ok, err := it.next(); ok || err != nil {
		t.Fatalf("end of region: ok=%v err=%v", ok, err)
	}

	bad := append([]byte{}, region...)
	bad[0] = 0x01
	it = hashEntryIter{p: bad, enc: encSet}
	if _, _, _, _, err := it.next(); err == nil {
		t.Fatal("reserved eflags bits decoded without error")
	}

	short := region[:len(region)-1]
	it = hashEntryIter{p: short, enc: encSet}
	var err error
	for err == nil {
		var ok bool
		_, _, _, ok, err = it.next()
		if !ok && err == nil {
			t.Fatal("truncated region walked off the end without error")
		}
	}

	if got := hashEntrySize(5, 0, 0, encSet); got != setEntryHdrLen+5 {
		t.Fatalf("hashEntrySize encSet = %d, want %d", got, setEntryHdrLen+5)
	}
}

// TestSetInlinePointOps drives the inline rung end to end: create,
// membership, duplicate add, remove, count, key deletion at zero, and
// the cold view after reopen.
func TestSetInlinePointOps(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	if !r.sadd("s", "a") {
		t.Fatal("first SAdd not created")
	}
	if r.sadd("s", "a") {
		t.Fatal("duplicate SAdd reported created")
	}
	r.sadd("s", "b")
	r.sadd("s", "c")
	if n := r.scard("s"); n != 3 {
		t.Fatalf("SCard = %d, want 3", n)
	}
	if !r.sismember("s", "b") || r.sismember("s", "zzz") {
		t.Fatal("membership answers wrong")
	}
	if n := r.scard("missing"); n != 0 {
		t.Fatalf("SCard(missing) = %d, want 0", n)
	}

	removed, err := r.se.SRem(ctx, []byte("s"), []byte("b"))
	if err != nil || !removed {
		t.Fatalf("SRem(b): removed=%v err=%v", removed, err)
	}
	removed, err = r.se.SRem(ctx, []byte("s"), []byte("b"))
	if err != nil || removed {
		t.Fatalf("SRem(b) again: removed=%v err=%v", removed, err)
	}
	if n := r.scard("s"); n != 2 {
		t.Fatalf("SCard after SRem = %d, want 2", n)
	}

	if _, err := r.se.SRem(ctx, []byte("s"), []byte("a")); err != nil {
		t.Fatalf("SRem(a): %v", err)
	}
	if _, err := r.se.SRem(ctx, []byte("s"), []byte("c")); err != nil {
		t.Fatalf("SRem(c): %v", err)
	}
	if enc := r.encoding("s"); enc != "" {
		t.Fatalf("empty set still answers encoding %q; the key must be gone", enc)
	}

	r.sadd("cold", "x")
	r.sadd("cold", "y")
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se2 := r.reopen()
	ok, err := se2.SIsMember(ctx, []byte("cold"), []byte("x"))
	if err != nil || !ok {
		t.Fatalf("cold SIsMember(x): ok=%v err=%v", ok, err)
	}
	n, err := se2.SCard(ctx, []byte("cold"))
	if err != nil || n != 2 {
		t.Fatalf("cold SCard = %d err=%v, want 2", n, err)
	}
}

// TestSetAllIntFlag pins the one-way intset flag: set on create by a
// canonical integer, cleared forever by the first non-integer member,
// never restored by removals, reset only by key recreation.
func TestSetAllIntFlag(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	r.sadd("ints", "1")
	r.sadd("ints", "-7")
	r.sadd("ints", "12345678901")
	if enc := r.encoding("ints"); enc != "intset" {
		t.Fatalf("all-integer inline set answers %q, want intset", enc)
	}

	r.sadd("ints", "not-a-number")
	if enc := r.encoding("ints"); enc != "listpack" {
		t.Fatalf("mixed inline set answers %q, want listpack", enc)
	}
	if _, err := r.se.SRem(ctx, []byte("ints"), []byte("not-a-number")); err != nil {
		t.Fatalf("SRem: %v", err)
	}
	if enc := r.encoding("ints"); enc != "listpack" {
		t.Fatalf("flag restored by removal: %q, want listpack (one-way)", enc)
	}

	for _, m := range []string{"ints", "not-a-number"} {
		if _, err := r.se.SRem(ctx, []byte("ints"), []byte(m)); err != nil {
			t.Fatalf("SRem(%q): %v", m, err)
		}
	}
	for _, m := range []string{"-7", "12345678901", "1"} {
		if _, err := r.se.SRem(ctx, []byte("ints"), []byte(m)); err != nil {
			t.Fatalf("SRem(%q): %v", m, err)
		}
	}
	r.sadd("ints", "9")
	if enc := r.encoding("ints"); enc != "intset" {
		t.Fatalf("recreated key answers %q, want intset (new generation)", enc)
	}

	// Non-canonical integer spellings are strings: leading zeros, a
	// plus sign, whitespace, minus zero.
	for i, m := range []string{"01", "+1", " 1", "1 ", "-0", ""} {
		key := fmt.Sprintf("nc%d", i)
		r.sadd(key, m)
		if enc := r.encoding(key); enc != "listpack" {
			t.Fatalf("first member %q answers %q, want listpack", m, enc)
		}
	}

	// The flag survives the round trip through the store.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se2 := r.reopen()
	enc, ok, err := se2.Encoding(ctx, []byte("ints"))
	if err != nil || !ok || enc != "intset" {
		t.Fatalf("cold encoding = (%q, %v, %v), want intset", enc, ok, err)
	}
}

// TestSetUpgradeToSegments pushes an inline set past the count
// threshold and checks the segmented rung end to end: encoding answer,
// exact count, full membership, and the cold read after reopen.
func TestSetUpgradeToSegments(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	n := hashInlineMaxCount + 40
	for i := range n {
		r.sadd("big", fmt.Sprintf("%d", i))
	}
	if enc := r.encoding("big"); enc != "hashtable" {
		t.Fatalf("upgraded set answers %q, want hashtable", enc)
	}
	if got := r.scard("big"); got != int64(n) {
		t.Fatalf("SCard = %d, want %d", got, n)
	}
	for i := range n {
		if !r.sismember("big", fmt.Sprintf("%d", i)) {
			t.Fatalf("member %d lost across the upgrade", i)
		}
	}
	if r.sismember("big", "nope") {
		t.Fatal("phantom member after upgrade")
	}

	// An all-integer set past the inline cap answers hashtable, the
	// documented divergence from Redis's 512-entry intset tier.
	if enc := r.encoding("big"); enc != "hashtable" {
		t.Fatalf("large all-int set answers %q, want hashtable", enc)
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se2 := r.reopen()
	got, err := se2.SCard(ctx, []byte("big"))
	if err != nil || got != int64(n) {
		t.Fatalf("cold SCard = %d err=%v, want %d", got, err, n)
	}
	ok, err := se2.SIsMember(ctx, []byte("big"), []byte("77"))
	if err != nil || !ok {
		t.Fatalf("cold SIsMember(77): ok=%v err=%v", ok, err)
	}
}

// TestSetWrongType checks the cross-type doors in all directions:
// set ops against string and hash keys, and string and hash ops
// against set keys, inline and segmented both.
func TestSetWrongType(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	if err := r.s.Set(ctx, []byte("str"), []byte("plain")); err != nil {
		t.Fatalf("Str.Set: %v", err)
	}
	if _, err := r.h.HSet(ctx, []byte("hash"), []byte("f"), []byte("v")); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	r.sadd("inl", "a")
	for i := range hashInlineMaxCount + 10 {
		r.sadd("seg", fmt.Sprintf("%d", i))
	}

	for _, key := range []string{"str", "hash"} {
		if _, err := r.se.SAdd(ctx, []byte(key), []byte("m")); !errors.Is(err, ErrWrongType) {
			t.Fatalf("SAdd(%s) error = %v, want ErrWrongType", key, err)
		}
		if _, err := r.se.SCard(ctx, []byte(key)); !errors.Is(err, ErrWrongType) {
			t.Fatalf("SCard(%s) error = %v, want ErrWrongType", key, err)
		}
		if _, err := r.se.SIsMember(ctx, []byte(key), []byte("m")); !errors.Is(err, ErrWrongType) {
			t.Fatalf("SIsMember(%s) error = %v, want ErrWrongType", key, err)
		}
	}
	for _, key := range []string{"inl", "seg"} {
		if _, err := r.h.HSet(ctx, []byte(key), []byte("f"), []byte("v")); !errors.Is(err, ErrWrongType) {
			t.Fatalf("HSet(%s) error = %v, want ErrWrongType", key, err)
		}
		if _, _, err := r.s.Get(ctx, []byte(key)); !errors.Is(err, ErrWrongType) {
			t.Fatalf("Str.Get(%s) error = %v, want ErrWrongType", key, err)
		}
	}
}

// TestSetReconcileAcceptance checks that the W3 hooks treat a
// segmented set root exactly like a hash root: ReconcileRef
// recognizes the sub and the root count patch round-trips through the
// shared header layout.
func TestSetReconcileAcceptance(t *testing.T) {
	root := appendHashSegRoot(nil, &hashSegRoot{
		sub: setSubSeg, rootgen: 1, rooth: 0xbeef, count: 4, nextSegid: 2,
		fence: []hashFenceEnt{{lo: 0, segid: 1, meta: hashSegMeta(4, 0)}},
	})
	rooth, ok := ReconcileRef(root)
	if !ok || rooth != 0xbeef {
		t.Fatalf("ReconcileRef(set root) = (%#x, %v), want (0xbeef, true)", rooth, ok)
	}
	r, err := decodeHashSegRoot(root, nil, nil)
	if err != nil {
		t.Fatalf("decodeHashSegRoot(set root): %v", err)
	}
	if r.sub != setSubSeg {
		t.Fatalf("decoded sub = %d, want %d", r.sub, setSubSeg)
	}

	// A valueless segment payload feeds SegCounts through the same
	// 12-byte header.
	entries := []hashSegEntry{{fh: 1, field: []byte("a")}, {fh: 2, field: []byte("b")}}
	seg := appendHashSegPayload(nil, entries, encSet)
	n, minExp, ok := SegCounts(seg)
	if !ok || n != 2 || minExp != 0 {
		t.Fatalf("SegCounts(set seg) = (%d, %d, %v), want (2, 0, true)", n, minExp, ok)
	}
}

func (r *setRig) smove(src, dst, member string) bool {
	r.t.Helper()
	moved, err := r.se.SMove(context.Background(), []byte(src), []byte(dst), []byte(member))
	if err != nil {
		r.t.Fatalf("SMove(%q, %q, %q): %v", src, dst, member, err)
	}
	return moved
}

// TestSMoveSemantics pins the Redis answers on inline sets: a missing
// member moves nothing and creates nothing, a present member lands in
// dst exactly once, src==dst answers by membership alone, and both
// keys type-gate before any write.
func TestSMoveSemantics(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	r.sadd("src", "a")
	r.sadd("src", "b")
	if r.smove("src", "dst", "nope") {
		t.Fatal("SMove moved a member src does not hold")
	}
	if r.encoding("dst") != "" {
		t.Fatal("a no-op SMove created the dst key")
	}

	if !r.smove("src", "dst", "a") {
		t.Fatal("SMove did not move a held member")
	}
	if r.sismember("src", "a") || !r.sismember("src", "b") || !r.sismember("dst", "a") {
		t.Fatal("membership after the move is wrong")
	}
	if r.scard("src") != 1 || r.scard("dst") != 1 {
		t.Fatalf("cards after the move = (%d, %d), want (1, 1)", r.scard("src"), r.scard("dst"))
	}

	// Member already in dst: src drops it, dst count holds.
	r.sadd("dst", "b")
	if !r.smove("src", "dst", "b") {
		t.Fatal("SMove onto an existing dst member answered false")
	}
	if r.encoding("src") != "" {
		t.Fatal("src key should be gone at zero members")
	}
	if r.scard("dst") != 2 {
		t.Fatalf("dst card = %d, want 2", r.scard("dst"))
	}

	// src == dst answers by membership and changes nothing.
	if !r.smove("dst", "dst", "a") {
		t.Fatal("SMove(dst, dst) with a held member answered false")
	}
	if r.smove("dst", "dst", "zz") {
		t.Fatal("SMove(dst, dst) with an absent member answered true")
	}
	if r.scard("dst") != 2 {
		t.Fatalf("dst card changed on src==dst to %d", r.scard("dst"))
	}

	// Wrong types raise before any write, even when the member is
	// absent from src, so a bad dst never leaves src half-moved.
	if err := r.s.Set(ctx, []byte("str"), []byte("plain")); err != nil {
		t.Fatalf("Str.Set: %v", err)
	}
	if _, err := r.se.SMove(ctx, []byte("dst"), []byte("str"), []byte("zz")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SMove to a string dst error = %v, want ErrWrongType", err)
	}
	if _, err := r.se.SMove(ctx, []byte("str"), []byte("dst"), []byte("a")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SMove from a string src error = %v, want ErrWrongType", err)
	}
	if _, err := r.h.HSet(ctx, []byte("hash"), []byte("f"), []byte("v")); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if _, err := r.se.SMove(ctx, []byte("dst"), []byte("hash"), []byte("zz")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SMove to a hash dst error = %v, want ErrWrongType", err)
	}
	if r.scard("dst") != 2 {
		t.Fatalf("dst card = %d after rejected moves, want 2", r.scard("dst"))
	}
}

// TestSMoveSegmented exercises both directions across the inline and
// segmented rungs, plus the cold view after a flush.
func TestSMoveSegmented(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	n := hashInlineMaxCount + 40
	for i := range n {
		r.sadd("big", fmt.Sprintf("m%04d", i))
	}
	if r.encoding("big") != "hashtable" {
		t.Fatalf("big encodes %q, want hashtable", r.encoding("big"))
	}

	// Out of a segmented set into a fresh inline one.
	if !r.smove("big", "side", "m0007") {
		t.Fatal("SMove out of a segmented set answered false")
	}
	if r.sismember("big", "m0007") || !r.sismember("side", "m0007") {
		t.Fatal("membership after the segmented move is wrong")
	}
	if r.scard("big") != int64(n-1) || r.scard("side") != 1 {
		t.Fatalf("cards = (%d, %d), want (%d, 1)", r.scard("big"), r.scard("side"), n-1)
	}

	// And back into the segmented set.
	if !r.smove("side", "big", "m0007") {
		t.Fatal("SMove into a segmented set answered false")
	}
	if r.encoding("side") != "" {
		t.Fatal("side key should be gone at zero members")
	}
	if r.scard("big") != int64(n) {
		t.Fatalf("big card = %d, want %d", r.scard("big"), n)
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se2 := r.reopen()
	ok, err := se2.SIsMember(ctx, []byte("big"), []byte("m0007"))
	if err != nil || !ok {
		t.Fatalf("cold SIsMember = (%v, %v), want member present", ok, err)
	}
	cnt, err := se2.SCard(ctx, []byte("big"))
	if err != nil || cnt != int64(n) {
		t.Fatalf("cold SCard = (%d, %v), want %d", cnt, err, n)
	}
}

// smoveMemberAt reads the moved member's location in the state a crash
// after batch p recovers to.
func smoveMemberAt(t *testing.T, r *setRig, p int, member string) (inSrc, inDst bool) {
	t.Helper()
	ms := r.rs.replayPrefix(t, p)
	tr := NewTiered(ms, TieredConfig{
		Budget:   Budget{Entries: 1024, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     uint64(p) + 300,
		NowMs:    func() int64 { return 1 << 41 },
	})
	se, err := NewSet(tr, HashConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	inSrc, err = se.SIsMember(ctx, []byte("src"), []byte(member))
	if err != nil {
		t.Fatalf("prefix %d: SIsMember(src): %v", p, err)
	}
	inDst, err = se.SIsMember(ctx, []byte("dst"), []byte(member))
	if err != nil {
		t.Fatalf("prefix %d: SIsMember(dst): %v", p, err)
	}
	return inSrc, inDst
}

// TestSMoveCrashPrefix is the frame-group test under the worst cut the
// drain can make: one-op batches put the add to dst and the remove
// from src in separate batches, and every prefix must still hold the
// member in at least one set. Add-first ordering is what makes a torn
// tail leave the member in both (the command replays as unfinished),
// never in neither.
func TestSMoveCrashPrefix(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	r.sadd("src", "m")
	r.sadd("src", "keep")
	r.sadd("dst", "other")
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	setup := len(r.rs.batches) // prefixes before this predate the member

	r.tr.dr.maxOps = 1
	if !r.smove("src", "dst", "m") {
		t.Fatal("SMove answered false")
	}
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	sawBoth := false
	for p := setup; p <= len(r.rs.batches); p++ {
		inSrc, inDst := smoveMemberAt(t, r, p, "m")
		if !inSrc && !inDst {
			t.Fatalf("prefix %d: member lost by a batch cut inside the move", p)
		}
		if inSrc && inDst {
			sawBoth = true
		}
	}
	if !sawBoth {
		t.Fatal("no prefix held the member in both sets; the one-op walk should visit the mid-move state")
	}
	inSrc, inDst := smoveMemberAt(t, r, len(r.rs.batches), "m")
	if inSrc || !inDst {
		t.Fatalf("full replay holds member (src=%v, dst=%v), want dst only", inSrc, inDst)
	}
}

// TestSMoveDirtyGuard forces the hazard the guard exists for: a dirty
// src root holds an early drain-queue position the remove would
// coalesce into, which under a batch cut commits the remove before the
// add. The guard must flush first, and the pair's post-images must
// then share one drain batch.
func TestSMoveDirtyGuard(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	r.sadd("src", "m")
	r.sadd("dst", "other")
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	setup := len(r.rs.batches)

	r.sadd("src", "early") // dirty src root with an old queue position
	before := len(r.rs.batches)
	if !r.smove("src", "dst", "m") {
		t.Fatal("SMove answered false")
	}
	guardEnd := len(r.rs.batches)
	if guardEnd == before {
		t.Fatal("SMove did not flush the dirty src root before writing")
	}
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	shared := false
	for _, b := range r.rs.batches[guardEnd:] {
		hasSrc, hasDst := false, false
		for _, op := range b.Ops {
			switch string(op.Rec.Key) {
			case "src":
				hasSrc = true
			case "dst":
				hasDst = true
			}
		}
		if hasSrc != hasDst {
			t.Fatal("the move's src and dst root images split across drain batches")
		}
		if hasSrc && hasDst {
			shared = true
		}
	}
	if !shared {
		t.Fatal("no drain batch carries both root images of the move")
	}
	for p := setup; p <= len(r.rs.batches); p++ {
		if inSrc, inDst := smoveMemberAt(t, r, p, "m"); !inSrc && !inDst {
			t.Fatalf("prefix %d: member lost", p)
		}
	}
}

// TestSetSegRootHotTag pins the writeSegRoot fix: a segmented set's
// root sits in the hot tier under its own type tag, not the hash's.
func TestSetSegRootHotTag(t *testing.T) {
	r := newSetRig(t)
	for i := range hashInlineMaxCount + 40 {
		r.sadd("s", fmt.Sprintf("m%04d", i))
	}
	if r.encoding("s") != "hashtable" {
		t.Fatalf("s encodes %q, want hashtable", r.encoding("s"))
	}
	_, tag, hit, _ := r.tr.ht.probeReadTag([]byte("s"))
	if !hit {
		t.Fatal("segmented set root not resident in the hot tier")
	}
	if tag != TagSet|TagRoot {
		t.Fatalf("hot root tag = %#x, want TagSet|TagRoot = %#x", tag, TagSet|TagRoot)
	}
}

// TestSMembers pins the streaming contract: begin runs once with the
// exact count before any member, inline sets stream in insertion
// order, segmented sets stream every member exactly once, and an
// absent key is begin(0).
func TestSMembers(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	members := func(key string) (int, []string) {
		t.Helper()
		count := -1
		var got []string
		err := r.se.SMembers(ctx, []byte(key), func(n int) {
			if count != -1 {
				t.Fatal("begin ran twice")
			}
			if len(got) != 0 {
				t.Fatal("begin ran after an emit")
			}
			count = n
		}, func(m []byte) {
			got = append(got, string(m))
		})
		if err != nil {
			t.Fatalf("SMembers(%q): %v", key, err)
		}
		return count, got
	}

	if n, got := members("ghost"); n != 0 || len(got) != 0 {
		t.Fatalf("absent key = (%d, %d members), want (0, 0)", n, len(got))
	}

	r.sadd("inl", "c")
	r.sadd("inl", "a")
	r.sadd("inl", "b")
	n, got := members("inl")
	if n != 3 || fmt.Sprint(got) != "[c a b]" {
		t.Fatalf("inline members = (%d, %v), want insertion order [c a b]", n, got)
	}

	total := hashInlineMaxCount + 40
	want := map[string]bool{}
	for i := range total {
		m := fmt.Sprintf("m%04d", i)
		r.sadd("seg", m)
		want[m] = true
	}
	n, got = members("seg")
	if n != total || len(got) != total {
		t.Fatalf("segmented members = (%d, %d emitted), want %d", n, len(got), total)
	}
	seen := map[string]bool{}
	for _, m := range got {
		if seen[m] {
			t.Fatalf("member %q emitted twice", m)
		}
		seen[m] = true
		if !want[m] {
			t.Fatalf("member %q was never added", m)
		}
	}

	if err := r.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatalf("Str.Set: %v", err)
	}
	if err := r.se.SMembers(ctx, []byte("str"), func(int) {}, func([]byte) {}); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SMembers(str) error = %v, want ErrWrongType", err)
	}
}

// TestSScan drives the fh cursor: an inline set answers any cursor
// with everything and a zero next cursor, and a segmented walk in
// small steps visits every member at least once with no fabrications,
// the Redis scan guarantee.
func TestSScan(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	r.sadd("inl", "a")
	r.sadd("inl", "b")
	var got []string
	next, err := r.se.SScan(ctx, []byte("inl"), 12345, 1, func(m []byte) {
		got = append(got, string(m))
	})
	if err != nil || next != 0 || len(got) != 2 {
		t.Fatalf("inline SScan = (next %d, %v, %v), want the whole set and cursor 0", next, got, err)
	}

	// Enough members for several segments (hashSegMax 4032 holds about
	// 500 of these), so the small-count walk must take many steps.
	total := 1200
	want := map[string]bool{}
	for i := range total {
		m := fmt.Sprintf("m%04d", i)
		r.sadd("seg", m)
		want[m] = true
	}
	if r.encoding("seg") != "hashtable" {
		t.Fatalf("seg encodes %q, want hashtable", r.encoding("seg"))
	}
	seen := map[string]bool{}
	steps := 0
	cursor := uint64(0)
	for {
		next, err := r.se.SScan(ctx, []byte("seg"), cursor, 16, func(m []byte) {
			if !want[string(m)] {
				t.Fatalf("SScan emitted %q, never added", m)
			}
			seen[string(m)] = true
		})
		if err != nil {
			t.Fatalf("SScan step %d: %v", steps, err)
		}
		steps++
		if next == 0 {
			break
		}
		if next <= cursor {
			t.Fatalf("cursor went backwards: %d after %d", next, cursor)
		}
		cursor = next
	}
	if len(seen) != total {
		t.Fatalf("scan saw %d members, want %d", len(seen), total)
	}
	if steps < 2 {
		t.Fatalf("segmented scan finished in %d step(s); count 16 over %d members should take several", steps, total)
	}

	if err := r.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatalf("Str.Set: %v", err)
	}
	if _, err := r.se.SScan(ctx, []byte("str"), 0, 10, func([]byte) {}); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SScan(str) error = %v, want ErrWrongType", err)
	}
}
