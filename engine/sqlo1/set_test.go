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
	region = appendHashEntry(region, []byte("alpha"), nil, 0, true)
	region = appendHashEntry(region, []byte(""), nil, 0, true)
	region = appendHashEntry(region, []byte("42"), nil, 0, true)
	if want := 3*setEntryHdrLen + 5 + 0 + 2; len(region) != want {
		t.Fatalf("encoded region is %d bytes, want %d", len(region), want)
	}

	it := hashEntryIter{p: region, valless: true}
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
	it = hashEntryIter{p: bad, valless: true}
	if _, _, _, _, err := it.next(); err == nil {
		t.Fatal("reserved eflags bits decoded without error")
	}

	short := region[:len(region)-1]
	it = hashEntryIter{p: short, valless: true}
	var err error
	for err == nil {
		var ok bool
		_, _, _, ok, err = it.next()
		if !ok && err == nil {
			t.Fatal("truncated region walked off the end without error")
		}
	}

	if got := hashEntrySize(5, 0, 0, true); got != setEntryHdrLen+5 {
		t.Fatalf("hashEntrySize valless = %d, want %d", got, setEntryHdrLen+5)
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
	seg := appendHashSegPayload(nil, entries, true)
	n, minExp, ok := SegCounts(seg)
	if !ok || n != 2 || minExp != 0 {
		t.Fatalf("SegCounts(set seg) = (%d, %d, %v), want (2, 0, true)", n, minExp, ok)
	}
}
