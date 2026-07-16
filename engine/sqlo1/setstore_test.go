package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// storeMembers reads a stored destination back through SMembers on
// the given runtime, with the emit count pinning exact dedupe.
func storeMembers(t *testing.T, se *Set, key string) (map[string]bool, int) {
	t.Helper()
	got := map[string]bool{}
	n := 0
	if err := se.SMembers(context.Background(), []byte(key), func(int) {}, func(m []byte) {
		got[string(m)] = true
		n++
	}); err != nil {
		t.Fatalf("SMembers(%q): %v", key, err)
	}
	return got, n
}

func TestSInterStoreInline(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()
	for _, m := range []string{"a", "b", "c", "d"} {
		r.sadd("s1", m)
	}
	for _, m := range []string{"b", "c", "e"} {
		r.sadd("s2", m)
	}

	n, err := r.se.SInterStore(ctx, []byte("d"), keyBytes([]string{"s1", "s2"}))
	if err != nil || n != 2 {
		t.Fatalf("SInterStore = %d, %v, want 2", n, err)
	}
	got, cnt := storeMembers(t, r.se, "d")
	wantSet(t, got, cnt, map[string]bool{"b": true, "c": true})
	if enc := r.encoding("d"); enc != "listpack" {
		t.Fatalf("inline string result encodes as %q, want listpack", enc)
	}
	if r.scard("d") != 2 {
		t.Fatalf("SCARD(d) = %d, want 2", r.scard("d"))
	}

	// An all-integer result answers intset, the ladder's inline flag
	// flowing through the build.
	for _, m := range []string{"1", "2", "30"} {
		r.sadd("i1", m)
		r.sadd("i2", m)
	}
	r.sadd("i1", "not-int")
	if _, err := r.se.SInterStore(ctx, []byte("di"), keyBytes([]string{"i1", "i2"})); err != nil {
		t.Fatalf("SInterStore(int): %v", err)
	}
	if enc := r.encoding("di"); enc != "intset" {
		t.Fatalf("all-int result encodes as %q, want intset", enc)
	}

	// The destination can be a source: the build reads the old plane
	// and the root PUT swaps it out.
	n, err = r.se.SInterStore(ctx, []byte("s1"), keyBytes([]string{"s1", "s2"}))
	if err != nil || n != 2 {
		t.Fatalf("SInterStore(dest=source) = %d, %v, want 2", n, err)
	}
	got, cnt = storeMembers(t, r.se, "s1")
	wantSet(t, got, cnt, map[string]bool{"b": true, "c": true})
}

func TestSInterStoreSegmented(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()
	a := rangeSet(t, r, "a", 0, 800)
	b := rangeSet(t, r, "b", 400, 1600)
	want := interOracle(a, b)

	n, err := r.se.SInterStore(ctx, []byte("d"), keyBytes([]string{"a", "b"}))
	if err != nil || n != 400 {
		t.Fatalf("SInterStore = %d, %v, want 400", n, err)
	}
	got, cnt := storeMembers(t, r.se, "d")
	wantSet(t, got, cnt, want)
	if enc := r.encoding("d"); enc != "hashtable" {
		t.Fatalf("segmented result encodes as %q, want hashtable", enc)
	}
	for _, m := range []string{"m00400", "m00799"} {
		if !r.sismember("d", m) {
			t.Fatalf("member %q missing from the stored result", m)
		}
	}
	if r.sismember("d", "m00399") {
		t.Fatal("member m00399 leaked into the intersection")
	}

	// Cold: the stored plane and its fence land before the root.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se := r.reopen()
	got, cnt = storeMembers(t, se, "d")
	wantSet(t, got, cnt, want)
}

func TestSUnionStore(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()
	a := rangeSet(t, r, "a", 0, 600)
	b := rangeSet(t, r, "b", 300, 900)
	r.sadd("tiny", "m00100")
	r.sadd("tiny", "zz-extra")
	want := map[string]bool{"zz-extra": true}
	for m := range a {
		want[m] = true
	}
	for m := range b {
		want[m] = true
	}

	// Mixed representations, a ghost as an empty set, and the overlap
	// deduped exactly (the emit count in storeMembers pins it).
	n, err := r.se.SUnionStore(ctx, []byte("d"), keyBytes([]string{"a", "b", "tiny", "ghost"}))
	if err != nil || n != int64(len(want)) {
		t.Fatalf("SUnionStore = %d, %v, want %d", n, err, len(want))
	}
	got, cnt := storeMembers(t, r.se, "d")
	wantSet(t, got, cnt, want)

	// Duplicate source keys change nothing.
	n, err = r.se.SUnionStore(ctx, []byte("d2"), keyBytes([]string{"a", "a", "a"}))
	if err != nil || n != 600 {
		t.Fatalf("SUnionStore(a, a, a) = %d, %v, want 600", n, err)
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se := r.reopen()
	got, cnt = storeMembers(t, se, "d")
	wantSet(t, got, cnt, want)
}

func TestSDiffStore(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()
	rangeSet(t, r, "a", 0, 800)
	rangeSet(t, r, "b", 400, 1600)
	want := map[string]bool{}
	for i := range 400 {
		want[fmt.Sprintf("m%05d", i)] = true
	}

	n, err := r.se.SDiffStore(ctx, []byte("d"), keyBytes([]string{"a", "b"}))
	if err != nil || n != 400 {
		t.Fatalf("SDiffStore = %d, %v, want 400", n, err)
	}
	got, cnt := storeMembers(t, r.se, "d")
	wantSet(t, got, cnt, want)

	// An empty result deletes the destination.
	n, err = r.se.SDiffStore(ctx, []byte("d"), keyBytes([]string{"a", "a"}))
	if err != nil || n != 0 {
		t.Fatalf("SDiffStore(a, a) = %d, %v, want 0", n, err)
	}
	if r.scard("d") != 0 {
		t.Fatal("empty diff left the destination behind")
	}
	if _, _, _, ok, err := r.tr.LookupEntry(ctx, []byte("d")); err != nil || ok {
		t.Fatalf("destination survives an empty result: ok=%v err=%v", ok, err)
	}
}

func TestSetStorePaged(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()
	long := func(i int) string {
		return fmt.Sprintf("f%05d-%054d", i, 0)
	}
	for i := range 16000 {
		r.sadd("big", long(i))
	}

	// A one-source union is the copy path, and 16000 long members push
	// the built fence past the flat cap, so the destination pages.
	n, err := r.se.SUnionStore(ctx, []byte("d"), keyBytes([]string{"big"}))
	if err != nil || n != 16000 {
		t.Fatalf("SUnionStore(big) = %d, %v, want 16000", n, err)
	}
	st, _, _, err := r.se.h.stateOf(ctx, []byte("d"))
	if err != nil || st != hashSegState {
		t.Fatalf("stateOf(d) = %v, %v", st, err)
	}
	if !r.se.h.segRoot.paged {
		t.Fatal("built destination is not paged; the fixture lost its point")
	}
	for i := 0; i < 16000; i += 1777 {
		if !r.sismember("d", long(i)) {
			t.Fatalf("member %q missing from the paged build", long(i))
		}
	}
	if r.sismember("d", "not-there") {
		t.Fatal("phantom member in the paged build")
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se := r.reopen()
	nn, err := se.SCard(ctx, []byte("d"))
	if err != nil || nn != 16000 {
		t.Fatalf("cold SCard(d) = %d, %v, want 16000", nn, err)
	}
	for i := 500; i < 16000; i += 3200 {
		ok, err := se.SIsMember(ctx, []byte("d"), []byte(long(i)))
		if err != nil || !ok {
			t.Fatalf("cold SIsMember(%q) = %v, %v", long(i), ok, err)
		}
	}
}

func TestSetStoreDoors(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()
	r.sadd("s", "a")
	r.sadd("s", "b")
	if err := r.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// A wrong-type source errors and the destination is untouched.
	r.sadd("keep", "x")
	if _, err := r.se.SUnionStore(ctx, []byte("keep"), keyBytes([]string{"s", "str"})); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SUnionStore(str source) = %v, want ErrWrongType", err)
	}
	if !r.sismember("keep", "x") {
		t.Fatal("failed store touched the destination")
	}
	if _, err := r.se.SDiffStore(ctx, []byte("d"), keyBytes([]string{"str", "s"})); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SDiffStore(str first) = %v, want ErrWrongType", err)
	}

	// SINTERSTORE's absent short circuit masks a later wrong type and
	// still deletes the destination.
	r.sadd("gone", "x")
	n, err := r.se.SInterStore(ctx, []byte("gone"), keyBytes([]string{"ghost", "str"}))
	if err != nil || n != 0 {
		t.Fatalf("SInterStore(ghost, str) = %d, %v, want the masked empty result", n, err)
	}
	if r.scard("gone") != 0 {
		t.Fatal("empty intersection left the destination behind")
	}
	// The wrong type before the absent key errors.
	if _, err := r.se.SInterStore(ctx, []byte("d"), keyBytes([]string{"str", "ghost"})); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SInterStore(str first) = %v, want ErrWrongType", err)
	}

	// A destination of any type is overwritten without a type check.
	if n, err := r.se.SUnionStore(ctx, []byte("str"), keyBytes([]string{"s"})); err != nil || n != 2 {
		t.Fatalf("SUnionStore over a string = %d, %v, want 2", n, err)
	}
	got, cnt := storeMembers(t, r.se, "str")
	wantSet(t, got, cnt, map[string]bool{"a": true, "b": true})

	// A segmented destination is overwritten through the plane bump.
	rangeSet(t, r, "wide", 0, 1200)
	if n, err := r.se.SUnionStore(ctx, []byte("wide"), keyBytes([]string{"s"})); err != nil || n != 2 {
		t.Fatalf("SUnionStore over a segmented set = %d, %v, want 2", n, err)
	}
	got, cnt = storeMembers(t, r.se, "wide")
	wantSet(t, got, cnt, map[string]bool{"a": true, "b": true})
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se := r.reopen()
	got, cnt = storeMembers(t, se, "wide")
	wantSet(t, got, cnt, map[string]bool{"a": true, "b": true})

	// The store is a fresh object: an old TTL on the destination does
	// not survive it, Redis's STORE rule.
	r.sadd("ttl", "old")
	if _, err := r.tr.ExpireAt(ctx, []byte("ttl"), (1<<41)+60_000); err != nil {
		t.Fatalf("ExpireAt: %v", err)
	}
	if n, err := r.se.SUnionStore(ctx, []byte("ttl"), keyBytes([]string{"s"})); err != nil || n != 2 {
		t.Fatalf("SUnionStore over a TTL key = %d, %v, want 2", n, err)
	}
	if _, _, expMs, ok, err := r.tr.LookupEntry(ctx, []byte("ttl")); err != nil || !ok || expMs != 0 {
		t.Fatalf("stored destination keeps expMs=%d ok=%v err=%v, want no TTL", expMs, ok, err)
	}
}
