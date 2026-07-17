package sqlo1

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// listRig is the inline-tier test rig: one Tiered over the recording
// store, the list layer, and the string layer beside it for the
// cross-type doors.
type listRig struct {
	t  *testing.T
	rs *recordingStore
	tr *Tiered
	l  *List
	s  *Str
}

func newListRig(t *testing.T) *listRig {
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
	return &listRig{t: t, rs: rs, tr: tr, l: NewList(tr), s: s}
}

// reopen builds a fresh runtime over the same store, the cold view a
// restart would see.
func (r *listRig) reopen() *List {
	r.t.Helper()
	tr := NewTiered(r.rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     12,
		NowMs:    func() int64 { return 1 << 41 },
	})
	return NewList(tr)
}

func (r *listRig) push(key string, left bool, elems ...string) int64 {
	r.t.Helper()
	bs := make([][]byte, len(elems))
	for i, e := range elems {
		bs[i] = []byte(e)
	}
	n, err := r.l.Push(context.Background(), []byte(key), left, false, bs...)
	if err != nil {
		r.t.Fatalf("Push(%q, left=%v): %v", key, left, err)
	}
	return n
}

func (r *listRig) pop(key string, left bool, count int) []string {
	r.t.Helper()
	vals, ok, err := r.l.Pop(context.Background(), []byte(key), left, count)
	if err != nil {
		r.t.Fatalf("Pop(%q, left=%v, %d): %v", key, left, count, err)
	}
	if !ok {
		return nil
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = string(v)
	}
	return out
}

// drain walks a list into strings without mutating it, via the decoded
// inline root.
func (r *listRig) elems(key string) []string {
	r.t.Helper()
	v, root, _, ok, err := r.tr.LookupEntry(context.Background(), []byte(key))
	if err != nil || !ok || !root {
		r.t.Fatalf("LookupEntry(%q) = root=%v ok=%v err=%v", key, root, ok, err)
	}
	li, err := decodeListInline(v)
	if err != nil {
		r.t.Fatalf("decode %q: %v", key, err)
	}
	var out []string
	it := listElemIter{p: li.elems}
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		out = append(out, string(e))
	}
	return out
}

func TestListInlineCodec(t *testing.T) {
	build := func(elems ...string) []byte {
		b := grow(nil, listInlineHdrLen)
		for _, e := range elems {
			b = appendListElem(b, []byte(e))
		}
		putListInlineHdr(b, len(elems))
		return b
	}

	for _, elems := range [][]string{
		{"a"},
		{"", "x", strings.Repeat("y", 500), ""},
		{"one", "two", "three"},
	} {
		li, err := decodeListInline(build(elems...))
		if err != nil {
			t.Fatalf("decode %v: %v", elems, err)
		}
		if li.count != len(elems) {
			t.Fatalf("count %d want %d", li.count, len(elems))
		}
		it := listElemIter{p: li.elems}
		for i, want := range elems {
			e, ok := it.next()
			if !ok || !bytes.Equal(e, []byte(want)) {
				t.Fatalf("element %d = %q, %v, want %q", i, e, ok, want)
			}
		}
		if _, ok := it.next(); ok {
			t.Fatal("iterator walked past the count")
		}
	}

	oversize := grow(nil, listInlineHdrLen)
	oversize = appendListElem(oversize, bytes.Repeat([]byte{'x'}, listInlineMax))
	putListInlineHdr(oversize, 1)

	corrupt := map[string][]byte{
		"empty":     {},
		"short hdr": {listSubInline, 0},
		"bad sub":   build("a", "b")[1:],
		"noded sub": func() []byte {
			b := build("a")
			b[0] = listSubNoded
			return b
		}(),
		"reserved lflags": func() []byte {
			b := build("a")
			b[1] = 1
			return b
		}(),
		"count zero": func() []byte {
			b := build("a")
			binary.LittleEndian.PutUint16(b[2:], 0)
			return b
		}(),
		"count high": func() []byte {
			b := build("a")
			binary.LittleEndian.PutUint16(b[2:], listInlineMaxCount+1)
			return b
		}(),
		"count mismatch": func() []byte {
			b := build("a", "b")
			binary.LittleEndian.PutUint16(b[2:], 1)
			return b
		}(),
		"torn element header": build("a", "b")[:listInlineHdrLen+2],
		"torn element bytes": func() []byte {
			b := build("a", "bcd")
			return b[:len(b)-2]
		}(),
		"oversize payload": oversize,
	}
	for name, p := range corrupt {
		if _, err := decodeListInline(p); err == nil {
			t.Errorf("%s: corrupt inline root decoded cleanly", name)
		}
	}
}

func TestListInlineDequeOps(t *testing.T) {
	r := newListRig(t)
	ctx := context.Background()

	// Missing-key probes.
	if vals, ok, err := r.l.Pop(ctx, []byte("q"), true, 1); err != nil || ok || vals != nil {
		t.Fatalf("Pop of a missing key = %v, %v, %v", vals, ok, err)
	}
	if n, err := r.l.Len(ctx, []byte("q")); err != nil || n != 0 {
		t.Fatalf("Len of a missing key = %d, %v", n, err)
	}

	// A multi-element left push lands one at a time, so it reads back
	// reversed; a right push keeps argument order.
	if n := r.push("q", true, "a", "b", "c"); n != 3 {
		t.Fatalf("LPUSH a b c = %d, want 3", n)
	}
	if n := r.push("q", false, "x", "y"); n != 5 {
		t.Fatalf("RPUSH x y = %d, want 5", n)
	}
	if got := r.elems("q"); !slices.Equal(got, []string{"c", "b", "a", "x", "y"}) {
		t.Fatalf("list order = %v", got)
	}
	if n, err := r.l.Len(ctx, []byte("q")); err != nil || n != 5 {
		t.Fatalf("Len = %d, %v, want 5", n, err)
	}

	// Pops come off in pop order: a right pop reads tail first.
	if got := r.pop("q", true, 1); !slices.Equal(got, []string{"c"}) {
		t.Fatalf("LPOP 1 = %v", got)
	}
	if got := r.pop("q", false, 2); !slices.Equal(got, []string{"y", "x"}) {
		t.Fatalf("RPOP 2 = %v", got)
	}
	if got := r.pop("q", true, 0); len(got) != 0 || got == nil {
		t.Fatalf("LPOP 0 on a live key = %v, want an empty non-nil reply", got)
	}

	// The X gate: a missing key stays missing.
	if n, err := r.l.Push(ctx, []byte("ghost"), true, true, []byte("e")); err != nil || n != 0 {
		t.Fatalf("LPUSHX on a missing key = %d, %v", n, err)
	}
	if exists, _, err := r.s.Entry(ctx, []byte("ghost")); err != nil || exists {
		t.Fatalf("LPUSHX created the key: %v, %v", exists, err)
	}
	if n, err := r.l.Push(ctx, []byte("q"), false, true, []byte("z")); err != nil || n != 3 {
		t.Fatalf("RPUSHX on a live key = %d, %v, want 3", n, err)
	}

	// Empty elements are legal list members.
	r.push("q", false, "")
	if got := r.pop("q", false, 1); !slices.Equal(got, []string{""}) {
		t.Fatalf("RPOP of an empty element = %v", got)
	}

	// Cold path: drain, evict, read through the store.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	r.tr.EvictAllForTest()
	if n, err := r.l.Len(ctx, []byte("q")); err != nil || n != 3 {
		t.Fatalf("cold Len = %d, %v, want 3", n, err)
	}

	// A reopened runtime sees the same list.
	l2 := r.reopen()
	if n, err := l2.Len(ctx, []byte("q")); err != nil || n != 3 {
		t.Fatalf("reopened Len = %d, %v, want 3", n, err)
	}

	// A cold pop reads through the store and rewrites the root.
	if got := r.pop("q", true, 1); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("cold LPOP = %v", got)
	}

	// An over-count pop empties the list and deletes the key.
	if got := r.pop("q", true, 10); !slices.Equal(got, []string{"a", "z"}) {
		t.Fatalf("draining LPOP = %v", got)
	}
	if exists, _, err := r.s.Entry(ctx, []byte("q")); err != nil || exists {
		t.Fatalf("empty list still exists: %v, %v", exists, err)
	}
}

func TestListInlineThresholds(t *testing.T) {
	r := newListRig(t)
	ctx := context.Background()

	// Count threshold: 128 elements stay inline, the 129th is the
	// noded slice's seam.
	for i := range listInlineMaxCount {
		r.push("counts", false, fmt.Sprintf("e%03d", i))
	}
	if enc, _, _ := r.l.Encoding(ctx, []byte("counts")); enc != "listpack" {
		t.Fatalf("encoding at the count ceiling = %q, want listpack", enc)
	}
	if _, err := r.l.Push(ctx, []byte("counts"), false, false, []byte("one-more")); !errors.Is(err, errListNoded) {
		t.Fatalf("element 129 = %v, want errListNoded", err)
	}
	// The refused push wrote nothing.
	if n, err := r.l.Len(ctx, []byte("counts")); err != nil || n != listInlineMaxCount {
		t.Fatalf("Len after the refused push = %d, %v, want %d", n, err, listInlineMaxCount)
	}
	if got := r.pop("counts", false, 1); !slices.Equal(got, []string{"e127"}) {
		t.Fatalf("tail after the refused push = %v", got)
	}

	// Size threshold, pinned to the byte: one element lands the payload
	// exactly on listInlineMax, and the smallest possible second push
	// (an empty element, four header bytes) goes over.
	exact := strings.Repeat("x", listInlineMax-listInlineHdrLen-listElemHdrLen)
	if n := r.push("sizes", false, exact); n != 1 {
		t.Fatalf("boundary push = %d, want 1", n)
	}
	if enc, _, _ := r.l.Encoding(ctx, []byte("sizes")); enc != "listpack" {
		t.Fatalf("encoding at exactly the cap = %q, want listpack", enc)
	}
	if _, err := r.l.Push(ctx, []byte("sizes"), false, false, []byte("")); !errors.Is(err, errListNoded) {
		t.Fatalf("size-crossing push = %v, want errListNoded", err)
	}
	if got := r.pop("sizes", true, 1); !slices.Equal(got, []string{exact}) {
		t.Fatalf("boundary element damaged by the refused push")
	}

	// A fresh key with one oversized element skips inline entirely, so
	// it is the noded slice's seam too, and the key stays absent.
	big := [][]byte{bytes.Repeat([]byte{'x'}, listInlineMax)}
	if _, err := r.l.Push(ctx, []byte("fresh"), false, false, big...); !errors.Is(err, errListNoded) {
		t.Fatalf("oversized fresh push = %v, want errListNoded", err)
	}
	if exists, _, err := r.s.Entry(ctx, []byte("fresh")); err != nil || exists {
		t.Fatalf("refused fresh push created the key: %v, %v", exists, err)
	}
}

func TestListWrongType(t *testing.T) {
	r := newListRig(t)
	ctx := context.Background()

	if err := r.s.Set(ctx, []byte("str"), []byte("plain")); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Set(ctx, []byte("rope"), bytes.Repeat([]byte{'r'}, 9<<10)); err != nil {
		t.Fatal(err)
	}
	r.push("list", false, "e")

	for _, key := range []string{"str", "rope"} {
		if _, err := r.l.Push(ctx, []byte(key), true, false, []byte("e")); !errors.Is(err, ErrWrongType) {
			t.Errorf("Push on %s = %v, want ErrWrongType", key, err)
		}
		if _, _, err := r.l.Pop(ctx, []byte(key), true, 1); !errors.Is(err, ErrWrongType) {
			t.Errorf("Pop on %s = %v, want ErrWrongType", key, err)
		}
		if _, err := r.l.Len(ctx, []byte(key)); !errors.Is(err, ErrWrongType) {
			t.Errorf("Len on %s = %v, want ErrWrongType", key, err)
		}
		if _, _, err := r.l.Encoding(ctx, []byte(key)); !errors.Is(err, ErrWrongType) {
			t.Errorf("Encoding on %s = %v, want ErrWrongType", key, err)
		}
	}

	// The string read doors bounce off a list root.
	if _, _, err := r.s.Get(ctx, []byte("list")); !errors.Is(err, ErrWrongType) {
		t.Errorf("GET on a list = %v, want ErrWrongType", err)
	}
	if _, err := r.s.Append(ctx, []byte("list"), []byte("x")); !errors.Is(err, ErrWrongType) {
		t.Errorf("APPEND on a list = %v, want ErrWrongType", err)
	}
	if _, err := r.s.IncrBy(ctx, []byte("list"), 1); !errors.Is(err, ErrWrongType) {
		t.Errorf("INCR on a list = %v, want ErrWrongType", err)
	}

	// MGET treats another type as a miss, never an error.
	got := []string{}
	err := r.s.MGet(ctx, [][]byte{[]byte("str"), []byte("list")}, func(v []byte, ok bool) {
		if ok {
			got = append(got, string(v))
		} else {
			got = append(got, "<nil>")
		}
	})
	if err != nil {
		t.Fatalf("MGET across types: %v", err)
	}
	if !slices.Equal(got, []string{"plain", "<nil>"}) {
		t.Fatalf("MGET across types = %v", got)
	}

	// SET and DEL take the key over: an inline list is planeless, so
	// the overwrite is a plain record write.
	if err := r.s.Set(ctx, []byte("list"), []byte("now-a-string")); err != nil {
		t.Fatalf("SET over a list: %v", err)
	}
	if v, ok, err := r.s.Get(ctx, []byte("list")); err != nil || !ok || string(v) != "now-a-string" {
		t.Fatalf("GET after takeover = %q, %v, %v", v, ok, err)
	}
	r.push("list2", false, "e")
	if dead, err := r.s.Del(ctx, []byte("list2")); err != nil || !dead {
		t.Fatalf("DEL of a list = %v, %v", dead, err)
	}
}

// nodedListStub hand-builds a root with the noded sub and the shared
// planed prefix; the layout lands with the noded slice, but the
// sniffer, the encoding answer, and the takeover door see it today.
func nodedListStub(rooth uint64) []byte {
	b := make([]byte, 32)
	b[0] = listSubNoded
	binary.LittleEndian.PutUint32(b[4:], 1)
	binary.LittleEndian.PutUint64(b[8:], rooth)
	return b
}

func TestListEncoding(t *testing.T) {
	r := newListRig(t)
	ctx := context.Background()

	if _, ok, err := r.l.Encoding(ctx, []byte("missing")); ok || err != nil {
		t.Fatalf("encoding of a missing key: %v, %v", ok, err)
	}
	r.push("q", false, "e")
	if enc, ok, _ := r.l.Encoding(ctx, []byte("q")); !ok || enc != "listpack" {
		t.Fatalf("inline encoding = %q, %v, want listpack", enc, ok)
	}

	// A noded root answers quicklist, and every list op on it is the
	// noded slice's seam.
	if err := r.tr.Set(ctx, []byte("noded"), nodedListStub(0x1234), TagList|TagRoot); err != nil {
		t.Fatal(err)
	}
	if enc, ok, _ := r.l.Encoding(ctx, []byte("noded")); !ok || enc != "quicklist" {
		t.Fatalf("noded encoding = %q, %v, want quicklist", enc, ok)
	}
	if _, err := r.l.Push(ctx, []byte("noded"), true, false, []byte("e")); !errors.Is(err, errListNoded) {
		t.Fatalf("Push on a noded root = %v, want errListNoded", err)
	}
	if _, _, err := r.l.Pop(ctx, []byte("noded"), true, 1); !errors.Is(err, errListNoded) {
		t.Fatalf("Pop on a noded root = %v, want errListNoded", err)
	}
	if _, err := r.l.Len(ctx, []byte("noded")); !errors.Is(err, errListNoded) {
		t.Fatalf("Len on a noded root = %v, want errListNoded", err)
	}

	// The takeover door reads the shared planed prefix and retires the
	// plane, so SET over a noded root already works.
	if err := r.s.Set(ctx, []byte("noded"), []byte("taken")); err != nil {
		t.Fatalf("SET over a noded root: %v", err)
	}
	if v, ok, err := r.s.Get(ctx, []byte("noded")); err != nil || !ok || string(v) != "taken" {
		t.Fatalf("GET after noded takeover = %q, %v, %v", v, ok, err)
	}
}

func TestListKeyTTLSurvivesWrites(t *testing.T) {
	r := newListRig(t)
	ctx := context.Background()

	r.push("q", false, "a")
	const at = int64(1<<41) + 60_000
	if ok, err := r.tr.ExpireAt(ctx, []byte("q"), at); err != nil || !ok {
		t.Fatalf("ExpireAt: %v, %v", ok, err)
	}
	r.push("q", false, "b")
	if _, _, expMs, ok, err := r.tr.LookupEntry(ctx, []byte("q")); err != nil || !ok || expMs != at {
		t.Fatalf("expiry after hot push = %d, %v, %v, want %d", expMs, ok, err, at)
	}

	// The stamp survives a write that pulls the key back through a
	// fresh hot header (the restamp path).
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	r.tr.EvictAllForTest()
	r.push("q", false, "c")
	if _, _, expMs, ok, err := r.tr.LookupEntry(ctx, []byte("q")); err != nil || !ok || expMs != at {
		t.Fatalf("expiry after cold push = %d, %v, %v, want %d", expMs, ok, err, at)
	}
	if got := r.pop("q", true, 1); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("pop after restamps = %v", got)
	}
	if _, _, expMs, ok, err := r.tr.LookupEntry(ctx, []byte("q")); err != nil || !ok || expMs != at {
		t.Fatalf("expiry after pop = %d, %v, %v, want %d", expMs, ok, err, at)
	}
}
