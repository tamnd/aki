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
	l, err := NewList(tr, ListConfig{})
	if err != nil {
		t.Fatalf("NewList: %v", err)
	}
	return &listRig{t: t, rs: rs, tr: tr, l: l, s: s}
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
	l, err := NewList(tr, ListConfig{})
	if err != nil {
		r.t.Fatalf("NewList: %v", err)
	}
	return l
}

// nodedRoot decodes key's root as a noded list root, for tests that
// pin fence shapes.
func (r *listRig) nodedRoot(key string) listNodeRoot {
	r.t.Helper()
	v, root, _, ok, err := r.tr.LookupEntry(context.Background(), []byte(key))
	if err != nil || !ok || !root {
		r.t.Fatalf("LookupEntry(%q) = root=%v ok=%v err=%v", key, root, ok, err)
	}
	nr, err := decodeListNodeRoot(v, nil, nil)
	if err != nil {
		r.t.Fatalf("decode noded root %q: %v", key, err)
	}
	return nr
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

func TestListUpgradeThresholds(t *testing.T) {
	r := newListRig(t)
	ctx := context.Background()

	// Count threshold: 128 elements stay inline, the 129th upgrades to
	// the noded layout and the data survives the move in order.
	want := []string{}
	for i := range listInlineMaxCount {
		e := fmt.Sprintf("e%03d", i)
		r.push("counts", false, e)
		want = append(want, e)
	}
	if enc, _, _ := r.l.Encoding(ctx, []byte("counts")); enc != "listpack" {
		t.Fatalf("encoding at the count ceiling = %q, want listpack", enc)
	}
	if n := r.push("counts", false, "one-more"); n != listInlineMaxCount+1 {
		t.Fatalf("element 129 = %d, want %d", n, listInlineMaxCount+1)
	}
	want = append(want, "one-more")
	if enc, _, _ := r.l.Encoding(ctx, []byte("counts")); enc != "quicklist" {
		t.Fatalf("encoding past the count ceiling = %q, want quicklist", enc)
	}
	if n, err := r.l.Len(ctx, []byte("counts")); err != nil || n != int64(len(want)) {
		t.Fatalf("Len after the upgrade = %d, %v, want %d", n, err, len(want))
	}
	if got := r.pop("counts", true, len(want)); !slices.Equal(got, want) {
		t.Fatalf("drain after the upgrade = %v, want %v", got, want)
	}
	if exists, _, err := r.s.Entry(ctx, []byte("counts")); err != nil || exists {
		t.Fatalf("drained list still exists: %v, %v", exists, err)
	}

	// Size threshold, pinned to the byte: one element lands the payload
	// exactly on listInlineMax and stays inline; the smallest possible
	// second push (an empty element, four header bytes) upgrades.
	exact := strings.Repeat("x", listInlineMax-listInlineHdrLen-listElemHdrLen)
	if n := r.push("sizes", false, exact); n != 1 {
		t.Fatalf("boundary push = %d, want 1", n)
	}
	if enc, _, _ := r.l.Encoding(ctx, []byte("sizes")); enc != "listpack" {
		t.Fatalf("encoding at exactly the cap = %q, want listpack", enc)
	}
	if n := r.push("sizes", false, ""); n != 2 {
		t.Fatalf("size-crossing push = %d, want 2", n)
	}
	if enc, _, _ := r.l.Encoding(ctx, []byte("sizes")); enc != "quicklist" {
		t.Fatalf("encoding past the byte cap = %q, want quicklist", enc)
	}
	if got := r.pop("sizes", false, 1); !slices.Equal(got, []string{""}) {
		t.Fatalf("RPOP after the byte-cap upgrade = %v", got)
	}
	if got := r.pop("sizes", true, 1); !slices.Equal(got, []string{exact}) {
		t.Fatalf("boundary element damaged by the upgrade")
	}
	if exists, _, err := r.s.Entry(ctx, []byte("sizes")); err != nil || exists {
		t.Fatalf("drained list still exists: %v, %v", exists, err)
	}

	// A fresh key with one oversized element skips inline entirely and
	// lands noded in one write.
	big := strings.Repeat("y", listInlineMax)
	if n := r.push("fresh", false, big); n != 1 {
		t.Fatalf("oversized fresh push = %d, want 1", n)
	}
	if enc, _, _ := r.l.Encoding(ctx, []byte("fresh")); enc != "quicklist" {
		t.Fatalf("oversized fresh encoding = %q, want quicklist", enc)
	}
	if got := r.pop("fresh", false, 1); !slices.Equal(got, []string{big}) {
		t.Fatalf("oversized element damaged by the fresh upgrade")
	}
}

// TestListNodeCuts pins the node cut rule to the byte: a node packs to
// exactly listNodeMax without cutting, and the next element cuts.
func TestListNodeCuts(t *testing.T) {
	r := newListRig(t)

	// One element sized so header plus two entries is exactly
	// listNodeMax, then an empty element: 4 + (4+4020) + (4+0) = 4032.
	a := strings.Repeat("a", listNodeMax-listNodeHdrLen-2*listElemHdrLen)
	if n := r.push("q", false, a, ""); n != 2 {
		t.Fatalf("push = %d, want 2", n)
	}
	nr := r.nodedRoot("q")
	if len(nr.fence) != 1 || nr.fence[0].count != 2 {
		t.Fatalf("exactly-full node split: fence = %+v", nr.fence)
	}

	// The node is exactly full, so the smallest push cuts a fresh one.
	r.push("q", false, "")
	nr = r.nodedRoot("q")
	if len(nr.fence) != 2 || nr.fence[0].count != 2 || nr.fence[1].count != 1 {
		t.Fatalf("cut off a full node: fence = %+v", nr.fence)
	}
	if nr.count != 3 {
		t.Fatalf("root count = %d, want 3", nr.count)
	}

	// An element over listNodeMax gets a node of its own.
	huge := strings.Repeat("h", listNodeMax+100)
	r.push("q", false, huge)
	nr = r.nodedRoot("q")
	if len(nr.fence) != 3 || nr.fence[2].count != 1 {
		t.Fatalf("oversize element node: fence = %+v", nr.fence)
	}
	if got := r.pop("q", false, 1); !slices.Equal(got, []string{huge}) {
		t.Fatalf("oversize element damaged: got %d bytes", len(got[0]))
	}
	// Dropping the oversize element drops its node whole.
	nr = r.nodedRoot("q")
	if len(nr.fence) != 2 || nr.count != 3 {
		t.Fatalf("fence after the oversize pop = %+v, count %d", nr.fence, nr.count)
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

	// Past the thresholds a real noded list answers quicklist, and it
	// never downgrades: Redis converts a shrunk quicklist back to
	// listpack, this ladder does not, the compat section's divergence.
	for i := range listInlineMaxCount + 1 {
		r.push("noded", false, fmt.Sprintf("e%03d", i))
	}
	if enc, ok, _ := r.l.Encoding(ctx, []byte("noded")); !ok || enc != "quicklist" {
		t.Fatalf("noded encoding = %q, %v, want quicklist", enc, ok)
	}
	r.pop("noded", true, listInlineMaxCount)
	if enc, ok, _ := r.l.Encoding(ctx, []byte("noded")); !ok || enc != "quicklist" {
		t.Fatalf("shrunk noded encoding = %q, %v, want quicklist (no downgrade)", enc, ok)
	}

	// The takeover door reads the shared planed prefix and retires the
	// plane, so SET over a noded root works; the recreate starts
	// inline under the fresh rootgen.
	if err := r.s.Set(ctx, []byte("noded"), []byte("taken")); err != nil {
		t.Fatalf("SET over a noded root: %v", err)
	}
	if v, ok, err := r.s.Get(ctx, []byte("noded")); err != nil || !ok || string(v) != "taken" {
		t.Fatalf("GET after noded takeover = %q, %v, %v", v, ok, err)
	}
	if dead, err := r.s.Del(ctx, []byte("noded")); err != nil || !dead {
		t.Fatalf("DEL after takeover = %v, %v", dead, err)
	}
	r.push("noded", false, "fresh")
	if enc, ok, _ := r.l.Encoding(ctx, []byte("noded")); !ok || enc != "listpack" {
		t.Fatalf("recreate encoding = %q, %v, want listpack", enc, ok)
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

	// The stamp survives the upgrade to nodes and noded writes.
	bs := make([][]byte, listInlineMaxCount+10)
	for i := range bs {
		bs[i] = []byte(fmt.Sprintf("u%03d", i))
	}
	if _, err := r.l.Push(ctx, []byte("q"), false, false, bs...); err != nil {
		t.Fatalf("upgrading push: %v", err)
	}
	if _, _, expMs, ok, err := r.tr.LookupEntry(ctx, []byte("q")); err != nil || !ok || expMs != at {
		t.Fatalf("expiry after the upgrade = %d, %v, %v, want %d", expMs, ok, err, at)
	}
	r.pop("q", false, 3)
	if _, _, expMs, ok, err := r.tr.LookupEntry(ctx, []byte("q")); err != nil || !ok || expMs != at {
		t.Fatalf("expiry after a noded pop = %d, %v, %v, want %d", expMs, ok, err, at)
	}
}

// listModel is the reference deque the noded tests compare against.
type listModel []string

func (m *listModel) push(left bool, elems ...string) {
	for _, e := range elems {
		if left {
			*m = append(listModel{e}, *m...)
		} else {
			*m = append(*m, e)
		}
	}
}

func (m *listModel) pop(left bool, count int) []string {
	k := min(count, len(*m))
	out := make([]string, 0, k)
	for range k {
		if left {
			out = append(out, (*m)[0])
			*m = (*m)[1:]
		} else {
			out = append(out, (*m)[len(*m)-1])
			*m = (*m)[:len(*m)-1]
		}
	}
	return out
}

func TestListNodedDeque(t *testing.T) {
	r := newListRig(t)
	ctx := context.Background()
	var m listModel

	// Build a list spanning several nodes, one push per element the
	// way a queue producer would.
	for i := range 300 {
		e := fmt.Sprintf("e%04d", i)
		r.push("q", false, e)
		m.push(false, e)
	}
	if enc, _, _ := r.l.Encoding(ctx, []byte("q")); enc != "quicklist" {
		t.Fatalf("encoding = %q, want quicklist", enc)
	}
	nr := r.nodedRoot("q")
	if len(nr.fence) < 3 {
		t.Fatalf("300 elements landed only %d nodes", len(nr.fence))
	}
	if n, err := r.l.Len(ctx, []byte("q")); err != nil || n != 300 {
		t.Fatalf("Len = %d, %v, want 300", n, err)
	}

	// Edge pops from both ends, then a pop spanning whole nodes plus a
	// partial one.
	if got := r.pop("q", true, 5); !slices.Equal(got, m.pop(true, 5)) {
		t.Fatalf("LPOP 5 = %v", got)
	}
	if got := r.pop("q", false, 3); !slices.Equal(got, m.pop(false, 3)) {
		t.Fatalf("RPOP 3 = %v", got)
	}
	if got := r.pop("q", true, 200); !slices.Equal(got, m.pop(true, 200)) {
		t.Fatalf("multi-node LPOP 200 diverged from the model")
	}
	nr = r.nodedRoot("q")
	if nr.count != uint64(len(m)) {
		t.Fatalf("root count = %d, model %d", nr.count, len(m))
	}

	// Cold path and a reopened runtime; the reopen check runs before
	// any cold mutation, which would sit hot in this runtime only.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	r.tr.EvictAllForTest()
	if n, err := r.l.Len(ctx, []byte("q")); err != nil || n != int64(len(m)) {
		t.Fatalf("cold Len = %d, %v, want %d", n, err, len(m))
	}
	l2 := r.reopen()
	if n, err := l2.Len(ctx, []byte("q")); err != nil || n != int64(len(m)) {
		t.Fatalf("reopened Len = %d, %v, want %d", n, err, len(m))
	}
	if got := r.pop("q", false, 40); !slices.Equal(got, m.pop(false, 40)) {
		t.Fatalf("cold RPOP 40 diverged from the model")
	}

	// The drain empties the list, deletes the key, and the recreate
	// starts inline.
	if got := r.pop("q", true, 1000); !slices.Equal(got, m.pop(true, 1000)) {
		t.Fatalf("draining LPOP diverged from the model")
	}
	if exists, _, err := r.s.Entry(ctx, []byte("q")); err != nil || exists {
		t.Fatalf("drained list still exists: %v, %v", exists, err)
	}
	r.push("q", false, "again")
	if enc, _, _ := r.l.Encoding(ctx, []byte("q")); enc != "listpack" {
		t.Fatalf("recreate encoding = %q, want listpack", enc)
	}
}

func TestListNodedPushBatching(t *testing.T) {
	r := newListRig(t)

	big := make([]string, 300)
	for i := range big {
		big[i] = fmt.Sprintf("b%04d", i)
	}

	// One right push of 300 elements upgrades straight into several
	// nodes and keeps argument order.
	var m listModel
	r.push("r", false, big...)
	m.push(false, big...)
	if got := r.pop("r", true, 300); !slices.Equal(got, []string(m)) {
		t.Fatalf("multi-node RPUSH drain diverged")
	}

	// One left push of 300 lands one at a time, so it reads back
	// reversed, across the amendment and every cut.
	m = nil
	r.push("l", true, big...)
	m.push(true, big...)
	if got := r.pop("l", true, 300); !slices.Equal(got, []string(m)) {
		t.Fatalf("multi-node LPUSH drain diverged")
	}

	// Batched pushes onto an existing noded list: the edge node amends
	// until full, then fresh nodes cut, on both edges.
	m = nil
	for i := range 200 {
		e := fmt.Sprintf("s%04d", i)
		r.push("q", false, e)
		m.push(false, e)
	}
	r.push("q", false, big[:150]...)
	m.push(false, big[:150]...)
	r.push("q", true, big[150:]...)
	m.push(true, big[150:]...)
	if n, err := r.l.Len(context.Background(), []byte("q")); err != nil || n != int64(len(m)) {
		t.Fatalf("Len = %d, %v, want %d", n, err, len(m))
	}
	if got := r.pop("q", true, len(m)); !slices.Equal(got, []string(m)) {
		t.Fatalf("batched noded pushes diverged from the model")
	}
}

func TestListFenceTransition(t *testing.T) {
	r := newListRig(t)
	ctx := context.Background()

	// Elements sized to own a node each fill the fence to its flat cap.
	e := strings.Repeat("x", listNodeMax-listNodeHdrLen-listElemHdrLen)
	for i := range listFenceMaxNodes {
		if _, err := r.l.Push(ctx, []byte("q"), false, false, []byte(e)); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	nr := r.nodedRoot("q")
	if nr.paged || len(nr.fence) != listFenceMaxNodes {
		t.Fatalf("fence holds %d nodes paged=%v, want %d flat", len(nr.fence), nr.paged, listFenceMaxNodes)
	}

	// The push that cuts past the cap moves the fence into pages, the
	// one-way second rung. The paging oracle owns the layout details;
	// this pins the ladder step at the real caps.
	if _, err := r.l.Push(ctx, []byte("q"), false, false, []byte(e)); err != nil {
		t.Fatalf("transition push: %v", err)
	}
	nr = r.nodedRoot("q")
	if !nr.paged || len(nr.pidx) != pageChunks(listFenceMaxNodes+1) {
		t.Fatalf("root after the transition: paged=%v pages=%d", nr.paged, len(nr.pidx))
	}
	want := int64(listFenceMaxNodes + 1)
	if n, err := r.l.Len(ctx, []byte("q")); err != nil || n != want {
		t.Fatalf("Len after the transition = %d, %v, want %d", n, err, want)
	}
	for i := range want {
		if got := r.pop("q", true, 1); len(got) != 1 || got[0] != e {
			t.Fatalf("pop %d diverged after the transition", i)
		}
	}
	if exists, _, err := r.s.Entry(ctx, []byte("q")); err != nil || exists {
		t.Fatalf("drained transitioned list still exists: %v, %v", exists, err)
	}

	// The upgrade path overshooting the flat cap pages the same way.
	huge := make([][]byte, listFenceMaxNodes+1)
	for i := range huge {
		huge[i] = []byte(e)
	}
	if _, err := r.l.Push(ctx, []byte("fresh"), false, false, huge...); err != nil {
		t.Fatalf("overshooting fresh push: %v", err)
	}
	if !r.nodedRoot("fresh").paged {
		t.Fatal("fresh overshoot did not page")
	}
}

func TestListNodedRootCodec(t *testing.T) {
	rt := listNodeRoot{
		rootgen:   3,
		rooth:     0xabcdef,
		count:     10,
		nextSegid: 7,
		fence: []listFenceEnt{
			{segid: 2, meta: 0x7f, count: 4},
			{segid: 0, count: 5},
			{segid: 6, meta: 1, count: 1},
		},
	}
	enc := appendListNodeRoot(nil, &rt)
	if len(enc) != listNodeRootHdrLen+3*listFenceEntLen {
		t.Fatalf("encoded root is %d bytes", len(enc))
	}
	dec, err := decodeListNodeRoot(enc, nil, nil)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if dec.rootgen != rt.rootgen || dec.rooth != rt.rooth || dec.count != rt.count || dec.nextSegid != rt.nextSegid || !slices.Equal(dec.fence, rt.fence) {
		t.Fatalf("round trip = %+v, want %+v", dec, rt)
	}

	mut := func(f func(b []byte)) []byte {
		b := slices.Clone(enc)
		f(b)
		return b
	}
	corrupt := map[string][]byte{
		"empty":                   {},
		"short hdr":               enc[:listNodeRootHdrLen-1],
		"bad sub":                 mut(func(b []byte) { b[0] = listSubInline }),
		"unknown lflags":          mut(func(b []byte) { b[1] = 2 }),
		"reserved bytes":          mut(func(b []byte) { b[2] = 1 }),
		"rootgen zero":            mut(func(b []byte) { binary.LittleEndian.PutUint32(b[4:], 0) }),
		"count zero":              mut(func(b []byte) { binary.LittleEndian.PutUint64(b[16:], 0) }),
		"node_count zero":         mut(func(b []byte) { binary.LittleEndian.PutUint32(b[32:], 0) }),
		"node_count over the cap": mut(func(b []byte) { binary.LittleEndian.PutUint32(b[32:], uint32(listFenceMaxNodes+1)) }),
		"size mismatch":           enc[:len(enc)-1],
		"segid at next_segid":     mut(func(b []byte) { binary.LittleEndian.PutUint64(b[24:], 2) }),
		"fence entry count zero": mut(func(b []byte) {
			binary.LittleEndian.PutUint32(b[listNodeRootHdrLen+8:], 0)
		}),
		"count sum mismatch": mut(func(b []byte) { binary.LittleEndian.PutUint64(b[16:], 11) }),
	}
	for name, p := range corrupt {
		if _, err := decodeListNodeRoot(p, nil, nil); err == nil {
			t.Errorf("%s: corrupt noded root decoded cleanly", name)
		}
	}

	// The paged form round-trips the page index instead of the fence.
	prt := listNodeRoot{
		rootgen:   4,
		rooth:     0xabcdef,
		count:     9,
		nextSegid: 40,
		paged:     true,
		pidx: []listFenceEnt{
			{segid: 30, count: 4},
			{segid: 31, count: 5},
		},
	}
	penc := appendListNodeRoot(nil, &prt)
	pdec, err := decodeListNodeRoot(penc, nil, nil)
	if err != nil {
		t.Fatalf("paged round trip: %v", err)
	}
	if !pdec.paged || pdec.count != prt.count || pdec.nextSegid != prt.nextSegid ||
		!slices.Equal(pdec.pidx, prt.pidx) || len(pdec.fence) != 0 {
		t.Fatalf("paged round trip = %+v, want %+v", pdec, prt)
	}
	pmut := func(f func(b []byte)) []byte {
		b := slices.Clone(penc)
		f(b)
		return b
	}
	pcorrupt := map[string][]byte{
		"paged sum mismatch":         pmut(func(b []byte) { binary.LittleEndian.PutUint64(b[16:], 10) }),
		"paged pageid at next_segid": pmut(func(b []byte) { binary.LittleEndian.PutUint64(b[24:], 31) }),
		"paged size mismatch":        penc[:len(penc)-1],
	}
	for name, p := range pcorrupt {
		if _, err := decodeListNodeRoot(p, nil, nil); err == nil {
			t.Errorf("%s: corrupt paged root decoded cleanly", name)
		}
	}
}

func TestListNodeCodec(t *testing.T) {
	b := grow(nil, listNodeHdrLen)
	for _, e := range []string{"", "abc", strings.Repeat("z", 700)} {
		b = appendListElem(b, []byte(e))
	}
	putListNodeHdr(b, 3)
	node, err := decodeListNode(b)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if node.n != 3 {
		t.Fatalf("n = %d, want 3", node.n)
	}
	it := listElemIter{p: node.elems}
	for _, want := range []string{"", "abc", strings.Repeat("z", 700)} {
		e, ok := it.next()
		if !ok || string(e) != want {
			t.Fatalf("element = %q, %v, want %q", e, ok, want)
		}
	}

	mut := func(f func(b []byte)) []byte {
		c := slices.Clone(b)
		f(c)
		return c
	}
	corrupt := map[string][]byte{
		"empty":        {},
		"short hdr":    b[:2],
		"n zero":       mut(func(b []byte) { binary.LittleEndian.PutUint16(b, 0) }),
		"n over ecap":  mut(func(b []byte) { binary.LittleEndian.PutUint16(b, listNodeMaxElems+1) }),
		"reserved":     mut(func(b []byte) { b[2] = 1 }),
		"n mismatch":   mut(func(b []byte) { binary.LittleEndian.PutUint16(b, 2) }),
		"torn header":  b[:listNodeHdrLen+2],
		"torn element": b[:len(b)-1],
	}
	for name, p := range corrupt {
		if _, err := decodeListNode(p); err == nil {
			t.Errorf("%s: corrupt node decoded cleanly", name)
		}
	}
}
