package sqlo1

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// hashRig is the inline-tier test rig: one Tiered over the recording
// store, the hash layer, and the string layer beside it for the
// cross-type doors.
type hashRig struct {
	t  *testing.T
	rs *recordingStore
	tr *Tiered
	h  *Hash
	s  *Str
}

func newHashRig(t *testing.T) *hashRig {
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
	return &hashRig{t: t, rs: rs, tr: tr, h: h, s: s}
}

// reopen builds a fresh runtime over the same store, the cold view a
// restart would see.
func (r *hashRig) reopen() *Hash {
	r.t.Helper()
	tr := NewTiered(r.rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     12,
		NowMs:    func() int64 { return 1 << 41 },
	})
	h, err := NewHash(tr, HashConfig{})
	if err != nil {
		r.t.Fatalf("NewHash: %v", err)
	}
	return h
}

func (r *hashRig) hset(key, field, val string) bool {
	r.t.Helper()
	created, err := r.h.HSet(context.Background(), []byte(key), []byte(field), []byte(val))
	if err != nil {
		r.t.Fatalf("HSet(%q, %q): %v", key, field, err)
	}
	return created
}

func (r *hashRig) hget(key, field string) (string, bool) {
	r.t.Helper()
	v, ok, err := r.h.HGet(context.Background(), []byte(key), []byte(field))
	if err != nil {
		r.t.Fatalf("HGet(%q, %q): %v", key, field, err)
	}
	return string(v), ok
}

// fields decodes key's stored inline payload and returns its fields in
// entry order, the iteration-order oracle HGETALL will inherit.
func (r *hashRig) fields(key string) []string {
	r.t.Helper()
	v, root, ok, err := r.tr.Lookup(context.Background(), []byte(key))
	if err != nil || !ok || !root {
		r.t.Fatalf("Lookup(%q): ok=%v root=%v err=%v", key, ok, root, err)
	}
	hi, err := decodeHashInline(v)
	if err != nil {
		r.t.Fatalf("decode inline payload of %q: %v", key, err)
	}
	var out []string
	it := hashEntryIter{p: hi.entries}
	for {
		f, _, _, ok, err := it.next()
		if err != nil {
			r.t.Fatalf("entry walk of %q: %v", key, err)
		}
		if !ok {
			return out
		}
		out = append(out, string(f))
	}
}

func TestHashEntryCodec(t *testing.T) {
	var region []byte
	region = appendHashEntry(region, []byte("plain"), []byte("value"), 0)
	region = appendHashEntry(region, []byte(""), []byte("empty-field"), 0)
	region = appendHashEntry(region, []byte("ttl"), []byte("v"), 12345)
	region = appendHashEntry(region, []byte("big"), bytes.Repeat([]byte{0xAB}, 300), 0)

	type want struct {
		f, v  string
		expMs int64
	}
	wants := []want{
		{"plain", "value", 0},
		{"", "empty-field", 0},
		{"ttl", "v", 12345},
		{"big", strings.Repeat("\xab", 300), 0},
	}
	it := hashEntryIter{p: region}
	for i, w := range wants {
		f, v, expMs, ok, err := it.next()
		if err != nil || !ok {
			t.Fatalf("entry %d: ok=%v err=%v", i, ok, err)
		}
		if string(f) != w.f || string(v) != w.v || expMs != w.expMs {
			t.Fatalf("entry %d = (%q, %q, %d), want (%q, %q, %d)", i, f, v, expMs, w.f, w.v, w.expMs)
		}
	}
	if _, _, _, ok, err := it.next(); ok || err != nil {
		t.Fatalf("iterator past the end: ok=%v err=%v", ok, err)
	}

	corrupt := map[string][]byte{
		"short header":      {0x00, 0x01},
		"reserved eflags":   append([]byte{0x02, 1, 0, 1, 0, 0, 0}, "fv"...),
		"overrun":           {0x00, 0xFF, 0xFF, 1, 0, 0, 0},
		"zero expiry w/bit": append(append([]byte{0x01, 1, 0, 1, 0, 0, 0}, "fv"...), 0, 0, 0, 0, 0, 0, 0, 0),
	}
	for name, region := range corrupt {
		it := hashEntryIter{p: region}
		if _, _, _, _, err := it.next(); err == nil {
			t.Errorf("%s: corrupt entry decoded cleanly", name)
		}
	}
}

func TestHashInlineRootCodec(t *testing.T) {
	p := appendHashInlineHdr(nil, 2, 500)
	p = appendHashEntry(p, []byte("a"), []byte("1"), 0)
	p = appendHashEntry(p, []byte("b"), []byte("2"), 500)
	hi, err := decodeHashInline(p)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if hi.count != 2 || hi.minExpMs != 500 {
		t.Fatalf("decoded count=%d minExp=%d, want 2, 500", hi.count, hi.minExpMs)
	}

	entry := func(f, v string, exp int64) []byte { return appendHashEntry(nil, []byte(f), []byte(v), exp) }
	oversize := appendHashInlineHdr(nil, 1, 0)
	oversize = appendHashEntry(oversize, []byte("f"), bytes.Repeat([]byte{'x'}, hashInlineMax), 0)
	corrupt := map[string][]byte{
		"short payload": {hashSubInline, 0, 1},
		"wrong sub": func() []byte {
			b := appendHashInlineHdr(nil, 1, 0)
			b[0] = ropeSub
			return append(b, entry("a", "1", 0)...)
		}(),
		"reserved hflags": func() []byte {
			b := appendHashInlineHdr(nil, 1, 0)
			b[1] = 0x04
			return append(b, entry("a", "1", 0)...)
		}(),
		"count mismatch": append(appendHashInlineHdr(nil, 3, 0), entry("a", "1", 0)...),
		"zero count":     appendHashInlineHdr(nil, 0, 0),
		"ttl flag disagree": func() []byte {
			b := appendHashInlineHdr(nil, 1, 0)
			b[1] = hflagAnyTTL
			return append(b, entry("a", "1", 0)...)
		}(),
		"min_expire wrong": append(appendHashInlineHdr(nil, 1, 999), entry("a", "1", 500)...),
		"oversize payload": oversize,
	}
	for name, p := range corrupt {
		if _, err := decodeHashInline(p); err == nil {
			t.Errorf("%s: corrupt inline root decoded cleanly", name)
		}
	}
}

func TestHashInlinePointOps(t *testing.T) {
	r := newHashRig(t)
	ctx := context.Background()

	if _, ok := r.hget("h", "f"); ok {
		t.Fatal("HGET of a missing key found something")
	}
	if n, err := r.h.HLen(ctx, []byte("h")); err != nil || n != 0 {
		t.Fatalf("HLEN of a missing key = %d, %v", n, err)
	}

	if !r.hset("h", "f1", "v1") || !r.hset("h", "f2", "v2") || !r.hset("h", "f3", "v3") {
		t.Fatal("fresh fields did not report created")
	}
	if r.hset("h", "f2", "V2") {
		t.Fatal("update of f2 reported created")
	}
	if v, ok := r.hget("h", "f2"); !ok || v != "V2" {
		t.Fatalf("f2 = %q, %v after update", v, ok)
	}
	if _, ok := r.hget("h", "nope"); ok {
		t.Fatal("HGET of a missing field found something")
	}
	if got := r.fields("h"); !slices.Equal(got, []string{"f1", "f2", "f3"}) {
		t.Fatalf("update moved a field: order %v", got)
	}
	if n, _ := r.h.HLen(ctx, []byte("h")); n != 3 {
		t.Fatalf("HLEN = %d, want 3", n)
	}

	if removed, err := r.h.HDel(ctx, []byte("h"), []byte("f2")); err != nil || !removed {
		t.Fatalf("HDEL f2 = %v, %v", removed, err)
	}
	if removed, _ := r.h.HDel(ctx, []byte("h"), []byte("f2")); removed {
		t.Fatal("second HDEL of f2 reported removed")
	}
	if got := r.fields("h"); !slices.Equal(got, []string{"f1", "f3"}) {
		t.Fatalf("order after delete: %v", got)
	}

	// Cold path: drain, evict, read through the store.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	r.tr.EvictAllForTest()
	if v, ok := r.hget("h", "f3"); !ok || v != "v3" {
		t.Fatalf("cold HGET f3 = %q, %v", v, ok)
	}
	if n, _ := r.h.HLen(ctx, []byte("h")); n != 2 {
		t.Fatalf("cold HLEN = %d, want 2", n)
	}

	// A reopened runtime sees the same hash.
	h2 := r.reopen()
	if v, ok, err := h2.HGet(ctx, []byte("h"), []byte("f1")); err != nil || !ok || string(v) != "v1" {
		t.Fatalf("reopened HGET f1 = %q, %v, %v", v, ok, err)
	}

	// Deleting the last fields deletes the key.
	for _, f := range []string{"f1", "f3"} {
		if removed, err := r.h.HDel(ctx, []byte("h"), []byte(f)); err != nil || !removed {
			t.Fatalf("HDEL %s = %v, %v", f, removed, err)
		}
	}
	if exists, _, err := r.s.Entry(ctx, []byte("h")); err != nil || exists {
		t.Fatalf("empty hash still exists: %v, %v", exists, err)
	}
}

func TestHashInlineThresholds(t *testing.T) {
	r := newHashRig(t)
	ctx := context.Background()

	// Count threshold: 128 small fields stay inline, the 129th
	// upgrades the hash to segments.
	for i := range hashInlineMaxCount {
		if !r.hset("counts", fmt.Sprintf("f%03d", i), "v") {
			t.Fatalf("field %d not created", i)
		}
	}
	if enc, _, _ := r.h.Encoding(ctx, []byte("counts")); enc != "listpack" {
		t.Fatalf("encoding at the count ceiling = %q, want listpack", enc)
	}
	// An update of an existing field still fits inline.
	if r.hset("counts", "f000", "v-bigger") {
		t.Fatal("update at the count ceiling reported created")
	}
	if !r.hset("counts", "f-one-more", "v") {
		t.Fatal("field 129 not created")
	}
	if enc, _, _ := r.h.Encoding(ctx, []byte("counts")); enc != "hashtable" {
		t.Fatalf("encoding after the count upgrade = %q, want hashtable", enc)
	}
	if n, _ := r.h.HLen(ctx, []byte("counts")); n != hashInlineMaxCount+1 {
		t.Fatalf("HLEN after the count upgrade = %d, want %d", n, hashInlineMaxCount+1)
	}
	if v, ok := r.hget("counts", "f000"); !ok || v != "v-bigger" {
		t.Fatalf("f000 = %q, %v after the upgrade", v, ok)
	}

	// Size threshold, pinned to the byte: grow a two-field hash so the
	// payload lands exactly on hashInlineMax, then push one byte over.
	// The crossing write is an update, so it must report created=false
	// through the upgrade.
	pad := hashInlineMax - hashInlineHdrLen - 2*(hashEntryHdrLen+1) - 1
	if !r.hset("sizes", "a", strings.Repeat("x", pad)) {
		t.Fatal("padding field not created")
	}
	if !r.hset("sizes", "b", "1") {
		t.Fatal("boundary field not created at exactly the cap")
	}
	if enc, _, _ := r.h.Encoding(ctx, []byte("sizes")); enc != "listpack" {
		t.Fatalf("encoding at exactly the cap = %q, want listpack", enc)
	}
	if r.hset("sizes", "b", "22") {
		t.Fatal("size-crossing update reported created")
	}
	if enc, _, _ := r.h.Encoding(ctx, []byte("sizes")); enc != "hashtable" {
		t.Fatalf("encoding after the size upgrade = %q, want hashtable", enc)
	}
	if v, ok := r.hget("sizes", "b"); !ok || v != "22" {
		t.Fatalf("b = %q, %v after the size upgrade", v, ok)
	}
	if v, ok := r.hget("sizes", "a"); !ok || v != strings.Repeat("x", pad) {
		t.Fatalf("padding field lost across the upgrade: ok=%v len=%d", ok, len(v))
	}

	// A fresh key with one oversized value skips inline entirely.
	if !r.hset("fresh", "f", strings.Repeat("x", hashInlineMax)) {
		t.Fatal("oversized fresh field not created")
	}
	if enc, _, _ := r.h.Encoding(ctx, []byte("fresh")); enc != "hashtable" {
		t.Fatalf("fresh oversized encoding = %q, want hashtable", enc)
	}
	if n, _ := r.h.HLen(ctx, []byte("fresh")); n != 1 {
		t.Fatalf("fresh oversized HLEN = %d, want 1", n)
	}
}

func TestHashWrongType(t *testing.T) {
	r := newHashRig(t)
	ctx := context.Background()

	// A plain string, a rope, and an inline hash.
	if err := r.s.Set(ctx, []byte("str"), []byte("plain")); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Set(ctx, []byte("rope"), bytes.Repeat([]byte{'r'}, 9<<10)); err != nil {
		t.Fatal(err)
	}
	r.hset("hash", "f", "v")

	for _, key := range []string{"str", "rope"} {
		if _, err := r.h.HSet(ctx, []byte(key), []byte("f"), []byte("v")); !errors.Is(err, ErrWrongType) {
			t.Errorf("HSET on %s = %v, want ErrWrongType", key, err)
		}
		if _, _, err := r.h.HGet(ctx, []byte(key), []byte("f")); !errors.Is(err, ErrWrongType) {
			t.Errorf("HGET on %s = %v, want ErrWrongType", key, err)
		}
		if _, err := r.h.HDel(ctx, []byte(key), []byte("f")); !errors.Is(err, ErrWrongType) {
			t.Errorf("HDEL on %s = %v, want ErrWrongType", key, err)
		}
		if _, err := r.h.HLen(ctx, []byte(key)); !errors.Is(err, ErrWrongType) {
			t.Errorf("HLEN on %s = %v, want ErrWrongType", key, err)
		}
	}

	// The string read doors bounce off a hash root.
	if _, _, err := r.s.Get(ctx, []byte("hash")); !errors.Is(err, ErrWrongType) {
		t.Errorf("GET on a hash = %v, want ErrWrongType", err)
	}
	if _, err := r.s.Range(ctx, []byte("hash"), 0, -1); !errors.Is(err, ErrWrongType) {
		t.Errorf("GETRANGE on a hash = %v, want ErrWrongType", err)
	}
	if _, _, err := r.s.Strlen(ctx, []byte("hash")); !errors.Is(err, ErrWrongType) {
		t.Errorf("STRLEN on a hash = %v, want ErrWrongType", err)
	}
	if _, err := r.s.Append(ctx, []byte("hash"), []byte("x")); !errors.Is(err, ErrWrongType) {
		t.Errorf("APPEND on a hash = %v, want ErrWrongType", err)
	}
	if _, err := r.s.IncrBy(ctx, []byte("hash"), 1); !errors.Is(err, ErrWrongType) {
		t.Errorf("INCR on a hash = %v, want ErrWrongType", err)
	}
	if _, err := r.s.IncrByFloat(ctx, []byte("hash"), 1); !errors.Is(err, ErrWrongType) {
		t.Errorf("INCRBYFLOAT on a hash = %v, want ErrWrongType", err)
	}
	if _, _, err := r.s.Encoding(ctx, []byte("hash")); !errors.Is(err, ErrWrongType) {
		t.Errorf("string Encoding on a hash = %v, want ErrWrongType", err)
	}

	// MGET treats another type as a miss, never an error.
	got := []string{}
	err := r.s.MGet(ctx, [][]byte{[]byte("str"), []byte("hash"), []byte("gone")}, func(v []byte, ok bool) {
		if ok {
			got = append(got, string(v))
		} else {
			got = append(got, "<nil>")
		}
	})
	if err != nil {
		t.Fatalf("MGET across types: %v", err)
	}
	if !slices.Equal(got, []string{"plain", "<nil>", "<nil>"}) {
		t.Fatalf("MGET across types = %v", got)
	}

	// MSETNX's gate counts a hash key as existing.
	if any, err := r.s.ExistsAny(ctx, [][]byte{[]byte("gone"), []byte("hash")}); err != nil || !any {
		t.Fatalf("ExistsAny over a hash = %v, %v", any, err)
	}

	// SET and DEL take the key over: an inline hash is planeless, so
	// the overwrite is a plain record write.
	if err := r.s.Set(ctx, []byte("hash"), []byte("now-a-string")); err != nil {
		t.Fatalf("SET over a hash: %v", err)
	}
	if v, ok, err := r.s.Get(ctx, []byte("hash")); err != nil || !ok || string(v) != "now-a-string" {
		t.Fatalf("GET after takeover = %q, %v, %v", v, ok, err)
	}
	r.hset("hash2", "f", "v")
	if dead, err := r.s.Del(ctx, []byte("hash2")); err != nil || !dead {
		t.Fatalf("DEL of a hash = %v, %v", dead, err)
	}
}

func TestHashEncoding(t *testing.T) {
	r := newHashRig(t)
	ctx := context.Background()

	if _, ok, err := r.h.Encoding(ctx, []byte("missing")); ok || err != nil {
		t.Fatalf("encoding of a missing key: %v, %v", ok, err)
	}
	r.hset("h", "f", "v")
	if enc, ok, _ := r.h.Encoding(ctx, []byte("h")); !ok || enc != "listpack" {
		t.Fatalf("inline encoding = %q, %v, want listpack", enc, ok)
	}

	// A real segmented hash answers hashtable.
	if !r.hset("wide", "f", strings.Repeat("x", hashInlineMax)) {
		t.Fatal("oversized field not created")
	}
	if enc, ok, _ := r.h.Encoding(ctx, []byte("wide")); !ok || enc != "hashtable" {
		t.Fatalf("segmented encoding = %q, %v, want hashtable", enc, ok)
	}
}

func TestHashKeyTTLSurvivesWrites(t *testing.T) {
	r := newHashRig(t)
	ctx := context.Background()

	r.hset("h", "f1", "v1")
	const at = int64(1<<41) + 60_000
	if ok, err := r.tr.ExpireAt(ctx, []byte("h"), at); err != nil || !ok {
		t.Fatalf("ExpireAt: %v, %v", ok, err)
	}
	r.hset("h", "f2", "v2")
	if _, _, expMs, ok, err := r.tr.LookupEntry(ctx, []byte("h")); err != nil || !ok || expMs != at {
		t.Fatalf("expiry after hot HSET = %d, %v, %v, want %d", expMs, ok, err, at)
	}

	// The stamp survives a write that pulls the key back through a
	// fresh hot header (the restamp path).
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	r.tr.EvictAllForTest()
	r.hset("h", "f3", "v3")
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	r.tr.EvictAllForTest()
	if _, _, expMs, ok, err := r.tr.LookupEntry(ctx, []byte("h")); err != nil || !ok || expMs != at {
		t.Fatalf("expiry after cold HSET = %d, %v, %v, want %d", expMs, ok, err, at)
	}
}
