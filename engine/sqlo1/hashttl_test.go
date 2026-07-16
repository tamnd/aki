package sqlo1

// Field-TTL battery, doc 06 section 4: the HEXPIRE family's per-field
// codes, lazy expiry on every point read, purge-first on the
// count-bearing walks, ReapDue's chain recompute at every rung of the
// ladder, and the ExpireHook registration door the doc 11 loop rides.

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// ttlBase is the fixed clock the TTL rigs start at.
const ttlBase = int64(1) << 41

// newHashTTLRig is newHashRig with a mutable clock: tests advance
// *clk to expire fields.
func newHashTTLRig(t *testing.T) (*hashRig, *int64) {
	t.Helper()
	clk := new(int64)
	*clk = ttlBase
	rs := newRecordingStore()
	tr := NewTiered(rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     11,
		NowMs:    func() int64 { return *clk },
	})
	h, err := NewHash(tr, HashConfig{})
	if err != nil {
		t.Fatalf("NewHash: %v", err)
	}
	return &hashRig{t: t, rs: rs, tr: tr, h: h}, clk
}

// reopenAt is hashRig.reopen with the rig's clock, so a cold view
// after a time advance judges expiry the same way the live one did.
func reopenAt(r *hashRig, clk *int64) *Hash {
	r.t.Helper()
	tr := NewTiered(r.rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     12,
		NowMs:    func() int64 { return *clk },
	})
	h, err := NewHash(tr, HashConfig{})
	if err != nil {
		r.t.Fatalf("NewHash: %v", err)
	}
	return h
}

func (r *hashRig) hexpire(key string, atMs int64, cond HExpireCond, fields ...string) []int64 {
	r.t.Helper()
	bs := make([][]byte, len(fields))
	for i, f := range fields {
		bs[i] = []byte(f)
	}
	res, err := r.h.HExpire(context.Background(), []byte(key), atMs, cond, bs, nil)
	if err != nil {
		r.t.Fatalf("HExpire(%q, %d): %v", key, atMs, err)
	}
	return res
}

func (r *hashRig) httl(key string, fields ...string) []int64 {
	r.t.Helper()
	bs := make([][]byte, len(fields))
	for i, f := range fields {
		bs[i] = []byte(f)
	}
	res, err := r.h.HTtl(context.Background(), []byte(key), bs, nil)
	if err != nil {
		r.t.Fatalf("HTtl(%q): %v", key, err)
	}
	return res
}

func (r *hashRig) hpersist(key string, fields ...string) []int64 {
	r.t.Helper()
	bs := make([][]byte, len(fields))
	for i, f := range fields {
		bs[i] = []byte(f)
	}
	res, err := r.h.HPersist(context.Background(), []byte(key), bs, nil)
	if err != nil {
		r.t.Fatalf("HPersist(%q): %v", key, err)
	}
	return res
}

func (r *hashRig) hlen(key string) int64 {
	r.t.Helper()
	n, err := r.h.HLen(context.Background(), []byte(key))
	if err != nil {
		r.t.Fatalf("HLen(%q): %v", key, err)
	}
	return n
}

func wantCodes(t *testing.T, got []int64, want ...int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("codes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("codes = %v, want %v", got, want)
		}
	}
}

func TestHashFieldTTLExpireCodes(t *testing.T) {
	r, clk := newHashTTLRig(t)

	// Missing key: -2 per field, no key created.
	wantCodes(t, r.hexpire("nope", ttlBase+1000, HExpireNone, "a", "b"), -2, -2)
	wantCodes(t, r.httl("nope", "a"), -2)
	wantCodes(t, r.hpersist("nope", "a"), -2)
	if n := r.hlen("nope"); n != 0 {
		t.Fatalf("HEXPIRE on a missing key created it: HLEN %d", n)
	}

	r.hset("h", "f1", "v1")
	r.hset("h", "f2", "v2")

	// Plain set, missing field, and the TTL readback.
	at := ttlBase + 100_000
	wantCodes(t, r.hexpire("h", at, HExpireNone, "f1", "ghost"), 1, -2)
	wantCodes(t, r.httl("h", "f1", "f2", "ghost"), at, -1, -2)

	// The condition table against a field with a TTL (f1 at `at`) and
	// one without (f2). GT and LT treat no-TTL as infinite.
	wantCodes(t, r.hexpire("h", at+1, HExpireNX, "f1", "f2"), 0, 1)
	wantCodes(t, r.hpersist("h", "f2"), 1)
	wantCodes(t, r.hexpire("h", at+1, HExpireXX, "f1", "f2"), 1, 0)
	wantCodes(t, r.hexpire("h", at, HExpireGT, "f1", "f2"), 0, 0)   // equal fails, no-TTL fails
	wantCodes(t, r.hexpire("h", at+2, HExpireGT, "f1"), 1)          // strictly greater
	wantCodes(t, r.hexpire("h", at+2, HExpireLT, "f1", "f2"), 0, 1) // equal fails, no-TTL passes
	wantCodes(t, r.hexpire("h", at, HExpireLT, "f1"), 1)            // strictly less
	wantCodes(t, r.httl("h", "f1", "f2"), at, at+2)

	// Equal time under no condition is a set that writes nothing.
	wantCodes(t, r.hexpire("h", at, HExpireNone, "f1"), 1)

	// A past time deletes, condition checked first.
	wantCodes(t, r.hexpire("h", *clk, HExpireNX, "f1"), 0)
	wantCodes(t, r.hexpire("h", *clk, HExpireNone, "f1"), 2)
	if _, ok := r.hget("h", "f1"); ok {
		t.Fatal("past-time HEXPIRE left the field readable")
	}
	if n := r.hlen("h"); n != 1 {
		t.Fatalf("HLEN after past-time delete = %d, want 1", n)
	}

	// Deleting the last field through a past time kills the key.
	wantCodes(t, r.hexpire("h", *clk, HExpireNone, "f2"), 2)
	if n := r.hlen("h"); n != 0 {
		t.Fatalf("HLEN after the hash emptied = %d, want 0", n)
	}
}

func TestHashFieldTTLLazyExpiry(t *testing.T) {
	r, clk := newHashTTLRig(t)
	ctx := context.Background()

	r.hset("h", "dead", "dv")
	r.hset("h", "live", "lv")
	wantCodes(t, r.hexpire("h", ttlBase+1000, HExpireNone, "dead"), 1)
	*clk = ttlBase + 2000

	// Every point read treats the dead field as absent while the bytes
	// wait for the reaper: the count still includes it.
	if _, ok := r.hget("h", "dead"); ok {
		t.Fatal("expired field still readable")
	}
	if removed, err := r.h.HDel(ctx, []byte("h"), []byte("dead")); err != nil || removed {
		t.Fatalf("HDel of an expired field = (%v, %v), want (false, nil)", removed, err)
	}
	wantCodes(t, r.httl("h", "dead"), -2)
	wantCodes(t, r.hpersist("h", "dead"), -2)
	if n := r.hlen("h"); n != 2 {
		t.Fatalf("HLEN = %d, want 2 (lazy model counts the unreaped)", n)
	}

	// HSCAN filters the dead entry without purging it.
	var seen []string
	if _, err := r.h.HScan(ctx, []byte("h"), 0, 100, func(f, v []byte) {
		seen = append(seen, string(f))
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0] != "live" {
		t.Fatalf("HSCAN emitted %v, want [live]", seen)
	}
	if n := r.hlen("h"); n != 2 {
		t.Fatalf("HLEN after HSCAN = %d, want 2 (scan must not purge)", n)
	}

	// Replacing the dead entry is a create on the wire and an update in
	// the count.
	if created := r.hset("h", "dead", "back"); !created {
		t.Fatal("HSET over an expired field answered update; the dead field was never observable")
	}
	if n := r.hlen("h"); n != 2 {
		t.Fatalf("HLEN after revive = %d, want 2", n)
	}
	if v, ok := r.hget("h", "dead"); !ok || v != "back" {
		t.Fatalf("revived field = (%q, %v)", v, ok)
	}
	wantCodes(t, r.httl("h", "dead"), -1) // HSET cleared the TTL

	// HINCRBY over an expired field restarts from zero with no TTL.
	wantCodes(t, r.hexpire("h", *clk+1000, HExpireNone, "dead"), 1)
	*clk += 2000
	n, err := r.h.HIncrBy(ctx, []byte("h"), []byte("dead"), 7)
	if err != nil || n != 7 {
		t.Fatalf("HIncrBy over an expired field = (%d, %v), want (7, nil)", n, err)
	}
	wantCodes(t, r.httl("h", "dead"), -1)
}

func TestHashFieldTTLIteratePurges(t *testing.T) {
	r, clk := newHashTTLRig(t)
	ctx := context.Background()

	r.hset("h", "a", "1")
	r.hset("h", "b", "2")
	r.hset("h", "c", "3")
	wantCodes(t, r.hexpire("h", ttlBase+1000, HExpireNone, "a", "c"), 1, 1)
	*clk = ttlBase + 5000

	// The count-bearing walk reaps first: the header is exact live.
	var got []string
	counted := -1
	if err := r.h.HIterate(ctx, []byte("h"), func(n int) { counted = n }, func(f, v []byte) {
		got = append(got, string(f)+"="+string(v))
	}); err != nil {
		t.Fatal(err)
	}
	if counted != 1 || len(got) != 1 || got[0] != "b=2" {
		t.Fatalf("HIterate = begin(%d) %v, want begin(1) [b=2]", counted, got)
	}
	if n := r.hlen("h"); n != 1 {
		t.Fatalf("HLEN after the purge = %d, want 1", n)
	}
	if fs := r.fields("h"); len(fs) != 1 || fs[0] != "b" {
		t.Fatalf("stored entries after the purge = %v, want [b]", fs)
	}

	// All dead: the walk reaps the hash into a dead key.
	r.hset("g", "x", "1")
	wantCodes(t, r.hexpire("g", *clk+100, HExpireNone, "x"), 1)
	*clk += 200
	counted = -1
	if err := r.h.HIterate(ctx, []byte("g"), func(n int) { counted = n }, func(f, v []byte) {
		t.Fatalf("emit of %q from an all-dead hash", f)
	}); err != nil {
		t.Fatal(err)
	}
	if counted != 0 {
		t.Fatalf("begin(%d) on an all-dead hash, want begin(0)", counted)
	}
	if n := r.hlen("g"); n != 0 {
		t.Fatalf("HLEN after the all-dead purge = %d, want 0", n)
	}
}

func TestHashFieldTTLRandNeverDead(t *testing.T) {
	r, clk := newHashTTLRig(t)
	ctx := context.Background()

	r.hset("h", "live", "1")
	r.hset("h", "d1", "2")
	r.hset("h", "d2", "3")
	wantCodes(t, r.hexpire("h", ttlBase+1000, HExpireNone, "d1", "d2"), 1, 1)
	*clk = ttlBase + 5000

	f, _, ok, err := r.h.HRandField(ctx, []byte("h"))
	if err != nil || !ok || string(f) != "live" {
		t.Fatalf("HRandField = (%q, %v, %v), want the one live field", f, ok, err)
	}
	if n := r.hlen("h"); n != 1 {
		t.Fatalf("HLEN after the draw's purge = %d, want 1", n)
	}

	// The count form's begin(n) arithmetic runs over live entries.
	r.hset("h", "d3", "4")
	wantCodes(t, r.hexpire("h", *clk+100, HExpireNone, "d3"), 1)
	*clk += 200
	counted := int64(-1)
	var got []string
	if err := r.h.HRandFieldCount(ctx, []byte("h"), 10, false, func(n int64) { counted = n }, func(f, v []byte) {
		got = append(got, string(f))
	}); err != nil {
		t.Fatal(err)
	}
	if counted != 1 || len(got) != 1 || got[0] != "live" {
		t.Fatalf("HRandFieldCount = begin(%d) %v, want begin(1) [live]", counted, got)
	}
}

func TestHashFieldTTLReapInline(t *testing.T) {
	r, clk := newHashTTLRig(t)
	ctx := context.Background()

	r.hset("h", "a", "1")
	r.hset("h", "b", "2")
	r.hset("h", "c", "3")
	wantCodes(t, r.hexpire("h", ttlBase+1000, HExpireNone, "a"), 1)
	wantCodes(t, r.hexpire("h", ttlBase+9000, HExpireNone, "c"), 1)

	// Not due yet: a no-op.
	if n, err := r.h.ReapDue(ctx, []byte("h")); err != nil || n != 0 {
		t.Fatalf("ReapDue before dueness = (%d, %v), want (0, nil)", n, err)
	}

	*clk = ttlBase + 2000
	if n, err := r.h.ReapDue(ctx, []byte("h")); err != nil || n != 1 {
		t.Fatalf("ReapDue = (%d, %v), want (1, nil)", n, err)
	}
	if n := r.hlen("h"); n != 2 {
		t.Fatalf("HLEN after the reap = %d, want 2", n)
	}
	// The rewritten root's min is exact: c's expiry. The decode
	// validates min against the entries, so the read is the oracle.
	if fs := r.fields("h"); len(fs) != 2 {
		t.Fatalf("entries after the reap = %v", fs)
	}
	wantCodes(t, r.httl("h", "b", "c"), -1, ttlBase+9000)

	// All remaining TTLs due: the reap kills the key.
	wantCodes(t, r.hexpire("h", *clk+100, HExpireNone, "b"), 1)
	*clk = ttlBase + 10_000
	if n, err := r.h.ReapDue(ctx, []byte("h")); err != nil || n != 2 {
		t.Fatalf("ReapDue = (%d, %v), want (2, nil)", n, err)
	}
	if n := r.hlen("h"); n != 0 {
		t.Fatalf("HLEN after the reap emptied the hash = %d, want 0", n)
	}
}

// segTTLFill grows key past the inline tier: n fields f000.. with
// 120-byte values, a few flat-fence segments.
func segTTLFill(t *testing.T, r *hashRig, key string, n int) {
	t.Helper()
	for i := range n {
		r.hset(key, fmt.Sprintf("f%03d", i), fmt.Sprintf("v-%d-%s", i, strings.Repeat("x", 120)))
	}
	if sr := r.segRootOf(key); len(sr.fence) < 2 {
		t.Fatalf("%d fields built only %d segments; the reap walk needs several", n, len(sr.fence))
	}
}

func TestHashFieldTTLReapSegmented(t *testing.T) {
	r, clk := newHashTTLRig(t)
	ctx := context.Background()
	segTTLFill(t, r, "h", 60)

	// TTL ten fields; the root min and the covering segments' metas
	// pick the chain up.
	var due []string
	for i := range 10 {
		due = append(due, fmt.Sprintf("f%03d", i))
	}
	res := r.hexpire("h", ttlBase+1000, HExpireNone, due...)
	wantCodes(t, res, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
	if sr := r.segRootOf("h"); sr.minExpMs != ttlBase+1000 {
		t.Fatalf("root min = %d, want %d", sr.minExpMs, ttlBase+1000)
	}

	*clk = ttlBase + 2000
	if n, err := r.h.ReapDue(ctx, []byte("h")); err != nil || n != 10 {
		t.Fatalf("ReapDue = (%d, %v), want (10, nil)", n, err)
	}
	sr := r.segRootOf("h")
	if sr.count != 50 || sr.minExpMs != 0 {
		t.Fatalf("root after the reap: count %d min %d, want 50 and 0", sr.count, sr.minExpMs)
	}
	for _, e := range sr.fence {
		if e.meta&hashMetaHasTTL != 0 {
			t.Fatalf("fence entry for segid %d still carries the has-TTL bit", e.segid)
		}
	}
	for i := 10; i < 60; i++ {
		if _, ok := r.hget("h", fmt.Sprintf("f%03d", i)); !ok {
			t.Fatalf("survivor f%03d unreadable after the reap", i)
		}
	}

	// Cold view: every segment decode revalidates count and min
	// exactness, so a clean read is the chain oracle.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	hc := reopenAt(r, clk)
	if n, err := hc.HLen(ctx, []byte("h")); err != nil || n != 50 {
		t.Fatalf("cold HLEN = (%d, %v), want 50", n, err)
	}
	if v, ok, err := hc.HGet(ctx, []byte("h"), []byte("f042")); err != nil || !ok || len(v) == 0 {
		t.Fatalf("cold HGet(f042) = (%q, %v, %v)", v, ok, err)
	}
}

func TestHashFieldTTLReapStaleEarlyMin(t *testing.T) {
	r, clk := newHashTTLRig(t)
	ctx := context.Background()
	segTTLFill(t, r, "h", 60)

	// f000 carries the min; deleting it live leaves the root min
	// stale-early (H-I6). The due reap finds nothing dead and raises
	// the min to the true earliest.
	wantCodes(t, r.hexpire("h", ttlBase+5000, HExpireNone, "f000"), 1)
	wantCodes(t, r.hexpire("h", ttlBase+8000, HExpireNone, "f001"), 1)
	if removed, err := r.h.HDel(ctx, []byte("h"), []byte("f000")); err != nil || !removed {
		t.Fatalf("HDel(f000) = (%v, %v)", removed, err)
	}
	if sr := r.segRootOf("h"); sr.minExpMs != ttlBase+5000 {
		t.Fatalf("root min after the delete = %d, want the stale-early %d", sr.minExpMs, ttlBase+5000)
	}

	*clk = ttlBase + 6000
	if n, err := r.h.ReapDue(ctx, []byte("h")); err != nil || n != 0 {
		t.Fatalf("ReapDue = (%d, %v), want (0, nil): nothing was dead", n, err)
	}
	sr := r.segRootOf("h")
	if sr.minExpMs != ttlBase+8000 {
		t.Fatalf("root min after the probe = %d, want raised to %d", sr.minExpMs, ttlBase+8000)
	}
	if sr.count != 59 {
		t.Fatalf("root count = %d, want 59", sr.count)
	}
}

func TestHashFieldTTLReapSegmentedDeath(t *testing.T) {
	r, clk := newHashTTLRig(t)
	ctx := context.Background()
	segTTLFill(t, r, "h", 40)

	var all []string
	for i := range 40 {
		all = append(all, fmt.Sprintf("f%03d", i))
	}
	res := r.hexpire("h", ttlBase+1000, HExpireNone, all...)
	for i, c := range res {
		if c != 1 {
			t.Fatalf("HEXPIRE of %s = %d, want 1", all[i], c)
		}
	}
	*clk = ttlBase + 2000
	if n, err := r.h.ReapDue(ctx, []byte("h")); err != nil || n != 40 {
		t.Fatalf("ReapDue = (%d, %v), want (40, nil)", n, err)
	}
	if n := r.hlen("h"); n != 0 {
		t.Fatalf("HLEN after the reap = %d, want 0", n)
	}
	// The plane retired: a recreate starts inline.
	r.hset("h", "fresh", "v")
	if enc, ok, err := r.h.Encoding(ctx, []byte("h")); err != nil || !ok || enc != "listpack" {
		t.Fatalf("recreated hash encoding = (%q, %v, %v), want listpack", enc, ok, err)
	}
}

func TestHashFieldTTLReapPaged(t *testing.T) {
	if testing.Short() {
		t.Skip("grows a paged hash")
	}
	r, clk := newHashTTLRig(t)
	ctx := context.Background()
	val := pageRigHash(t, r, "pg", 600)
	if !r.segRootOf("pg").paged {
		t.Fatal("600 fat fields left the root flat")
	}

	// TTL a spread of fields across the fh space so multiple pages
	// carry due segments.
	var due []string
	for i := 0; i < 600; i += 7 {
		due = append(due, fmt.Sprintf("f%04d", i))
	}
	res := r.hexpire("pg", ttlBase+1000, HExpireNone, due...)
	for i, c := range res {
		if c != 1 {
			t.Fatalf("HEXPIRE of %s = %d, want 1", due[i], c)
		}
	}

	*clk = ttlBase + 2000
	n, err := r.h.ReapDue(ctx, []byte("pg"))
	if err != nil || n != len(due) {
		t.Fatalf("ReapDue = (%d, %v), want (%d, nil)", n, err, len(due))
	}
	sr := r.segRootOf("pg")
	if int(sr.count) != 600-len(due) || sr.minExpMs != 0 {
		t.Fatalf("root after the reap: count %d min %d, want %d and 0", sr.count, sr.minExpMs, 600-len(due))
	}

	// Survivors readable hot and cold; the dead stay gone.
	if _, ok := r.hget("pg", "f0008"); !ok {
		t.Fatal("survivor f0008 unreadable")
	}
	if _, ok := r.hget("pg", "f0000"); ok {
		t.Fatal("reaped f0000 still readable")
	}
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	hc := reopenAt(r, clk)
	if got, err := hc.HLen(ctx, []byte("pg")); err != nil || int(got) != 600-len(due) {
		t.Fatalf("cold HLEN = (%d, %v), want %d", got, err, 600-len(due))
	}
	if v, ok, err := hc.HGet(ctx, []byte("pg"), []byte("f0599")); err != nil || !ok || string(v) != val(599) {
		t.Fatalf("cold HGet(f0599) = (%q, %v, %v)", v, ok, err)
	}
}

func TestHashFieldTTLExpireHook(t *testing.T) {
	r, clk := newHashTTLRig(t)
	ctx := context.Background()

	type fire struct {
		key string
		min int64
	}
	var fires []fire
	r.h.ExpireHook = func(key []byte, minExpMs int64) {
		fires = append(fires, fire{key: string(key), min: minExpMs})
	}
	last := func() fire {
		t.Helper()
		if len(fires) == 0 {
			t.Fatal("expected a hook fire")
		}
		return fires[len(fires)-1]
	}

	r.hset("h", "a", "1")
	r.hset("h", "b", "2")
	if len(fires) != 0 {
		t.Fatalf("TTL-less writes fired the hook: %v", fires)
	}

	// First TTL: 0 -> at. A later TTL above the min is silent; a lower
	// one re-fires.
	at := ttlBase + 5000
	r.hexpire("h", at, HExpireNone, "a")
	if f := last(); f.key != "h" || f.min != at {
		t.Fatalf("hook after the first TTL = %+v, want {h %d}", f, at)
	}
	n := len(fires)
	r.hexpire("h", at+1000, HExpireNone, "b")
	if len(fires) != n {
		t.Fatalf("a TTL above the min fired the hook: %+v", fires[len(fires)-1])
	}
	r.hexpire("h", at-1000, HExpireNone, "b")
	if f := last(); f.min != at-1000 {
		t.Fatalf("hook after the lower TTL = %+v, want min %d", f, at-1000)
	}

	// HPERSIST of the min holder raises the min back to a's.
	r.hpersist("h", "b")
	if f := last(); f.min != at {
		t.Fatalf("hook after HPERSIST = %+v, want min %d", f, at)
	}

	// The reap that clears the last TTL fires zero.
	*clk = at + 1000
	if _, err := r.h.ReapDue(ctx, []byte("h")); err != nil {
		t.Fatal(err)
	}
	if f := last(); f.min != 0 {
		t.Fatalf("hook after the reap = %+v, want min 0", f)
	}

	// The delete that kills a hash with a pending TTL fires zero too.
	fires = fires[:0]
	r.hset("g", "x", "1")
	r.hexpire("g", *clk+5000, HExpireNone, "x")
	if removed, err := r.h.HDel(ctx, []byte("g"), []byte("x")); err != nil || !removed {
		t.Fatalf("HDel = (%v, %v)", removed, err)
	}
	if f := last(); f.key != "g" || f.min != 0 {
		t.Fatalf("hook after the key death = %+v, want {g 0}", f)
	}
}

// TestServerHashFieldTTLWire drives the dispatch loop directly with a
// pinned clock, so every reply byte including the four TTL readback
// conversions is deterministic. Error texts are Redis 8's, verbatim.
func TestServerHashFieldTTLWire(t *testing.T) {
	srv, err := NewServer(NewMemStore())
	if err != nil {
		t.Fatal(err)
	}
	clk := ttlBase
	srv.now = func() int64 { return clk }
	do := func(args ...string) string {
		t.Helper()
		bs := make([][]byte, len(args))
		for i, a := range args {
			bs[i] = []byte(a)
		}
		return string(srv.dispatch(nil, bs))
	}
	want := func(got, want string) {
		t.Helper()
		if got != want {
			t.Fatalf("reply = %q, want %q", got, want)
		}
	}

	want(do("HSET", "h", "f1", "v1", "f2", "v2"), ":2\r\n")

	// Set and read back through all four commands.
	want(do("HEXPIRE", "h", "100", "FIELDS", "2", "f1", "ghost"), "*2\r\n:1\r\n:-2\r\n")
	at := clk + 100_000
	want(do("HTTL", "h", "FIELDS", "3", "f1", "f2", "ghost"), "*3\r\n:100\r\n:-1\r\n:-2\r\n")
	want(do("HPTTL", "h", "FIELDS", "1", "f1"), "*1\r\n:100000\r\n")
	want(do("HEXPIRETIME", "h", "FIELDS", "1", "f1"), fmt.Sprintf("*1\r\n:%d\r\n", (at+999)/1000))
	want(do("HPEXPIRETIME", "h", "FIELDS", "1", "f1"), fmt.Sprintf("*1\r\n:%d\r\n", at))

	// The other three set variants land the same absolute time.
	want(do("HPEXPIRE", "h", "100000", "FIELDS", "1", "f1"), "*1\r\n:1\r\n")
	want(do("HEXPIREAT", "h", fmt.Sprint(at/1000), "FIELDS", "1", "f1"), "*1\r\n:1\r\n")
	want(do("HPEXPIREAT", "h", fmt.Sprint(at), "FIELDS", "1", "f1"), "*1\r\n:1\r\n")
	want(do("HPEXPIRETIME", "h", "FIELDS", "1", "f1"), fmt.Sprintf("*1\r\n:%d\r\n", at))

	// Conditions on the wire.
	want(do("HEXPIRE", "h", "200", "NX", "FIELDS", "2", "f1", "f2"), "*2\r\n:0\r\n:1\r\n")
	want(do("HPERSIST", "h", "FIELDS", "2", "f1", "f2"), "*2\r\n:1\r\n:1\r\n")
	want(do("HTTL", "h", "FIELDS", "1", "f1"), "*1\r\n:-1\r\n")
	want(do("HEXPIRE", "h", "100", "XX", "FIELDS", "1", "f1"), "*1\r\n:0\r\n")
	want(do("HEXPIRE", "h", "100", "GT", "FIELDS", "1", "f1"), "*1\r\n:0\r\n")
	want(do("HEXPIRE", "h", "100", "LT", "FIELDS", "1", "f1"), "*1\r\n:1\r\n")
	want(do("HEXPIRE", "h", "50", "GT", "FIELDS", "1", "f1"), "*1\r\n:0\r\n")
	want(do("HEXPIRE", "h", "200", "GT", "FIELDS", "1", "f1"), "*1\r\n:1\r\n")

	// A past absolute time deletes on the spot.
	want(do("HPEXPIREAT", "h", fmt.Sprint(clk), "FIELDS", "1", "f1"), "*1\r\n:2\r\n")
	want(do("HGET", "h", "f1"), "$-1\r\n")
	want(do("HLEN", "h"), ":1\r\n")

	// Lazy expiry across a clock advance, then the count-bearing walk
	// purges and the emptied key dies.
	want(do("HEXPIRE", "h", "50", "FIELDS", "1", "f2"), "*1\r\n:1\r\n")
	clk += 60_000
	want(do("HGET", "h", "f2"), "$-1\r\n")
	want(do("HLEN", "h"), ":1\r\n")
	want(do("HGETALL", "h"), "*0\r\n")
	want(do("HLEN", "h"), ":0\r\n")

	// Missing key: -2 per field after the grammar checks.
	want(do("HEXPIRE", "h", "100", "FIELDS", "2", "f1", "f2"), "*2\r\n:-2\r\n:-2\r\n")
	want(do("HTTL", "h", "FIELDS", "1", "f1"), "*1\r\n:-2\r\n")
	want(do("HPERSIST", "h", "FIELDS", "1", "f1"), "*1\r\n:-2\r\n")

	// The grammar's error table, Redis 8 texts verbatim.
	want(do("HEXPIRE", "h", "100", "FIELDS", "1"), "-ERR wrong number of arguments for 'hexpire' command\r\n")
	want(do("HTTL", "h", "FIELDS", "1"), "-ERR wrong number of arguments for 'httl' command\r\n")
	want(do("HPERSIST", "h", "FIELDS", "1"), "-ERR wrong number of arguments for 'hpersist' command\r\n")
	want(do("SET", "s", "v"), "+OK\r\n")
	want(do("HEXPIRE", "s", "notanum", "FIELDS", "1", "f"),
		"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	want(do("HTTL", "s", "FIELDS", "1", "f"),
		"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	want(do("HEXPIRE", "h", "notanum", "FIELDS", "1", "f"),
		"-ERR value is not an integer or out of range\r\n")
	want(do("HEXPIRE", "h", "-1", "FIELDS", "1", "f"),
		"-ERR invalid expire time, must be >= 0\r\n")
	want(do("HEXPIRE", "h", fmt.Sprint(hfeMaxAbsTimeMs/1000+1), "FIELDS", "1", "f"),
		"-ERR invalid expire time in 'hexpire' command\r\n")
	want(do("HPEXPIREAT", "h", fmt.Sprint(hfeMaxAbsTimeMs+1), "FIELDS", "1", "f"),
		"-ERR invalid expire time in 'hpexpireat' command\r\n")
	want(do("HEXPIRE", "h", "100", "BADCOND", "FIELDS", "1", "f"),
		"-ERR Mandatory argument FIELDS is missing or not at the right position\r\n")
	want(do("HTTL", "h", "NOTFIELDS", "1", "f"),
		"-ERR Mandatory argument FIELDS is missing or not at the right position\r\n")
	want(do("HEXPIRE", "h", "100", "FIELDS", "0", "f"),
		"-ERR Parameter `numFields` should be greater than 0\r\n")
	want(do("HEXPIRE", "h", "100", "FIELDS", "x", "f"),
		"-ERR Parameter `numFields` should be greater than 0\r\n")
	want(do("HTTL", "h", "FIELDS", "0", "f"),
		"-ERR Number of fields must be a positive integer\r\n")
	want(do("HPERSIST", "h", "FIELDS", "x", "f"),
		"-ERR Number of fields must be a positive integer\r\n")
	want(do("HEXPIRE", "h", "100", "FIELDS", "2", "f1"),
		"-ERR The `numfields` parameter must match the number of arguments\r\n")
	want(do("HTTL", "h", "FIELDS", "1", "f1", "f2"),
		"-ERR The `numfields` parameter must match the number of arguments\r\n")
}
