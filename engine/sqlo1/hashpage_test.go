package sqlo1

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// The paged-fence rungs: the 129th segment moves the fence into rtype
// 5 pages behind the root's page index, and everything the flat fence
// answered keeps answering through loadPage. These tests drive the
// transition and the first page split with fat values, then re-check
// the surface hot, cold, and reopened.

// pageRigHash builds a hash under key whose n fat fields push the
// fence well past hashFenceMaxSegs segments, and returns the value
// maker. 800-byte values put about 4 entries in a segment, so 600
// fields cross the paging transition and 1200 split page zero.
func pageRigHash(t *testing.T, r *hashRig, key string, n int) func(i int) string {
	t.Helper()
	val := func(i int) string {
		return fmt.Sprintf("v-%d-%s", i, strings.Repeat("y", 800))
	}
	for i := range n {
		r.hset(key, fmt.Sprintf("f%04d", i), val(i))
	}
	return val
}

// TestHashPagedTransition crosses the 128-segment boundary and checks
// the paged root against the full surface: exact count, every field
// readable, full iteration, the HSCAN cursor across pages, the
// 3-record cold read path, and a plane retire.
func TestHashPagedTransition(t *testing.T) {
	if testing.Short() {
		t.Skip("grows a paged hash")
	}
	r := newHashRig(t)
	ctx := context.Background()
	const n = 600

	val := pageRigHash(t, r, "pg", n)
	sr := r.segRootOf("pg")
	if !sr.paged {
		t.Fatalf("%d fat fields left the fence flat at %d segments", n, len(sr.fence))
	}
	if len(sr.pidx) < 1 {
		t.Fatal("paged root with an empty page index")
	}
	if sr.count != n {
		t.Fatalf("paged root count = %d, want %d", sr.count, n)
	}
	if enc, _, err := r.h.Encoding(ctx, []byte("pg")); err != nil || enc != "hashtable" {
		t.Fatalf("paged encoding = %q, %v", enc, err)
	}
	if hlen, err := r.h.HLen(ctx, []byte("pg")); err != nil || hlen != n {
		t.Fatalf("HLEN = %d, %v", hlen, err)
	}
	for _, i := range []int{0, 1, n / 3, n / 2, n - 2, n - 1} {
		f := fmt.Sprintf("f%04d", i)
		if v, ok := r.hget("pg", f); !ok || v != val(i) {
			t.Fatalf("field %q = (%.20q, %v)", f, v, ok)
		}
	}
	if _, ok := r.hget("pg", "absent"); ok {
		t.Fatal("absent field readable on a paged hash")
	}

	// Full iteration covers every field exactly once.
	got, count := iterAll(t, r.h, "pg")
	if count != n {
		t.Fatalf("iterate count = %d, want %d", count, n)
	}
	for i := range n {
		f := fmt.Sprintf("f%04d", i)
		if got[f] != val(i) {
			t.Fatalf("iterate field %s = %.20q", f, got[f])
		}
	}

	// The HSCAN cursor walks across page boundaries without loss.
	scanned := map[string]string{}
	cursor := uint64(0)
	for steps := 0; ; steps++ {
		next, err := r.h.HScan(ctx, []byte("pg"), cursor, 40, func(f, v []byte) {
			scanned[string(f)] = string(v)
		})
		if err != nil {
			t.Fatalf("HScan step %d: %v", steps, err)
		}
		if next == 0 {
			break
		}
		cursor = next
		if steps > n {
			t.Fatal("HScan cursor never terminated")
		}
	}
	if len(scanned) != n {
		t.Fatalf("HScan covered %d fields, want %d", len(scanned), n)
	}

	// Cold: the point read pays exactly three IO rounds, root then
	// fence page then segment; a fresh runtime reproduces the hash.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	h2 := r.reopen()
	before := r.rs.readRounds
	if v, ok, err := h2.HGet(ctx, []byte("pg"), []byte("f0000")); err != nil || !ok || string(v) != val(0) {
		t.Fatalf("cold HGET = (%.20q, %v, %v)", v, ok, err)
	}
	if rounds := r.rs.readRounds - before; rounds != 3 {
		t.Fatalf("cold paged HGET took %d IO rounds, want 3", rounds)
	}
	if got, count := iterAll(t, h2, "pg"); count != n || len(got) != n {
		t.Fatalf("cold iterate = %d fields", count)
	}

	// HMGET across the paged fence, cold, still batches correctly.
	fields := [][]byte{[]byte("f0000"), []byte("f0300"), []byte("f0599"), []byte("nope")}
	var mv []string
	var mok []bool
	err := h2.HMGet(ctx, []byte("pg"), fields, func(v []byte, ok bool) {
		mv = append(mv, string(v))
		mok = append(mok, ok)
	})
	if err != nil {
		t.Fatalf("cold HMGET: %v", err)
	}
	if !mok[0] || mv[0] != val(0) || !mok[1] || mv[1] != val(300) || !mok[2] || mv[2] != val(599) || mok[3] {
		t.Fatalf("cold HMGET = %v", mok)
	}

	// SET over the paged hash retires the plane like any other root.
	if err := r.s.Set(ctx, []byte("pg"), []byte("now-a-string")); err != nil {
		t.Fatalf("SET over a paged hash: %v", err)
	}
	if v, ok, err := r.s.Get(ctx, []byte("pg")); err != nil || !ok || string(v) != "now-a-string" {
		t.Fatalf("GET after takeover = (%q, %v, %v)", v, ok, err)
	}
}

// TestHashPagedSplitAndMerge grows past a page split, then deletes
// most fields so the in-page merges fold segments away, and finally
// empties the key.
func TestHashPagedSplitAndMerge(t *testing.T) {
	if testing.Short() {
		t.Skip("grows a two-page hash")
	}
	r := newHashRig(t)
	ctx := context.Background()
	const n = 1200

	val := pageRigHash(t, r, "sp", n)
	sr := r.segRootOf("sp")
	if !sr.paged || len(sr.pidx) < 2 {
		t.Fatalf("%d fat fields produced paged=%v with %d pages, wanted a page split", n, sr.paged, len(sr.pidx))
	}
	if sr.count != n {
		t.Fatalf("count after the split = %d, want %d", sr.count, n)
	}
	got, count := iterAll(t, r.h, "sp")
	if count != n {
		t.Fatalf("iterate count = %d, want %d", count, n)
	}
	for i := range n {
		f := fmt.Sprintf("f%04d", i)
		if got[f] != val(i) {
			t.Fatalf("iterate field %s = %.20q", f, got[f])
		}
	}

	// Delete 90 percent: the lazy merges run inside pages and the
	// count stays exact throughout.
	for i := range n - 120 {
		f := fmt.Appendf(nil, "f%04d", i)
		if removed, err := r.h.HDel(ctx, []byte("sp"), f); err != nil || !removed {
			t.Fatalf("HDEL %d = %v, %v", i, removed, err)
		}
	}
	sr = r.segRootOf("sp")
	if sr.count != 120 {
		t.Fatalf("count after deletes = %d, want 120", sr.count)
	}
	for i := n - 120; i < n; i++ {
		f := fmt.Sprintf("f%04d", i)
		if v, ok := r.hget("sp", f); !ok || v != val(i) {
			t.Fatalf("survivor %q lost: ok=%v", f, ok)
		}
	}

	// Cold reopen over the shrunken paged root.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got, count := iterAll(t, r.reopen(), "sp"); count != 120 || len(got) != 120 {
		t.Fatalf("cold iterate after deletes = %d fields", count)
	}

	// Emptying the hash kills the key, and a recreate starts inline.
	for i := n - 120; i < n; i++ {
		if removed, err := r.h.HDel(ctx, []byte("sp"), fmt.Appendf(nil, "f%04d", i)); err != nil || !removed {
			t.Fatalf("final HDEL %d = %v, %v", i, removed, err)
		}
	}
	if exists, _, err := r.s.Entry(ctx, []byte("sp")); err != nil || exists {
		t.Fatalf("emptied paged hash still exists: %v, %v", exists, err)
	}
	r.hset("sp", "f", "v")
	if enc, _, _ := r.h.Encoding(ctx, []byte("sp")); enc != "listpack" {
		t.Fatalf("recreate after paged death = %q, want listpack", enc)
	}
}

// TestHashPagedRand runs the HRANDFIELD ladder over a paged hash:
// the two-level draw, replacement sampling, and all three distinct
// rungs answer from pages exactly like the flat fence.
func TestHashPagedRand(t *testing.T) {
	if testing.Short() {
		t.Skip("grows a paged hash")
	}
	r := newHashRig(t)
	ctx := context.Background()
	const n = 600

	val := pageRigHash(t, r, "rnd", n)
	if !r.segRootOf("rnd").paged {
		t.Fatal("rig hash did not page")
	}
	want := map[string]string{}
	for i := range n {
		want[fmt.Sprintf("f%04d", i)] = val(i)
	}
	r.h.rngState = 1

	f, v, ok, err := r.h.HRandField(ctx, []byte("rnd"))
	if err != nil || !ok {
		t.Fatalf("HRandField = %v, %v", ok, err)
	}
	if want[string(f)] != string(v) {
		t.Fatalf("HRandField returned %q, not a member", f)
	}

	// With replacement: every draw is a member.
	_, fields, vals := randAll(t, r.h, "rnd", 50, true)
	if len(fields) != 50 {
		t.Fatalf("replacement drew %d, want 50", len(fields))
	}
	for i, f := range fields {
		if want[f] != vals[i] {
			t.Fatalf("replacement draw %d: %q", i, f)
		}
	}

	// Distinct, rejection rung (count*3 < hlen).
	announced, fields, vals := randAll(t, r.h, "rnd", 20, false)
	if announced != 20 {
		t.Fatalf("distinct announced %d, want 20", announced)
	}
	distinctMembers(t, want, fields, vals)

	// Distinct, reservoir rung (count*3 >= hlen).
	announced, fields, vals = randAll(t, r.h, "rnd", 250, false)
	if announced != 250 {
		t.Fatalf("reservoir announced %d, want 250", announced)
	}
	distinctMembers(t, want, fields, vals)

	// Distinct, emit-all rung (count >= hlen).
	announced, fields, vals = randAll(t, r.h, "rnd", n+100, false)
	if announced != n {
		t.Fatalf("emit-all announced %d, want %d", announced, n)
	}
	distinctMembers(t, want, fields, vals)
}
