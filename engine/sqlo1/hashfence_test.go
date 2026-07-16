package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// segRootOf reads key's stored root payload and decodes it as a
// segmented root, failing the test on anything else. This is the
// fence oracle: decodeHashSegRoot validates ordering, coverage, and
// the mint counter, so a passing decode is most of the invariant.
func (r *hashRig) segRootOf(key string) hashSegRoot {
	r.t.Helper()
	v, root, ok, err := r.tr.Lookup(context.Background(), []byte(key))
	if err != nil || !ok || !root {
		r.t.Fatalf("Lookup(%q): ok=%v root=%v err=%v", key, ok, root, err)
	}
	sr, err := decodeHashSegRoot(v, nil)
	if err != nil {
		r.t.Fatalf("segmented root of %q: %v", key, err)
	}
	return sr
}

// TestHashUpgrade drives both threshold crossings into segments and
// checks the segmented hash against a map reference, hot, cold, and
// reopened.
func TestHashUpgrade(t *testing.T) {
	r := newHashRig(t)
	ctx := context.Background()

	ref := map[string]string{}
	for i := range hashInlineMaxCount + 1 {
		f := fmt.Sprintf("field-%03d", i)
		ref[f] = fmt.Sprintf("v%d", i)
		if !r.hset("h", f, ref[f]) {
			t.Fatalf("field %d not created", i)
		}
	}
	sr := r.segRootOf("h")
	if sr.count != uint64(len(ref)) {
		t.Fatalf("root count = %d, reference holds %d", sr.count, len(ref))
	}
	if sr.rooth == 0 || sr.rootgen != 1 {
		t.Fatalf("upgraded root rooth=%#x rootgen=%d", sr.rooth, sr.rootgen)
	}

	check := func(h *Hash, when string) {
		t.Helper()
		for f, want := range ref {
			v, ok, err := h.HGet(ctx, []byte("h"), []byte(f))
			if err != nil || !ok || string(v) != want {
				t.Fatalf("%s HGET %q = (%q, %v, %v), want %q", when, f, v, ok, err, want)
			}
		}
		if _, ok, err := h.HGet(ctx, []byte("h"), []byte("absent")); ok || err != nil {
			t.Fatalf("%s HGET of a missing field: %v, %v", when, ok, err)
		}
		if n, err := h.HLen(ctx, []byte("h")); err != nil || n != int64(len(ref)) {
			t.Fatalf("%s HLEN = %d, %v, want %d", when, n, err, len(ref))
		}
	}
	check(r.h, "hot")

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	r.tr.EvictAllForTest()
	check(r.h, "cold")
	check(r.reopen(), "reopened")
}

// TestHashSegmentedChurn churns a segmented hash against a map
// reference through updates, deletes, and re-creates, crossing split
// and merge territory, and re-validates the root via the decode
// oracle after every operation.
func TestHashSegmentedChurn(t *testing.T) {
	r := newHashRig(t)
	ctx := context.Background()

	// Values sized so a few hundred fields span several segments.
	val := func(i, gen int) string {
		return fmt.Sprintf("gen%d-%s", gen, strings.Repeat("x", 40+i%25))
	}
	ref := map[string]string{}
	for i := range 300 {
		f := fmt.Sprintf("field-%04d", i)
		ref[f] = val(i, 0)
		created, err := r.h.HSet(ctx, []byte("churn"), []byte(f), []byte(ref[f]))
		if err != nil {
			t.Fatalf("HSET %d: %v", i, err)
		}
		if i >= hashInlineMaxCount+1 && !created {
			t.Fatalf("HSET %d not created", i)
		}
	}
	sr := r.segRootOf("churn")
	if len(sr.fence) < 3 {
		t.Fatalf("300 wide fields produced %d segments, wanted the split exercised", len(sr.fence))
	}

	rng := uint64(0x2545f4914f6cdd1d)
	next := func(n int) int {
		rng ^= rng << 13
		rng ^= rng >> 7
		rng ^= rng << 17
		return int(rng % uint64(n))
	}
	for op := range 3000 {
		f := fmt.Sprintf("field-%04d", next(340))
		switch next(3) {
		case 0, 1:
			v := val(op, 1+op)
			created, err := r.h.HSet(ctx, []byte("churn"), []byte(f), []byte(v))
			if err != nil {
				t.Fatalf("op %d HSET %q: %v", op, f, err)
			}
			if _, there := ref[f]; created == there {
				t.Fatalf("op %d HSET %q created=%v, reference says %v", op, f, created, there)
			}
			ref[f] = v
		case 2:
			removed, err := r.h.HDel(ctx, []byte("churn"), []byte(f))
			if err != nil {
				t.Fatalf("op %d HDEL %q: %v", op, f, err)
			}
			if _, there := ref[f]; removed != there {
				t.Fatalf("op %d HDEL %q removed=%v, reference says %v", op, f, removed, there)
			}
			delete(ref, f)
		}
		sr := r.segRootOf("churn")
		if sr.count != uint64(len(ref)) {
			t.Fatalf("op %d: root count %d, reference holds %d", op, sr.count, len(ref))
		}
		if v, ok, err := r.h.HGet(ctx, []byte("churn"), []byte(f)); err != nil {
			t.Fatalf("op %d HGET %q: %v", op, f, err)
		} else if want, there := ref[f]; ok != there || (ok && string(v) != want) {
			t.Fatalf("op %d HGET %q = (%q, %v), want (%q, %v)", op, f, v, ok, want, there)
		}
	}

	// Cold sweep at the end: every reference field survives eviction.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	r.tr.EvictAllForTest()
	for f, want := range ref {
		v, ok, err := r.h.HGet(ctx, []byte("churn"), []byte(f))
		if err != nil || !ok || string(v) != want {
			t.Fatalf("cold HGET %q = (%q, %v, %v), want %q", f, v, ok, err, want)
		}
	}
}

// TestHashSegmentedMergeShrinks deletes a multi-segment hash down and
// checks the lazy merge folds segments away instead of stranding
// empties.
func TestHashSegmentedMergeShrinks(t *testing.T) {
	r := newHashRig(t)
	ctx := context.Background()

	wide := strings.Repeat("y", 50)
	for i := range 250 {
		r.hset("m", fmt.Sprintf("field-%04d", i), wide)
	}
	grown := len(r.segRootOf("m").fence)
	if grown < 3 {
		t.Fatalf("build produced %d segments, wanted several", grown)
	}
	for i := range 240 {
		if removed, err := r.h.HDel(ctx, []byte("m"), fmt.Appendf(nil, "field-%04d", i)); err != nil || !removed {
			t.Fatalf("HDEL %d = %v, %v", i, removed, err)
		}
	}
	sr := r.segRootOf("m")
	if sr.count != 10 {
		t.Fatalf("count after deletes = %d, want 10", sr.count)
	}
	if len(sr.fence) >= grown {
		t.Fatalf("fence still %d entries after deleting 240 of 250 (was %d)", len(sr.fence), grown)
	}
	for i := 240; i < 250; i++ {
		f := fmt.Sprintf("field-%04d", i)
		if v, ok := r.hget("m", f); !ok || v != wide {
			t.Fatalf("survivor %q = (%q, %v)", f, v, ok)
		}
	}

	// Deleting the rest kills the key, and a recreate starts inline.
	for i := 240; i < 250; i++ {
		if removed, err := r.h.HDel(ctx, []byte("m"), fmt.Appendf(nil, "field-%04d", i)); err != nil || !removed {
			t.Fatalf("final HDEL %d = %v, %v", i, removed, err)
		}
	}
	if exists, _, err := r.s.Entry(ctx, []byte("m")); err != nil || exists {
		t.Fatalf("emptied segmented hash still exists: %v, %v", exists, err)
	}
	r.hset("m", "f", "v")
	if enc, _, _ := r.h.Encoding(ctx, []byte("m")); enc != "listpack" {
		t.Fatalf("recreate after segmented death = %q, want listpack", enc)
	}
}

// TestHashSegmentedTakeover: SET and DEL over a segmented hash retire
// the plane generically through the shared root prefix.
func TestHashSegmentedTakeover(t *testing.T) {
	r := newHashRig(t)
	ctx := context.Background()

	build := func(key string) {
		for i := range 200 {
			r.hset(key, fmt.Sprintf("field-%04d", i), strings.Repeat("z", 50))
		}
		if enc, _, _ := r.h.Encoding(ctx, []byte(key)); enc != "hashtable" {
			r.t.Fatalf("%s did not go segmented", key)
		}
	}

	build("set-over")
	if err := r.s.Set(ctx, []byte("set-over"), []byte("now-a-string")); err != nil {
		t.Fatalf("SET over a segmented hash: %v", err)
	}
	if v, ok, err := r.s.Get(ctx, []byte("set-over")); err != nil || !ok || string(v) != "now-a-string" {
		t.Fatalf("GET after takeover = %q, %v, %v", v, ok, err)
	}

	build("del-over")
	if dead, err := r.s.Del(ctx, []byte("del-over")); err != nil || !dead {
		t.Fatalf("DEL of a segmented hash = %v, %v", dead, err)
	}
	if exists, _, err := r.s.Entry(ctx, []byte("del-over")); err != nil || exists {
		t.Fatalf("deleted segmented hash still exists: %v, %v", exists, err)
	}

	// The key is reusable as a fresh hash after both takeovers.
	r.hset("del-over", "f", "v")
	if v, ok := r.hget("del-over", "f"); !ok || v != "v" {
		t.Fatalf("recreate after DEL takeover = (%q, %v)", v, ok)
	}
}

// TestHashSegRootCodecCorrupt is the corrupt table for the segmented
// root decode: everything checkable without touching segments fails
// at the root read.
func TestHashSegRootCodecCorrupt(t *testing.T) {
	base := func() hashSegRoot {
		return hashSegRoot{
			rootgen:   3,
			rooth:     0xabcdef,
			count:     10,
			nextSegid: 2,
			fence:     []hashFenceEnt{{lo: 0, segid: 0, meta: 0}, {lo: 1 << 40, segid: 1, meta: 2}},
		}
	}
	if _, err := decodeHashSegRoot(appendHashSegRoot(nil, &hashSegRoot{
		rootgen: 1, rooth: 7, count: 1, nextSegid: 1,
		fence: []hashFenceEnt{{lo: 0, segid: 0}},
	}), nil); err != nil {
		t.Fatalf("minimal root rejected: %v", err)
	}

	corrupt := map[string]func() []byte{
		"short header": func() []byte { return []byte{hashSubSeg, 0, 0} },
		"wrong sub": func() []byte {
			r := base()
			p := appendHashSegRoot(nil, &r)
			p[0] = ropeSub
			return p
		},
		"fence paged bit": func() []byte {
			r := base()
			p := appendHashSegRoot(nil, &r)
			p[1] |= hflagFencePaged
			return p
		},
		"reserved hflags": func() []byte {
			r := base()
			p := appendHashSegRoot(nil, &r)
			p[1] |= 0x80
			return p
		},
		"reserved bytes": func() []byte {
			r := base()
			p := appendHashSegRoot(nil, &r)
			p[2] = 1
			return p
		},
		"zero rootgen": func() []byte {
			r := base()
			r.rootgen = 0
			return appendHashSegRoot(nil, &r)
		},
		"zero count": func() []byte {
			r := base()
			r.count = 0
			return appendHashSegRoot(nil, &r)
		},
		"ttl flag disagree": func() []byte {
			r := base()
			p := appendHashSegRoot(nil, &r)
			p[1] |= hflagAnyTTL
			return p
		},
		"truncated fence": func() []byte {
			r := base()
			p := appendHashSegRoot(nil, &r)
			return p[:len(p)-4]
		},
		"first lo nonzero": func() []byte {
			r := base()
			r.fence[0].lo = 5
			return appendHashSegRoot(nil, &r)
		},
		"fence out of order": func() []byte {
			r := base()
			r.fence[1].lo = 0
			return appendHashSegRoot(nil, &r)
		},
		"segid past mint": func() []byte {
			r := base()
			r.fence[1].segid = 9
			return appendHashSegRoot(nil, &r)
		},
	}
	for name, build := range corrupt {
		if _, err := decodeHashSegRoot(build(), nil); err == nil {
			t.Errorf("%s: corrupt segmented root decoded cleanly", name)
		}
	}

	// The fence cap is exact: 128 entries decode, 129 do not.
	full := hashSegRoot{rootgen: 1, rooth: 7, nextSegid: hashFenceMaxSegs + 1, count: 1}
	for i := range hashFenceMaxSegs {
		full.fence = append(full.fence, hashFenceEnt{lo: uint64(i) << 50, segid: uint64(i)})
	}
	if _, err := decodeHashSegRoot(appendHashSegRoot(nil, &full), nil); err != nil {
		t.Fatalf("root at the fence cap rejected: %v", err)
	}
	full.fence = append(full.fence, hashFenceEnt{lo: uint64(hashFenceMaxSegs) << 50, segid: hashFenceMaxSegs})
	full.nextSegid++
	if _, err := decodeHashSegRoot(appendHashSegRoot(nil, &full), nil); err == nil {
		t.Fatal("root past the fence cap decoded cleanly")
	}
}

// TestHashFencePagedBoundary grows one hash until its fence would
// outgrow the root and checks the crossing write fails with the
// paging sentinel while the hash stays intact.
func TestHashFencePagedBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("grows a 128-segment hash")
	}
	r := newHashRig(t)
	ctx := context.Background()

	wide := strings.Repeat("w", 220)
	var paged bool
	var fields int
	for i := range 40000 {
		f := fmt.Sprintf("field-%05d", i)
		_, err := r.h.HSet(ctx, []byte("cap"), []byte(f), []byte(wide))
		if err != nil {
			if !errors.Is(err, errHashFencePaged) {
				t.Fatalf("HSET %d: %v", i, err)
			}
			paged = true
			fields = i
			break
		}
	}
	if !paged {
		t.Fatal("40000 wide fields never hit the fence cap")
	}
	sr := r.segRootOf("cap")
	if len(sr.fence) != hashFenceMaxSegs {
		t.Fatalf("fence at the refusal holds %d entries, want %d", len(sr.fence), hashFenceMaxSegs)
	}
	if sr.count != uint64(fields) {
		t.Fatalf("count after the refusal = %d, want %d", sr.count, fields)
	}
	// The refused write did not land, everything before it did.
	if _, ok := r.hget("cap", fmt.Sprintf("field-%05d", fields)); ok {
		t.Fatal("the refused field is readable")
	}
	for _, i := range []int{0, fields / 2, fields - 1} {
		f := fmt.Sprintf("field-%05d", i)
		if v, ok := r.hget("cap", f); !ok || v != wide {
			t.Fatalf("field %q lost after the refusal: ok=%v", f, ok)
		}
	}
	// Updates of existing fields still work at the cap.
	if created, err := r.h.HSet(ctx, []byte("cap"), []byte("field-00000"), []byte("small")); err != nil || created {
		t.Fatalf("update at the cap = %v, %v", created, err)
	}
	if v, ok := r.hget("cap", "field-00000"); !ok || v != "small" {
		t.Fatalf("update at the cap did not land: (%q, %v)", v, ok)
	}
}
