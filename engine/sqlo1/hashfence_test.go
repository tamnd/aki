package sqlo1

import (
	"context"
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
	sr, err := decodeHashSegRoot(v, nil, nil)
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
			sub:       hashSubSeg,
			rootgen:   3,
			rooth:     0xabcdef,
			count:     10,
			nextSegid: 2,
			fence:     []hashFenceEnt{{lo: 0, segid: 0, meta: 0}, {lo: 1 << 40, segid: 1, meta: 2}},
		}
	}
	if _, err := decodeHashSegRoot(appendHashSegRoot(nil, &hashSegRoot{
		sub:     hashSubSeg,
		rootgen: 1, rooth: 7, count: 1, nextSegid: 1,
		fence: []hashFenceEnt{{lo: 0, segid: 0}},
	}), nil, nil); err != nil {
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
		"paged bit over zero-weight entries": func() []byte {
			// Flipping the paged bit reinterprets the fence entries as
			// index entries; the first one's meta of 0 becomes a page
			// weight of 0, which no real page can have.
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
		if _, err := decodeHashSegRoot(build(), nil, nil); err == nil {
			t.Errorf("%s: corrupt segmented root decoded cleanly", name)
		}
	}

	// The flat fence cap is exact: 128 entries decode, 129 do not (a
	// 129th segment pages the fence instead of growing it).
	full := hashSegRoot{sub: hashSubSeg, rootgen: 1, rooth: 7, nextSegid: hashFenceMaxSegs + 1, count: 1}
	for i := range hashFenceMaxSegs {
		full.fence = append(full.fence, hashFenceEnt{lo: uint64(i) << 50, segid: uint64(i)})
	}
	if _, err := decodeHashSegRoot(appendHashSegRoot(nil, &full), nil, nil); err != nil {
		t.Fatalf("root at the fence cap rejected: %v", err)
	}
	full.fence = append(full.fence, hashFenceEnt{lo: uint64(hashFenceMaxSegs) << 50, segid: hashFenceMaxSegs})
	full.nextSegid++
	if _, err := decodeHashSegRoot(appendHashSegRoot(nil, &full), nil, nil); err == nil {
		t.Fatal("root past the fence cap decoded cleanly")
	}
}

// TestHashPagedRootCodec is the decode table for paged roots and the
// fence page payload: the valid shapes round-trip and everything
// checkable without the pages themselves fails at the root read.
func TestHashPagedRootCodec(t *testing.T) {
	base := func() hashSegRoot {
		return hashSegRoot{
			sub:     hashSubSeg,
			rootgen: 2, rooth: 0xfeed, count: 600, nextSegid: 131, paged: true,
			pidx: []hashPageEnt{
				{lo: 0, pageid: 129, weight: 40},
				{lo: 1 << 41, pageid: 130, weight: 25},
			},
		}
	}
	r := base()
	sr, err := decodeHashSegRoot(appendHashSegRoot(nil, &r), nil, nil)
	if err != nil {
		t.Fatalf("valid paged root rejected: %v", err)
	}
	if !sr.paged || sr.pi != -1 || len(sr.fence) != 0 {
		t.Fatalf("paged decode state: paged=%v pi=%d fence=%d", sr.paged, sr.pi, len(sr.fence))
	}
	if len(sr.pidx) != 2 || sr.pidx[0] != (hashPageEnt{0, 129, 40}) || sr.pidx[1] != (hashPageEnt{1 << 41, 130, 25}) {
		t.Fatalf("page index round-trip: %+v", sr.pidx)
	}

	corrupt := map[string]func() []byte{
		"empty page index": func() []byte {
			r := base()
			r.pidx = nil
			return appendHashSegRoot(nil, &r)
		},
		"index first lo nonzero": func() []byte {
			r := base()
			r.pidx[0].lo = 9
			return appendHashSegRoot(nil, &r)
		},
		"index out of order": func() []byte {
			r := base()
			r.pidx[1].lo = 0
			return appendHashSegRoot(nil, &r)
		},
		"pageid at mint": func() []byte {
			r := base()
			r.pidx[1].pageid = r.nextSegid
			return appendHashSegRoot(nil, &r)
		},
		"zero page weight": func() []byte {
			r := base()
			r.pidx[1].weight = 0
			return appendHashSegRoot(nil, &r)
		},
		"truncated index": func() []byte {
			r := base()
			p := appendHashSegRoot(nil, &r)
			return p[:len(p)-4]
		},
		"index past the cap": func() []byte {
			r := base()
			r.pidx = r.pidx[:0]
			for i := range hashFencePageIdxMax + 1 {
				r.pidx = append(r.pidx, hashPageEnt{lo: uint64(i) << 40, pageid: uint64(i), weight: 1})
			}
			r.nextSegid = uint64(hashFencePageIdxMax) + 2
			return appendHashSegRoot(nil, &r)
		},
	}
	for name, build := range corrupt {
		if _, err := decodeHashSegRoot(build(), nil, nil); err == nil {
			t.Errorf("%s: corrupt paged root decoded cleanly", name)
		}
	}
}

// TestHashFencePageCodec is the decode table for the rtype 5 page
// payload itself.
func TestHashFencePageCodec(t *testing.T) {
	ents := []hashFenceEnt{
		{lo: 3, segid: 4, meta: 2},
		{lo: 1 << 30, segid: 9, meta: 0x12},
	}
	got, err := decodeHashFencePage(appendHashFencePage(nil, ents), nil, 10)
	if err != nil {
		t.Fatalf("valid fence page rejected: %v", err)
	}
	if len(got) != 2 || got[0] != ents[0] || got[1] != ents[1] {
		t.Fatalf("fence page round-trip: %+v", got)
	}
	// A page's first lo is not forced to zero: only page 0 covers from
	// zero, and loadPage checks the head against the index instead.
	if _, err := decodeHashFencePage(appendHashFencePage(nil, ents), nil, 0); err != nil {
		t.Fatalf("segid bound not skipped at nextSegid 0: %v", err)
	}
	if _, err := decodeHashFencePage(appendHashFencePage(nil, ents), nil, 9); err == nil {
		t.Fatal("segid at the mint counter decoded cleanly")
	}

	corrupt := map[string]func() []byte{
		"short header": func() []byte { return []byte{1, 0} },
		"reserved bytes": func() []byte {
			p := appendHashFencePage(nil, ents)
			p[2] = 1
			return p
		},
		"zero entries": func() []byte { return appendHashFencePage(nil, nil) },
		"length mismatch": func() []byte {
			p := appendHashFencePage(nil, ents)
			return p[:len(p)-4]
		},
		"out of order": func() []byte {
			bad := []hashFenceEnt{{lo: 5, segid: 1}, {lo: 5, segid: 2}}
			return appendHashFencePage(nil, bad)
		},
		"past the page cap": func() []byte {
			var big []hashFenceEnt
			for i := range hashFencePageMax + 1 {
				big = append(big, hashFenceEnt{lo: uint64(i), segid: uint64(i)})
			}
			return appendHashFencePage(nil, big)
		},
	}
	for name, build := range corrupt {
		if _, err := decodeHashFencePage(build(), nil, 0); err == nil {
			t.Errorf("%s: corrupt fence page decoded cleanly", name)
		}
	}
}
