package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// randAll drains one HRandFieldCount call: the announced count, the
// emits in order, and the begin-before-emit contract.
func randAll(t *testing.T, h *Hash, key string, count int64, withReplacement bool) (int64, []string, []string) {
	t.Helper()
	announced := int64(-1)
	var fields, vals []string
	err := h.HRandFieldCount(context.Background(), []byte(key), count, withReplacement, func(n int64) {
		if announced != -1 {
			t.Fatalf("begin ran twice")
		}
		announced = n
	}, func(f, v []byte) {
		if announced == -1 {
			t.Fatalf("emit before begin")
		}
		fields = append(fields, string(f))
		vals = append(vals, string(v))
	})
	if err != nil {
		t.Fatalf("HRandFieldCount(%q, %d, %v): %v", key, count, withReplacement, err)
	}
	if announced != int64(len(fields)) {
		t.Fatalf("HRandFieldCount(%q, %d, %v) announced %d, emitted %d", key, count, withReplacement, announced, len(fields))
	}
	return announced, fields, vals
}

// distinctMembers asserts fields are pairwise distinct members of
// want with the matching values.
func distinctMembers(t *testing.T, want map[string]string, fields, vals []string) {
	t.Helper()
	seen := map[string]bool{}
	for i, f := range fields {
		wv, ok := want[f]
		if !ok {
			t.Fatalf("draw %d returned %q, not a member", i, f)
		}
		if vals[i] != wv {
			t.Fatalf("draw %d: field %q value %q, want %q", i, f, vals[i], wv)
		}
		if seen[f] {
			t.Fatalf("distinct sample returned %q twice", f)
		}
		seen[f] = true
	}
}

func TestHRandFieldSingle(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)
	r.h.rngState = 1

	_, _, ok, err := r.h.HRandField(ctx, []byte("missing"))
	if err != nil || ok {
		t.Fatalf("HRandField(missing) = ok=%v, %v; want absent", ok, err)
	}

	if err := r.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := r.h.HRandField(ctx, []byte("str")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("HRandField on a string = %v, want ErrWrongType", err)
	}

	r.hset("h", "a", "1")
	r.hset("h", "b", "2")
	r.hset("h", "c", "3")
	want := map[string]string{"a": "1", "b": "2", "c": "3"}
	hit := map[string]bool{}
	for range 300 {
		f, v, ok, err := r.h.HRandField(ctx, []byte("h"))
		if err != nil || !ok {
			t.Fatalf("HRandField = ok=%v, %v", ok, err)
		}
		if want[string(f)] != string(v) {
			t.Fatalf("HRandField drew %q=%q, not a member pair", f, v)
		}
		hit[string(f)] = true
	}
	if len(hit) != 3 {
		t.Fatalf("300 draws over 3 fields hit only %v", hit)
	}
}

func TestHRandFieldCountInline(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)
	r.h.rngState = 2

	if n, _, _ := randAll(t, r.h, "missing", 5, false); n != 0 {
		t.Fatalf("absent key announced %d", n)
	}
	if err := r.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	err := r.h.HRandFieldCount(ctx, []byte("str"), 2, false, func(int64) {}, func(f, v []byte) {})
	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("HRandFieldCount on a string = %v, want ErrWrongType", err)
	}

	want := map[string]string{}
	for _, f := range []string{"a", "b", "c", "d", "e"} {
		r.hset("h", f, "v"+f)
		want[f] = "v" + f
	}
	if n, _, _ := randAll(t, r.h, "h", 0, false); n != 0 {
		t.Fatalf("count 0 announced %d", n)
	}

	// count above HLEN returns everything, in entry order.
	n, fields, _ := randAll(t, r.h, "h", 10, false)
	if n != 5 || fmt.Sprint(fields) != fmt.Sprint(r.fields("h")) {
		t.Fatalf("count 10 of 5 = %d %v, want the full entry order", n, fields)
	}

	// A partial sample is distinct, member-only, and in entry order.
	order := map[string]int{}
	for i, f := range r.fields("h") {
		order[f] = i
	}
	for range 50 {
		n, fields, vals := randAll(t, r.h, "h", 2, false)
		if n != 2 {
			t.Fatalf("count 2 announced %d", n)
		}
		distinctMembers(t, want, fields, vals)
		if order[fields[0]] >= order[fields[1]] {
			t.Fatalf("distinct inline sample out of entry order: %v", fields)
		}
	}

	// Replacement draws hit every member across enough tries.
	n, fields, vals := randAll(t, r.h, "h", 200, true)
	if n != 200 {
		t.Fatalf("replacement count announced %d", n)
	}
	hit := map[string]bool{}
	for i, f := range fields {
		if want[f] != vals[i] {
			t.Fatalf("replacement draw %d: %q=%q not a member pair", i, f, vals[i])
		}
		hit[f] = true
	}
	if len(hit) != 5 {
		t.Fatalf("200 replacement draws over 5 fields hit only %v", hit)
	}
}

func TestHRandFieldCountSegmented(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)
	r.h.rngState = 3
	val := segRigHash(t, r, "big", 200)
	want := map[string]string{}
	for i := range 200 {
		want[fmt.Sprintf("f%03d", i)] = val(i)
	}

	// count >= HLEN walks everything exactly once.
	n, fields, vals := randAll(t, r.h, "big", 500, false)
	if n != 200 {
		t.Fatalf("count 500 of 200 announced %d", n)
	}
	distinctMembers(t, want, fields, vals)

	// count*3 >= HLEN runs the reservoir.
	n, fields, vals = randAll(t, r.h, "big", 150, false)
	if n != 150 {
		t.Fatalf("reservoir count announced %d", n)
	}
	distinctMembers(t, want, fields, vals)

	// Below the reservoir line, rejection over the weighted primitive.
	n, fields, vals = randAll(t, r.h, "big", 20, false)
	if n != 20 {
		t.Fatalf("rejection count announced %d", n)
	}
	distinctMembers(t, want, fields, vals)

	// Replacement coverage: 6000 draws over 200 fields see them all.
	n, fields, vals = randAll(t, r.h, "big", 6000, true)
	if n != 6000 {
		t.Fatalf("replacement count announced %d", n)
	}
	hit := map[string]bool{}
	for i, f := range fields {
		if want[f] != vals[i] {
			t.Fatalf("replacement draw %d: %q not a member pair", i, f)
		}
		hit[f] = true
	}
	if len(hit) != 200 {
		t.Fatalf("6000 replacement draws hit only %d of 200 fields", len(hit))
	}

	// The single form draws members too.
	f, v, ok, err := r.h.HRandField(ctx, []byte("big"))
	if err != nil || !ok || want[string(f)] != string(v) {
		t.Fatalf("segmented HRandField = %q=%q ok=%v, %v", f, v, ok, err)
	}

	// Cold arm: every ladder rung answers off a fresh runtime.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	h2 := r.reopen()
	h2.rngState = 4
	for _, tc := range []struct {
		count int64
		repl  bool
		wantN int64
	}{{20, false, 20}, {150, false, 150}, {300, false, 200}, {40, true, 40}} {
		n, fields, vals := randAll(t, h2, "big", tc.count, tc.repl)
		if n != tc.wantN {
			t.Fatalf("cold count %d repl=%v announced %d, want %d", tc.count, tc.repl, n, tc.wantN)
		}
		if !tc.repl {
			distinctMembers(t, want, fields, vals)
		} else {
			for i, f := range fields {
				if want[f] != vals[i] {
					t.Fatalf("cold replacement draw %d: %q not a member pair", i, f)
				}
			}
		}
	}
}

// TestFenceFillValve drives the rejection sampler's valve directly:
// honest draws cannot make 20*count+100 tries all collide, so the
// valve is exercised with a hand-primed picked map. With the first
// segment fully picked the fill must skip into the second, and the
// entries it emits must be the first unpicked ones in fence order.
func TestFenceFillValve(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)
	segRigHash(t, r, "big", 200)

	if st, _, _, err := r.h.stateOf(ctx, []byte("big")); err != nil || st != hashSegState {
		t.Fatalf("stateOf(big) = %v, %v; want segmented", st, err)
	}
	root := &r.h.segRoot
	if len(root.fence) < 2 {
		t.Fatalf("rig built only %d segments", len(root.fence))
	}

	// The fence-order oracle: every entry with its dedupe key.
	type ent struct {
		key  uint64
		f, v string
	}
	var all []ent
	for i := range root.fence {
		seg, err := r.h.readSeg(ctx, root.fence[i].segid)
		if err != nil {
			t.Fatal(err)
		}
		it := hashEntryIter{p: seg.entries}
		for j := 0; ; j++ {
			f, v, _, ok, err := it.next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			all = append(all, ent{root.fence[i].segid*4096 + uint64(j), string(f), string(v)})
		}
	}

	// Poison the whole first segment plus every third entry after it.
	firstSeg := root.fence[0].segid
	r.h.picked = map[uint64]struct{}{}
	var wantFill []ent
	for i, e := range all {
		if e.key/4096 == firstSeg || i%3 == 0 {
			r.h.picked[e.key] = struct{}{}
		} else {
			wantFill = append(wantFill, e)
		}
	}

	got := []ent{}
	remaining, err := r.h.fenceFill(ctx, 10, func(f, v []byte) {
		got = append(got, ent{0, string(f), string(v)})
	})
	if err != nil || remaining != 0 {
		t.Fatalf("fenceFill = remaining %d, %v", remaining, err)
	}
	for i := range got {
		if got[i].f != wantFill[i].f || got[i].v != wantFill[i].v {
			t.Fatalf("fill entry %d = %q, want %q (fence order with picked skipped)", i, got[i].f, wantFill[i].f)
		}
	}

	// Ran dry: ask for more than the unpicked residue holds.
	for _, e := range all {
		r.h.picked[e.key] = struct{}{}
	}
	remaining, err = r.h.fenceFill(ctx, 5, func(f, v []byte) {
		t.Fatalf("fully picked fill emitted %q", f)
	})
	if err != nil || remaining != 5 {
		t.Fatalf("dry fenceFill = remaining %d, %v; want 5", remaining, err)
	}
}
