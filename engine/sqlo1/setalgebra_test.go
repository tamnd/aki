package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// algCollect runs one algebra streamer and returns the emitted
// members copied out (emitted bytes die when emit returns) plus the
// emit count, so duplicate emissions cannot hide behind a map.
func algCollect(t *testing.T, walk func(emit func(m []byte)) error) (map[string]bool, int) {
	t.Helper()
	got := map[string]bool{}
	n := 0
	if err := walk(func(m []byte) {
		got[string(m)] = true
		n++
	}); err != nil {
		t.Fatalf("algebra walk: %v", err)
	}
	return got, n
}

func sinterOf(t *testing.T, se *Set, keys ...string) (map[string]bool, int) {
	t.Helper()
	return algCollect(t, func(emit func(m []byte)) error {
		return se.SInter(context.Background(), keyBytes(keys), emit)
	})
}

func sunionOf(t *testing.T, se *Set, keys ...string) (map[string]bool, int) {
	t.Helper()
	return algCollect(t, func(emit func(m []byte)) error {
		return se.SUnion(context.Background(), keyBytes(keys), emit)
	})
}

func sdiffOf(t *testing.T, se *Set, keys ...string) (map[string]bool, int) {
	t.Helper()
	return algCollect(t, func(emit func(m []byte)) error {
		return se.SDiff(context.Background(), keyBytes(keys), emit)
	})
}

func keyBytes(keys []string) [][]byte {
	out := make([][]byte, len(keys))
	for i, k := range keys {
		out[i] = []byte(k)
	}
	return out
}

// wantSet asserts an algebra result against an oracle map, with the
// emit count pinning that nothing emitted twice.
func wantSet(t *testing.T, got map[string]bool, n int, want map[string]bool) {
	t.Helper()
	if n != len(got) {
		t.Fatalf("emitted %d members, %d distinct: something emitted twice", n, len(got))
	}
	if len(got) != len(want) {
		t.Fatalf("got %d members, want %d", len(got), len(want))
	}
	for m := range want {
		if !got[m] {
			t.Fatalf("member %q missing from the result", m)
		}
	}
}

// rangeSet seeds key with members m<lo>..m<hi-1> and returns the
// oracle map.
func rangeSet(t *testing.T, r *setRig, key string, lo, hi int) map[string]bool {
	t.Helper()
	want := map[string]bool{}
	for i := lo; i < hi; i++ {
		m := fmt.Sprintf("m%05d", i)
		r.sadd(key, m)
		want[m] = true
	}
	return want
}

// interOracle intersects oracle maps.
func interOracle(sets ...map[string]bool) map[string]bool {
	out := map[string]bool{}
	for m := range sets[0] {
		in := true
		for _, s := range sets[1:] {
			if !s[m] {
				in = false
				break
			}
		}
		if in {
			out[m] = true
		}
	}
	return out
}

func TestSInterInline(t *testing.T) {
	r := newSetRig(t)
	for _, m := range []string{"a", "b", "c", "d"} {
		r.sadd("s1", m)
	}
	for _, m := range []string{"b", "c", "e"} {
		r.sadd("s2", m)
	}
	for _, m := range []string{"b", "c", "f"} {
		r.sadd("s3", m)
	}

	got, n := sinterOf(t, r.se, "s1", "s2", "s3")
	wantSet(t, got, n, map[string]bool{"b": true, "c": true})

	// One key is the set itself; duplicate keys change nothing.
	got, n = sinterOf(t, r.se, "s2")
	wantSet(t, got, n, map[string]bool{"b": true, "c": true, "e": true})
	got, n = sinterOf(t, r.se, "s1", "s1", "s2")
	wantSet(t, got, n, map[string]bool{"b": true, "c": true})

	// Any absent key empties the intersection.
	got, n = sinterOf(t, r.se, "s1", "ghost", "s2")
	wantSet(t, got, n, map[string]bool{})
}

func TestSInterDoors(t *testing.T) {
	r := newSetRig(t)
	r.sadd("s", "a")
	if err := r.s.Set(context.Background(), []byte("str"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// A wrong type before any absent key errors.
	err := r.se.SInter(context.Background(), keyBytes([]string{"str", "s"}), func([]byte) {})
	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("SInter(str first) = %v, want ErrWrongType", err)
	}
	// An absent key before the wrong type masks it, Redis's sequential
	// lookup door.
	err = r.se.SInter(context.Background(), keyBytes([]string{"ghost", "str"}), func(m []byte) {
		t.Fatalf("emitted %q from an empty intersection", m)
	})
	if err != nil {
		t.Fatalf("SInter(ghost first) = %v, want the empty result", err)
	}
	// SUNION and SDIFF look at every key, so the same shape errors.
	err = r.se.SUnion(context.Background(), keyBytes([]string{"ghost", "str"}), func([]byte) {})
	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("SUnion(ghost, str) = %v, want ErrWrongType", err)
	}
	err = r.se.SDiff(context.Background(), keyBytes([]string{"ghost", "str"}), func([]byte) {})
	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("SDiff(ghost, str) = %v, want ErrWrongType", err)
	}
}

func TestSInterSegmented(t *testing.T) {
	r := newSetRig(t)
	a := rangeSet(t, r, "a", 0, 800)
	b := rangeSet(t, r, "b", 400, 1600)
	want := interOracle(a, b)
	if len(want) != 400 {
		t.Fatalf("oracle holds %d members, want 400", len(want))
	}

	// Driver is the smaller set whichever side it is on.
	got, n := sinterOf(t, r.se, "a", "b")
	wantSet(t, got, n, want)
	got, n = sinterOf(t, r.se, "b", "a")
	wantSet(t, got, n, want)

	// An inline driver against a segmented target routes through the
	// same window filter.
	r.sadd("tiny", "m00500")
	r.sadd("tiny", "m01500")
	r.sadd("tiny", "nothere")
	got, n = sinterOf(t, r.se, "tiny", "b")
	wantSet(t, got, n, map[string]bool{"m00500": true, "m01500": true})

	// Three-way, mixed representations.
	got, n = sinterOf(t, r.se, "a", "b", "tiny")
	wantSet(t, got, n, map[string]bool{"m00500": true})

	// Cold: a fresh runtime over the same store sees the same algebra.
	if err := r.tr.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se := r.reopen()
	got, n = algCollect(t, func(emit func(m []byte)) error {
		return se.SInter(context.Background(), keyBytes([]string{"a", "b"}), emit)
	})
	wantSet(t, got, n, want)
}

func TestSInterPaged(t *testing.T) {
	r := newSetRig(t)
	// The spop paged shape: long members push the fence past 128
	// segments so the root pages.
	long := func(i int) string {
		return fmt.Sprintf("f%05d-%054d", i, 0)
	}
	for i := range 16000 {
		r.sadd("big", long(i))
	}
	st, _, _, err := r.se.h.stateOf(context.Background(), []byte("big"))
	if err != nil || st != hashSegState {
		t.Fatalf("stateOf(big) = %v, %v", st, err)
	}
	if !r.se.h.segRoot.paged {
		t.Fatal("big is not paged; the fixture lost its point")
	}

	want := map[string]bool{}
	for i := 100; i < 16000; i += 1600 {
		r.sadd("probe", long(i))
		want[long(i)] = true
	}
	r.sadd("probe", "absent-member")

	// The small driver probes across page boundaries.
	got, n := sinterOf(t, r.se, "probe", "big")
	wantSet(t, got, n, want)

	// The paged set as the driver streams its own pages.
	got, n = sinterOf(t, r.se, "big", "probe")
	wantSet(t, got, n, want)

	if err := r.tr.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se := r.reopen()
	got, n = algCollect(t, func(emit func(m []byte)) error {
		return se.SInter(context.Background(), keyBytes([]string{"probe", "big"}), emit)
	})
	wantSet(t, got, n, want)
}

func TestSInterCard(t *testing.T) {
	r := newSetRig(t)
	rangeSet(t, r, "a", 0, 800)
	rangeSet(t, r, "b", 400, 1600)

	card := func(limit int64, keys ...string) int64 {
		t.Helper()
		n, err := r.se.SInterCard(context.Background(), keyBytes(keys), limit)
		if err != nil {
			t.Fatalf("SInterCard(%v, %d): %v", keys, limit, err)
		}
		return n
	}
	if n := card(0, "a", "b"); n != 400 {
		t.Fatalf("unlimited card = %d, want 400", n)
	}
	if n := card(150, "a", "b"); n != 150 {
		t.Fatalf("limited card = %d, want 150", n)
	}
	if n := card(4000, "a", "b"); n != 400 {
		t.Fatalf("over-limit card = %d, want 400", n)
	}
	if n := card(0, "a", "ghost"); n != 0 {
		t.Fatalf("absent-key card = %d, want 0", n)
	}
	if n := card(1, "a", "a"); n != 1 {
		t.Fatalf("self card at limit 1 = %d, want 1", n)
	}
}

func TestSUnion(t *testing.T) {
	r := newSetRig(t)
	a := rangeSet(t, r, "a", 0, 600)
	b := rangeSet(t, r, "b", 300, 900)
	for _, m := range []string{"m00100", "x", "y"} {
		r.sadd("c", m)
	}
	want := map[string]bool{"x": true, "y": true}
	for m := range a {
		want[m] = true
	}
	for m := range b {
		want[m] = true
	}
	if len(want) != 902 {
		t.Fatalf("oracle holds %d members, want 902", len(want))
	}

	// Overlaps collapse across segmented and inline sources, absent
	// keys act as empty, and the emit count proves the dedupe (2101
	// input members, 902 out).
	got, n := sunionOf(t, r.se, "a", "b", "ghost", "c")
	wantSet(t, got, n, want)

	got, n = sunionOf(t, r.se, "ghost", "ghost2")
	wantSet(t, got, n, map[string]bool{})

	got, n = sunionOf(t, r.se, "c")
	wantSet(t, got, n, map[string]bool{"m00100": true, "x": true, "y": true})

	if err := r.tr.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se := r.reopen()
	got, n = algCollect(t, func(emit func(m []byte)) error {
		return se.SUnion(context.Background(), keyBytes([]string{"a", "b", "c"}), emit)
	})
	wantSet(t, got, n, want)
}

func TestSDiff(t *testing.T) {
	r := newSetRig(t)
	a := rangeSet(t, r, "a", 0, 800)
	rangeSet(t, r, "b", 400, 1600)
	r.sadd("c", "m00000")
	r.sadd("c", "m00001")

	want := map[string]bool{}
	for m := range a {
		want[m] = true
	}
	for i := 400; i < 800; i++ {
		delete(want, fmt.Sprintf("m%05d", i))
	}
	delete(want, "m00000")
	delete(want, "m00001")
	if len(want) != 398 {
		t.Fatalf("oracle holds %d members, want 398", len(want))
	}

	got, n := sdiffOf(t, r.se, "a", "b", "c")
	wantSet(t, got, n, want)

	// The first set drives whatever its size; an absent first set is
	// empty and absent rest sets remove nothing.
	got, n = sdiffOf(t, r.se, "ghost", "a")
	wantSet(t, got, n, map[string]bool{})
	got, n = sdiffOf(t, r.se, "c", "ghost")
	wantSet(t, got, n, map[string]bool{"m00000": true, "m00001": true})
	got, n = sdiffOf(t, r.se, "c")
	wantSet(t, got, n, map[string]bool{"m00000": true, "m00001": true})

	// Diffing a set against itself is empty however it is segmented.
	got, n = sdiffOf(t, r.se, "a", "a")
	wantSet(t, got, n, map[string]bool{})

	if err := r.tr.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se := r.reopen()
	got, n = algCollect(t, func(emit func(m []byte)) error {
		return se.SDiff(context.Background(), keyBytes([]string{"a", "b", "c"}), emit)
	})
	wantSet(t, got, n, want)
}
