package main

import (
	"testing"
)

// TestArmsAgree is what CI drives: the fused reply build must be byte-identical
// to the two-phase one across the window sweep, since the gate verdict rests on
// the fused arm being a drop-in replacement. A reply-shape drift would make the
// speed numbers meaningless.
func TestArmsAgree(t *testing.T) {
	for _, w := range []int{0, 1, 2, 10, 100, 1000} {
		s := makeSource(w, 2, 8, 16)
		a := twoPhase(s, nil)
		b := fused(s, nil)
		if string(a) != string(b) {
			t.Fatalf("window %d: fused reply != two-phase reply\n two-phase %q\n fused     %q", w, a, b)
		}
	}
}

// TestPrependArrayHeader pins the header-shift helper: shifting a body right to
// make room for the array header must leave the body bytes intact and place a
// well-formed header in front, across counts whose decimal width changes (the
// shift width tracks the digit count).
func TestPrependArrayHeader(t *testing.T) {
	for _, n := range []int{0, 1, 9, 10, 99, 100, 12345} {
		body := []byte("BODYBYTES")
		buf := append([]byte(nil), body...)
		buf = prependArrayHeader(buf, 0, n)
		want := string(appendArrayHeader(nil, n)) + string(body)
		if string(buf) != want {
			t.Fatalf("n=%d: got %q want %q", n, buf, want)
		}
	}
}

// TestPrependAtOffset confirms the shift respects a non-zero body offset: a reply
// buffer that already holds bytes before the range body (the reused cx.Aux case)
// must keep its prefix and only shift the body written after it.
func TestPrependAtOffset(t *testing.T) {
	prefix := []byte("PREFIX")
	body := []byte("abcdef")
	buf := append(append([]byte(nil), prefix...), body...)
	buf = prependArrayHeader(buf, len(prefix), 3)
	want := string(prefix) + string(appendArrayHeader(nil, 3)) + string(body)
	if string(buf) != want {
		t.Fatalf("offset prepend: got %q want %q", buf, want)
	}
}

// TestReusedBufferNoAlias catches the reused-buffer trap: fused() truncates and
// rebuilds the same backing buffer per call, so a stale view from a prior call
// must not leak into a later reply. Two different sources through one buffer must
// each read back their own bytes.
func TestReusedBufferNoAlias(t *testing.T) {
	buf := make([]byte, 0, 1<<16)
	s1 := makeSource(50, 2, 8, 16)
	s2 := makeSource(120, 3, 4, 8)
	buf = fused(s1, buf)
	got1 := string(buf)
	buf = fused(s2, buf)
	got2 := string(buf)
	if got1 == got2 {
		t.Fatal("distinct sources produced identical replies through the reused buffer")
	}
	if want := string(fused(s2, nil)); got2 != want {
		t.Fatalf("reused-buffer reply != fresh-buffer reply\n got  %q\n want %q", got2, want)
	}
}

// TestFusedFewerAllocs is the lab's quantitative claim in test form: the fused
// arm allocates strictly less than the two-phase arm on a non-trivial window,
// because it drops the gather slice and the per-entry clones. If a refactor ever
// reintroduced a per-entry alloc this would catch it.
func TestFusedFewerAllocs(t *testing.T) {
	s := makeSource(1000, 2, 8, 16)
	buf := make([]byte, 0, 1<<20)
	bufF := make([]byte, 0, 1<<20)
	a2 := testing.AllocsPerRun(100, func() { buf = twoPhase(s, buf) })
	af := testing.AllocsPerRun(100, func() { bufF = fused(s, bufF) })
	if !(af < a2) {
		t.Fatalf("fused allocs %.0f not below two-phase allocs %.0f", af, a2)
	}
}
