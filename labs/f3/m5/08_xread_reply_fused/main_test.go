package main

import (
	"testing"
)

// TestArmsAgree is the CI guard: the fused nested reply must be byte-identical to
// the two-phase one across the sweep, including the empty-stream roll-back and the
// all-empty null-array case, since the gate verdict rests on the fused build being
// a drop-in replacement.
func TestArmsAgree(t *testing.T) {
	cases := []struct{ streams, entriesPer, empty int }{
		{1, 0, 0},   // single empty stream -> null array
		{1, 1, 0},   // single one-entry stream
		{1, 100, 0}, // single dense stream
		{3, 10, 0},  // several dense streams
		{4, 10, 2},  // every other stream empty -> roll-back
		{5, 0, 1},   // all streams empty -> null array
		{8, 50, 3},  // sparse empties among dense streams
	}
	for _, c := range cases {
		s := makeStreams(c.streams, c.entriesPer, 2, 8, c.empty)
		a := twoPhase(s, -1, nil)
		b := fused(s, -1, nil)
		if string(a) != string(b) {
			t.Fatalf("streams=%d entries=%d empty=%d: fused != two-phase\n two-phase %q\n fused     %q",
				c.streams, c.entriesPer, c.empty, a, b)
		}
	}
}

// TestLimitHonored confirms the COUNT cap frames the same prefix through both arms:
// a limited read must stop at the cap and still produce byte-identical replies.
func TestLimitHonored(t *testing.T) {
	s := makeStreams(3, 100, 2, 8, 0)
	for _, limit := range []int{1, 5, 50, 200} {
		a := twoPhase(s, limit, nil)
		b := fused(s, limit, nil)
		if string(a) != string(b) {
			t.Fatalf("limit=%d: fused != two-phase\n two-phase %q\n fused     %q", limit, a, b)
		}
	}
}

// TestReusedBufferNoAlias catches the reused-buffer trap: fused truncates and
// rebuilds the same backing buffer per call, so a stale view from a prior call
// must not leak into a later reply.
func TestReusedBufferNoAlias(t *testing.T) {
	buf := make([]byte, 0, 1<<16)
	s1 := makeStreams(4, 30, 2, 8, 0)
	s2 := makeStreams(2, 70, 3, 4, 0)
	buf = fused(s1, -1, buf)
	got1 := string(buf)
	buf = fused(s2, -1, buf)
	got2 := string(buf)
	if got1 == got2 {
		t.Fatal("distinct sources produced identical replies through the reused buffer")
	}
	if want := string(fused(s2, -1, nil)); got2 != want {
		t.Fatalf("reused-buffer reply != fresh-buffer reply\n got  %q\n want %q", got2, want)
	}
}

// TestRollBackLeavesNoBytes confirms an empty stream contributes zero bytes: a
// reply of only-empty streams must be exactly the null array, no stray pair header.
func TestRollBackLeavesNoBytes(t *testing.T) {
	s := makeStreams(5, 0, 2, 8, 1) // all empty
	got := fused(s, -1, nil)
	if want := "*-1\r\n"; string(got) != want {
		t.Fatalf("all-empty fused reply = %q, want %q", got, want)
	}
}

// TestFusedFewerAllocs is the lab's quantitative claim in test form: the fused arm
// allocates strictly less than the two-phase arm on a dense multi-stream read,
// since it drops the gather slices and the per-entry clones.
func TestFusedFewerAllocs(t *testing.T) {
	s := makeStreams(8, 1000, 2, 16, 0)
	buf := make([]byte, 0, 1<<20)
	bufF := make([]byte, 0, 1<<20)
	a2 := testing.AllocsPerRun(50, func() { buf = twoPhase(s, -1, buf) })
	af := testing.AllocsPerRun(50, func() { bufF = fused(s, -1, bufF) })
	if !(af < a2) {
		t.Fatalf("fused allocs %.0f not below two-phase allocs %.0f", af, a2)
	}
}
