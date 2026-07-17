package store

import (
	"bytes"
	"testing"
)

// TestAkiSpillProvisionalResolvesToLogWord stages a batch of runs, reads each
// back from the pending buffer while provisional, resolves the batch, and reads
// each run from its published log word: the deferred-publish round trip writeRun
// will ride.
func TestAkiSpillProvisionalResolvesToLogWord(t *testing.T) {
	sp := newAkiSpill(newTestAkiVlog(t, 1))

	runs := [][]byte{[]byte("first"), bytes.Repeat([]byte("m"), 2500), []byte("third")}
	words := make([]uint64, len(runs))
	for i, r := range runs {
		w := sp.stageRun(r, nil)
		if !isProvisional(w) {
			t.Fatalf("run %d word %#x not provisional", i, w)
		}
		if provisionalIndex(w) != i {
			t.Fatalf("run %d provisional index = %d", i, provisionalIndex(w))
		}
		got, err := sp.readProvisional(w)
		if err != nil {
			t.Fatalf("read provisional %d: %v", i, err)
		}
		if !bytes.Equal(got, r) {
			t.Fatalf("provisional read %d = %q, want %q", i, got, r)
		}
		words[i] = w
	}
	if sp.staged() != len(runs) {
		t.Fatalf("staged = %d, want %d", sp.staged(), len(runs))
	}

	resolved, err := sp.resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(resolved) != len(runs) {
		t.Fatalf("resolved %d words, want %d", len(resolved), len(runs))
	}
	var buf []byte
	for i := range runs {
		w := resolved[i]
		if w&inLogBit == 0 {
			t.Fatalf("resolved word %d %#x missing inLogBit", i, w)
		}
		if isProvisional(w) {
			t.Fatalf("resolved word %d still provisional", i)
		}
		got, err := sp.v.readAt(w&addrMask, len(runs[i]), buf)
		if err != nil {
			t.Fatalf("read log word %d: %v", i, err)
		}
		if !bytes.Equal(got, runs[i]) {
			t.Fatalf("log read %d = %q, want %q", i, got, runs[i])
		}
		buf = got[:0]
	}
}

// TestAkiSpillTwoPartRunIsOneFrame stages a run assembled from a and b and
// confirms it reads back as one contiguous blob of len(a)+len(b), so the
// two-part form writeRun uses survives the per-value framing.
func TestAkiSpillTwoPartRunIsOneFrame(t *testing.T) {
	sp := newAkiSpill(newTestAkiVlog(t, 0))

	a := []byte("head-")
	b := []byte("tail")
	w := sp.stageRun(a, b)

	got, err := sp.readProvisional(w)
	if err != nil {
		t.Fatalf("read provisional: %v", err)
	}
	if string(got) != "head-tail" {
		t.Fatalf("provisional two-part read = %q, want head-tail", got)
	}

	resolved, err := sp.resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	got, err = sp.v.readAt(resolved[0]&addrMask, len(a)+len(b), nil)
	if err != nil {
		t.Fatalf("read log word: %v", err)
	}
	if string(got) != "head-tail" {
		t.Fatalf("log two-part read = %q, want head-tail", got)
	}
}

// TestAkiSpillReusesConcatBufferAcrossRuns stages two two-part runs through the
// shared concat buffer and confirms the first run's staged bytes survive the
// second run reusing the buffer, since Stage copies into the pending batch.
func TestAkiSpillReusesConcatBufferAcrossRuns(t *testing.T) {
	sp := newAkiSpill(newTestAkiVlog(t, 2))

	w0 := sp.stageRun([]byte("aaa"), []byte("111"))
	w1 := sp.stageRun([]byte("bbbb"), []byte("22"))

	got0, err := sp.readProvisional(w0)
	if err != nil {
		t.Fatalf("read provisional 0: %v", err)
	}
	if string(got0) != "aaa111" {
		t.Fatalf("run 0 = %q, want aaa111 (concat buffer reuse corrupted it)", got0)
	}
	got1, err := sp.readProvisional(w1)
	if err != nil {
		t.Fatalf("read provisional 1: %v", err)
	}
	if string(got1) != "bbbb22" {
		t.Fatalf("run 1 = %q, want bbbb22", got1)
	}
}

// TestAkiSpillEmptyResolveIsNil resolves with nothing staged and confirms no
// segment is cut and no words come back.
func TestAkiSpillEmptyResolveIsNil(t *testing.T) {
	sp := newAkiSpill(newTestAkiVlog(t, 0))

	words, err := sp.resolve()
	if err != nil || words != nil {
		t.Fatalf("empty resolve = %v/%v, want nil/nil", words, err)
	}
}
