package store

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// newAkiSpillStore opens a store over a fresh .aki so akispill and the spill
// ledger are live, the isolation harness for the deferred-publish patch before
// writeRun routes onto it.
func newAkiSpillStore(t *testing.T) *Store {
	t.Helper()
	f, err := akifile.Create(filepath.Join(t.TempDir(), "spill.aki"), akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	s, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 0})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestResolveSpillPatchesRecords stages a batch of runs, writes each provisional
// word into a value-area pointer and records it, then resolves the batch: every
// pointer flips from its provisional word to the absolute log word the cut
// assigned, the length and capacity beside it survive, the bytes read back, and
// the ledger empties. The store-side end of the deferred-publish contract.
func TestResolveSpillPatchesRecords(t *testing.T) {
	s := newAkiSpillStore(t)

	vals := [][]byte{[]byte("alpha"), bytes.Repeat([]byte("b"), 3000), []byte("gamma")}
	vsList := make([]uint64, len(vals))
	for i, v := range vals {
		word := s.akispill.stageRun(v, nil)
		if !isProvisional(word) {
			t.Fatalf("stageRun %d returned a non-provisional word %#x", i, word)
		}
		vs, ok := s.arenaAlloc(ptrSize)
		if !ok {
			t.Fatalf("arenaAlloc for pointer %d", i)
		}
		s.writePtr(vs, word, uint32(len(v)), uint32(len(v)))
		s.recordSpill(vs, word)
		vsList[i] = vs

		// The staged bytes read back through the provisional word before the cut.
		got, err := s.akispill.readProvisional(word)
		if err != nil || !bytes.Equal(got, v) {
			t.Fatalf("provisional read %d = %q/%v, want %q", i, got, err, v)
		}
	}

	// Every pointer still holds a provisional word before the boundary.
	for i, vs := range vsList {
		if w, _, _ := s.readPtr(vs); !isProvisional(w) {
			t.Fatalf("pointer %d not provisional before resolve, word=%#x", i, w)
		}
	}

	if err := s.resolveSpill(); err != nil {
		t.Fatalf("resolveSpill: %v", err)
	}

	for i, vs := range vsList {
		word, vlen, vcap := s.readPtr(vs)
		if isProvisional(word) {
			t.Fatalf("pointer %d still provisional after resolve, word=%#x", i, word)
		}
		if word&inLogBit == 0 {
			t.Fatalf("pointer %d did not resolve to a log word, word=%#x", i, word)
		}
		if int(vlen) != len(vals[i]) || int(vcap) != len(vals[i]) {
			t.Fatalf("pointer %d len/cap = %d/%d, want %d", i, vlen, vcap, len(vals[i]))
		}
		got, err := s.akivlog.readAt(word&runAddrMask, int(vlen), nil)
		if err != nil || !bytes.Equal(got, vals[i]) {
			t.Fatalf("resolved read %d = %q/%v, want %q", i, got, err, vals[i])
		}
	}
	if len(s.spillLedger) != 0 {
		t.Fatalf("ledger not cleared, len=%d", len(s.spillLedger))
	}
}

// TestResolveSpillTwoPartRun stages an old+add run as one frame and confirms the
// resolved pointer reads the two halves back contiguous, the APPEND shape
// writeRun hands as a two-part run.
func TestResolveSpillTwoPartRun(t *testing.T) {
	s := newAkiSpillStore(t)

	a := bytes.Repeat([]byte("A"), 1500)
	b := bytes.Repeat([]byte("B"), 1500)
	word := s.akispill.stageRun(a, b)
	vs, ok := s.arenaAlloc(ptrSize)
	if !ok {
		t.Fatal("arenaAlloc")
	}
	n := uint32(len(a) + len(b))
	s.writePtr(vs, word, n, n)
	s.recordSpill(vs, word)

	if err := s.resolveSpill(); err != nil {
		t.Fatalf("resolveSpill: %v", err)
	}
	rw, vlen, _ := s.readPtr(vs)
	got, err := s.akivlog.readAt(rw&runAddrMask, int(vlen), nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if want := append(append([]byte{}, a...), b...); !bytes.Equal(got, want) {
		t.Fatalf("two-part run read back %d bytes, want %d contiguous", len(got), len(want))
	}
}

// TestRecordSpillIgnoresNonProvisional confirms an arena or already-published
// word never enters the ledger, so resolveSpill only ever patches real spills.
func TestRecordSpillIgnoresNonProvisional(t *testing.T) {
	s := newAkiSpillStore(t)

	s.recordSpill(0, 4096)              // a plain arena offset
	s.recordSpill(0, inLogBit|uint64(8)) // an already-published log word
	if len(s.spillLedger) != 0 {
		t.Fatalf("non-provisional words entered the ledger, len=%d", len(s.spillLedger))
	}
	if err := s.resolveSpill(); err != nil {
		t.Fatalf("empty resolveSpill: %v", err)
	}
}
