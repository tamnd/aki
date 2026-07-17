package sqlo1

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const testDBID = 0x00db1d00feedbeef

// replayAll collects every replayed frame, cloning payloads since they
// alias the scan buffer.
func replayAll(t *testing.T, w *wal) []walFrame {
	t.Helper()
	var got []walFrame
	if err := w.Replay(func(fr walFrame) error {
		fr.Payload = append([]byte(nil), fr.Payload...)
		got = append(got, fr)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestWalPolicyConstants(t *testing.T) {
	if walSegmentSize != 64<<20 {
		t.Fatal("segment size drifted from the doc 03 ring spec")
	}
	if walFsyncWindow != 2*time.Millisecond || walBatchCap != 256 {
		t.Fatal("group commit policy drifted from the doc 03 defaults")
	}
	ops := []uint8{walOpPut, walOpDel, walOpPexpire, walOpGenbump, walOpSeal, walOpCkpt, walOpTrim}
	for i, op := range ops {
		if op != uint8(i+1) {
			t.Fatalf("op %d numbered %d, doc 03 section 12.2 disagrees", i+1, op)
		}
	}
	if walPath("/x/db.aki") != "/x/db.aki.aki-wal" {
		t.Fatal("sidecar naming drifted")
	}
}

func TestWalAppendFlushReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.aki-wal")
	w, err := openWAL(path, testDBID, 1<<12)
	if err != nil {
		t.Fatal(err)
	}

	// One command, three frames, one batch: contiguous seqs, invisible
	// before Flush.
	for i := range 3 {
		seq, err := w.Append(2, walOpPut, uint8(i), []byte("payload"))
		if err != nil {
			t.Fatal(err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("seq %d, want %d", seq, i+1)
		}
	}
	if got := replayAll(t, w); len(got) != 0 {
		t.Fatalf("%d frames visible before Flush", len(got))
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	got := replayAll(t, w)
	if len(got) != 3 {
		t.Fatalf("replayed %d frames, want 3", len(got))
	}
	for i, fr := range got {
		if fr.Seq != uint64(i+1) || fr.Shard != 2 || fr.Op != walOpPut || fr.Oflags != uint8(i) || string(fr.Payload) != "payload" {
			t.Fatalf("frame %d round-tripped wrong: %+v", i, fr)
		}
	}

	// Oversize frames are refused up front.
	if _, err := w.Append(0, walOpPut, 0, make([]byte, 1<<12)); err != errWalTooLarge {
		t.Fatalf("oversize append: %v", err)
	}
	w.Close()

	// Reopen picks up the chain.
	w, err = openWAL(path, testDBID, 1<<12)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if seq, _ := w.Append(0, walOpDel, 0, []byte("k")); seq != 4 {
		t.Fatalf("reopen resumed at seq %d, want 4", seq)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := replayAll(t, w); len(got) != 4 {
		t.Fatalf("replayed %d frames after reopen append, want 4", len(got))
	}
}

// buildWal writes one frame per flush and returns the file bytes plus
// each frame's end offset.
func buildWal(t *testing.T, path string, sizes []int) ([]byte, []int64) {
	t.Helper()
	w, err := openWAL(path, testDBID, 1<<12)
	if err != nil {
		t.Fatal(err)
	}
	offs := []int64{0}
	for i, n := range sizes {
		if _, err := w.Append(0, walOpPut, 0, bytes.Repeat([]byte{byte('a' + i)}, n)); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		offs = append(offs, offs[len(offs)-1]+int64(walHdrSize+n))
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return full, offs
}

func TestWalTornTailMatrix(t *testing.T) {
	dir := t.TempDir()
	sizes := []int{0, 5, 33, 100, 7, 64, 1, 250, 12, 40}
	full, offs := buildWal(t, filepath.Join(dir, "base.aki-wal"), sizes)

	check := func(name string, cut int64, want int) {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		w, err := openWAL(p, testDBID, 1<<12)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got := replayAll(t, w)
		if len(got) != want {
			t.Fatalf("%s: cut at %d replayed %d frames, want %d", name, cut, len(got), want)
		}
		for i, fr := range got {
			if fr.Seq != uint64(i+1) || len(fr.Payload) != sizes[i] {
				t.Fatalf("%s: frame %d wrong after cut", name, i)
			}
		}
		// The tear is also where writing resumes: the next append must
		// extend the surviving chain.
		if seq, _ := w.Append(0, walOpDel, 0, []byte("resume")); seq != uint64(want+1) {
			t.Fatalf("%s: resumed at seq %d, want %d", name, seq, want+1)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if got := replayAll(t, w); len(got) != want+1 {
			t.Fatalf("%s: post-resume replay %d frames, want %d", name, len(got), want+1)
		}
		w.Close()
	}

	for i := 1; i <= len(sizes); i++ {
		start, end := offs[i-1], offs[i]
		check(walFrameName("bound", i), end, i)
		// Mid-frame cuts: inside flen, at fcrc, inside the header, and
		// mid-payload where there is one.
		for j, cut := range []int64{start + 1, start + 4, start + walHdrSize - 1} {
			check(walFrameName("mid", i*10+j), cut, i-1)
		}
		if end-start > walHdrSize {
			check(walFrameName("pay", i), start+walHdrSize+(end-start-walHdrSize)/2, i-1)
		}
	}
}

func walFrameName(kind string, i int) string {
	return kind + "-" + string(rune('A'+i/10)) + string(rune('a'+i%10)) + ".aki-wal"
}

func TestWalMidFileCorruptionEndsReplay(t *testing.T) {
	dir := t.TempDir()
	sizes := []int{10, 10, 10, 10, 10, 10, 10, 10}
	full, offs := buildWal(t, filepath.Join(dir, "base.aki-wal"), sizes)

	// Flip one payload byte in frame 5: frames 6..8 are intact and
	// individually valid, and must still be ignored by design.
	mut := append([]byte(nil), full...)
	mut[offs[4]+walHdrSize+3] ^= 0xFF
	p := filepath.Join(dir, "corrupt.aki-wal")
	if err := os.WriteFile(p, mut, 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := openWAL(p, testDBID, 1<<12)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if got := replayAll(t, w); len(got) != 4 {
		t.Fatalf("replayed %d frames past a mid-file tear, want 4", len(got))
	}
}

func TestWalForeignSidecarRefused(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "w.aki-wal")
	buildWal(t, p, []int{10})
	if _, err := openWAL(p, testDBID+1, 1<<12); err != errWalForeign {
		t.Fatalf("foreign sidecar opened: %v", err)
	}
}

func TestWalRingRecycles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring.aki-wal")
	segSize := int64(1 << 12)
	w, err := openWAL(path, testDBID, segSize)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("r"), 300) // flen 328, 12 per segment

	write := func(n int) {
		t.Helper()
		for range n {
			if _, err := w.Append(0, walOpPut, 0, payload); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(30) // fills segments 0 and 1, lands in 2
	st, _ := os.Stat(path)
	if st.Size() != 3*segSize {
		t.Fatalf("file %d bytes before trim, want 3 segments", st.Size())
	}

	// Trim past segment 0's last seq: the ring must reuse it instead
	// of growing.
	w.SetTrim(12)
	write(12)
	st, _ = os.Stat(path)
	if st.Size() != 3*segSize {
		t.Fatalf("file %d bytes after trimmed write, want 3 segments still", st.Size())
	}
	got := replayAll(t, w)
	if len(got) != 30 || got[0].Seq != 13 || got[len(got)-1].Seq != 42 {
		t.Fatalf("replay after recycle: %d frames, first %d, last %d; want 30 frames 13..42",
			len(got), got[0].Seq, got[len(got)-1].Seq)
	}
	w.Close()

	// A cold open of the recycled ring reads the same story.
	w, err = openWAL(path, testDBID, segSize)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.SetTrim(12)
	got = replayAll(t, w)
	if len(got) != 30 || got[0].Seq != 13 || got[len(got)-1].Seq != 42 {
		t.Fatalf("cold reopen replay: %d frames, first %d, last %d",
			len(got), got[0].Seq, got[len(got)-1].Seq)
	}
	if seq, _ := w.Append(0, walOpPut, 0, payload); seq != 43 {
		t.Fatalf("recycled ring resumed at %d, want 43", seq)
	}
}

// A recycled segment keeps its previous-life chain on disk until it is
// actually overwritten, and that remnant can carry lower seqs than
// every live segment while the frames that once connected it to the
// tail are gone. Reopen must anchor on the chain ending at the highest
// seq, not the lowest firstSeq, or the whole live tail is discarded as
// a previous life. Found by the xcatchup 10M build losing 439K acked
// entries across a clean close.
func TestWalStaleRemnantReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remnant.aki-wal")
	segSize := int64(1 << 12)
	w, err := openWAL(path, testDBID, segSize)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("r"), 300) // flen 328, 12 per segment

	write := func(n int) {
		t.Helper()
		for range n {
			if _, err := w.Append(0, walOpPut, 0, payload); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Four segments, then two recycle rounds: seg0 hosts 49..60, gets
	// trimmed, and is overwritten by 73..84. That destroys the frames
	// connecting the stale seg2+seg3 remnant (25..48) to the live
	// chain (61..84), which is exactly the ring state a long build
	// leaves behind.
	write(48)
	w.SetTrim(24)
	write(12) // recycles seg0: 49..60
	w.SetTrim(60)
	write(12) // recycles seg1: 61..72
	write(12) // recycles seg0 again: 73..84
	w.Close()

	w, err = openWAL(path, testDBID, segSize)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.LastSeq() != 84 {
		t.Fatalf("reopen resumed at %d, want 84: the stale remnant won the chain", w.LastSeq())
	}
	w.SetTrim(60)
	got := replayAll(t, w)
	if len(got) != 24 || got[0].Seq != 61 || got[len(got)-1].Seq != 84 {
		t.Fatalf("replay: %d frames, first %d, last %d; want 24 frames 61..84",
			len(got), got[0].Seq, got[len(got)-1].Seq)
	}

	// The disconnected remnant segments were recycled on open: further
	// writes reuse them instead of growing the file.
	write(12)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 4*segSize {
		t.Fatalf("file %d bytes after post-reopen writes, want 4 segments still", st.Size())
	}
}
