package sqlo1b

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

const recSegSize = 1 << 16

type sinkFrame struct {
	seq     uint64
	op      uint8
	payload string
}

type fakeSink struct {
	frames []sinkFrame
	failAt uint64
}

func (s *fakeSink) ApplyData(fr sqlo1.WALFrame) error {
	if s.failAt != 0 && fr.Seq == s.failAt {
		return errBoom
	}
	s.frames = append(s.frames, sinkFrame{fr.Seq, fr.Op, string(fr.Payload)})
	return nil
}

type staticSrc struct{ roots Roots }

func (s *staticSrc) Drain(uint64) error             { return nil }
func (s *staticSrc) FlushIndex(uint64) error        { return nil }
func (s *staticSrc) Snapshot(uint64) (Roots, error) { return s.roots, nil }

type recRig struct {
	f       *os.File
	walPath string
	sb      *Superblock // as committed by the checkpoint, seq 2
}

// newRecRig builds a crashed shard with real history: three old PUTs,
// a real checkpoint (seq 2, trim 3, CKPT frame at seq 4), then a
// post-checkpoint tail of PUT, SEAL, PEXPIRE that only replay can
// recover. The WAL is closed as a crash leaves it.
func newRecRig(t *testing.T) *recRig {
	t.Helper()
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "b.aki"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	sb, err := NewSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	if err := InitSuperblocks(f, sb); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(dir, "b.aki-wal")
	w, err := sqlo1.OpenWAL(walPath, sb.WALDBID(), recSegSize)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if _, err := w.Append(0, sqlo1.WALOpPut, 0, fmt.Appendf(nil, "old %d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	c := &Checkpointer{WAL: w, File: f}
	next, err := c.Run(sb, &staticSrc{roots: Roots{Dir: FullPtr{Pos: 11, Sum: 12}}})
	if err != nil {
		t.Fatal(err)
	}

	emit := func(op uint8, payload []byte) {
		t.Helper()
		if _, err := w.Append(0, op, 0, payload); err != nil {
			t.Fatal(err)
		}
	}
	emit(sqlo1.WALOpPut, []byte("young put"))
	emit(sqlo1.WALOpSeal, SealOp{Extent: 7, Sum: 70, Kind: KindVlog}.Encode())
	emit(sqlo1.WALOpPexpire, []byte("young pexpire"))
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return &recRig{f: f, walPath: walPath, sb: next}
}

func TestRecover(t *testing.T) {
	r := newRecRig(t)
	sink := &fakeSink{}
	rec, err := Recover(r.f, r.walPath, recSegSize, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.WAL.Close()

	if rec.Super.Seq != 2 || rec.Super.WALTrimSeq != 3 {
		t.Fatalf("picked seq %d trim %d", rec.Super.Seq, rec.Super.WALTrimSeq)
	}
	if rec.Super.DirRoot != (FullPtr{Pos: 11, Sum: 12}) {
		t.Fatal("picked superblock lost the checkpointed roots")
	}
	// The tail is CKPT(4), PUT(5), SEAL(6), PEXPIRE(7); the sink gets
	// the data ops only, and none of the three checkpointed old PUTs.
	want := []sinkFrame{
		{5, sqlo1.WALOpPut, "young put"},
		{7, sqlo1.WALOpPexpire, "young pexpire"},
	}
	if len(sink.frames) != len(want) {
		t.Fatalf("sink saw %+v", sink.frames)
	}
	for i, fr := range want {
		if sink.frames[i] != fr {
			t.Fatalf("sink frame %d is %+v, want %+v", i, sink.frames[i], fr)
		}
	}
	if rec.Tip != 7 {
		t.Fatalf("tip %d, want 7", rec.Tip)
	}
	if rec.Format.CkptSuper != 2 {
		t.Fatalf("fold ckpt %d", rec.Format.CkptSuper)
	}
	q := rec.Quarantine()
	if len(q) != 1 || q[0].Extent != 7 || q[0].Sum != 70 {
		t.Fatalf("quarantine set %+v", q)
	}

	// The recovered WAL keeps appending where the crash left off.
	seq, err := rec.WAL.Append(0, sqlo1.WALOpPut, 0, []byte("after recovery"))
	if err != nil {
		t.Fatal(err)
	}
	if seq != 8 {
		t.Fatalf("first post-recovery seq %d, want 8", seq)
	}
}

func TestRecoverEmptyTail(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "b.aki"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sb, err := NewSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	if err := InitSuperblocks(f, sb); err != nil {
		t.Fatal(err)
	}
	sink := &fakeSink{}
	rec, err := Recover(f, filepath.Join(dir, "b.aki-wal"), recSegSize, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.WAL.Close()
	if rec.Tip != 0 || len(sink.frames) != 0 || len(rec.Quarantine()) != 0 {
		t.Fatalf("fresh database recovered tip %d, %d frames", rec.Tip, len(sink.frames))
	}
}

// TestRecoverTornTail flips one payload byte in the last frame: replay
// must deliver everything before the tear and stop without error,
// because nothing after the first tear was ever acknowledged.
func TestRecoverTornTail(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "b.aki"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sb, err := NewSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	if err := InitSuperblocks(f, sb); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(dir, "b.aki-wal")
	w, err := sqlo1.OpenWAL(walPath, sb.WALDBID(), recSegSize)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"first frame", "second frame"} {
		if _, err := w.Append(0, sqlo1.WALOpPut, 0, []byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	tear := bytes.Index(raw, []byte("second frame"))
	if tear < 0 {
		t.Fatal("second frame not found in the sidecar")
	}
	raw[tear] ^= 0xFF
	if err := os.WriteFile(walPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	sink := &fakeSink{}
	rec, err := Recover(f, walPath, recSegSize, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.WAL.Close()
	if len(sink.frames) != 1 || sink.frames[0].payload != "first frame" {
		t.Fatalf("sink saw %+v, want the first frame only", sink.frames)
	}
	if rec.Tip != 1 {
		t.Fatalf("tip %d, want 1", rec.Tip)
	}
}

func TestRecoverRefusals(t *testing.T) {
	t.Run("no superblock", func(t *testing.T) {
		dir := t.TempDir()
		f, err := os.Create(filepath.Join(dir, "b.aki"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := f.Truncate(2 * SuperblockSize); err != nil {
			t.Fatal(err)
		}
		if _, err := Recover(f, filepath.Join(dir, "b.aki-wal"), recSegSize, nil); !errors.Is(err, ErrNoSuperblock) {
			t.Fatalf("zeroed roots recovered: %v", err)
		}
	})

	t.Run("foreign wal", func(t *testing.T) {
		// Replace the rig's sidecar with one another database wrote:
		// the mixup must refuse loudly, never read as a torn tail.
		r := newRecRig(t)
		if err := os.Remove(r.walPath); err != nil {
			t.Fatal(err)
		}
		w, err := sqlo1.OpenWAL(r.walPath, r.sb.WALDBID()+1, recSegSize)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Append(0, sqlo1.WALOpPut, 0, []byte("foreign")); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := Recover(r.f, r.walPath, recSegSize, nil); err == nil {
			t.Fatal("another database's sidecar recovered")
		}
	})

	t.Run("ckpt past the survivor", func(t *testing.T) {
		dir := t.TempDir()
		f, err := os.Create(filepath.Join(dir, "b.aki"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		sb, err := NewSuperblock()
		if err != nil {
			t.Fatal(err)
		}
		if err := InitSuperblocks(f, sb); err != nil {
			t.Fatal(err)
		}
		walPath := filepath.Join(dir, "b.aki-wal")
		w, err := sqlo1.OpenWAL(walPath, sb.WALDBID(), recSegSize)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Append(0, sqlo1.WALOpCkpt, 0, CkptOp{SuperSeq: 9}.Encode()); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := Recover(f, walPath, recSegSize, nil); err == nil {
			t.Fatal("CKPT past the surviving superblock recovered")
		}
	})

	t.Run("sink error aborts", func(t *testing.T) {
		r := newRecRig(t)
		if _, err := Recover(r.f, r.walPath, recSegSize, &fakeSink{failAt: 5}); !errors.Is(err, errBoom) {
			t.Fatalf("sink failure: %v", err)
		}
	})
}

// countReader counts data-file bytes read and the highest offset
// touched, to prove F5 by construction rather than by stopwatch.
type countReader struct {
	r      io.ReaderAt
	bytes  atomic.Int64
	maxOff atomic.Int64
}

func (c *countReader) ReadAt(p []byte, off int64) (int, error) {
	c.bytes.Add(int64(len(p)))
	if end := off + int64(len(p)); end > c.maxOff.Load() {
		c.maxOff.Store(end)
	}
	return c.r.ReadAt(p, off)
}

// TestRecoverReadsAreO1 varies the data-file size three orders of
// magnitude with an identical WAL tail and asserts recovery reads the
// same two superblock slots and nothing else: recovery time is flat
// in data size by construction, O(WAL tail) only.
func TestRecoverReadsAreO1(t *testing.T) {
	read := func(t *testing.T, extents uint64) (int64, int64, int) {
		t.Helper()
		dir := t.TempDir()
		f, err := os.Create(filepath.Join(dir, "b.aki"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		sb, err := NewSuperblock()
		if err != nil {
			t.Fatal(err)
		}
		sb.ExtentCount = extents
		if err := InitSuperblocks(f, sb); err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(int64(extents) * DefaultExtentSize); err != nil {
			t.Fatal(err)
		}
		walPath := filepath.Join(dir, "b.aki-wal")
		w, err := sqlo1.OpenWAL(walPath, sb.WALDBID(), recSegSize)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 10 {
			if _, err := w.Append(0, sqlo1.WALOpPut, 0, fmt.Appendf(nil, "tail %d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		cr := &countReader{r: f}
		sink := &fakeSink{}
		rec, err := Recover(cr, walPath, recSegSize, sink)
		if err != nil {
			t.Fatal(err)
		}
		rec.WAL.Close()
		return cr.bytes.Load(), cr.maxOff.Load(), len(sink.frames)
	}

	smallBytes, smallOff, smallFrames := read(t, 4)
	bigBytes, bigOff, bigFrames := read(t, 4096) // a 4 GiB file, sparse
	if smallFrames != 10 || bigFrames != 10 {
		t.Fatalf("tail deliveries %d and %d, want 10", smallFrames, bigFrames)
	}
	if smallBytes != bigBytes || smallBytes != 2*SuperblockSize {
		t.Fatalf("data-file reads %d then %d bytes, want both %d", smallBytes, bigBytes, 2*SuperblockSize)
	}
	if smallOff != bigOff || bigOff > 2*SuperblockSize {
		t.Fatalf("recovery touched offset %d of a big file, roots end at %d", bigOff, 2*SuperblockSize)
	}
}

func TestRestoreGrid(t *testing.T) {
	// A snapshot grid of 8 extents with extent 2 in use, then a tail
	// that sealed extent 5 (inside the snapshot) and extent 12 (the
	// file grew past the snapshot).
	g := NewGrid(8)
	if _, err := g.Allocate(KindVlog, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Seal(KindVlog, 0); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadGrid(g.Allocmap(), g.ExtentCount())
	if err != nil {
		t.Fatal(err)
	}

	rec := &Recovery{Super: &Superblock{WALTrimSeq: 3}}
	for i, ext := range []uint64{5, 12} {
		payload := SealOp{Extent: ext, Sum: ext * 10, Kind: KindVlog}.Encode()
		if err := rec.Format.Apply(4+uint64(i), FrameSeal, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := rec.RestoreGrid(loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.ExtentCount() != 13 {
		t.Fatalf("grid grew to %d extents, want 13", loaded.ExtentCount())
	}
	for _, ext := range []uint64{5, 12} {
		if loaded.State(ext) != StateSealed {
			t.Fatalf("extent %d is %s after restore", ext, loaded.State(ext))
		}
	}
	// The snapshot's own not-free extent is untouched.
	if loaded.State(1) != StateSealed {
		t.Fatalf("snapshot extent 1 is %s", loaded.State(1))
	}

	// A replayed seal on an extent some stream holds active is an
	// inconsistency, not something to paper over.
	live := NewGrid(8)
	if _, err := live.Allocate(KindVlog, 0); err != nil {
		t.Fatal(err)
	}
	bad := &Recovery{Super: &Superblock{}}
	if err := bad.Format.Apply(1, FrameSeal, SealOp{Extent: 1, Sum: 1, Kind: KindVlog}.Encode()); err != nil {
		t.Fatal(err)
	}
	if err := bad.RestoreGrid(live); err == nil {
		t.Fatal("seal over an active extent restored")
	}
}
