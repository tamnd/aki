package wal

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/format"
	"github.com/tamnd/aki/vfs"
)

const testPageSize = 4096

func page(b byte) []byte {
	p := make([]byte, testPageSize)
	for i := range p {
		p[i] = b
	}
	return p
}

func newTestWAL(t *testing.T) (*WAL, vfs.VFS) {
	t.Helper()
	fsys := vfs.NewMem()
	w, err := Create(fsys, "db.aki-wal", testPageSize, Options{Salt1: 0x11111111, Salt2: 0x22222222})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return w, fsys
}

func TestHeaderRoundTrip(t *testing.T) {
	w, fsys := newTestWAL(t)
	w.Close()
	f, _ := fsys.Open("db.aki-wal", false)
	defer f.Close()
	buf := make([]byte, HeaderSize)
	f.ReadAt(buf, 0)
	h, err := parseHeader(buf)
	if err != nil {
		t.Fatal(err)
	}
	if h.pageSize != testPageSize || h.salt1 != 0x11111111 || h.salt2 != 0x22222222 {
		t.Errorf("header parsed wrong: %+v", h)
	}
}

func TestCommitAndRead(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()
	if err := w.CommitTxn([]Frame{
		{PageNo: 5, Data: page(0xAA)},
		{PageNo: 6, Data: page(0xBB)},
	}, 7); err != nil {
		t.Fatal(err)
	}
	if w.FrameCount() != 2 {
		t.Errorf("frame count %d want 2", w.FrameCount())
	}
	got, ok, err := w.Read(5)
	if err != nil || !ok {
		t.Fatalf("Read(5): ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, page(0xAA)) {
		t.Error("page 5 content wrong")
	}
	if _, ok, _ := w.Read(99); ok {
		t.Error("Read(99) should miss")
	}
	if w.DBSizeAfter() != 7 {
		t.Errorf("db size %d want 7", w.DBSizeAfter())
	}
}

func TestLatestVersionWins(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()
	w.CommitTxn([]Frame{{PageNo: 3, Data: page(0x01)}}, 4)
	w.CommitTxn([]Frame{{PageNo: 3, Data: page(0x02)}}, 4)
	got, _, _ := w.Read(3)
	if got[0] != 0x02 {
		t.Errorf("latest version not returned: %#x", got[0])
	}
}

func TestRecoverAfterReopen(t *testing.T) {
	fsys := vfs.NewMem()
	w, _ := Create(fsys, "r.aki-wal", testPageSize, Options{Salt1: 1, Salt2: 2})
	w.CommitTxn([]Frame{{PageNo: 1, Data: page(0x10)}}, 2)
	w.CommitTxn([]Frame{{PageNo: 2, Data: page(0x20)}, {PageNo: 1, Data: page(0x11)}}, 3)
	w.Close()

	w2, err := Open(fsys, "r.aki-wal", testPageSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w2.Close()
	if w2.FrameCount() != 3 {
		t.Errorf("recovered frames %d want 3", w2.FrameCount())
	}
	got, ok, _ := w2.Read(1)
	if !ok || got[0] != 0x11 {
		t.Errorf("recovered page 1 = %#x ok=%v", got[0], ok)
	}
	if w2.DBSizeAfter() != 3 {
		t.Errorf("recovered db size %d want 3", w2.DBSizeAfter())
	}
	// A further commit chains correctly onto the recovered checksum.
	if err := w2.CommitTxn([]Frame{{PageNo: 4, Data: page(0x40)}}, 5); err != nil {
		t.Fatalf("append after recover: %v", err)
	}
}

func TestRecoverDiscardsUncommittedTail(t *testing.T) {
	fsys := vfs.NewMem()
	w, _ := Create(fsys, "t.aki-wal", testPageSize, Options{Salt1: 3, Salt2: 4})
	w.CommitTxn([]Frame{{PageNo: 1, Data: page(0xAA)}}, 2)
	w.Close()

	// Manually append a non-commit frame (db_size_after=0) with a valid
	// checksum chain but no commit; recovery must discard it.
	f, _ := fsys.Open("t.aki-wal", false)
	// Reopen WAL to learn the running checksum, then hand-write a dangling frame.
	w2, _ := Open(fsys, "t.aki-wal", testPageSize)
	s1, s2 := w2.cksum[0], w2.cksum[1]
	w2.Close()
	fhdr := make([]byte, FrameHeaderSize)
	binEnc(fhdr[0:], 9)  // page 9
	binEnc(fhdr[4:], 0)  // not a commit
	binEnc(fhdr[8:], 3)  // salt1
	binEnc(fhdr[12:], 4) // salt2
	pg := page(0x99)
	c1, c2 := walChecksum(s1, s2, fhdr[0:16])
	c1, c2 = walChecksum(c1, c2, pg)
	binEnc(fhdr[16:], c1)
	binEnc(fhdr[20:], c2)
	off := frameOffset(1, testPageSize)
	f.WriteAt(fhdr, off)
	f.WriteAt(pg, off+FrameHeaderSize)
	f.Close()

	w3, err := Open(fsys, "t.aki-wal", testPageSize)
	if err != nil {
		t.Fatal(err)
	}
	defer w3.Close()
	if w3.FrameCount() != 1 {
		t.Errorf("frames after recover %d want 1 (dangling discarded)", w3.FrameCount())
	}
	if _, ok, _ := w3.Read(9); ok {
		t.Error("uncommitted page 9 should not be visible")
	}
}

func TestRecoverStopsAtTornFrame(t *testing.T) {
	fsys := vfs.NewMem()
	w, _ := Create(fsys, "tear.aki-wal", testPageSize, Options{Salt1: 5, Salt2: 6})
	w.CommitTxn([]Frame{{PageNo: 1, Data: page(0x01)}}, 2)
	w.CommitTxn([]Frame{{PageNo: 2, Data: page(0x02)}}, 3)
	w.Close()

	// Corrupt the second frame's payload to break its checksum.
	f, _ := fsys.Open("tear.aki-wal", false)
	off := frameOffset(1, testPageSize) + FrameHeaderSize
	f.WriteAt([]byte{0xFF, 0xFF}, off)
	f.Close()

	w2, err := Open(fsys, "tear.aki-wal", testPageSize)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if w2.FrameCount() != 1 {
		t.Errorf("frames %d want 1 (stop before torn frame)", w2.FrameCount())
	}
	if w2.DBSizeAfter() != 2 {
		t.Errorf("db size %d want 2", w2.DBSizeAfter())
	}
}

func TestCheckpointAppliesToMain(t *testing.T) {
	fsys := vfs.NewMem()
	// Build a main file with 4 pages of zeros.
	main, _ := fsys.Open("db.aki", true)
	main.Truncate(4 * testPageSize)
	w, _ := Create(fsys, "db.aki-wal", testPageSize, Options{Salt1: 7, Salt2: 8})
	w.CommitTxn([]Frame{{PageNo: 1, Data: page(0xAB)}, {PageNo: 3, Data: page(0xCD)}}, 4)
	seqBefore := w.CheckpointSeq()
	if err := w.Checkpoint(main); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if w.CheckpointSeq() != seqBefore+1 {
		t.Errorf("checkpoint seq not bumped: %d", w.CheckpointSeq())
	}
	if w.FrameCount() != 0 {
		t.Errorf("WAL not reset: %d frames", w.FrameCount())
	}
	// Verify the main file got the page images.
	buf := make([]byte, testPageSize)
	main.ReadAt(buf, 1*testPageSize)
	if buf[0] != 0xAB {
		t.Errorf("main page 1 = %#x want 0xAB", buf[0])
	}
	main.ReadAt(buf, 3*testPageSize)
	if buf[0] != 0xCD {
		t.Errorf("main page 3 = %#x want 0xCD", buf[0])
	}
	main.Close()
}

func TestPageSizeMismatch(t *testing.T) {
	fsys := vfs.NewMem()
	w, _ := Create(fsys, "m.aki-wal", testPageSize, Options{Salt1: 1, Salt2: 1})
	w.Close()
	if _, err := Open(fsys, "m.aki-wal", 8192); err != ErrPageSize {
		t.Errorf("got %v want ErrPageSize", err)
	}
}

func TestValidPageSizeGuard(t *testing.T) {
	// Sanity: testPageSize is a legal page size.
	if !format.ValidPageSize(testPageSize) {
		t.Fatal("test page size is not valid")
	}
}

// binEnc is a little-endian uint32 writer for the hand-crafted frame tests.
func binEnc(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}
