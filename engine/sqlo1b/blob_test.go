package sqlo1b

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// blobRecord builds an encoded string record of exactly rlen bytes.
func blobRecord(t *testing.T, rlen int) []byte {
	t.Helper()
	vlen := rlen - recHdrSize - recTailSize - 1
	if vlen < 0 {
		t.Fatalf("rlen %d below the envelope minimum", rlen)
	}
	val := make([]byte, vlen)
	for i := range val {
		val[i] = byte(i * 7)
	}
	enc, err := (&Record{RType: RecString, Key: []byte("b"), Value: val}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	if len(enc) != rlen {
		t.Fatalf("built %d bytes, want %d", len(enc), rlen)
	}
	return enc
}

// TestBlobThresholdBoundary pins both sides of the 3968-byte line: at
// the threshold the record is slotted work and PlaceBlob refuses it,
// one byte over it is a blob and a group still fits it.
func TestBlobThresholdBoundary(t *testing.T) {
	ext := make([]byte, 16*GroupSize)
	if _, err := PlaceBlob(ext, 1, blobRecord(t, BlobThreshold)); err == nil {
		t.Fatal("record at the threshold placed as a blob")
	}
	at := blobRecord(t, BlobThreshold)
	g := NewGroupBuilder(GroupSize)
	if !g.Fits(len(at)) {
		t.Fatal("record at the threshold does not fit a slotted group")
	}
	next, err := PlaceBlob(ext, 1, blobRecord(t, BlobThreshold+1))
	if err != nil {
		t.Fatal(err)
	}
	if next != 2 {
		t.Fatalf("threshold+1 blob spans to group %d, want 2", next)
	}
}

// TestBlobCeilingBoundary pins the extent-payload ceiling: a blob at
// group 0 may fill the extent to its last byte, one byte more is the
// type layer's problem.
func TestBlobCeilingBoundary(t *testing.T) {
	const extentSize = 16 * GroupSize
	ext := make([]byte, extentSize)
	max := blobRecord(t, extentSize-ExtentHeaderSize)
	next, err := PlaceBlob(ext, 0, max)
	if err != nil {
		t.Fatal(err)
	}
	if int(next) != extentSize/GroupSize {
		t.Fatalf("flush blob ends at group %d, want %d", next, extentSize/GroupSize)
	}
	if _, err := PlaceBlob(make([]byte, extentSize), 0, blobRecord(t, extentSize-ExtentHeaderSize+1)); err == nil {
		t.Fatal("blob past the extent payload placed")
	}
	if _, err := PlaceBlob(make([]byte, extentSize), 15, blobRecord(t, GroupSize+1)); err == nil {
		t.Fatal("blob overrunning from the last group placed")
	}
}

func TestBlobPlaceRejects(t *testing.T) {
	rec := blobRecord(t, BlobThreshold+100)
	if _, err := PlaceBlob(make([]byte, GroupSize+7), 0, rec); err == nil {
		t.Error("ragged extent image accepted")
	}
	if _, err := PlaceBlob(make([]byte, 4*GroupSize), 4, rec); err == nil {
		t.Error("start group past the extent accepted")
	}
	short := append([]byte{}, rec...)
	binary.LittleEndian.PutUint32(short, uint32(len(short)-1))
	if _, err := PlaceBlob(make([]byte, 4*GroupSize), 0, short); err == nil {
		t.Error("rlen disagreeing with the byte count accepted")
	}
}

// TestBlobRunGroups pins the run arithmetic, including group 0's
// header-shortened payload.
func TestBlobRunGroups(t *testing.T) {
	cases := []struct {
		start uint16
		rlen  int
		want  int
	}{
		{1, GroupSize, 1},
		{1, GroupSize + 1, 2},
		{0, Group0Payload, 1},
		{0, Group0Payload + 1, 2},
		{3, 3 * GroupSize, 3},
		{0, 16*GroupSize - ExtentHeaderSize, 16},
	}
	for _, c := range cases {
		if got := BlobRunGroups(c.start, c.rlen); got != c.want {
			t.Errorf("BlobRunGroups(%d, %d) = %d, want %d", c.start, c.rlen, got, c.want)
		}
	}
}

// TestBlobRoundtripFile writes blob runs into extent images, lands
// them in a file at nonzero extent indexes, and reads them back
// through ReadBlob: two blobs sharing an extent, one starting at
// group 0, one filling to the extent's end.
func TestBlobRoundtripFile(t *testing.T) {
	const extentSize = 16 * GroupSize
	f, err := os.Create(filepath.Join(t.TempDir(), "blob.aki"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	recs := []struct {
		ext   uint64
		start uint16
		rlen  int
	}{
		{2, 0, Group0Payload + 900},
		{2, 2, BlobThreshold + 1},
		{3, 5, 10*GroupSize + 5},
	}
	images := map[uint64][]byte{2: make([]byte, extentSize), 3: make([]byte, extentSize)}
	var want [][]byte
	for _, rc := range recs {
		enc := blobRecord(t, rc.rlen)
		want = append(want, enc)
		if _, err := PlaceBlob(images[rc.ext], rc.start, enc); err != nil {
			t.Fatal(err)
		}
	}
	for ext, img := range images {
		if _, err := f.WriteAt(img, int64(ext)*extentSize); err != nil {
			t.Fatal(err)
		}
	}

	for i, rc := range recs {
		pos, err := NewBlobPos(rc.ext, rc.start)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ReadBlob(f, extentSize, pos)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		reenc, err := got.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(reenc, want[i]) {
			t.Fatalf("record %d diverged after the file roundtrip", i)
		}
	}
}

func TestBlobReadRejects(t *testing.T) {
	const extentSize = 16 * GroupSize
	f, err := os.Create(filepath.Join(t.TempDir(), "blob.aki"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	img := make([]byte, extentSize)
	enc := blobRecord(t, 2*GroupSize)
	if _, err := PlaceBlob(img, 1, enc); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(img, 0); err != nil {
		t.Fatal(err)
	}
	goodPos, err := NewBlobPos(0, 1)
	if err != nil {
		t.Fatal(err)
	}

	slotted, err := NewPos(0, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBlob(f, extentSize, slotted); err == nil {
		t.Error("slotted position read as a blob")
	}
	past, err := NewBlobPos(0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBlob(f, extentSize, past); err == nil {
		t.Error("start group past the extent read")
	}

	// A tiny rlen at the escape position is not a legal blob even if
	// the record itself is intact.
	tiny := make([]byte, extentSize)
	small, err := (&Record{RType: RecString, Key: []byte("s"), Value: []byte("v")}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	copy(tiny[3*GroupSize:], small)
	if _, err := f.WriteAt(tiny, extentSize); err != nil {
		t.Fatal(err)
	}
	tinyPos, err := NewBlobPos(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBlob(f, extentSize, tinyPos); err == nil {
		t.Error("under-threshold rlen read as a blob")
	}

	// An rlen running past the extent must fail on bounds, before any
	// giant allocation or far read.
	huge := make([]byte, 8)
	binary.LittleEndian.PutUint32(huge, uint32(extentSize))
	if _, err := f.WriteAt(huge, 2*extentSize+5*GroupSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(3 * extentSize); err != nil {
		t.Fatal(err)
	}
	hugePos, err := NewBlobPos(2, 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBlob(f, extentSize, hugePos); err == nil {
		t.Error("rlen past the extent read")
	}

	// Corruption anywhere in the run fails the envelope: flip one byte
	// of the tail group on disk.
	mut := append([]byte{}, img...)
	mut[2*GroupSize+100] ^= 1
	if _, err := f.WriteAt(mut, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBlob(f, extentSize, goodPos); err == nil {
		t.Error("corrupted blob run decoded")
	}
}
