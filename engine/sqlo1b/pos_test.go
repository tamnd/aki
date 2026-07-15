package sqlo1b

import "testing"

func TestPosRoundtrip(t *testing.T) {
	cases := []struct {
		extent uint64
		group  uint16
		slot   uint16
	}{
		{0, 0, 0},
		{1, 0, 0},
		{7, 255, 42},
		{maxExtent, maxGroup, BlobSlot - 1},
	}
	for _, c := range cases {
		p, err := NewPos(c.extent, c.group, c.slot)
		if err != nil {
			t.Fatalf("NewPos(%d,%d,%d): %v", c.extent, c.group, c.slot, err)
		}
		if p.Extent() != c.extent || p.Group() != c.group || p.Slot() != c.slot {
			t.Fatalf("roundtrip (%d,%d,%d) -> (%d,%d,%d)", c.extent, c.group, c.slot, p.Extent(), p.Group(), p.Slot())
		}
		if p.IsBlob() {
			t.Fatalf("(%d,%d,%d) reads as blob", c.extent, c.group, c.slot)
		}
	}
}

// TestPosBitLayout pins the doc 03 section 5.1 packing: extent in
// bits 63..24, group in 23..12, slot in 11..0.
func TestPosBitLayout(t *testing.T) {
	p, err := NewPos(1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if want := Pos(1<<24 | 1<<12 | 1); p != want {
		t.Fatalf("packed %#x, want %#x", uint64(p), uint64(want))
	}
	b, err := NewBlobPos(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if want := Pos(2<<24 | 3<<12 | 0xFFF); b != want {
		t.Fatalf("blob packed %#x, want %#x", uint64(b), uint64(want))
	}
}

func TestPosRanges(t *testing.T) {
	if _, err := NewPos(maxExtent+1, 0, 0); err == nil {
		t.Fatal("extent past 40 bits accepted")
	}
	if _, err := NewPos(0, maxGroup+1, 0); err == nil {
		t.Fatal("group past 12 bits accepted")
	}
	if _, err := NewPos(0, 0, BlobSlot); err == nil {
		t.Fatal("slot 4095 accepted outside the blob escape")
	}
	if _, err := NewBlobPos(maxExtent+1, 0); err == nil {
		t.Fatal("blob extent past 40 bits accepted")
	}
	if _, err := NewBlobPos(0, maxGroup+1); err == nil {
		t.Fatal("blob group past 12 bits accepted")
	}
}

func TestBlobPos(t *testing.T) {
	p, err := NewBlobPos(9, 17)
	if err != nil {
		t.Fatal(err)
	}
	if !p.IsBlob() || p.Extent() != 9 || p.Group() != 17 {
		t.Fatalf("blob pos reads ext %d group %d blob %v", p.Extent(), p.Group(), p.IsBlob())
	}
}

func TestFullPtrVerify(t *testing.T) {
	data := []byte("directory chunk bytes")
	pos, err := NewPos(3, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	ptr := MakeFullPtr(pos, data)
	if Pos(ptr.Pos) != pos {
		t.Fatalf("pointer pos %#x, want %#x", ptr.Pos, uint64(pos))
	}
	if err := ptr.Verify(data); err != nil {
		t.Fatalf("clean data fails verify: %v", err)
	}
	tampered := append([]byte(nil), data...)
	tampered[0] ^= 1
	if err := ptr.Verify(tampered); err == nil {
		t.Fatal("tampered data passes verify")
	}
	if err := ptr.Verify(data[:len(data)-1]); err == nil {
		t.Fatal("truncated data passes verify")
	}
}
