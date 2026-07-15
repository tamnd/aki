package sqlo1b

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestGroupBuildParseRoundtrip(t *testing.T) {
	g := NewGroupBuilder(GroupSize)
	recs := [][]byte{
		bytes.Repeat([]byte{0xA1}, 100),
		bytes.Repeat([]byte{0xB2}, 1),
		bytes.Repeat([]byte{0xC3}, 977),
	}
	for i, rec := range recs {
		slot, err := g.Append(rec)
		if err != nil {
			t.Fatal(err)
		}
		if int(slot) != i {
			t.Fatalf("record %d landed in slot %d", i, slot)
		}
	}
	img := g.Close()
	if len(img) != GroupSize {
		t.Fatalf("image is %d bytes, want %d", len(img), GroupSize)
	}

	v, err := ParseGroup(img)
	if err != nil {
		t.Fatal(err)
	}
	if v.Records() != len(recs) {
		t.Fatalf("parsed %d records, want %d", v.Records(), len(recs))
	}
	for i, want := range recs {
		got, err := v.Record(uint16(i))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) < len(want) || !bytes.Equal(got[:len(want)], want) {
			t.Fatalf("record %d does not lead its slice", i)
		}
		if i < len(recs)-1 && len(got) != len(want) {
			t.Fatalf("record %d slice is %d bytes, want exactly %d", i, len(got), len(want))
		}
	}
	if _, err := v.Record(uint16(len(recs))); err == nil {
		t.Fatal("slot past the table read without error")
	}
}

// TestGroupTableLayout pins the on-disk shape: records, pad marker,
// zeros, slot offsets in append order, count in the last two bytes.
func TestGroupTableLayout(t *testing.T) {
	g := NewGroupBuilder(GroupSize)
	if _, err := g.Append(bytes.Repeat([]byte{7}, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Append(bytes.Repeat([]byte{8}, 20)); err != nil {
		t.Fatal(err)
	}
	img := g.Close()
	if got := binary.LittleEndian.Uint16(img[GroupSize-2:]); got != 2 {
		t.Fatalf("count %d in the last two bytes, want 2", got)
	}
	tstart := GroupSize - tableLen(2)
	if off0 := binary.LittleEndian.Uint16(img[tstart:]); off0 != 0 {
		t.Fatalf("slot 0 offset %d, want 0", off0)
	}
	if off1 := binary.LittleEndian.Uint16(img[tstart+2:]); off1 != 10 {
		t.Fatalf("slot 1 offset %d, want 10", off1)
	}
	if got := binary.LittleEndian.Uint32(img[30:]); got != PadMarker {
		t.Fatalf("pad marker %#x after the last record, want %#x", got, PadMarker)
	}
	for i := 34; i < tstart; i++ {
		if img[i] != 0 {
			t.Fatalf("pad byte %d not zero", i)
		}
	}
}

func TestGroupFits(t *testing.T) {
	g := NewGroupBuilder(GroupSize)
	// One record filling everything but its own table entry and count.
	max := GroupSize - tableLen(1)
	if !g.Fits(max) {
		t.Fatalf("exact-fit record of %d bytes rejected", max)
	}
	if g.Fits(max + 1) {
		t.Fatal("oversize record accepted")
	}
	if g.Fits(0) {
		t.Fatal("empty record fits")
	}
	if _, err := g.Append(bytes.Repeat([]byte{1}, max)); err != nil {
		t.Fatal(err)
	}
	if g.Fits(1) {
		t.Fatal("full group still fits a record")
	}
	if _, err := g.Append([]byte{1}); err == nil {
		t.Fatal("append into a full group succeeded")
	}
	if _, err := g.Append(nil); err == nil {
		t.Fatal("empty record appended")
	}

	// Exact fit leaves no pad gap: records touch the slot table.
	img := g.Close()
	v, err := ParseGroup(img)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := v.Record(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec) != max {
		t.Fatalf("exact-fit record reads %d bytes, want %d", len(rec), max)
	}
}

func TestGroupZeroPayload(t *testing.T) {
	// Group 0 sits behind the extent header and is 64 bytes shorter.
	g := NewGroupBuilder(Group0Payload)
	if _, err := g.Append([]byte{1}); err != nil {
		t.Fatal(err)
	}
	img := g.Close()
	if len(img) != Group0Payload {
		t.Fatalf("group 0 image is %d bytes, want %d", len(img), Group0Payload)
	}
	if _, err := ParseGroup(img); err != nil {
		t.Fatal(err)
	}
}

func TestGroupEmpty(t *testing.T) {
	img := NewGroupBuilder(GroupSize).Close()
	if got := binary.LittleEndian.Uint32(img[0:]); got != PadMarker {
		t.Fatalf("empty group leads with %#x, want the pad marker", got)
	}
	v, err := ParseGroup(img)
	if err != nil {
		t.Fatal(err)
	}
	if v.Records() != 0 {
		t.Fatalf("empty group parses %d records", v.Records())
	}
}

func TestParseGroupRejects(t *testing.T) {
	if _, err := ParseGroup([]byte{0}); err == nil {
		t.Fatal("one-byte image parsed")
	}

	// Count larger than the image can hold.
	img := NewGroupBuilder(GroupSize).Close()
	binary.LittleEndian.PutUint16(img[GroupSize-2:], 60000)
	if _, err := ParseGroup(img); err == nil {
		t.Fatal("overflowing record count parsed")
	}

	// Offset past the table start.
	g := NewGroupBuilder(GroupSize)
	if _, err := g.Append([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	img = g.Close()
	binary.LittleEndian.PutUint16(img[GroupSize-tableLen(1):], GroupSize)
	if _, err := ParseGroup(img); err == nil {
		t.Fatal("offset past the table parsed")
	}

	// Out-of-order offsets.
	g = NewGroupBuilder(GroupSize)
	for range 2 {
		if _, err := g.Append([]byte{9, 9}); err != nil {
			t.Fatal(err)
		}
	}
	img = g.Close()
	tstart := GroupSize - tableLen(2)
	binary.LittleEndian.PutUint16(img[tstart:], 2)
	binary.LittleEndian.PutUint16(img[tstart+2:], 0)
	if _, err := ParseGroup(img); err == nil {
		t.Fatal("out-of-order offsets parsed")
	}
}

// TestGroupSlotCap pins that the builder never mints slot 4095, the
// blob escape.
func TestGroupSlotCap(t *testing.T) {
	g := NewGroupBuilder(3 * GroupSize) // roomy: cap by slots, not bytes
	for range BlobSlot {
		if _, err := g.Append([]byte{1}); err != nil {
			t.Fatalf("append %d: %v", g.Records(), err)
		}
	}
	if _, err := g.Append([]byte{1}); err == nil {
		t.Fatal("builder minted slot 4095")
	}
}
