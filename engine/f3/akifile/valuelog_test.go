package akifile

import (
	"bytes"
	"testing"
)

// TestValuePointerRoundTrip marshals a pointer and parses it back byte for byte.
func TestValuePointerRoundTrip(t *testing.T) {
	p := ValuePointer{ValueOff: 0x1122334455667788, ValueLen: 0x99aabbcc, ValueCRC: 0xddeeff00}
	b := p.Marshal()
	if len(b) != PointerSize {
		t.Fatalf("marshal len = %d, want %d", len(b), PointerSize)
	}
	got, err := ParseValuePointer(b)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got != p {
		t.Fatalf("round trip = %+v, want %+v", got, p)
	}
}

// TestParseValuePointerShort refuses a buffer that cannot hold the 16 bytes.
func TestParseValuePointerShort(t *testing.T) {
	if _, err := ParseValuePointer(make([]byte, PointerSize-1)); err != ErrShort {
		t.Fatalf("short parse err = %v, want ErrShort", err)
	}
}

// TestAppendAndReadValueFrame frames a value, builds the pointer the caller
// would publish (payload base plus the frame's value offset), and reads the
// bytes straight back through the pointer.
func TestAppendAndReadValueFrame(t *testing.T) {
	const base = 4096 // the segment payload's file offset
	val := []byte("the quick brown fox")

	payload, vf := AppendValueFrame(nil, val)
	if vf.FrameOff != 0 {
		t.Fatalf("first frame off = %d, want 0", vf.FrameOff)
	}
	if vf.ValueOff <= vf.FrameOff {
		t.Fatalf("value off %d did not skip the varint at %d", vf.ValueOff, vf.FrameOff)
	}
	if int(vf.ValueLen) != len(val) {
		t.Fatalf("value len = %d, want %d", vf.ValueLen, len(val))
	}

	ptr := ValuePointer{ValueOff: base + vf.ValueOff, ValueLen: vf.ValueLen, ValueCRC: vf.CRC}
	got, err := ReadValue(payload, base, ptr)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("read = %q, want %q", got, val)
	}
}

// TestReadValueCatchesTornBlob is the pointer-CRC guard: a byte flipped under a
// published pointer reads back as a checksum mismatch, never as rot.
func TestReadValueCatchesTornBlob(t *testing.T) {
	val := []byte("durable-or-detected")
	payload, vf := AppendValueFrame(nil, val)
	ptr := ValuePointer{ValueOff: vf.ValueOff, ValueLen: vf.ValueLen, ValueCRC: vf.CRC}

	payload[vf.ValueOff+3] ^= 0xff // media rot inside the value bytes
	if _, err := ReadValue(payload, 0, ptr); err != ErrChecksum {
		t.Fatalf("torn blob err = %v, want ErrChecksum", err)
	}
}

// TestReadValueBounds refuses a pointer whose range runs past the payload or
// sits below the base.
func TestReadValueBounds(t *testing.T) {
	payload, vf := AppendValueFrame(nil, []byte("abc"))
	ptr := ValuePointer{ValueOff: vf.ValueOff, ValueLen: vf.ValueLen + 100, ValueCRC: vf.CRC}
	if _, err := ReadValue(payload, 0, ptr); err != ErrShort {
		t.Fatalf("overrun err = %v, want ErrShort", err)
	}
	below := ValuePointer{ValueOff: 10, ValueLen: 4, ValueCRC: 0}
	if _, err := ReadValue(payload, 4096, below); err != ErrShort {
		t.Fatalf("below-base err = %v, want ErrShort", err)
	}
}

// TestEmptyValueFrame frames and reads a zero-length value, the SET "" case.
func TestEmptyValueFrame(t *testing.T) {
	payload, vf := AppendValueFrame(nil, nil)
	ptr := ValuePointer{ValueOff: vf.ValueOff, ValueLen: vf.ValueLen, ValueCRC: vf.CRC}
	got, err := ReadValue(payload, 0, ptr)
	if err != nil {
		t.Fatalf("read empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty value read %d bytes", len(got))
	}
}

// TestValueLogWalkSelfDelimits packs several values into one payload and walks
// them back frame by frame, the recovery reader's job, ending exactly at the
// tail. Each frame's value offset also serves a pointer read, so the walk and
// the point path agree.
func TestValueLogWalkSelfDelimits(t *testing.T) {
	vals := [][]byte{
		[]byte("alpha"),
		{},
		bytes.Repeat([]byte("x"), 300),
		[]byte("omega"),
	}
	var payload []byte
	frames := make([]ValueFrame, len(vals))
	for i, v := range vals {
		payload, frames[i] = AppendValueFrame(payload, v)
	}

	off := uint64(0)
	for i, want := range vals {
		vf, val, next, err := NextValueFrame(payload, off)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if vf.FrameOff != frames[i].FrameOff || vf.ValueOff != frames[i].ValueOff {
			t.Fatalf("frame %d loc = %+v, want %+v", i, vf, frames[i])
		}
		if !bytes.Equal(val, want) {
			t.Fatalf("frame %d val = %q, want %q", i, val, want)
		}
		ptr := ValuePointer{ValueOff: vf.ValueOff, ValueLen: vf.ValueLen, ValueCRC: vf.CRC}
		pval, err := ReadValue(payload, 0, ptr)
		if err != nil || !bytes.Equal(pval, want) {
			t.Fatalf("frame %d pointer read = %q/%v, want %q", i, pval, err, want)
		}
		off = next
	}
	if off != uint64(len(payload)) {
		t.Fatalf("walk ended at %d, want tail %d", off, len(payload))
	}
}

// TestWalkStopsAtTornTail stops the walk where the last frame was cut short by a
// crash: a value truncated mid-bytes is ErrShort, and a value whose CRC no
// longer matches is ErrChecksum. Either way the walk halts at the last intact
// frame, which is the durable-tail cut.
func TestWalkStopsAtTornTail(t *testing.T) {
	whole, _ := AppendValueFrame(nil, []byte("first"))
	full, second := AppendValueFrame(whole, []byte("second-value"))

	// Truncate inside the second value: the walk reads the first, then runs out.
	truncated := full[:second.ValueOff+3]
	if _, _, _, err := NextValueFrame(truncated, second.FrameOff); err != ErrShort {
		t.Fatalf("truncated tail err = %v, want ErrShort", err)
	}

	// Flip a byte in the second value: framing is intact, the CRC catches it.
	corrupt := append([]byte(nil), full...)
	corrupt[second.ValueOff+1] ^= 0xff
	if _, _, _, err := NextValueFrame(corrupt, second.FrameOff); err != ErrChecksum {
		t.Fatalf("corrupt tail err = %v, want ErrChecksum", err)
	}
}

// TestNextValueFrameAtTail returns ErrShort when the walk has reached the end.
func TestNextValueFrameAtTail(t *testing.T) {
	payload, _ := AppendValueFrame(nil, []byte("only"))
	if _, _, _, err := NextValueFrame(payload, uint64(len(payload))); err != ErrShort {
		t.Fatalf("at-tail err = %v, want ErrShort", err)
	}
}
