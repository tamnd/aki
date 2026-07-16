package obs1

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestHeaderLayout(t *testing.T) {
	b := AppendHeader(nil, Header{Format: FormatSegment, FVersion: 3, Writer: 0xdeadbeef01})
	if len(b) != HeaderSize {
		t.Fatalf("header is %d bytes, want %d", len(b), HeaderSize)
	}
	// The doc 03 section 2 field table, byte for byte.
	if got := string(b[0:15]); got != "tamndaki fmt001" {
		t.Fatalf("magic %q", got)
	}
	if b[15] != 0 {
		t.Fatal("magic missing trailing NUL")
	}
	if got := binary.LittleEndian.Uint16(b[16:18]); got != 0x6F05 {
		t.Fatalf("format field 0x%04x, want 0x6F05", got)
	}
	if got := binary.LittleEndian.Uint16(b[18:20]); got != 3 {
		t.Fatalf("fversion field %d, want 3", got)
	}
	if got, want := binary.LittleEndian.Uint32(b[20:24]), crc32c(b[0:20]); got != want {
		t.Fatalf("hcrc 0x%08x, computed 0x%08x", got, want)
	}
	if got := binary.LittleEndian.Uint64(b[24:32]); got != 0xdeadbeef01 {
		t.Fatalf("writer field 0x%x", got)
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	for f := FormatRoot; f <= FormatTombstone; f++ {
		h := Header{Format: f, FVersion: 1, Writer: uint64(f) * 7}
		got, err := ParseHeader(AppendHeader(nil, h))
		if err != nil || got != h {
			t.Fatalf("%v: got %+v, %v", f, got, err)
		}
		if _, err := ParseHeaderAs(AppendHeader(nil, h), f); err != nil {
			t.Fatalf("%v as itself: %v", f, err)
		}
	}
}

func TestHeaderRejects(t *testing.T) {
	good := AppendHeader(nil, Header{Format: FormatWAL, FVersion: 1, Writer: 9})

	if _, err := ParseHeader(good[:HeaderSize-1]); err == nil {
		t.Fatal("truncated header parsed")
	}
	flip := func(i int) []byte {
		b := bytes.Clone(good)
		b[i] ^= 0x01
		return b
	}
	if _, err := ParseHeader(flip(0)); err == nil {
		t.Fatal("flipped magic parsed")
	}
	if _, err := ParseHeader(flip(20)); err == nil {
		t.Fatal("flipped hcrc parsed")
	}
	if _, err := ParseHeader(flip(16)); err == nil {
		t.Fatal("flipped format parsed (hcrc must cover it)")
	}
	// Unknown format codes with a valid crc still fail.
	for _, f := range []Format{0x0000, 0x6F00, 0x6F08, 0xFFFF} {
		if _, err := ParseHeader(AppendHeader(nil, Header{Format: f, FVersion: 1})); err == nil {
			t.Fatalf("unknown format 0x%04x parsed", uint16(f))
		}
	}
	// The cross-type check: a WAL header is not a root.
	if _, err := ParseHeaderAs(good, FormatRoot); err == nil {
		t.Fatal("cross-typed object accepted")
	}
	// The writer field sits outside the hcrc window by design (doc 03
	// section 2 covers bytes 0..19): a flipped writer byte still parses,
	// with the flipped value.
	h, err := ParseHeader(flip(24))
	if err != nil || h.Writer != 8 {
		t.Fatalf("flipped writer: %+v, %v", h, err)
	}
}

// FuzzParseHeader holds the parser to the doc 03 contract: clean errors
// on anything malformed, and on success the header re-encodes to the
// exact input bytes, so nothing off-spec is silently accepted.
func FuzzParseHeader(f *testing.F) {
	for fm := FormatRoot; fm <= FormatTombstone; fm++ {
		f.Add(AppendHeader(nil, Header{Format: fm, FVersion: 1, Writer: uint64(fm)}))
	}
	good := AppendHeader(nil, Header{Format: FormatChain, FVersion: 2, Writer: 42})
	f.Add(good[:8])
	f.Add(good[:31])
	f.Add(append(bytes.Clone(good), 0xEE))
	for _, i := range []int{0, 15, 16, 18, 20, 24} {
		b := bytes.Clone(good)
		b[i] ^= 0x80
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := ParseHeader(b)
		if err != nil {
			return
		}
		if !bytes.Equal(AppendHeader(nil, h), b[:HeaderSize]) {
			t.Fatalf("accepted bytes do not re-encode: %+v from %x", h, b[:HeaderSize])
		}
		if _, err := ParseHeaderAs(b, h.Format); err != nil {
			t.Fatalf("ParseHeaderAs disagrees with ParseHeader: %v", err)
		}
	})
}
