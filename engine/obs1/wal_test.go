package obs1

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func sampleWAL() []WALSection {
	return []WALSection{
		{Group: 3, Epoch: 9, Frames: []WALFrame{
			{Kind: 1, Flags: 0, Slot: 100, Seq: 10, Key: []byte("user:1"), Payload: []byte("v1")},
			{Kind: 1, Flags: 2, Slot: 101, Seq: 11, Key: []byte("user:2"), Payload: bytes.Repeat([]byte("x"), 300)},
			{Kind: 2, Flags: 0, Slot: 100, Seq: 15, Key: []byte("user:1")},
		}},
		{Group: 7, Epoch: 2, Frames: []WALFrame{
			{Kind: 5, Slot: 900, Seq: 1, Key: []byte("q"), Payload: []byte("payload")},
		}},
	}
}

func mustWAL(t *testing.T, writer uint64, sections []WALSection) []byte {
	t.Helper()
	b, err := AppendWAL(nil, writer, sections)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestWALRoundTrip(t *testing.T) {
	want := sampleWAL()
	b := mustWAL(t, 42, want)
	got, h, err := ParseWAL(b)
	if err != nil {
		t.Fatal(err)
	}
	if h.Format != FormatWAL || h.FVersion != 1 || h.Writer != 42 {
		t.Fatalf("header %+v", h)
	}
	if len(got) != len(want) {
		t.Fatalf("%d sections, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Group != want[i].Group || got[i].Epoch != want[i].Epoch || len(got[i].Frames) != len(want[i].Frames) {
			t.Fatalf("section %d: %+v", i, got[i])
		}
		for j, f := range want[i].Frames {
			g := got[i].Frames[j]
			if g.Kind != f.Kind || g.Flags != f.Flags || g.Slot != f.Slot || g.Seq != f.Seq ||
				!bytes.Equal(g.Key, f.Key) || !bytes.Equal(g.Payload, f.Payload) {
				t.Fatalf("section %d frame %d: got %+v want %+v", i, j, g, f)
			}
		}
	}
	if again, err := AppendWAL(nil, h.Writer, got); err != nil || !bytes.Equal(again, b) {
		t.Fatalf("re-encode differs (err %v)", err)
	}
}

// TestWALRangedPath walks the recovery route a real reader takes: tail
// from the last 16 bytes, footer from the tail's coordinates, one section
// from its index entry's span, no whole-object parse involved.
func TestWALRangedPath(t *testing.T) {
	b := mustWAL(t, 7, sampleWAL())
	footerOff, footerLen, err := ParseWALTail(b[len(b)-16:])
	if err != nil {
		t.Fatal(err)
	}
	index, err := ParseWALFooter(b[footerOff : footerOff+uint64(footerLen)])
	if err != nil {
		t.Fatal(err)
	}
	if len(index) != 2 || index[0].Group != 3 || index[1].Group != 7 {
		t.Fatalf("index %+v", index)
	}
	if index[1].NFrames != 1 || index[1].FirstSeq != 1 || index[1].LastSeq != 1 {
		t.Fatalf("index[1] %+v", index[1])
	}
	off, n := index[1].SectionSpan()
	s, err := ParseWALSection(b[off:off+n], index[1])
	if err != nil {
		t.Fatal(err)
	}
	if s.Group != 7 || string(s.Frames[0].Payload) != "payload" {
		t.Fatalf("section %+v", s)
	}
}

func TestWALWriterRejects(t *testing.T) {
	if _, err := AppendWAL(nil, 1, nil); err == nil {
		t.Error("zero sections accepted")
	}
	if _, err := AppendWAL(nil, 1, []WALSection{{Group: 1}}); err == nil {
		t.Error("empty section accepted")
	}
	if _, err := AppendWAL(nil, 1, []WALSection{{Frames: []WALFrame{{Seq: 5}, {Seq: 5}}}}); err == nil {
		t.Error("non-increasing seq accepted")
	}
	if _, err := AppendWAL(nil, 1, []WALSection{{Frames: []WALFrame{{Seq: 1, Key: make([]byte, 70000)}}}}); err == nil {
		t.Error("oversized key accepted")
	}
}

func TestWALParseRejects(t *testing.T) {
	good := mustWAL(t, 1, sampleWAL())

	flip := func(name string, i int) {
		t.Helper()
		b := append([]byte(nil), good...)
		b[i] ^= 0x01
		if _, _, err := ParseWAL(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	flip("section group byte vs index", HeaderSize) // caught by the index cross-check
	flip("comp byte", HeaderSize+6)                 // codec not landed
	flip("reserved byte", HeaderSize+7)             // reserved must be zero
	flip("frame byte", HeaderSize+walSectionHdr+4)  // section crc
	flip("tail crc", len(good)-1)                   // tail crc
	flip("footer offset in tail", len(good)-16)     // tail crc covers it
	footerOff, _, err := ParseWALTail(good[len(good)-16:])
	if err != nil {
		t.Fatal(err)
	}
	flip("footer byte", int(footerOff))

	if _, _, err := ParseWAL(good[:len(good)-1]); err == nil {
		t.Error("truncated object accepted")
	}
	if _, _, err := ParseWAL(good[:HeaderSize+8]); err == nil {
		t.Error("headerless-tail object accepted")
	}
	badVersion := AppendHeader(nil, Header{Format: FormatWAL, FVersion: 2, Writer: 1})
	badVersion = append(badVersion, good[HeaderSize:]...)
	if _, _, err := ParseWAL(badVersion); err == nil {
		t.Error("fversion 2 accepted")
	}
	crossType := AppendHeader(nil, Header{Format: FormatSegment, FVersion: 1, Writer: 1})
	crossType = append(crossType, good[HeaderSize:]...)
	if _, _, err := ParseWAL(crossType); err == nil {
		t.Error("cross-typed object accepted")
	}
}

// TestWALSectionShapeRejects hand-builds section bytes with a valid crc so
// the frame-shape checks themselves are reached, not shadowed by the crc.
func TestWALSectionShapeRejects(t *testing.T) {
	build := func(mut func(frames []byte) []byte) ([]byte, WALIndexEntry) {
		one := []WALSection{{Group: 1, Epoch: 1, Frames: []WALFrame{
			{Seq: 1, Key: []byte("k"), Payload: []byte("p")},
			{Seq: 2, Key: []byte("k")},
		}}}
		obj, err := AppendWAL(nil, 9, one)
		if err != nil {
			t.Fatal(err)
		}
		footerOff, footerLen, _ := ParseWALTail(obj[len(obj)-16:])
		index, _ := ParseWALFooter(obj[footerOff : footerOff+uint64(footerLen)])
		e := index[0]
		off, n := e.SectionSpan()
		sec := append([]byte(nil), obj[off:off+n]...)
		frames := mut(append([]byte(nil), sec[walSectionHdr:len(sec)-4]...))
		out := append([]byte(nil), sec[:walSectionHdr]...)
		binary.LittleEndian.PutUint32(out[10:], uint32(len(frames)))
		binary.LittleEndian.PutUint32(out[14:], uint32(len(frames)))
		out = append(out, frames...)
		out = binary.LittleEndian.AppendUint32(out, crc32c(frames))
		e.RawLen, e.StoredLen = uint32(len(frames)), uint32(len(frames))
		return out, e
	}

	t.Run("flen past the section", func(t *testing.T) {
		sec, e := build(func(fr []byte) []byte {
			binary.LittleEndian.PutUint32(fr[0:4], uint32(len(fr)+50))
			return fr
		})
		if _, err := ParseWALSection(sec, e); err == nil || !strings.Contains(err.Error(), "does not fit") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("flen smaller than its key", func(t *testing.T) {
		sec, e := build(func(fr []byte) []byte {
			binary.LittleEndian.PutUint32(fr[0:4], walFrameFixed)
			return fr // klen still 1, so flen < fixed+klen
		})
		if _, err := ParseWALSection(sec, e); err == nil || !strings.Contains(err.Error(), "does not fit") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("trailing sub-frame bytes", func(t *testing.T) {
		sec, e := build(func(fr []byte) []byte { return append(fr, 1, 2, 3) })
		if _, err := ParseWALSection(sec, e); err == nil || !strings.Contains(err.Error(), "truncated") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("frame count vs index", func(t *testing.T) {
		sec, e := build(func(fr []byte) []byte { return fr })
		e.NFrames = 3
		if _, err := ParseWALSection(sec, e); err == nil || !strings.Contains(err.Error(), "index says 3") {
			t.Fatalf("err = %v", err)
		}
	})
}

func FuzzParseWAL(f *testing.F) {
	one, _ := AppendWAL(nil, 1, []WALSection{{Group: 1, Epoch: 1, Frames: []WALFrame{{Seq: 1, Key: []byte("k")}}}})
	multi, _ := AppendWAL(nil, 1<<40, sampleWAL())
	f.Add(one)
	f.Add(multi)
	f.Add(one[:HeaderSize])
	f.Add(multi[:len(multi)-1])
	f.Add(append(append([]byte(nil), one...), 0))
	for _, off := range []int{0, 16, HeaderSize, HeaderSize + 6, HeaderSize + walSectionHdr, len(one) - 16, len(one) - 1} {
		b := append([]byte(nil), one...)
		b[off] ^= 0x80
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		sections, h, err := ParseWAL(b)
		if err != nil {
			return
		}
		again, err := AppendWAL(nil, h.Writer, sections)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("accepted bytes do not re-encode to the input (err %v)", err)
		}
	})
}
