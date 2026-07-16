package obs1

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func sampleCheckpoint() Checkpoint {
	return Checkpoint{
		Through: ChainPos{DD: 1, Seq: 4096},
		Members: []Member{
			{Node: 0xA1, Incarnation: 2, Resp: "10.0.0.1:6379", Mesh: "10.0.0.1:7379", Weight: 100, Version: "v0.1.0"},
			{Node: 0xB2, Incarnation: 1, Resp: "10.0.0.2:6379", Mesh: "10.0.0.2:7379", Weight: 100, Version: "v0.1.0"},
		},
		Leases: []LeaseEntry{
			{Group: 0, Node: 0xA1, Epoch: 7, DeadlineMS: 1_752_600_010_000},
			{Group: 2, Node: 0xB2, Epoch: 3, DeadlineMS: 1_752_600_012_000},
		},
		Groups: []GroupCursor{
			{ManSeq: 5, FoldPos: ChainPos{DD: 1, Seq: 4000}},
			{ManSeq: 0, FoldPos: ChainPos{}},
			{ManSeq: 9, FoldPos: ChainPos{DD: 0, Seq: 3990}},
		},
	}
}

func mustCheckpoint(t *testing.T, writer uint64, c Checkpoint) []byte {
	t.Helper()
	b, err := AppendCheckpoint(nil, writer, c)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCheckpointRoundTrip(t *testing.T) {
	want := sampleCheckpoint()
	b := mustCheckpoint(t, 0xA1, want)
	got, h, err := ParseCheckpoint(b)
	if err != nil {
		t.Fatal(err)
	}
	if h.Writer != 0xA1 || !reflect.DeepEqual(got, want) {
		t.Fatalf("parsed %+v header %+v", got, h)
	}
	again := mustCheckpoint(t, h.Writer, got)
	if !bytes.Equal(again, b) {
		t.Fatal("re-encode differs")
	}

	// The empty checkpoint: a fresh chain summarized through zero.
	empty := Checkpoint{}
	eb := mustCheckpoint(t, 1, empty)
	if len(eb) != HeaderSize+ckptMin {
		t.Fatalf("empty checkpoint is %d bytes", len(eb))
	}
	got, _, err = ParseCheckpoint(eb)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, empty) {
		t.Fatalf("empty parse %+v", got)
	}
}

func TestCheckpointWriterRejects(t *testing.T) {
	mut := func(f func(*Checkpoint)) Checkpoint {
		c := sampleCheckpoint()
		f(&c)
		return c
	}
	cases := map[string]Checkpoint{
		"duplicate member node":  mut(func(c *Checkpoint) { c.Members[1].Node = c.Members[0].Node }),
		"descending members":     mut(func(c *Checkpoint) { c.Members[1].Node = 0x01 }),
		"duplicate lease group":  mut(func(c *Checkpoint) { c.Leases[1].Group = c.Leases[0].Group }),
		"descending leases":      mut(func(c *Checkpoint) { c.Leases = []LeaseEntry{{Group: 2}, {Group: 0}} }),
		"fold pos past ceiling":  mut(func(c *Checkpoint) { c.Groups[0].FoldPos.Seq = chainSeqMax + 1 }),
		"member invalid version": mut(func(c *Checkpoint) { c.Members[0].Version = string(make([]byte, 256)) }),
	}
	for name, c := range cases {
		if _, err := AppendCheckpoint(nil, 1, c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestCheckpointParseRejects(t *testing.T) {
	good := mustCheckpoint(t, 0xA1, sampleCheckpoint())

	flip := func(name string, i int) {
		t.Helper()
		b := append([]byte(nil), good...)
		b[i] ^= 0x01
		if _, _, err := ParseCheckpoint(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	flip("through byte", HeaderSize)
	flip("member row byte", HeaderSize+9+4+2+1)
	flip("final crc byte", len(good)-1)

	if _, _, err := ParseCheckpoint(good[:len(good)-1]); err == nil {
		t.Error("truncated checkpoint accepted")
	}
	if _, _, err := ParseCheckpoint(good[:HeaderSize+8]); err == nil {
		t.Error("headerless stub accepted")
	}
	badVersion := AppendHeader(nil, Header{Format: FormatCheckpoint, FVersion: 2, Writer: 1})
	badVersion = append(badVersion, good[HeaderSize:]...)
	if _, _, err := ParseCheckpoint(badVersion); err == nil {
		t.Error("fversion 2 accepted")
	}
	crossType := AppendHeader(nil, Header{Format: FormatChain, FVersion: 1, Writer: 1})
	crossType = append(crossType, good[HeaderSize:]...)
	if _, _, err := ParseCheckpoint(crossType); err == nil {
		t.Error("cross-typed object accepted")
	}

	// A section body flip with the final crc restored reaches the section
	// crc check itself instead of the outer one.
	sectioned := append([]byte(nil), good...)
	sectioned[HeaderSize+9+4+2] ^= 0x01
	reCRC(sectioned)
	if _, _, err := ParseCheckpoint(sectioned); err == nil {
		t.Error("section body flip with valid final crc accepted")
	}

	// Section length overrunning the payload, final crc restored.
	overrun := append([]byte(nil), good...)
	binary.LittleEndian.PutUint32(overrun[HeaderSize+9:], 1<<20)
	reCRC(overrun)
	if _, _, err := ParseCheckpoint(overrun); err == nil {
		t.Error("section length overrun accepted")
	}

	// Extra bytes after the last section, final crc valid over them.
	trailing := append([]byte(nil), good[:len(good)-4]...)
	trailing = append(trailing, 0)
	trailing = binary.LittleEndian.AppendUint32(trailing, crc32c(trailing[HeaderSize:]))
	if _, _, err := ParseCheckpoint(trailing); err == nil {
		t.Error("trailing section bytes accepted")
	}
}

func FuzzParseCheckpoint(f *testing.F) {
	good, err := AppendCheckpoint(nil, 0xA1, sampleCheckpoint())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	empty, err := AppendCheckpoint(nil, 1, Checkpoint{})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(empty)
	f.Fuzz(func(t *testing.T, b []byte) {
		c, h, err := ParseCheckpoint(b)
		if err != nil {
			return
		}
		again, err := AppendCheckpoint(nil, h.Writer, c)
		if err != nil {
			t.Fatalf("accepted checkpoint fails re-encode: %v", err)
		}
		if !bytes.Equal(again, b) {
			t.Fatal("accepted checkpoint re-encodes differently")
		}
	})
}
