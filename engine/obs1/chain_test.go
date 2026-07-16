package obs1

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
)

func sampleBatch() ChainBatch {
	return ChainBatch{
		BatchID:     0xB47C,
		Incarnation: 3,
		Records: []ChainRecord{
			CommitRecord{WALNode: 0xA1, WALSeq: 12, WALSize: 4096, Sections: []CommitSection{
				{Group: 1, Epoch: 7, Offset: 32, StoredLen: 900, NFrames: 4, FirstSeq: 10, LastSeq: 13},
				{Group: 5, Epoch: 2, Offset: 940, StoredLen: 60, NFrames: 1, FirstSeq: 3, LastSeq: 3},
			}},
			GrantRecord{Group: 5, Node: 0xA1, Epoch: 3},
			ReleaseRecord{Group: 1, Epoch: 7},
			HeartbeatRecord{},
			MemberRecord{Op: MemberJoin, Member: Member{
				Node: 0xB2, Incarnation: 1, Resp: "10.0.0.2:6379", Mesh: "10.0.0.2:7379", Weight: 100, Version: "v0.1.0",
			}},
			CheckpointRecord{Pos: ChainPos{DD: 1, Seq: 4096}},
		},
	}
}

func mustBatch(t *testing.T, writer uint64, batch ChainBatch) []byte {
	t.Helper()
	b, err := AppendChainBatch(nil, writer, batch)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestChainBatchRoundTrip(t *testing.T) {
	want := sampleBatch()
	b := mustBatch(t, 0xA1, want)
	got, h, err := ParseChainBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if h.Writer != 0xA1 || !reflect.DeepEqual(got, want) {
		t.Fatalf("parsed %+v header %+v", got, h)
	}
	again := mustBatch(t, h.Writer, got)
	if !bytes.Equal(again, b) {
		t.Fatal("re-encode differs")
	}

	// A heartbeat-only batch is the smallest legal chain object.
	beat := ChainBatch{BatchID: 1, Incarnation: 1, Records: []ChainRecord{HeartbeatRecord{}}}
	got, _, err = ParseChainBatch(mustBatch(t, 2, beat))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, beat) {
		t.Fatalf("heartbeat batch parsed %+v", got)
	}
}

// TestCommitRepeatsWALIndex pins the doc 03 claim that a commit record is
// the WAL footer index repeated: build a real WAL object, lift its footer
// rows into a commit, and prove the round trip preserves them.
func TestCommitRepeatsWALIndex(t *testing.T) {
	wal := mustWAL(t, 0xA1, sampleWAL())
	footerOff, footerLen, err := ParseTail(wal[len(wal)-TailSize:])
	if err != nil {
		t.Fatal(err)
	}
	entries, err := ParseWALFooter(wal[footerOff : footerOff+uint64(footerLen)])
	if err != nil {
		t.Fatal(err)
	}
	commit := CommitRecord{WALNode: 0xA1, WALSeq: 1, WALSize: uint64(len(wal))}
	for _, e := range entries {
		commit.Sections = append(commit.Sections, e.CommitSection())
	}
	batch := ChainBatch{BatchID: 9, Incarnation: 1, Records: []ChainRecord{commit}}
	got, _, err := ParseChainBatch(mustBatch(t, 0xA1, batch))
	if err != nil {
		t.Fatal(err)
	}
	back := got.Records[0].(CommitRecord)
	if len(back.Sections) != len(entries) {
		t.Fatalf("%d sections back, want %d", len(back.Sections), len(entries))
	}
	for i, e := range entries {
		if back.Sections[i] != e.CommitSection() {
			t.Fatalf("section %d: %+v vs footer %+v", i, back.Sections[i], e)
		}
	}
}

func TestChainKeys(t *testing.T) {
	if k := chainKey("db/a", 0, 7); k != "db/a/chain/00/0000000000000007" {
		t.Fatalf("chainKey %q", k)
	}
	if k := chainCkptKey("db/a", 12, 4096); k != "db/a/chain/12/ckpt/0000000000004096" {
		t.Fatalf("chainCkptKey %q", k)
	}
}

func TestChainWriterRejects(t *testing.T) {
	one := func(rec ChainRecord) ChainBatch {
		return ChainBatch{BatchID: 1, Incarnation: 1, Records: []ChainRecord{rec}}
	}
	okSection := CommitSection{Group: 1, Epoch: 1, StoredLen: 10, NFrames: 1, FirstSeq: 1, LastSeq: 1}
	hugeCommit := CommitRecord{Sections: make([]CommitSection, 1724)}
	for i := range hugeCommit.Sections {
		hugeCommit.Sections[i] = okSection
	}
	cases := map[string]ChainBatch{
		"empty batch":             {BatchID: 1, Incarnation: 1},
		"commit with no sections": one(CommitRecord{WALNode: 1}),
		"commit zero frames":      one(CommitRecord{Sections: []CommitSection{{Group: 1, FirstSeq: 1, LastSeq: 1}}}),
		"commit seq inverted":     one(CommitRecord{Sections: []CommitSection{{Group: 1, NFrames: 1, FirstSeq: 5, LastSeq: 4}}}),
		"member op 0":             one(MemberRecord{Op: 0}),
		"member op 3":             one(MemberRecord{Op: 3}),
		"member long version":     one(MemberRecord{Op: MemberJoin, Member: Member{Version: strings.Repeat("v", 256)}}),
		"member long endpoint":    one(MemberRecord{Op: MemberJoin, Member: Member{Resp: strings.Repeat("a", 65536)}}),
		"record body over cap":    one(hugeCommit),
	}
	for name, batch := range cases {
		if _, err := AppendChainBatch(nil, 1, batch); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestChainParseRejects(t *testing.T) {
	good := mustBatch(t, 0xA1, sampleBatch())

	flip := func(name string, i int) {
		t.Helper()
		b := append([]byte(nil), good...)
		b[i] ^= 0x01
		if _, _, err := ParseChainBatch(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	flip("batch id byte", HeaderSize)
	flip("record byte", HeaderSize+chainBatchHdr+3)
	flip("crc byte", len(good)-1)

	if _, _, err := ParseChainBatch(good[:len(good)-1]); err == nil {
		t.Error("truncated batch accepted")
	}
	if _, _, err := ParseChainBatch(good[:HeaderSize+4]); err == nil {
		t.Error("headerless stub accepted")
	}
	badVersion := AppendHeader(nil, Header{Format: FormatChain, FVersion: 2, Writer: 1})
	badVersion = append(badVersion, good[HeaderSize:]...)
	if _, _, err := ParseChainBatch(badVersion); err == nil {
		t.Error("fversion 2 accepted")
	}
	crossType := AppendHeader(nil, Header{Format: FormatCheckpoint, FVersion: 1, Writer: 1})
	crossType = append(crossType, good[HeaderSize:]...)
	if _, _, err := ParseChainBatch(crossType); err == nil {
		t.Error("cross-typed object accepted")
	}

	// The shape checks behind the crc, each with the crc restored.
	shape := func(name string, mut func(b []byte)) {
		t.Helper()
		b := append([]byte(nil), good...)
		mut(b)
		reCRC(b)
		if _, _, err := ParseChainBatch(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	shape("unknown kind", func(b []byte) { b[HeaderSize+chainBatchHdr+2] = 0x07 })
	shape("zero rlen", func(b []byte) { binary.LittleEndian.PutUint16(b[HeaderSize+chainBatchHdr:], 0) })
	shape("rlen past payload", func(b []byte) { binary.LittleEndian.PutUint16(b[HeaderSize+chainBatchHdr:], 0xFFFF) })
	shape("undercounted records", func(b []byte) { binary.LittleEndian.PutUint16(b[HeaderSize+12:], 5) })
	shape("overcounted records", func(b []byte) { binary.LittleEndian.PutUint16(b[HeaderSize+12:], 7) })
	shape("zero records", func(b []byte) { binary.LittleEndian.PutUint16(b[HeaderSize+12:], 0) })
}

func FuzzParseChainBatch(f *testing.F) {
	good, err := AppendChainBatch(nil, 0xA1, sampleBatch())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	beat, err := AppendChainBatch(nil, 2, ChainBatch{BatchID: 1, Incarnation: 1, Records: []ChainRecord{HeartbeatRecord{}}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(beat)
	f.Fuzz(func(t *testing.T, b []byte) {
		batch, h, err := ParseChainBatch(b)
		if err != nil {
			return
		}
		again, err := AppendChainBatch(nil, h.Writer, batch)
		if err != nil {
			t.Fatalf("accepted batch fails re-encode: %v", err)
		}
		if !bytes.Equal(again, b) {
			t.Fatal("accepted batch re-encodes differently")
		}
	})
}
