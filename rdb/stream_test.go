package rdb

import (
	"bytes"
	"fmt"
	"testing"
)

// sampleStream builds a stream with the given entry count, plus one consumer group
// with two consumers sharing a pending list, so the round-trip tests exercise the
// entry, group, PEL and consumer paths together.
func sampleStream(n int) *StreamData {
	sd := &StreamData{
		EntriesAdded: uint64(n),
	}
	for i := 0; i < n; i++ {
		sd.Entries = append(sd.Entries, StreamEntry{
			MS:  1000 + uint64(i),
			Seq: 0,
			Fields: [][]byte{
				[]byte("field"), []byte(fmt.Sprintf("val-%d", i)),
				[]byte("n"), []byte(fmt.Sprintf("%d", i)),
			},
		})
	}
	if n > 0 {
		sd.FirstMS, sd.FirstSeq = sd.Entries[0].MS, sd.Entries[0].Seq
		last := sd.Entries[n-1]
		sd.LastMS, sd.LastSeq = last.MS, last.Seq
	}
	sd.MaxDelMS, sd.MaxDelSeq = 500, 0
	if n >= 2 {
		sd.Groups = []StreamGroup{{
			Name:        []byte("g1"),
			LastMS:      sd.Entries[1].MS,
			LastSeq:     sd.Entries[1].Seq,
			EntriesRead: 2,
			PEL: []StreamPEL{
				{MS: sd.Entries[0].MS, Seq: 0, DeliveryTime: 111111, DeliveryCount: 1},
				{MS: sd.Entries[1].MS, Seq: 0, DeliveryTime: 222222, DeliveryCount: 3},
			},
			Consumers: []StreamConsumer{
				{Name: []byte("alice"), SeenTime: 111111, ActiveTime: 111111,
					PendingIDs: []StreamID{{MS: sd.Entries[0].MS, Seq: 0}}},
				{Name: []byte("bob"), SeenTime: 222222, ActiveTime: 222222,
					PendingIDs: []StreamID{{MS: sd.Entries[1].MS, Seq: 0}}},
			},
		}}
	}
	return sd
}

// checkStreamEqual fails when two decoded streams differ in any field the codec is
// meant to preserve.
func checkStreamEqual(t *testing.T, got, want *StreamData) {
	t.Helper()
	if got == nil {
		t.Fatal("decoded stream is nil")
	}
	if len(got.Entries) != len(want.Entries) {
		t.Fatalf("entries = %d want %d", len(got.Entries), len(want.Entries))
	}
	for i := range want.Entries {
		ge, we := got.Entries[i], want.Entries[i]
		if ge.MS != we.MS || ge.Seq != we.Seq {
			t.Fatalf("entry %d id = %d-%d want %d-%d", i, ge.MS, ge.Seq, we.MS, we.Seq)
		}
		if len(ge.Fields) != len(we.Fields) {
			t.Fatalf("entry %d fields = %d want %d", i, len(ge.Fields), len(we.Fields))
		}
		for j := range we.Fields {
			if !bytes.Equal(ge.Fields[j], we.Fields[j]) {
				t.Fatalf("entry %d field %d = %q want %q", i, j, ge.Fields[j], we.Fields[j])
			}
		}
	}
	if got.LastMS != want.LastMS || got.LastSeq != want.LastSeq {
		t.Fatalf("last id = %d-%d want %d-%d", got.LastMS, got.LastSeq, want.LastMS, want.LastSeq)
	}
	if got.FirstMS != want.FirstMS || got.FirstSeq != want.FirstSeq {
		t.Fatalf("first id = %d-%d want %d-%d", got.FirstMS, got.FirstSeq, want.FirstMS, want.FirstSeq)
	}
	if got.MaxDelMS != want.MaxDelMS || got.MaxDelSeq != want.MaxDelSeq {
		t.Fatalf("max deleted id = %d-%d want %d-%d", got.MaxDelMS, got.MaxDelSeq, want.MaxDelMS, want.MaxDelSeq)
	}
	if got.EntriesAdded != want.EntriesAdded {
		t.Fatalf("entries added = %d want %d", got.EntriesAdded, want.EntriesAdded)
	}
	if len(got.Groups) != len(want.Groups) {
		t.Fatalf("groups = %d want %d", len(got.Groups), len(want.Groups))
	}
	for i := range want.Groups {
		gg, wg := got.Groups[i], want.Groups[i]
		if !bytes.Equal(gg.Name, wg.Name) {
			t.Fatalf("group %d name = %q want %q", i, gg.Name, wg.Name)
		}
		if gg.LastMS != wg.LastMS || gg.LastSeq != wg.LastSeq || gg.EntriesRead != wg.EntriesRead {
			t.Fatalf("group %d header mismatch: %+v vs %+v", i, gg, wg)
		}
		if len(gg.PEL) != len(wg.PEL) {
			t.Fatalf("group %d pel = %d want %d", i, len(gg.PEL), len(wg.PEL))
		}
		for j := range wg.PEL {
			if gg.PEL[j] != wg.PEL[j] {
				t.Fatalf("group %d pel %d = %+v want %+v", i, j, gg.PEL[j], wg.PEL[j])
			}
		}
		if len(gg.Consumers) != len(wg.Consumers) {
			t.Fatalf("group %d consumers = %d want %d", i, len(gg.Consumers), len(wg.Consumers))
		}
		for j := range wg.Consumers {
			gc, wc := gg.Consumers[j], wg.Consumers[j]
			if !bytes.Equal(gc.Name, wc.Name) || gc.SeenTime != wc.SeenTime || gc.ActiveTime != wc.ActiveTime {
				t.Fatalf("group %d consumer %d header mismatch: %+v vs %+v", i, j, gc, wc)
			}
			if len(gc.PendingIDs) != len(wc.PendingIDs) {
				t.Fatalf("group %d consumer %d pending = %d want %d", i, j, len(gc.PendingIDs), len(wc.PendingIDs))
			}
			for k := range wc.PendingIDs {
				if gc.PendingIDs[k] != wc.PendingIDs[k] {
					t.Fatalf("group %d consumer %d pending %d = %+v want %+v", i, j, k, gc.PendingIDs[k], wc.PendingIDs[k])
				}
			}
		}
	}
}

// TestStreamDumpRoundTrip serializes a stream into a DUMP payload and reads it back.
func TestStreamDumpRoundTrip(t *testing.T) {
	want := sampleStream(5)
	payload, err := Marshal(Value{Kind: KindStream, Stream: want})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Kind != KindStream {
		t.Fatalf("kind = %v want KindStream", got.Kind)
	}
	checkStreamEqual(t, got.Stream, want)
}

// TestStreamMultiNode checks a stream larger than one macro node round-trips, so the
// chunking and the per-node master diffs are exercised.
func TestStreamMultiNode(t *testing.T) {
	want := sampleStream(250)
	payload, err := Marshal(Value{Kind: KindStream, Stream: want})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	checkStreamEqual(t, got.Stream, want)
}

// TestStreamEmpty checks a stream with no entries and no groups round-trips, the
// shape an XADD-then-trim or a freshly created group-only stream can leave behind.
func TestStreamEmpty(t *testing.T) {
	want := &StreamData{LastMS: 42, LastSeq: 7, EntriesAdded: 3, MaxDelMS: 42, MaxDelSeq: 7}
	payload, err := Marshal(Value{Kind: KindStream, Stream: want})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	checkStreamEqual(t, got.Stream, want)
}

// TestStreamInFile checks a stream survives a whole-file snapshot round-trip beside
// an ordinary key, the path SAVE and a full sync take.
func TestStreamInFile(t *testing.T) {
	want := sampleStream(3)
	snap := Snapshot{DBs: []DBData{{Index: 0, Entries: []Entry{
		{Key: []byte("s"), Value: Value{Kind: KindStream, Stream: want}, ExpireMS: -1},
		{Key: []byte("k"), Value: Value{Kind: KindString, Str: []byte("v")}, ExpireMS: -1},
	}}}}
	blob, err := MarshalFile(snap)
	if err != nil {
		t.Fatalf("MarshalFile: %v", err)
	}
	out, err := UnmarshalFile(blob)
	if err != nil {
		t.Fatalf("UnmarshalFile: %v", err)
	}
	db0 := findDB(t, out, 0)
	got := entryByKey(db0, "s").Value
	if got.Kind != KindStream {
		t.Fatalf("s kind = %v want KindStream", got.Kind)
	}
	checkStreamEqual(t, got.Stream, want)
	if string(entryByKey(db0, "k").Value.Str) != "v" {
		t.Fatalf("k = %q", entryByKey(db0, "k").Value.Str)
	}
}
