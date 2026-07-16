package akifile

import "testing"

// TestCheckpointRoundTrip builds a full checkpoint payload the way the writer
// will, streaming the header then entries, and reads it all back.
func TestCheckpointRoundTrip(t *testing.T) {
	entries := []CkptEntry{
		{KeyHash: 0x1111111111111111, RecordAddr: 0x2000, Slot: 3, Flags: 0},
		{KeyHash: 0x2222222222222222, RecordAddr: 0x8000000000004000, Slot: 7, Flags: CkptTombstone},
		{KeyHash: 0x3333333333333333, RecordAddr: 0x6000, Slot: 11, Flags: 0},
	}
	h := CkptHeader{
		FullOrDelta: CkptFull,
		CkptLogPos:  4242,
		EntryCount:  uint64(len(entries)),
		BucketCount: 1024,
		SeqHigh:     4200,
	}

	payload := AppendCkptHeader(nil, h)
	for _, e := range entries {
		payload = AppendCkptEntry(payload, e)
	}
	if len(payload) != CkptHeaderLen+len(entries)*CkptEntrySize {
		t.Fatalf("payload len = %d, want %d", len(payload), CkptHeaderLen+len(entries)*CkptEntrySize)
	}

	got, err := ParseCkptHeader(payload)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	if got != h {
		t.Fatalf("header = %+v, want %+v", got, h)
	}

	decoded, err := CkptEntries(payload, got)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(decoded) != len(entries) {
		t.Fatalf("got %d entries, want %d", len(decoded), len(entries))
	}
	for i := range entries {
		if decoded[i] != entries[i] {
			t.Fatalf("entry %d = %+v, want %+v", i, decoded[i], entries[i])
		}
	}
}

// TestCheckpointDeltaCarriesBase keeps the base offset a delta extends and the
// tombstone flags through a round trip.
func TestCheckpointDeltaCarriesBase(t *testing.T) {
	h := CkptHeader{
		FullOrDelta: CkptDelta,
		CkptLogPos:  9000,
		EntryCount:  1,
		BucketCount: 2048,
		BaseCkptOff: 0x40000,
		SeqHigh:     8999,
	}
	payload := AppendCkptEntry(AppendCkptHeader(nil, h), CkptEntry{KeyHash: 1, Flags: CkptTombstone})
	got, err := ParseCkptHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.FullOrDelta != CkptDelta || got.BaseCkptOff != 0x40000 {
		t.Fatalf("delta header lost its base: %+v", got)
	}
	es, err := CkptEntries(payload, got)
	if err != nil || len(es) != 1 || es[0].Flags&CkptTombstone == 0 {
		t.Fatalf("delta tombstone lost: %+v/%v", es, err)
	}
}

// TestParseCkptHeaderShort refuses a header buffer below the fixed size.
func TestParseCkptHeaderShort(t *testing.T) {
	if _, err := ParseCkptHeader(make([]byte, CkptHeaderLen-1)); err != ErrShort {
		t.Fatalf("short err = %v, want ErrShort", err)
	}
}

// TestParseCkptHeaderBadMagic refuses a payload that is not a checkpoint.
func TestParseCkptHeaderBadMagic(t *testing.T) {
	b := make([]byte, CkptHeaderLen)
	copy(b[0:4], "XXXX")
	b[4] = CkptFull
	if _, err := ParseCkptHeader(b); err != ErrMagic {
		t.Fatalf("bad magic err = %v, want ErrMagic", err)
	}
}

// TestParseCkptHeaderRejectsUnknownKind refuses a full-or-delta byte that is
// neither, and a full dump that carries a base offset.
func TestParseCkptHeaderRejectsUnknownKind(t *testing.T) {
	unknown := AppendCkptHeader(nil, CkptHeader{FullOrDelta: 9})
	if _, err := ParseCkptHeader(unknown); err != ErrCheckpoint {
		t.Fatalf("unknown kind err = %v, want ErrCheckpoint", err)
	}
	fullWithBase := AppendCkptHeader(nil, CkptHeader{FullOrDelta: CkptFull, BaseCkptOff: 0x1000})
	if _, err := ParseCkptHeader(fullWithBase); err != ErrCheckpoint {
		t.Fatalf("full-with-base err = %v, want ErrCheckpoint", err)
	}
}

// TestCkptEntriesRejectsOverrunCount catches a corrupt entry_count that claims
// more entries than the payload can hold, so a torn checkpoint cannot over-read.
func TestCkptEntriesRejectsOverrunCount(t *testing.T) {
	h := CkptHeader{FullOrDelta: CkptFull, EntryCount: 1}
	payload := AppendCkptEntry(AppendCkptHeader(nil, h), CkptEntry{KeyHash: 1})
	// Re-stamp the count to two while only one entry is present.
	le.PutUint64(payload[16:24], 2)
	bad, err := ParseCkptHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := CkptEntries(payload, bad); err != ErrLength {
		t.Fatalf("overrun count err = %v, want ErrLength", err)
	}
}

// TestCkptEntriesEmpty decodes a full dump of an empty index: a header, no
// entries.
func TestCkptEntriesEmpty(t *testing.T) {
	payload := AppendCkptHeader(nil, CkptHeader{FullOrDelta: CkptFull})
	h, err := ParseCkptHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	es, err := CkptEntries(payload, h)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(es) != 0 {
		t.Fatalf("empty dump decoded %d entries", len(es))
	}
}
