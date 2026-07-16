package akifile

import "testing"

// TestSegStatsRoundTrip builds a full seg-stats table the way the writer will,
// streaming the header then rows, and reads it all back.
func TestSegStatsRoundTrip(t *testing.T) {
	entries := []SegStatsEntry{
		{SegOff: 0x4000, LiveBytes: 8192, DeadBytes: 0},
		{SegOff: 0x8000, LiveBytes: 4096, DeadBytes: 4096},
		{SegOff: 0xC000, LiveBytes: 0, DeadBytes: 16384},
	}
	h := SegStatsHeader{
		FullOrDelta: SegStatsFull,
		CkptLogPos:  4242,
		EntryCount:  uint64(len(entries)),
	}

	payload := AppendSegStatsHeader(nil, h)
	for _, e := range entries {
		payload = AppendSegStatsEntry(payload, e)
	}
	if len(payload) != SegStatsHeaderLen+len(entries)*SegStatsEntrySize {
		t.Fatalf("payload len = %d, want %d", len(payload), SegStatsHeaderLen+len(entries)*SegStatsEntrySize)
	}

	got, err := ParseSegStatsHeader(payload)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	if got != h {
		t.Fatalf("header = %+v, want %+v", got, h)
	}

	decoded, err := SegStatsEntries(payload, got)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(decoded) != len(entries) {
		t.Fatalf("got %d rows, want %d", len(decoded), len(entries))
	}
	for i := range entries {
		if decoded[i] != entries[i] {
			t.Fatalf("row %d = %+v, want %+v", i, decoded[i], entries[i])
		}
	}
}

// TestSegStatsDeltaCarriesBaseAndFreed keeps the base offset a delta extends and the
// freed flag through a round trip, the two things a delta adds over a full.
func TestSegStatsDeltaCarriesBaseAndFreed(t *testing.T) {
	h := SegStatsHeader{
		FullOrDelta: SegStatsDelta,
		CkptLogPos:  9000,
		EntryCount:  2,
		BaseCkptOff: 0x40000,
	}
	payload := AppendSegStatsHeader(nil, h)
	payload = AppendSegStatsEntry(payload, SegStatsEntry{SegOff: 0x4000, LiveBytes: 100, DeadBytes: 8092})
	payload = AppendSegStatsEntry(payload, SegStatsEntry{SegOff: 0x8000, Flags: SegStatsFreed})

	got, err := ParseSegStatsHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.FullOrDelta != SegStatsDelta || got.BaseCkptOff != 0x40000 {
		t.Fatalf("delta header lost its base: %+v", got)
	}
	es, err := SegStatsEntries(payload, got)
	if err != nil || len(es) != 2 {
		t.Fatalf("delta rows: %+v/%v", es, err)
	}
	if es[1].Flags&SegStatsFreed == 0 || es[1].SegOff != 0x8000 {
		t.Fatalf("freed row lost its flag: %+v", es[1])
	}
}

// TestParseSegStatsHeaderShort refuses a header buffer below the fixed size.
func TestParseSegStatsHeaderShort(t *testing.T) {
	if _, err := ParseSegStatsHeader(make([]byte, SegStatsHeaderLen-1)); err != ErrShort {
		t.Fatalf("short err = %v, want ErrShort", err)
	}
}

// TestParseSegStatsHeaderBadMagic refuses a payload that is not a seg-stats table.
func TestParseSegStatsHeaderBadMagic(t *testing.T) {
	b := make([]byte, SegStatsHeaderLen)
	copy(b[0:4], "XXXX")
	b[4] = SegStatsFull
	if _, err := ParseSegStatsHeader(b); err != ErrMagic {
		t.Fatalf("bad magic err = %v, want ErrMagic", err)
	}
}

// TestParseSegStatsHeaderRejectsUnknownKind refuses a full-or-delta byte that is
// neither, and a full table that carries a base offset.
func TestParseSegStatsHeaderRejectsUnknownKind(t *testing.T) {
	unknown := AppendSegStatsHeader(nil, SegStatsHeader{FullOrDelta: 9})
	if _, err := ParseSegStatsHeader(unknown); err != ErrSegStats {
		t.Fatalf("unknown kind err = %v, want ErrSegStats", err)
	}
	fullWithBase := AppendSegStatsHeader(nil, SegStatsHeader{FullOrDelta: SegStatsFull, BaseCkptOff: 0x1000})
	if _, err := ParseSegStatsHeader(fullWithBase); err != ErrSegStats {
		t.Fatalf("full-with-base err = %v, want ErrSegStats", err)
	}
}

// TestSegStatsEntriesRejectsOverrunCount catches a corrupt entry_count that claims
// more rows than the payload can hold, so a torn table cannot over-read.
func TestSegStatsEntriesRejectsOverrunCount(t *testing.T) {
	h := SegStatsHeader{FullOrDelta: SegStatsFull, EntryCount: 1}
	payload := AppendSegStatsEntry(AppendSegStatsHeader(nil, h), SegStatsEntry{SegOff: 0x4000})
	// Re-stamp the count to two while only one row is present.
	le.PutUint64(payload[16:24], 2)
	bad, err := ParseSegStatsHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := SegStatsEntries(payload, bad); err != ErrLength {
		t.Fatalf("overrun count err = %v, want ErrLength", err)
	}
}

// TestSegStatsEntriesEmpty decodes a full table of a shard with no tracked
// segments: a header, no rows.
func TestSegStatsEntriesEmpty(t *testing.T) {
	payload := AppendSegStatsHeader(nil, SegStatsHeader{FullOrDelta: SegStatsFull})
	h, err := ParseSegStatsHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	es, err := SegStatsEntries(payload, h)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(es) != 0 {
		t.Fatalf("empty table decoded %d rows", len(es))
	}
}
