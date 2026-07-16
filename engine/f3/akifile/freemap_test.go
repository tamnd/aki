package akifile

import "testing"

// TestFreeMapRoundTrip builds a free-map payload the way the writer will, streaming
// the header then runs, and reads it all back.
func TestFreeMapRoundTrip(t *testing.T) {
	entries := []FreeExtent{
		{StartOff: 0x4000, Length: 8192},
		{StartOff: 0x10000, Length: 4096, Flags: FreeMapPending},
		{StartOff: 0x20000, Length: 65536},
	}
	h := FreeMapHeader{EntryCount: uint64(len(entries))}

	payload := AppendFreeMapHeader(nil, h)
	for _, e := range entries {
		payload = AppendFreeExtent(payload, e)
	}
	if len(payload) != FreeMapHeaderLen+len(entries)*FreeExtentSize {
		t.Fatalf("payload len = %d, want %d", len(payload), FreeMapHeaderLen+len(entries)*FreeExtentSize)
	}

	got, err := ParseFreeMapHeader(payload)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	if got != h {
		t.Fatalf("header = %+v, want %+v", got, h)
	}

	decoded, err := FreeExtents(payload, got)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(decoded) != len(entries) {
		t.Fatalf("got %d runs, want %d", len(decoded), len(entries))
	}
	for i := range entries {
		if decoded[i] != entries[i] {
			t.Fatalf("run %d = %+v, want %+v", i, decoded[i], entries[i])
		}
	}
}

// TestFreeMapTotalsSplitFreeAndPending confirms the F9 forward-progress signal: the
// free total counts only allocatable runs, pending runs are held out until the flip.
func TestFreeMapTotalsSplitFreeAndPending(t *testing.T) {
	entries := []FreeExtent{
		{StartOff: 0x4000, Length: 8192},
		{StartOff: 0x10000, Length: 4096, Flags: FreeMapPending},
		{StartOff: 0x20000, Length: 65536},
	}
	free, pending := FreeMapTotals(entries)
	if free != 8192+65536 {
		t.Fatalf("free total = %d, want %d", free, 8192+65536)
	}
	if pending != 4096 {
		t.Fatalf("pending total = %d, want 4096", pending)
	}
}

// TestParseFreeMapHeaderShort refuses a header buffer below the fixed size.
func TestParseFreeMapHeaderShort(t *testing.T) {
	if _, err := ParseFreeMapHeader(make([]byte, FreeMapHeaderLen-1)); err != ErrShort {
		t.Fatalf("short err = %v, want ErrShort", err)
	}
}

// TestParseFreeMapHeaderBadMagic refuses a payload that is not a free map.
func TestParseFreeMapHeaderBadMagic(t *testing.T) {
	b := make([]byte, FreeMapHeaderLen)
	copy(b[0:4], "XXXX")
	if _, err := ParseFreeMapHeader(b); err != ErrMagic {
		t.Fatalf("bad magic err = %v, want ErrMagic", err)
	}
}

// TestFreeExtentsRejectsOverrunCount catches a corrupt entry_count that claims more
// runs than the payload can hold, so a torn free map cannot over-read.
func TestFreeExtentsRejectsOverrunCount(t *testing.T) {
	h := FreeMapHeader{EntryCount: 1}
	payload := AppendFreeExtent(AppendFreeMapHeader(nil, h), FreeExtent{StartOff: 0x4000, Length: 4096})
	// Re-stamp the count to two while only one run is present.
	le.PutUint64(payload[8:16], 2)
	bad, err := ParseFreeMapHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := FreeExtents(payload, bad); err != ErrLength {
		t.Fatalf("overrun count err = %v, want ErrLength", err)
	}
}

// TestFreeExtentsEmpty decodes a free map with nothing reclaimable: a header, no
// runs, and zero totals.
func TestFreeExtentsEmpty(t *testing.T) {
	payload := AppendFreeMapHeader(nil, FreeMapHeader{})
	h, err := ParseFreeMapHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	es, err := FreeExtents(payload, h)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(es) != 0 {
		t.Fatalf("empty free map decoded %d runs", len(es))
	}
	if free, pending := FreeMapTotals(es); free != 0 || pending != 0 {
		t.Fatalf("empty totals = %d/%d, want 0/0", free, pending)
	}
}
