package akifile

import (
	"bytes"
	"testing"
)

func encodeMetaKV(pairs []MetaKVPair) []byte {
	payload := AppendMetaKVHeader(nil, MetaKVHeader{EntryCount: uint64(len(pairs))})
	for _, p := range pairs {
		payload = AppendMetaKVPair(payload, p.Key, p.Value)
	}
	return payload
}

// TestMetaKVRoundTrip builds a map of config and provenance entries, including an
// empty value, and reads them back unchanged.
func TestMetaKVRoundTrip(t *testing.T) {
	pairs := []MetaKVPair{
		{Key: []byte("import.source"), Value: []byte("RDB v12")},
		{Key: []byte("import.hash"), Value: []byte("9f86d081884c7d65")},
		{Key: []byte("config.maxmemory"), Value: []byte("512mb")},
		{Key: []byte("note"), Value: []byte("")}, // an empty value round-trips
	}
	payload := encodeMetaKV(pairs)

	h, err := ParseMetaKVHeader(payload)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	if h.EntryCount != 4 {
		t.Fatalf("entry count = %d, want 4", h.EntryCount)
	}
	got, err := MetaKVPairs(payload, h)
	if err != nil {
		t.Fatalf("pairs: %v", err)
	}
	if len(got) != len(pairs) {
		t.Fatalf("got %d pairs, want %d", len(got), len(pairs))
	}
	for i := range pairs {
		if !bytes.Equal(got[i].Key, pairs[i].Key) || !bytes.Equal(got[i].Value, pairs[i].Value) {
			t.Fatalf("pair %d = %q=%q, want %q=%q", i, got[i].Key, got[i].Value, pairs[i].Key, pairs[i].Value)
		}
	}
}

// TestMetaKVLookupFindsProvenance confirms the lookup a file-info read uses, and that
// an absent key reports not-found.
func TestMetaKVLookupFindsProvenance(t *testing.T) {
	pairs, err := MetaKVPairs(encodeMetaKV([]MetaKVPair{
		{Key: []byte("import.source"), Value: []byte("RDB v12")},
	}), MetaKVHeader{EntryCount: 1})
	if err != nil {
		t.Fatalf("pairs: %v", err)
	}
	if v, ok := MetaKVLookup(pairs, "import.source"); !ok || string(v) != "RDB v12" {
		t.Fatalf("lookup = %q,%v, want RDB v12,true", v, ok)
	}
	if _, ok := MetaKVLookup(pairs, "absent"); ok {
		t.Fatalf("absent key reported present")
	}
}

// TestMetaKVPairsCopiesOut confirms the decoded bytes do not alias the payload, so a
// caller may recycle the buffer.
func TestMetaKVPairsCopiesOut(t *testing.T) {
	payload := encodeMetaKV([]MetaKVPair{{Key: []byte("k"), Value: []byte("v")}})
	pairs, err := MetaKVPairs(payload, MetaKVHeader{EntryCount: 1})
	if err != nil {
		t.Fatalf("pairs: %v", err)
	}
	for i := range payload {
		payload[i] = 0xff
	}
	if string(pairs[0].Key) != "k" || string(pairs[0].Value) != "v" {
		t.Fatalf("decoded pair aliased the payload: %q=%q", pairs[0].Key, pairs[0].Value)
	}
}

// TestParseMetaKVHeaderShort refuses a header buffer below the fixed size.
func TestParseMetaKVHeaderShort(t *testing.T) {
	if _, err := ParseMetaKVHeader(make([]byte, MetaKVHeaderLen-1)); err != ErrShort {
		t.Fatalf("short err = %v, want ErrShort", err)
	}
}

// TestParseMetaKVHeaderBadMagic refuses a payload that is not a meta_kv map.
func TestParseMetaKVHeaderBadMagic(t *testing.T) {
	b := make([]byte, MetaKVHeaderLen)
	copy(b[0:4], "XXXX")
	if _, err := ParseMetaKVHeader(b); err != ErrMagic {
		t.Fatalf("bad magic err = %v, want ErrMagic", err)
	}
}

// TestMetaKVPairsRejectsOverrunCount catches an entry_count that claims more pairs
// than the payload can hold.
func TestMetaKVPairsRejectsOverrunCount(t *testing.T) {
	payload := encodeMetaKV([]MetaKVPair{{Key: []byte("k"), Value: []byte("v")}})
	le.PutUint64(payload[8:16], 1000) // claim far more pairs than are present
	bad, err := ParseMetaKVHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := MetaKVPairs(payload, bad); err != ErrLength {
		t.Fatalf("overrun count err = %v, want ErrLength", err)
	}
}

// TestMetaKVPairsRejectsOverrunFieldLength catches a value length that runs past the
// payload, a torn map that must not over-read.
func TestMetaKVPairsRejectsOverrunFieldLength(t *testing.T) {
	payload := encodeMetaKV([]MetaKVPair{{Key: []byte("k"), Value: []byte("v")}})
	// The value length word sits after the header, the key-length word, and the
	// one-byte key: 16 + 4 + 1 = 21.
	le.PutUint32(payload[21:25], 500)
	if _, err := MetaKVPairs(payload, MetaKVHeader{EntryCount: 1}); err != ErrLength {
		t.Fatalf("overrun field length err = %v, want ErrLength", err)
	}
}

// TestMetaKVPairsEmpty decodes a map with no entries: a header and nothing after it.
func TestMetaKVPairsEmpty(t *testing.T) {
	payload := AppendMetaKVHeader(nil, MetaKVHeader{})
	h, err := ParseMetaKVHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	pairs, err := MetaKVPairs(payload, h)
	if err != nil {
		t.Fatalf("pairs: %v", err)
	}
	if len(pairs) != 0 {
		t.Fatalf("empty map decoded %d pairs", len(pairs))
	}
}
