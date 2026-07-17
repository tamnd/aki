package akifile

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"
)

// layRecords stages one batch of rows for a shard and cuts it, returning the file so a
// test reads back what a dump folds. Each call is its own segment, so successive calls
// model the append order a supersession and a tombstone rely on.
func layRecords(t *testing.T, f *File, shard uint16, seq uint64, rows []RecordRow) {
	t.Helper()
	w := NewRecordLogWriter(f, shard)
	for _, r := range rows {
		w.Stage(r)
	}
	if _, err := w.Flush(seq); err != nil {
		t.Fatalf("flush shard %d seq %d: %v", shard, seq, err)
	}
}

// inlineExpiring is inlineRow with an expiry stamped, to prove the dump carries the
// TTL a record frames.
func inlineExpiring(key, val string, expire uint64) RecordRow {
	r := inlineRow(key, val)
	r.ExpireAt = expire
	return r
}

func sepRow(key string, word uint64, vlen uint32) RecordRow {
	return RecordRow{ValueWord: word, ValueLen: vlen, Key: []byte(key)}
}

func tombRow(key string) RecordRow {
	return RecordRow{Flags: RecFlagTombstone, Key: []byte(key)}
}

// TestDumpFoldsLiveSet lays a mix of inline sets, a separated set, a supersession, and
// a tombstone across two shards, then dumps and checks the fold: the newest inline
// value wins, a deleted key is gone, a separated record surfaces its pointer word, and
// each shard's records are keyed to it. This is the logical-contents contract a
// crash-matrix diff rests on.
func TestDumpFoldsLiveSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.aki")
	f, err := Create(path, CreateOptions{ShardCount: 2, SepThreshold: 64, Sync: SyncNo})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = f.Close() }()

	// Shard 0: set a inline, set b separated, then supersede a and delete b.
	layRecords(t, f, 0, 1, []RecordRow{inlineRow("a", "a1"), sepRow("b", 0x1234, 9)})
	layRecords(t, f, 0, 2, []RecordRow{inlineExpiring("a", "a2", 77), tombRow("b")})
	// Shard 1: one live inline key.
	layRecords(t, f, 1, 1, []RecordRow{inlineRow("x", "x1")})

	var got []DumpRecord
	if err := Dump(f, func(r DumpRecord) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("dump: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("dumped %d records, want 2 (a superseded, b deleted, x live)", len(got))
	}
	a := got[0]
	if a.Shard != 0 || string(a.Key) != "a" || string(a.Value) != "a2" || a.ExpireAt != 77 {
		t.Fatalf("record 0 = %+v, want shard 0 key a value a2 expire 77", a)
	}
	if a.Flags&RecFlagInline == 0 {
		t.Fatalf("record a lost its inline flag: %#x", a.Flags)
	}
	x := got[1]
	if x.Shard != 1 || string(x.Key) != "x" || string(x.Value) != "x1" {
		t.Fatalf("record 1 = %+v, want shard 1 key x value x1", x)
	}
}

// TestWriteDumpJSONLRoundTrips dumps to JSONL and decodes each line back, proving the
// export is well-formed and carries the fields a load would read: the key and value as
// bytes, the flags, and a separated record's pointer word.
func TestWriteDumpJSONLRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jsonl.aki")
	f, err := Create(path, CreateOptions{ShardCount: 1, SepThreshold: 64, Sync: SyncNo})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = f.Close() }()

	layRecords(t, f, 0, 1, []RecordRow{inlineRow("hello", "world"), sepRow("big", 0xABCD, 4096)})

	var buf bytes.Buffer
	if err := WriteDump(&buf, f); err != nil {
		t.Fatalf("write dump: %v", err)
	}

	dec := json.NewDecoder(&buf)
	var lines []dumpLine
	for dec.More() {
		var l dumpLine
		if err := dec.Decode(&l); err != nil {
			t.Fatalf("decode line: %v", err)
		}
		lines = append(lines, l)
	}
	if len(lines) != 2 {
		t.Fatalf("dumped %d lines, want 2", len(lines))
	}
	// Sorted within the shard: "big" precedes "hello".
	if string(lines[0].Key) != "big" || lines[0].Inline || lines[0].ValueWord != 0xABCD || lines[0].ValueLen != 4096 {
		t.Fatalf("line 0 = %+v, want separated big word 0xABCD len 4096", lines[0])
	}
	if string(lines[1].Key) != "hello" || !lines[1].Inline || string(lines[1].Value) != "world" {
		t.Fatalf("line 1 = %+v, want inline hello world", lines[1])
	}
}
