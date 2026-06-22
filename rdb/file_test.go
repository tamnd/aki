package rdb

import (
	"bytes"
	"testing"
)

// TestFileRoundTrip writes a snapshot with two databases, a few value types, an
// expiry, and access metadata, then reads it back and checks the keys land in the
// right database with the right contents.
func TestFileRoundTrip(t *testing.T) {
	snap := Snapshot{
		DBs: []DBData{
			{Index: 0, Entries: []Entry{
				{Key: []byte("s"), Value: Value{Kind: KindString, Str: []byte("hello")}, ExpireMS: -1},
				{Key: []byte("n"), Value: Value{Kind: KindString, Str: []byte("100")}, ExpireMS: 1893456000000},
			}},
			{Index: 3, Entries: []Entry{
				{Key: []byte("l"), Value: Value{Kind: KindList, List: [][]byte{[]byte("a"), []byte("b")}}, ExpireMS: -1},
				{Key: []byte("h"), Value: Value{Kind: KindHash, Hash: []Field{{Field: []byte("f"), Value: []byte("v")}}}, ExpireMS: -1},
			}},
		},
	}

	blob, err := MarshalFile(snap)
	if err != nil {
		t.Fatalf("MarshalFile: %v", err)
	}
	if string(blob[:5]) != "REDIS" {
		t.Fatalf("magic = %q", blob[:5])
	}

	out, err := UnmarshalFile(blob)
	if err != nil {
		t.Fatalf("UnmarshalFile: %v", err)
	}

	db0 := findDB(t, out, 0)
	if len(db0) != 2 {
		t.Fatalf("db0 entries = %d want 2", len(db0))
	}
	if string(entryByKey(db0, "s").Value.Str) != "hello" {
		t.Fatalf("s = %q", entryByKey(db0, "s").Value.Str)
	}
	if got := entryByKey(db0, "n").ExpireMS; got != 1893456000000 {
		t.Fatalf("n expire = %d", got)
	}

	db3 := findDB(t, out, 3)
	if len(db3) != 2 {
		t.Fatalf("db3 entries = %d want 2", len(db3))
	}
	l := entryByKey(db3, "l").Value
	if len(l.List) != 2 || string(l.List[0]) != "a" || string(l.List[1]) != "b" {
		t.Fatalf("l = %q", l.List)
	}
	h := entryByKey(db3, "h").Value
	if len(h.Hash) != 1 || string(h.Hash[0].Field) != "f" || string(h.Hash[0].Value) != "v" {
		t.Fatalf("h = %+v", h.Hash)
	}
}

// TestFileIdleAndFreq checks the IDLE and FREQ access opcodes survive a round trip.
func TestFileIdleAndFreq(t *testing.T) {
	snap := Snapshot{DBs: []DBData{{Index: 0, Entries: []Entry{
		{Key: []byte("a"), Value: Value{Kind: KindString, Str: []byte("x")}, ExpireMS: -1, Idle: 42, HasIdle: true},
		{Key: []byte("b"), Value: Value{Kind: KindString, Str: []byte("y")}, ExpireMS: -1, Freq: 7, HasFreq: true},
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
	a := entryByKey(db0, "a")
	if !a.HasIdle || a.Idle != 42 {
		t.Fatalf("a idle = %d has=%v", a.Idle, a.HasIdle)
	}
	b := entryByKey(db0, "b")
	if !b.HasFreq || b.Freq != 7 {
		t.Fatalf("b freq = %d has=%v", b.Freq, b.HasFreq)
	}
}

// TestFileAux checks custom aux fields round-trip and the default redis-ver field
// is written.
func TestFileAux(t *testing.T) {
	snap := Snapshot{
		Aux: map[string]string{"custom-key": "custom-val"},
		DBs: []DBData{{Index: 0, Entries: []Entry{
			{Key: []byte("k"), Value: Value{Kind: KindString, Str: []byte("v")}, ExpireMS: -1},
		}}},
	}
	blob, err := MarshalFile(snap)
	if err != nil {
		t.Fatalf("MarshalFile: %v", err)
	}
	out, err := UnmarshalFile(blob)
	if err != nil {
		t.Fatalf("UnmarshalFile: %v", err)
	}
	if out.Aux["custom-key"] != "custom-val" {
		t.Fatalf("custom aux = %q", out.Aux["custom-key"])
	}
	if out.Aux["redis-ver"] == "" {
		t.Fatalf("redis-ver aux missing")
	}
}

// TestFileFunctions checks function library sources round-trip through the
// FUNCTION2 opcode and stay in order, alongside the keyspace.
func TestFileFunctions(t *testing.T) {
	snap := Snapshot{
		Functions: []string{
			"#!lua name=liba\nredis.register_function('a', function() end)",
			"#!lua name=libb\nredis.register_function('b', function() end)",
		},
		DBs: []DBData{{Index: 0, Entries: []Entry{
			{Key: []byte("k"), Value: Value{Kind: KindString, Str: []byte("v")}, ExpireMS: -1},
		}}},
	}
	blob, err := MarshalFile(snap)
	if err != nil {
		t.Fatalf("MarshalFile: %v", err)
	}
	out, err := UnmarshalFile(blob)
	if err != nil {
		t.Fatalf("UnmarshalFile: %v", err)
	}
	if len(out.Functions) != 2 {
		t.Fatalf("functions = %v want two", out.Functions)
	}
	if out.Functions[0] != snap.Functions[0] || out.Functions[1] != snap.Functions[1] {
		t.Fatalf("functions out of order or altered: %v", out.Functions)
	}
	if len(findDB(t, out, 0)) != 1 {
		t.Fatalf("db0 should still have its key")
	}
}

// TestFileEmptyDBSkipped checks a database with no keys writes no selector and so
// does not appear on read.
func TestFileEmptyDBSkipped(t *testing.T) {
	snap := Snapshot{DBs: []DBData{
		{Index: 0, Entries: nil},
		{Index: 1, Entries: []Entry{{Key: []byte("k"), Value: Value{Kind: KindString, Str: []byte("v")}, ExpireMS: -1}}},
	}}
	blob, err := MarshalFile(snap)
	if err != nil {
		t.Fatalf("MarshalFile: %v", err)
	}
	out, err := UnmarshalFile(blob)
	if err != nil {
		t.Fatalf("UnmarshalFile: %v", err)
	}
	for _, db := range out.DBs {
		if db.Index == 0 {
			t.Fatalf("empty db0 should not appear")
		}
	}
	if len(findDB(t, out, 1)) != 1 {
		t.Fatalf("db1 should have one key")
	}
}

// TestFileBadMagic checks a header without the REDIS magic is refused.
func TestFileBadMagic(t *testing.T) {
	blob := append([]byte("XXXXX0012"), 0xFF)
	if _, err := UnmarshalFile(blob); err == nil {
		t.Fatal("bad magic accepted")
	}
}

// TestFileBadVersion checks a header version past what aki writes is refused.
func TestFileBadVersion(t *testing.T) {
	snap := Snapshot{DBs: []DBData{{Index: 0, Entries: []Entry{
		{Key: []byte("k"), Value: Value{Kind: KindString, Str: []byte("v")}, ExpireMS: -1},
	}}}}
	blob, _ := MarshalFile(snap)
	copy(blob[5:9], []byte("9999"))
	if _, err := UnmarshalFile(blob); err == nil {
		t.Fatal("future version accepted")
	}
}

// TestFileCRCTamper checks that flipping a payload byte fails the trailing CRC.
func TestFileCRCTamper(t *testing.T) {
	snap := Snapshot{DBs: []DBData{{Index: 0, Entries: []Entry{
		{Key: []byte("k"), Value: Value{Kind: KindString, Str: []byte("value")}, ExpireMS: -1},
	}}}}
	blob, _ := MarshalFile(snap)
	// Flip a byte inside the key record, well clear of the header and checksum.
	blob[20] ^= 0xFF
	if _, err := UnmarshalFile(blob); err == nil {
		t.Fatal("tampered file accepted")
	}
}

// TestFileTruncated checks a file cut short of its checksum is refused.
func TestFileTruncated(t *testing.T) {
	snap := Snapshot{DBs: []DBData{{Index: 0, Entries: []Entry{
		{Key: []byte("k"), Value: Value{Kind: KindString, Str: []byte("v")}, ExpireMS: -1},
	}}}}
	blob, _ := MarshalFile(snap)
	if _, err := UnmarshalFile(blob[:len(blob)-4]); err == nil {
		t.Fatal("truncated file accepted")
	}
}

func findDB(t *testing.T, snap Snapshot, index int) []Entry {
	t.Helper()
	for _, db := range snap.DBs {
		if db.Index == index {
			return db.Entries
		}
	}
	t.Fatalf("db %d not found", index)
	return nil
}

func entryByKey(entries []Entry, key string) Entry {
	for _, e := range entries {
		if bytes.Equal(e.Key, []byte(key)) {
			return e
		}
	}
	return Entry{}
}
