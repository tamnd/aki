package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tamnd/aki/rdb"
)

// sampleSnapshot builds a snapshot with one key of every supported type across
// two databases, used by the JSONL round-trip tests.
func sampleSnapshot() rdb.Snapshot {
	return rdb.Snapshot{DBs: []rdb.DBData{
		{Index: 0, Entries: []rdb.Entry{
			{Key: []byte("s"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("hello")}, ExpireMS: -1},
			{Key: []byte("n"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("100")}, ExpireMS: 1893456000000},
			{Key: []byte("l"), Value: rdb.Value{Kind: rdb.KindList, List: [][]byte{[]byte("a"), []byte("b")}}, ExpireMS: -1},
			{Key: []byte("st"), Value: rdb.Value{Kind: rdb.KindSet, Set: [][]byte{[]byte("x"), []byte("y")}}, ExpireMS: -1},
		}},
		{Index: 2, Entries: []rdb.Entry{
			{Key: []byte("h"), Value: rdb.Value{Kind: rdb.KindHash, Hash: []rdb.Field{{Field: []byte("f1"), Value: []byte("v1")}, {Field: []byte("f2"), Value: []byte("v2")}}}, ExpireMS: -1},
			{Key: []byte("z"), Value: rdb.Value{Kind: rdb.KindZSet, ZSet: []rdb.Member{{Member: []byte("m1"), Score: 1.5}, {Member: []byte("m2"), Score: 2}}}, ExpireMS: -1},
		}},
	}}
}

// countKeys totals the entries across all databases of a snapshot.
func countKeys(snap rdb.Snapshot) int {
	n := 0
	for _, db := range snap.DBs {
		n += len(db.Entries)
	}
	return n
}

// findEntry returns the entry for a key in a database index, or nil.
func findEntry(snap rdb.Snapshot, db int, key string) *rdb.Entry {
	for _, d := range snap.DBs {
		if d.Index != db {
			continue
		}
		for i := range d.Entries {
			if string(d.Entries[i].Key) == key {
				return &d.Entries[i]
			}
		}
	}
	return nil
}

// TestJSONLRoundTrip dumps every type to JSONL and parses it back, checking the
// values survive intact.
func TestJSONLRoundTrip(t *testing.T) {
	snap := sampleSnapshot()
	var buf bytes.Buffer
	n, err := dumpJSONL(snap, &buf)
	if err != nil {
		t.Fatalf("dumpJSONL: %v", err)
	}
	if n != 6 {
		t.Fatalf("dumped %d keys want 6", n)
	}

	got, err := importJSONL(buf.Bytes())
	if err != nil {
		t.Fatalf("importJSONL: %v", err)
	}
	if countKeys(got) != 6 {
		t.Fatalf("parsed %d keys want 6", countKeys(got))
	}

	if e := findEntry(got, 0, "n"); e == nil || e.ExpireMS != 1893456000000 {
		t.Fatalf("ttl not preserved: %+v", e)
	}
	l := findEntry(got, 0, "l")
	if l == nil || len(l.Value.List) != 2 || string(l.Value.List[0]) != "a" {
		t.Fatalf("list not preserved: %+v", l)
	}
	h := findEntry(got, 2, "h")
	if h == nil || len(h.Value.Hash) != 2 {
		t.Fatalf("hash not preserved: %+v", h)
	}
	z := findEntry(got, 2, "z")
	if z == nil || len(z.Value.ZSet) != 2 {
		t.Fatalf("zset not preserved: %+v", z)
	}
	// The zset member order is preserved, so the scores line up by position.
	if z.Value.ZSet[0].Score != 1.5 || z.Value.ZSet[1].Score != 2 {
		t.Fatalf("zset scores = %v", z.Value.ZSet)
	}
}

// TestJSONLBinaryValue checks a non-UTF-8 value round-trips through base64.
func TestJSONLBinaryValue(t *testing.T) {
	raw := []byte{0x00, 0xff, 0xfe, 0x01}
	snap := rdb.Snapshot{DBs: []rdb.DBData{{Index: 0, Entries: []rdb.Entry{
		{Key: []byte("bin"), Value: rdb.Value{Kind: rdb.KindString, Str: raw}, ExpireMS: -1},
	}}}}

	var buf bytes.Buffer
	if _, err := dumpJSONL(snap, &buf); err != nil {
		t.Fatalf("dumpJSONL: %v", err)
	}
	if !strings.Contains(buf.String(), "\"binary\":true") {
		t.Fatalf("binary record not flagged: %s", buf.String())
	}

	got, err := importJSONL(buf.Bytes())
	if err != nil {
		t.Fatalf("importJSONL: %v", err)
	}
	e := findEntry(got, 0, "bin")
	if e == nil || !bytes.Equal(e.Value.Str, raw) {
		t.Fatalf("binary value not preserved: %+v", e)
	}
}

// TestJSONLTextValueNotBase64 checks an ordinary value stays human-readable.
func TestJSONLTextValueNotBase64(t *testing.T) {
	snap := rdb.Snapshot{DBs: []rdb.DBData{{Index: 0, Entries: []rdb.Entry{
		{Key: []byte("greeting"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("hello world")}, ExpireMS: -1},
	}}}}
	var buf bytes.Buffer
	if _, err := dumpJSONL(snap, &buf); err != nil {
		t.Fatalf("dumpJSONL: %v", err)
	}
	if !strings.Contains(buf.String(), "hello world") {
		t.Fatalf("text value should be plain, got %s", buf.String())
	}
	if strings.Contains(buf.String(), "\"binary\":true") {
		t.Fatalf("text value should not be flagged binary: %s", buf.String())
	}
}

// TestDumpImportJSONLCLI runs the full CLI path: dump an .aki to JSONL, import it
// into a fresh .aki, and dump that to RDB to confirm the keys survived.
func TestDumpImportJSONLCLI(t *testing.T) {
	dir := t.TempDir()
	rdbSrc := filepath.Join(dir, "in.rdb")
	first := filepath.Join(dir, "first.aki")
	jsonlFile := filepath.Join(dir, "dump.jsonl")
	second := filepath.Join(dir, "second.aki")
	out := filepath.Join(dir, "back.rdb")

	blob, err := rdb.MarshalFile(sampleSnapshot())
	if err != nil {
		t.Fatalf("MarshalFile: %v", err)
	}
	if err := os.WriteFile(rdbSrc, blob, 0o644); err != nil {
		t.Fatalf("write rdb src: %v", err)
	}
	if err := cmdImport([]string{rdbSrc, "--target", first}); err != nil {
		t.Fatalf("import rdb: %v", err)
	}
	if err := cmdDump([]string{"--file", first, "--format", "jsonl", "--output", jsonlFile}); err != nil {
		t.Fatalf("dump jsonl: %v", err)
	}
	// The file should be detected as JSONL with no --format on import.
	if err := cmdImport([]string{jsonlFile, "--target", second}); err != nil {
		t.Fatalf("import jsonl: %v", err)
	}
	if err := cmdDump([]string{"--file", second, "--output", out}); err != nil {
		t.Fatalf("dump back: %v", err)
	}
	back, _ := os.ReadFile(out)
	got, err := rdb.UnmarshalFile(back)
	if err != nil {
		t.Fatalf("UnmarshalFile: %v", err)
	}
	if countKeys(got) != 6 {
		t.Fatalf("round-tripped keys = %d want 6", countKeys(got))
	}
}

// TestImportDetectJSONL checks format detection picks JSONL from a leading brace.
func TestImportDetectJSONL(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "data.jsonl")
	target := filepath.Join(dir, "out.aki")
	line := `{"db":0,"key":"k","type":"string","ttl":-1,"value":"v"}` + "\n"
	if err := os.WriteFile(src, []byte(line), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if err := cmdImport([]string{src, "--target", target}); err != nil {
		t.Fatalf("import detect jsonl: %v", err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("target not created: %v", err)
	}
}
