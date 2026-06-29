package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/rdb"
)

// TestImportDumpRoundTrip writes a dump.rdb, imports it into a fresh .aki file,
// dumps that back out, and checks the keys survive the round trip through the CLI.
func TestImportDumpRoundTrip(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.rdb")
	target := filepath.Join(dir, "out.aki")
	out := filepath.Join(dir, "back.rdb")

	snap := rdb.Snapshot{DBs: []rdb.DBData{
		{Index: 0, Entries: []rdb.Entry{
			{Key: []byte("s"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("hello")}, ExpireMS: -1},
			{Key: []byte("l"), Value: rdb.Value{Kind: rdb.KindList, List: [][]byte{[]byte("a"), []byte("b")}}, ExpireMS: -1},
		}},
		{Index: 2, Entries: []rdb.Entry{
			{Key: []byte("h"), Value: rdb.Value{Kind: rdb.KindHash, Hash: []rdb.Field{{Field: []byte("f"), Value: []byte("v")}}}, ExpireMS: -1},
		}},
	}}
	blob, err := rdb.MarshalFile(snap)
	if err != nil {
		t.Fatalf("MarshalFile: %v", err)
	}
	if err := os.WriteFile(src, blob, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	if err := cmdCheck([]string{"--rdb", src}); err != nil {
		t.Fatalf("check --rdb: %v", err)
	}
	if err := cmdImport([]string{src, "--target", target}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("target not created: %v", err)
	}
	if err := cmdDump([]string{"--file", target, "--output", out}); err != nil {
		t.Fatalf("dump: %v", err)
	}

	back, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	got, err := rdb.UnmarshalFile(back)
	if err != nil {
		t.Fatalf("UnmarshalFile back: %v", err)
	}
	keys := 0
	for _, db := range got.DBs {
		keys += len(db.Entries)
	}
	if keys != 3 {
		t.Fatalf("round-tripped keys = %d want 3", keys)
	}
}

// TestImportRDBIntoFreshKeyspace covers the server --load-rdb path: a fresh .aki
// file loads a dump.rdb and the keys are readable afterward.
func TestImportRDBIntoFreshKeyspace(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.rdb")
	target := filepath.Join(dir, "fresh.aki")

	snap := rdb.Snapshot{DBs: []rdb.DBData{{Index: 0, Entries: []rdb.Entry{
		{Key: []byte("a"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("1")}, ExpireMS: -1},
		{Key: []byte("b"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("2")}, ExpireMS: -1},
	}}}}
	blob, _ := rdb.MarshalFile(snap)
	if err := os.WriteFile(src, blob, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	ks, closeKS, err := openKeyspace(target, 16, 0, 0, nil)
	if err != nil {
		t.Fatalf("openKeyspace: %v", err)
	}
	defer closeKS()

	n, _, err := importRDBInto(ks, src, -1)
	if err != nil {
		t.Fatalf("importRDBInto: %v", err)
	}
	if n != 2 {
		t.Fatalf("loaded %d keys want 2", n)
	}
}

// TestImportDryRun parses without writing the target.
func TestImportDryRun(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.rdb")
	target := filepath.Join(dir, "out.aki")

	snap := rdb.Snapshot{DBs: []rdb.DBData{{Index: 0, Entries: []rdb.Entry{
		{Key: []byte("k"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("v")}, ExpireMS: -1},
	}}}}
	blob, _ := rdb.MarshalFile(snap)
	if err := os.WriteFile(src, blob, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	if err := cmdImport([]string{src, "--target", target, "--dry-run"}); err != nil {
		t.Fatalf("import dry run: %v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("dry run should not create target, stat err = %v", err)
	}
}

// TestImportRejectsNonRDB fails on a file without the REDIS magic.
func TestImportRejectsNonRDB(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "bad.rdb")
	if err := os.WriteFile(src, []byte("not an rdb file"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if err := cmdImport([]string{src, "--target", filepath.Join(dir, "out.aki")}); err == nil {
		t.Fatal("import of non-RDB accepted")
	}
}

// TestImportSingleDB limits the import to one source database.
func TestImportSingleDB(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.rdb")
	target := filepath.Join(dir, "out.aki")
	out := filepath.Join(dir, "back.rdb")

	snap := rdb.Snapshot{DBs: []rdb.DBData{
		{Index: 0, Entries: []rdb.Entry{{Key: []byte("a"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("1")}, ExpireMS: -1}}},
		{Index: 1, Entries: []rdb.Entry{{Key: []byte("b"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("2")}, ExpireMS: -1}}},
	}}
	blob, _ := rdb.MarshalFile(snap)
	if err := os.WriteFile(src, blob, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	if err := cmdImport([]string{src, "--target", target, "--db", "1"}); err != nil {
		t.Fatalf("import --db 1: %v", err)
	}
	if err := cmdDump([]string{"--file", target, "--output", out}); err != nil {
		t.Fatalf("dump: %v", err)
	}
	back, _ := os.ReadFile(out)
	got, err := rdb.UnmarshalFile(back)
	if err != nil {
		t.Fatalf("UnmarshalFile: %v", err)
	}
	keys := 0
	for _, db := range got.DBs {
		keys += len(db.Entries)
	}
	if keys != 1 {
		t.Fatalf("single-db import keys = %d want 1", keys)
	}
}
