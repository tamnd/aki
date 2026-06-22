package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tamnd/aki/rdb"
)

// TestCheckAkiHealthy imports a small dump into a fresh .aki file and runs the
// integrity checker over it, expecting a clean pass.
func TestCheckAkiHealthy(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.rdb")
	target := filepath.Join(dir, "data.aki")

	snap := rdb.Snapshot{DBs: []rdb.DBData{
		{Index: 0, Entries: []rdb.Entry{
			{Key: []byte("a"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("1")}, ExpireMS: -1},
			{Key: []byte("b"), Value: rdb.Value{Kind: rdb.KindString, Str: []byte("2")}, ExpireMS: -1},
		}},
	}}
	blob, err := rdb.MarshalFile(snap)
	if err != nil {
		t.Fatalf("MarshalFile: %v", err)
	}
	if err := os.WriteFile(src, blob, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if err := cmdImport([]string{src, "--target", target}); err != nil {
		t.Fatalf("import: %v", err)
	}

	var buf bytes.Buffer
	code := checkAki(target, false, false, &buf)
	if code != 0 {
		t.Fatalf("checkAki code = %d, want 0\n%s", code, buf.String())
	}
	if !strings.Contains(buf.String(), "PASSED") {
		t.Errorf("output missing PASSED:\n%s", buf.String())
	}
}

// TestCheckAkiMissing reports a critical failure when the file does not exist.
func TestCheckAkiMissing(t *testing.T) {
	dir := t.TempDir()
	var buf bytes.Buffer
	code := checkAki(filepath.Join(dir, "nope.aki"), false, false, &buf)
	if code != 3 {
		t.Fatalf("checkAki code = %d, want 3\n%s", code, buf.String())
	}
}
