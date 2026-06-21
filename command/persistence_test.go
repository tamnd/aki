package command

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/tamnd/aki/rdb"
)

// TestSaveWritesFile checks SAVE writes a dump.rdb to the configured dir and that
// the file parses back to the data that was set.
func TestSaveWritesFile(t *testing.T) {
	r, c := startData(t)
	dir := t.TempDir()
	if got := sendLine(t, r, c, "CONFIG SET dir "+dir); got != "+OK" {
		t.Fatalf("CONFIG SET dir = %q", got)
	}
	sendLine(t, r, c, "SET s hello")
	sendLine(t, r, c, "RPUSH l a b c")

	if got := sendLine(t, r, c, "SAVE"); got != "+OK" {
		t.Fatalf("SAVE = %q", got)
	}

	blob, err := os.ReadFile(filepath.Join(dir, "dump.rdb"))
	if err != nil {
		t.Fatalf("read dump.rdb: %v", err)
	}
	snap, err := rdb.UnmarshalFile(blob)
	if err != nil {
		t.Fatalf("UnmarshalFile: %v", err)
	}
	entries := snapEntries(snap, 0)
	if len(entries) != 2 {
		t.Fatalf("entries = %d want 2", len(entries))
	}
}

// TestLastsave reports zero before any save and a real timestamp afterwards.
func TestLastsave(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "LASTSAVE"); got != ":0" {
		t.Fatalf("LASTSAVE before save = %q want :0", got)
	}
	dir := t.TempDir()
	sendLine(t, r, c, "CONFIG SET dir "+dir)
	sendLine(t, r, c, "SET k v")
	sendLine(t, r, c, "SAVE")
	got := sendLine(t, r, c, "LASTSAVE")
	if got == ":0" || got[0] != ':' {
		t.Fatalf("LASTSAVE after save = %q", got)
	}
	n, err := strconv.ParseInt(got[1:], 10, 64)
	if err != nil || n <= 0 {
		t.Fatalf("LASTSAVE timestamp = %q", got)
	}
}

// TestBgsaveWritesFile checks BGSAVE eventually writes the file. It polls LASTSAVE
// until the background goroutine reports a completed save.
func TestBgsaveWritesFile(t *testing.T) {
	r, c := startData(t)
	dir := t.TempDir()
	sendLine(t, r, c, "CONFIG SET dir "+dir)
	sendLine(t, r, c, "SET k v")

	if got := sendLine(t, r, c, "BGSAVE"); got != "+Background saving started" {
		t.Fatalf("BGSAVE = %q", got)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := sendLine(t, r, c, "LASTSAVE"); got != ":0" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("BGSAVE did not complete in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, err := os.Stat(filepath.Join(dir, "dump.rdb")); err != nil {
		t.Fatalf("dump.rdb missing after BGSAVE: %v", err)
	}
}

// TestSaveCustomDbfilename honors the dbfilename directive.
func TestSaveCustomDbfilename(t *testing.T) {
	r, c := startData(t)
	dir := t.TempDir()
	sendLine(t, r, c, "CONFIG SET dir "+dir)
	sendLine(t, r, c, "CONFIG SET dbfilename snap.rdb")
	sendLine(t, r, c, "SET k v")
	sendLine(t, r, c, "SAVE")
	if _, err := os.Stat(filepath.Join(dir, "snap.rdb")); err != nil {
		t.Fatalf("snap.rdb missing: %v", err)
	}
}

// TestParseSavePoints covers the save directive parsing including the disable form.
func TestParseSavePoints(t *testing.T) {
	pts := parseSavePoints("900 1 300 10 60 10000")
	if len(pts) != 3 {
		t.Fatalf("points = %d want 3", len(pts))
	}
	if pts[0].seconds != 900 || pts[0].changes != 1 {
		t.Fatalf("point 0 = %+v", pts[0])
	}
	if len(parseSavePoints("")) != 0 {
		t.Fatalf("empty save should yield no points")
	}
}

func snapEntries(snap rdb.Snapshot, index int) []rdb.Entry {
	for _, db := range snap.DBs {
		if db.Index == index {
			return db.Entries
		}
	}
	return nil
}
