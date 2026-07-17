package store

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// newRecordSeamStore opens an .aki-backed store plus the shared file handle, so a
// test can drive SetString through the durable path and then walk the record log
// the commit cut.
func newRecordSeamStore(t *testing.T) (*Store, *akifile.File) {
	t.Helper()
	f, err := akifile.Create(filepath.Join(t.TempDir(), "seam.aki"), akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s, err := Open(Options{
		ArenaBytes:       4 << 20,
		SegBytes:         1 << 20,
		AkiValueLog:      f,
		Shard:            1,
		ResidentCapBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("open aki store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(); _ = f.Close() })
	return s, f
}

// walkSeam collects every record row the log holds, in append order, so a test can
// assert what a string commit left durable.
func walkSeam(t *testing.T, f *akifile.File) []akifile.RecordRow {
	t.Helper()
	var rows []akifile.RecordRow
	err := f.WalkRecords(akifile.PageSize, func(_ uint64, row akifile.RecordRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatalf("walk records: %v", err)
	}
	return rows
}

// TestSetStringLogsRecord confirms a fresh SET on the .aki path cuts one durable
// record row carrying the key, value length, and no expiry, the durable-append
// contract for the plain string commit.
func TestSetStringLogsRecord(t *testing.T) {
	s, f := newRecordSeamStore(t)

	if err := s.SetString([]byte("alpha"), []byte("hello"), 0, 0, false); err != nil {
		t.Fatalf("set: %v", err)
	}
	rows := walkSeam(t, f)
	if len(rows) != 1 {
		t.Fatalf("got %d logged rows, want 1", len(rows))
	}
	if string(rows[0].Key) != "alpha" {
		t.Fatalf("logged key = %q, want alpha", rows[0].Key)
	}
	if rows[0].ValueLen != 5 {
		t.Fatalf("logged value len = %d, want 5", rows[0].ValueLen)
	}
	if rows[0].ExpireAt != 0 {
		t.Fatalf("logged expiry = %d, want 0", rows[0].ExpireAt)
	}
	if rows[0].Flags != 0 {
		t.Fatalf("logged flags = %d, want 0 for a live record", rows[0].Flags)
	}
}

// TestSetStringLogsExpiry confirms an absolute expiry rides the logged row, so a
// replay re-applies the deadline.
func TestSetStringLogsExpiry(t *testing.T) {
	s, f := newRecordSeamStore(t)

	const at = int64(1_700_000_000_000)
	if err := s.SetString([]byte("k"), []byte("v"), 0, at, false); err != nil {
		t.Fatalf("set: %v", err)
	}
	rows := walkSeam(t, f)
	if len(rows) != 1 {
		t.Fatalf("got %d logged rows, want 1", len(rows))
	}
	if rows[0].ExpireAt != uint64(at) {
		t.Fatalf("logged expiry = %d, want %d", rows[0].ExpireAt, at)
	}
}

// TestSetStringOverwriteLogsEachCommit confirms every SET of a key logs a row, the
// in-place overwrite included, so the durable log carries the record's whole
// history and a replay lands on the latest value.
func TestSetStringOverwriteLogsEachCommit(t *testing.T) {
	s, f := newRecordSeamStore(t)

	key := []byte("dup")
	if err := s.SetString(key, []byte("one"), 0, 0, false); err != nil {
		t.Fatalf("set one: %v", err)
	}
	if err := s.SetString(key, []byte("two"), 0, 0, false); err != nil {
		t.Fatalf("set two: %v", err)
	}
	rows := walkSeam(t, f)
	if len(rows) != 2 {
		t.Fatalf("got %d logged rows, want 2 (one per commit)", len(rows))
	}
	for i, r := range rows {
		if string(r.Key) != "dup" {
			t.Fatalf("row %d key = %q, want dup", i, r.Key)
		}
	}
	if rows[1].ValueLen != 3 {
		t.Fatalf("last row value len = %d, want 3", rows[1].ValueLen)
	}
}

// TestSetStringSeparatedLogsDurableWord confirms a separated value's logged row
// carries the log-resident run word (inLogBit set), the durable locator a replay
// can deref, not a volatile arena offset.
func TestSetStringSeparatedLogsDurableWord(t *testing.T) {
	f, err := akifile.Create(filepath.Join(t.TempDir(), "sep.aki"), akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	// A tiny resident cap forces the separated run past residency into the shared
	// value region, so its logged word is log-resident, not an arena offset.
	s, err := Open(Options{
		ArenaBytes:       4 << 20,
		SegBytes:         1 << 20,
		AkiValueLog:      f,
		Shard:            1,
		ResidentCapBytes: 64,
	})
	if err != nil {
		t.Fatalf("open aki store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(); _ = f.Close() })

	// Past strInlineMax so the value separates; the run spills to the shared value
	// region, so its word is log-resident.
	big := make([]byte, strInlineMax+64)
	for i := range big {
		big[i] = byte('a' + i%26)
	}
	if err := s.SetString([]byte("wide"), big, 0, 0, false); err != nil {
		t.Fatalf("set: %v", err)
	}
	rows := walkSeam(t, f)
	if len(rows) != 1 {
		t.Fatalf("got %d logged rows, want 1", len(rows))
	}
	if rows[0].ValueWord&inLogBit == 0 {
		t.Fatalf("separated row word %#x is not log-resident", rows[0].ValueWord)
	}
	if rows[0].ValueLen != uint32(len(big)) {
		t.Fatalf("logged value len = %d, want %d", rows[0].ValueLen, len(big))
	}
}

// TestSetStringNoHandleLogsNothing confirms the default volatile store never
// touches a record log, so the durable path adds nothing when it is off.
func TestSetStringNoHandleLogsNothing(t *testing.T) {
	s, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open plain store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if s.akirlog != nil {
		t.Fatal("plain store built a record log")
	}
	if err := s.SetString([]byte("k"), []byte("v"), 0, 0, false); err != nil {
		t.Fatalf("set: %v", err)
	}
}
