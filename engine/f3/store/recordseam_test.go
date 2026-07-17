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

// TestDelLogsTombstone confirms a delete cuts a tombstone row carrying the key
// and the tombstone flag, so a replay clears the entry instead of resurrecting the
// value the SET before it logged.
func TestDelLogsTombstone(t *testing.T) {
	s, f := newRecordSeamStore(t)

	key := []byte("gone")
	if err := s.SetString(key, []byte("v"), 0, 0, false); err != nil {
		t.Fatalf("set: %v", err)
	}
	if !s.Del(key, 0) {
		t.Fatal("del reported absent for a live key")
	}
	rows := walkSeam(t, f)
	if len(rows) != 2 {
		t.Fatalf("got %d logged rows, want 2 (a set then a tombstone)", len(rows))
	}
	tomb := rows[1]
	if tomb.Flags&akifile.RecFlagTombstone == 0 {
		t.Fatalf("second row flags = %d, want the tombstone bit", tomb.Flags)
	}
	if string(tomb.Key) != "gone" {
		t.Fatalf("tombstone key = %q, want gone", tomb.Key)
	}
	if tomb.ValueLen != 0 || tomb.ExpireAt != 0 {
		t.Fatalf("tombstone carried value len %d expiry %d, want zero", tomb.ValueLen, tomb.ExpireAt)
	}
	if err := s.TakeRecordLogErr(); err != nil {
		t.Fatalf("durability fault after a clean delete: %v", err)
	}
}

// TestDelAbsentLogsNothing confirms a delete of a missing key cuts no tombstone,
// so the log records only real supersessions.
func TestDelAbsentLogsNothing(t *testing.T) {
	s, f := newRecordSeamStore(t)

	if s.Del([]byte("nope"), 0) {
		t.Fatal("del reported present for a missing key")
	}
	if rows := walkSeam(t, f); len(rows) != 0 {
		t.Fatalf("got %d logged rows for a no-op delete, want 0", len(rows))
	}
}

// TestSetExpireInPlaceLogsRecord confirms changing a slotted record's deadline
// re-logs the record with the new expiry, so an EXPIRE survives a crash rather
// than replaying to the old deadline the original SET carried.
func TestSetExpireInPlaceLogsRecord(t *testing.T) {
	s, f := newRecordSeamStore(t)

	key, val := []byte("ttl"), []byte("v")
	const first = int64(1_700_000_000_000)
	const second = int64(1_800_000_000_000)
	if err := s.SetString(key, val, 0, first, false); err != nil {
		t.Fatalf("set: %v", err)
	}
	ok, err := s.SetExpire(key, val, second, 1)
	if err != nil {
		t.Fatalf("set expire: %v", err)
	}
	if !ok {
		t.Fatal("set expire reported absent for a live key")
	}
	rows := walkSeam(t, f)
	if len(rows) != 2 {
		t.Fatalf("got %d logged rows, want 2 (the set then the re-expire)", len(rows))
	}
	if rows[1].ExpireAt != uint64(second) {
		t.Fatalf("re-expire row expiry = %d, want %d", rows[1].ExpireAt, second)
	}
}

// TestPersistLogsRecord confirms removing a deadline re-logs the record with a
// zero expiry, so a PERSIST survives a crash rather than replaying to the deadline
// the SET carried.
func TestPersistLogsRecord(t *testing.T) {
	s, f := newRecordSeamStore(t)

	key := []byte("keep")
	if err := s.SetString(key, []byte("v"), 0, 1_700_000_000_000, false); err != nil {
		t.Fatalf("set: %v", err)
	}
	if !s.Persist(key, 1) {
		t.Fatal("persist reported no deadline for a key that had one")
	}
	rows := walkSeam(t, f)
	if len(rows) != 2 {
		t.Fatalf("got %d logged rows, want 2 (the set then the persist)", len(rows))
	}
	if rows[1].ExpireAt != 0 {
		t.Fatalf("persist row expiry = %d, want 0", rows[1].ExpireAt)
	}
	if err := s.TakeRecordLogErr(); err != nil {
		t.Fatalf("durability fault after a clean persist: %v", err)
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
