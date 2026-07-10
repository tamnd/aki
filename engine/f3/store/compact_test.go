package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These tests cover CompactLog, the value-log reclamation that rewrites the
// live log-resident runs forward into a fresh log and drops the dead bytes a
// same-key overwrite or a delete left behind. What matters is that it
// reclaims every dead byte (tail falls to the live size, dead falls to zero)
// while preserving every live value byte-for-byte, leaving inline records and
// arena runs untouched, that it is a safe no-op with no log, idempotent when
// run twice, and that a mid-rewrite failure aborts with the original log and
// every pointer intact. CompactLog runs quiesced (the owner between
// commands), so these are single-goroutine tests.

// newLogStore opens a store with a value log and a one-byte resident budget,
// so every separated value spills to the log: the shape the compaction suite
// wants, where all run bytes live on disk.
func newLogStore(t testing.TB, arenaBytes int) *Store {
	t.Helper()
	s, err := Open(Options{
		ArenaBytes:       arenaBytes,
		SegBytes:         int(align8(maxRecordBytes)),
		VlogPath:         filepath.Join(t.TempDir(), "values.log"),
		ResidentCapBytes: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestCompactReclaimsDead overwrites one separated key several times, leaving
// all but the last value dead in the log, then compacts. After compaction the
// log holds only the live value: tail equals the last value's length, dead is
// zero, and the key still reads the last value.
func TestCompactReclaimsDead(t *testing.T) {
	s := newLogStore(t, 1<<20)
	sizes := []int{2048, 3072, 1030, 4096, 2560}
	var last []byte
	for _, sz := range sizes {
		last = bytes.Repeat([]byte("v"), sz)
		if err := s.Set([]byte("k"), last); err != nil {
			t.Fatalf("Set %d: %v", sz, err)
		}
	}
	total, dead := s.LogBytes()
	if dead == 0 || dead >= total {
		t.Fatalf("before compact: total=%d dead=%d, want 0 < dead < total", total, dead)
	}
	if err := s.CompactLog(); err != nil {
		t.Fatalf("CompactLog: %v", err)
	}
	total, dead = s.LogBytes()
	if dead != 0 {
		t.Fatalf("dead after compact = %d, want 0", dead)
	}
	if total != uint64(len(last)) {
		t.Fatalf("total after compact = %d, want %d (only the live value)", total, len(last))
	}
	got, ok := s.Get([]byte("k"), nil)
	if !ok || !bytes.Equal(got, last) {
		t.Fatalf("Get after compact = (%d bytes, %v), want the live value intact", len(got), ok)
	}
}

// TestCompactPreservesLiveValues writes several separated keys, overwrites
// some so the log carries dead bytes, then compacts and checks every key
// still reads its latest value and the log tail equals the sum of the live
// values with dead at zero.
func TestCompactPreservesLiveValues(t *testing.T) {
	s := newLogStore(t, 1<<20)
	want := map[string][]byte{}
	// First round: install a value for every key.
	for i := 0; i < 16; i++ {
		k := fmt.Sprintf("k%02d", i)
		v := bytes.Repeat([]byte{byte('a' + i)}, 1100+i*64)
		if err := s.Set([]byte(k), v); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
		want[k] = v
	}
	// Second round: overwrite the even keys, leaving their first value dead.
	for i := 0; i < 16; i += 2 {
		k := fmt.Sprintf("k%02d", i)
		v := bytes.Repeat([]byte{byte('A' + i)}, 2048+i*32)
		if err := s.Set([]byte(k), v); err != nil {
			t.Fatalf("overwrite %s: %v", k, err)
		}
		want[k] = v
	}
	if _, dead := s.LogBytes(); dead == 0 {
		t.Fatal("expected dead bytes from the overwrites before compact")
	}
	if err := s.CompactLog(); err != nil {
		t.Fatalf("CompactLog: %v", err)
	}
	var liveSum uint64
	for k, v := range want {
		got, ok := s.Get([]byte(k), nil)
		if !ok || !bytes.Equal(got, v) {
			t.Fatalf("Get %s after compact = (%d bytes, %v), want %d bytes intact", k, len(got), ok, len(v))
		}
		liveSum += uint64(len(v))
	}
	total, dead := s.LogBytes()
	if dead != 0 {
		t.Fatalf("dead after compact = %d, want 0", dead)
	}
	if total != liveSum {
		t.Fatalf("total after compact = %d, want %d (sum of live values)", total, liveSum)
	}
}

// TestCompactAllDead deletes the only separated key before compacting.
// Nothing is live, so the fresh log is empty: tail and dead both fall to zero
// and the key stays absent.
func TestCompactAllDead(t *testing.T) {
	s := newLogStore(t, 1<<20)
	big := strings.Repeat("z", 4096)
	if err := s.Set([]byte("k"), []byte(big)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !s.Delete([]byte("k")) {
		t.Fatal("Delete returned false")
	}
	if err := s.CompactLog(); err != nil {
		t.Fatalf("CompactLog: %v", err)
	}
	total, dead := s.LogBytes()
	if total != 0 || dead != 0 {
		t.Fatalf("after compacting an all-dead log total=%d dead=%d, want 0/0", total, dead)
	}
	if _, ok := s.Get([]byte("k"), nil); ok {
		t.Fatal("deleted key present after compact")
	}
}

// TestCompactNoLog confirms CompactLog on a pure in-memory store is a safe
// no-op: it returns nil, does not panic, and leaves the store readable.
func TestCompactNoLog(t *testing.T) {
	s := testStore(t, 2)
	if err := s.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s.CompactLog(); err != nil {
		t.Fatalf("CompactLog on log-less store: %v", err)
	}
	if got, ok := s.Get([]byte("k"), nil); !ok || string(got) != "v" {
		t.Fatalf("Get after no-op compact = (%q, %v), want v", got, ok)
	}
}

// TestCompactIdempotent compacts a clean log (no dead bytes) and then
// compacts again. Both leave every live value intact, tail at the live size,
// and dead at zero: a second compaction of an already-live log changes
// nothing observable.
func TestCompactIdempotent(t *testing.T) {
	s := newLogStore(t, 1<<20)
	want := map[string][]byte{}
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("k%d", i)
		v := bytes.Repeat([]byte{byte('a' + i)}, 1100+i*128)
		if err := s.Set([]byte(k), v); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
		want[k] = v
	}
	var liveSum uint64
	for _, v := range want {
		liveSum += uint64(len(v))
	}
	for round := 0; round < 3; round++ {
		if err := s.CompactLog(); err != nil {
			t.Fatalf("CompactLog round %d: %v", round, err)
		}
		total, dead := s.LogBytes()
		if dead != 0 || total != liveSum {
			t.Fatalf("round %d: total=%d dead=%d, want %d/0", round, total, dead, liveSum)
		}
		for k, v := range want {
			got, ok := s.Get([]byte(k), nil)
			if !ok || !bytes.Equal(got, v) {
				t.Fatalf("round %d Get %s = (%d bytes, %v), want intact", round, k, len(got), ok)
			}
		}
	}
}

// TestCompactMixedInlineSeparated stores a mix of small inline values (below
// the embed cap, never in the log) and separated values, overwrites some of
// each, then compacts. The inline values are untouched by compaction and the
// separated ones are reclaimed: the log tail equals only the sum of the live
// separated values.
func TestCompactMixedInlineSeparated(t *testing.T) {
	s := newLogStore(t, 1<<20)
	inline := map[string][]byte{}
	sep := map[string][]byte{}
	for i := 0; i < 10; i++ {
		ik := fmt.Sprintf("i%d", i)
		iv := bytes.Repeat([]byte{byte('a' + i)}, 32) // below the embed cap, stays inline
		if err := s.Set([]byte(ik), iv); err != nil {
			t.Fatalf("Set inline %s: %v", ik, err)
		}
		inline[ik] = iv

		sk := fmt.Sprintf("s%d", i)
		sv := bytes.Repeat([]byte{byte('A' + i)}, 1100+i*64) // past the embed cap, separated
		if err := s.Set([]byte(sk), sv); err != nil {
			t.Fatalf("Set separated %s: %v", sk, err)
		}
		sep[sk] = sv
	}
	// Overwrite the first few of each kind, leaving dead log bytes for the
	// separated ones and nothing in the log for the inline ones.
	for i := 0; i < 4; i++ {
		ik := fmt.Sprintf("i%d", i)
		iv := bytes.Repeat([]byte{byte('z')}, 40)
		if err := s.Set([]byte(ik), iv); err != nil {
			t.Fatalf("overwrite inline %s: %v", ik, err)
		}
		inline[ik] = iv

		sk := fmt.Sprintf("s%d", i)
		sv := bytes.Repeat([]byte{byte('Z')}, 2048+i*64)
		if err := s.Set([]byte(sk), sv); err != nil {
			t.Fatalf("overwrite separated %s: %v", sk, err)
		}
		sep[sk] = sv
	}
	if err := s.CompactLog(); err != nil {
		t.Fatalf("CompactLog: %v", err)
	}
	for ik, iv := range inline {
		got, ok := s.Get([]byte(ik), nil)
		if !ok || !bytes.Equal(got, iv) {
			t.Fatalf("inline Get %s after compact = (%d bytes, %v), want intact", ik, len(got), ok)
		}
	}
	var liveSep uint64
	for sk, sv := range sep {
		got, ok := s.Get([]byte(sk), nil)
		if !ok || !bytes.Equal(got, sv) {
			t.Fatalf("separated Get %s after compact = (%d bytes, %v), want intact", sk, len(got), ok)
		}
		liveSep += uint64(len(sv))
	}
	total, dead := s.LogBytes()
	if dead != 0 {
		t.Fatalf("dead after compact = %d, want 0", dead)
	}
	if total != liveSep {
		t.Fatalf("total after compact = %d, want %d (only live separated values, no inline)", total, liveSep)
	}
}

// TestCompactSurvivesSplitDirectory writes enough separated keys to split the
// index into several segments (so the directory carries real fan-out and
// overflow chains exist inside segments), then compacts. Every key must
// survive: the walk visits each segment exactly once through the directory
// dedupe, home buckets and overflow slabs both.
func TestCompactSurvivesSplitDirectory(t *testing.T) {
	s := newLogStore(t, 1<<22)
	want := map[string][]byte{}
	const n = 2000
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%04d", i)
		v := bytes.Repeat([]byte{byte(i)}, 1100+i%64)
		if err := s.Set([]byte(k), v); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
		want[k] = v
	}
	if s.Splits() == 0 {
		t.Fatal("expected at least one segment split before compact")
	}
	// Overwrite half, seeding dead bytes across the segments.
	for i := 0; i < n; i += 2 {
		k := fmt.Sprintf("key-%04d", i)
		v := bytes.Repeat([]byte{byte(i + 1)}, 1200+i%48)
		if err := s.Set([]byte(k), v); err != nil {
			t.Fatalf("overwrite %s: %v", k, err)
		}
		want[k] = v
	}
	if err := s.CompactLog(); err != nil {
		t.Fatalf("CompactLog: %v", err)
	}
	var liveSum uint64
	for k, v := range want {
		got, ok := s.Get([]byte(k), nil)
		if !ok || !bytes.Equal(got, v) {
			t.Fatalf("Get %s after compact = (%d bytes, %v), want intact", k, len(got), ok)
		}
		liveSum += uint64(len(v))
	}
	total, dead := s.LogBytes()
	if dead != 0 || total != liveSum {
		t.Fatalf("after compact total=%d dead=%d, want %d/0", total, dead, liveSum)
	}
}

// TestCompactAbortSafety corrupts the log under a live store and confirms a
// failed compaction changes nothing: the store stays on its original log,
// every pointer still names it, the values below the corruption point read
// intact, and the half-built ".compact" file is gone. The corruption is a
// truncation that cuts only the last-appended value, so the earlier values
// prove the pointers did not move.
func TestCompactAbortSafety(t *testing.T) {
	s := newLogStore(t, 1<<20)
	want := map[string][]byte{}
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := bytes.Repeat([]byte{byte('a' + i)}, 1500+i*100)
		if err := s.Set([]byte(k), v); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
		want[k] = v
	}
	// One overwrite so the log carries dead bytes and a compaction has work.
	want["k0"] = bytes.Repeat([]byte("Z"), 2000)
	if err := s.Set([]byte("k0"), want["k0"]); err != nil {
		t.Fatalf("overwrite k0: %v", err)
	}
	totalBefore, deadBefore := s.LogBytes()
	// Land the buffered appends, then cut into the last-appended value (k0's
	// overwrite sits at the tail), so its read back fails while every earlier
	// value stays whole on disk. Without the flush the tail bytes would still
	// be served from the pending buffer and the truncation would cut nothing.
	if err := s.vlog.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	logPath := s.vlog.path
	if err := s.vlog.f.Truncate(int64(totalBefore) - 100); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	err := s.CompactLog()
	if err == nil {
		t.Fatal("CompactLog on a truncated log returned nil, want an abort")
	}
	if err != errCompactRead {
		t.Fatalf("CompactLog error = %v, want errCompactRead", err)
	}
	// The store is still on its original log under its original path.
	if s.vlog.path != logPath {
		t.Fatalf("log path after abort = %q, want %q", s.vlog.path, logPath)
	}
	if _, statErr := os.Stat(logPath + ".compact"); !os.IsNotExist(statErr) {
		t.Fatalf("stat .compact after abort = %v, want not-exist", statErr)
	}
	// The accounting did not move: an aborted compaction reclaims nothing.
	if total, dead := s.LogBytes(); total != totalBefore || dead != deadBefore {
		t.Fatalf("LogBytes after abort = %d/%d, want %d/%d unchanged", total, dead, totalBefore, deadBefore)
	}
	// Every value below the cut still reads intact through the original log:
	// the pointers did not move.
	for i := 1; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		got, ok := s.Get([]byte(k), nil)
		if !ok || !bytes.Equal(got, want[k]) {
			t.Fatalf("Get %s after abort = (%d bytes, %v), want intact", k, len(got), ok)
		}
	}
}
