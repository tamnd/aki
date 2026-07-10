package store

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

// These tests pin the separated band and its accounting: values past the
// embed cap live in a run outside the record (arena while the resident budget
// allows, value log past it), every string path reads and writes them
// through the run pointer, full replaces re-select the band from scratch, and
// the band census plus the dead-byte ledger stay honest at every transition.

// newArenaSepStore opens a store with no log: separated runs stay in the
// arena.
func newArenaSepStore(t testing.TB) *Store {
	t.Helper()
	return testStore(t, 8)
}

func mustSet(t *testing.T, s *Store, k string, v []byte) {
	t.Helper()
	if err := s.Set([]byte(k), v); err != nil {
		t.Fatalf("Set %s (%d bytes): %v", k, len(v), err)
	}
}

func checkGet(t *testing.T, s *Store, k string, want []byte) {
	t.Helper()
	got, ok := s.Get([]byte(k), nil)
	if !ok || !bytes.Equal(got, want) {
		t.Fatalf("Get %s = (%d bytes, %v), want %d bytes", k, len(got), ok, len(want))
	}
}

// TestSeparatedBandBoundary pins the band edge: strInlineMax bytes embed,
// one more separates, and both read back intact.
func TestSeparatedBandBoundary(t *testing.T) {
	s := newArenaSepStore(t)
	edge := bytes.Repeat([]byte("e"), strInlineMax)
	over := bytes.Repeat([]byte("o"), strInlineMax+1)
	mustSet(t, s, "edge", edge)
	mustSet(t, s, "over", over)
	st := s.Stats()
	if st.Embedded != 1 || st.Separated != 1 {
		t.Fatalf("bands = %+v, want one embedded and one separated", st)
	}
	checkGet(t, s, "edge", edge)
	checkGet(t, s, "over", over)
	if n, ok := s.StrLen([]byte("over"), 0); !ok || n != int64(strInlineMax+1) {
		t.Fatalf("StrLen over = %d,%v", n, ok)
	}
}

// TestSeparatedFullReplaceReselects overwrites a separated key with a small
// value and an int, checking each replace re-runs band selection from scratch
// and the census follows.
func TestSeparatedFullReplaceReselects(t *testing.T) {
	s := newArenaSepStore(t)
	big := bytes.Repeat([]byte("b"), 3000)
	mustSet(t, s, "k", big)
	if st := s.Stats(); st.Separated != 1 {
		t.Fatalf("bands after big set = %+v, want separated 1", st)
	}
	mustSet(t, s, "k", []byte("small"))
	if st := s.Stats(); st.Separated != 0 || st.Embedded != 1 {
		t.Fatalf("bands after shrink = %+v, want embedded 1", st)
	}
	checkGet(t, s, "k", []byte("small"))
	mustSet(t, s, "k", []byte("12345"))
	if st := s.Stats(); st.Int != 1 || st.Embedded != 0 {
		t.Fatalf("bands after int set = %+v, want int 1", st)
	}
	mustSet(t, s, "k", big)
	if st := s.Stats(); st.Separated != 1 || st.Int != 0 {
		t.Fatalf("bands after regrow = %+v, want separated 1", st)
	}
	checkGet(t, s, "k", big)
}

// TestAppendCrossesIntoSeparated grows a value through APPEND from the int
// band through embedded and across the embed cap, checking the value stays
// intact at every step and the census tracks the transitions.
func TestAppendCrossesIntoSeparated(t *testing.T) {
	s := newArenaSepStore(t)
	mustSet(t, s, "k", []byte("7"))
	if st := s.Stats(); st.Int != 1 {
		t.Fatalf("bands = %+v, want int 1", st)
	}
	want := []byte("7")
	chunk := bytes.Repeat([]byte("x"), 200)
	for len(want) <= strInlineMax+400 {
		want = append(want, chunk...)
		n, err := s.Append([]byte("k"), chunk, 0)
		if err != nil {
			t.Fatalf("Append at %d: %v", len(want), err)
		}
		if n != int64(len(want)) {
			t.Fatalf("Append length = %d, want %d", n, len(want))
		}
		checkGet(t, s, "k", want)
	}
	st := s.Stats()
	if st.Separated != 1 || st.Int != 0 || st.Embedded != 0 {
		t.Fatalf("bands after growth = %+v, want separated 1", st)
	}
	// Keep appending inside the separated band: growth is a run swap, the
	// record itself never republishes.
	for i := 0; i < 10; i++ {
		want = append(want, chunk...)
		if _, err := s.Append([]byte("k"), chunk, 0); err != nil {
			t.Fatalf("separated Append: %v", err)
		}
	}
	checkGet(t, s, "k", want)
	if n := s.Len(); n != 1 {
		t.Fatalf("Len = %d, want 1", n)
	}
}

// TestSetRangeCrossesIntoSeparated writes past the embed cap with SETRANGE,
// including a zero-filled gap, and checks the patched value both when the key
// exists and when SETRANGE creates it.
func TestSetRangeCrossesIntoSeparated(t *testing.T) {
	s := newArenaSepStore(t)
	mustSet(t, s, "k", []byte("head"))
	n, err := s.SetRange([]byte("k"), 2000, []byte("tail"), 0)
	if err != nil {
		t.Fatalf("SetRange: %v", err)
	}
	want := make([]byte, 2004)
	copy(want, "head")
	copy(want[2000:], "tail")
	if n != int64(len(want)) {
		t.Fatalf("SetRange length = %d, want %d", n, len(want))
	}
	checkGet(t, s, "k", want)
	if st := s.Stats(); st.Separated != 1 {
		t.Fatalf("bands = %+v, want separated 1", st)
	}
	// Patch inside the separated value in place.
	if _, err := s.SetRange([]byte("k"), 1000, []byte("MID"), 0); err != nil {
		t.Fatalf("SetRange mid: %v", err)
	}
	copy(want[1000:], "MID")
	checkGet(t, s, "k", want)
	// Create-on-miss straight into the separated band, gap zero-filled.
	if _, err := s.SetRange([]byte("fresh"), 1500, []byte("end"), 0); err != nil {
		t.Fatalf("SetRange create: %v", err)
	}
	fresh := make([]byte, 1503)
	copy(fresh[1500:], "end")
	checkGet(t, s, "fresh", fresh)
}

// TestSeparatedNotInt confirms arithmetic on a separated value answers
// ErrNotInt without touching the run.
func TestSeparatedNotInt(t *testing.T) {
	s := newArenaSepStore(t)
	v := bytes.Repeat([]byte("9"), 2000)
	mustSet(t, s, "k", v)
	if _, err := s.IncrBy([]byte("k"), 1, 0); err != ErrNotInt {
		t.Fatalf("IncrBy on separated = %v, want ErrNotInt", err)
	}
	checkGet(t, s, "k", v)
}

// TestSeparatedSpillsToLog opens a store with a value log and a tiny resident
// budget and checks separated values land in the log (LogRuns, LogBytes),
// read back intact, and their overwrites and deletes feed the dead counter.
func TestSeparatedSpillsToLog(t *testing.T) {
	s := newLogStore(t, 1<<20)
	v1 := bytes.Repeat([]byte("a"), 2048)
	v2 := bytes.Repeat([]byte("b"), 4096)
	mustSet(t, s, "k1", v1)
	mustSet(t, s, "k2", v2)
	st := s.Stats()
	if st.Separated != 2 || st.LogRuns != 2 {
		t.Fatalf("stats = %+v, want 2 separated in the log", st)
	}
	if total, dead := s.LogBytes(); total != uint64(len(v1)+len(v2)) || dead != 0 {
		t.Fatalf("LogBytes = %d/%d, want %d/0", total, dead, len(v1)+len(v2))
	}
	checkGet(t, s, "k1", v1)
	checkGet(t, s, "k2", v2)
	// Overwrite: the old run's bytes go dead, the census stays at two.
	v1b := bytes.Repeat([]byte("A"), 3000)
	mustSet(t, s, "k1", v1b)
	if _, dead := s.LogBytes(); dead != uint64(len(v1)) {
		t.Fatalf("dead after overwrite = %d, want %d", dead, len(v1))
	}
	if st := s.Stats(); st.Separated != 2 || st.LogRuns != 2 {
		t.Fatalf("stats after overwrite = %+v, want 2/2", st)
	}
	checkGet(t, s, "k1", v1b)
	// Delete: the run dies and leaves the census.
	if !s.Delete([]byte("k2")) {
		t.Fatal("Delete k2 returned false")
	}
	if _, dead := s.LogBytes(); dead != uint64(len(v1)+len(v2)) {
		t.Fatalf("dead after delete = %d, want %d", dead, len(v1)+len(v2))
	}
	if st := s.Stats(); st.Separated != 1 || st.LogRuns != 1 {
		t.Fatalf("stats after delete = %+v, want 1/1", st)
	}
}

// TestAppendOnLogRun grows a log-resident value: a log run is immutable, so
// the growth writes a fresh run and the old bytes go dead.
func TestAppendOnLogRun(t *testing.T) {
	s := newLogStore(t, 1<<20)
	v := bytes.Repeat([]byte("v"), 2048)
	mustSet(t, s, "k", v)
	add := []byte("-more")
	n, err := s.Append([]byte("k"), add, 0)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	want := append(append([]byte{}, v...), add...)
	if n != int64(len(want)) {
		t.Fatalf("Append length = %d, want %d", n, len(want))
	}
	checkGet(t, s, "k", want)
	if _, dead := s.LogBytes(); dead != uint64(len(v)) {
		t.Fatalf("dead after log-run growth = %d, want %d", dead, len(v))
	}
	if st := s.Stats(); st.Separated != 1 || st.LogRuns != 1 {
		t.Fatalf("stats = %+v, want 1/1", st)
	}
	// SETRANGE over a log run likewise rewrites.
	if _, err := s.SetRange([]byte("k"), 10, []byte("PATCH"), 0); err != nil {
		t.Fatalf("SetRange: %v", err)
	}
	copy(want[10:], "PATCH")
	checkGet(t, s, "k", want)
}

// TestSeparatedExpiryReap sets a deadline on a log-resident value and reads
// past it: the lazy reap must release the run into the dead ledger and drop
// the census entry.
func TestSeparatedExpiryReap(t *testing.T) {
	s := newLogStore(t, 1<<20)
	v := bytes.Repeat([]byte("t"), 2048)
	if err := s.SetString([]byte("k"), v, 1000, 2000, false); err != nil {
		t.Fatalf("SetString: %v", err)
	}
	if _, ok := s.GetString([]byte("k"), 1500, nil); !ok {
		t.Fatal("key absent before its deadline")
	}
	if _, ok := s.GetString([]byte("k"), 2500, nil); ok {
		t.Fatal("key present past its deadline")
	}
	if _, dead := s.LogBytes(); dead != uint64(len(v)) {
		t.Fatalf("dead after reap = %d, want %d", dead, len(v))
	}
	if st := s.Stats(); st.Separated != 0 || st.LogRuns != 0 {
		t.Fatalf("stats after reap = %+v, want empty", st)
	}
}

// TestValueCeilingLifted pins the ceiling with the chunked band wired: a
// value past the old 64KiB field width stores fine (chunked band), and the
// 512MiB proto-max-bulk-len limit is the only refusal left, checked through
// SETRANGE so the test never allocates a value that size.
func TestValueCeilingLifted(t *testing.T) {
	s := newArenaSepStore(t)
	v := bytes.Repeat([]byte("x"), maxVal+1)
	mustSet(t, s, "k", v)
	checkGet(t, s, "k", v)
	if st := s.Stats(); st.Chunked != 1 {
		t.Fatalf("stats = %+v, want one chunked record", st)
	}
	if _, err := s.SetRange([]byte("k"), maxValueLen, []byte("x"), 0); err != ErrTooBig {
		t.Fatalf("SetRange past ceiling = %v, want ErrTooBig", err)
	}
}

// TestResetClearsLog checks Reset rewinds the band census and the log
// alongside the index and arena.
func TestResetClearsLog(t *testing.T) {
	s := newLogStore(t, 1<<20)
	mustSet(t, s, "k", []byte(strings.Repeat("v", 2048)))
	s.Reset()
	if st := s.Stats(); st != (BandStats{}) {
		t.Fatalf("stats after Reset = %+v, want zero", st)
	}
	if total, dead := s.LogBytes(); total != 0 || dead != 0 {
		t.Fatalf("LogBytes after Reset = %d/%d, want 0/0", total, dead)
	}
	if _, ok := s.Get([]byte("k"), nil); ok {
		t.Fatal("key present after Reset")
	}
	// The store still takes and spills writes after the rewind.
	v := bytes.Repeat([]byte("w"), 3000)
	mustSet(t, s, "k2", v)
	checkGet(t, s, "k2", v)
	if st := s.Stats(); st.Separated != 1 || st.LogRuns != 1 {
		t.Fatalf("stats after post-Reset write = %+v, want 1/1", st)
	}
}

// TestSeparatedArenaStaysResident checks that under an open budget the runs
// stay in the arena: the log exists but holds nothing.
func TestSeparatedArenaStaysResident(t *testing.T) {
	s, err := Open(Options{
		ArenaBytes: 8 + 8*int(align8(maxRecordBytes)),
		SegBytes:   int(align8(maxRecordBytes)),
		VlogPath:   filepath.Join(t.TempDir(), "values.log"),
		// No resident cap: nothing spills.
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	v := bytes.Repeat([]byte("r"), 4096)
	mustSet(t, s, "k", v)
	checkGet(t, s, "k", v)
	st := s.Stats()
	if st.Separated != 1 || st.LogRuns != 0 {
		t.Fatalf("stats = %+v, want separated 1 with an empty log", st)
	}
	if total, _ := s.LogBytes(); total != 0 {
		t.Fatalf("log total = %d, want 0", total)
	}
}
