package store

import (
	"bytes"
	"path/filepath"
	"testing"
)

// TestMemLedgerBands walks a key through every band with SET, APPEND, and
// delete, checking the ledger balances at each step: charges appear when a
// record publishes or grows, and everything cancels when the keys leave.
func TestMemLedgerBands(t *testing.T) {
	s := New(64<<20, 1<<20)
	base := s.Mem()
	if base.Keys != 0 || base.ArenaLiveBytes != 0 || base.ChunkedBytes != 0 {
		t.Fatalf("fresh store ledger not empty: %+v", base)
	}
	if base.IndexBytes == 0 {
		t.Fatal("fresh store reports no index bytes")
	}
	if base.UsedMemory() != base.IndexBytes {
		t.Fatalf("empty UsedMemory = %d, want index bytes %d", base.UsedMemory(), base.IndexBytes)
	}

	// Inline band.
	if err := s.Set([]byte("small"), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	m := s.Mem()
	if m.Keys != 1 || m.ArenaLiveBytes == 0 || m.ChunkedBytes != 0 {
		t.Fatalf("after inline set: %+v", m)
	}
	if m.UsedMemory() <= base.UsedMemory() {
		t.Fatalf("used_memory did not grow: %d -> %d", base.UsedMemory(), m.UsedMemory())
	}

	// Separated band: past the inline cap, the run charges too.
	sep := bytes.Repeat([]byte("s"), 4096)
	if err := s.Set([]byte("sep"), sep); err != nil {
		t.Fatal(err)
	}
	prev := m
	m = s.Mem()
	if m.ArenaLiveBytes < prev.ArenaLiveBytes+uint64(len(sep)) {
		t.Fatalf("separated run not charged: %d -> %d", prev.ArenaLiveBytes, m.ArenaLiveBytes)
	}
	if m.ChunkedBytes != 0 {
		t.Fatalf("chunked bytes on a separated value: %d", m.ChunkedBytes)
	}

	// Chunked band: value length lands in ChunkedBytes exactly.
	giant := bytes.Repeat([]byte("g"), 200_000)
	if err := s.Set([]byte("giant"), giant); err != nil {
		t.Fatal(err)
	}
	m = s.Mem()
	if m.ChunkedBytes != uint64(len(giant)) {
		t.Fatalf("chunked bytes = %d, want %d", m.ChunkedBytes, len(giant))
	}

	// APPEND on the chunked record moves the charge by the delta.
	add := bytes.Repeat([]byte("a"), 50_000)
	if _, err := s.Append([]byte("giant"), add, 0); err != nil {
		t.Fatal(err)
	}
	m = s.Mem()
	if m.ChunkedBytes != uint64(len(giant)+len(add)) {
		t.Fatalf("chunked bytes after append = %d, want %d", m.ChunkedBytes, len(giant)+len(add))
	}

	// APPEND that crosses the chunk threshold rebands and moves the charge in.
	if err := s.Set([]byte("grower"), bytes.Repeat([]byte("x"), 60<<10)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append([]byte("grower"), bytes.Repeat([]byte("y"), 8<<10), 0); err != nil {
		t.Fatal(err)
	}
	m = s.Mem()
	want := uint64(len(giant) + len(add) + 68<<10)
	if m.ChunkedBytes != want {
		t.Fatalf("chunked bytes after reband = %d, want %d", m.ChunkedBytes, want)
	}

	// Overwriting a chunked key with a small value credits the whole charge.
	if err := s.Set([]byte("grower"), []byte("tiny")); err != nil {
		t.Fatal(err)
	}
	m = s.Mem()
	if m.ChunkedBytes != uint64(len(giant)+len(add)) {
		t.Fatalf("chunked bytes after overwrite = %d, want %d", m.ChunkedBytes, len(giant)+len(add))
	}

	// Deleting everything cancels every arena and chunk charge.
	for _, k := range []string{"small", "sep", "giant", "grower"} {
		if !s.Delete([]byte(k)) {
			t.Fatalf("delete %q reported absent", k)
		}
	}
	m = s.Mem()
	if m.Keys != 0 || m.ArenaLiveBytes != 0 || m.ChunkedBytes != 0 {
		t.Fatalf("ledger did not drain: %+v", m)
	}
	if m.UsedMemory() < base.UsedMemory() {
		t.Fatalf("index shrank below its floor: %d -> %d", base.UsedMemory(), m.UsedMemory())
	}
}

// TestMemLedgerVlog pins the disk/memory split: spilled value bytes show up
// in the log figures and never in UsedMemory, and overwrites move live log
// bytes to dead without touching the total.
func TestMemLedgerVlog(t *testing.T) {
	s, err := Open(Options{
		ArenaBytes:       8 << 20,
		SegBytes:         1 << 18,
		VlogPath:         filepath.Join(t.TempDir(), "vlog"),
		ResidentCapBytes: 64 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	val := bytes.Repeat([]byte("v"), 8192)
	for i := byte(0); i < 32; i++ {
		if err := s.Set([]byte{'k', i}, val); err != nil {
			t.Fatal(err)
		}
	}
	m := s.Mem()
	if m.VlogTotalBytes == 0 || m.VlogLiveBytes == 0 {
		t.Fatalf("no spill: %+v", m)
	}
	if m.UsedMemory() >= m.VlogLiveBytes+m.IndexBytes+m.ArenaLiveBytes {
		t.Fatalf("UsedMemory counts log bytes: %+v", m)
	}

	// An overwrite of a logged value strands the old run as dead bytes. The
	// early keys stayed resident under the cap, so churn a late one, which
	// spilled for sure.
	prev := m
	if err := s.Set([]byte{'k', 31}, val); err != nil {
		t.Fatal(err)
	}
	m = s.Mem()
	dead := m.VlogTotalBytes - m.VlogLiveBytes
	prevDead := prev.VlogTotalBytes - prev.VlogLiveBytes
	if dead < prevDead+uint64(len(val)) {
		t.Fatalf("overwrite did not kill the old run: dead %d -> %d", prevDead, dead)
	}

	// A delete kills its run too.
	prev = m
	if !s.Delete([]byte{'k', 30}) {
		t.Fatal("delete reported absent")
	}
	m = s.Mem()
	if m.VlogLiveBytes >= prev.VlogLiveBytes {
		t.Fatalf("delete did not shrink live log bytes: %d -> %d", prev.VlogLiveBytes, m.VlogLiveBytes)
	}
}
