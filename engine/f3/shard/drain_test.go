package shard

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/store"
)

// drainStore opens a store with a small resident cap and a cold region behind a
// roomy arena, the migratorStore shape from the store package: a flood of small
// keys drives the live charge past the cap with nothing for the residency hand
// to move, so the whole-record cold drain is the only valve.
func drainStore(t *testing.T, capBytes uint64) *store.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(store.Options{
		ArenaBytes:       16 << 20,
		SegBytes:         256 << 10,
		VlogPath:         filepath.Join(dir, "vlog"),
		ColdPath:         filepath.Join(dir, "cold"),
		ResidentCapBytes: capBytes,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestDrainColdAsyncMigrates drives the whole owner-cooperative migrator through
// the real shard wiring: phase 1 stages a drain and submits it, the off-owner I/O
// worker pwrites the frames to the cold region, and the completion runs phase 2's
// flips in owner program order. After it, records have moved cold and every key
// still reads its value, from the arena or from a frame. This is the integration
// the store-level tests stub the pwrite for.
func TestDrainColdAsyncMigrates(t *testing.T) {
	const cap = 1 << 20
	s := drainStore(t, cap)
	w := newWorker(0, s)

	const n = 40000
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		v := fmt.Appendf(nil, "v-%d", i)
		if err := s.Set(k, v); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if !s.NeedsColdDrain() {
		t.Fatal("fixture did not cross the cap")
	}

	// A handful of boundaries: each stages one drain (bounded by the pool and the
	// buffer ceiling) and the completions flip the prior ones. Drive the owner's
	// completion step between them, the same advanceIntents the run loop runs.
	for pass := 0; pass < 32 && s.NeedsColdDrain(); pass++ {
		w.drainCold()
		w.advanceIntents()
	}
	// stop joins the I/O goroutine, so every submitted drain has run its pwrite
	// and posted its completion to the owner queue. Draining that queue then flips
	// or drops every staged run and returns its buffer to the pool, deterministic
	// without leaning on the goroutine's timing.
	w.io.stop()
	for i := 0; i < 64 && w.io.pool.out > 0; i++ {
		if w.advanceIntents() == 0 {
			break
		}
	}

	if s.Cold().Records == 0 {
		t.Fatal("no records migrated cold through the async path")
	}
	if w.io.pool.out != 0 {
		t.Fatalf("pool has %d buffers checked out after the drains, want 0", w.io.pool.out)
	}

	// Every key answers, wherever it now lives.
	var dst []byte
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		got, ok := s.GetString(k, 0, dst)
		want := fmt.Sprintf("v-%d", i)
		if !ok || string(got) != want {
			t.Fatalf("key %d = %q,%v, want %q", i, got, ok, want)
		}
	}
}
