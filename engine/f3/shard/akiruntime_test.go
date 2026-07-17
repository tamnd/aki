package shard

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestAkiRuntimeWiresSharedFile confirms Config.AkiPath stands up the durable
// topology: one shared file and one group-commit writer own the append cursor, and
// every worker's store borrows them. The default scratch path leaves both nil, so a
// memory-only or per-shard-vlog runtime pays nothing for the durable seam.
func TestAkiRuntimeWiresSharedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wire.aki")
	rt, err := Open(Config{Shards: 4, ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiPath: path})
	if err != nil {
		t.Fatalf("open aki runtime: %v", err)
	}
	if rt.aki == nil || rt.gw == nil {
		t.Fatalf("aki runtime left file=%v writer=%v, want both set", rt.aki != nil, rt.gw != nil)
	}
	rt.Stop()

	plain, err := Open(Config{Shards: 2, ArenaBytes: 4 << 20, SegBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open plain runtime: %v", err)
	}
	if plain.aki != nil || plain.gw != nil {
		t.Fatal("plain runtime built a shared file or writer without AkiPath")
	}
	plain.Stop()
}

// TestAkiRuntimeRecoversAcrossRestart is the runtime seam's end-to-end proof: a first
// runtime opens the shared file fresh, every shard commits keys through the one
// writer, and it stops (which joins the writer and closes the file). A second runtime
// opens the same path, recovers each shard's index from the durable log on open, and
// reads every key back at its shard. This closes the durable arc at the runtime level:
// a process restart rebuilds the whole keyspace from the one .aki file with no
// per-shard scratch and no external replay step.
func TestAkiRuntimeRecoversAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.aki")
	const shards, per = 4, 50
	cfg := Config{Shards: shards, ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiPath: path}

	key := func(s, i int) []byte { return []byte(fmt.Sprintf("s%d-k%03d", s, i)) }
	val := func(s, i int) []byte { return []byte(fmt.Sprintf("s%d-v%03d", s, i)) }

	// First run: write per shard directly through each owner store, then stop so only
	// the file's durable bytes carry into the restart.
	rt, err := Open(cfg)
	if err != nil {
		t.Fatalf("open first runtime: %v", err)
	}
	for s := 0; s < shards; s++ {
		st := rt.workers[s].st
		for i := 0; i < per; i++ {
			if err := st.SetString(key(s, i), val(s, i), 0, 0, false); err != nil {
				t.Fatalf("set shard %d key %d: %v", s, i, err)
			}
		}
	}
	rt.Stop()

	// Second run: reopen the same path. Recovery runs during Open, so every key is
	// live before the first command.
	rt2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen runtime: %v", err)
	}
	defer rt2.Stop()
	for s := 0; s < shards; s++ {
		st := rt2.workers[s].st
		for i := 0; i < per; i++ {
			got, ok := st.GetString(key(s, i), 0, nil)
			if !ok {
				t.Fatalf("shard %d key %d absent after restart", s, i)
			}
			if string(got) != string(val(s, i)) {
				t.Fatalf("shard %d key %d = %q, want %q", s, i, got, val(s, i))
			}
		}
		// A key the run never wrote stays absent: recovery invents nothing.
		if _, ok := st.GetString(key(s, per), 0, nil); ok {
			t.Fatalf("shard %d recovered a key that was never written", s)
		}
	}
}

// TestAkiRuntimeRejectsUnreadableFile confirms a path that exists but is not a valid
// .aki fails the open rather than silently re-creating over it, so a damaged or
// wrong-format file is never overwritten.
func TestAkiRuntimeRejectsUnreadableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "garbage.aki")
	if err := os.WriteFile(path, []byte("not an aki file at all, just bytes"), 0o644); err != nil {
		t.Fatalf("seed garbage file: %v", err)
	}
	if _, err := Open(Config{Shards: 2, ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiPath: path}); err == nil {
		t.Fatal("open over a non-.aki file returned no error")
	}
}
