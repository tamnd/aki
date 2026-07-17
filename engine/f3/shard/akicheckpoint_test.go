package shard

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// TestAkiRuntimeCheckpointsOnStop is the PR5 proof: a clean Stop on the shared-.aki
// path writes every shard's index checkpoint and commits them as the file's live root
// in one meta flip. It opens a runtime fresh, writes keys per shard, and stops. Then it
// opens the file directly and recovers it: the committed root must name a checkpoint
// for every shard, each row's live-record count must match what the shard wrote, and
// the tail entry must sit past the settled prefix. This is what turns the next open's
// recovery from a full-log walk into the bounded checkpoint-plus-tail path.
func TestAkiRuntimeCheckpointsOnStop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt.aki")
	const shards, per = 3, 40
	cfg := Config{Shards: shards, ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiPath: path}

	rt, err := Open(cfg)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	for s := 0; s < shards; s++ {
		st := rt.workers[s].st
		for i := 0; i < per; i++ {
			k := []byte(fmt.Sprintf("s%d-k%03d", s, i))
			if err := st.SetString(k, k, 0, 0, false); err != nil {
				t.Fatalf("set shard %d key %d: %v", s, i, err)
			}
		}
	}
	rt.Stop()

	// Inspect the committed root the clean shutdown left behind.
	f, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("reopen file: %v", err)
	}
	defer func() { _ = f.Close() }()
	rec, err := f.Recover()
	if err != nil {
		t.Fatalf("recover file: %v", err)
	}
	if rec.SRT == nil {
		t.Fatal("clean stop left no committed root; recovery would walk the whole log")
	}
	if len(rec.SRT.Rows) != shards {
		t.Fatalf("root names %d shards, want %d", len(rec.SRT.Rows), shards)
	}
	for s, row := range rec.SRT.Rows {
		if row.IndexCkptOff == 0 {
			t.Fatalf("shard %d row names no checkpoint segment", s)
		}
		if row.LiveRecords != per {
			t.Fatalf("shard %d checkpoint counts %d live records, want %d", s, row.LiveRecords, per)
		}
		if row.FirstTailSeg == 0 {
			t.Fatalf("shard %d row names no tail entry point", s)
		}
	}
}

// TestAkiRuntimeRecoversFromCheckpoint proves the clean-shutdown checkpoint is not
// just committed but authoritative: a second runtime opened at the same path recovers
// every shard through the bounded path and reads back exactly what was written, with
// nothing bleeding across shards. It is the end-to-end counterpart of the
// checkpoint-inspection test above.
func TestAkiRuntimeRecoversFromCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt-e2e.aki")
	const shards, per = 3, 40
	cfg := Config{Shards: shards, ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiPath: path}

	val := func(s, i int) []byte { return []byte(fmt.Sprintf("s%d-v%03d", s, i)) }
	key := func(s, i int) []byte { return []byte(fmt.Sprintf("s%d-k%03d", s, i)) }

	rt, err := Open(cfg)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
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
				t.Fatalf("shard %d key %d absent after checkpoint recovery", s, i)
			}
			if string(got) != string(val(s, i)) {
				t.Fatalf("shard %d key %d = %q, want %q", s, i, got, val(s, i))
			}
		}
	}
}
