package sqlo1_test

// DBSIZE's cold half over the real Track B store: the key-entry count
// survives a WAL-replay reopen (rebuilt by applyPut), survives a
// checkpointed reopen (restored from the superblock), and excludes
// plane records outright.

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

func TestKeyEntriesOverB(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "acct.aki")
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}

	tr := sqlo1.NewTiered(db, sqlo1.TieredConfig{
		Budget: sqlo1.Budget{Entries: 256, Arenas: 64 << 20},
		Seed:   11,
	})
	s, err := sqlo1.NewStr(tr, sqlo1.StrConfig{RopeMin: 64, Log2Chunk: 6})
	if err != nil {
		t.Fatal(err)
	}

	// Three plain keys and one rope: four addressable keys, and the
	// rope's segments must not inflate the count.
	for k, v := range map[string][]byte{
		"a":    []byte("1"),
		"b":    []byte("2"),
		"c":    []byte("3"),
		"rope": bytes.Repeat([]byte{'r'}, 300),
	} {
		if err := s.Set(ctx, []byte(k), v); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := db.Stats().KeyEntries; got != 4 {
		t.Fatalf("KeyEntries after drain = %d, want 4", got)
	}
	tr.EvictAllForTest()
	if got := tr.KeyCount(); got != 4 {
		t.Fatalf("KeyCount cold = %d, want 4", got)
	}

	// A delete drains as a tombstone and the count follows it down.
	if _, err := s.Del(ctx, []byte("c")); err != nil {
		t.Fatal(err)
	}
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := db.Stats().KeyEntries; got != 3 {
		t.Fatalf("KeyEntries after delete = %d, want 3", got)
	}

	// Reopen without a checkpoint: the superblock still says zero and
	// WAL replay rebuilds the count through the same apply paths.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := sqlo1b.OpenStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	if got := db2.Stats().KeyEntries; got != 3 {
		t.Fatalf("KeyEntries after replay reopen = %d, want 3", got)
	}

	// Checkpoint, reopen again: this time the count rides the
	// superblock and the replay of covered batches must not double it.
	if err := db2.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	db3, err := sqlo1b.OpenStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	if got := db3.Stats().KeyEntries; got != 3 {
		t.Fatalf("KeyEntries after checkpointed reopen = %d, want 3", got)
	}
}
