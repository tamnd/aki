package sqlo1_test

// The sampling reaper end to end over the real Track B store: a reap
// pass finds cold expired records, ReapStep tombstones them through
// the drain cycle, a reaped rope retires its plane with the genbump
// riding the same batch, and a live hot rewrite shields its stale
// cold record from the pass entirely.

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

func TestReapStepOverB(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reap.aki")
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tr := newTieredOverB(t, db, 256, -1, 31)
	s, err := sqlo1.NewStr(tr, sqlo1.StrConfig{RopeMin: 64, Log2Chunk: 6})
	if err != nil {
		t.Fatal(err)
	}

	rope := bytes.Repeat([]byte{'r'}, 300)
	for k, v := range map[string][]byte{
		"p1":   []byte("one"),
		"p2":   []byte("two"),
		"rope": rope,
		"hotv": []byte("v1"),
		"live": []byte("keep"),
	} {
		if err := s.Set(ctx, []byte(k), v); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}

	// The rope root's identity, captured before it dies: the store's
	// generation probe is how the test sees the retirement later.
	val, root, ok, err := tr.Lookup(ctx, []byte("rope"))
	if err != nil || !ok || !root {
		t.Fatalf("rope root lookup: root=%v ok=%v err=%v", root, ok, err)
	}
	rooth, gen, err := s.RootIDForTest(val)
	if err != nil {
		t.Fatal(err)
	}

	// Four keys die in the past; everything drains cold so the only
	// copies the reaper can see are the store's.
	past := time.Now().UnixMilli() - 60_000
	for _, k := range []string{"p1", "p2", "rope", "hotv"} {
		tr.SetExpireForTest([]byte(k), past)
	}
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	tr.EvictAllForTest()
	keysBefore := db.Stats().Keys

	// A hot rewrite of one dead key: its cold record is stale, and the
	// reaper must yield to the live copy instead of deleting it.
	if err := s.Set(ctx, []byte("hotv"), []byte("v2")); err != nil {
		t.Fatal(err)
	}

	reaped := 0
	for i := 0; reaped < 3; i++ {
		if i >= 200 {
			t.Fatalf("reaper stuck at %d of 3 after %d passes", reaped, i)
		}
		n, err := s.ReapStep(ctx)
		if err != nil {
			t.Fatalf("ReapStep: %v", err)
		}
		reaped += n
	}
	st := tr.Stats()
	if st.Reaped != 3 {
		t.Fatalf("stats count %d reaped, want 3", st.Reaped)
	}
	if st.ReapSkips == 0 {
		t.Fatal("the shielded hot rewrite never booked a skip")
	}
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Three index entries gone, one gained: the two plain keys and the
	// rope root tombstoned, and the retirement's genbump minting the
	// plane's generation record. The rope's segments stay until
	// compaction, dead by rootgen.
	if got := db.Stats().Keys; got != keysBefore-2 {
		t.Fatalf("store holds %d keys after the reap, want %d", got, keysBefore-2)
	}
	if live, err := db.RootLive(rooth, gen); err != nil || live {
		t.Fatalf("reaped rope plane still live (err %v): the genbump missed the tombstone's batch", err)
	}

	// The shielded rewrite and the untouched key survive the pass.
	tr.EvictAllForTest()
	if v, ok, err := s.Get(ctx, []byte("hotv")); err != nil || !ok || string(v) != "v2" {
		t.Fatalf("hotv after reap = %q %v %v, want the live rewrite", v, ok, err)
	}
	if _, ok, err := s.Get(ctx, []byte("live")); err != nil || !ok {
		t.Fatalf("no-expiry key lost to the reaper: ok=%v err=%v", ok, err)
	}
}

// TestReapStepNoCapability pins the off-ramp: a store without ReapScan
// makes ReapStep a no-op, which is what keeps the server's reap ticker
// harmless over MemStore.
func TestReapStepNoCapability(t *testing.T) {
	tr := sqlo1.NewTiered(sqlo1.NewMemStore(), sqlo1.TieredConfig{
		Budget: sqlo1.Budget{Entries: 16, Arenas: 1 << 20},
	})
	s, err := sqlo1.NewStr(tr, sqlo1.StrConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.ReapStep(context.Background()); n != 0 || err != nil {
		t.Fatalf("ReapStep over MemStore = %d, %v, want 0, nil", n, err)
	}
}
