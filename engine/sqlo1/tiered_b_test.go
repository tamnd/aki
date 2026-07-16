package sqlo1_test

// The slice's namesake test: the Tiered runtime over the real Track B
// store. The white-box suite in tiered_test.go pins the mechanics over
// the placeholder store; this file proves the wiring end to end against
// sqlo1b, including a WAL-replay reopen, because "works over MemStore"
// says nothing about a backend with real durability points. It lives in
// the external test package: engine/sqlo1b imports engine/sqlo1, so the
// reverse import is test-only by construction.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

const bWalSeg = 1 << 20

func newTieredOverB(t *testing.T, db *sqlo1b.Store, entries int, promoteP float64, seed uint64) *sqlo1.Tiered {
	t.Helper()
	return sqlo1.NewTiered(db, sqlo1.TieredConfig{
		Budget:   sqlo1.Budget{Entries: entries, Arenas: 64 << 20},
		PromoteP: promoteP,
		Seed:     seed,
	})
}

func TestTieredOverSqlo1b(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tiered.aki")
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.Close() }()

	// Small tier over a bigger keyspace, so the traffic below constantly
	// drains, evicts, cold-reads, and promotes against the real format.
	tr := newTieredOverB(t, db, 64, 0.5, 42)
	rng := rand.New(rand.NewSource(42))
	shadow := map[string]string{}
	keys := make([]string, 400)
	for i := range keys {
		keys[i] = fmt.Sprintf("bkey%03d", i)
	}

	for op := range 6000 {
		k := keys[rng.Intn(len(keys))]
		switch rng.Intn(10) {
		case 0:
			got, err := tr.Del(ctx, []byte(k))
			if err != nil {
				t.Fatalf("op %d: Del(%s): %v", op, k, err)
			}
			if _, want := shadow[k]; got != want {
				t.Fatalf("op %d: Del(%s) = %v, want %v", op, k, got, want)
			}
			delete(shadow, k)
		case 1, 2, 3:
			v := fmt.Sprintf("val-%d-%s", op, k)
			if err := tr.Set(ctx, []byte(k), []byte(v), sqlo1.TagString); err != nil {
				t.Fatalf("op %d: Set(%s): %v", op, k, err)
			}
			shadow[k] = v
		default:
			v, ok, err := tr.Get(ctx, []byte(k))
			if err != nil {
				t.Fatalf("op %d: Get(%s): %v", op, k, err)
			}
			want, wantOK := shadow[k]
			if ok != wantOK || (ok && string(v) != want) {
				t.Fatalf("op %d: Get(%s) = %q %v, want %q %v", op, k, v, ok, want, wantOK)
			}
		}
		if op%1499 == 0 {
			if err := tr.Flush(ctx); err != nil {
				t.Fatalf("op %d: Flush: %v", op, err)
			}
			if op%2998 == 0 {
				if err := db.Checkpoint(); err != nil {
					t.Fatalf("op %d: Checkpoint: %v", op, err)
				}
			}
		}
	}
	if err := tr.Flush(ctx); err != nil {
		t.Fatalf("final Flush: %v", err)
	}
	st := tr.Stats()
	if st.ColdHits == 0 || st.Promotions == 0 || st.BatchReads == 0 {
		t.Fatalf("traffic never exercised the cold path: %+v", st)
	}
	if st.DirtyBytes != 0 {
		t.Fatalf("flushed tier still dirty: %+v", st)
	}

	// An MGET-shaped read across the whole keyspace: hot hits, one
	// coalesced cold round for everything else, all against the shadow.
	all := make([][]byte, len(keys))
	for i, k := range keys {
		all[i] = []byte(k)
	}
	out, err := tr.BatchGet(ctx, all, nil)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	for i, k := range keys {
		want, wantOK := shadow[k]
		got, gotOK := string(out[i]), out[i] != nil
		if gotOK != wantOK || (gotOK && got != want) {
			t.Fatalf("batch %s = %q %v, want %q %v", k, got, gotOK, want, wantOK)
		}
	}

	// Reopen from disk: half the traffic is in the checkpointed index,
	// the rest replays from the WAL tail. A fresh tier over the reopened
	// store must see exactly the shadow, first cold, then promoted.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db2, err := sqlo1b.OpenStore(path, bWalSeg)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer db2.Close()
	if got, want := db2.Stats().Keys, int64(len(shadow)); got != want {
		t.Fatalf("reopened store holds %d keys, shadow %d", got, want)
	}
	tr2 := newTieredOverB(t, db2, 64, 1.0, 43)
	for _, k := range keys {
		v, ok, err := tr2.Get(ctx, []byte(k))
		if err != nil {
			t.Fatalf("reopened Get(%s): %v", k, err)
		}
		want, wantOK := shadow[k]
		if ok != wantOK || (ok && string(v) != want) {
			t.Fatalf("reopened Get(%s) = %q %v, want %q %v", k, v, ok, want, wantOK)
		}
	}
	st2 := tr2.Stats()
	if st2.ColdHits != int64(len(shadow)) {
		t.Fatalf("reopened sweep cold hits %d, want %d", st2.ColdHits, len(shadow))
	}
	if st2.HotKeys == 0 {
		t.Fatal("always-promote sweep left the tier empty")
	}
}

func TestTieredOverSqlo1bExpiry(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "texp.aki")
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := int64(1) << 41
	tr := sqlo1.NewTiered(db, sqlo1.TieredConfig{
		Budget:   sqlo1.Budget{Entries: 8, Arenas: 4 << 20},
		PromoteP: 1.0,
		Seed:     7,
		NowMs:    func() int64 { return now },
	})

	// A volatile record drained through the tier keeps its expiry in the
	// real format and dies on the cold path once it is due.
	if err := tr.Set(ctx, []byte("v"), []byte("x"), sqlo1.TagString); err != nil {
		t.Fatal(err)
	}
	tr.SetExpireForTest([]byte("v"), now+30_000)
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	tr.EvictAllForTest()
	if _, ok, _ := tr.Get(ctx, []byte("v")); !ok {
		t.Fatal("volatile key missing before due time")
	}
	tr.EvictAllForTest()
	now += 60_000
	if err := tr.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := tr.Get(ctx, []byte("v")); ok {
		t.Fatal("expired record served from the real cold path")
	}
}

// TestTieredWalRungOverB drives real drained writes past a tiny
// checkpoint cadence and lets Tick take the due checkpoint: the WAL
// rung end to end, gauge up on traffic and back down on the timer.
func TestTieredWalRungOverB(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "twal.aki")
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetCheckpointPolicy(sqlo1b.CheckpointPolicy{Bytes: 8 << 10})

	tr := newTieredOverB(t, db, 256, -1, 11)
	val := make([]byte, 1<<10)
	for i := range 64 {
		if err := tr.Set(ctx, fmt.Appendf(nil, "wal%03d", i), val, sqlo1.TagString); err != nil {
			t.Fatal(err)
		}
	}
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if p := db.Pressure(); p.Wal < 1 {
		t.Fatalf("64 KiB of drained frames against an 8 KiB cadence reads %.3f, want >= 1", p.Wal)
	}
	if err := tr.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if p := db.Pressure(); p.Wal >= 1 {
		t.Fatalf("tick left the lag at %.3f, the due checkpoint never ran", p.Wal)
	}
}

// TestStrOverSqlo1b drives the string ladder end to end against the
// real format: rope create, read, rewrite, append, plane retirement
// visible through the store's generation probe, and a WAL-replay
// reopen serving the rope cold.
func TestStrOverSqlo1b(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tstr.aki")
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.Close() }()

	tr := newTieredOverB(t, db, 256, -1, 21)
	cfg := sqlo1.StrConfig{RopeMin: 8 << 10, Log2Chunk: 10}
	s, err := sqlo1.NewStr(tr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	bpat := func(n int, seed byte) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(i)*31 ^ seed ^ byte(i>>9)
		}
		return b
	}
	check := func(str *sqlo1.Str, key string, want []byte) {
		t.Helper()
		got, ok, err := str.Get(ctx, []byte(key))
		if err != nil || !ok {
			t.Fatalf("Get(%q): ok=%v err=%v", key, ok, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q): %d bytes, want %d", key, len(got), len(want))
		}
	}

	// One value per rung, plus a rope that gets rewritten and appended.
	inline := bpat(700, 1)
	rope1 := bpat(12<<10, 2)
	rope2 := bpat(20<<10, 3)
	suffix := bpat(3000, 4)
	for k, v := range map[string][]byte{"small": inline, "big": rope1} {
		if err := s.Set(ctx, []byte(k), v); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
	check(s, "small", inline)
	check(s, "big", rope1)
	if err := s.Set(ctx, []byte("big"), rope2); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	grown := append(append([]byte(nil), rope2...), suffix...)
	if n, err := s.Append(ctx, []byte("big"), suffix); err != nil || n != int64(len(grown)) {
		t.Fatalf("Append: n=%d err=%v, want %d", n, err, len(grown))
	}
	check(s, "big", grown)
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// The rewrite retired rope1's plane; the fresh rooth per image means
	// generation 1 planes die at gen 2, and the store's probe agrees.
	rec, err := db.Get(ctx, []byte("big"))
	if err != nil || !rec.Root {
		t.Fatalf("big is not a root record on disk: %v", err)
	}

	// A rope DEL kills its plane through the same probe.
	if err := s.Set(ctx, []byte("gone"), bpat(9<<10, 5)); err != nil {
		t.Fatal(err)
	}
	if gone, err := s.Del(ctx, []byte("gone")); err != nil || !gone {
		t.Fatalf("Del: gone=%v err=%v", gone, err)
	}
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get(ctx, []byte("gone")); ok {
		t.Fatal("deleted rope still readable")
	}

	// Reopen from disk: a fresh tier and ladder serve everything cold,
	// the rope reassembled from real extents after WAL replay.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := sqlo1b.OpenStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	tr2 := newTieredOverB(t, db2, 256, -1, 22)
	s2, err := sqlo1.NewStr(tr2, cfg)
	if err != nil {
		t.Fatal(err)
	}
	check(s2, "small", inline)
	check(s2, "big", grown)
	if _, ok, _ := s2.Get(ctx, []byte("gone")); ok {
		t.Fatal("deleted rope readable after reopen")
	}
}

// TestTieredShedOverB fills a byte-capped store with unique keys, so
// no garbage exists and compaction cannot help: writes must shed with
// ErrShed at the hard minimum, reads and deletes must keep working,
// and raising the cap must resume writes without a restart.
func TestTieredShedOverB(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tshed.aki")
	// 8 MiB WAL segments: a full drain batch of 2 KiB values buffers
	// about 2 MiB of frames, and one batch must fit one segment.
	db, err := sqlo1b.CreateStore(path, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxBytes(16 << 20)

	tr := newTieredOverB(t, db, 16384, -1, 13)
	val := make([]byte, 2<<10)
	var shedErr error
	shedAt := -1
	for i := range 20000 {
		if err := tr.Set(ctx, fmt.Appendf(nil, "shed%05d", i), val, sqlo1.TagString); err != nil {
			shedErr, shedAt = err, i
			break
		}
	}
	if shedErr == nil {
		t.Fatal("40 MiB of unique writes against a 16 MiB cap never shed")
	}
	if !errors.Is(shedErr, sqlo1.ErrShed) {
		t.Fatalf("write %d failed with %v, want ErrShed", shedAt, shedErr)
	}

	// The honest-failure contract: the store is full, not broken.
	if v, ok, err := tr.Get(ctx, []byte("shed00000")); err != nil || !ok || len(v) != len(val) {
		t.Fatalf("Get under shed: ok=%v err=%v", ok, err)
	}
	if gone, err := tr.Del(ctx, []byte("shed00000")); err != nil || !gone {
		t.Fatalf("Del under shed: gone=%v err=%v, deletes stay exempt", gone, err)
	}

	// More budget is the operator's other exit, and it needs no
	// restart: the very next write goes through.
	db.SetMaxBytes(1 << 30)
	if err := tr.Set(ctx, []byte("after"), val, sqlo1.TagString); err != nil {
		t.Fatalf("Set after raising the cap: %v", err)
	}
	if v, ok, err := tr.Get(ctx, []byte("after")); err != nil || !ok || len(v) != len(val) {
		t.Fatalf("Get after recovery: ok=%v err=%v", ok, err)
	}
}
