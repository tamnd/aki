package sqlo1_test

// The T2 slice 5 torn-tail matrix: the real hash ladder over Tiered
// over sqlo1b, killed after every WAL frame prefix, must recover to a
// state where HLEN equals the number of reachable fields exactly
// (H-I2, rule W3) and every reachable value is one its field really
// held. The scenario walks the whole representation ladder (inline,
// upgrade, count-only windows, splits, shrink, delete and recreate)
// so the matrix crosses structural and elided frames in every order
// the write path can produce them.

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// tornFrame is one captured WAL data frame, payload copied out of the
// replay buffer.
type tornFrame struct {
	shard  uint16
	op     uint8
	oflags uint8
	pay    []byte
}

type tornCapture struct {
	frames []tornFrame
}

func (c *tornCapture) ApplyData(fr sqlo1.WALFrame) error {
	c.frames = append(c.frames, tornFrame{
		shard: fr.Shard, op: fr.Op, oflags: fr.Oflags, pay: bytes.Clone(fr.Payload),
	})
	return nil
}

func TestHashTornTailMatrix(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "torn.aki")
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	tr := newTieredOverB(t, db, 8192, 0, 1)
	h, err := sqlo1.NewHash(tr, sqlo1.HashConfig{})
	if err != nil {
		t.Fatal(err)
	}

	// The write history: every value a field ever held, and the live
	// shadow. A torn tail may legally lose a suffix of acknowledged
	// flushes here (the crash harness owns the durability contract);
	// what it must never do is disagree with itself.
	key := []byte("crashhash")
	hist := map[string]map[string]bool{}
	shadow := map[string]string{}
	hset := func(field, val string) {
		t.Helper()
		if _, err := h.HSet(ctx, key, []byte(field), []byte(val)); err != nil {
			t.Fatalf("HSet %s: %v", field, err)
		}
		if hist[field] == nil {
			hist[field] = map[string]bool{}
		}
		hist[field][val] = true
		shadow[field] = val
	}
	hdel := func(field string) {
		t.Helper()
		if _, err := h.HDel(ctx, key, []byte(field)); err != nil {
			t.Fatalf("HDel %s: %v", field, err)
		}
		delete(shadow, field)
	}
	flush := func() {
		t.Helper()
		if err := tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Phase 1: inline.
	for i := range 5 {
		hset(fmt.Sprintf("f%03d", i), fmt.Sprintf("p1-%d", i))
	}
	flush()
	// Phase 2: through the 128-count threshold, the upgrade.
	for i := 5; i < 140; i++ {
		hset(fmt.Sprintf("f%03d", i), fmt.Sprintf("p2-%d", i))
	}
	flush()
	// Phase 3: a count-only window, updates plus new fields.
	for _, i := range []int{10, 20, 30} {
		hset(fmt.Sprintf("f%03d", i), fmt.Sprintf("p3u-%d", i))
	}
	for i := 140; i < 150; i++ {
		hset(fmt.Sprintf("f%03d", i), fmt.Sprintf("p3-%d", i))
	}
	flush()
	// Phase 4: fat values force segment splits.
	fat := string(bytes.Repeat([]byte("x"), 150))
	for i := 150; i < 230; i++ {
		hset(fmt.Sprintf("f%03d", i), fmt.Sprintf("p4-%d-%s", i, fat))
	}
	flush()
	// Phase 5: another count-only window over the split fences.
	for i := 230; i < 245; i++ {
		hset(fmt.Sprintf("f%03d", i), fmt.Sprintf("p5-%d", i))
	}
	flush()
	// Phase 6: shrink; emptied segments and lazy merges.
	for i := 150; i < 200; i++ {
		hdel(fmt.Sprintf("f%03d", i))
	}
	flush()
	// Phase 7: a pure update, the zero-delta window with no root image
	// in the batch at all (rule W1).
	hset("f000", "p7-final")
	hset("f001", "p7-final")
	flush()
	// Phase 8: the collection dies (root DEL plus genbump).
	if _, err := tr.Del(ctx, key); err != nil {
		t.Fatal(err)
	}
	shadow = map[string]string{}
	flush()
	// Phase 9: rebirth under a fresh rooth, inline then upgraded, so
	// stale rootkey mappings from the first life sit in the tail.
	for i := range 5 {
		hset(fmt.Sprintf("g%03d", i), fmt.Sprintf("p9-%d", i))
	}
	flush()
	for i := 5; i < 135; i++ {
		hset(fmt.Sprintf("g%03d", i), fmt.Sprintf("p9b-%d", i))
	}
	flush()

	final := make(map[string]string, len(shadow))
	maps.Copy(final, shadow)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Capture every data frame the run emitted.
	df, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var cap tornCapture
	rec, err := sqlo1b.Recover(df, sqlo1.WALPath(path), bWalSeg, &cap)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Super.WALTrimSeq != 0 {
		t.Fatalf("scenario checkpointed (trim %d), the matrix needs the whole tail", rec.Super.WALTrimSeq)
	}
	dbid := rec.Super.WALDBID()
	rec.WAL.Close()
	df.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cap.frames) < 40 {
		t.Fatalf("scenario emitted only %d frames, too thin for a matrix", len(cap.frames))
	}

	// The field universe the sweep probes at every cut.
	universe := make([]string, 0, len(hist))
	for f := range hist {
		universe = append(universe, f)
	}

	for n := 0; n <= len(cap.frames); n++ {
		cut := filepath.Join(dir, "cut.aki")
		if err := os.WriteFile(cut, data, 0o644); err != nil {
			t.Fatal(err)
		}
		os.Remove(sqlo1.WALPath(cut))
		w, err := sqlo1.OpenWAL(sqlo1.WALPath(cut), dbid, bWalSeg)
		if err != nil {
			t.Fatal(err)
		}
		for _, fr := range cap.frames[:n] {
			if _, err := w.Append(fr.shard, fr.op, fr.oflags, fr.pay); err != nil {
				t.Fatalf("cut %d: %v", n, err)
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		db2, err := sqlo1b.OpenStore(cut, bWalSeg)
		if err != nil {
			t.Fatalf("cut %d: recovery failed: %v", n, err)
		}
		tr2 := newTieredOverB(t, db2, 8192, 0, 1)
		h2, err := sqlo1.NewHash(tr2, sqlo1.HashConfig{})
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range [][]byte{[]byte("crashhash"), []byte("g-never")} {
			if _, _, err := h2.Encoding(ctx, key); err != nil {
				t.Fatalf("cut %d: Encoding(%s): %v", n, key, err)
			}
		}
		visible := map[string]string{}
		for _, f := range universe {
			v, ok, err := h2.HGet(ctx, key, []byte(f))
			if err != nil {
				t.Fatalf("cut %d: HGet %s: %v", n, f, err)
			}
			if !ok {
				continue
			}
			if !hist[f][string(v)] {
				t.Fatalf("cut %d: field %s holds %q, a value it never wrote", n, f, v)
			}
			visible[f] = string(v)
		}
		hlen, err := h2.HLen(ctx, key)
		if err != nil {
			t.Fatalf("cut %d: HLen: %v", n, err)
		}
		if int(hlen) != len(visible) {
			t.Fatalf("cut %d: HLEN %d but %d fields reachable (W3 count drift)", n, hlen, len(visible))
		}
		if n == len(cap.frames) {
			if len(visible) != len(final) {
				t.Fatalf("full tail: %d fields, want %d", len(visible), len(final))
			}
			for f, v := range final {
				if visible[f] != v {
					t.Fatalf("full tail: %s = %q, want %q", f, visible[f], v)
				}
			}
		}
		if err := db2.Close(); err != nil {
			t.Fatalf("cut %d: close: %v", n, err)
		}
	}
}
