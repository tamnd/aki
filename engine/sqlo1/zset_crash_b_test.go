package sqlo1_test

// The T4 slice 4 torn-tail matrix: the real zset dual-write surface
// over Tiered over sqlo1b, killed after every WAL frame prefix, must
// recover to a state where the two families are the same set exactly
// (Z-I4 over Z-I1): every reachable member's score is one it really
// held, the run walk is a bijection onto the reachable members with
// matching sortables, and ZCARD agrees with both. The scenario walks
// the whole ladder (inline, upgrade, dual moves, increments, removes
// with run merges, key death, rebirth) so the matrix crosses the
// rollback discipline's frames in every order the write path emits.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

func TestZSetTornTailMatrix(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "torn.aki")
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	tr := newTieredOverB(t, db, 8192, 0, 1)
	z, err := sqlo1.NewZSet(tr, sqlo1.HashConfig{})
	if err != nil {
		t.Fatal(err)
	}

	// The write history: every score a member ever held, and the live
	// shadow. A torn tail may legally lose a suffix of acknowledged
	// flushes; what it must never do is let the families disagree.
	key := []byte("crashboard")
	hist := map[string]map[float64]bool{}
	shadow := map[string]float64{}
	zadd := func(member string, score float64) {
		t.Helper()
		if _, _, _, _, err := z.ZAdd(ctx, key, []byte(member), score, sqlo1.ZAddFlags{}); err != nil {
			t.Fatalf("ZAdd %s: %v", member, err)
		}
		if hist[member] == nil {
			hist[member] = map[float64]bool{}
		}
		hist[member][score] = true
		shadow[member] = score
	}
	zincr := func(member string, incr float64) {
		t.Helper()
		out, err := z.ZIncrBy(ctx, key, incr, []byte(member))
		if err != nil {
			t.Fatalf("ZIncrBy %s: %v", member, err)
		}
		if hist[member] == nil {
			hist[member] = map[float64]bool{}
		}
		hist[member][out] = true
		shadow[member] = out
	}
	zrem := func(member string) {
		t.Helper()
		if _, err := z.ZRem(ctx, key, []byte(member)); err != nil {
			t.Fatalf("ZRem %s: %v", member, err)
		}
		delete(shadow, member)
	}
	flush := func() {
		t.Helper()
		if err := tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Phase 1: inline.
	for i := range 5 {
		zadd(fmt.Sprintf("m%03d", i), float64(i))
	}
	flush()
	// Phase 2: through the 128-count threshold, the dual upgrade, with
	// quantized scores so equal-score chains form.
	for i := 5; i < 140; i++ {
		zadd(fmt.Sprintf("m%03d", i), float64(i%11))
	}
	flush()
	// Phase 3: dual moves and increments on the segmented rung, each
	// one member entry rewrite plus a run delete plus a run insert
	// under one root frame.
	for _, i := range []int{10, 20, 30, 40} {
		zadd(fmt.Sprintf("m%03d", i), float64(i%7)+100)
	}
	for _, i := range []int{15, 25, 35} {
		zincr(fmt.Sprintf("m%03d", i), 2.5)
	}
	flush()
	// Phase 4: growth to several runs and member segment splits.
	for i := 140; i < 330; i++ {
		zadd(fmt.Sprintf("m%03d", i), float64(i%13)*1.5)
	}
	flush()
	// Phase 5: removals; emptied runs, lazy merges on both sides.
	for i := 100; i < 160; i++ {
		zrem(fmt.Sprintf("m%03d", i))
	}
	flush()
	// Phase 6: the no-write door (a same-score add) beside real moves,
	// so batches mix commands with and without frames.
	zadd("m005", shadow["m005"])
	zadd("m006", -3.25)
	zincr("m007", -8)
	flush()
	// Phase 7: the collection dies member by member, the last removal
	// retiring the plane behind a genbump.
	for m := range shadow {
		zrem(m)
	}
	flush()
	// Phase 8: rebirth under a fresh rooth, inline then upgraded, so
	// stale rootkey mappings from the first life sit in the tail.
	for i := range 5 {
		zadd(fmt.Sprintf("r%03d", i), float64(i)+0.5)
	}
	flush()
	for i := 5; i < 135; i++ {
		zadd(fmt.Sprintf("r%03d", i), float64(i%17))
	}
	flush()

	finalCard := len(shadow)
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

	universe := make([]string, 0, len(hist))
	for m := range hist {
		universe = append(universe, m)
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
		z2, err := sqlo1.NewZSet(tr2, sqlo1.HashConfig{})
		if err != nil {
			t.Fatal(err)
		}

		// Member family: every reachable score is one really written.
		visible := map[string]float64{}
		for _, m := range universe {
			sc, ok, err := z2.MemScoreForTest(ctx, key, []byte(m))
			if err != nil {
				t.Fatalf("cut %d: memScore %s: %v", n, m, err)
			}
			if !ok {
				continue
			}
			if !hist[m][sc] {
				t.Fatalf("cut %d: member %s holds %g, a score it never wrote", n, m, sc)
			}
			visible[m] = sc
		}
		card, err := z2.ZCard(ctx, key)
		if err != nil {
			t.Fatalf("cut %d: ZCard: %v", n, err)
		}
		if int(card) != len(visible) {
			t.Fatalf("cut %d: ZCARD %d but %d members reachable (Z-I2 drift)", n, card, len(visible))
		}

		// Score family: on the segmented rung the run walk must be a
		// bijection onto the reachable members with matching
		// sortables (Z-I1); inline keeps its pairs in the root and
		// has no runs to check.
		enc, encOK, err := z2.Encoding(ctx, key)
		if err != nil {
			t.Fatalf("cut %d: Encoding: %v", n, err)
		}
		if encOK && enc == "skiplist" {
			walked := map[string]bool{}
			werr := z2.RunWalkForTest(ctx, key, func(s uint64, m []byte) {
				sc, held := visible[string(m)]
				if !held {
					t.Fatalf("cut %d: run walk emitted %q, unreachable on the member side", n, m)
				}
				if sqlo1.ZScoreSortableForTest(sc) != s {
					t.Fatalf("cut %d: run walk scores %q at %#x, member side holds %g", n, m, s, sc)
				}
				if walked[string(m)] {
					t.Fatalf("cut %d: run walk emitted %q twice", n, m)
				}
				walked[string(m)] = true
			})
			if werr != nil {
				t.Fatalf("cut %d: run walk: %v", n, werr)
			}
			if len(walked) != len(visible) {
				t.Fatalf("cut %d: run walk holds %d members, member side %d (Z-I1 drift)", n, len(walked), len(visible))
			}
		}
		if n == len(cap.frames) && len(visible) != finalCard {
			t.Fatalf("full tail: %d members, want %d", len(visible), finalCard)
		}
		if err := db2.Close(); err != nil {
			t.Fatalf("cut %d: close: %v", n, err)
		}
	}
}
