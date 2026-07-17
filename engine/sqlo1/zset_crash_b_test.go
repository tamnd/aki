package sqlo1_test

// The T4 torn-tail matrices: the real zset dual-write surface over
// Tiered over sqlo1b, killed after every WAL frame prefix, must
// recover to a state where the two families are the same set exactly
// (Z-I4 over Z-I1): every reachable member's score is one it really
// held, the run walk is a bijection onto the reachable members with
// matching sortables, and ZCARD agrees with both. The slice 4 matrix
// walks the flat-fence ladder (inline, upgrade, dual moves,
// increments, removes with run merges, key death, rebirth); the
// slice 5 matrix shrinks the fence caps and feeds fat members so the
// same cuts land across the paged fence's transition, leaf and upper
// splits, page rewrites and the whole death ladder, with the page
// load cross-checks riding along at every cut.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// ztornRig is the write half of a torn-tail matrix: the live zset,
// the history of every score a member ever held, and the live
// shadow. A torn tail may legally lose a suffix of acknowledged
// flushes; what it must never do is let the families disagree.
type ztornRig struct {
	t      *testing.T
	ctx    context.Context
	z      *sqlo1.ZSet
	tr     *sqlo1.Tiered
	key    []byte
	hist   map[string]map[float64]bool
	shadow map[string]float64
}

func (r *ztornRig) zadd(member string, score float64) {
	r.t.Helper()
	if _, _, _, _, err := r.z.ZAdd(r.ctx, r.key, []byte(member), score, sqlo1.ZAddFlags{}); err != nil {
		r.t.Fatalf("ZAdd %s: %v", member, err)
	}
	if r.hist[member] == nil {
		r.hist[member] = map[float64]bool{}
	}
	r.hist[member][score] = true
	r.shadow[member] = score
}

func (r *ztornRig) zincr(member string, incr float64) {
	r.t.Helper()
	out, err := r.z.ZIncrBy(r.ctx, r.key, incr, []byte(member))
	if err != nil {
		r.t.Fatalf("ZIncrBy %s: %v", member, err)
	}
	if r.hist[member] == nil {
		r.hist[member] = map[float64]bool{}
	}
	r.hist[member][out] = true
	r.shadow[member] = out
}

func (r *ztornRig) zrem(member string) {
	r.t.Helper()
	if _, err := r.z.ZRem(r.ctx, r.key, []byte(member)); err != nil {
		r.t.Fatalf("ZRem %s: %v", member, err)
	}
	delete(r.shadow, member)
}

func (r *ztornRig) flush() {
	r.t.Helper()
	if err := r.tr.Flush(r.ctx); err != nil {
		r.t.Fatal(err)
	}
}

// runZSetTornMatrix runs scenario against a fresh store, captures
// every data frame it emitted, then replays every WAL prefix and
// checks both invariants at each cut.
func runZSetTornMatrix(t *testing.T, key string, minFrames int, scenario func(r *ztornRig)) {
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
	rig := &ztornRig{
		t: t, ctx: ctx, z: z, tr: tr, key: []byte(key),
		hist:   map[string]map[float64]bool{},
		shadow: map[string]float64{},
	}
	scenario(rig)

	finalCard := len(rig.shadow)
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
	if len(cap.frames) < minFrames {
		t.Fatalf("scenario emitted only %d frames, too thin for a matrix", len(cap.frames))
	}

	universe := make([]string, 0, len(rig.hist))
	for m := range rig.hist {
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
			sc, ok, err := z2.MemScoreForTest(ctx, rig.key, []byte(m))
			if err != nil {
				t.Fatalf("cut %d: memScore %s: %v", n, m, err)
			}
			if !ok {
				continue
			}
			if !rig.hist[m][sc] {
				t.Fatalf("cut %d: member %s holds %g, a score it never wrote", n, m, sc)
			}
			visible[m] = sc
		}
		card, err := z2.ZCard(ctx, rig.key)
		if err != nil {
			t.Fatalf("cut %d: ZCard: %v", n, err)
		}
		if int(card) != len(visible) {
			t.Fatalf("cut %d: ZCARD %d but %d members reachable (Z-I2 drift)", n, card, len(visible))
		}

		// Score family: on the segmented rung the run walk must be a
		// bijection onto the reachable members with matching
		// sortables (Z-I1); inline keeps its pairs in the root and
		// has no runs to check. In paged mode the walk also loads
		// every fence page through the parent cross-checks, so a torn
		// page linkage fails loudly here.
		enc, encOK, err := z2.Encoding(ctx, rig.key)
		if err != nil {
			t.Fatalf("cut %d: Encoding: %v", n, err)
		}
		if encOK && enc == "skiplist" {
			walked := map[string]bool{}
			werr := z2.RunWalkForTest(ctx, rig.key, func(s uint64, m []byte) {
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

func TestZSetTornTailMatrix(t *testing.T) {
	runZSetTornMatrix(t, "crashboard", 40, func(r *ztornRig) {
		// Phase 1: inline.
		for i := range 5 {
			r.zadd(fmt.Sprintf("m%03d", i), float64(i))
		}
		r.flush()
		// Phase 2: through the 128-count threshold, the dual upgrade,
		// with quantized scores so equal-score chains form.
		for i := 5; i < 140; i++ {
			r.zadd(fmt.Sprintf("m%03d", i), float64(i%11))
		}
		r.flush()
		// Phase 3: dual moves and increments on the segmented rung,
		// each one member entry rewrite plus a run delete plus a run
		// insert under one root frame.
		for _, i := range []int{10, 20, 30, 40} {
			r.zadd(fmt.Sprintf("m%03d", i), float64(i%7)+100)
		}
		for _, i := range []int{15, 25, 35} {
			r.zincr(fmt.Sprintf("m%03d", i), 2.5)
		}
		r.flush()
		// Phase 4: growth to several runs and member segment splits.
		for i := 140; i < 330; i++ {
			r.zadd(fmt.Sprintf("m%03d", i), float64(i%13)*1.5)
		}
		r.flush()
		// Phase 5: removals; emptied runs, lazy merges on both sides.
		for i := 100; i < 160; i++ {
			r.zrem(fmt.Sprintf("m%03d", i))
		}
		r.flush()
		// Phase 6: the no-write door (a same-score add) beside real
		// moves, so batches mix commands with and without frames.
		r.zadd("m005", r.shadow["m005"])
		r.zadd("m006", -3.25)
		r.zincr("m007", -8)
		r.flush()
		// Phase 7: the collection dies member by member, the last
		// removal retiring the plane behind a genbump.
		for m := range r.shadow {
			r.zrem(m)
		}
		r.flush()
		// Phase 8: rebirth under a fresh rooth, inline then upgraded,
		// so stale rootkey mappings from the first life sit in the
		// tail.
		for i := range 5 {
			r.zadd(fmt.Sprintf("r%03d", i), float64(i)+0.5)
		}
		r.flush()
		for i := 5; i < 135; i++ {
			r.zadd(fmt.Sprintf("r%03d", i), float64(i%17))
		}
		r.flush()
	})
}

func TestZSetPagedTornTailMatrix(t *testing.T) {
	// Caps small enough that ~40 fat members walk the whole paged
	// ladder: flat cap 2 forces the transition on the third run, leaf
	// cap 4 and upper cap 2 force leaf and upper splits early, root 6
	// leaves headroom so the scenario never hits the third-level
	// wall.
	restore := sqlo1.SetZFenceCapsForTest(2, 4, 2, 6)
	defer restore()
	fat := func(i int) string {
		return fmt.Sprintf("f%03d:%s", i, strings.Repeat("x", 680))
	}
	runZSetTornMatrix(t, "pagedboard", 80, func(r *ztornRig) {
		ctx := context.Background()
		// Phase 1: inline, small members.
		for i := range 3 {
			r.zadd(fmt.Sprintf("s%02d", i), float64(i)+0.5)
		}
		r.flush()
		// Phase 2: fat members blow the inline budget, upgrade, and
		// cross the flat-to-paged transition and the first leaf
		// splits; quantized scores form equal-score chains that span
		// page boundaries.
		for i := range 16 {
			r.zadd(fat(i), float64(i%5))
		}
		r.flush()
		// Phase 3: more fat growth through upper splits.
		for i := 16; i < 40; i++ {
			r.zadd(fat(i), float64(i%5))
		}
		r.flush()
		if paged, err := r.z.FencePagedForTest(ctx, r.key); err != nil || !paged {
			r.t.Fatalf("scenario never paged the fence (paged %v, err %v)", paged, err)
		}
		// Phase 4: dual moves and increments so page rewrites and run
		// pair frames interleave in one batch.
		for _, i := range []int{2, 7, 12, 22} {
			r.zadd(fat(i), float64(i%3)+50)
		}
		for _, i := range []int{5, 15, 25} {
			r.zincr(fat(i), 1.25)
		}
		r.flush()
		// Phase 5: small members inside one equal-score chain, then
		// removals from it, driving in-leaf merges.
		for i := range 12 {
			r.zadd(fmt.Sprintf("c%02d", i), 2.25)
		}
		r.flush()
		for i := range 8 {
			r.zrem(fmt.Sprintf("c%02d", i))
		}
		r.flush()
		// Phase 6: removals sweep whole score bands, killing runs,
		// leaves and uppers behind them; paged mode is one-way, so
		// the shrunken fence stays on pages.
		for i := range 40 {
			if i%5 >= 2 && i%3 != 0 {
				r.zrem(fat(i))
			}
		}
		r.flush()
		for i := range 40 {
			if _, live := r.shadow[fat(i)]; live && i%2 == 0 {
				r.zrem(fat(i))
			}
		}
		r.flush()
		// Phase 7: the no-write door beside a real move.
		r.zadd("s01", r.shadow["s01"])
		r.zadd("s02", -7.5)
		r.flush()
		// Phase 8: key death, then rebirth back through the
		// transition so stale page records from the first life sit in
		// the tail.
		for m := range r.shadow {
			r.zrem(m)
		}
		r.flush()
		for i := range 12 {
			r.zadd(fat(100+i), float64(i%4)+0.25)
		}
		r.flush()
		if paged, err := r.z.FencePagedForTest(ctx, r.key); err != nil || !paged {
			r.t.Fatalf("rebirth never re-paged the fence (paged %v, err %v)", paged, err)
		}
	})
}
