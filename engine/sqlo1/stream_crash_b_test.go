package sqlo1_test

// The T6 fence paging crash row: an XADD-only cadence over the paged
// fence at dialed caps, the WAL cut after every frame. Every command
// boundary is a legal recovery image, and at every prefix the recovered
// streams (full-range IDs and fields, XLEN agreeing) must match one
// boundary exactly: a transition, a tail page rewrite, or a fresh page
// spill must never surface half-done. The trim phase brings the
// deletion rows: whole-run and whole-page drops, a boundary run
// rewrite, and a trim to empty must all land atomically too, because
// each rides one batch and the recovery tail truncation drops a torn
// batch whole. Every snapshot also carries the root accounting line
// (count, entries-added, last-generated-ID, max-deleted-ID), so the
// counters are exact at every cut and the XSETID phase's root
// rewrites are all-or-nothing like everything else. The group phase
// adds the kind 4 rows: every snapshot renders the full group table,
// so a fresh group's flush-before-root discipline, the record-only
// SETID and consumer rewrites, a destroy's move-and-drop compaction,
// and the MKSTREAM empty create all recover whole, and the last
// delivered ID persists at every cut. The PEL phase adds the kind 5
// rows at dialed segment caps: deliveries that cut fresh segments
// flush them before the record that references them, so a torn prefix
// shows the pre-delivery image or the whole delivery and never a
// dangling fence slot, and acks, consumer deletions with pending
// entries, and the drop of an emptied segment land whole the same way
// (X-I5); every snapshot renders the full pending table.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

func TestStreamPagedTornTail(t *testing.T) {
	defer sqlo1.SetStreamFenceCapsForTest(3, 2, 4)()
	defer sqlo1.SetStreamPelCapsForTest(64, 1024, 100)()
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "torn.aki")
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	tr := newTieredOverB(t, db, 8192, 0, 1)
	x, err := sqlo1.NewStream(tr, sqlo1.StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	flush := func() {
		t.Helper()
		if err := tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// ~1800 B values put two entries in a run, so the cadence alternates
	// tail run amendments with run cuts, and the dialed caps (three flat
	// runs, two per page, four index slots) drive every paged rung with
	// two-digit entry counts.
	med := strings.Repeat("m", 1800)
	keys := []string{"s", "s2", "s3"}
	snapshot := func(xx *sqlo1.Stream) map[string][]string {
		t.Helper()
		img := map[string][]string{}
		for _, k := range keys {
			line, ok, err := xx.StreamRootLineForTest(ctx, []byte(k))
			if err != nil {
				t.Fatalf("root line(%q): %v", k, err)
			}
			if !ok {
				continue
			}
			var ents []string
			err = xx.RangeAllForTest(ctx, []byte(k), func(ms, seq uint64, fv [][]byte) {
				parts := make([]string, len(fv))
				for i, b := range fv {
					parts[i] = string(b)
				}
				ents = append(ents, fmt.Sprintf("%d-%d|%s", ms, seq, strings.Join(parts, ",")))
			})
			if err != nil {
				t.Fatalf("Range(%q): %v", k, err)
			}
			n, err := xx.Len(ctx, []byte(k))
			if err != nil {
				t.Fatalf("Len(%q): %v", k, err)
			}
			if int(n) != len(ents) {
				t.Fatalf("XLEN(%q) = %d but %d entries reachable", k, n, len(ents))
			}
			groups, err := xx.GroupLinesForTest(ctx, []byte(k))
			if err != nil {
				t.Fatalf("group lines(%q): %v", k, err)
			}
			pels, err := xx.PelLinesForTest(ctx, []byte(k))
			if err != nil {
				t.Fatalf("pel lines(%q): %v", k, err)
			}
			img[k] = append(append(append(ents, "root|"+line), groups...), pels...)
		}
		return img
	}

	var images []map[string][]string
	images = append(images, map[string][]string{})
	mark := func() {
		t.Helper()
		images = append(images, snapshot(x))
	}
	add := func(key string, ms uint64, val string) {
		t.Helper()
		if err := x.AddExplicitForTest(ctx, []byte(key), ms, 1, [][]byte{[]byte("v"), []byte(val)}); err != nil {
			t.Fatalf("Add(%q, %d): %v", key, ms, err)
		}
		mark()
	}

	// Phase 1: creates and flat tail amendments on both keys.
	add("s", 1, med)
	add("s", 2, med)
	add("s2", 1, "tiny")
	add("s2", 2, "tiny")
	flush()

	// Phase 2: cuts fill the flat fence and the seventh add pages it,
	// with a mid-phase flush so the transition rides its own batch tail.
	for ms := uint64(3); ms <= 6; ms++ {
		add("s", ms, med)
	}
	flush()
	add("s", 7, med)
	if paged, err := x.StreamFencePagedForTest(ctx, []byte("s")); err != nil || !paged {
		t.Fatalf("scenario did not page: %v, %v", paged, err)
	}
	flush()

	// Phase 3: paged growth, amendments riding tail page rewrites,
	// in-place page growth, and fresh page spills, with flushes every
	// third command so neighbors coalesce into one drain batch.
	for ms := uint64(8); ms <= 16; ms++ {
		add("s", ms, med)
		if ms%3 == 0 {
			flush()
		}
	}
	flush()

	// Phase 4: the third-level refusal is side-effect free: no new
	// boundary image, and the visible state still matches the last one.
	fat := strings.Repeat("x", 5000)
	err = x.AddExplicitForTest(ctx, []byte("s"), 17, 1, [][]byte{[]byte("v"), []byte(fat)})
	if !errors.Is(err, sqlo1.ErrStreamFenceThirdLevelForTest) {
		t.Fatalf("third-level err = %v", err)
	}
	if got := snapshot(x); len(got["s"]) != len(images[len(images)-1]["s"]) {
		t.Fatalf("refusal changed the stream: %d entries, want %d", len(got["s"]), len(images[len(images)-1]["s"]))
	}

	// Phase 5: small entries amend the tail run at the wall, and the
	// flat side stream keeps growing.
	add("s", 17, "small")
	add("s", 18, "small")
	add("s2", 3, "tiny")
	flush()

	// Phase 6: the death rows. An approximate trim drops whole runs and
	// empties head pages, an exact MINID trim rewrites the boundary run
	// in its page, and an exact trim to zero leaves s2 alive but empty,
	// each in one batch the tail truncation can only take or drop whole.
	trim := func(key string, byID bool, maxlen int64, minidMs uint64, approx bool) {
		t.Helper()
		if _, err := x.TrimForTest(ctx, []byte(key), byID, maxlen, minidMs, 0, approx, 0); err != nil {
			t.Fatalf("Trim(%q): %v", key, err)
		}
		mark()
	}
	trim("s", false, 10, 0, true)
	flush()
	trim("s", true, 0, 13, false)
	flush()
	trim("s2", false, 0, 0, false)
	add("s2", 4, "tiny")
	flush()

	// Phase 7: XSETID rewrites the root accounting in place, one batch
	// per call, and an append on the moved last ID proves generation
	// resumes above it.
	setid := func(key string, ms uint64, setAdded bool, added uint64, setMaxDel bool, mdMs uint64) {
		t.Helper()
		if err := x.SetIDForTest(ctx, []byte(key), ms, 0, setAdded, added, setMaxDel, mdMs, 0); err != nil {
			t.Fatalf("SetID(%q): %v", key, err)
		}
		mark()
	}
	setid("s", 500, true, 40, true, 12)
	flush()
	setid("s2", 9, false, 0, false, 0)
	add("s2", 10, "tiny")
	flush()

	// Phase 8: group records. A fresh group flushes before the root
	// that references it, the SETID and consumer rewrites are
	// record-only single batches, a destroy moves the tail ordinal and
	// drops the vacated record after the root, and the MKSTREAM create
	// mints a count 0 stream the next add appends onto; the snapshot's
	// group lines hold the last delivered IDs at every cut.
	gcreate := func(key, group string, ms uint64, mkstream bool, read int64) {
		t.Helper()
		if err := x.GroupCreateForTest(ctx, []byte(key), []byte(group), ms, 0, mkstream, read); err != nil {
			t.Fatalf("GroupCreate(%q, %q): %v", key, group, err)
		}
		mark()
	}
	gsetid := func(key, group string, ms uint64, read int64) {
		t.Helper()
		if err := x.GroupSetIDForTest(ctx, []byte(key), []byte(group), ms, 0, read); err != nil {
			t.Fatalf("GroupSetID(%q, %q): %v", key, group, err)
		}
		mark()
	}
	gconsumer := func(key, group, consumer string) {
		t.Helper()
		created, err := x.GroupCreateConsumer(ctx, []byte(key), []byte(group), []byte(consumer), 777)
		if err != nil || !created {
			t.Fatalf("GroupCreateConsumer(%q, %q) = %v, %v", key, consumer, created, err)
		}
		mark()
	}
	gdestroy := func(key, group string, want bool) {
		t.Helper()
		destroyed, err := x.GroupDestroy(ctx, []byte(key), []byte(group))
		if err != nil || destroyed != want {
			t.Fatalf("GroupDestroy(%q, %q) = %v, %v", key, group, destroyed, err)
		}
		if destroyed {
			mark()
		}
	}
	gcreate("s", "alpha", 5, false, -1)
	gcreate("s", "beta", 0, false, 3)
	gconsumer("s", "alpha", "c1")
	gconsumer("s", "alpha", "c2")
	flush()
	gsetid("s", "alpha", 600, -1)
	if pending, err := x.GroupDelConsumer(ctx, []byte("s"), []byte("alpha"), []byte("c1")); err != nil || pending != 0 {
		t.Fatalf("GroupDelConsumer = %d, %v", pending, err)
	}
	mark()
	gcreate("s2", "solo", 1, false, -1)
	flush()
	gcreate("s3", "boot", 0, true, -1)
	add("s3", 1, "tiny")
	gcreate("s", "gamma", 7, false, -1)
	gdestroy("s", "alpha", true)
	flush()
	gdestroy("s2", "solo", true)
	gdestroy("s2", "solo", false)
	flush()

	// Phase 9: PEL segments. At the 64-byte cap a four-entry delivery
	// cuts two segments, so the flush-before-record discipline is live:
	// a cut inside the delivery recovers to the pre-delivery image with
	// at most orphaned segments no fence references. The acks rewrite a
	// segment and drop an emptied one in the record's batch, the
	// consumer deletion sweeps its pending entries the same way, and
	// the final delivery on the paged stream leaves a live PEL in the
	// last image.
	deliver := func(key, group, cons string, count int64, now int64, want int) {
		t.Helper()
		n, err := x.ReadGroupNewForTest(ctx, []byte(key), []byte(group), []byte(cons), count, false, now)
		if err != nil || n != want {
			t.Fatalf("ReadGroupNew(%q, %q, %q) = %d, %v, want %d", key, group, cons, n, err, want)
		}
		mark()
	}
	ack := func(key, group string, want int64, ids ...[2]uint64) {
		t.Helper()
		n, err := x.AckForTest(ctx, []byte(key), []byte(group), ids)
		if err != nil || n != want {
			t.Fatalf("Ack(%q, %q) = %d, %v, want %d", key, group, n, err, want)
		}
		mark()
	}
	for ms := uint64(2); ms <= 10; ms++ {
		add("s3", ms, "tiny")
	}
	flush()
	deliver("s3", "boot", "w1", 4, 888, 4)
	flush()
	deliver("s3", "boot", "w2", 3, 890, 3)
	ack("s3", "boot", 2, [2]uint64{2, 1}, [2]uint64{4, 1})
	flush()
	deliver("s3", "boot", "w1", -1, 892, 3)
	flush()
	if pending, err := x.GroupDelConsumer(ctx, []byte("s3"), []byte("boot"), []byte("w2")); err != nil || pending != 3 {
		t.Fatalf("GroupDelConsumer(w2) = %d, %v", pending, err)
	}
	mark()
	flush()
	ack("s3", "boot", 5, [2]uint64{1, 1}, [2]uint64{3, 1}, [2]uint64{8, 1}, [2]uint64{9, 1}, [2]uint64{10, 1})
	flush()
	deliver("s", "beta", "bc", 2, 900, 2)
	flush()

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
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
	if len(cap.frames) < 50 {
		t.Fatalf("scenario emitted only %d frames, too thin for a matrix", len(cap.frames))
	}

	sameImage := func(a, b map[string][]string) bool {
		if len(a) != len(b) {
			return false
		}
		for k, es := range a {
			bs, ok := b[k]
			if !ok || len(bs) != len(es) {
				return false
			}
			for i := range es {
				if es[i] != bs[i] {
					return false
				}
			}
		}
		return true
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
		x2, err := sqlo1.NewStream(tr2, sqlo1.StreamConfig{})
		if err != nil {
			t.Fatal(err)
		}

		visible := snapshot(x2)
		found := false
		for _, img := range images {
			if sameImage(visible, img) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("cut %d: streams match no command boundary (a torn paged append)", n)
		}
		if n == len(cap.frames) && !sameImage(visible, images[len(images)-1]) {
			t.Fatalf("full tail: streams do not match the final image")
		}
		if err := db2.Close(); err != nil {
			t.Fatalf("cut %d: close: %v", n, err)
		}
	}
}
