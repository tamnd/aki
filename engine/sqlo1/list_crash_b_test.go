package sqlo1_test

// The T5 slice 7 exit-gate row: the two-root frame group. LMOVE dirties
// two roots in one command, and the WAL cut after every frame must
// never surface a state where the moved element sits in both lists or
// in neither. The check is by image: at every prefix the recovered
// pair of lists (plus the tiny third key the death phase drains) must
// match one command-boundary snapshot exactly, with LLEN agreeing on
// every key (L-I2 at every cut).
//
// The slice 8 matrix below runs the same discipline over the paged
// fence: transition, spills both directions, page splits, page death,
// trim, rem, and moves between paged lists, cut after every frame.

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

func TestListMoveTornTail(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "torn.aki")
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	tr := newTieredOverB(t, db, 8192, 0, 1)
	li, err := sqlo1.NewList(tr, sqlo1.ListConfig{})
	if err != nil {
		t.Fatal(err)
	}
	flush := func() {
		t.Helper()
		if err := tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Fat elements so the source goes noded by bytes and the
	// destination crosses the inline threshold mid-cadence.
	fat := func(i int) string {
		return fmt.Sprintf("e%03d:%s", i, strings.Repeat("x", 296))
	}
	keys := []string{"srcq", "dstq", "tiny"}
	snapshot := func(ll *sqlo1.List) map[string][]string {
		t.Helper()
		img := map[string][]string{}
		for _, k := range keys {
			var elems []string
			err := ll.Range(ctx, []byte(k), 0, -1, func(int) {}, func(e []byte) {
				elems = append(elems, string(e))
			})
			if err != nil {
				t.Fatalf("Range(%q): %v", k, err)
			}
			n, err := ll.Len(ctx, []byte(k))
			if err != nil {
				t.Fatalf("Len(%q): %v", k, err)
			}
			if int(n) != len(elems) {
				t.Fatalf("LLEN(%q) = %d but %d elements reachable", k, n, len(elems))
			}
			if len(elems) > 0 {
				img[k] = elems
			}
		}
		return img
	}

	// Every command boundary is a legal recovery image; a cut landing
	// inside a move must roll it back or complete it whole.
	var images []map[string][]string
	images = append(images, map[string][]string{})
	push := func(key string, elems ...string) {
		t.Helper()
		bs := make([][]byte, len(elems))
		for i, e := range elems {
			bs[i] = []byte(e)
		}
		if _, err := li.Push(ctx, []byte(key), false, false, bs...); err != nil {
			t.Fatalf("Push(%q): %v", key, err)
		}
		images = append(images, snapshot(li))
	}
	move := func(src, dst string, srcLeft, dstLeft, wantOK bool) {
		t.Helper()
		_, ok, err := li.Move(ctx, []byte(src), []byte(dst), srcLeft, dstLeft)
		if err != nil {
			t.Fatalf("Move(%q, %q): %v", src, dst, err)
		}
		if ok != wantOK {
			t.Fatalf("Move(%q, %q) ok = %v, want %v", src, dst, ok, wantOK)
		}
		images = append(images, snapshot(li))
	}

	// Phase 1: a noded source, an inline three-element side list.
	srcElems := make([]string, 140)
	for i := range srcElems {
		srcElems[i] = fat(i)
	}
	push("srcq", srcElems...)
	push("tiny", "t0", "t1", "t2")
	flush()

	// Phase 2: queue-shaped moves, each its own commit point. The
	// seventh push crosses the destination's inline byte threshold, so
	// the cadence lands moves on an inline and a noded destination.
	for range 5 {
		move("srcq", "dstq", false, true, true)
		flush()
	}
	for range 3 {
		move("srcq", "dstq", true, false, true)
		flush()
	}

	// Phase 3: the flush guard's door. The push dirties the source
	// root, so the move must flush first to keep its pair contiguous.
	push("srcq", fat(900))
	move("srcq", "dstq", false, true, true)
	flush()

	// Phase 4: two moves coalescing into one drain batch.
	move("srcq", "dstq", false, true, true)
	move("srcq", "dstq", true, false, true)
	flush()

	// Phase 5: same-key rotation both ways, and the writeless same-end
	// move between them.
	move("dstq", "dstq", true, false, true)
	flush()
	move("dstq", "dstq", false, true, true)
	move("dstq", "dstq", true, true, true)
	flush()

	// Phase 6: the side list drains to death through moves, the last
	// one deleting the key, then a missing-source move no-ops.
	for range 3 {
		move("tiny", "dstq", true, true, true)
		flush()
	}
	move("tiny", "dstq", true, true, false)
	flush()

	// Phase 7: rebirth by moving back out of the destination.
	move("dstq", "tiny", false, false, true)
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
		li2, err := sqlo1.NewList(tr2, sqlo1.ListConfig{})
		if err != nil {
			t.Fatal(err)
		}

		visible := snapshot(li2)
		found := false
		for _, img := range images {
			if sameImage(visible, img) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("cut %d: lists match no command boundary (a torn move)", n)
		}
		if n == len(cap.frames) && !sameImage(visible, images[len(images)-1]) {
			t.Fatalf("full tail: lists do not match the final image")
		}
		if err := db2.Close(); err != nil {
			t.Fatalf("cut %d: close: %v", n, err)
		}
	}
}

func TestListPagedTornTail(t *testing.T) {
	defer sqlo1.SetListFenceCapsForTest(6, 4, 8)()
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "torn.aki")
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		t.Fatal(err)
	}
	tr := newTieredOverB(t, db, 8192, 0, 1)
	li, err := sqlo1.NewList(tr, sqlo1.ListConfig{})
	if err != nil {
		t.Fatal(err)
	}
	flush := func() {
		t.Helper()
		if err := tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// ~700 B elements make a node hold five, so the dialed caps (six
	// flat entries, four per page, eight index slots) drive every rung
	// with two-digit element counts.
	fat := func(i int) string {
		return fmt.Sprintf("p%03d:%s", i, strings.Repeat("y", 695))
	}
	keys := []string{"q", "q2"}
	snapshot := func(ll *sqlo1.List) map[string][]string {
		t.Helper()
		img := map[string][]string{}
		for _, k := range keys {
			var elems []string
			err := ll.Range(ctx, []byte(k), 0, -1, func(int) {}, func(e []byte) {
				elems = append(elems, string(e))
			})
			if err != nil {
				t.Fatalf("Range(%q): %v", k, err)
			}
			n, err := ll.Len(ctx, []byte(k))
			if err != nil {
				t.Fatalf("Len(%q): %v", k, err)
			}
			if int(n) != len(elems) {
				t.Fatalf("LLEN(%q) = %d but %d elements reachable", k, n, len(elems))
			}
			if len(elems) > 0 {
				img[k] = elems
			}
		}
		return img
	}

	var images []map[string][]string
	images = append(images, map[string][]string{})
	mark := func() {
		t.Helper()
		images = append(images, snapshot(li))
	}
	push := func(key string, left bool, elems ...string) {
		t.Helper()
		bs := make([][]byte, len(elems))
		for i, e := range elems {
			bs[i] = []byte(e)
		}
		if _, err := li.Push(ctx, []byte(key), left, false, bs...); err != nil {
			t.Fatalf("Push(%q): %v", key, err)
		}
		mark()
	}
	popn := func(key string, left bool, count int) {
		t.Helper()
		if _, _, err := li.Pop(ctx, []byte(key), left, count); err != nil {
			t.Fatalf("Pop(%q): %v", key, err)
		}
		mark()
	}

	// Phase 1: one batched push carries the upgrade straight past the
	// flat cap, eight nodes into two full pages.
	elems := make([]string, 40)
	for i := range elems {
		elems[i] = fat(i)
	}
	push("q", false, elems...)
	if paged, err := li.ListFencePagedForTest(ctx, []byte("q")); err != nil || !paged {
		t.Fatalf("scenario did not page: %v, %v", paged, err)
	}
	flush()

	// Phase 2: single right pushes amend the edge node in place, cut
	// fresh nodes, and spill a fresh tail page; half the commands share
	// a drain batch with the next.
	for i := 40; i < 46; i++ {
		push("q", false, fat(i))
		if i%2 == 0 {
			flush()
		}
	}
	flush()

	// Phase 3: a batched left push over a full head page spills at the
	// front, the partial-first chunking.
	push("q", true, fat(46), fat(47), fat(48), fat(49), fat(50), fat(51))
	flush()

	// Phase 4: in-place rewrites inside pages, both a node body (LSET)
	// and a pivot insert that splits its node and its page.
	if err := li.Set(ctx, []byte("q"), 20, []byte("SWAP:"+fat(20)[5:])); err != nil {
		t.Fatal(err)
	}
	mark()
	if n, err := li.Insert(ctx, []byte("q"), true, []byte(fat(30)), []byte(fat(70))); err != nil || n < 0 {
		t.Fatalf("Insert = %d, %v", n, err)
	}
	mark()
	if n, err := li.Insert(ctx, []byte("q"), false, []byte(fat(30)), []byte(fat(71))); err != nil || n < 0 {
		t.Fatalf("Insert = %d, %v", n, err)
	}
	mark()
	flush()

	// Phase 5: pops cross page boundaries and kill pages at both ends.
	popn("q", true, 12)
	flush()
	popn("q", false, 7)
	flush()

	// Phase 6: a trim that drops whole pages and shrinks both edge
	// pages, the dead page records queuing behind the root.
	if err := li.Trim(ctx, []byte("q"), 5, -6); err != nil {
		t.Fatal(err)
	}
	mark()
	flush()

	// Phase 7: planted duplicates removed across pages in both
	// directions.
	n, err := li.Len(ctx, []byte("q"))
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int64{2, n / 2, n - 3} {
		if err := li.Set(ctx, []byte("q"), i, []byte("dup")); err != nil {
			t.Fatal(err)
		}
		mark()
	}
	if got, err := li.Rem(ctx, []byte("q"), 2, []byte("dup")); err != nil || got != 2 {
		t.Fatalf("Rem = %d, %v", got, err)
	}
	mark()
	if got, err := li.Rem(ctx, []byte("q"), -1, []byte("dup")); err != nil || got != 1 {
		t.Fatalf("Rem = %d, %v", got, err)
	}
	mark()
	flush()

	// Phase 8: moves between two paged lists, one pair left unflushed
	// so it coalesces into a single drain batch.
	elems2 := make([]string, 40)
	for i := range elems2 {
		elems2[i] = fat(100 + i)
	}
	push("q2", false, elems2...)
	flush()
	mv := func(src, dst string, srcLeft, dstLeft bool) {
		t.Helper()
		if _, ok, err := li.Move(ctx, []byte(src), []byte(dst), srcLeft, dstLeft); err != nil || !ok {
			t.Fatalf("Move(%q, %q) = %v, %v", src, dst, ok, err)
		}
		mark()
	}
	mv("q", "q2", false, true)
	flush()
	mv("q", "q2", true, false)
	mv("q2", "q", true, false)
	flush()

	// Phase 9: the paged list drains to death whole, then the key is
	// reborn inline.
	n, err = li.Len(ctx, []byte("q"))
	if err != nil {
		t.Fatal(err)
	}
	popn("q", true, int(n))
	flush()
	push("q", false, "r0", "r1")
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
	if len(cap.frames) < 80 {
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
		li2, err := sqlo1.NewList(tr2, sqlo1.ListConfig{})
		if err != nil {
			t.Fatal(err)
		}

		visible := snapshot(li2)
		found := false
		for _, img := range images {
			if sameImage(visible, img) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("cut %d: lists match no command boundary (a torn paged edit)", n)
		}
		if n == len(cap.frames) && !sameImage(visible, images[len(images)-1]) {
			t.Fatalf("full tail: lists do not match the final image")
		}
		if err := db2.Close(); err != nil {
			t.Fatalf("cut %d: close: %v", n, err)
		}
	}
}
