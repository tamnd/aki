package sqlo1b

// Compaction tests (doc 04 section 10): lookup-based relocation over
// one sealed vlog extent, the rootgen liveness probe, the lazy-expiry
// backstop, the garbage accounting feed, and the crash story where an
// uncheckpointed compaction evaporates on reopen.

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// fillSealed writes 950-byte records through small batches until the
// vlog stream rolls, returning the sealed extent and the keys whose
// records landed inside it.
func (r *storeRig) fillSealed(t *testing.T, prefix string) (uint64, []string) {
	t.Helper()
	r.apply(t, putOp(prefix+"seed", []byte("x"), 0))
	first := r.s.vlog.ext
	keys := []string{prefix + "seed"}
	n := 0
	for batch := 0; r.s.vlog.ext == first; batch++ {
		if batch >= 400 {
			t.Fatalf("vlog extent never rolled after %d batches", batch)
		}
		ops := make([]sqlo1.Op, 0, 8)
		for range 8 {
			k := fmt.Sprintf("%sfill%05d", prefix, n)
			keys = append(keys, k)
			ops = append(ops, putOp(k, bytes.Repeat([]byte{'v'}, 950), 0))
			n++
		}
		r.apply(t, ops...)
	}
	var in []string
	for _, k := range keys {
		if r.posOf(t, k).Extent() == first {
			in = append(in, k)
		}
	}
	return first, in
}

// TestCompactRelocatesLiveAndSkipsDead seals an extent, kills a third
// of its records by overwrite and a third by delete, and compacts:
// survivors relocate and stay readable, dead records skip, the extent
// quarantines, and the checkpoint releases it to free.
func TestCompactRelocatesLiveAndSkipsDead(t *testing.T) {
	ctx := context.Background()
	r := newStoreRig(t)
	ext, in := r.fillSealed(t, "")
	if len(in) < 30 {
		t.Fatalf("only %d records landed in the sealed extent", len(in))
	}

	var wantGarbage uint64
	var ops []sqlo1.Op
	for i, k := range in {
		if i%3 == 2 {
			continue // keeper
		}
		old, err := r.s.resolveAt(r.posOf(t, k))
		if err != nil {
			t.Fatal(err)
		}
		wantGarbage += uint64(old.EncodedLen())
		if i%3 == 0 {
			ops = append(ops, putOp(k, []byte("rewritten"), 0))
		} else {
			ops = append(ops, delOp(k))
		}
	}
	r.apply(t, ops...)
	if got := r.s.ExtentGarbage(ext); got != wantGarbage {
		t.Fatalf("extent %d garbage %d, want %d", ext, got, wantGarbage)
	}

	cs, err := r.s.CompactExtent(ctx, ext)
	if err != nil {
		t.Fatal(err)
	}
	keepers := len(in) - len(ops)
	if cs.Relocated != keepers || cs.Superseded != len(ops) {
		t.Fatalf("compact stats %+v, want %d relocated and %d superseded", cs, keepers, len(ops))
	}
	if cs.DeadSegments != 0 || cs.Expired != 0 {
		t.Fatalf("compact stats %+v counted segments or expiry on a plain extent", cs)
	}
	if st := r.s.grid.State(ext); st != StateQuarantined {
		t.Fatalf("compacted extent is %s, want quarantined", st)
	}
	if got := r.s.ExtentGarbage(ext); got != 0 {
		t.Fatalf("freed extent still books %d garbage bytes", got)
	}
	for _, k := range in {
		if _, ok := r.sh[k]; !ok {
			continue // deleted above, no index entry to check
		}
		if r.posOf(t, k).Extent() == ext {
			t.Fatalf("%s still points into the freed extent", k)
		}
	}
	r.verify(t)

	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if st := r.s.grid.State(ext); st != StateFree {
		t.Fatalf("extent is %s after the checkpoint, want free", st)
	}
	r.reopen(t)
	r.verify(t)
}

// TestCompactDropsDeadSegmentsAndExpired seals an extent holding two
// roots' segments and some expiring records, bumps one root and lets
// the expiry pass, then compacts: the dead root's segments and the
// expired records lose their index entries, everything else
// relocates.
func TestCompactDropsDeadSegmentsAndExpired(t *testing.T) {
	ctx := context.Background()
	r := newStoreRig(t)
	segKey := func(rooth, segid uint64) string {
		sk, err := NewSubkey(rooth, SubkindSeg, segid)
		if err != nil {
			t.Fatal(err)
		}
		return string(sk.Encode())
	}
	segOp := func(rooth, segid uint64) sqlo1.Op {
		op := putOp(segKey(rooth, segid), bytes.Repeat([]byte{'s'}, 950), 0)
		op.Rec.Gen = 1
		return op
	}

	const liveRoot, deadRoot = 0x1111, 0x2222
	r.apply(t, putOp("seed", []byte("x"), 0))
	first := r.s.vlog.ext
	classOf := map[string]string{}
	n := uint64(0)
	for batch := 0; r.s.vlog.ext == first; batch++ {
		if batch >= 400 {
			t.Fatalf("vlog extent never rolled after %d batches", batch)
		}
		lk, dk := segKey(liveRoot, n), segKey(deadRoot, n)
		ek := fmt.Sprintf("exp%05d", n)
		pk := fmt.Sprintf("plain%05d", n)
		classOf[lk], classOf[dk], classOf[ek], classOf[pk] = "live", "dead", "exp", "live"
		r.apply(t,
			segOp(liveRoot, n),
			segOp(deadRoot, n),
			putOp(ek, bytes.Repeat([]byte{'e'}, 950), r.now+60_000),
			putOp(pk, bytes.Repeat([]byte{'p'}, 950), 0),
		)
		n++
	}

	r.genbump(t, deadRoot, 2)
	r.now += 120_000 // every expiring record is now past due
	want := map[string]int{}
	inExt := map[string]bool{}
	for k, class := range classOf {
		if r.posOf(t, k).Extent() == first {
			want[class]++
			inExt[k] = true
		}
	}

	cs, err := r.s.CompactExtent(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	// The seed record and the genbump record may share the extent;
	// they relocate as plain live records.
	if cs.DeadSegments != want["dead"] || cs.Expired != want["exp"] || cs.Relocated < want["live"] {
		t.Fatalf("compact stats %+v, want %d dead segments, %d expired, at least %d relocated", cs, want["dead"], want["exp"], want["live"])
	}
	if cs.Superseded != 0 {
		t.Fatalf("compact stats %+v counted superseded records with no overwrites", cs)
	}

	// Dropped entries leave the shadow so verify's Keys equation and
	// the guaranteed-miss check see the same store the index does.
	for k := range inExt {
		if classOf[k] == "exp" || classOf[k] == "dead" {
			delete(r.sh, k)
		}
	}
	r.checkLive(t, deadRoot, 1, false)
	r.checkLive(t, liveRoot, 1, true)
	r.verify(t)
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.reopen(t)
	r.verify(t)
}

// TestCompactCrashEvaporates compacts without checkpointing and
// reopens: recovery replays the WAL into fresh positions, the
// quarantine dissolves back to sealed, reads all work, and a second
// compaction finds nothing left alive in the extent.
func TestCompactCrashEvaporates(t *testing.T) {
	ctx := context.Background()
	r := newStoreRig(t)
	ext, in := r.fillSealed(t, "")
	var ops []sqlo1.Op
	for i, k := range in {
		if i%4 == 0 {
			ops = append(ops, putOp(k, []byte("rewritten"), 0))
		}
	}
	r.apply(t, ops...)
	if _, err := r.s.CompactExtent(ctx, ext); err != nil {
		t.Fatal(err)
	}
	r.verify(t)

	r.reopen(t)
	if st := r.s.grid.State(ext); st != StateSealed {
		t.Fatalf("extent is %s after an uncheckpointed compaction and reopen, want sealed", st)
	}
	r.verify(t)

	// Replay re-drained every record to fresh positions, so the whole
	// extent probes dead now.
	cs, err := r.s.CompactExtent(ctx, ext)
	if err != nil {
		t.Fatal(err)
	}
	if cs.Relocated != 0 || cs.Superseded == 0 {
		t.Fatalf("recompaction after replay found %+v, want everything superseded", cs)
	}
	r.verify(t)
}

// TestCompactGuards pins the refusals: active, free, and out-of-grid
// extents do not compact, and the store stays usable after each
// refusal.
func TestCompactGuards(t *testing.T) {
	ctx := context.Background()
	r := newStoreRig(t)
	r.apply(t, putOp("k", []byte("v"), 0))

	if _, err := r.s.CompactExtent(ctx, r.s.vlog.ext); err == nil {
		t.Fatal("compacting the active vlog extent did not error")
	}
	free := r.s.grid.ExtentCount() - 1
	if r.s.grid.State(free) != StateFree {
		t.Fatalf("extent %d is %s, want free", free, r.s.grid.State(free))
	}
	if _, err := r.s.CompactExtent(ctx, free); err == nil {
		t.Fatal("compacting a free extent did not error")
	}
	if _, err := r.s.CompactExtent(ctx, r.s.grid.ExtentCount()+7); err == nil {
		t.Fatal("compacting past the grid did not error")
	}
	r.verify(t)
}

// TestCompactRelocatesLoneBigRecords pins the relocation trim: a
// slotted record alone in its group comes back from GroupView.Record
// as the whole slice up to the slot table, 4092 bytes of pad tail
// included, so relocating the untrimmed slice crosses BlobThreshold
// and misroutes a slotted record to the blob path. Found by the
// tiered crash suite's preflight (seed 4000001, wide value band).
func TestCompactRelocatesLoneBigRecords(t *testing.T) {
	ctx := context.Background()
	r := newStoreRig(t)
	// 3000-byte values encode past half a group's payload, so every
	// record sits alone in its group with an rlen far under the
	// threshold and a slot slice far over it.
	val := bytes.Repeat([]byte{'b'}, 3000)
	r.apply(t, putOp("lone-seed", val, 0))
	first := r.s.vlog.ext
	keys := []string{"lone-seed"}
	for n := 0; r.s.vlog.ext == first; n++ {
		if n >= 400 {
			t.Fatalf("vlog extent never rolled after %d records", n)
		}
		k := fmt.Sprintf("lone%05d", n)
		keys = append(keys, k)
		r.apply(t, putOp(k, val, 0))
	}
	var in []string
	for _, k := range keys {
		if r.posOf(t, k).Extent() == first {
			in = append(in, k)
		}
	}
	if len(in) < 4 {
		t.Fatalf("only %d records landed in the sealed extent", len(in))
	}
	var ops []sqlo1.Op
	for i, k := range in {
		if i%2 == 0 {
			ops = append(ops, delOp(k))
		}
	}
	r.apply(t, ops...)

	cs, err := r.s.CompactExtent(ctx, first)
	if err != nil {
		t.Fatalf("compacting lone big records: %v", err)
	}
	if want := len(in) - len(ops); cs.Relocated != want {
		t.Fatalf("compact stats %+v, want %d relocated", cs, want)
	}
	for i, k := range in {
		if i%2 == 0 {
			continue
		}
		rec, err := r.s.Get(ctx, []byte(k))
		if err != nil {
			t.Fatalf("Get(%s) after relocation: %v", k, err)
		}
		if !bytes.Equal(rec.Value, val) {
			t.Fatalf("Get(%s) came back %d bytes, want %d", k, len(rec.Value), len(val))
		}
	}
	r.verify(t)
	r.reopen(t)
	r.verify(t)
}
