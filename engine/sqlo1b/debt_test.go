package sqlo1b

// Debt controller tests (doc 04 section 10 policy): the 75 percent
// utilization threshold, selection by garbage with the slotted-over-
// blob tie rule, blob extent relocation, and the WA telemetry.

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// fillBlobSealed rolls the blob stream with 32 KiB values (over the
// blob threshold, under the rig's 64 KiB WAL segment) and returns the
// sealed blob extent and the keys whose runs landed inside it.
func (r *storeRig) fillBlobSealed(t *testing.T) (uint64, []string) {
	t.Helper()
	val := bytes.Repeat([]byte{'b'}, 32<<10)
	r.apply(t, putOp("blob00", val, 0))
	first := r.s.blob.ext
	keys := []string{"blob00"}
	for i := 1; r.s.blob.ext == first; i++ {
		if i > 64 {
			t.Fatal("blob stream never rolled")
		}
		k := fmt.Sprintf("blob%02d", i)
		keys = append(keys, k)
		r.apply(t, putOp(k, val, 0))
	}
	var in []string
	for _, k := range keys {
		if r.posOf(t, k).Extent() == first {
			in = append(in, k)
		}
	}
	return first, in
}

// TestCompactBlobExtent relocates a sealed blob extent: superseded
// runs skip, survivors re-append through the blob stream and stay
// readable across a checkpointed reopen.
func TestCompactBlobExtent(t *testing.T) {
	ctx := context.Background()
	r := newStoreRig(t)
	ext, in := r.fillBlobSealed(t)
	if len(in) < 8 {
		t.Fatalf("only %d blob runs landed in the sealed extent", len(in))
	}
	var ops []sqlo1.Op
	for i, k := range in {
		if i%2 == 0 {
			ops = append(ops, delOp(k))
		}
	}
	r.apply(t, ops...)

	cs, err := r.s.CompactExtent(ctx, ext)
	if err != nil {
		t.Fatal(err)
	}
	if cs.Superseded != len(ops) || cs.Relocated != len(in)-len(ops) {
		t.Fatalf("compact stats %+v, want %d superseded and %d relocated", cs, len(ops), len(in)-len(ops))
	}
	if st := r.s.grid.State(ext); st != StateQuarantined {
		t.Fatalf("compacted blob extent is %s, want quarantined", st)
	}
	for _, k := range in {
		if _, ok := r.sh[k]; !ok {
			continue
		}
		p := r.posOf(t, k)
		if p.Extent() == ext {
			t.Fatalf("%s still points into the freed blob extent", k)
		}
		if !p.IsBlob() {
			t.Fatalf("%s relocated to a non-blob position %v", k, p)
		}
	}
	r.verify(t)
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.reopen(t)
	r.verify(t)
}

// TestDebtThreshold pins the 75 percent utilization rule: an extent
// under a quarter garbage is not debt, one over it is, and paying the
// debt down converges to no candidates over threshold.
func TestDebtThreshold(t *testing.T) {
	ctx := context.Background()
	r := newStoreRig(t)
	ext, in := r.fillSealed(t, "")

	// Kill roughly 10 percent: under the quarter-capacity threshold.
	var ops []sqlo1.Op
	for i, k := range in {
		if i%10 == 0 {
			ops = append(ops, delOp(k))
		}
	}
	r.apply(t, ops...)
	if got := r.s.ExtentGarbage(ext); got >= r.s.debtThreshold() {
		t.Fatalf("10 percent kill booked %d garbage, at or over the %d threshold", got, r.s.debtThreshold())
	}
	if _, compacted, err := r.s.CompactStep(ctx); err != nil || compacted {
		t.Fatalf("CompactStep under threshold: compacted=%v err=%v", compacted, err)
	}
	if st := r.s.grid.State(ext); st != StateSealed {
		t.Fatalf("under-threshold extent is %s, want sealed untouched", st)
	}

	// Kill another 30 percent: now past it.
	ops = ops[:0]
	for i, k := range in {
		if i%10 != 0 && i%3 == 0 {
			ops = append(ops, delOp(k))
		}
	}
	r.apply(t, ops...)
	if got := r.s.ExtentGarbage(ext); got < r.s.debtThreshold() {
		t.Fatalf("40 percent kill booked %d garbage, under the %d threshold", got, r.s.debtThreshold())
	}
	d := r.s.DebtStats()
	if d.OverThreshold != 1 {
		t.Fatalf("debt stats %+v, want exactly one extent over threshold", d)
	}
	cs, compacted, err := r.s.CompactStep(ctx)
	if err != nil || !compacted {
		t.Fatalf("CompactStep over threshold: compacted=%v err=%v", compacted, err)
	}
	if cs.Relocated == 0 || cs.Superseded == 0 {
		t.Fatalf("compact stats %+v, want both survivors and garbage", cs)
	}
	if st := r.s.grid.State(ext); st != StateQuarantined {
		t.Fatalf("paid extent is %s, want quarantined", st)
	}
	if _, compacted, err := r.s.CompactStep(ctx); err != nil || compacted {
		t.Fatalf("CompactStep after paydown: compacted=%v err=%v", compacted, err)
	}
	r.verify(t)
}

// TestDebtSelectionOrder seals two extents and hands the dirtier one
// more garbage: CompactStep must take it first and the cleaner one
// next, highest garbage first.
func TestDebtSelectionOrder(t *testing.T) {
	ctx := context.Background()
	r := newStoreRig(t)
	ext1, in1 := r.fillSealed(t, "a-")
	ext2, in2 := r.fillSealed(t, "b-")
	if ext1 == ext2 {
		t.Fatalf("both fills sealed the same extent %d", ext1)
	}

	var ops []sqlo1.Op
	for i, k := range in1 {
		if i%3 != 0 {
			ops = append(ops, delOp(k))
		}
	}
	for i, k := range in2 {
		if i%3 == 0 {
			ops = append(ops, delOp(k))
		}
	}
	r.apply(t, ops...)
	if g1, g2 := r.s.ExtentGarbage(ext1), r.s.ExtentGarbage(ext2); g1 <= g2 {
		t.Fatalf("garbage %d on ext1 vs %d on ext2, the test wants ext1 dirtier", g1, g2)
	}

	if _, compacted, err := r.s.CompactStep(ctx); err != nil || !compacted {
		t.Fatalf("first step: compacted=%v err=%v", compacted, err)
	}
	if st := r.s.grid.State(ext1); st != StateQuarantined {
		t.Fatalf("first step took extent in state %s, want the dirtier ext1 quarantined", st)
	}
	if st := r.s.grid.State(ext2); st != StateSealed {
		t.Fatalf("ext2 is %s after the first step, want still sealed", st)
	}
	if _, compacted, err := r.s.CompactStep(ctx); err != nil || !compacted {
		t.Fatalf("second step: compacted=%v err=%v", compacted, err)
	}
	if st := r.s.grid.State(ext2); st != StateQuarantined {
		t.Fatalf("ext2 is %s after the second step, want quarantined", st)
	}
	r.verify(t)
}

// TestDebtTiePrefersSlotted pins the doc 04 tie rule directly: equal
// booked garbage on a sealed slotted extent and a sealed blob extent
// selects the slotted one.
func TestDebtTiePrefersSlotted(t *testing.T) {
	r := newStoreRig(t)
	slotted, _ := r.fillSealed(t, "")
	blob, _ := r.fillBlobSealed(t)

	r.s.mu.Lock()
	need := r.s.debtThreshold()
	r.s.garbageExt = map[uint64]uint64{slotted: need, blob: need}
	got, ok, err := r.s.selectDebt()
	r.s.mu.Unlock()
	if err != nil || !ok {
		t.Fatalf("selectDebt: ok=%v err=%v", ok, err)
	}
	if got != slotted {
		t.Fatalf("tie selected extent %d, want the slotted %d over the blob %d", got, slotted, blob)
	}
}

// TestDebtWATelemetry watches the write amplification counters over
// a fill, an overwrite wave, and a compaction: logical bytes track
// what the batches carried, physical bytes stay ahead of them, the
// compactor's share lands in RelocatedBytes, and reopen resets the
// gauges.
func TestDebtWATelemetry(t *testing.T) {
	ctx := context.Background()
	r := newStoreRig(t)
	if d := r.s.DebtStats(); d.LogicalBytes != 0 || d.WA() != 0 {
		t.Fatalf("fresh store debt stats %+v", d)
	}
	ext, in := r.fillSealed(t, "")
	var ops []sqlo1.Op
	for i, k := range in {
		if i%2 == 0 {
			ops = append(ops, putOp(k, []byte("rewritten"), 0))
		}
	}
	r.apply(t, ops...)

	before := r.s.DebtStats()
	if before.LogicalBytes == 0 || before.DataBytes < before.LogicalBytes {
		t.Fatalf("debt stats %+v: data bytes should cover every logical byte plus padding", before)
	}
	if before.WA() < 1 {
		t.Fatalf("data-path WA %.3f under 1 with no compaction yet", before.WA())
	}
	if before.RelocatedBytes != 0 || before.Compactions != 0 {
		t.Fatalf("debt stats %+v before any compaction", before)
	}

	cs, err := r.s.CompactExtent(ctx, ext)
	if err != nil {
		t.Fatal(err)
	}
	after := r.s.DebtStats()
	if after.RelocatedBytes != uint64(cs.RelocatedBytes) || after.Compactions != 1 {
		t.Fatalf("debt stats %+v, compactor reported %+v", after, cs)
	}
	if after.LogicalBytes != before.LogicalBytes {
		t.Fatalf("compaction moved the logical gauge %d -> %d", before.LogicalBytes, after.LogicalBytes)
	}
	if after.DataBytes <= before.DataBytes {
		t.Fatal("relocation wrote nothing by the physical gauge")
	}
	if after.WA() <= before.WA() {
		t.Fatalf("WA %.3f -> %.3f: compaction must show up as amplification", before.WA(), after.WA())
	}
	t.Logf("data-path WA %.3f before compaction, %.3f after", before.WA(), after.WA())

	// A checkpointed reopen replays nothing, so every gauge resets;
	// the counters read as rates since open by design.
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.reopen(t)
	if d := r.s.DebtStats(); d.LogicalBytes != 0 || d.DataBytes != 0 || d.Compactions != 0 {
		t.Fatalf("reopen kept the runtime gauges: %+v", d)
	}
	r.verify(t)
}
