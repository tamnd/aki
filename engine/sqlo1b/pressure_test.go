package sqlo1b

// Pressure gauge tests: the WAL rung's lag ratio across checkpoint
// and crash reopen, the free-extent rung's reserve-to-hard-minimum
// slide under a byte cap, and the CompactOnce verb.

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

func TestPressureWalGauge(t *testing.T) {
	r := newStoreRig(t)
	if p := r.s.Pressure(); p.Wal != 0 || p.Extent != 0 || p.Shed {
		t.Fatalf("fresh store pressure %+v", p)
	}
	pol := CheckpointPolicy{Bytes: 4 << 10}
	r.s.SetCheckpointPolicy(pol)

	var ops []sqlo1.Op
	for i := range 8 {
		ops = append(ops, putOp(fmt.Sprintf("wal%02d", i), bytes.Repeat([]byte{'w'}, 950), 0))
	}
	r.apply(t, ops...)
	if p := r.s.Pressure(); p.Wal < 1 {
		t.Fatalf("8 KiB of frames against a 4 KiB cadence reads %.3f, want >= 1", p.Wal)
	}

	// The checkpoint trims; only its own CKPT frame stays uncounted
	// live, so the gauge drops to nearly zero.
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if p := r.s.Pressure(); p.Wal >= 0.5 {
		t.Fatalf("post-checkpoint lag reads %.3f, want under 0.5", p.Wal)
	}

	// The gauge survives a crash reopen: replay rebuilds it, so a
	// process that dies before ever checkpointing still feels the lag.
	ops = ops[:0]
	for i := range 8 {
		ops = append(ops, putOp(fmt.Sprintf("more%02d", i), bytes.Repeat([]byte{'m'}, 950), 0))
	}
	r.apply(t, ops...)
	r.reopen(t)
	r.s.SetCheckpointPolicy(pol)
	if p := r.s.Pressure(); p.Wal < 1 {
		t.Fatalf("reopen dropped the lag gauge to %.3f, want >= 1", p.Wal)
	}
	r.verify(t)
}

func TestPressureExtentGauge(t *testing.T) {
	r := newStoreRig(t)
	r.apply(t, putOp("k", []byte("v"), 0))

	// Uncapped, the rung reads zero: the file grows on demand and the
	// gauge has no budget to measure against.
	if p := r.s.Pressure(); p.Extent != 0 || p.Shed {
		t.Fatalf("uncapped pressure %+v", p)
	}

	es := int64(r.s.sb.ExtentSize)
	cnt := int64(r.s.sb.ExtentCount)
	free := int64(r.s.grid.FreeCount())
	hard := int64(r.s.hardMinExtents())
	reserve := 2 * hard

	// capFor sizes the byte cap so total headroom (free extents plus
	// grow room) lands exactly on h extents.
	capFor := func(h int64) int64 { return es * (cnt + h - free) }
	if hard-free < 0 {
		t.Fatalf("rig has %d free extents, over the %d hard minimum; the capFor helper needs grow room", free, hard)
	}

	r.s.SetMaxBytes(capFor(reserve))
	if p := r.s.Pressure(); p.Extent != 0 || p.Shed {
		t.Fatalf("at the reserve line pressure is %+v, want zero", p)
	}
	mid := hard + (reserve-hard)/2
	r.s.SetMaxBytes(capFor(mid))
	pm := r.s.Pressure()
	if pm.Extent <= 0 || pm.Extent >= 1 || pm.Shed {
		t.Fatalf("between reserve and hard minimum pressure is %+v, want extent in (0,1) unshed", pm)
	}
	r.s.SetMaxBytes(capFor(hard))
	ph := r.s.Pressure()
	if ph.Extent < 1 || !ph.Shed {
		t.Fatalf("at the hard minimum pressure is %+v, want extent >= 1 and shed", ph)
	}
	if pm.Extent >= ph.Extent {
		t.Fatalf("gauge fell from %.3f to %.3f as the cap tightened", pm.Extent, ph.Extent)
	}
	r.s.SetMaxBytes(0)
	if p := r.s.Pressure(); p.Extent != 0 || p.Shed {
		t.Fatalf("uncapping left pressure at %+v", p)
	}
}

func TestCompactOnceVerb(t *testing.T) {
	ctx := context.Background()
	r := newStoreRig(t)
	_, in := r.fillSealed(t, "")
	var ops []sqlo1.Op
	for i, k := range in {
		if i%2 == 0 {
			ops = append(ops, delOp(k))
		}
	}
	r.apply(t, ops...)

	compacted, err := r.s.CompactOnce(ctx)
	if err != nil || !compacted {
		t.Fatalf("CompactOnce over threshold: %v %v", compacted, err)
	}
	compacted, err = r.s.CompactOnce(ctx)
	if err != nil || compacted {
		t.Fatalf("CompactOnce at steady state: %v %v", compacted, err)
	}
	r.verify(t)
}
