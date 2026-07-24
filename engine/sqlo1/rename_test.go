package sqlo1

import (
	"context"
	"fmt"
	"testing"
)

// TestRenameBasic pins RENAME's answer shape and the TTL move on
// plain strings: src's expiry follows the value, and a dst that had
// its own TTL loses it when src had none.
func TestRenameBasic(t *testing.T) {
	ctx := context.Background()
	rig := newHashRig(t)
	set := func(k, v string) {
		t.Helper()
		if err := rig.s.Set(ctx, []byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	// Missing src.
	existed, _, err := rig.s.Rename(ctx, []byte("nosuch"), []byte("b"), false)
	if err != nil || existed {
		t.Fatalf("missing src: existed=%v err=%v", existed, err)
	}

	// Plain move.
	set("a", "v1")
	existed, done, err := rig.s.Rename(ctx, []byte("a"), []byte("b"), false)
	if err != nil || !existed || !done {
		t.Fatalf("rename a->b: %v %v %v", existed, done, err)
	}
	if v, ok, _ := rig.s.Get(ctx, []byte("b")); !ok || string(v) != "v1" {
		t.Fatalf("b = %q %v, want v1", v, ok)
	}
	if _, ok, _ := rig.s.Get(ctx, []byte("a")); ok {
		t.Fatal("a still exists after rename")
	}

	// Same key: RENAME is done, RENAMENX is not.
	if _, done, _ := rig.s.Rename(ctx, []byte("b"), []byte("b"), false); !done {
		t.Fatal("same-key RENAME not done")
	}
	if _, done, _ := rig.s.Rename(ctx, []byte("b"), []byte("b"), true); done {
		t.Fatal("same-key RENAMENX done")
	}

	// NX refusal leaves both keys alone; NX onto a free name moves.
	set("c", "v2")
	if _, done, _ := rig.s.Rename(ctx, []byte("c"), []byte("b"), true); done {
		t.Fatal("RENAMENX onto a live dst done")
	}
	if v, _, _ := rig.s.Get(ctx, []byte("b")); string(v) != "v1" {
		t.Fatalf("refused NX clobbered dst: %q", v)
	}
	if v, _, _ := rig.s.Get(ctx, []byte("c")); string(v) != "v2" {
		t.Fatalf("refused NX moved src: %q", v)
	}
	if _, done, _ := rig.s.Rename(ctx, []byte("c"), []byte("d"), true); !done {
		t.Fatal("RENAMENX onto a free name refused")
	}

	// TTL follows the value.
	now := int64(1 << 41)
	set("t1", "x")
	if _, err := rig.tr.ExpireAt(ctx, []byte("t1"), now+60_000); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rig.s.Rename(ctx, []byte("t1"), []byte("t2"), false); err != nil {
		t.Fatal(err)
	}
	if _, _, expMs, ok, _ := rig.tr.LookupEntry(ctx, []byte("t2")); !ok || expMs != now+60_000 {
		t.Fatalf("t2 expMs = %d %v, want %d", expMs, ok, now+60_000)
	}

	// A dst with its own TTL loses it under a persistent src.
	set("p1", "x")
	set("p2", "y")
	if _, err := rig.tr.ExpireAt(ctx, []byte("p2"), now+60_000); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rig.s.Rename(ctx, []byte("p1"), []byte("p2"), false); err != nil {
		t.Fatal(err)
	}
	if _, _, expMs, ok, _ := rig.tr.LookupEntry(ctx, []byte("p2")); !ok || expMs != 0 {
		t.Fatalf("p2 expMs = %d %v, want 0", expMs, ok)
	}
}

// TestRenameRootMove pins the doc 12 section 2.2 contract: a
// segmented collection moves as one root record, its minted rooth
// keeps every segment subkey valid under the new name, and the cold
// view a restart sees agrees. It also covers the overwrite arm: a
// rename onto a live segmented hash retires the old plane.
func TestRenameRootMove(t *testing.T) {
	ctx := context.Background()
	rig := newHashRig(t)

	const n = 500
	for i := 0; i < n; i++ {
		f := fmt.Sprintf("f%04d", i)
		if _, err := rig.h.HSet(ctx, []byte("big"), []byte(f), []byte("v"+f)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := rig.h.HSet(ctx, []byte("victim"), []byte("dead"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := rig.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Rename over the live segmented dst.
	existed, done, err := rig.s.Rename(ctx, []byte("big"), []byte("victim"), false)
	if err != nil || !existed || !done {
		t.Fatalf("rename big->victim: %v %v %v", existed, done, err)
	}
	if err := rig.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// The hot view: every field resolves under the new name, the old
	// name and the old dst content are gone.
	if v, ok, err := rig.h.HGet(ctx, []byte("victim"), []byte("f0123")); err != nil || !ok || string(v) != "vf0123" {
		t.Fatalf("victim f0123 = %q %v %v", v, ok, err)
	}
	if _, ok, _ := rig.h.HGet(ctx, []byte("victim"), []byte("dead")); ok {
		t.Fatal("old dst field survived the overwrite")
	}
	if _, ok, _ := rig.h.HGet(ctx, []byte("big"), []byte("f0123")); ok {
		t.Fatal("src still answers after rename")
	}
	if tag, ok, _ := rig.tr.TypeTag(ctx, []byte("victim")); !ok || tag != TagHash {
		t.Fatalf("victim TypeTag = %d %v, want hash", tag, ok)
	}

	// The cold view: a fresh runtime over the same store sees the
	// same keyspace, so the root image, the tombstone, and the plane
	// all drained correctly.
	h2 := rig.reopen()
	if v, ok, err := h2.HGet(ctx, []byte("victim"), []byte("f0456")); err != nil || !ok || string(v) != "vf0456" {
		t.Fatalf("cold victim f0456 = %q %v %v", v, ok, err)
	}
	if _, ok, _ := h2.HGet(ctx, []byte("big"), []byte("f0456")); ok {
		t.Fatal("cold src still answers after rename")
	}
	if n2, err := h2.HLen(ctx, []byte("victim")); err != nil || n2 != n {
		t.Fatalf("cold HLEN = %d %v, want %d", n2, err, n)
	}
}

// TestRenameIO is the milestone's O(1) IO-count test: renaming a
// collection bills one root image and one tombstone whatever the
// collection holds, so the bill for a 100-field hash and a 2000-field
// hash is byte-for-byte the same op count.
func TestRenameIO(t *testing.T) {
	ctx := context.Background()

	bill := func(fields int) (puts, dels int) {
		t.Helper()
		rig := newHashRig(t)
		for i := 0; i < fields; i++ {
			f := fmt.Sprintf("f%05d", i)
			if _, err := rig.h.HSet(ctx, []byte("h"), []byte(f), []byte("v"+f)); err != nil {
				t.Fatal(err)
			}
		}
		if err := rig.tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		mark := len(rig.rs.batches)
		if _, _, err := rig.s.Rename(ctx, []byte("h"), []byte("h2"), false); err != nil {
			t.Fatal(err)
		}
		if err := rig.tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		for _, b := range rig.rs.batches[mark:] {
			for _, op := range b.Ops {
				if op.Del {
					dels++
				} else {
					puts++
				}
			}
		}
		return puts, dels
	}

	smallPuts, smallDels := bill(100)
	bigPuts, bigDels := bill(2000)
	if smallPuts != bigPuts || smallDels != bigDels {
		t.Fatalf("bill grew with the collection: %d/%d at 100 fields, %d/%d at 2000", smallPuts, smallDels, bigPuts, bigDels)
	}
	// The exact bill: the dst root image and the src tombstone. The
	// count pins the root-move contract, not just its scaling.
	if bigPuts != 1 || bigDels != 1 {
		t.Fatalf("rename billed %d puts, %d dels, want 1 and 1", bigPuts, bigDels)
	}
}
