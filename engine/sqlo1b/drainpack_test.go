package sqlo1b

// Drain-to-extents tests (doc 04 section 7): the open vlog group
// carries across batches through write-through images, puts place in
// collection-then-size-class order, and a filling stream still seals
// extents with the carried builder in play.

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// posOf resolves the vlog position behind a key's index entry, dirty
// chain or cold.
func (r *storeRig) posOf(t *testing.T, key string) Pos {
	t.Helper()
	h := KeyHash([]byte(key))
	bucket := BucketOf(PlacementBits(h), r.s.level, r.s.split)
	chain, ok := r.s.dirty[bucket]
	if !ok {
		var err error
		if chain, err = r.s.coldChain(bucket); err != nil {
			t.Fatal(err)
		}
	}
	var (
		pos   Pos
		found bool
		perr  error
	)
	for _, c := range chain {
		c.Probe(Fingerprint(h), func(_ int, _ uint16, vptr uint64) bool {
			rec, err := r.s.resolveAt(Pos(vptr))
			if err != nil {
				perr = err
				return false
			}
			if bytes.Equal(rec.Key, []byte(key)) {
				pos, found = Pos(vptr), true
				return false
			}
			return true
		})
		if perr != nil {
			t.Fatal(perr)
		}
		if found {
			return pos
		}
	}
	t.Fatalf("no index entry for %q", key)
	return 0
}

// TestDrainOpenGroupCarriesAcrossBatches drives many small batches and
// asserts they share one vlog group instead of padding out a group
// each: the group builder survives the batch boundary and each batch
// end rewrites a fuller image in place.
func TestDrainOpenGroupCarriesAcrossBatches(t *testing.T) {
	r := newStoreRig(t)
	keys := make([]string, 0, 32)
	for b := range 8 {
		ops := make([]sqlo1.Op, 0, 4)
		for i := range 4 {
			k := fmt.Sprintf("pk%02d", b*4+i)
			keys = append(keys, k)
			ops = append(ops, putOp(k, fmt.Appendf(nil, "value-%02d-%02d", b, i), 0))
		}
		r.apply(t, ops...)
	}
	if r.s.vlog.next != 0 || r.s.gb == nil {
		t.Fatalf("32 small records across 8 batches closed a group: next %d, open %v", r.s.vlog.next, r.s.gb != nil)
	}
	first := r.posOf(t, keys[0])
	seen := map[uint16]bool{}
	for _, k := range keys {
		p := r.posOf(t, k)
		if p.Extent() != first.Extent() || p.Group() != first.Group() {
			t.Fatalf("%s landed at %s, first record at %s", k, p, first)
		}
		if seen[p.Slot()] {
			t.Fatalf("%s shares slot %d", k, p.Slot())
		}
		seen[p.Slot()] = true
	}
	r.verify(t)

	// The partial image is what checkpoints and what a reopen reads.
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.reopen(t)
	r.verify(t)
}

// TestDrainGroupImageRewriteAcrossCheckpoints checkpoints between two
// batches that share the open group, so the second checkpoint's image
// rewrites the first's in place. The settled record must read
// identically through both images and after a reopen.
func TestDrainGroupImageRewriteAcrossCheckpoints(t *testing.T) {
	r := newStoreRig(t)
	r.apply(t, putOp("alpha", []byte("first-image"), 0))
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.verify(t) // cold read off the one-record image
	pa := r.posOf(t, "alpha")

	r.apply(t, putOp("beta", []byte("second-image"), 0))
	pb := r.posOf(t, "beta")
	if pa.Extent() != pb.Extent() || pa.Group() != pb.Group() {
		t.Fatalf("beta landed at %s, alpha at %s: group did not carry across the checkpoint", pb, pa)
	}
	if got := r.posOf(t, "alpha"); got != pa {
		t.Fatalf("alpha moved from %s to %s", pa, got)
	}
	r.verify(t)
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.reopen(t)
	r.verify(t)
}

// TestDrainPlacementSortsByCollectionThenClass mixes plain records of
// two size classes with segment records of two collections in one
// scrambled batch and asserts the vlog placement order: plain records
// first by size class, then each collection's segments adjacent, with
// batch order breaking ties. Apply semantics run in batch order
// regardless, which verify pins.
func TestDrainPlacementSortsByCollectionThenClass(t *testing.T) {
	r := newStoreRig(t)
	segKey := func(rooth uint64, segid uint64) string {
		sk, err := NewSubkey(rooth, SubkindSeg, segid)
		if err != nil {
			t.Fatal(err)
		}
		return string(sk.Encode())
	}
	r1s0 := segKey(0x1111, 0)
	r1s1 := segKey(0x1111, 1)
	r2s0 := segKey(0x2222, 0)
	segOp := func(key string, tag byte) sqlo1.Op {
		op := putOp(key, bytes.Repeat([]byte{tag}, 100), 0)
		op.Rec.Gen = 1
		return op
	}
	r.apply(t,
		segOp(r2s0, 'c'),
		putOp("big", bytes.Repeat([]byte{'B'}, 600), 0),
		segOp(r1s0, 'a'),
		putOp("sm", []byte("tiny"), 0),
		segOp(r1s1, 'b'),
	)
	want := []string{"sm", "big", r1s0, r1s1, r2s0}
	first := r.posOf(t, want[0])
	for i, k := range want {
		p := r.posOf(t, k)
		if p.Extent() != first.Extent() || p.Group() != first.Group() {
			t.Fatalf("record %d landed at %s, first at %s", i, p, first)
		}
		if int(p.Slot()) != i {
			t.Fatalf("record %d (%x) at slot %d", i, k, p.Slot())
		}
	}
	r.verify(t)
}

// TestDrainSealsOnFillWithCarriedGroup pushes enough payload through
// small batches to fill and seal the first vlog extent while the
// carried builder is in play, then checkpoints and reopens so the
// sealed extent serves cold reads.
func TestDrainSealsOnFillWithCarriedGroup(t *testing.T) {
	r := newStoreRig(t)
	r.apply(t, putOp("seed", []byte("x"), 0))
	firstExt := r.s.vlog.ext
	n := 0
	for batch := 0; r.s.vlog.ext == firstExt; batch++ {
		if batch >= 400 {
			t.Fatalf("vlog extent never rolled after %d batches", batch)
		}
		ops := make([]sqlo1.Op, 0, 8)
		for range 8 {
			ops = append(ops, putOp(fmt.Sprintf("fill%05d", n), bytes.Repeat([]byte{'v'}, 950), 0))
			n++
		}
		r.apply(t, ops...)
	}
	r.verify(t)
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.reopen(t)
	r.verify(t)
}
