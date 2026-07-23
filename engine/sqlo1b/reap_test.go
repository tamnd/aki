package sqlo1b

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// fillSealedTTL seals one vlog extent with 950-byte records that all
// expire at the given deadline, alternating a second deadline when
// one is provided, and returns the sealed extent and its keys.
func (r *storeRig) fillSealedTTL(t *testing.T, prefix string, deadlines ...int64) (uint64, []string) {
	t.Helper()
	r.apply(t, putOp(prefix+"seed", []byte("x"), deadlines[0]))
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
			ops = append(ops, putOp(k, bytes.Repeat([]byte{'v'}, 950), deadlines[n%len(deadlines)]))
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

// TestExpiredCreditFiresCompaction drives the doc 11 section 3.2
// expired-fraction term: a pure-TTL extent books zero garbage, so
// only the near-class credit can make the debt controller pick it.
// The credit realizes at the extent's latest deadline, not its
// earliest, so live short-TTL neighbors are never relocated early.
func TestExpiredCreditFiresCompaction(t *testing.T) {
	ctx := context.Background()
	r := newStoreRig(t)
	early := r.now + 60_000
	late := r.now + 120_000
	ext, in := r.fillSealedTTL(t, "", early, late)
	if len(in) < 30 {
		t.Fatalf("only %d records landed in the sealed extent", len(in))
	}

	// Nothing is due and nothing was overwritten: no candidate.
	if _, ran, err := r.s.CompactStep(ctx); err != nil || ran {
		t.Fatalf("CompactStep before any deadline: ran %v, err %v", ran, err)
	}

	// Past the early deadline but not the late one: half the extent
	// is dead, and the credit still holds back because relocating the
	// live half now would be the early-relocation churn the section
	// 3.2 deadline rule exists to avoid.
	r.now = early + 10_000
	if _, ran, err := r.s.CompactStep(ctx); err != nil || ran {
		t.Fatalf("CompactStep before the latest deadline: ran %v, err %v", ran, err)
	}

	// Past the latest deadline the whole extent is dead: the credit
	// crosses the debt threshold with zero booked garbage, the pick
	// fires, and every record reaps instead of relocating.
	r.now = late + 10_000
	cs, ran, err := r.s.CompactStep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("CompactStep did not fire on a fully expired extent")
	}
	if cs.Expired != len(in) || cs.Relocated != 0 {
		t.Fatalf("expired %d relocated %d, want %d and 0", cs.Expired, cs.Relocated, len(in))
	}
	if cs.ExpiredBytes == 0 {
		t.Fatal("ExpiredBytes stayed zero across a full reap")
	}
	if st := r.s.grid.State(ext); st != StateQuarantined {
		t.Fatalf("extent %d state %s after reap, want quarantined", ext, st)
	}
	if _, ok := r.s.nearExt[ext]; ok {
		t.Fatalf("extent %d kept its near credit after compaction", ext)
	}

	ds := r.s.DebtStats()
	if ds.ExpiredDrops != uint64(cs.Expired) || ds.ExpiredBytes != uint64(cs.ExpiredBytes) {
		t.Fatalf("DebtStats drops %d bytes %d, want %d and %d", ds.ExpiredDrops, ds.ExpiredBytes, cs.Expired, cs.ExpiredBytes)
	}

	// The credit was consumed with the extent: no repeat pick.
	if _, ran, err := r.s.CompactStep(ctx); err != nil || ran {
		t.Fatalf("CompactStep after the reap: ran %v, err %v", ran, err)
	}
	for _, k := range in {
		delete(r.sh, k)
	}
	r.verify(t)
}

// TestNearCreditScope pins what the credit does not cover: mid and
// far records book nothing (their extents wait on booked garbage or
// the sampling reaper), and a checkpointed reopen starts the
// advisory map empty the same way it starts the garbage map empty.
// WAL replay past a checkpoint re-books through applyPut, which is
// the same rebuild-by-replay behavior supersessions give garbage.
func TestNearCreditScope(t *testing.T) {
	r := newStoreRig(t)
	mid := r.now + 24*60*60*1000
	ext, _ := r.fillSealedTTL(t, "", mid)
	if _, ok := r.s.nearExt[ext]; ok {
		t.Fatal("mid-class records booked near credit")
	}

	near := r.now + 60_000
	ext2, _ := r.fillSealedTTL(t, "n", near)
	nd, ok := r.s.nearExt[ext2]
	if !ok || nd.bytes == 0 || nd.deadline != near {
		t.Fatalf("near extent credit %+v, want bytes > 0 at deadline %d", nd, near)
	}

	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.reopen(t)
	if len(r.s.nearExt) != 0 {
		t.Fatalf("checkpointed reopen kept %d near credits, want none", len(r.s.nearExt))
	}
	r.verify(t)
}
