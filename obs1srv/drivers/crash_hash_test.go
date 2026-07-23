package drivers

// The state-hash half of the O1 crash row (doc 10 W-I3 at the O1c
// points): after each crash scenario settles, the bucket's committed
// stream must replay to one state hash no matter how the walk is cut.
// The walk is the store-boundary replayer from the O1b suite, rebuilt
// here on the exported engine surface: chain batches in order through
// the production follower, one ranged GET per committed section, frames
// gated by the per-group applied seq, a running digest over the
// accepted stream. Frames are deterministic post-decision effects, so
// equal streams are equal states.

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
)

// hashCollector keeps every chain batch's commit records in chain order.
type hashCollector struct {
	batches [][]obs1.CommitRecord
}

func (c *hashCollector) ApplyChain(pos obs1.ChainPos, h obs1.Header, b obs1.ChainBatch) error {
	var recs []obs1.CommitRecord
	for _, r := range b.Records {
		if cr, ok := r.(obs1.CommitRecord); ok {
			recs = append(recs, cr)
		}
	}
	c.batches = append(c.batches, recs)
	return nil
}

// hashWalk is the seq-gated digest walk over one bucket.
type hashWalk struct {
	store    obs1.Store
	applied  map[uint16]uint64
	h        hash.Hash64
	accepted int
	skipped  int
}

func newHashWalk(store obs1.Store) *hashWalk {
	return &hashWalk{store: store, applied: make(map[uint16]uint64), h: fnv.New64a()}
}

func (r *hashWalk) apply(batches [][]obs1.CommitRecord) error {
	for _, recs := range batches {
		for _, rec := range recs {
			for _, cs := range rec.Sections {
				if err := r.section(rec, cs); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *hashWalk) section(rec obs1.CommitRecord, cs obs1.CommitSection) error {
	e := obs1.WALIndexEntry{
		Group: cs.Group, Epoch: cs.Epoch, Offset: cs.Offset,
		StoredLen: cs.StoredLen, RawLen: cs.StoredLen, NFrames: cs.NFrames,
		FirstSeq: cs.FirstSeq, LastSeq: cs.LastSeq,
	}
	off, n := e.SectionSpan()
	key := fmt.Sprintf("p/wal/%016x/%016d", rec.WALNode, rec.WALSeq)
	b, _, err := r.store.GetRange(context.Background(), key, off, n)
	if err != nil {
		return fmt.Errorf("section GET %s: %w", key, err)
	}
	sec, err := obs1.ParseWALSection(b, e)
	if err != nil {
		return err
	}
	for _, f := range sec.Frames {
		cur := r.applied[sec.Group]
		if f.Seq <= cur {
			r.skipped++
			continue
		}
		if f.Seq != cur+1 {
			return fmt.Errorf("group %d frame seq %d after applied %d: the committed stream has a gap", sec.Group, f.Seq, cur)
		}
		r.applied[sec.Group] = f.Seq
		var hdr [18]byte
		binary.LittleEndian.PutUint16(hdr[0:2], sec.Group)
		binary.LittleEndian.PutUint64(hdr[2:10], f.Seq)
		hdr[10] = f.Kind
		hdr[11] = f.Flags
		binary.LittleEndian.PutUint16(hdr[12:14], f.Slot)
		binary.LittleEndian.PutUint32(hdr[14:18], uint32(len(f.Key)))
		r.h.Write(hdr[:])
		r.h.Write(f.Key)
		binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(f.Payload)))
		r.h.Write(hdr[0:4])
		r.h.Write(f.Payload)
		r.accepted++
	}
	return nil
}

// assertReplayIdempotent walks the crashed bucket's chain and pins the
// state hash three ways: a fresh full walk, a second fresh walk that
// must agree, and restart shapes that stop at a prefix and re-walk the
// whole chain, which must skip the covered frames and land on the same
// hash. Orphan WAL objects the chain never names stay invisible by
// construction of the walk.
func assertReplayIdempotent(t *testing.T, store obs1.Store) {
	t.Helper()
	col := &hashCollector{}
	ap, err := obs1.NewChainAppender(store, "p", 0, 0xFA, 1, obs1.ChainPos{}, col)
	if err != nil {
		t.Fatal(err)
	}
	if err := ap.Follow(context.Background()); err != nil {
		t.Fatal(err)
	}
	batches := col.batches

	full := newHashWalk(store)
	if err := full.apply(batches); err != nil {
		t.Fatal(err)
	}
	if full.accepted == 0 {
		t.Fatal("the committed stream replayed no frames")
	}
	if full.skipped != 0 {
		t.Fatalf("fresh replay skipped %d frames: a committed seq was re-emitted", full.skipped)
	}
	want := full.h.Sum64()

	again := newHashWalk(store)
	if err := again.apply(batches); err != nil {
		t.Fatal(err)
	}
	if again.h.Sum64() != want {
		t.Fatal("two walks of one bucket disagree on the state hash")
	}

	for _, k := range []int{0, len(batches) / 2, len(batches)} {
		pre := newHashWalk(store)
		if err := pre.apply(batches[:k]); err != nil {
			t.Fatalf("prefix %d: %v", k, err)
		}
		if err := pre.apply(batches); err != nil {
			t.Fatalf("prefix %d re-walk: %v", k, err)
		}
		if pre.h.Sum64() != want {
			t.Fatalf("prefix %d then a full re-walk missed the full state", k)
		}
	}
	t.Logf("state hash %016x over %d frames, idempotent across cuts", want, full.accepted)
}
