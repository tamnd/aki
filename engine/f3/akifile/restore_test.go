package akifile

import (
	"errors"
	"testing"
)

// TestReplayToBarrierIncludesUpToCut replays a stream with records on both sides of a
// barrier and confirms the image is exactly the cut: every data segment at or below the
// watermark, the barrier segment itself skipped, and nothing appended after it.
func TestReplayToBarrierIncludesUpToCut(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if _, err := f.AppendGroup([]Pending{
		{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("in-1")},
		{Shard: 1, Kind: KindLog, ShardSeq: 1, Payload: []byte("in-2")},
	}); err != nil {
		t.Fatalf("append pre-barrier: %v", err)
	}
	shards := []BarrierShard{{TailSeg: 0x1000, TailSeq: 1}, {TailSeg: 0x2000, TailSeq: 2}}
	_, wbar := appendBarrier(t, f, shards)
	if _, err := f.AppendGroup([]Pending{
		{Shard: 0, Kind: KindLog, ShardSeq: 2, Payload: []byte("out-1")},
		{Shard: 1, Kind: KindLog, ShardSeq: 2, Payload: []byte("out-2")},
	}); err != nil {
		t.Fatalf("append post-barrier: %v", err)
	}

	var seen []string
	size, _ := dev.Size()
	hdr, rows, err := ReplayToBarrier(dev, prefix, PageSize, uint64(size), wbar, func(_ uint64, h *SegHeader, payload []byte) error {
		if h.GlobalSeq > wbar {
			t.Fatalf("visited a segment past the cut: seq %d", h.GlobalSeq)
		}
		seen = append(seen, string(payload))
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(seen) != 2 || seen[0] != "in-1" || seen[1] != "in-2" {
		t.Fatalf("replayed %v, want the two pre-barrier records", seen)
	}
	if hdr.Wbar != wbar || len(rows) != 2 {
		t.Fatalf("barrier = %+v with %d shards, want wbar %d 2 shards", hdr, len(rows), wbar)
	}
}

// TestReplayToBarrierNoBarrier refuses to replay a stream that holds no barrier at the
// asked watermark: without a cut line there is no image to materialize.
func TestReplayToBarrierNoBarrier(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("x")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	size, _ := dev.Size()
	if _, _, err := ReplayToBarrier(dev, prefix, PageSize, uint64(size), 42, nil); err != ErrNoBarrier {
		t.Fatalf("err = %v, want ErrNoBarrier", err)
	}
}

// TestReplayToBarrierRejectsInconsistent refuses to replay against a barrier whose
// shard seq outruns its watermark: a forged or torn cut fails before a record is read.
func TestReplayToBarrierRejectsInconsistent(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	// The barrier lands at global_seq 1; a shard seq of 5 cannot arise below Wbar 1.
	payload := encodeBarrier(BarrierHeader{Wbar: 1, ShardCount: 1}, []BarrierShard{{TailSeq: 5}})
	if _, err := f.AppendGroup([]Pending{{Shard: ShardOwnerless, Kind: KindBarrier, Payload: payload}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	size, _ := dev.Size()
	if _, _, err := ReplayToBarrier(dev, prefix, PageSize, uint64(size), 1, nil); err != ErrBarrier {
		t.Fatalf("err = %v, want ErrBarrier", err)
	}
}

// TestReplayToBarrierPropagatesVisitError fails the whole replay when a consumer cannot
// apply a committed record, so a restore never silently drops data the image holds.
func TestReplayToBarrierPropagatesVisitError(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("boom")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	appendBarrier(t, f, []BarrierShard{{TailSeq: 1}})

	boom := errors.New("consumer refused")
	size, _ := dev.Size()
	_, _, err := ReplayToBarrier(dev, prefix, PageSize, uint64(size), f.GlobalSeq(), func(_ uint64, _ *SegHeader, _ []byte) error {
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the consumer error", err)
	}
}
