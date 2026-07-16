package akifile

import "testing"

// appendBarrier frames a barrier over shards and appends it as a barrier segment,
// stamping the payload Wbar with the global_seq the writer is about to assign so the
// segment's own seq and the recorded watermark agree, the way the coordinator writes
// it. It returns the segment offset and the barrier's Wbar.
func appendBarrier(t *testing.T, f *File, shards []BarrierShard) (uint64, uint64) {
	t.Helper()
	wbar := f.GlobalSeq() + 1
	payload := encodeBarrier(BarrierHeader{Wbar: wbar, ShardCount: uint64(len(shards))}, shards)
	offs, err := f.AppendGroup([]Pending{{Shard: ShardOwnerless, Kind: KindBarrier, Payload: payload}})
	if err != nil {
		t.Fatalf("append barrier: %v", err)
	}
	return offs[0], wbar
}

// TestFindBarrierAtWatermark walks the stream past ordinary log segments to the
// snapshot barrier and reads back its cut: the offset, the watermark, and every
// shard's tail position.
func TestFindBarrierAtWatermark(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if _, err := f.AppendGroup([]Pending{
		{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("a")},
		{Shard: 1, Kind: KindLog, ShardSeq: 1, Payload: []byte("b")},
	}); err != nil {
		t.Fatalf("append log: %v", err)
	}
	shards := []BarrierShard{{TailSeg: 0x1000, TailSeq: 1}, {TailSeg: 0x2000, TailSeq: 2}}
	barOff, wbar := appendBarrier(t, f, shards)

	size, _ := dev.Size()
	h, rows, at, err := FindBarrier(dev, prefix, PageSize, uint64(size), wbar)
	if err != nil {
		t.Fatalf("find barrier: %v", err)
	}
	if at != barOff || h.Wbar != wbar || h.ShardCount != 2 {
		t.Fatalf("barrier = @%d %+v, want @%d wbar %d 2 shards", at, h, barOff, wbar)
	}
	for i := range shards {
		if rows[i] != shards[i] {
			t.Fatalf("shard %d = %+v, want %+v", i, rows[i], shards[i])
		}
	}
}

// TestFindBarrierNotFound scans a stream that holds no barrier at the asked watermark
// and reports ErrNoBarrier rather than a torn cut.
func TestFindBarrierNotFound(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("a")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	appendBarrier(t, f, []BarrierShard{{TailSeq: 1}})

	size, _ := dev.Size()
	if _, _, _, err := FindBarrier(dev, prefix, PageSize, uint64(size), 999); err != ErrNoBarrier {
		t.Fatalf("err = %v, want ErrNoBarrier", err)
	}
}

// TestFindBarrierRejectsSeqPastWbar refuses a barrier whose watermark matches but a
// shard seq outruns it, a cut the single writer's total order cannot produce.
func TestFindBarrierRejectsSeqPastWbar(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	// One prior segment, so the barrier lands at global_seq 2; give a shard seq of 3.
	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("a")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	_, wbar := appendBarrier(t, f, []BarrierShard{{TailSeq: wbarPlus(f)}})

	size, _ := dev.Size()
	if _, _, _, err := FindBarrier(dev, prefix, PageSize, uint64(size), wbar); err != ErrBarrier {
		t.Fatalf("err = %v, want ErrBarrier", err)
	}
}

// wbarPlus is the seq one past the barrier's own watermark: the writer has already
// bumped global_seq for the barrier, so this is a shard tail that outruns the cut.
func wbarPlus(f *File) uint64 { return f.GlobalSeq() + 2 }

// TestFindBarrierRejectsSeqMismatch refuses a barrier whose recorded Wbar disagrees
// with the segment's own global_seq: the payload claims a watermark the writer never
// stamped on the frame, a forged or corrupt cut.
func TestFindBarrierRejectsSeqMismatch(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	// The barrier lands at global_seq 1 but its payload claims Wbar 5.
	payload := encodeBarrier(BarrierHeader{Wbar: 5, ShardCount: 1}, []BarrierShard{{TailSeq: 1}})
	if _, err := f.AppendGroup([]Pending{{Shard: ShardOwnerless, Kind: KindBarrier, Payload: payload}}); err != nil {
		t.Fatalf("append: %v", err)
	}

	size, _ := dev.Size()
	if _, _, _, err := FindBarrier(dev, prefix, PageSize, uint64(size), 5); err != ErrBarrier {
		t.Fatalf("err = %v, want ErrBarrier on a seq mismatch", err)
	}
}
