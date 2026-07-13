package stream

import "testing"

// The gc rewrite of partially-tombstoned sealed blocks (spec 2064/f3/14 section 6.5),
// as white-box invariants over a directly built native stream, so the test drives
// s.gc() itself rather than waiting on the worker idle boundary the command path runs
// it from. A native stream packs 128 fixed-schema entries per block, so appending 300
// entries yields the block layout [1..128][129..256][257..300], the middle one sealed,
// the last one the open tail gc never touches.

// mkNative builds a native stream holding entries with IDs ms=1..n, seq=0, each the
// same "f"/"v" schema, so the fixed-schema packing gives predictable 128-entry blocks.
// n above the inline cap forces the native band.
func mkNative(t *testing.T, n int) *stream {
	t.Helper()
	s := newStream()
	for i := 1; i <= n; i++ {
		s.appendEntry(streamID{ms: uint64(i)}, []field{{name: []byte("f"), value: []byte("v")}})
	}
	if s.kind != bandNative {
		t.Fatalf("stream stayed inline at n=%d, want native", n)
	}
	return s
}

// liveIDs walks every block and returns the live entry IDs (ms values) in stream
// order, the ground truth the directory reads must reproduce.
func liveIDs(s *stream) []uint64 {
	var out []uint64
	lo := bound{id: streamID{}}
	hi := bound{id: streamID{ms: ^uint64(0), seq: ^uint64(0)}}
	for _, e := range s.collectRange(lo, hi, false, -1) {
		out = append(out, e.id.ms)
	}
	return out
}

// tombstoneMS tombstones the entries with the given ms values and asserts each removed
// a live one, keeping s.length in step so the block counters match the reads.
func tombstoneMS(t *testing.T, s *stream, ms ...uint64) {
	t.Helper()
	for _, m := range ms {
		if !s.delete(streamID{ms: m}) {
			t.Fatalf("delete(%d) removed nothing, expected a live entry", m)
		}
	}
}

// TestGcRewritesPartiallyDeadSealedBlock pins the core rewrite: a sealed block whose
// dead fraction reaches the ratio is re-encoded to its live entries, its bytes shrink,
// its firstID advances when the master was tombstoned, and every read still resolves.
func TestGcRewritesPartiallyDeadSealedBlock(t *testing.T) {
	s := mkNative(t, 300)
	first := s.blocks[0]
	if first.count != 128 {
		t.Fatalf("block 0 count = %d, want 128", first.count)
	}
	// Tombstone every odd ms in block 0 (1,3,..,127): 64 of 128, exactly the ratio,
	// and interleaved so the survivors are not a prefix and the master (ms=1) dies.
	var kill []uint64
	for m := uint64(1); m <= 127; m += 2 {
		kill = append(kill, m)
	}
	tombstoneMS(t, s, kill...)
	beforeSize := first.size()

	st := s.gc()
	if st.rewritten != 1 || st.dropped != 0 {
		t.Fatalf("gc stats = %+v, want 1 rewritten 0 dropped", st)
	}
	if st.reclaimed <= 0 {
		t.Fatalf("gc reclaimed %d bytes, want > 0", st.reclaimed)
	}
	nb := s.blocks[0]
	if nb.count != 64 || nb.deleted != 0 {
		t.Fatalf("rewritten block count=%d deleted=%d, want 64/0", nb.count, nb.deleted)
	}
	if nb.size() >= beforeSize {
		t.Fatalf("rewrite did not shrink block: %d -> %d", beforeSize, nb.size())
	}
	// The master (ms=1) was tombstoned, so the first survivor (ms=2) is the new first.
	if nb.first != (streamID{ms: 2}) {
		t.Fatalf("rewritten firstID = %v, want 2-0", nb.first)
	}
	// The directory still points ms=2 at physical block 0, and the later blocks stay
	// put at their logical refs.
	if got := s.floorBlock(streamID{ms: 2}); got != 0 {
		t.Fatalf("floorBlock(2) = %d, want 0 after firstID advance", got)
	}
	if got := s.floorBlock(streamID{ms: 128}); got != 0 {
		t.Fatalf("floorBlock(128) = %d, want 0", got)
	}
	if got := s.floorBlock(streamID{ms: 129}); got != 1 {
		t.Fatalf("floorBlock(129) = %d, want 1", got)
	}
	// The reads reproduce exactly the survivors: 2,4,..,128 then 129..300.
	want := make([]uint64, 0, int(s.length))
	for m := uint64(2); m <= 128; m += 2 {
		want = append(want, m)
	}
	for m := uint64(129); m <= 300; m++ {
		want = append(want, m)
	}
	assertMS(t, liveIDs(s), want)
	if uint64(len(want)) != s.length {
		t.Fatalf("s.length = %d, want %d", s.length, len(want))
	}
}

// TestGcDropsFullyDeadSealedBlock pins that a sealed block gone fully dead is removed
// whole (no rewrite) and the directory is rebuilt so later blocks still resolve.
func TestGcDropsFullyDeadSealedBlock(t *testing.T) {
	s := mkNative(t, 300)
	var kill []uint64
	for m := uint64(1); m <= 128; m++ { // all of block 0
		kill = append(kill, m)
	}
	tombstoneMS(t, s, kill...)
	if s.blocks[0].live() != 0 {
		t.Fatalf("block 0 live = %d, want 0", s.blocks[0].live())
	}

	st := s.gc()
	if st.dropped != 1 || st.rewritten != 0 {
		t.Fatalf("gc stats = %+v, want 0 rewritten 1 dropped", st)
	}
	if len(s.blocks) != 2 {
		t.Fatalf("blocks after drop = %d, want 2", len(s.blocks))
	}
	if s.base != 0 {
		t.Fatalf("base after interior drop = %d, want 0 (dir rebuilt)", s.base)
	}
	// The former block 1 ([129..256]) is now physical and logical block 0.
	if got := s.floorBlock(streamID{ms: 129}); got != 0 {
		t.Fatalf("floorBlock(129) = %d, want 0 after drop", got)
	}
	if got := s.floorBlock(streamID{ms: 257}); got != 1 {
		t.Fatalf("floorBlock(257) = %d, want 1 after drop", got)
	}
	want := make([]uint64, 0, int(s.length))
	for m := uint64(129); m <= 300; m++ {
		want = append(want, m)
	}
	assertMS(t, liveIDs(s), want)
}

// TestGcLeavesBelowRatioBlock pins that a sealed block below the gc ratio is left
// alone: the reclaim would not pay for the copy, so no rewrite fires.
func TestGcLeavesBelowRatioBlock(t *testing.T) {
	s := mkNative(t, 300)
	var kill []uint64
	for m := uint64(1); m <= 40; m++ { // 40 of 128, below the 64 the 0.5 ratio needs
		kill = append(kill, m)
	}
	tombstoneMS(t, s, kill...)
	beforeSize := s.blocks[0].size()

	st := s.gc()
	if st.rewritten != 0 || st.dropped != 0 {
		t.Fatalf("gc touched a below-ratio block: %+v, want zero", st)
	}
	if s.blocks[0].size() != beforeSize || s.blocks[0].deleted != 40 {
		t.Fatalf("below-ratio block changed: size %d->%d deleted=%d", beforeSize, s.blocks[0].size(), s.blocks[0].deleted)
	}
}

// TestGcLeavesTailBlock pins that gc never rewrites or drops the open tail, even when
// it is fully tombstoned, since it is still filling.
func TestGcLeavesTailBlock(t *testing.T) {
	s := mkNative(t, 300)
	tail := s.blocks[len(s.blocks)-1] // [257..300]
	for m := uint64(257); m <= 300; m++ {
		tombstoneMS(t, s, m)
	}
	if tail.live() != 0 {
		t.Fatalf("tail live = %d, want 0", tail.live())
	}

	st := s.gc()
	if st.rewritten != 0 || st.dropped != 0 {
		t.Fatalf("gc touched the tail: %+v, want zero", st)
	}
	if len(s.blocks) != 3 || s.blocks[len(s.blocks)-1] != tail {
		t.Fatalf("tail block was moved or dropped")
	}
}

// TestGcInlineNoop pins that an inline stream, which compacts on trim, is a gc no-op.
func TestGcInlineNoop(t *testing.T) {
	s := newStream()
	for i := 1; i <= 5; i++ {
		s.appendEntry(streamID{ms: uint64(i)}, []field{{name: []byte("f"), value: []byte("v")}})
	}
	if s.kind != bandInline {
		t.Fatalf("stream upgraded early, want inline")
	}
	s.delete(streamID{ms: 2})
	if st := s.gc(); st != (gcStats{}) {
		t.Fatalf("gc on inline stream = %+v, want zero", st)
	}
}

// TestMaintainDrainsDirtyOnce pins the registry seam: markDirty enqueues a stream at
// most once, and maintain runs one gc pass per dirty stream then clears the flag and
// the worklist.
func TestMaintainDrainsDirtyOnce(t *testing.T) {
	g := &reg{m: make(map[string]*stream)}
	s := mkNative(t, 300)
	g.m["s"] = s
	var kill []uint64
	for m := uint64(1); m <= 70; m++ { // past the ratio so gc rewrites block 0
		kill = append(kill, m)
	}
	tombstoneMS(t, s, kill...)
	beforeSize := s.blocks[0].size()

	g.markDirty(s)
	g.markDirty(s) // a second mark before the pass must not double-enqueue
	if !s.gcDirty || len(g.dirty) != 1 {
		t.Fatalf("markDirty state = dirty:%v len:%d, want true/1", s.gcDirty, len(g.dirty))
	}

	g.maintain()
	if s.gcDirty || len(g.dirty) != 0 {
		t.Fatalf("after maintain dirty:%v len:%d, want false/0", s.gcDirty, len(g.dirty))
	}
	if s.blocks[0].size() >= beforeSize {
		t.Fatalf("maintain did not run gc: block size %d -> %d", beforeSize, s.blocks[0].size())
	}
	// A second maintain with nothing dirty is a cheap no-op that changes nothing.
	g.maintain()
	if len(g.dirty) != 0 {
		t.Fatalf("idle maintain grew dirty to %d", len(g.dirty))
	}
}

// assertMS fails unless got equals want element for element, the live ms sequence a
// read must return.
func assertMS(t *testing.T, got, want []uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("read %d entries, want %d\n got=%v\nwant=%v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d = %d, want %d", i, got[i], want[i])
		}
	}
}
