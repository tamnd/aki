package akifile

// The forkless snapshot coordinator's emit path (spec 2064/f3/07 section 5, "The
// forkless log-watermark snapshot protocol"). A snapshot is cut by writing one barrier
// segment into the normal stream: the writer assigns it the next global_seq as the
// watermark Wbar, and the payload records where every shard's log tail sat at that
// instant. The image is every record with global_seq <= Wbar plus the checkpoints, and
// ReplayToBarrier restores it. This is the write half; FindBarrier and ReplayToBarrier
// are the read half.

// WriteBarrier emits a snapshot barrier and returns its segment offset and watermark. It
// assigns the barrier the next global_seq the writer would hand out as its Wbar, frames
// the caller's per-shard tail positions under that watermark, and appends it as one
// owner-less barrier segment group-committed like any other write, so the cut sits in the
// same total order it bounds.
//
// It refuses to emit an inconsistent cut. A shard whose recorded tail seq outruns Wbar
// cannot arise from the single writer's total order, so WriteBarrier returns ErrBarrier
// before writing rather than laying down a barrier a later restore would reject. Because
// the watermark is the next seq and every real shard tail is at or below it by
// construction, a genuine cut always passes.
func (f *File) WriteBarrier(shards []BarrierShard) (off, wbar uint64, err error) {
	wbar = f.globalSeq + 1
	h := BarrierHeader{Wbar: wbar, ShardCount: uint64(len(shards))}
	if !BarrierConsistent(h, shards) {
		return 0, 0, ErrBarrier
	}
	offs, err := f.AppendGroup([]Pending{{
		Shard:   ShardOwnerless,
		Kind:    KindBarrier,
		Payload: MarshalBarrier(h, shards),
	}})
	if err != nil {
		return 0, 0, err
	}
	return offs[0], wbar, nil
}

// CommitSnapshotRoot installs the SRT as the root of the point-in-time snapshot cut at
// wbar (spec 2064/f3/07 section 5, protocol step 3). Once each shard has cut a checkpoint
// covering the barrier and the caller has assembled those roots into srt, this stamps the
// snapshot flag and the watermark into the table and commits it through the ordinary
// checkpoint meta flip. The live meta -> SRT chain then names the cut, so the copy path
// reads Wbar from the SRT header without scanning the log, and the 128-byte meta slot is
// left untouched.
//
// A snapshot root must name a real watermark: wbar zero is the ordinary-table sentinel a
// reader uses to tell a plain root from a snapshot root, so CommitSnapshotRoot refuses a
// zero wbar with ErrSnapshotWatermark. Every genuine cut takes the next global_seq, which
// is at least one, so a real snapshot always passes.
func (f *File) CommitSnapshotRoot(srt *SRT, extents []Extent, stats CheckpointStats, globals CheckpointGlobals, wbar uint64) error {
	if wbar == 0 {
		return ErrSnapshotWatermark
	}
	srt.Flags |= SRTSnapshotRoot
	srt.SnapWbar = wbar
	return f.CheckpointWithGlobals(srt, extents, stats, globals)
}
