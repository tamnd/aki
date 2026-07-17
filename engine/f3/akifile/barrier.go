package akifile

// The barrier codec (spec 2064/f3/07 section 5, "The forkless log-watermark snapshot
// protocol"). A barrier segment is the cut line of a point-in-time snapshot: the
// coordinator asks the writer to emit it, the writer assigns it global_seq = Wbar in
// the normal stream and fsyncs its group, and the payload records Wbar and every
// shard's tail position at that instant. The snapshot image is every record with
// global_seq <= Wbar plus the checkpoints, and restoring it is recovery with a stop
// line at Wbar (test T7).
//
// Wbar is a genuine global cut because global_seq is assigned by the one writer in a
// total order across shards, so inclusion is a single integer comparison. Each shard
// row records where that shard's log tail sat at the barrier and the highest stamp it
// had received, both at or below Wbar by construction; a row whose seq outruns Wbar is
// impossible, which BarrierConsistent checks.
//
// A barrier is a one-shot event, written once and never updated, so the payload is
// flat like the free map, not a full-or-delta accumulator. It carries no checksum of
// its own; the barrier segment header's payload CRC covers it, so the BAR3 magic is a
// kind cross-check, not an integrity field. Codec only: it frames into and reads out
// of a caller-owned payload and never touches a File. The coordinator that drives the
// snapshot protocol and the copy path that walks the file to Wbar are separate slices.

// BarrierMagic is the barrier payload sentinel.
const BarrierMagic = "BAR3"

const (
	// BarrierHeaderLen is the fixed barrier header size.
	BarrierHeaderLen = 24
	// BarrierShardSize is one shard's tail position: tail_seg u64 and tail_seq u64.
	BarrierShardSize = 16
)

// BarrierHeader records the snapshot watermark and the shard count whose tail
// positions follow. Wbar is the global_seq the writer assigned this barrier, the cut
// every record is compared against.
type BarrierHeader struct {
	Wbar       uint64 // the barrier's own global_seq, the snapshot cut line
	ShardCount uint64 // per-shard tail rows that follow the header
}

// BarrierShard is one shard's tail position at the barrier instant: where its log
// chain ended and the highest global_seq it had received, both at or below Wbar.
type BarrierShard struct {
	TailSeg uint64 // offset of the shard's last segment at the barrier
	TailSeq uint64 // the shard's highest global_seq at or below Wbar
}

// AppendBarrierHeader frames a barrier header onto dst. Shard rows follow with
// AppendBarrierShard, one per shard in shard order.
func AppendBarrierHeader(dst []byte, h BarrierHeader) []byte {
	var b [BarrierHeaderLen]byte
	copy(b[0:4], BarrierMagic)
	// b[4:8] reserved, left zero.
	le.PutUint64(b[8:16], h.Wbar)
	le.PutUint64(b[16:24], h.ShardCount)
	return append(dst, b[:]...)
}

// AppendBarrierShard frames one 16-byte shard tail row onto dst.
func AppendBarrierShard(dst []byte, s BarrierShard) []byte {
	var b [BarrierShardSize]byte
	le.PutUint64(b[0:8], s.TailSeg)
	le.PutUint64(b[8:16], s.TailSeq)
	return append(dst, b[:]...)
}

// MarshalBarrier frames a whole barrier payload: the header followed by one tail row per
// shard in shard order, the bytes a barrier segment carries. It is the emit-side inverse
// of ParseBarrierHeader plus BarrierShards, so the coordinator frames a cut with one call
// the way MarshalExtents frames the extent map.
func MarshalBarrier(h BarrierHeader, shards []BarrierShard) []byte {
	payload := AppendBarrierHeader(nil, h)
	for _, s := range shards {
		payload = AppendBarrierShard(payload, s)
	}
	return payload
}

// ParseBarrierHeader decodes and validates a barrier header: only the magic, since
// the header carries no invariant beyond its watermark and count.
func ParseBarrierHeader(b []byte) (BarrierHeader, error) {
	if len(b) < BarrierHeaderLen {
		return BarrierHeader{}, ErrShort
	}
	if string(b[0:4]) != BarrierMagic {
		return BarrierHeader{}, ErrMagic
	}
	return BarrierHeader{
		Wbar:       le.Uint64(b[8:16]),
		ShardCount: le.Uint64(b[16:24]),
	}, nil
}

// BarrierShards decodes every shard tail row in a barrier payload after its header,
// the load path a snapshot restore uses to bound each shard's replay. It bounds
// shard_count against the payload so a corrupt count cannot over-read; a count that
// outruns the bytes is ErrLength.
func BarrierShards(payload []byte, h BarrierHeader) ([]BarrierShard, error) {
	if uint64(len(payload)) < BarrierHeaderLen {
		return nil, ErrShort
	}
	avail := (uint64(len(payload)) - BarrierHeaderLen) / BarrierShardSize
	if h.ShardCount > avail {
		return nil, ErrLength
	}
	shards := make([]BarrierShard, h.ShardCount)
	off := uint64(BarrierHeaderLen)
	for i := range shards {
		shards[i] = BarrierShard{
			TailSeg: le.Uint64(payload[off : off+8]),
			TailSeq: le.Uint64(payload[off+8 : off+16]),
		}
		off += BarrierShardSize
	}
	return shards, nil
}

// BarrierConsistent reports whether a barrier is a genuine cut: every shard's tail
// seq is at or below Wbar. A row whose seq outruns the watermark cannot arise from
// the single writer's total order, so it marks a corrupt or forged barrier a restore
// must refuse.
func BarrierConsistent(h BarrierHeader, shards []BarrierShard) bool {
	for _, s := range shards {
		if s.TailSeq > h.Wbar {
			return false
		}
	}
	return true
}
