package akifile

// The cold-chunk region on the File: the cold tier's counterpart to the
// value-log region (valueregion.go). The migrator demotes whole records out of
// a shard's arena into self-delimiting cold frames (the store's coldframe
// codec, spec 2064/f3/06 section 2.3), and this is where a batch of those
// frames lands in the .aki append space instead of a per-shard scratch file.
//
// It is a distinct segment kind from the value log on purpose. A value_log
// segment holds value runs a resident record still points at, and CompactValues
// rewrites those runs forward. A cold_chunk segment holds whole cold frames the
// tier-tagged index names directly, so its liveness is Bitcask-style (the index
// names a frame or it is dead) and its reclaim is the migrator's, not the value
// log's. Sharing a kind would let a value-log rewrite drop cold frames, which is
// exactly why the scratch tiers were separate files (cold.go).
//
// A cold frame is opaque to this layer but self-delimiting: its leading u32 is
// the whole frame's byte length (the store's coldframe total field), so both a
// point read and the recovery walk re-derive the next boundary with no index.
// The segment payload CRC the group-commit writer stamps is the torn guard, so
// no per-frame checksum rides here; a frame reaches a walk visit only from a
// segment whose payload already verified.
//
// This is the codec's first consumer but still store-agnostic beyond the total
// field the file format itself commits to: it turns frames into offsets and
// offsets back into frames against the File. The slice that redirects the
// store's per-shard cold log here is the next consumer up.

// coldFrameTotal reads a cold frame's leading u32 length at off in a cold_chunk
// payload, the byte count of the whole frame including the length field. It is
// the one field this layer reads inside an otherwise opaque frame, the self
// delimiter a linear walk steps by.
func coldFrameTotal(payload []byte, off uint64) (uint64, error) {
	if off+4 > uint64(len(payload)) {
		return 0, ErrShort
	}
	return uint64(le.Uint32(payload[off:])), nil
}

// AppendColdFrames writes a batch of whole cold frames into one cold_chunk
// segment owned by shard through the group-commit writer (one header, one fsync
// for the batch) and returns the absolute file offset of each frame in order.
// The frames pack contiguously, so one segment holds a whole migration quantum
// and the offset a tier entry keeps points straight at the frame's first byte,
// where a later point read starts. An empty batch writes no segment.
func (f *File) AppendColdFrames(shard uint16, shardSeq uint64, frames [][]byte) ([]uint64, error) {
	if len(frames) == 0 {
		return nil, nil
	}
	var payload []byte
	offs := make([]uint64, len(frames))
	for i, fr := range frames {
		offs[i] = uint64(len(payload))
		payload = append(payload, fr...)
	}
	segOffs, err := f.AppendGroup([]Pending{{
		Shard:    shard,
		Kind:     KindColdChunk,
		ShardSeq: shardSeq,
		Payload:  payload,
	}})
	if err != nil {
		return nil, err
	}
	base := segOffs[0] + SegHeaderLen
	for i := range offs {
		offs[i] += base
	}
	return offs, nil
}

// ReadColdFrame reads the whole cold frame at absolute offset off: one pread of
// the leading u32 to learn the frame's total, then a pread of that many bytes,
// the two-pread cold read doc 06 section 2.3 prices. dst is reused when it has
// the room. The returned slice is the frame verbatim for the store's coldframe
// codec to parse; a total below the length field itself is a corrupt pointer and
// returns ErrLength.
func (f *File) ReadColdFrame(off uint64, dst []byte) ([]byte, error) {
	var h [4]byte
	if _, err := f.dev.ReadAt(h[:], int64(off)); err != nil {
		return nil, err
	}
	total := int(le.Uint32(h[:]))
	if total < 4 {
		return nil, ErrLength
	}
	if cap(dst) < total {
		dst = make([]byte, total)
	}
	dst = dst[:total]
	if _, err := f.dev.ReadAt(dst, int64(off)); err != nil {
		return nil, err
	}
	return dst, nil
}

// WalkColdFrames walks every cold_chunk segment in the append space from `from`
// up to the durable tail and calls visit for each frame with its absolute offset
// and bytes, the enumerate side the tier-tagged index rebuild and the migrator's
// reclaim both take. It reuses ReplayTail's tail walk, so it stops exactly where
// recovery would resume, and only descends into cold_chunk segments, skipping the
// log, value-log, and checkpoint segments interleaved in the same space. A frame
// whose total runs past the payload stops the walk with an error, the durable cut
// a recovering reader takes; a visit that errors stops it too and the error
// propagates. The frame slice aliases the payload buffer, valid only for the call.
func (f *File) WalkColdFrames(from uint64, visit func(off uint64, frame []byte) error) error {
	_, err := ReplayTail(f.dev, f.prefix, from, f.cursor, func(off uint64, h *SegHeader, payload []byte) error {
		if h.Kind != KindColdChunk {
			return nil
		}
		base := off + SegHeaderLen
		for cur := uint64(0); cur < uint64(len(payload)); {
			total, err := coldFrameTotal(payload, cur)
			if err != nil {
				return err
			}
			if total < 4 || cur+total > uint64(len(payload)) {
				return ErrShort
			}
			if err := visit(base+cur, payload[cur:cur+total]); err != nil {
				return err
			}
			cur += total
		}
		return nil
	})
	return err
}
