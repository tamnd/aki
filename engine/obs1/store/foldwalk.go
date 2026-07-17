package store

// The segment folder's window onto the staged cold stream. This file is
// obs1-only, not an f3 port: f3 consumes a staged drain through the pwrite
// seam alone, while the obs1 fold pass (spec 2064/obs1 doc 06 section 1)
// reads the same bytes to pack bucket segments, so the frame codecs stay
// unexported and the folder walks them through here.

// ChunkKindBit is the high bit of a frame's kind byte, set on a packed
// collection chunk and clear on a whole record, the same dispatch a
// recovery walk uses. A folder that wraps whole-record runs into chunk
// frames of its own sets it the same way, so segment chunks and
// cold-region chunks parse alike.
const ChunkKindBit = frameChunk

// FoldFrame is one staged frame as the folder sees it. Key, Disc, Payload,
// and Frame alias the drain buffer and stay valid only until the drain
// completes; a consumer that outlives the call copies out, exactly as the
// migrator does.
type FoldFrame struct {
	// Kind is the frame's kind byte verbatim: a record kind on a whole
	// record, the collection kind with ChunkKindBit set on a packed chunk.
	Kind  byte
	Flags byte

	// Chunk marks a packed collection chunk; Count is its element count,
	// 1 on a whole record. Disc is the chunk's first discriminator, nil
	// on a whole record.
	Chunk bool
	Count uint16
	Disc  []byte

	// Pointer marks a whole record whose value region is a band pointer
	// into the local value log, not the value bytes; its bytes are not in
	// the frame, so a fold routes it separately or skips it.
	Pointer bool

	Key []byte

	// Frame is the whole self-delimiting frame, leading total included,
	// and Payload is its value region (a whole record) or packed blob (a
	// chunk).
	Frame   []byte
	Payload []byte
}

// WalkStagedFrames scans buf, a staged drain buffer, and calls fn for each
// frame in stage order. It dispatches on ChunkKindBit exactly as the
// recovery walk does, and a torn or corrupt frame stops the walk with the
// codec's error. A non-nil error from fn stops the walk and is returned.
func WalkStagedFrames(buf []byte, fn func(FoldFrame) error) error {
	for len(buf) > 0 {
		if len(buf) < coldHdr {
			return errColdShort
		}
		var out FoldFrame
		var n int
		if buf[4]&frameChunk != 0 {
			f, adv, err := decodeChunkFrame(buf)
			if err != nil {
				return err
			}
			out = FoldFrame{
				Kind: f.kind, Flags: f.flags, Chunk: true, Count: f.count,
				Disc: f.disc, Key: f.key, Frame: buf[:adv], Payload: f.payload,
			}
			n = adv
		} else {
			f, adv, err := decodeColdFrame(buf)
			if err != nil {
				return err
			}
			out = FoldFrame{
				Kind: f.kind, Flags: f.flags, Count: 1,
				Pointer: f.flags&(flagSep|flagChunked) != 0,
				Key:     f.key, Frame: buf[:adv], Payload: f.value,
			}
			n = adv
		}
		if err := fn(out); err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}

// SetFoldTap registers fn to hear every staged drain buffer, called on the
// owner goroutine after the stage pass is final and before the pwrite, so
// an eligibility mark taken inside fn covers every mutation the frames
// reflect. The buffer is recycled when the drain completes; fn copies what
// it keeps. Fix it before the store serves, like every owner seam.
func (s *Store) SetFoldTap(fn func(frames []byte)) { s.foldTap = fn }

// AppendRecordFrame writes one whole-record cold frame onto dst, the
// folder-side counterpart of the migrator's framing: run-chunk payloads
// and fold tests build frames byte-identical to staged ones.
func AppendRecordFrame(dst []byte, kind, flags byte, vlen uint32, key, value []byte) []byte {
	return appendColdFrame(dst, kind, flags, vlen, key, value)
}

// AppendRunChunk writes one packed chunk frame onto dst, the same codec
// the demoter writes for collection chunks. The folder wraps a run of
// whole-record frames this way (doc 08 section 2): kind carries the run's
// record kind with ChunkKindBit set, count the record count, disc the run's
// first key fingerprint, and payload the concatenated record frames.
func AppendRunChunk(dst []byte, kind, flags byte, count uint16, key, disc, payload []byte) []byte {
	return appendChunkFrame(dst, kind, flags, count, key, disc, payload)
}
