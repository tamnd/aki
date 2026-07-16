package sqlo1

// Rope constants and the rope root payload codec, doc 05 section 1.1.
// A rope is the third rung of the string representation ladder: the
// value lives as fixed-size chunk segments under a minted rooth, and
// the record under the user key holds only this root payload. Chunk
// addressing is pure arithmetic (byte B lives in chunk B>>log2chunk),
// so no fence table exists anywhere in the string model.

import (
	"encoding/binary"
	"fmt"
)

const (
	// DefaultLog2Chunk is the chunk size exponent new ropes are built
	// with: 13 (8 KiB) per the ropechunk lab's local provisional
	// verdict (results/sqlo1/t1-ropechunk-local.md), which rejected
	// the doc 05 prior of 15 on every mix and both arms. This is a
	// lab-movable default, not a format fact: every root records its
	// own log2chunk, so the gate-box rerun can move the default
	// without touching stored data.
	DefaultLog2Chunk = 13

	// DefaultRopeMin is the ladder's blob-to-rope boundary: a value at
	// or past it is written as a rope. Lab-swept alongside ropechunk;
	// doc 05 section 1 fixes the default at 1 MiB.
	DefaultRopeMin = 1 << 20

	// MaxValueLen is the Redis string value cap, doc 05 section 1.
	MaxValueLen = 512 << 20

	// ropeSub is the sub-representation byte a string rope root
	// carries; other string subs are unassigned in v0.
	ropeSub = 1

	// ropeRootLen is the encoded root payload size.
	ropeRootLen = 40

	// chunkKind is the subkey kind of rope chunk segments; kind 2 is
	// reserved for the popcount cache segments (doc 05 section 3.2).
	chunkKind = 1

	// minLog2Chunk and maxLog2Chunk bound what the codec accepts.
	// They are structural sanity limits, far wider than any default
	// the labs would pick, so a moved default never strands old data.
	minLog2Chunk = 6
	maxLog2Chunk = 30
)

// ropeRoot is the decoded rope root payload. rooth lives in the
// payload per doc 03 section 6.3: it is minted, not derived from the
// key, so a future RENAME moves the root record and every chunk
// subkey keeps resolving.
type ropeRoot struct {
	log2chunk  uint8
	rootgen    uint32
	rooth      uint64
	totalLen   uint64
	chunkCount uint64
	pcSegCount uint64
}

// chunkSize returns the rope's fixed chunk size in bytes.
func (r ropeRoot) chunkSize() uint64 {
	return 1 << r.log2chunk
}

// appendRopeRoot encodes r onto dst: sub, log2chunk, reserved,
// rootgen, rooth, total_len, chunk_count, pc_seg_count, all
// little-endian.
func appendRopeRoot(dst []byte, r ropeRoot) []byte {
	var b [ropeRootLen]byte
	b[0] = ropeSub
	b[1] = r.log2chunk
	binary.LittleEndian.PutUint32(b[4:], r.rootgen)
	binary.LittleEndian.PutUint64(b[8:], r.rooth)
	binary.LittleEndian.PutUint64(b[16:], r.totalLen)
	binary.LittleEndian.PutUint64(b[24:], r.chunkCount)
	binary.LittleEndian.PutUint64(b[32:], r.pcSegCount)
	return append(dst, b[:]...)
}

// decodeRopeRoot parses and validates a rope root payload. Everything
// checkable without touching chunks is checked here, so a corrupt
// root fails loudly at the root read instead of as a wrong-sized
// assembly later.
func decodeRopeRoot(p []byte) (ropeRoot, error) {
	if len(p) == 0 || p[0] != ropeSub {
		// Another type's root is not corruption, it is the caller
		// operating on the wrong kind of key; the sub namespace is
		// global (hash.go), so the sniff settles which before the
		// length check can mistake a foreign payload for a torn rope.
		if tag, _, err := sniffRoot(p); err == nil && tag != TagString {
			return ropeRoot{}, ErrWrongType
		}
		if len(p) == 0 {
			return ropeRoot{}, fmt.Errorf("sqlo1: empty root payload")
		}
		return ropeRoot{}, fmt.Errorf("sqlo1: string root sub %d is not a rope", p[0])
	}
	if len(p) != ropeRootLen {
		return ropeRoot{}, fmt.Errorf("sqlo1: rope root payload of %d bytes, want %d", len(p), ropeRootLen)
	}
	if p[2] != 0 || p[3] != 0 {
		return ropeRoot{}, fmt.Errorf("sqlo1: rope root reserved bytes are set")
	}
	r := ropeRoot{
		log2chunk:  p[1],
		rootgen:    binary.LittleEndian.Uint32(p[4:]),
		rooth:      binary.LittleEndian.Uint64(p[8:]),
		totalLen:   binary.LittleEndian.Uint64(p[16:]),
		chunkCount: binary.LittleEndian.Uint64(p[24:]),
		pcSegCount: binary.LittleEndian.Uint64(p[32:]),
	}
	if r.log2chunk < minLog2Chunk || r.log2chunk > maxLog2Chunk {
		return ropeRoot{}, fmt.Errorf("sqlo1: rope log2chunk %d outside [%d, %d]", r.log2chunk, minLog2Chunk, maxLog2Chunk)
	}
	if r.rootgen == 0 {
		return ropeRoot{}, fmt.Errorf("sqlo1: rope root with generation zero")
	}
	if r.totalLen == 0 {
		return ropeRoot{}, fmt.Errorf("sqlo1: rope root with zero length")
	}
	if want := (r.totalLen + r.chunkSize() - 1) >> r.log2chunk; r.chunkCount != want {
		return ropeRoot{}, fmt.Errorf("sqlo1: rope of %d bytes at log2chunk %d claims %d chunks, want %d", r.totalLen, r.log2chunk, r.chunkCount, want)
	}
	if r.chunkCount > maxSegid {
		return ropeRoot{}, fmt.Errorf("sqlo1: rope chunk count %d exceeds the segid space", r.chunkCount)
	}
	if want := (r.chunkCount + pcChunksPerSeg - 1) / pcChunksPerSeg; r.pcSegCount != 0 && r.pcSegCount != want {
		return ropeRoot{}, fmt.Errorf("sqlo1: rope of %d chunks claims %d popcount segments, want 0 or %d", r.chunkCount, r.pcSegCount, want)
	}
	return r, nil
}

// putChunkKey writes the subkey of chunk segid under rooth into
// dst[:SubkeySize], the doc 03 6.3 layout Subkey.Encode produces;
// rope_test pins the equivalence. A shared buffer works because every
// seam door copies key bytes before returning.
func putChunkKey(dst []byte, rooth, segid uint64) {
	binary.LittleEndian.PutUint64(dst, rooth)
	dst[8] = chunkKind
	var seg [8]byte
	binary.LittleEndian.PutUint64(seg[:], segid)
	copy(dst[9:SubkeySize], seg[:7])
}
