package store

import "encoding/binary"

// The store-side cold chunk plane (spec 2064/f3/06 sections 6.2 and 6.5): the
// append and read primitives a collection type drives to pack many ordered
// members into one cold frame and read them back. They sit over the same cold
// region the whole-record migrator uses (cold.go), under the same
// self-delimiting liveness rule, and they are type-agnostic: the store frames
// and returns opaque (kind, disc, payload) triples and never interprets the
// packed blob. The collection package owns the discriminator order and the
// payload encoding; the store owns the region, the frame boundary, and the
// frameChunk recovery bit.
//
// This is the store half of the per-type cold chunk form. The set demotion pass
// (set/cold.go) is the first consumer: it walks a partition's members in hash
// order, fills a payload, and calls AppendChunk once per chunk, then reads a
// chunk back through ReadChunk to confirm a member or to promote the whole chunk.
// The whole-record plane and the chunk plane share the region and its compactor,
// distinguished by the frameChunk bit on the kind byte.

// Chunk is one decoded cold chunk, the view ReadChunk returns. Kind is the
// collection type with the frameChunk bit already masked off, Count the packed
// element count a rank descent accumulates, and Disc and Payload alias the
// caller's buffer (the one ReadChunk was handed and returned), valid until that
// buffer is reused. The store never reads inside Payload; the collection decodes
// it.
type Chunk struct {
	Kind    byte
	Count   int
	Disc    []byte
	Payload []byte
}

// AppendChunk frames one packed chunk and appends it to the cold region,
// returning its offset. kind is the plain collection kind (below frameChunk); the
// store sets the frameChunk bit so the recovery walk can tell a chunk from a
// whole record. disc is the first discriminator (the resident directory keys the
// chunk by it), and payload is the collection's packed members in logical order,
// opaque here. It reports false when no cold region is configured or the region
// has taken a sticky write error, exactly the gate the whole-record demote path
// uses, so a broken region degrades to leaving the collection resident.
func (s *Store) AppendChunk(kind, flags byte, count uint16, key, disc, payload []byte) (uint64, bool) {
	if s.cold == nil || s.cold.werr != nil {
		return 0, false
	}
	s.frameBuf = appendChunkFrame(s.frameBuf[:0], kind|frameChunk, flags, count, key, disc, payload)
	off, err := s.cold.append(s.frameBuf)
	if err != nil {
		return 0, false
	}
	return off, true
}

// ReadChunk preads the chunk frame at off, decodes it, and returns its view plus
// the buffer that backs it. dst is reused when it fits and returned so the caller
// keeps the backing store alive for as long as it reads Disc and Payload; passing
// dst back on the next call recycles it. It reports false when there is no cold
// region, the pread fails, or the frame is torn. The header is read first for the
// frame length, then the whole frame in one more pread, matching the two-step
// read the whole-record path uses.
func (s *Store) ReadChunk(off uint64, dst []byte) (Chunk, []byte, bool) {
	if s.cold == nil {
		return Chunk{}, dst, false
	}
	var h [chunkHdr]byte
	if err := s.cold.readFill(off, h[:]); err != nil {
		return Chunk{}, dst, false
	}
	total := int(binary.LittleEndian.Uint32(h[0:]))
	if total < chunkHdr {
		return Chunk{}, dst, false
	}
	buf, err := s.cold.readInto(off, total, dst)
	if err != nil {
		return Chunk{}, buf, false
	}
	f, _, derr := decodeChunkFrame(buf)
	if derr != nil {
		return Chunk{}, buf, false
	}
	return Chunk{
		Kind:    f.kind &^ frameChunk,
		Count:   int(f.count),
		Disc:    f.disc,
		Payload: f.payload,
	}, buf, true
}
