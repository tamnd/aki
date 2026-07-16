package list

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/f3/store"
)

// The list cold chunk form (spec 2064/f3/06 sections 6 and 7, plan
// M7-slice-cold-chunk-list). A list's native band is a ring of chunk slabs the
// store's arena budget cannot see, so its cold tier is a demotion pass that pushes
// whole interior chunks out of resident RAM: the chunk's live frames are packed
// into a cold-region frame (store.AppendChunk), the resident blob and directory
// are released, and the ring keeps the chunk handle with its live window so the
// count and the Fenwick directory over counts stay untouched. A demoted chunk
// carries only a cold-region offset; a read preads the frame and walks it.
//
// Unlike the set (member hash) and the zset (score), a list needs no discriminator
// search to place a cold chunk for a read. The ring walk that resolves a dense
// index already lands on the chunk handle, and the handle carries the offset
// directly, so a read never touches a directory. The shared directory keyed by a
// per-list demote sequence serves dirty tracking and recovery and lands with the
// demote pass; this slice is the inert read plumbing that pass and the promote
// path build on. It routes LINDEX and LRANGE through a cold-aware frame read and
// leaves LPOS, the interior mutators, and the demote pass to their own slices,
// none of which run until the trigger wires demotion live.

// kindList is the collection kind byte a list chunk carries, a plain kind below
// frameChunk (store.AppendChunk sets the recovery bit itself). An M8 recovery walk
// reads it to dispatch a cold list chunk back into the list registry.
const kindList byte = 0x03

// listCold is a list's cold-tier state, built on the first demote and held on the
// native band. st is the store the cold frames live in and scratch is the pread
// buffer every cold read reuses, so a steady cold read allocates nothing. The
// demote-sequence directory the pass and recovery need lands with the demote
// slice; the read plumbing needs only the store and the buffer, since a chunk
// handle carries its own cold offset. Owner-local, so nothing locks.
type listCold struct {
	st      *store.Store
	scratch []byte
}

// payload preads the cold chunk at off into the shared scratch and returns its
// packed-frame payload. The bytes alias scratch and are valid only until the next
// cold read on this list, the single-call lifetime a resident blob alias already
// carries. It returns nil on a torn frame, which a caller reads as an empty chunk.
func (lc *listCold) payload(off uint64) []byte {
	ck, buf, ok := lc.st.ReadChunk(off, lc.scratch)
	lc.scratch = buf
	if !ok {
		return nil
	}
	return ck.Payload
}

// appendFrame packs one value into a cold payload: an unsigned-varint length then
// the raw bytes, byte-identical to the frame the resident blob stores
// (chunk.writeFrame). A cold read walks the payload exactly as a resident read
// walks the blob, so the demote pass packs with this and the read side needs no
// separate decode.
func appendFrame(payload, v []byte) []byte {
	payload = binary.AppendUvarint(payload, uint64(len(v)))
	return append(payload, v...)
}

// coldFrameAt returns the value of the ord-th frame in a cold chunk's packed
// payload, walking the length-prefixed frames from the front. A list chunk holds
// at most chunkElemCap frames, so the walk is bounded; a sequential reader
// (rangeInto) walks the payload once instead of calling this per element. The
// returned bytes alias payload.
func coldFrameAt(payload []byte, ord int) []byte {
	off := 0
	for i := 0; i < ord; i++ {
		vlen, w := binary.Uvarint(payload[off:])
		off += w + int(vlen)
	}
	vlen, w := binary.Uvarint(payload[off:])
	return payload[off+w : off+w+int(vlen)]
}
