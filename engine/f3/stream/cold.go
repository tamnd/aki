package stream

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/engine/f3/tier"
)

// The stream cold chunk form (spec 2064/f3/06 sections 6 and 7, plan
// M7-slice-cold-chunk-stream). A stream's native band is an append log of packed
// blocks the store's arena budget cannot see, so its cold tier is a demotion pass
// that pushes whole front (oldest) blocks out of resident RAM. A stream block is
// the cleanest cold unit in the engine: its blob is already the master-delta wire
// form a cold read decodes, so a demote spills the blob whole through
// store.AppendChunk with no repack, and a cold read preads the payload and walks it
// with the block's resident master schema exactly as a resident walk does.
//
// Like the list, a stream carries order in its native directory over block handles,
// not in a searched cold directory: a range seek resolves the owning block through
// floorBlock, and the handle carries the cold offset directly, so a read never
// searches the shared directory. The stream keys its native directory by block
// firstID (naturally ordered and monotonic, the zset-score shape); the shared
// tier.Directory here serves only dirty-tracking and M8 recovery, one descriptor
// per shed block keyed by firstID so a recovery walk and the promote path find the
// cold frames again. The demote pass sheds only old front blocks and never runs
// until the trigger wires it live (F).

// kindStream is the collection kind byte a stream block carries, one above the hash
// (store.AppendChunk sets the recovery bit itself). An M8 recovery walk reads it to
// dispatch a cold stream block back into the stream registry at its firstID.
const kindStream byte = 0x05

const (
	// demoteTailMargin is how many of the newest blocks the demote pass keeps
	// resident, including the open tail block every XADD extends, so a fresh append
	// and an XREAD $ never pread. Two blocks absorb the boundary: the open tail plus
	// the most recently sealed block a just-behind XREAD cursor reaches. It is the
	// stream analogue of the list's end margin, an F9 lab knob the trigger tunes.
	demoteTailMargin = 2
	// demoteQuantum bounds the front blocks one demote pass sheds, so the trigger
	// drains a long log a bounded run at a time rather than in one unbounded sweep
	// that could stall the owner. The trigger calls the pass again while the stream
	// still overshoots the resident cap.
	demoteQuantum = 8
)

// streamCold is a stream's cold-tier state, built on the first demote and held on
// the native band. st is the store the cold frames live in and scratch is the pread
// buffer every cold read reuses, so a steady cold read allocates nothing. dir is
// the demote directory: one descriptor per shed block keyed by firstID, the record
// recovery and the promote path read, since a read itself rides the block handle
// offset and never touches it. Owner-local, so nothing locks.
type streamCold struct {
	st      *store.Store
	dir     tier.Directory
	scratch []byte
}

// residentBytes is the cold state's own resident footprint: the demote-sequence
// directory. The demoted block blobs are already gone from resBlob (demote drops
// each shed blob's length), so this adds only the directory the cold state itself
// keeps resident, matching the other cold forms. The pread scratch is left out on
// purpose, the same bounded per-read buffer those forms exclude to keep the running
// total from drifting between commands.
func (sc *streamCold) residentBytes() uint64 {
	return uint64(sc.dir.Bytes())
}

// payload preads the cold block at off into the shared scratch and returns its
// packed-blob payload. The bytes alias scratch and are valid only until the next
// cold read on this stream, the single-call lifetime a resident blob alias already
// carries. It returns nil on a torn frame, which a caller reads as an empty block.
func (sc *streamCold) payload(off uint64) []byte {
	ck, buf, ok := sc.st.ReadChunk(off, sc.scratch)
	sc.scratch = buf
	if !ok {
		return nil
	}
	return ck.Payload
}

// discID writes a stream ID as the 16-byte big-endian (ms, seq) key the demote
// directory orders on, so bytes.Compare reproduces the ID's numeric order (the
// order-preserving encoding tier.Directory assumes). The firstID of a block is its
// smallest ID, unique and stable across the block's life, so it is a clean
// partition key with no overlap between blocks.
func discID(id streamID) [16]byte {
	var d [16]byte
	binary.BigEndian.PutUint64(d[0:], id.ms)
	binary.BigEndian.PutUint64(d[8:], id.seq)
	return d
}

// dropDescriptor removes the demote descriptor for the block whose firstID is first
// and whose cold frame sits at off, the resident-directory side of both a promote (the
// block comes resident) and a whole-block drop (the block leaves the log). Blocks
// partition the demote space by firstID with no overlap, so a Floor on the block's own
// firstID lands on its descriptor exactly; the offset guard aborts on a drifted
// directory rather than removing the wrong one. Resident-only, no pread.
func (sc *streamCold) dropDescriptor(first streamID, off uint64) {
	disc := discID(first)
	if idx, found := sc.dir.Floor(disc[:]); found {
		if dOff, _, _ := sc.dir.At(idx); dOff == off {
			sc.dir.Remove(idx)
		}
	}
}

// demote spills a bounded run of the log's oldest resident blocks into the cold
// region, releases their resident blobs, and returns how many blocks it shed. It
// keeps demoteTailMargin newest blocks resident (including the open tail), sweeps
// the front in log order up to demoteQuantum blocks per call, and skips blocks an
// earlier quantum already shed. Unlike the set (member hash) and the zset (score),
// a stream block demotes whole: its blob is already the wire form, so the pass
// spills it straight into one cold chunk with no repack, keyed by the block firstID.
//
// It appends every block first and commits nothing until the whole run lands: only
// on a clean append does it record each handle's cold offset, insert the demote
// descriptors, and release the blobs. A refused append abandons the run with every
// block still resident (the orphan frames the append-only region holds are dead
// space the compactor reclaims), so a broken cold region degrades demotion to a
// no-op rather than a torn stream. Owner goroutine only.
func (s *stream) demote(st *store.Store, key []byte) int {
	limit := len(s.blocks) - demoteTailMargin
	if limit <= 0 {
		return 0 // the whole log fits in the resident tail margin
	}
	if s.cold == nil {
		s.cold = &streamCold{st: st}
	}
	type placed struct {
		i     int
		off   uint64
		disc  [16]byte
		count int
	}
	var runs []placed
	for i := 0; i < limit && len(runs) < demoteQuantum; i++ {
		b := s.blocks[i]
		if b.cold() || len(b.blob) == 0 {
			continue // already cold from an earlier quantum, or an empty handle
		}
		disc := discID(b.first)
		off, ok := st.AppendChunk(kindStream, 0, uint16(b.count), key, disc[:], b.blob)
		if !ok {
			return 0 // broken region: abandon, every block stays resident
		}
		runs = append(runs, placed{i: i, off: off, disc: disc, count: b.count})
	}
	if len(runs) == 0 {
		return 0 // front already cold from earlier quanta
	}

	// Commit: one descriptor per shed block keyed by firstID, then flip each handle
	// to the cold form. Dropping the blob reference frees it to GC directly (a block
	// blob is a whole allocation, not a region of a shared slab), and resBlob drops by
	// the shed length so residentBytes tracks the freed bytes with no walk. The header
	// and names stay resident, so covers and the directory floor read the cold block
	// unchanged.
	for _, r := range runs {
		s.cold.dir.Insert(r.disc[:], uint32(r.count), r.off)
		b := s.blocks[r.i]
		s.resBlob -= uint64(len(b.blob))
		b.blob = nil
		b.coldOff = r.off
	}
	return len(runs)
}

// promote brings the cold block at log index i back resident: it preads the packed
// blob, copies it into a fresh resident blob on the same handle so the log position,
// the directory floor, and the header counts are untouched, clears the cold marker,
// and drops the demote descriptor. It is the unconditional bring-up of section 7.3,
// the response to an XDEL or an exact-XTRIM boundary that reaches a cold block; a
// resident block is a no-op. A torn cold frame leaves the block cold (its read path
// still preads it), so promote never publishes a partial block. Owner goroutine only.
func (s *stream) promote(i int) {
	b := s.blocks[i]
	if !b.cold() {
		return
	}
	off := b.coldOff
	ck, buf, ok := s.cold.st.ReadChunk(off, s.cold.scratch)
	s.cold.scratch = buf
	if !ok {
		return
	}
	// Copy the payload into a fresh resident blob: ck.Payload aliases the shared pread
	// scratch, which the next cold read reuses, so the block must own its bytes. The
	// names offsets already index into these bytes (the payload is byte-identical to
	// the shed blob), so the master schema needs no rebuild.
	b.blob = append([]byte(nil), ck.Payload...)
	b.coldOff = 0
	s.resBlob += uint64(len(b.blob))
	s.cold.dropDescriptor(b.first, off)
}

// promoteIfCold brings the block at log index i resident when a demote pass has shed
// it, the guard the write paths run before they mutate a block's blob in place: an
// XDEL or an exact-XTRIM boundary tombstone flips a flag byte the cold block no longer
// holds resident, so the block must come up first (section 7.3). A resident block is a
// plain no-op, so an all-resident stream never enters the cold branch (L9).
func (s *stream) promoteIfCold(i int) {
	if s.blocks[i].cold() {
		s.promote(i)
	}
}

// forgetCold drops a cold block's demote descriptor when the block leaves the log
// whole, an approximate XTRIM front drop releasing the handle. The cold frame itself
// becomes an orphan the compactor reclaims (section 6.6, the dead-space rule), so this
// preads nothing: the block keeps its firstID header resident, enough to find and drop
// the descriptor. A resident block holds no descriptor and is a no-op.
func (s *stream) forgetCold(b *block) {
	if !b.cold() {
		return
	}
	s.cold.dropDescriptor(b.first, b.coldOff)
}
