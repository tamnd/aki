package stream

import (
	"encoding/binary"

	structs "github.com/tamnd/aki/engine/f3/struct"
)

// A stream is one key's worth of entries (spec 2064/f3/14 section 4), living in
// one of two bands. Tiny streams start in the inline band: a single block held
// under a small entry-count and byte cap, the ~40-byte header carrying lastID,
// maxDeletedID and the counters. On the first breach of a cap, a group, or a fat
// entry, the stream upgrades one-way to the native band: an append log of blocks
// indexed by a counted directory over their first IDs, so XDEL and every range
// seek find their block in O(log C) rather than a linear scan.
//
// The two bands share the block encoding of slice 1, so the inline block is
// already a valid native block: upgrade is a band flag flip, not a re-encode
// (unlike Redis, whose inline blob and listpack node differ). lastID never moves
// backward, not even when XDEL tombstones the last entry, so it is the sole
// source for the next auto ID (section 3.6).
type band uint8

const (
	bandInline band = iota
	bandNative
)

// Inline caps (section 4.2). A stream stays inline while it holds at most this
// many entries in at most this many bytes and has no groups; the first XADD that
// would break either cap, or any XGROUP, upgrades it. The delta lab may retune
// these; the block geometry (4096/128) is a hard ceiling far above them.
const (
	inlineMaxEntries = 16
	inlineMaxBytes   = 512
)

type stream struct {
	kind         band
	lastID       streamID // greatest ID ever assigned, never lowered by XDEL
	maxDeletedID streamID // greatest tombstoned ID (section 6.5)
	entriesAdded uint64   // lifetime XADD count, never lowered
	length       uint64   // live entries (XLEN)
	blocks       []*block // inline: 0 or 1; native: the append log

	// dir is the native band's counted directory (section 3.4): the M2 counted
	// B+ tree keyed by block firstID, mapping to a block index in blocks. It is
	// nil while inline (the single block needs no index) and seeded on upgrade.
	// The key is the 16-byte ID split across the tree's two key halves: the ms
	// is the 8-byte score, the seq the 8-byte member resolved through the Members
	// callback on the rare same-ms tie. A block is inserted once, when it closes
	// (opens), so the append touches the tree once per ~128 entries, the O(log C)
	// insert the monotonic fast path of section 3.5 is built around.
	dir *structs.Tree
	// dirKey is the Members scratch: a block firstID's seq formatted big-endian
	// for the tie-break compare. Owner-only and read once per compare, so one
	// reused array is safe.
	dirKey [8]byte
	// base is the logical index of blocks[0], the count of front blocks XTRIM has
	// dropped over the stream's life (section 6.6). Directory references are
	// logical indices, so a dropped block does not shift every surviving block's
	// stored reference: the physical slot is blocks[ref-base]. Rank, and thus
	// floorBlock, still returns a physical position because the tree holds only
	// surviving blocks, so only Member and dirInsert carry the offset.
	base uint32

	// groups is the consumer-group table (section 7.2), nil until the first
	// XGROUP CREATE. A stream that holds a group is always native (section 4.4),
	// so this is populated only in the native band. The delivery ledger (the PEL
	// tree-plus-hash over slabs) hangs off each group and fills as XREADGROUP
	// delivers (slice 6); this slice carries the group and consumer records and
	// their lifecycle.
	groups map[string]*streamGroup

	// gcDirty marks that a tombstone has landed in a native sealed block since the
	// last gc pass, so the owner's between-batches maintainer (gc.go) should visit
	// this stream. It is set by the XDEL and exact-XTRIM handlers and cleared by the
	// maintainer, both on the owner goroutine, and keeps the stream in the registry
	// dirty list at most once between passes.
	gcDirty bool
}

// Member resolves a directory reference (a block index) to its block firstID's
// seq in big-endian bytes, the tie-break key the counted tree compares when two
// blocks share a firstID ms. It satisfies structs.Members for the directory.
func (s *stream) Member(ref uint32) []byte {
	putSeq(s.dirKey[:], s.blocks[ref-s.base].first.seq)
	return s.dirKey[:]
}

// putSeq writes a seq as the big-endian member key the counted tree orders on
// within an ms, so bytes.Compare reproduces numeric seq order. The directory and
// the PEL both key on an ID's seq this way.
func putSeq(dst []byte, seq uint64) { binary.BigEndian.PutUint64(dst, seq) }

// seqKey formats an ID's seq as the big-endian member key, allocating a fresh
// array for a one-shot compare (the Members callbacks reuse a scratch instead).
func seqKey(id streamID) []byte {
	var b [8]byte
	putSeq(b[:], id.seq)
	return b[:]
}

// dirInsert records a newly opened block in the directory. idx is the physical
// slot; the stored reference is the logical index idx+base, so it stays valid
// after front blocks are dropped and the slice reslides.
func (s *stream) dirInsert(idx int) {
	b := s.blocks[idx]
	s.dir.Insert(b.first.ms, seqKey(b.first), uint32(idx)+s.base, s)
}

// floorBlock returns the index of the last block whose firstID is at or below id,
// the block a forward seek to id starts in. It clamps to 0 when id precedes every
// block, so a walk from there still filters correctly. The caller guarantees the
// native band (dir != nil) and at least one block.
func (s *stream) floorBlock(id streamID) int {
	rank, present := s.dir.Rank(id.ms, seqKey(id), s)
	if present {
		return int(rank)
	}
	if rank == 0 {
		return 0
	}
	return int(rank) - 1
}

func newStream() *stream { return &stream{kind: bandInline} }

// tail returns the block a new entry would extend, or nil when the stream holds
// no block yet.
func (s *stream) tail() *block {
	if len(s.blocks) == 0 {
		return nil
	}
	return s.blocks[len(s.blocks)-1]
}

// appendEntry writes one entry with the pre-allocated, pre-validated id and
// fields, upgrading the band if the entry would break an inline cap, and
// advances lastID and the counters. The caller (XADD) has already checked id
// strictly exceeds lastID, so the underlying block append always accepts it.
func (s *stream) appendEntry(id streamID, fields []field) {
	if s.kind == bandInline {
		s.appendInline(id, fields)
	} else {
		s.appendNative(id, fields)
	}
	s.lastID = id
	s.entriesAdded++
	s.length++
}

// appendInline extends the single inline block, or upgrades to native when the
// entry would push the block past an inline cap (including a first entry whose
// own frame already exceeds the byte cap, which upgrades then lands in a block
// of its own).
func (s *stream) appendInline(id streamID, fields []field) {
	b := s.tail()
	if b == nil {
		b = newBlock()
		s.blocks = append(s.blocks, b)
	}
	if b.count+1 > inlineMaxEntries || b.size()+b.projectedFrame(id, fields) > inlineMaxBytes {
		s.upgrade()
		s.appendNative(id, fields)
		return
	}
	b.appendEntry(id, fields)
}

// upgrade flips the stream to the native band (section 4.3). The inline block is
// already a valid, well-under-budget native block, so it simply becomes the
// first block of the append log, and the counted directory is built and seeded
// with it. One-way per invariant F4.
func (s *stream) upgrade() {
	s.kind = bandNative
	s.dir = structs.NewTree()
	for i := range s.blocks {
		s.dirInsert(i)
	}
}

// ensureNative upgrades an inline stream to the native band so it can carry a
// group table (section 4.4): the group ledger has no packed-blob form, and a
// stream someone attaches a group to has declared itself a work queue, not a
// tiny mailbox. A native stream is left untouched. A zero-entry inline stream
// (XGROUP CREATE MKSTREAM on a fresh key) becomes a native stream with an empty
// directory and no blocks, ~200 bytes, and every group code path can then assume
// the native form.
func (s *stream) ensureNative() {
	if s.kind == bandInline {
		s.upgrade()
	}
}

// appendNative extends the tail block, opening a fresh block when the tail is
// full (or when there is none yet). A full-block append fails, so the new entry
// masters a new block. A single fat entry masters its own block regardless of
// size (the master always lands), which is the section 3.7 solo-block path.
func (s *stream) appendNative(id streamID, fields []field) {
	if b := s.tail(); b != nil && b.appendEntry(id, fields) {
		return
	}
	nb := newBlock()
	nb.appendEntry(id, fields)
	s.blocks = append(s.blocks, nb)
	s.dirInsert(len(s.blocks) - 1)
}

// delete tombstones the entry with ID id and reports whether it removed a live
// one, advancing length and maxDeletedID. The native band seeks the block via the
// directory (O(log C), section 6.5); the inline band has the one block. A block
// whose span does not cover id holds nothing to delete.
func (s *stream) delete(id streamID) bool {
	b := s.blockFor(id)
	if b == nil || !b.covers(id) {
		return false
	}
	if b.tombstone(id) {
		s.length--
		if id.cmp(s.maxDeletedID) > 0 {
			s.maxDeletedID = id
		}
		return true
	}
	return false
}

// blockFor returns the block that would hold id: the directory floor block in the
// native band, the single block in the inline band, or nil for an empty stream.
func (s *stream) blockFor(id streamID) *block {
	if len(s.blocks) == 0 {
		return nil
	}
	if s.dir != nil {
		return s.blocks[s.floorBlock(id)]
	}
	return s.blocks[0]
}
