package stream

// A stream is one key's worth of entries (spec 2064/f3/14 section 4), living in
// one of two bands. Tiny streams start in the inline band: a single block held
// under a small entry-count and byte cap, the ~40-byte header carrying lastID,
// maxDeletedID and the counters. On the first breach of a cap, a group, or a fat
// entry, the stream upgrades one-way to the native band: an append log of blocks
// (the counted directory over their first IDs arrives in slice 3, so here XDEL
// and any seek scan the block slice linearly).
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
// first block of the append log; the counted directory is seeded in slice 3.
// One-way per invariant F4.
func (s *stream) upgrade() { s.kind = bandNative }

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
}

// delete tombstones the entry with ID id and reports whether it removed a live
// one, advancing length and maxDeletedID. It scans blocks linearly (the counted
// directory replaces this in slice 3) and stops at the block whose span covers
// id.
func (s *stream) delete(id streamID) bool {
	for _, b := range s.blocks {
		if !b.covers(id) {
			continue
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
	return false
}
