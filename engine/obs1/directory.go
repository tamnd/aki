// The directory (spec 2064/obs1 doc 05 section 2.3): the per-group RAM
// structure that turns a keymap locator into (object key, block span,
// offset in block), which is everything a cold read needs to plan its
// GET. It holds the manifest's live segment list with each segment's
// block index and a slim cut of its chunk index, footers minus their
// blooms, so cost is per chunk, not per record.
//
// Two feeds fill it. Fold publish adds each durable segment's footer the
// moment its watermark gate opens, before the keymap placements apply,
// so the keymap never points at a segment the directory cannot resolve.
// Takeover rebuilds it from the manifest with one ranged footer GET per
// segment. Chunk key bytes are not retained: the keymap locates
// whole-record run chunks by position alone, and a collection chunk
// keeps only its key's u64 fingerprint, which is all the field planner
// needs to gather one collection's chunks inside a segment.
package obs1

import (
	"fmt"
	"sort"
	"sync"

	"github.com/tamnd/aki/engine/obs1/store"
)

// DirRef is a resolved locator: the object to GET, the block's index row
// (BlockSpan gives the exact byte range), and where the chunk frame
// starts in the decompressed block.
type DirRef struct {
	ObjKey     string
	Block      SegmentBlockEntry
	OffInBlock uint32
	ChunkKind  uint8
}

// dirChunk is the resident cut of one SegmentChunkEntry: placement plus
// the run facts scan planning and rewrite selection read, and for a
// collection chunk the key's fingerprint, which is what lets the field
// planner gather one collection's chunks without retaining key bytes
// (the doc 05 cost rule: the directory stays per-chunk, and a u64 is the
// per-chunk price of collection planning).
type dirChunk struct {
	block     uint32
	off       uint32
	firstDisc uint64
	fp        uint64 // bloomHash h1 of the chunk key; collection chunks only
	kind      uint8
	flags     uint8
	count     uint16
	liveHint  uint16
}

// dirChunkCost is the accounting charge per resident chunk entry.
const dirChunkCost = 32

// dirSeg is one live segment: its object key, block index, and chunk
// index.
type dirSeg struct {
	objKey string
	blocks []SegmentBlockEntry
	chunks []dirChunk
}

// Directory is one group's resolver. The folder's putter goroutine adds
// on publish and shard owners resolve on reads, so it carries its own
// mutex like the group's Keymap.
type Directory struct {
	mu    sync.Mutex
	segs  map[uint32]*dirSeg
	nchnk int
	bytes int
}

// NewDirectory returns an empty directory; publish and takeover fill it.
func NewDirectory() *Directory {
	return &Directory{segs: make(map[uint32]*dirSeg)}
}

// dirSegOverhead approximates the per-segment map and struct cost the
// Bytes accounting charges beyond the slices themselves.
const dirSegOverhead = 64

// Add books one durable segment under the keymap's truncated SegSeq.
// The footer must have parsed already; Add re-checks only the facts the
// directory itself relies on, and any error is a loud bug, not an
// operational condition.
func (d *Directory) Add(objKey string, f *SegmentFooter) error {
	if f.SegSeq == 0 {
		return fmt.Errorf("obs1: directory refuses SegSeq 0, the keymap's empty-slot sentinel")
	}
	if f.SegSeq > (1<<32)-1 {
		return fmt.Errorf("obs1: SegSeq %d does not fit the locator's 32 bits", f.SegSeq)
	}
	if len(f.Blocks) == 0 || len(f.Chunks) == 0 {
		return fmt.Errorf("obs1: directory refuses a segment with %d blocks and %d chunks", len(f.Blocks), len(f.Chunks))
	}
	s := &dirSeg{
		objKey: objKey,
		blocks: append([]SegmentBlockEntry(nil), f.Blocks...),
		chunks: make([]dirChunk, len(f.Chunks)),
	}
	for i, c := range f.Chunks {
		if int(c.Block) >= len(f.Blocks) {
			return fmt.Errorf("obs1: chunk %d points at block %d of %d", i, c.Block, len(f.Blocks))
		}
		s.chunks[i] = dirChunk{
			block: c.Block, off: c.OffInBlock, firstDisc: c.FirstDisc,
			kind: c.Kind, flags: c.Flags, count: c.Count, liveHint: c.LiveHint,
		}
		if c.Kind&store.ChunkKindBit != 0 && c.Flags&store.ChunkFlagRun == 0 {
			h1, _ := bloomHash(c.Key)
			s.chunks[i].fp = h1
		}
	}
	seg := uint32(f.SegSeq)
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.segs[seg]; ok {
		return fmt.Errorf("obs1: directory already holds segment %d", seg)
	}
	d.segs[seg] = s
	d.nchnk += len(s.chunks)
	d.bytes += dirSegOverhead + len(s.objKey) + len(s.blocks)*segBlockEntry + len(s.chunks)*dirChunkCost
	return nil
}

// Resolve turns a keymap locator into a GET plan. Only tier 0, the
// segment tier, resolves here; the other tiers are future placements.
// A miss means the locator's segment is not live in this directory,
// which a reader treats as retry-after-refresh, never as key-absent.
func (d *Directory) Resolve(l KeyLoc) (DirRef, bool) {
	if l.Tier != 0 {
		return DirRef{}, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := d.segs[l.Seg]
	if !ok || int(l.Chunk) >= len(s.chunks) {
		return DirRef{}, false
	}
	c := s.chunks[l.Chunk]
	return DirRef{
		ObjKey:     s.objKey,
		Block:      s.blocks[c.block],
		OffInBlock: c.off,
		ChunkKind:  c.kind,
	}, true
}

// ResolveField plans a point read into a cold collection (doc 08 section
// 3): within the segment the keymap locator pins, it finds the chunk of
// the collection fp that owns disc, the greatest first discriminator at
// or below it among the collection's chunks, falling back to the first
// chunk when disc precedes them all (the field can only be an overlay
// add then, and the fetched chunk answers absent, which the resident
// state usually already knew). The locator's own chunk index only names
// the segment; the floor here picks the chunk, so a placement is valid
// pointing at any chunk of the collection.
func (d *Directory) ResolveField(l KeyLoc, fp uint64, disc uint64) (DirRef, bool) {
	return d.ResolveFieldKind(l, fp, disc, 0)
}

// ResolveFieldKind is ResolveField restricted to one chunk kind, the
// dual-projection point plan (doc 08 section 5): a zset keeps member
// chunks and score runs under one collection key, distinguished by kind
// and carrying discriminators from different coordinate spaces, so a
// floor that mixed them could land a member read on a score run. Kind
// zero means any, which is the single-projection types' call.
func (d *Directory) ResolveFieldKind(l KeyLoc, fp uint64, disc uint64, kind uint8) (DirRef, bool) {
	if l.Tier != 0 {
		return DirRef{}, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := d.segs[l.Seg]
	if !ok {
		return DirRef{}, false
	}
	best, first := -1, -1
	for i := range s.chunks {
		c := &s.chunks[i]
		if !d.collChunk(c, fp, kind) {
			continue
		}
		if first < 0 || c.firstDisc < s.chunks[first].firstDisc {
			first = i
		}
		if c.firstDisc <= disc && (best < 0 || c.firstDisc >= s.chunks[best].firstDisc) {
			best = i
		}
	}
	if best < 0 {
		best = first
	}
	if best < 0 {
		return DirRef{}, false
	}
	c := s.chunks[best]
	return DirRef{
		ObjKey:     s.objKey,
		Block:      s.blocks[c.block],
		OffInBlock: c.off,
		ChunkKind:  c.kind,
	}, true
}

// CollChunks plans a whole-collection read (HGETALL and the scan family):
// every chunk of the collection fp inside the locator's segment, in
// discriminator order. The caller coalesces refs that share a block into
// one ranged GET, which is what makes the request bill the doc 08 ceil
// identity rather than a GET per chunk.
func (d *Directory) CollChunks(l KeyLoc, fp uint64) []DirRef {
	if l.Tier != 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := d.segs[l.Seg]
	if !ok {
		return nil
	}
	type ord struct {
		disc uint64
		ref  DirRef
	}
	var out []ord
	for i := range s.chunks {
		c := &s.chunks[i]
		if !d.collChunk(c, fp, 0) {
			continue
		}
		out = append(out, ord{disc: c.firstDisc, ref: DirRef{
			ObjKey:     s.objKey,
			Block:      s.blocks[c.block],
			OffInBlock: c.off,
			ChunkKind:  c.kind,
		}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].disc < out[j].disc })
	refs := make([]DirRef, len(out))
	for i := range out {
		refs[i] = out[i].ref
	}
	return refs
}

// collChunk reports whether c is a collection chunk of the fingerprint:
// a packed chunk that is not a folder run, keyed by the collection whose
// fingerprint the keymap and the planner share. A nonzero kind restricts
// the match to one projection of a dual-projection collection.
func (d *Directory) collChunk(c *dirChunk, fp uint64, kind uint8) bool {
	if kind != 0 && c.kind != kind {
		return false
	}
	return c.kind&store.ChunkKindBit != 0 && c.flags&store.ChunkFlagRun == 0 && c.fp == fp
}

// Drop removes a segment, the GC seam: a segment leaves the directory
// when compaction retires it from the manifest. Reports whether the
// segment was live.
func (d *Directory) Drop(seg uint32) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := d.segs[seg]
	if !ok {
		return false
	}
	delete(d.segs, seg)
	d.nchnk -= len(s.chunks)
	d.bytes -= dirSegOverhead + len(s.objKey) + len(s.blocks)*segBlockEntry + len(s.chunks)*dirChunkCost
	return true
}

// Segments reports the live segment count.
func (d *Directory) Segments() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.segs)
}

// Chunks reports the resident chunk entry count, the doc 05 cost unit.
func (d *Directory) Chunks() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nchnk
}

// Bytes reports the approximate resident cost, the doc 10 memory gate's
// directory line.
func (d *Directory) Bytes() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.bytes
}
