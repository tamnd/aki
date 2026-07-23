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
// segment. Chunk keys are not retained: the keymap locates whole-record
// run chunks by position alone, and nothing produces collection chunks
// yet, so the per-chunk key column waits for the demoters that need it.
package obs1

import (
	"fmt"
	"sync"
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
// the run facts scan planning and rewrite selection read. 24 bytes.
type dirChunk struct {
	block     uint32
	off       uint32
	firstDisc uint64
	kind      uint8
	count     uint16
	liveHint  uint16
}

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
			kind: c.Kind, count: c.Count, liveHint: c.LiveHint,
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
	d.bytes += dirSegOverhead + len(s.objKey) + len(s.blocks)*segBlockEntry + len(s.chunks)*24
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
	d.bytes -= dirSegOverhead + len(s.objKey) + len(s.blocks)*segBlockEntry + len(s.chunks)*24
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
