package sqlo1b

import (
	"bytes"
	"fmt"
)

// The cold read path (doc 04 section 5, steps 2..4): directory to
// chunk to record group, at most three group reads fully cold, plus
// one per chain link the probe has to follow. Parking and
// continuation slots are owner-runtime business in a later
// milestone; this is the synchronous resolution the store calls.

// A GroupSource serves one 4 KiB group image by (extent, group).
// The store wires it to the IO layer; the contract is that the
// returned image is what the writer laid out (a slotted group for
// vlog groups, eight packed chunks for index groups, one directory
// page for directory groups).
type GroupSource interface {
	ReadGroup(extent uint64, group uint16) ([]byte, error)
}

// A DirSource resolves a bucket number to its base chunk's full
// pointer. The resident Directory satisfies it with zero IO; PagedDir
// is the fully cold shape that pays a group read per resolution.
type DirSource interface {
	Get(chunkNo uint64) (FullPtr, error)
}

// PagedDir resolves buckets by reading the directory page through
// the group source on every call: the fully cold read shape, one
// group read per lookup. B6 puts a budgeted cache in front; resident
// mode (Directory) is the fully warm shape. Root is the parsed radix
// root, verified against dir_root by whoever opened the file.
type PagedDir struct {
	Root   []FullPtr
	DirLen uint64
	Groups GroupSource
}

// Get reads and verifies the page holding chunkNo, then returns its
// entry.
func (d *PagedDir) Get(chunkNo uint64) (FullPtr, error) {
	if chunkNo >= d.DirLen {
		return FullPtr{}, fmt.Errorf("sqlo1b: directory has %d buckets, no bucket %d", d.DirLen, chunkNo)
	}
	pageNo := chunkNo / DirPageEntries
	if pageNo >= uint64(len(d.Root)) {
		return FullPtr{}, fmt.Errorf("sqlo1b: radix root has %d pages, bucket %d needs page %d", len(d.Root), chunkNo, pageNo)
	}
	pp := d.Root[pageNo]
	pos := Pos(pp.Pos)
	raw, err := d.Groups.ReadGroup(pos.Extent(), pos.Group())
	if err != nil {
		return FullPtr{}, fmt.Errorf("sqlo1b: directory page %d: %w", pageNo, err)
	}
	if err := pp.Verify(raw); err != nil {
		return FullPtr{}, fmt.Errorf("sqlo1b: directory page %d: %w", pageNo, err)
	}
	ents, err := DecodeDirPage(raw, pageNo, d.DirLen)
	if err != nil {
		return FullPtr{}, err
	}
	return ents[chunkNo%DirPageEntries], nil
}

// ReadStats counts what a reader's probes cost. GroupSource calls
// are the caller's to count; these are the reader-internal events
// the IO count alone cannot attribute.
type ReadStats struct {
	ChunkReads   uint64 // chunk group reads, base and chain
	ChainFollows uint64 // chain links followed past the base chunk
	RecordReads  uint64 // record resolutions for fingerprint candidates
	FalseHits    uint64 // record reads whose key bytes did not match
}

// IndexReader resolves keys through the cold index. Blob is the
// escape for byte-addressed records: the store wires it to ReadBlob
// with its file handle; a reader that never meets blob positions can
// leave it nil. Compressed reports whether an extent holds
// compressed frame groups (cgroup.go) instead of raw slotted groups;
// the store wires it to its extent-eflags cache, and nil reads
// everything as raw, which keeps readers over uncompressed fixtures
// working. Frames, when set, memoizes decoded frame payloads across
// point reads (framecache.go); nil decodes every compressed group
// fresh.
type IndexReader struct {
	Dir        DirSource
	Groups     GroupSource
	Blob       func(Pos) (*Record, error)
	Compressed func(extent uint64) (bool, error)
	Frames     *FrameCache
	Stats      ReadStats
}

// readChunk fetches the group holding a chunk and carves out the
// verified image. The full pointer's sum covers the 512-byte chunk,
// not the group, so seven neighbors stay outside the check.
func (r *IndexReader) readChunk(ptr FullPtr, chunkNo uint64) (*Chunk, error) {
	img, err := r.chunkImage(Pos(ptr.Pos))
	if err != nil {
		return nil, err
	}
	if err := ptr.Verify(img); err != nil {
		return nil, fmt.Errorf("sqlo1b: chunk %d: %w", chunkNo, err)
	}
	return ParseChunk(img, chunkNo)
}

// readChainChunk follows a chain pointer, whose truncated check32
// stands in for a full sum (doc 03 8.5; the overflow chunk's own
// strict parse and chunk_no_lo back it up).
func (r *IndexReader) readChainChunk(pos Pos, check uint32, chunkNo uint64) (*Chunk, error) {
	img, err := r.chunkImage(pos)
	if err != nil {
		return nil, err
	}
	if got := ChunkCheck32(img); got != check {
		return nil, fmt.Errorf("sqlo1b: chain chunk of bucket %d checks %#x, pointer says %#x", chunkNo, got, check)
	}
	return ParseChunk(img, chunkNo)
}

func (r *IndexReader) chunkImage(pos Pos) ([]byte, error) {
	if pos.Slot() >= chunksPerGroup {
		return nil, fmt.Errorf("sqlo1b: chunk slot %d, chunks sit %d to a group", pos.Slot(), chunksPerGroup)
	}
	grp, err := r.Groups.ReadGroup(pos.Extent(), pos.Group())
	if err != nil {
		return nil, err
	}
	if len(grp) != GroupSize {
		return nil, fmt.Errorf("sqlo1b: chunk group is %d bytes, want %d", len(grp), GroupSize)
	}
	r.Stats.ChunkReads++
	off := int(pos.Slot()) * ChunkSize
	return grp[off : off+ChunkSize], nil
}

// resolveRecord reads the record behind a candidate vptr.
func (r *IndexReader) resolveRecord(pos Pos) (*Record, error) {
	r.Stats.RecordReads++
	if pos.IsBlob() {
		if r.Blob == nil {
			return nil, fmt.Errorf("sqlo1b: blob position %v with no blob reader", pos)
		}
		return r.Blob(pos)
	}
	grp, err := r.Groups.ReadGroup(pos.Extent(), pos.Group())
	if err != nil {
		return nil, err
	}
	var comp bool
	if r.Compressed != nil {
		if comp, err = r.Compressed(pos.Extent()); err != nil {
			return nil, err
		}
	}
	var raw []byte
	if comp {
		view, err := r.Frames.View(pos.Extent(), pos.Group(), grp)
		if err != nil {
			return nil, err
		}
		raw, err = view.Record(pos.Slot())
		if err != nil {
			return nil, err
		}
	} else {
		view, err := ParseGroup(grp)
		if err != nil {
			return nil, err
		}
		raw, err = view.Record(pos.Slot())
		if err != nil {
			return nil, err
		}
	}
	return DecodeRecord(raw)
}

// Lookup resolves key through the cold index under the given
// hash_epoch. A miss returns (nil, 0, nil): absence is an answer,
// not an error. Fingerprint candidates resolve in slot order, base
// chunk before chain, and every non-matching resolution counts as a
// false hit.
func (r *IndexReader) Lookup(key []byte, hashEpoch uint64) (*Record, Pos, error) {
	h := KeyHash(key)
	split, level := UnpackHashEpoch(hashEpoch)
	bucket := BucketOf(PlacementBits(h), level, split)
	ptr, err := r.Dir.Get(bucket)
	if err != nil {
		return nil, 0, err
	}
	c, err := r.readChunk(ptr, bucket)
	if err != nil {
		return nil, 0, err
	}
	fp := Fingerprint(h)
	for {
		var candidates []uint64
		c.Probe(fp, func(i int, meta uint16, vptr uint64) bool {
			candidates = append(candidates, vptr)
			return true
		})
		for _, vptr := range candidates {
			rec, err := r.resolveRecord(Pos(vptr))
			if err != nil {
				return nil, 0, err
			}
			if bytes.Equal(rec.Key, key) {
				return rec, Pos(vptr), nil
			}
			r.Stats.FalseHits++
		}
		if !c.Chained() {
			return nil, 0, nil
		}
		pos, check, err := c.ChainPtr()
		if err != nil {
			return nil, 0, err
		}
		r.Stats.ChainFollows++
		c, err = r.readChainChunk(pos, check, bucket)
		if err != nil {
			return nil, 0, err
		}
	}
}
