package sqlo1b

import (
	"encoding/binary"
	"fmt"
)

// The directory (doc 03 section 8.4) maps chunk number to the full
// pointer of the bucket's base chunk: packed position plus xxhash64
// of the 512-byte chunk image, 16 bytes per bucket, about 0.38 bytes
// of RAM per key. Overflow chunks are not directory entries; they
// hang off their base chunk's chain pointer.
//
// Physically the directory is pages of 256 entries (one 4 KiB group
// each) in directory extents, found through a one-level radix root:
// an array of full pointers to the pages, itself stored in directory
// extents and referenced by the superblock's dir_root. Any page is
// two reads from the superblock.
//
// This slice is resident mode: the whole directory lives in RAM and
// pages exist only as the checkpoint image. Paging under a budget is
// B6's business. The directory's length is not stored; it is always
// NumBuckets of the hash_epoch committed beside it.

// DirPageEntries is how many 16-byte entries fill one 4 KiB page.
const DirPageEntries = GroupSize / dirEntrySize

const dirEntrySize = 16

// DirPages returns how many pages a directory of dirLen buckets
// occupies, and the radix root's byte length is 16 times that.
func DirPages(dirLen uint64) uint64 {
	return (dirLen + DirPageEntries - 1) / DirPageEntries
}

// Directory is the resident bucket-to-chunk map. It grows one bucket
// per split, in step with AdvanceSplit, and is never sparse: every
// bucket has a base chunk from the moment it exists.
type Directory struct {
	ptrs []FullPtr
}

// NewDirectory returns the directory of a fresh table: one bucket,
// whose chunk pointer the caller sets once the chunk is written.
func NewDirectory(first FullPtr) *Directory {
	return &Directory{ptrs: []FullPtr{first}}
}

// Len reports the bucket count, which must always equal NumBuckets
// of the table's (level, split).
func (d *Directory) Len() uint64 { return uint64(len(d.ptrs)) }

// Get returns bucket chunkNo's chunk pointer.
func (d *Directory) Get(chunkNo uint64) (FullPtr, error) {
	if chunkNo >= d.Len() {
		return FullPtr{}, fmt.Errorf("sqlo1b: directory has %d buckets, no bucket %d", d.Len(), chunkNo)
	}
	return d.ptrs[chunkNo], nil
}

// Set rewrites bucket chunkNo's chunk pointer after a copy-on-write
// chunk rewrite.
func (d *Directory) Set(chunkNo uint64, p FullPtr) error {
	if chunkNo >= d.Len() {
		return fmt.Errorf("sqlo1b: directory has %d buckets, no bucket %d", d.Len(), chunkNo)
	}
	d.ptrs[chunkNo] = p
	return nil
}

// Append adds the next bucket, created by a split, and returns its
// number. The split's new bucket is always bucketNo + 2^level ==
// NumBuckets(level, split), so appending is the only growth.
func (d *Directory) Append(p FullPtr) uint64 {
	d.ptrs = append(d.ptrs, p)
	return d.Len() - 1
}

// Pages encodes the directory as its checkpoint image: full 4 KiB
// pages of little-endian (pos, sum) pairs, the last page zero past
// its live entries.
func (d *Directory) Pages() [][]byte {
	n := DirPages(d.Len())
	out := make([][]byte, 0, n)
	for start := uint64(0); start < d.Len(); start += DirPageEntries {
		end := min(start+DirPageEntries, d.Len())
		page := make([]byte, GroupSize)
		for i, p := range d.ptrs[start:end] {
			binary.LittleEndian.PutUint64(page[i*dirEntrySize:], p.Pos)
			binary.LittleEndian.PutUint64(page[i*dirEntrySize+8:], p.Sum)
		}
		out = append(out, page)
	}
	return out
}

// EncodeDirRoot builds the radix root image from the page pointers
// the store minted while writing Pages.
func EncodeDirRoot(pages []FullPtr) []byte {
	b := make([]byte, len(pages)*dirEntrySize)
	for i, p := range pages {
		binary.LittleEndian.PutUint64(b[i*dirEntrySize:], p.Pos)
		binary.LittleEndian.PutUint64(b[i*dirEntrySize+8:], p.Sum)
	}
	return b
}

// ParseDirRoot decodes the radix root of a directory known to hold
// dirLen buckets (from the hash_epoch committed beside dir_root).
// The image must be exactly one pointer per page: the root's own
// checksum lives in dir_root, so a legal root has one encoding.
func ParseDirRoot(b []byte, dirLen uint64) ([]FullPtr, error) {
	n := DirPages(dirLen)
	if uint64(len(b)) != n*dirEntrySize {
		return nil, fmt.Errorf("sqlo1b: dir root is %d bytes, %d buckets need %d", len(b), dirLen, n*dirEntrySize)
	}
	out := make([]FullPtr, n)
	for i := range out {
		out[i].Pos = binary.LittleEndian.Uint64(b[i*dirEntrySize:])
		out[i].Sum = binary.LittleEndian.Uint64(b[i*dirEntrySize+8:])
	}
	return out, nil
}

// DecodeDirPage decodes page pageNo of a directory of dirLen
// buckets. The image must be exactly one group, zero past the live
// entries; the caller has already verified it against the root's
// full pointer.
func DecodeDirPage(b []byte, pageNo, dirLen uint64) ([]FullPtr, error) {
	if len(b) != GroupSize {
		return nil, fmt.Errorf("sqlo1b: directory page is %d bytes, want %d", len(b), GroupSize)
	}
	if pageNo >= DirPages(dirLen) {
		return nil, fmt.Errorf("sqlo1b: page %d of a %d-page directory", pageNo, DirPages(dirLen))
	}
	live := min(dirLen-pageNo*DirPageEntries, DirPageEntries)
	for i := live * dirEntrySize; i < GroupSize; i++ {
		if b[i] != 0 {
			return nil, fmt.Errorf("sqlo1b: nonzero byte %#x past the live entries at offset %d", b[i], i)
		}
	}
	out := make([]FullPtr, live)
	for i := range out {
		out[i].Pos = binary.LittleEndian.Uint64(b[i*dirEntrySize:])
		out[i].Sum = binary.LittleEndian.Uint64(b[i*dirEntrySize+8:])
	}
	return out, nil
}

// LoadDirectory rebuilds the resident directory from a verified root
// image. fetch returns the raw group behind a page pointer; every
// page is checksum-verified against its root entry before a single
// entry is trusted, which is the two-reads-from-the-superblock
// contract with the checksums walked down the tree.
func LoadDirectory(root []byte, dirLen uint64, fetch func(FullPtr) ([]byte, error)) (*Directory, error) {
	pagePtrs, err := ParseDirRoot(root, dirLen)
	if err != nil {
		return nil, err
	}
	ptrs := make([]FullPtr, 0, dirLen)
	for pageNo, pp := range pagePtrs {
		raw, err := fetch(pp)
		if err != nil {
			return nil, fmt.Errorf("sqlo1b: directory page %d: %w", pageNo, err)
		}
		if err := pp.Verify(raw); err != nil {
			return nil, fmt.Errorf("sqlo1b: directory page %d: %w", pageNo, err)
		}
		ents, err := DecodeDirPage(raw, uint64(pageNo), dirLen)
		if err != nil {
			return nil, err
		}
		ptrs = append(ptrs, ents...)
	}
	if uint64(len(ptrs)) != dirLen {
		return nil, fmt.Errorf("sqlo1b: directory pages held %d entries, hash_epoch says %d", len(ptrs), dirLen)
	}
	return &Directory{ptrs: ptrs}, nil
}
