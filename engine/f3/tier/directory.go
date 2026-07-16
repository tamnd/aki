package tier

import (
	"bytes"
	"sort"
)

// The resident chunk directory (spec 2064/f3/06 section 6.3): per cold
// collection, an ordered array of chunk descriptors, owner-local, never
// persisted, rebuilt at open from frame reads. It is the structure that keeps a
// cold collection's ranked and ranged queries resident: a directory over chunks
// answers "which chunk owns this discriminator" with a binary search and "how many
// elements sit left of this chunk" with a prefix sum, so a cold read pays at most
// one pread of the owning chunk and never a metadata read. About 32 bytes per
// chunk (0.16 to 0.32 bytes per cold element at the packing factor), which is what
// keeps the cold metadata fully resident where an element-per-row index would not
// fit.
//
// One directory is shared in shape across every collection type; only the
// discriminator changes (member hash for a set, score-then-member for a zset,
// position for a list, field for a hash, id for a stream). The contract is that a
// type encodes its discriminator so byte-lexicographic order is the collection's
// logical order, which every one of those does with an order-preserving encoding,
// so the directory compares discriminators with bytes.Compare and needs no
// per-type comparator.

// maxDisc is the inline first-discriminator width a descriptor keeps (section
// 6.3, "bounded, up to 32 bytes inline"). A longer discriminator is truncated by
// the caller before it reaches here; a truncated prefix still orders, so the
// binary search stays correct, and the pread that confirms a hit reads the real
// element bytes anyway.
const maxDisc = 32

// The descriptor status bits, a chunk's small resident state: one being promoted
// out of the cold tier, one whose native neighbors changed and wants a repack, one
// already superseded and awaiting the compactor. Owner-only, never persisted.
const (
	DescPromoting = 1 << 0
	DescDirty     = 1 << 1
	DescDead      = 1 << 2
)

// desc is one chunk's resident descriptor. It is a flat value with no pointers so
// the directory is one contiguous slice, cache-friendly on the binary-search
// descent, and the memory math (about 32 bytes per chunk) holds without a
// per-descriptor heap allocation.
type desc struct {
	disc   [maxDisc]byte // first discriminator, inline
	off    uint64        // cold-region frame offset
	count  uint32        // elements packed in the chunk
	dlen   uint8         // discriminator length, at most maxDisc
	status uint8         // status bits
}

func (d *desc) discBytes() []byte { return d.disc[:d.dlen] }

// Directory is the ordered descriptor array plus the running element total. The
// total lets SCARD-class counts and rank descents avoid walking the descriptors,
// and it stays exact across insert and remove.
type Directory struct {
	descs []desc
	total uint64
}

// Len reports the live chunk count.
func (dir *Directory) Len() int { return len(dir.descs) }

// Total reports the element count across every live chunk, the resident figure a
// cold SCARD adds to the native structure's count.
func (dir *Directory) Total() uint64 { return dir.total }

// search returns the index where disc is or would be, by discriminator order. The
// bool reports an exact discriminator match (a chunk whose first element is disc).
func (dir *Directory) search(disc []byte) (int, bool) {
	i := sort.Search(len(dir.descs), func(i int) bool {
		return bytes.Compare(dir.descs[i].discBytes(), disc) >= 0
	})
	if i < len(dir.descs) && bytes.Equal(dir.descs[i].discBytes(), disc) {
		return i, true
	}
	return i, false
}

// Insert adds a chunk at off with count elements whose first discriminator is
// disc, keeping the array in discriminator order. disc is copied inline (truncated
// to maxDisc), so the caller's buffer is free to recycle. A discriminator already
// present is a caller error (chunks partition the space with no overlap, section
// 6.5); Insert overwrites the colliding descriptor rather than duplicating a key,
// which keeps the invariant that the search is unambiguous.
func (dir *Directory) Insert(disc []byte, count uint32, off uint64) {
	i, exact := dir.search(disc)
	if exact {
		dir.total -= uint64(dir.descs[i].count)
		dir.descs[i].off = off
		dir.descs[i].count = count
		dir.descs[i].status = 0
		dir.total += uint64(count)
		return
	}
	var d desc
	d.dlen = uint8(copy(d.disc[:], disc))
	d.off = off
	d.count = count
	dir.descs = append(dir.descs, desc{})
	copy(dir.descs[i+1:], dir.descs[i:])
	dir.descs[i] = d
	dir.total += uint64(count)
}

// Floor returns the index of the chunk that owns disc: the last chunk whose first
// discriminator is at or below disc, since chunk i spans [first_i, first_{i+1}).
// ok is false when disc sorts below every chunk, which is a resident miss (the
// discriminator is outside the cold tier's range, so no pread is owed).
func (dir *Directory) Floor(disc []byte) (idx int, ok bool) {
	i := sort.Search(len(dir.descs), func(i int) bool {
		return bytes.Compare(dir.descs[i].discBytes(), disc) > 0
	})
	if i == 0 {
		return 0, false
	}
	return i - 1, true
}

// RankBefore sums the element counts of every chunk left of idx, the prefix a rank
// descent accumulates before it reads the owning chunk (section 6.3). It is a
// linear sum here; the Fenwick relief the spec names is a lab knob that engages
// only above a chunk-count threshold, so the plain sum is the correct starting
// point and stays exact.
func (dir *Directory) RankBefore(idx int) uint64 {
	var n uint64
	for i := 0; i < idx && i < len(dir.descs); i++ {
		n += uint64(dir.descs[i].count)
	}
	return n
}

// At returns the off, count, and status of the chunk at idx, for the caller that
// walks the directory in order (a range read, a scan, a promotion pass).
func (dir *Directory) At(idx int) (off uint64, count uint32, status uint8) {
	d := &dir.descs[idx]
	return d.off, d.count, d.status
}

// DiscAt returns a copy of the chunk's first discriminator, for a range read that
// merges the cold run with the resident run by discriminator order.
func (dir *Directory) DiscAt(idx int) []byte {
	return append([]byte(nil), dir.descs[idx].discBytes()...)
}

// SetStatus replaces the status bits of the chunk at idx (mark promoting before a
// promotion pread, dirty before a repack, dead before the compactor reclaims it).
func (dir *Directory) SetStatus(idx int, status uint8) { dir.descs[idx].status = status }

// Remove drops the chunk at idx, keeping the array ordered and the total exact.
// This is the promotion path (the chunk unpacked into the native structure and its
// frame marked dead) and the compaction path (a folded or split chunk's old
// descriptor).
func (dir *Directory) Remove(idx int) {
	dir.total -= uint64(dir.descs[idx].count)
	copy(dir.descs[idx:], dir.descs[idx+1:])
	dir.descs = dir.descs[:len(dir.descs)-1]
}

// descBytes is one descriptor's heap cell width: the 32-byte inline discriminator,
// the eight-byte offset, the four-byte count, and the two one-byte fields, padded
// to a struct that a []desc packs at 48 bytes on a 64-bit target.
const descBytes = 48

// Bytes is the directory's resident heap footprint: the descriptor array's
// capacity times the per-descriptor cell width. It is the directory's term of a
// cold collection's resident-byte estimate (spec 2064/f3/06 section 6.3), so a
// demoted set counts the metadata it keeps resident against the slab it freed.
func (dir *Directory) Bytes() int { return cap(dir.descs) * descBytes }
