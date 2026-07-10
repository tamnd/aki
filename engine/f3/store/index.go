package store

// The per-shard index: an open-addressed table of 64-byte cache-line buckets
// mapping key hash to a 48-bit arena address, behind an extendible-hashing
// directory of fixed-size segments (spec 2064/f3/04 section 2). One pinned
// worker owns the whole shard, so every load is a plain load and every store
// is a plain store: no CAS on publish, no fence, no retry loop. Growth is a
// dashtable-style segment split, never a rehash: a full segment's entries
// redistribute into two children by the next hash bit, touching nothing
// outside that one segment, so steady-state operations carry no rehash cursor
// and no transient double-size bucket array.

const (
	slotsPerBucket = 7   // 7 entry words + the link word = one 64-byte line
	homeBuckets    = 128 // home buckets per index segment: 8KiB of buckets
	chainCap       = 2   // overflow buckets a home bucket may chain before the segment splits

	// segCapacity is a segment's nominal entry capacity, the base the split
	// threshold is a fraction of.
	segCapacity = homeBuckets * slotsPerBucket

	// splitLoadNum/100 is the split threshold: a segment splits when its live
	// count crosses this fraction of nominal capacity, so a split produces two
	// children near 44 percent that refill through the band the workload uses.
	// Chains absorb home-bucket misses below the threshold, which is what makes
	// the trigger a load factor rather than a first-placement failure. Lab
	// constant (LAB-3).
	splitLoadNum = 87

	// maxDepth caps the directory depth. A segment already at maxDepth grows
	// its chains past chainCap instead of splitting, so the index degrades to
	// longer probes rather than an unbounded directory.
	maxDepth = 24
)

// bucket is one cache line: seven entry words and a link. The link is the
// ordinal-plus-one of the next overflow bucket in the owning segment's
// overflow slab, or zero for none. A zero entry word is an empty slot; a probe
// scans the whole bucket and its chain regardless, so a deleted entry is just
// a hole and no tombstone state exists.
type bucket struct {
	slots [slotsPerBucket]uint64
	link  uint64
}

// indexSegment is 128 home buckets plus their overflow chains. localDepth is
// how many directory bits this segment consumes; used counts live entries and
// chained the subset living in overflow buckets; threshold is the split
// trigger with this segment's jitter already applied. The overflow slab is
// per-segment so a split frees a segment's chains with it and never strands a
// bucket another segment links.
type indexSegment struct {
	localDepth uint8
	used       uint16
	chained    uint16
	threshold  uint16
	buckets    [homeBuckets]bucket
	overflow   []bucket
}

// index is the directory plus the segment slab. dir holds segment ordinals
// into segs, indexed by the top gd bits of the hash; multiple slots point at
// one segment while its localDepth is below gd. freeOrds recycles the ordinals
// of split-away segments. splits counts segment splits for tests and the
// ledger.
type index struct {
	gd       uint8
	dir      []uint32
	segs     []*indexSegment
	freeOrds []uint32
	splits   uint64
}

// splitThreshold is the split trigger for the segment at ordinal ord: the base
// load factor plus a deterministic per-segment jitter of about two percent, so
// uniform load does not split every segment in the same window and the
// shard-level utilization dip flattens into overlapping sawtooths.
func splitThreshold(ord uint32) uint16 {
	base := segCapacity * splitLoadNum / 100
	jitter := int((uint64(ord)*0x9e3779b97f4a7c15)>>59) - 16 // -16..+15 entries
	return uint16(base + jitter)
}

func newIndex() index {
	seg := &indexSegment{threshold: splitThreshold(0)}
	return index{
		gd:   0,
		dir:  []uint32{0},
		segs: []*indexSegment{seg},
	}
}

// dirIndex takes the top gd bits of the hash. The directory bits sit above the
// in-segment bucket bits (bits 8..14) and below any future shard bits, so a
// split, which consumes one more directory bit, never changes a key's
// in-segment bucket or its tag.
func dirIndex(h uint64, gd uint8) uint64 {
	if gd == 0 {
		return 0
	}
	return h >> (64 - gd)
}

// bucketIndex picks the home bucket from hash bits 8..14, disjoint from the
// directory bits and the tag by construction.
func bucketIndex(h uint64) uint64 { return (h >> 8) & (homeBuckets - 1) }

// segFor returns the segment the hash routes to and its ordinal.
func (ix *index) segFor(h uint64) (*indexSegment, uint32) {
	ord := ix.dir[dirIndex(h, ix.gd)]
	return ix.segs[ord], ord
}

// allocOrd hands out a slab ordinal, recycling a split-away segment's slot
// before growing the slab.
func (ix *index) allocOrd(seg *indexSegment) uint32 {
	if n := len(ix.freeOrds); n > 0 {
		ord := ix.freeOrds[n-1]
		ix.freeOrds = ix.freeOrds[:n-1]
		ix.segs[ord] = seg
		return ord
	}
	ix.segs = append(ix.segs, seg)
	return uint32(len(ix.segs) - 1)
}

// findEntry probes the home bucket and its overflow chain for a live entry
// whose tag matches and whose record carries this key. It returns a pointer to
// the entry word so a replace is one plain store and a delete goes through
// deleteAt, plus whether the slot lives in an overflow bucket (the chained
// counter's book-keeping). The tag rejects a slot before the arena is touched;
// a tag hit verifies the key bytes in the record.
func (s *Store) findEntry(h uint64, key []byte) (slot *uint64, addr uint64, inOverflow bool) {
	seg, _ := s.idx.segFor(h)
	tag := tagOf(h)
	b := &seg.buckets[bucketIndex(h)]
	overflow := false
	for {
		for i := 0; i < slotsPerBucket; i++ {
			w := b.slots[i]
			if w != 0 && w>>tagShift == tag && s.recordMatches(w&addrMask, key) {
				return &b.slots[i], w & addrMask, overflow
			}
		}
		if b.link == 0 {
			return nil, 0, false
		}
		b = &seg.overflow[b.link-1]
		overflow = true
	}
}

// insertEntry places a new entry word, splitting the target segment as many
// times as the placement needs room. The caller guarantees the key is not
// already present (a replace stores through the slot findEntry returned
// instead).
func (s *Store) insertEntry(h uint64, word uint64) {
	for {
		seg, ord := s.idx.segFor(h)
		atCap := seg.localDepth >= maxDepth
		if !atCap && seg.used >= seg.threshold {
			s.splitSegment(ord)
			continue
		}
		if placeEntry(seg, h, word, atCap) {
			return
		}
		// The chain hit its cap before the load threshold did: split on chain
		// pressure so the probe tail stays bounded.
		s.splitSegment(ord)
	}
}

// placeEntry puts word in the first empty slot of h's home bucket or its
// chain, growing the chain by one overflow bucket when every slot is taken and
// the chain is under chainCap. unbounded lifts the cap for a segment that can
// no longer split. It reports false when placement needs a split first.
func placeEntry(seg *indexSegment, h uint64, word uint64, unbounded bool) bool {
	b := &seg.buckets[bucketIndex(h)]
	chainLen := 0
	inOverflow := false
	for {
		for i := 0; i < slotsPerBucket; i++ {
			if b.slots[i] == 0 {
				b.slots[i] = word
				seg.used++
				if inOverflow {
					seg.chained++
				}
				return true
			}
		}
		if b.link != 0 {
			b = &seg.overflow[b.link-1]
			chainLen++
			inOverflow = true
			continue
		}
		if chainLen >= chainCap && !unbounded {
			return false
		}
		// Grow the chain. The append may move the slab, so the link is set
		// through a re-fetched pointer, never the possibly stale b.
		seg.overflow = append(seg.overflow, bucket{})
		newIdx := uint64(len(seg.overflow)) // ordinal+1
		if inOverflow {
			// b was a slab pointer; re-derive the linking bucket from the
			// chain walk position: it is the bucket at ordinal newIdx-2's
			// predecessor only in general chains, so relink via a fresh walk.
			relinkTail(seg, h, newIdx)
		} else {
			seg.buckets[bucketIndex(h)].link = newIdx
		}
		nb := &seg.overflow[newIdx-1]
		nb.slots[0] = word
		seg.used++
		seg.chained++
		return true
	}
}

// relinkTail walks h's chain to its last bucket and links the freshly appended
// overflow bucket there. It runs only on the chain-growth path, once per new
// overflow bucket, so the extra walk is off the per-entry cost.
func relinkTail(seg *indexSegment, h uint64, newIdx uint64) {
	b := &seg.buckets[bucketIndex(h)]
	for b.link != 0 && b.link != newIdx {
		b = &seg.overflow[b.link-1]
	}
	b.link = newIdx
}

// deleteAt clears a found entry. The emptied slot is a hole the probe
// tolerates, so nothing shifts and nothing is marked.
func (s *Store) deleteAt(h uint64, slot *uint64, inOverflow bool) {
	seg, _ := s.idx.segFor(h)
	*slot = 0
	seg.used--
	if inOverflow {
		seg.chained--
	}
}

// splitSegment replaces the segment at ordinal ord with two children at
// localDepth+1, redistributing its entries by the next hash bit. The hash is
// recomputed from each record's key bytes, so the entry's in-segment bucket
// and tag are preserved by construction and only the directory routing
// changes. The split touches this one segment's entries and its directory
// slots; every other segment's buckets are never read or written, which is the
// no-pause property the growth test pins.
func (s *Store) splitSegment(ord uint32) {
	ix := &s.idx
	old := ix.segs[ord]
	d := old.localDepth

	// The directory doubles first when the segment already consumes every
	// directory bit. Doubling is a copy of a few KiB: new slot 2i and 2i+1
	// both point where old slot i did.
	if d == ix.gd {
		ndir := make([]uint32, len(ix.dir)*2)
		for i, o := range ix.dir {
			ndir[2*i] = o
			ndir[2*i+1] = o
		}
		ix.dir = ndir
		ix.gd++
	}

	child0 := &indexSegment{localDepth: d + 1}
	child1 := &indexSegment{localDepth: d + 1}
	ord0 := ord // child0 reuses the old ordinal so unrelated dir slots keep their value
	ix.segs[ord0] = child0
	ord1 := ix.allocOrd(child1)
	child0.threshold = splitThreshold(ord0)
	child1.threshold = splitThreshold(ord1)

	// Repoint the directory: the old segment covered every slot sharing its
	// top-d bits; the (d+1)-th bit from the top now selects the child.
	// The covered slots are contiguous and span-aligned: all slots whose top d
	// bits equal the old segment's prefix. Recover the base from the first slot
	// still pointing at the old ordinal; the scan is O(len(dir)) and runs once
	// per split, against a directory that stays a few KiB.
	span := uint64(1) << (ix.gd - d)
	base := uint64(0)
	for i, o := range ix.dir {
		if o == ord0 {
			base = uint64(i) &^ (span - 1)
			break
		}
	}
	half := span / 2
	for j := uint64(0); j < span; j++ {
		if j < half {
			ix.dir[base+j] = ord0
		} else {
			ix.dir[base+j] = ord1
		}
	}

	// Redistribute. Entries re-place through the children's own home buckets;
	// a child that comes out over its own chain cap splits again through the
	// generic insert path.
	redistribute := func(b *bucket) {
		for i := 0; i < slotsPerBucket; i++ {
			w := b.slots[i]
			if w == 0 {
				continue
			}
			h := Hash(s.keyAt(w & addrMask))
			s.insertEntry(h, w)
		}
	}
	for i := range old.buckets {
		redistribute(&old.buckets[i])
	}
	for i := range old.overflow {
		redistribute(&old.overflow[i])
	}
	ix.splits++
}
