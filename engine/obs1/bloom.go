// The segment footer's blocked bloom filter (doc 03 section 5): one
// 64-byte block per probe target so a membership test touches a single
// cache line. 10 bits per key and 7 probes are the doc defaults; both are
// O1c lab knobs, and the on-bucket form carries no parameters, so the
// query side derives everything from the filter's length.
package obs1

import "hash/fnv"

const (
	bloomBitsPerKey = 10
	bloomProbes     = 7
	bloomBlockBytes = 64
)

// bloomHash gives two independent hashes: h1 picks the block, h2 feeds
// the probe positions. h2 must not be derived arithmetically from h1, or
// every probe collapses to a function of h1's low bits and the filter's
// false positive rate explodes; hashing the key again with a salt byte
// keeps them independent. FNV-1a is stdlib, stable across builds and
// platforms, and plenty for a filter whose worst failure is a wasted GET.
func bloomHash(key []byte) (h1, h2 uint64) {
	f := fnv.New64a()
	f.Write(key)
	h1 = f.Sum64()
	f.Write([]byte{0x0b})
	h2 = f.Sum64()
	return h1, h2
}

// BuildBloom builds a blocked bloom filter over the member keys. The
// result is always a whole number of 64-byte blocks; zero keys give a
// single empty block so the filter is never zero-length.
func BuildBloom(keys [][]byte) []byte {
	nblocks := (len(keys)*bloomBitsPerKey + bloomBlockBytes*8 - 1) / (bloomBlockBytes * 8)
	if nblocks == 0 {
		nblocks = 1
	}
	filter := make([]byte, nblocks*bloomBlockBytes)
	for _, key := range keys {
		h1, h2 := bloomHash(key)
		block := filter[int(h1%uint64(nblocks))*bloomBlockBytes:]
		for i := range bloomProbes {
			// Disjoint 9-bit windows of h2: 7 probes use 63 of its bits.
			bit := (h2 >> (9 * i)) % (bloomBlockBytes * 8)
			block[bit/8] |= 1 << (bit % 8)
		}
	}
	return filter
}

// BloomMayContain reports whether the key may be a member. A malformed
// filter answers true, because the filter is an optimization and its
// failure mode must be a wasted GET, never a missed record.
func BloomMayContain(filter, key []byte) bool {
	if len(filter) == 0 || len(filter)%bloomBlockBytes != 0 {
		return true
	}
	nblocks := len(filter) / bloomBlockBytes
	h1, h2 := bloomHash(key)
	block := filter[int(h1%uint64(nblocks))*bloomBlockBytes:]
	for i := range bloomProbes {
		bit := (h2 >> (9 * i)) % (bloomBlockBytes * 8)
		if block[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}
