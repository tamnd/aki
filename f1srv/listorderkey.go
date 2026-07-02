package f1srv

import "encoding/binary"

// Order keys are the sort keys the order-statistic list model stores its elements under
// (spec 2064/f1_rewrite_ltm/08 sections 3 to 6, and impl/27). A list element's identity is its
// position in the deque, not its bytes, so the model manufactures an order-preserving key for
// every element and stores the element one-per-row under it, exactly the element-per-row shape
// the hash, set, and zset already use. A byte comparison of two order keys equals the two
// elements' list order, so the element rows of one list sort head to tail and the shared
// order-statistic index (engine/f1raw oindex, task 165) maps a client position to a key in
// O(log n). This file is the order-key allocator, the piece that decides what key a new element
// gets. It is deliberately standalone and unwired: the live list still runs the dense-position
// model, and this unit is proven on its own before any command moves onto it.
//
// There are two allocation paths, because a list only ever grows in two ways.
//
// End growth (RPUSH, LPUSH), the hot path. Elements land past an end, so their keys come from a
// per-end int64 sequence: RPUSH takes the next tail seq and steps it up, LPUSH takes the next
// head seq and steps it down. orderKeyEnd encodes that seq order-preservingly into a fixed 8
// bytes, the same width and encoding the dense model already uses, so the hot push path pays no
// more than it does today. The one change from the dense model is that a popped seq is retired,
// never reused: the sequence only ever advances outward. Retiring keys is what lets a pop delete
// its row off the stripe lock, because no later push can ever target that key again (impl/27).
//
// Interior growth (LINSERT before or after a pivot), the rare path. A new element goes strictly
// between two neighbours, so its key must sort strictly between their two keys with no surviving
// row moving. orderKeyBetween manufactures such a key. Repeated inserts at one spot subdivide,
// growing the key a byte at a time rather than hitting the fixed precision wall a float midpoint
// scheme would (section 13.1, scheme B over scheme A); a bounded rebalance for pathological
// hammering is a later slice (section 13.2). Keys allocated here are never reused either.

// orderKeyEnd encodes an end-sequence value into an 8-byte order-preserving key. The encoding is
// big-endian uint64 with the sign bit flipped, so a plain byte comparison of two keys equals the
// signed order of their seqs: a head seq pushed below zero sorts before a tail seq above it, and
// adjacent seqs produce adjacent keys. This is byte-for-byte the encoding listElemKey uses for a
// dense position, so an element's on-disk key shape does not change when a list moves onto the
// order-statistic model, only how the seq is chosen (retired, never reused) does.
func orderKeyEnd(seq int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(seq)^(1<<63))
	return b[:]
}

// orderKeyBetween returns a key k with lo < k < hi under a plain byte comparison, so a new
// element inserted between two neighbours sorts strictly between them. A nil lo means "before
// every key" (insert before the head) and a nil hi means "after every key" (insert past the
// tail); the caller guarantees lo < hi whenever both are set, which LINSERT does by passing a
// pivot and its immediate neighbour.
//
// The algorithm is a base-256 midpoint descent. At each byte position it takes lo's digit (or 0
// past lo's end) and hi's digit (or 256 past hi's end or when hi is unbound) and, if a value fits
// strictly between them, emits that midpoint and stops. When the two digits are equal or adjacent
// there is no room at this position, so it copies lo's digit and descends; the first position
// where lo's digit is strictly below hi's unbinds hi (any suffix now keeps k below hi), and the
// next descent always finds room against the 256 ceiling, so the loop terminates in at most one
// step past the point the two keys diverge.
//
// The returned key never ends in 0x00. The final emitted byte is a midpoint strictly above lo's
// digit, so it is at least 1; only interior copied digits can be 0. That invariant is load
// bearing: if a stored key could end in 0x00, a hi of the form lo followed by zero bytes would
// have no strict-between key expressible as a copy-and-descend, and repeated inserts could wedge.
// Because no key this allocator emits ends in 0x00, and backbone keys are fixed 8-byte (never a
// strict prefix of a shorter key), that degenerate hi never arises. See listorderkey_test.go for
// the exhaustive and randomized proof of lo < k < hi and the no-trailing-zero invariant.
func orderKeyBetween(lo, hi []byte) []byte {
	var out []byte
	for i := 0; ; i++ {
		l := 0
		if i < len(lo) {
			l = int(lo[i])
		}
		h := 256
		if hi != nil && i < len(hi) {
			h = int(hi[i])
		}
		mid := (l + h) / 2
		if mid != l {
			// A value fits strictly between the two digits: emit it and stop. mid is above l
			// (so out > lo) and below h (so out < hi), and mid >= 1 so out cannot end in 0x00.
			return append(out, byte(mid))
		}
		// No room at this digit. Copy lo's digit and descend. Crossing a position where lo's
		// digit is strictly below hi's means any suffix keeps out below hi, so drop the hi bound
		// and the next descent finds room against the 256 ceiling.
		out = append(out, byte(l))
		if l < h {
			hi = nil
		}
	}
}
