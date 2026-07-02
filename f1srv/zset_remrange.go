package f1srv

import (
	"encoding/binary"
	"math"
	"strconv"
)

// ZREMRANGEBYRANK, ZREMRANGEBYSCORE, and ZREMRANGEBYLEX are the destructive twins of the range
// reads (spec 2064/f1_rewrite_ltm/07 section 2). Each turns its bound into the same half-open index
// window the read forms compute, collects exactly that window off the family's order index, drops
// both rows of every member in it, and returns the count removed. By-rank and by-score work the
// window on the score family; by-lex works it on the member family. The cost tracks the window, not
// the cardinality, so trimming a hundred members off a million-member board reads and unlinks a
// hundred members and never materializes the collection.

// dropZsetMember removes a member's two rows: the member-family row keyed by the member bytes and
// the score-family row keyed by the sortable score then member. The caller holds the stripe lock
// and adjusts the header count once for the whole batch. member may be an immutable arena subslice;
// it is only read, and the key builders write into kbuf, so there is no aliasing.
func (c *connState) dropZsetMember(zkey, member []byte, score float64) {
	mk := c.zmemberKey(zkey, member)
	c.srv.store.CollRemove(mk)
	c.srv.store.DeleteKind(mk, kindZsetMember)
	sk := c.zscoreKey(zkey, score, member)
	c.srv.store.CollRemove(sk)
	c.srv.store.DeleteKind(sk, kindZsetScore)
}

// dropScoreWindow drops every member whose score-family key sits at ascending indices [lo, hi)
// under the score prefix, and returns how many were removed. The window keys carry the member in
// their tail and the sortable score in their prefix, so each drop needs no extra read.
func (c *connState) dropScoreWindow(zkey, prefix []byte, plen, lo, hi int) int {
	keys := c.collectWindow(prefix, lo, hi)
	n := len(keys)
	for _, k := range keys {
		c.dropZsetMember(zkey, k[plen+8:], decodeSortableScore(k[plen:plen+8]))
	}
	return n
}

// finishRemRange applies a batch removal's count to the header and replies with it. removed is the
// number of members dropped; the caller passes the pre-removal cardinality so the new count is a
// subtraction, not a re-count.
func (c *connState) finishRemRange(zkey []byte, card, removed int, mu locker) {
	if removed > 0 {
		newCard := card - removed
		if newCard < 0 {
			newCard = 0
		}
		if err := c.zsetSetCard(zkey, uint64(newCard)); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	mu.Unlock()
	c.writeInt(int64(removed))
}

// locker is the minimal unlock surface finishRemRange needs, so it can release whichever stripe
// mutex the command took without importing the sync type name at each call site.
type locker interface{ Unlock() }

// cmdZRemRangeByRank removes the members at rank window [start, stop], normalized against the
// cardinality exactly like ZRANGE by index (negative counts from the end, out-of-range clamps or
// empties). It works the window on the score family, which is in rank order.
func (c *connState) cmdZRemRangeByRank(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'zremrangebyrank' command")
		return
	}
	start, err1 := strconv.ParseInt(string(argv[2]), 10, 64)
	stop, err2 := strconv.ParseInt(string(argv[3]), 10, 64)
	if err1 != nil || err2 != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	zkey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(zkey)]
	mu.Lock()
	if c.stringConflict(zkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	card := int64(c.zsetCard(zkey))
	if card == 0 {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	if start < 0 {
		start += card
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += card
	}
	if start > stop || start >= card {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	if stop >= card {
		stop = card - 1
	}
	prefix := c.zscorePrefix(zkey)
	plen := len(prefix)
	removed := c.dropScoreWindow(zkey, prefix, plen, int(start), int(stop)+1)
	c.finishRemRange(zkey, int(card), removed, mu)
}

// cmdZRemRangeByScore removes the members whose scores fall in [min, max], with the '(' exclusive
// prefixes and the infinities the by-score reads use. It ranks both bounds on the score family and
// drops the window between them.
func (c *connState) cmdZRemRangeByScore(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'zremrangebyscore' command")
		return
	}
	lo, loExcl, err1 := parseScoreSpec(argv[2])
	hi, hiExcl, err2 := parseScoreSpec(argv[3])
	if err1 != nil || err2 != nil {
		c.writeErr("ERR min or max is not a float")
		return
	}
	zkey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(zkey)]
	mu.Lock()
	if c.stringConflict(zkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	card := int(c.zsetCard(zkey))
	if card == 0 {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	prefix := c.zscorePrefix(zkey)
	plen := len(prefix)
	startIdx := c.zscoreRankBoundary(prefix, lo, loExcl, card)
	endIdx := c.zscoreRankBoundary(prefix, hi, !hiExcl, card)
	if startIdx >= endIdx {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	removed := c.dropScoreWindow(zkey, prefix, plen, startIdx, endIdx)
	c.finishRemRange(zkey, card, removed, mu)
}

// cmdZRemRangeByLex removes the members whose bytes fall in [min, max] under the lex order, with the
// '['/'(' bounds and the '-'/'+' infinities the by-lex reads use. It ranks both bounds on the
// member family, then for each member in the window reads its score from the member row to address
// and drop the matching score row.
func (c *connState) cmdZRemRangeByLex(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'zremrangebylex' command")
		return
	}
	loKind, loMember, loExcl, err1 := parseLexSpec(argv[2])
	hiKind, hiMember, hiExcl, err2 := parseLexSpec(argv[3])
	if err1 != nil || err2 != nil {
		c.writeErr("ERR min or max not valid string range item")
		return
	}
	zkey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(zkey)]
	mu.Lock()
	if c.stringConflict(zkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	card := int(c.zsetCard(zkey))
	if card == 0 {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	prefix := c.zmemberPrefix(zkey)
	plen := len(prefix)

	startIdx := 0
	switch loKind {
	case lexMinInf:
		startIdx = 0
	case lexMaxInf:
		startIdx = card
	default:
		startIdx = c.zlexRankBoundary(prefix, loMember, loExcl, card)
	}
	var endIdx int
	switch hiKind {
	case lexMaxInf:
		endIdx = card
	case lexMinInf:
		endIdx = 0
	default:
		endIdx = c.zlexRankBoundary(prefix, hiMember, !hiExcl, card)
	}
	if startIdx >= endIdx {
		mu.Unlock()
		c.writeInt(0)
		return
	}

	// The window keys are member-family rows; each carries its score in the row value, which is
	// what addresses the score row to drop alongside it.
	keys := c.collectWindow(prefix, startIdx, endIdx)
	removed := 0
	for _, mk := range keys {
		member := mk[plen:]
		v, ok := c.srv.store.GetKind(mk, c.vbuf[:0], kindZsetMember)
		c.vbuf = v
		if !ok {
			continue
		}
		score := math.Float64frombits(binary.LittleEndian.Uint64(v))
		c.dropZsetMember(zkey, member, score)
		removed++
	}
	c.finishRemRange(zkey, card, removed, mu)
}
