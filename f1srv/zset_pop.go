package f1srv

import "strconv"

// ZPOPMIN and ZPOPMAX remove and return the members with the lowest or highest scores (spec
// 2064/f1_rewrite_ltm/07 section 2, the order view). They are bounded extreme pops: the score
// family is already in score-then-member order, so the count lowest members are the window at
// indices [0, count) and the count highest are the window at [card-count, card). Each is one
// positional seek to the window's first row plus a bounded forward walk, so popping k members off a
// million-member board reads k rows, never the whole set.
//
// Reply is a flat array of member, score, member, score. ZPOPMIN emits ascending (lowest first);
// ZPOPMAX emits descending (highest first), the same window read back to front. The no-count form
// and the count form share this flat shape in RESP2, so the reply builder does not distinguish
// them.
func (c *connState) cmdZPop(argv [][]byte, max bool) {
	name := "zpopmin"
	if max {
		name = "zpopmax"
	}
	if len(argv) < 2 || len(argv) > 3 {
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	count := 1
	if len(argv) == 3 {
		n, err := strconv.Atoi(string(argv[2]))
		if err != nil {
			c.writeErr("ERR value is not an integer or out of range")
			return
		}
		if n < 0 {
			c.writeErr("ERR value is out of range, must be positive")
			return
		}
		count = n
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
	if card == 0 || count == 0 {
		mu.Unlock()
		c.writeArrayHeader(0)
		return
	}
	if count > card {
		count = card
	}

	// The popped window in the score family's ascending order. ZPOPMIN takes the front, ZPOPMAX
	// the back; both are collected ascending here and ZPOPMAX reverses at emit time.
	lo := 0
	if max {
		lo = card - count
	}
	prefix := c.zscorePrefix(zkey)
	plen := len(prefix)
	keys := c.collectWindow(prefix, lo, lo+count)

	// Remove each member's two rows before replying. The score keys are immutable arena
	// subslices, so unlinking them from the ordered index leaves the bytes valid to emit.
	for _, k := range keys {
		member := k[plen+8:]
		c.srv.store.CollRemove(k)
		c.srv.store.DeleteKind(k, kindZsetScore)
		mk := c.zmemberKey(zkey, member)
		c.srv.store.CollRemove(mk)
		c.srv.store.DeleteKind(mk, kindZsetMember)
	}
	newCard := card - len(keys)
	if err := c.zsetSetCard(zkey, uint64(newCard)); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	mu.Unlock()

	c.writeArrayHeader(len(keys) * 2)
	if max {
		for i := len(keys) - 1; i >= 0; i-- {
			c.writeBulk(keys[i][plen+8:])
			c.writeScore(decodeSortableScore(keys[i][plen : plen+8]))
		}
		return
	}
	for _, k := range keys {
		c.writeBulk(k[plen+8:])
		c.writeScore(decodeSortableScore(k[plen : plen+8]))
	}
}
