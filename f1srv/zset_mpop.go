package f1srv

import "strconv"

// ZMPOP pops from the first non-empty sorted set among the given keys (spec
// 2064/f1_rewrite_ltm/07 section 2, the multi-key pop view). It is the non-blocking multi-key twin
// of ZPOPMIN/ZPOPMAX: it probes the keys left to right, and on the first one with members it pops
// the count lowest (MIN) or highest (MAX) exactly as ZPOP does, off the score family's window, then
// returns that key's name with the popped member-score pairs and stops. If every key is empty the
// reply is a null array.
//
// The reply nests one level deeper than ZPOP: a two-element array of the key name and an array of
// [member, score] pairs, rather than ZPOP's flat member, score, member, score. Only the first
// non-empty key is touched, so the cost is one probe per empty key ahead of it plus the bounded pop,
// never a walk of any set.
func (c *connState) cmdZMPop(argv [][]byte) {
	// ZMPOP numkeys key [key ...] <MIN | MAX> [COUNT count]. The floor is three so numkeys parses
	// before the fuller arity is known: "ZMPOP 0 MIN" must report the numkeys error, not arity.
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'zmpop' command")
		return
	}
	numkeys, err := strconv.Atoi(string(argv[1]))
	if err != nil || numkeys <= 0 {
		c.writeErr("ERR numkeys should be greater than 0")
		return
	}
	keysEnd := 2 + numkeys
	// Room for the key list plus the mandatory MIN/MAX token.
	if len(argv) < keysEnd+1 {
		c.writeErr("ERR syntax error")
		return
	}
	var max bool
	switch {
	case eqFold(argv[keysEnd], "MIN"):
		max = false
	case eqFold(argv[keysEnd], "MAX"):
		max = true
	default:
		c.writeErr("ERR syntax error")
		return
	}
	count := 1
	rest := argv[keysEnd+1:]
	switch len(rest) {
	case 0:
	case 2:
		if !eqFold(rest[0], "COUNT") {
			c.writeErr("ERR syntax error")
			return
		}
		n, err := strconv.Atoi(string(rest[1]))
		if err != nil || n <= 0 {
			c.writeErr("ERR count should be greater than 0")
			return
		}
		count = n
	default:
		c.writeErr("ERR syntax error")
		return
	}

	for _, zkey := range argv[2:keysEnd] {
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
			continue
		}
		cnt := count
		if cnt > card {
			cnt = card
		}
		lo := 0
		if max {
			lo = card - cnt
		}
		prefix := c.zscorePrefix(zkey)
		plen := len(prefix)
		keys := c.collectWindow(prefix, lo, lo+cnt)
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

		// Reply: [key, [[member, score], ...]]. MIN emits ascending, MAX descending, the same
		// window read back to front.
		c.writeArrayHeader(2)
		c.writeBulk(zkey)
		c.writeArrayHeader(len(keys))
		if max {
			for i := len(keys) - 1; i >= 0; i-- {
				c.writeArrayHeader(2)
				c.writeBulk(keys[i][plen+8:])
				c.writeScore(decodeSortableScore(keys[i][plen : plen+8]))
			}
			return
		}
		for _, k := range keys {
			c.writeArrayHeader(2)
			c.writeBulk(k[plen+8:])
			c.writeScore(decodeSortableScore(k[plen : plen+8]))
		}
		return
	}

	// Every key was empty or missing.
	c.writeNilArray()
}
