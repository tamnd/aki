package f1srv

import (
	"strconv"
	"time"
)

// The blocking sorted-set pops (BZPOPMIN, BZPOPMAX, BZMPOP) wait for a member instead of
// replying immediately when their keys are empty. They are the sorted-set twins of the
// blocking list pops (spec 2064/f1_rewrite_ltm/07 section 2, the multi-key pop view, read
// with the list model's section 9 on blocking): a blocked client holds no member, reads no
// row, and materializes nothing, it is one entry in the shared per-key wait queue plus a
// timer. The wakeup is an ordinary extreme pop through the same stripe lock a foreground
// ZPOPMIN takes, so a member is delivered exactly once to exactly one client, in the order
// the clients blocked.
//
// The wait registry is the one blockReg the list and stream commands share, and the FIFO
// wake in signalListKey is exactly right here: a popped member goes to a single client, so
// only the queue head is woken, the same as a list element. A sorted set becomes ready when
// it gains members, so cmdZAdd and the insert branch of cmdZIncrBy call signalListKey after
// a successful add, the same way the push commands signal a list. The atomic fast-out in
// signalListKey keeps a ZADD with no client blocked anywhere off the registry lock, so the
// hot add path pays a single atomic load.
//
// Parking uses the connection's own goroutine and only when connState.blockable is set, the
// same rule the list pops follow: under the shared-goroutine reactor a park would stall the
// loop, so there the command serves non-blocking (immediate member or nil). The score column
// is formatted exactly like ZSCORE through writeScore.

// zpopExtreme removes up to count extreme members from zkey and returns their score-family
// keys in ascending score order together with the prefix length, so the caller reads member
// = k[plen+8:] and score = decodeSortableScore(k[plen : plen+8]). It returns a nil slice when
// the set is empty. The caller holds the zkey stripe lock and has already ruled out a string
// conflict. This is the same bounded window pop ZPOPMIN/ZPOPMAX and ZMPOP take: one positional
// seek to the window plus a walk of count rows, never a scan of the whole set.
func (c *connState) zpopExtreme(zkey []byte, max bool, count int) (keys [][]byte, plen int, err error) {
	card := int(c.zsetCard(zkey))
	if card == 0 {
		return nil, 0, nil
	}
	if count > card {
		count = card
	}
	lo := 0
	if max {
		lo = card - count
	}
	prefix := c.zscorePrefix(zkey)
	plen = len(prefix)
	keys = c.collectWindow(prefix, lo, lo+count)
	for _, k := range keys {
		member := k[plen+8:]
		c.srv.store.CollRemove(k)
		c.srv.store.DeleteKind(k, kindZsetScore)
		mk := c.zmemberKey(zkey, member)
		c.srv.store.CollRemove(mk)
		c.srv.store.DeleteKind(mk, kindZsetMember)
	}
	if err := c.zsetSetCard(zkey, uint64(card-len(keys))); err != nil {
		return keys, plen, err
	}
	return keys, plen, nil
}

func (c *connState) cmdBZPopMin(argv [][]byte) { c.blockingZPop(argv, false) }
func (c *connState) cmdBZPopMax(argv [][]byte) { c.blockingZPop(argv, true) }

// blockingZPop is the shared body for BZPOPMIN (min) and BZPOPMAX. Both take one or more keys
// and a trailing timeout: they pop the single lowest (min) or highest (max) scoring member
// from the first key that is a non-empty sorted set, scanning the keys left to right, and
// reply with a three-element array of that key's name, the member, and its score. When every
// key is empty they block on all of them until a ZADD wakes them or the timeout fires, at
// which point the reply is a null array. A key holding a plain string is WRONGTYPE, checked in
// scan order so an earlier string errors before a later servable key is reached.
func (c *connState) blockingZPop(argv [][]byte, max bool) {
	name := "bzpopmin"
	if max {
		name = "bzpopmax"
	}
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	d, forever, errMsg := parseTimeout(argv[len(argv)-1])
	if errMsg != "" {
		c.writeErr(errMsg)
		return
	}
	keys := argv[1 : len(argv)-1]

	var w *listWaiter
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		if w != nil {
			c.srv.removeWaiter(w, keys)
		}
	}()
	for {
		// Serve pass: the first non-empty key yields one extreme member. Running this every loop
		// turn is also the recheck that closes the register-then-park race, so a ZADD that lands
		// between an empty scan and the registration is never missed.
		for _, key := range keys {
			mu := &c.srv.incrMu[c.srv.stripe(key)]
			mu.Lock()
			if c.stringConflict(key) {
				mu.Unlock()
				c.writeErr(wrongType)
				return
			}
			popped, plen, err := c.zpopExtreme(key, max, 1)
			if err != nil {
				mu.Unlock()
				c.writeErr("ERR " + err.Error())
				return
			}
			if len(popped) > 0 {
				k := popped[0]
				mu.Unlock()
				c.writeArrayHeader(3)
				c.writeBulk(key)
				c.writeBulk(k[plen+8:])
				c.writeScore(decodeSortableScore(k[plen : plen+8]))
				c.srv.signalListKey(key) // the key may still hold members for another waiter
				return
			}
			mu.Unlock()
		}
		// Every key empty. On the goroutine driver this connection parks below; under the reactor
		// it hands the command to a park goroutine that reruns it with parking enabled.
		if !c.blockable {
			ac := dupArgv(argv)
			if c.parkOnReactor(func() { c.blockingZPop(ac, max) }) {
				return
			}
			c.writeNilArray()
			return
		}
		if w == nil {
			w = &listWaiter{ch: make(chan struct{}, 1)}
			c.srv.addWaiter(w, keys)
			continue // recheck under the registration before parking
		}
		c.flushBeforeBlock()
		if timer == nil && !forever {
			timer = time.NewTimer(d)
		}
		var timeout <-chan time.Time
		if timer != nil {
			timeout = timer.C
		}
		select {
		case <-w.ch:
			// Woken by a ZADD: loop back and rescan the keys.
		case <-timeout:
			c.writeNilArray()
			return
		case <-c.parkCancel:
			return
		}
	}
}

// cmdBZMPop implements BZMPOP timeout numkeys key [key ...] <MIN | MAX> [COUNT count], the
// blocking ZMPOP: it pops up to count extreme members from the first non-empty key, scanning
// left to right, and blocks on all the keys until one gains members or the timeout fires. The
// served reply is ZMPOP's nested [key, [[member, score], ...]] array; a timeout is a null
// array. A listed key holding a plain string is WRONGTYPE.
func (c *connState) cmdBZMPop(argv [][]byte) {
	// BZMPOP timeout numkeys key [key ...] <MIN | MAX> [COUNT count]. The floor is five, one
	// key plus the mandatory MIN/MAX, so "BZMPOP 0 0 MIN" reports the arity error, matching Redis.
	if len(argv) < 5 {
		c.writeErr("ERR wrong number of arguments for 'bzmpop' command")
		return
	}
	d, forever, errMsg := parseTimeout(argv[1])
	if errMsg != "" {
		c.writeErr(errMsg)
		return
	}
	numkeys, err := strconv.Atoi(string(argv[2]))
	if err != nil || numkeys <= 0 {
		c.writeErr("ERR numkeys should be greater than 0")
		return
	}
	keysEnd := 3 + numkeys
	// Room for the key list plus the mandatory MIN/MAX token.
	if len(argv) < keysEnd+1 {
		c.writeErr("ERR syntax error")
		return
	}
	keys := argv[3:keysEnd]
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

	var w *listWaiter
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		if w != nil {
			c.srv.removeWaiter(w, keys)
		}
	}()
	for {
		for _, key := range keys {
			mu := &c.srv.incrMu[c.srv.stripe(key)]
			mu.Lock()
			if c.stringConflict(key) {
				mu.Unlock()
				c.writeErr(wrongType)
				return
			}
			popped, plen, err := c.zpopExtreme(key, max, count)
			if err != nil {
				mu.Unlock()
				c.writeErr("ERR " + err.Error())
				return
			}
			if len(popped) == 0 {
				mu.Unlock()
				continue
			}
			mu.Unlock()

			// Reply: [key, [[member, score], ...]]. MIN emits ascending, MAX descending, the same
			// window read back to front.
			c.writeArrayHeader(2)
			c.writeBulk(key)
			c.writeArrayHeader(len(popped))
			if max {
				for i := len(popped) - 1; i >= 0; i-- {
					c.writeArrayHeader(2)
					c.writeBulk(popped[i][plen+8:])
					c.writeScore(decodeSortableScore(popped[i][plen : plen+8]))
				}
			} else {
				for _, k := range popped {
					c.writeArrayHeader(2)
					c.writeBulk(k[plen+8:])
					c.writeScore(decodeSortableScore(k[plen : plen+8]))
				}
			}
			c.srv.signalListKey(key) // the key may still hold members for another waiter
			return
		}
		if !c.blockable {
			ac := dupArgv(argv)
			if c.parkOnReactor(func() { c.cmdBZMPop(ac) }) {
				return
			}
			c.writeNilArray()
			return
		}
		if w == nil {
			w = &listWaiter{ch: make(chan struct{}, 1)}
			c.srv.addWaiter(w, keys)
			continue
		}
		c.flushBeforeBlock()
		if timer == nil && !forever {
			timer = time.NewTimer(d)
		}
		var timeout <-chan time.Time
		if timer != nil {
			timeout = timer.C
		}
		select {
		case <-w.ch:
		case <-timeout:
			c.writeNilArray()
			return
		case <-c.parkCancel:
			return
		}
	}
}
