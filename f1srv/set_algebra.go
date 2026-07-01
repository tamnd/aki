package f1srv

import (
	"bytes"
	"encoding/binary"

	"github.com/tamnd/aki/engine/f1raw"
)

// Set algebra (SINTER/SUNION/SDIFF and SINTERCARD) is a k-way merge over the
// member-ordered composite keys, never a materialize (spec 2064/f1_rewrite_ltm/06
// section 5). Every set's member rows already sort in member-byte order under the
// ordered element index, so the algebra rides forward cursors: SUNION is a k-way merge
// emitting each distinct member once, SINTER drives off the smallest set and point-probes
// the rest, and SDIFF walks the first set and rejects any member the others hold. The
// peak memory is k cursors plus one member in hand, so an intersection of billion-member
// sets never pulls a whole source into RAM.
//
// The RESP2 array count has to precede the elements, but a merge does not know its result
// count up front. SINTER and SDIFF bound their result by one driving set (the smallest,
// the first), so they buffer the qualifying members (arena-stable subslices) and frame
// from the buffer length. SUNION's result is unbounded (the sum of all sources), so it
// runs the merge twice under the source locks, counting first and emitting second, which
// keeps the framing exact without ever holding the whole union.
//
// Locking: an algebra read takes every source set's stripe lock (distinct stripes, in
// ascending index order so it can never deadlock against another multi-key write) for the
// span of the operation, so the sets it reads cannot change under it. That makes the two
// SUNION passes see identical state, so the framed count always matches what is streamed.

// setCursor is a forward, member-ordered cursor over one set's member rows on the f1raw
// ordered element index. cur is the current member (the composite key past the prefix, an
// arena-stable subslice) or nil when the set is exhausted. Each cursor owns its prefix and
// batch buffers so several can run at once during a k-way merge, unlike the single shared
// c.pbuf a lone enumeration uses.
type setCursor struct {
	st     *f1raw.Store
	prefix []byte
	plen   int
	after  []byte
	batch  [][]byte
	idx    int
	done   bool
	cur    []byte
}

// newSetCursor opens a member-ordered cursor over skey, positioned on the first member
// (cur nil when the set is empty). The prefix is a fresh copy, not c.pbuf, so k cursors in
// one merge never share a prefix buffer.
func (c *connState) newSetCursor(skey []byte) *setCursor {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	prefix := make([]byte, 0, n+len(skey))
	prefix = append(prefix, tmp[:n]...)
	prefix = append(prefix, skey...)
	sc := &setCursor{
		st:     c.srv.store,
		prefix: prefix,
		plen:   len(prefix),
		batch:  make([][]byte, 0, hashScanBatch),
	}
	sc.advance()
	return sc
}

// advance moves the cursor to the next member, refilling from the ordered index in bounded
// batches, and sets cur to nil once the set is exhausted. Every yielded member is a
// subslice of the immutable arena, valid for the store's life, so a merge holds it without
// copying even after the cursor refills its batch buffer.
func (sc *setCursor) advance() {
	if sc.idx < len(sc.batch) {
		sc.cur = sc.batch[sc.idx][sc.plen:]
		sc.idx++
		return
	}
	if sc.done {
		sc.cur = nil
		return
	}
	keys, last := sc.st.CollScan(sc.prefix, sc.after, hashScanBatch, sc.batch[:0])
	sc.batch = keys
	sc.idx = 0
	if last == nil {
		sc.done = true
	} else {
		sc.after = last
	}
	if len(keys) == 0 {
		sc.cur = nil
		return
	}
	sc.cur = sc.batch[sc.idx][sc.plen:]
	sc.idx++
}

// lockStripes takes the stripe locks for every distinct key in keys, in ascending stripe
// index order so a multi-key read can never deadlock against SMOVE or another algebra call
// that touches an overlapping key set, and returns an unlock closure. Keys that map to the
// same stripe lock it once. The set of distinct stripes is small (one per source), so the
// linear dedup and insertion sort cost nothing measurable.
func (c *connState) lockStripes(keys [][]byte) func() {
	idxs := make([]uint32, 0, len(keys))
	for _, k := range keys {
		s := c.srv.stripe(k)
		dup := false
		for _, e := range idxs {
			if e == s {
				dup = true
				break
			}
		}
		if !dup {
			idxs = append(idxs, s)
		}
	}
	for i := 1; i < len(idxs); i++ {
		for j := i; j > 0 && idxs[j] < idxs[j-1]; j-- {
			idxs[j], idxs[j-1] = idxs[j-1], idxs[j]
		}
	}
	for _, s := range idxs {
		c.srv.incrMu[s].Lock()
	}
	return func() {
		for i := len(idxs) - 1; i >= 0; i-- {
			c.srv.incrMu[idxs[i]].Unlock()
		}
	}
}

// anyStringConflict reports whether any of the keys is held by a plain string, in which
// case the whole algebra command is WRONGTYPE. It probes the string namespace only, so it
// never trips over a set's own header or member rows.
func (c *connState) anyStringConflict(keys [][]byte) bool {
	for _, k := range keys {
		if c.stringConflict(k) {
			return true
		}
	}
	return false
}

// sunionEach runs the k-way merge over every source set and calls emit once for each
// distinct member, in member-byte order. It advances every cursor that sits on the emitted
// member, so a member shared by several sets is emitted exactly once. emit returns false to
// stop early; SUNION never does, but the signature matches the other iterators.
func (c *connState) sunionEach(keys [][]byte, emit func([]byte) bool) {
	cursors := make([]*setCursor, len(keys))
	for i, k := range keys {
		cursors[i] = c.newSetCursor(k)
	}
	for {
		var smallest []byte
		found := false
		for _, sc := range cursors {
			if sc.cur == nil {
				continue
			}
			if !found || bytes.Compare(sc.cur, smallest) < 0 {
				smallest = sc.cur
				found = true
			}
		}
		if !found {
			return
		}
		if !emit(smallest) {
			return
		}
		for _, sc := range cursors {
			if sc.cur != nil && bytes.Equal(sc.cur, smallest) {
				sc.advance()
			}
		}
	}
}

// sinterEach drives off the smallest source set and calls emit for each member the every
// other source also holds, in the driver's member order (spec section 5.2). Reading the k
// O(1) header counts first lets it pick the smallest set to iterate and point-probe the
// rest, so the work is bounded by the smallest set, not the largest. If any source is empty
// the intersection is empty and it emits nothing. emit returns false to stop early, which
// SINTERCARD uses to stop at its LIMIT.
func (c *connState) sinterEach(keys [][]byte, emit func([]byte) bool) {
	driverIdx := 0
	minCard := ^uint64(0)
	for i, k := range keys {
		card := c.setCard(k)
		if card == 0 {
			return // an empty source means an empty intersection
		}
		if card < minCard {
			minCard = card
			driverIdx = i
		}
	}
	driver := c.newSetCursor(keys[driverIdx])
	for driver.cur != nil {
		m := driver.cur
		inAll := true
		for i, k := range keys {
			if i == driverIdx {
				continue
			}
			if !c.srv.store.ExistsKind(c.memberKey(k, m), kindSetMember) {
				inAll = false
				break
			}
		}
		if inAll {
			if !emit(m) {
				return
			}
		}
		driver.advance()
	}
}

// sdiffEach walks the first source set and calls emit for each member none of the other
// sources hold, in the first set's member order (spec section 5). SDIFF is not commutative,
// so the first key is always the driver and the rest are probed. The result is bounded by
// the first set.
func (c *connState) sdiffEach(keys [][]byte, emit func([]byte) bool) {
	driver := c.newSetCursor(keys[0])
	rest := keys[1:]
	for driver.cur != nil {
		m := driver.cur
		inRest := false
		for _, k := range rest {
			if c.srv.store.ExistsKind(c.memberKey(k, m), kindSetMember) {
				inRest = true
				break
			}
		}
		if !inRest {
			if !emit(m) {
				return
			}
		}
		driver.advance()
	}
}

// cmdSInter answers SINTER by buffering the members present in every source (bounded by the
// smallest set) and framing the reply from the buffer length. The buffered members are
// arena-stable subslices, so they survive the driver cursor refilling its batch.
func (c *connState) cmdSInter(argv [][]byte) {
	// SINTER key [key ...]
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'sinter' command")
		return
	}
	keys := argv[1:]
	unlock := c.lockStripes(keys)
	if c.anyStringConflict(keys) {
		unlock()
		c.writeErr(wrongType)
		return
	}
	out := make([][]byte, 0)
	c.sinterEach(keys, func(m []byte) bool {
		out = append(out, m)
		return true
	})
	c.writeArrayHeader(len(out))
	for _, m := range out {
		c.writeBulk(m)
	}
	unlock()
}

// cmdSDiff answers SDIFF by buffering the first set's members that no other source holds
// (bounded by the first set) and framing from the buffer length.
func (c *connState) cmdSDiff(argv [][]byte) {
	// SDIFF key [key ...]
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'sdiff' command")
		return
	}
	keys := argv[1:]
	unlock := c.lockStripes(keys)
	if c.anyStringConflict(keys) {
		unlock()
		c.writeErr(wrongType)
		return
	}
	out := make([][]byte, 0)
	c.sdiffEach(keys, func(m []byte) bool {
		out = append(out, m)
		return true
	})
	c.writeArrayHeader(len(out))
	for _, m := range out {
		c.writeBulk(m)
	}
	unlock()
}

// cmdSUnion answers SUNION with a two-pass k-way merge: it counts the distinct members
// first, frames the array with that count, then merges again to stream them, all under the
// source locks so the two passes see identical state. This keeps the peak memory at k
// cursors even for a union of enormous sets, where buffering the whole result would blow
// the larger-than-memory budget.
func (c *connState) cmdSUnion(argv [][]byte) {
	// SUNION key [key ...]
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'sunion' command")
		return
	}
	keys := argv[1:]
	unlock := c.lockStripes(keys)
	if c.anyStringConflict(keys) {
		unlock()
		c.writeErr(wrongType)
		return
	}
	n := 0
	c.sunionEach(keys, func([]byte) bool {
		n++
		return true
	})
	c.writeArrayHeader(n)
	c.sunionEach(keys, func(m []byte) bool {
		c.writeBulk(m)
		return true
	})
	unlock()
}

// cmdSInterCard answers SINTERCARD numkeys key [key ...] [LIMIT limit]: it counts the
// intersection with the smallest-set-first probe and stops as soon as it reaches a positive
// LIMIT, so a bounded existence check on huge sets never walks the whole intersection. LIMIT
// 0 means no limit (count them all).
func (c *connState) cmdSInterCard(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'sintercard' command")
		return
	}
	numkeys, err := atoi64(argv[1])
	if err != nil {
		c.writeErr("ERR numkeys should be greater than 0")
		return
	}
	if numkeys <= 0 {
		c.writeErr("ERR numkeys should be greater than 0")
		return
	}
	nk := int(numkeys)
	if 2+nk > len(argv) {
		c.writeErr("ERR Number of keys can't be greater than number of args")
		return
	}
	keys := argv[2 : 2+nk]
	limit := 0
	rest := argv[2+nk:]
	if len(rest) > 0 {
		if len(rest) != 2 || !eqFold(rest[0], "LIMIT") {
			c.writeErr("ERR syntax error")
			return
		}
		l, err := atoi64(rest[1])
		if err != nil || l < 0 {
			c.writeErr("ERR LIMIT can't be negative")
			return
		}
		limit = int(l)
	}
	unlock := c.lockStripes(keys)
	if c.anyStringConflict(keys) {
		unlock()
		c.writeErr(wrongType)
		return
	}
	count := 0
	c.sinterEach(keys, func([]byte) bool {
		count++
		if limit > 0 && count >= limit {
			return false
		}
		return true
	})
	unlock()
	c.writeInt(int64(count))
}
