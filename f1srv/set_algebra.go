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

// partWalk is a forward, member-ordered walk over one contiguous member-row run: a whole
// unpartitioned set, or one partition of a partitioned set. prefix bounds that run and plen is
// where the member starts (past the length-prefixed set key, and past the partition byte for a
// partition run), so cur = batch[idx][plen:] is the bare member. Each walk owns its prefix and
// batch buffers so P partition walks (and k cursors in a merge) never share a buffer.
type partWalk struct {
	st     *f1raw.Store
	prefix []byte
	plen   int
	after  []byte
	batch  [][]byte
	idx    int
	done   bool
	cur    []byte
}

// advance moves the walk to the next member, refilling from the ordered index in bounded
// batches, and sets cur to nil once the run is exhausted. Every yielded member is a subslice of
// the immutable arena, valid for the store's life, so a merge holds it without copying even
// after the walk refills its batch buffer.
func (pw *partWalk) advance() {
	if pw.idx < len(pw.batch) {
		pw.cur = pw.batch[pw.idx][pw.plen:]
		pw.idx++
		return
	}
	if pw.done {
		pw.cur = nil
		return
	}
	keys, last := pw.st.CollScan(pw.prefix, pw.after, hashScanBatch, pw.batch[:0])
	pw.batch = keys
	pw.idx = 0
	if last == nil {
		pw.done = true
	} else {
		pw.after = last
	}
	if len(keys) == 0 {
		pw.cur = nil
		return
	}
	pw.cur = pw.batch[pw.idx][pw.plen:]
	pw.idx++
}

// setCursor is a forward, member-ordered cursor over one whole set's members, in pure member-byte
// order, whatever the set's partition count. cur is the current member (an arena-stable subslice)
// or nil at exhaustion. An unpartitioned set is one partWalk (single); a partitioned set is P
// per-partition walks merged into one member-ordered stream. The merge matters because the P
// partition rows sort by (partition, member) under the whole-set prefix, not by member, so a lone
// walk of that prefix would break the k-way merge that every algebra caller relies on. A member
// routes to exactly one partition (PartitionOf), so no member is ever in two walks and the merge
// is a plain min-scan with no dedup.
type setCursor struct {
	single *partWalk   // non-nil for an unpartitioned set (P==1): the whole-set walk
	walks  []*partWalk // non-nil for a partitioned set (P>1): one walk per partition
	cur    []byte
}

// newPartWalk opens a walk over one member run of skey: the whole set when p==1, or partition
// part when p>1. The prefix is a fresh copy, not a shared scratch buffer, so P walks in one
// partitioned cursor never share a prefix. The member offset is len(prefix), which already skips
// the partition byte when present, so advance strips exactly the member.
func (c *connState) newPartWalk(skey []byte, part, p int) *partWalk {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	prefix := make([]byte, 0, n+len(skey)+1)
	prefix = append(prefix, tmp[:n]...)
	prefix = append(prefix, skey...)
	if p > 1 {
		prefix = append(prefix, byte(part))
	}
	pw := &partWalk{
		st:     c.srv.store,
		prefix: prefix,
		plen:   len(prefix),
		batch:  make([][]byte, 0, hashScanBatch),
	}
	pw.advance()
	return pw
}

// newSetCursor opens a member-ordered cursor over skey, positioned on the first member (cur nil
// when the set is empty). It reads the set's partition count once: an unpartitioned set gets one
// direct walk (the hot common path, no per-member merge), a partitioned set gets P walks merged.
func (c *connState) newSetCursor(skey []byte) *setCursor {
	sc := &setCursor{}
	if p := c.partitionsFor(skey); p > 1 {
		sc.walks = make([]*partWalk, p)
		for part := 0; part < p; part++ {
			sc.walks[part] = c.newPartWalk(skey, part, p)
		}
	} else {
		sc.single = c.newPartWalk(skey, 0, 1)
	}
	sc.advance()
	return sc
}

// advance moves the cursor to the next member in member-byte order. The unpartitioned cursor just
// steps its lone walk. The partitioned cursor takes the smallest member currently at any partition
// walk's front and advances that walk, so the P sorted per-partition streams merge into one sorted
// stream; because a member lives in exactly one partition, exactly one walk sits on the smallest,
// but it advances every walk equal to it defensively so a would-be duplicate never stalls the merge.
func (sc *setCursor) advance() {
	if sc.single != nil {
		// Read the current front, then step the walk to prepare the next, exactly as the
		// merge path reads each walk's front before advancing the one at the minimum. The
		// walk is already positioned on its first member at construction, so reading before
		// stepping yields that first member instead of skipping past it.
		sc.cur = sc.single.cur
		sc.single.advance()
		return
	}
	var min []byte
	found := false
	for _, pw := range sc.walks {
		if pw.cur == nil {
			continue
		}
		if !found || bytes.Compare(pw.cur, min) < 0 {
			min = pw.cur
			found = true
		}
	}
	if !found {
		sc.cur = nil
		return
	}
	sc.cur = min
	for _, pw := range sc.walks {
		if pw.cur != nil && bytes.Equal(pw.cur, min) {
			pw.advance()
		}
	}
}

// lockStripes takes the stripe locks for every distinct key in keys, in ascending stripe
// index order so a multi-key read can never deadlock against SMOVE or another algebra call
// that touches an overlapping key set, and returns an unlock closure. A partitioned key
// contributes every one of its partition stripes (stripePart per partition), because its
// member writers hold per-partition stripe write locks, not the whole-key stripe, so a
// whole-key stripe alone would not exclude them. An unpartitioned key contributes its one
// whole-key stripe. Stripes are deduplicated (two partitions or two keys can hash to one
// stripe) and taken in ascending index order, the same global order lockSetPartitionsShared
// uses, so exclusive algebra locks and shared SMEMBERS locks over overlapping partition
// stripes acquire in one order and never form a cycle. The distinct-stripe set stays small,
// so the linear dedup and insertion sort cost nothing measurable.
func (c *connState) lockStripes(keys [][]byte) func() {
	idxs := make([]uint32, 0, len(keys))
	add := func(s uint32) {
		for _, e := range idxs {
			if e == s {
				return
			}
		}
		idxs = append(idxs, s)
	}
	for _, k := range keys {
		if p := c.partitionsFor(k); p > 1 {
			for part := 0; part < p; part++ {
				add(c.srv.stripePart(k, part))
			}
		} else {
			add(c.srv.stripe(k))
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

// setMemberExists reports whether member is in set skey, routing the probe to the member's
// partition when skey is partitioned (spec 2064/f1_rewrite_ltm/19 section 6.9). The
// intersection and difference drivers probe non-driver sources one member at a time, and a
// partitioned source stores that member only under its routed partition key, so an
// unpartitioned memberKey probe would miss it. For an unpartitioned set it is byte-identical
// to the plain probe. member is an arena-stable driver member, so building the composite key
// into the per-connection scratch is safe: the result is consumed before the next probe.
func (c *connState) setMemberExists(skey, member []byte) bool {
	if p := c.partitionsFor(skey); p > 1 {
		part := f1raw.PartitionOf(member, p)
		return c.srv.store.ExistsKind(c.partMemberKey(skey, member, part, p), kindSetMember)
	}
	return c.srv.store.ExistsKind(c.memberKey(skey, member), kindSetMember)
}

// sinterProbeWeight is how many cursor-advance steps one point-probe of a source costs,
// used by sinterEach to choose its strategy. A probe builds the composite member key and
// walks the lock-free hash index, which is several times the cost of a single ordered-cursor
// advance, so this is deliberately above one. It only steers the merge-vs-probe choice; both
// paths return the same members, so the exact value trades a little work either side of the
// crossover and never affects correctness.
const sinterProbeWeight = 4

// sinterEach yields every member present in all source sets, in ascending member-byte order,
// and returns early when emit returns false (SINTERCARD's LIMIT). It picks between two exact
// strategies from the O(1) header cardinalities:
//
//   - a sorted k-way merge (sinterMergeEach) that walks every source cursor once in lockstep,
//     costing about the sum of the cardinalities with no per-member key build or hash probe.
//     This is only possible because f1raw keeps every set's members in one sort order, a
//     property a hashtable set does not have, and it is what makes SINTER a merge rather than
//     a probe here.
//   - the classic drive-off-the-smallest-set probe (sinterProbeEach) that iterates the
//     smallest source and point-probes the rest, costing about the smallest cardinality times
//     the probe weight. This wins when one source is far smaller than the others.
//
// It uses whichever the cardinalities say is cheaper. Any empty source makes the intersection
// empty and it yields nothing.
func (c *connState) sinterEach(keys [][]byte, emit func([]byte) bool) {
	var sumCard, minCard uint64 = 0, ^uint64(0)
	driverIdx := 0
	for i, k := range keys {
		card := c.setCard(k)
		if card == 0 {
			return // an empty source means an empty intersection
		}
		sumCard += card
		if card < minCard {
			minCard = card
			driverIdx = i
		}
	}
	// Merge cost is about sumCard cursor steps; probe cost is about minCard*(k-1) probes,
	// each sinterProbeWeight steps. Take the cheaper. A lone source (k==1) has no other
	// source to probe, so the merge (a single cursor walk) is always the right path.
	probeCost := minCard * uint64(len(keys)-1) * sinterProbeWeight
	if len(keys) == 1 || sumCard <= probeCost {
		c.sinterMergeEach(keys, emit)
		return
	}
	c.sinterProbeEach(keys, driverIdx, emit)
}

// sinterMergeEach yields the intersection by a sorted k-way merge over the source cursors: it
// repeatedly takes the largest member currently at any cursor front, advances every cursor
// that sits below it, and yields the member when all cursors have caught up to it exactly.
// Every advance makes forward progress, so the whole merge is bounded by the sum of the source
// cardinalities and touches each member row once. It allocates nothing per member and never
// probes the hash index. emit returns false to stop early.
func (c *connState) sinterMergeEach(keys [][]byte, emit func([]byte) bool) {
	cursors := make([]*setCursor, len(keys))
	for i, k := range keys {
		sc := c.newSetCursor(k)
		if sc.cur == nil {
			return // an empty source means an empty intersection
		}
		cursors[i] = sc
	}
	for {
		max := cursors[0].cur
		for _, sc := range cursors[1:] {
			if bytes.Compare(sc.cur, max) > 0 {
				max = sc.cur
			}
		}
		allEqual := true
		for _, sc := range cursors {
			for bytes.Compare(sc.cur, max) < 0 {
				sc.advance()
				if sc.cur == nil {
					return // this source is exhausted, so nothing more can intersect
				}
			}
			if !bytes.Equal(sc.cur, max) {
				allEqual = false
			}
		}
		if allEqual {
			if !emit(max) {
				return
			}
			for _, sc := range cursors {
				sc.advance()
				if sc.cur == nil {
					return
				}
			}
		}
	}
}

// sinterProbeEach yields the intersection by iterating the smallest source (driverIdx, already
// chosen from the header counts) and point-probing every other source for each member. The
// work is bounded by the smallest source, which wins when it is far smaller than the rest.
// emit returns false to stop early.
func (c *connState) sinterProbeEach(keys [][]byte, driverIdx int, emit func([]byte) bool) {
	driver := c.newSetCursor(keys[driverIdx])
	for driver.cur != nil {
		m := driver.cur
		inAll := true
		for i, k := range keys {
			if i == driverIdx {
				continue
			}
			if !c.setMemberExists(k, m) {
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
			if c.setMemberExists(k, m) {
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
