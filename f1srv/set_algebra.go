package f1srv

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/f1raw"
)

// Set algebra (SINTER/SUNION/SDIFF and SINTERCARD) reads each source set by enumerating its
// dense member vector, never the global ordered index and never a whole-source materialize
// (spec 2064/f1_rewrite_ltm/20). A set owes no member order, so the algebra does not need one:
// SINTER drives off the smallest source and point-probes the rest through the hash index, SDIFF
// walks the first source and rejects any member the others hold, and SUNION enumerates every
// source and deduplicates through a seen-set. None of the three depends on the sources arriving
// in sorted order, which is what lets them read the unordered vector instead of the ordered index.
//
// The RESP2 array count has to precede the elements. SINTER and SDIFF bound their result by one
// driving set (the smallest, the first), so they buffer the qualifying members (arena-stable
// subslices) and frame from the buffer length. SUNION's result is the distinct union, so it
// buffers the deduplicated members and frames from that buffer; the seen-set it builds to
// deduplicate is O(union) in memory, exactly as Redis's own dict-backed SUNION is.
//
// Locking: an algebra read takes every source set's stripe lock (distinct stripes, in
// ascending index order so it can never deadlock against another multi-key write) for the
// span of the operation, so the sets it reads cannot change under it. setVecEach reads the
// vectors under those already-held locks and takes none of its own.

// setVecEach enumerates every live member of set skey, calling emit with each member (the bare
// member bytes, an arena-stable subslice). It reads the set's dense member vector, not the ordered
// index (spec 2064/f1_rewrite_ltm/20 section 6): an unpartitioned set walks its one whole-set
// vector, a partitioned set walks its P partition vectors in turn. It resolves a partitioned set's
// vectors through the descriptor (SetPartVec*, the same path streamSet and the draw use) so a vector
// this enumeration builds is registered for descriptor-driven teardown and cannot leak past a DEL or
// a grow (section 6.1). emit returns false to stop early, and setVecEach then returns false so a
// caller like SINTERCARD's LIMIT or an intersection driver can cut the walk short. The caller holds
// every source's stripe lock, so the layout and the vectors are stable and setVecEach locks nothing.
//
// Buffer discipline: setVecEach owns its bounding prefix, freshly allocated rather than borrowed from
// the connection's pbuf/ppbuf scratch, because a probing or storing emit reuses those same scratch
// buffers (setMemberExists builds into kbuf, but storeAlgebra's insert builds the destination base
// into ppbuf and the destination prefix into pbuf). A borrowed prefix would be clobbered mid-walk by
// such an emit, dropping every member past the first scan batch; a walk that owns its prefix survives
// any emit. Each yielded member points into the immutable arena, so it stays valid after the scan
// buffer refills and after any probe or store.
func (c *connState) setVecEach(skey []byte, emit func([]byte) bool) bool {
	scan := make([][]byte, 0, hashScanBatch)
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	if p := c.partitionsFor(skey); p > 1 {
		// base = uvarint(len(skey)) | skey | <partByte placeholder>, the partition-scan prefix
		// SetPartVecScanDown rewrites the last byte of per partition (matching partScanBase).
		base := make([]byte, 0, n+len(skey)+1)
		base = append(base, tmp[:n]...)
		base = append(base, skey...)
		base = append(base, 0)
		moff := len(base) // member starts past uvarint(len)|skey|byte(part)
		for part := 0; part < p; part++ {
			hi := -1
			for {
				keys, next := c.srv.store.SetPartVecScanDown(base, p, part, hi, hashScanBatch, scan[:0])
				for _, k := range keys {
					if !emit(k[moff:]) {
						return false
					}
				}
				if next == 0 {
					break
				}
				hi = next
			}
		}
		return true
	}
	// prefix = uvarint(len(skey)) | skey, the whole-set bounding prefix (matching setPrefix).
	prefix := make([]byte, 0, n+len(skey))
	prefix = append(prefix, tmp[:n]...)
	prefix = append(prefix, skey...)
	plen := len(prefix)
	hi := -1
	for {
		keys, next := c.srv.store.SetVecScanDown(prefix, hi, hashScanBatch, scan[:0])
		for _, k := range keys {
			if !emit(k[plen:]) {
				return false
			}
		}
		if next == 0 {
			break
		}
		hi = next
	}
	return true
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
// When adaptive partitioning is off no key's P ever changes, so one pass locks the exact stripe set.
// When it is armed a migration could grow one of these keys after this reads its P but before it
// takes the stripes, leaving the key's new partitions unlocked. The retry re-reads every key's P
// under the acquired locks and, if any grew, releases and redoes the acquisition over the wider
// layout. It converges because P only ever rises and is bounded by the configured cap, and it takes
// the same ascending stripe-index order every iteration, so a migration holding an overlapping
// superset of stripes and this call can never form a cycle.
func (c *connState) lockStripes(keys [][]byte) func() {
	for {
		idxs := make([]uint32, 0, len(keys))
		add := func(s uint32) {
			for _, e := range idxs {
				if e == s {
					return
				}
			}
			idxs = append(idxs, s)
		}
		ps := make([]int, len(keys))
		for i, k := range keys {
			p := c.partitionsFor(k)
			ps[i] = p
			if p > 1 {
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
		if c.srv.setPartMax > 1 {
			stale := false
			for i, k := range keys {
				if c.partitionsFor(k) != ps[i] {
					stale = true
					break
				}
			}
			if stale {
				for i := len(idxs) - 1; i >= 0; i-- {
					c.srv.incrMu[idxs[i]].Unlock()
				}
				continue
			}
		}
		return func() {
			for i := len(idxs) - 1; i >= 0; i-- {
				c.srv.incrMu[idxs[i]].Unlock()
			}
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

// sunionEach calls emit once for each distinct member across all source sets. It enumerates every
// source's member vector and deduplicates through a seen-set keyed by the member bytes, so a member
// several sources share is emitted exactly once. The seen-set is O(distinct union) in memory, the
// same cost Redis's dict-backed SUNION pays; there is no sorted-merge shortcut because the vector is
// unordered. emit returns false to stop early; the read SUNION never does, but SUNIONSTORE's insert
// can fail and stop the walk.
func (c *connState) sunionEach(keys [][]byte, emit func([]byte) bool) {
	seen := make(map[string]struct{})
	for _, k := range keys {
		stop := false
		c.setVecEach(k, func(m []byte) bool {
			if _, ok := seen[string(m)]; ok {
				return true
			}
			seen[string(m)] = struct{}{}
			if !emit(m) {
				stop = true
				return false
			}
			return true
		})
		if stop {
			return
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

// sinterEach yields every member present in all source sets and returns early when emit returns
// false (SINTERCARD's LIMIT). It drives off the smallest source, chosen from the O(1) header
// cardinalities, and point-probes every other source through the hash index for each of the
// smallest source's members, so the work is bounded by the smallest source. Any empty source
// makes the intersection empty and it yields nothing.
//
// The ordered-index era had a second strategy, a sorted k-way merge over the sources that cost the
// sum of the cardinalities with no per-member probe. That merge existed only because every set's
// members sat in one sort order under the global ordered index (spec 2064/f1_rewrite_ltm/20). The
// dense member vector is unordered, so there is no sorted-merge form to fall back to; SINTER always
// probes off the smallest source, which is the same strategy Redis uses.
func (c *connState) sinterEach(keys [][]byte, emit func([]byte) bool) {
	minCard := ^uint64(0)
	driverIdx := 0
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
	c.sinterProbeEach(keys, driverIdx, emit)
}

// sinterProbeEach yields the intersection by enumerating the smallest source (driverIdx, already
// chosen from the header counts) and point-probing every other source for each member. The work is
// bounded by the smallest source. A lone source (no other source to probe) yields all its members,
// which is the intersection of one set with itself. emit returns false to stop early.
func (c *connState) sinterProbeEach(keys [][]byte, driverIdx int, emit func([]byte) bool) {
	c.setVecEach(keys[driverIdx], func(m []byte) bool {
		for i, k := range keys {
			if i == driverIdx {
				continue
			}
			if !c.setMemberExists(k, m) {
				return true // not in every source, skip but keep walking the driver
			}
		}
		return emit(m)
	})
}

// sdiffEach walks the first source set and calls emit for each member none of the other sources
// hold, in the first set's enumeration order (spec section 5). SDIFF is not commutative, so the
// first key is always the driver and the rest are probed through the hash index. The result is
// bounded by the first set.
func (c *connState) sdiffEach(keys [][]byte, emit func([]byte) bool) {
	rest := keys[1:]
	c.setVecEach(keys[0], func(m []byte) bool {
		for _, k := range rest {
			if c.setMemberExists(k, m) {
				return true // present in a later source, not in the difference
			}
		}
		return emit(m)
	})
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
