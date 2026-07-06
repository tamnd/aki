package f1srv

import (
	"sync"

	"github.com/tamnd/aki/engine/f1raw"
)

// This file holds the intra-key set partition migration (spec 2064/f1_rewrite_ltm/19 section 11.6):
// the one-way transition that grows a hot set from its current partition count to a larger power of
// two, re-homing every member into the new layout, plus the hot-path retry guards that let a routed
// single-member op run correctly while a migration of the same key is in flight.
//
// The transition is one-way: a set only ever grows P (engage P=1 to P', then grow P' to a larger
// P''), and DELETE plus recreate resets it to P=1 through the registry drop. Growth-only is what lets
// the migration freeze [0, newP), the union of the old and new partition sets, and know it covers
// every member's old and new home at once.

// lockSetMigration takes the whole-key stripe and every partition stripe over [0, newP) of set skey
// exclusively, deduplicated and in ascending stripe-index order, and returns the locked stripes so
// unlockSetMigration releases them in reverse. Because a migration only ever raises P, freezing
// [0, newP) covers both the set's current partitions and its post-growth partitions, so every routed
// single-member writer (which holds one partition stripe) and every whole-key count writer
// (setBumpCard, which holds the whole-key stripe) is excluded for the span of the re-home. The
// whole-key stripe is folded into the same deduplicated set because stripePart can collide with
// stripe(skey) and a migration must never lock one incrMu twice. Ascending index order matches the
// order lockStripes and lockSetPartitionsShared take, so a migration and a concurrent algebra or
// SMEMBERS lock over overlapping stripes acquire in one order and can never form a cycle.
func (c *connState) lockSetMigration(skey []byte, newP int) []uint32 {
	stripes := make([]uint32, 0, newP+1)
	add := func(s uint32) {
		for _, e := range stripes {
			if e == s {
				return
			}
		}
		stripes = append(stripes, s)
	}
	add(c.srv.stripe(skey))
	for part := 0; part < newP; part++ {
		add(c.srv.stripePart(skey, part))
	}
	for i := 1; i < len(stripes); i++ {
		for j := i; j > 0 && stripes[j] < stripes[j-1]; j-- {
			stripes[j], stripes[j-1] = stripes[j-1], stripes[j]
		}
	}
	for _, s := range stripes {
		c.srv.incrMu[s].Lock()
	}
	return stripes
}

// unlockSetMigration releases the exclusive stripe locks lockSetMigration took, in reverse order.
func (c *connState) unlockSetMigration(stripes []uint32) {
	for i := len(stripes) - 1; i >= 0; i-- {
		c.srv.incrMu[stripes[i]].Unlock()
	}
}

// engageSetPartitions grows set skey to newP partitions, re-homing every member into the newP layout
// under the whole-key stripe and every partition stripe over [0, newP) held exclusively. It is the
// migration primitive slice 6b's engage-and-grow trigger drives; slice 6a-2 builds and tests it in
// isolation. It is a no-op when newP does not exceed the set's current partition count, since the
// transition only ever grows P.
//
// The re-home is ordered insert-new, publish, delete-old, drop-vectors so a lock-free single-member
// reader is always correct:
//
//   - Phase 0 gathers every member from the current layout by walking its dense member vector(s) into
//     a packed buffer, so the re-home that follows can rewrite rows under the same prefix without the
//     scan revisiting them. The bare member bytes are copied out, so they outlive the vector teardown.
//   - Phase 1 inserts each member's new-layout row before any old row is deleted, so a member is
//     findable in at least one home at every instant; a lock-free SISMEMBER never sees a present
//     member as absent.
//   - Phase 2 stamps the header with newP (preserving the cardinality word, which the re-home does
//     not change) and publishes newP into the registry. Only after phase 1 has populated every new
//     home does any reader or writer that reads P route to it.
//   - Phase 3 deletes each member's old-layout row, now that the new home is live and published.
//   - Phase 4 drops the set's dense draw vectors so the new layout rebuilds them lazily on first draw
//     against the re-homed rows rather than reading stale pointers into deleted records.
//
// A grow (old P greater than one) leaves a member whose partition byte is unchanged under the larger
// P exactly where it is: its old row already is its new row, so phases 1 and 3 both skip it.
//
// Concurrent single-member writers are excluded by the partition stripes: a writer blocks taking its
// partition lock, and when it acquires it (after this releases) the retry guard in lockMemberPartition
// re-reads P and re-routes to the new home. Concurrent whole-key count writers (setBumpCard) are
// excluded by the whole-key stripe, so a count bump lands either fully before this reads the header
// (and is included in the preserved count) or fully after (and is applied by CountAddInt64 onto the
// stamped newP header); either way the cardinality stays exact. Multi-partition readers that lock a
// P-sized stripe set (SMEMBERS, set algebra, SSCAN, the weighted draw) are made grow-safe when the
// engage trigger goes live in slice 6b; until then this primitive is exercised only by tests, which
// race it against the single-member ops the guards below cover.
func (c *connState) engageSetPartitions(skey []byte, newP int) {
	if newP < 2 || newP <= c.srv.partitionP(skey) {
		return
	}
	stripes := c.lockSetMigration(skey, newP)
	defer c.unlockSetMigration(stripes)

	// Re-read P under the lock: a racing migration of the same key holds these same stripes, so by the
	// time this one acquires them the set may already be at or past newP, leaving nothing to do.
	oldP := c.srv.partitionP(skey)
	if newP <= oldP {
		return
	}

	// Phase 0: gather every member by walking the current layout's dense member vector(s) rather than
	// the ordered index (spec 2064/f1_rewrite_ltm/20): the vector is the authoritative membership
	// structure for the set type, so the re-home reads it exactly as SMEMBERS does and never descends
	// the skip-list. The migration holds every stripe over [0, newP), a superset of the current
	// partitions, so the layout is frozen and one drained downward walk per current partition yields
	// every live member once. moff strips the current layout's key header and, when the set is already
	// partitioned, its one partition byte, recovering the bare member bytes. SetVecScanDown and
	// SetPartVecScanDown build the vector on first use, so a set that grew large enough to migrate
	// before it was ever drawn from still resolves its members. The recovered bytes are copied into buf,
	// so they stay valid after phase 4 tears the vectors down.
	prefix := c.setPrefix(skey)
	moff := len(prefix)
	if oldP > 1 {
		moff++
	}
	var buf []byte
	var ends []int
	scan := make([][]byte, 0, hashScanBatch)
	if oldP > 1 {
		base := c.partScanBase(skey)
		for part := 0; part < oldP; part++ {
			hi := -1
			for {
				keys, next := c.srv.store.SetPartVecScanDown(base, oldP, part, hi, hashScanBatch, scan[:0])
				for _, k := range keys {
					buf = append(buf, k[moff:]...)
					ends = append(ends, len(buf))
				}
				if next == 0 {
					break
				}
				hi = next
			}
		}
	} else {
		hi := -1
		for {
			keys, next := c.srv.store.SetVecScanDown(prefix, hi, hashScanBatch, scan[:0])
			for _, k := range keys {
				buf = append(buf, k[moff:]...)
				ends = append(ends, len(buf))
			}
			if next == 0 {
				break
			}
			hi = next
		}
	}

	// Phase 1: insert new-layout rows.
	start := 0
	for _, end := range ends {
		member := buf[start:end]
		start = end
		newPart := f1raw.PartitionOf(member, newP)
		if oldP > 1 && f1raw.PartitionOf(member, oldP) == newPart {
			continue
		}
		nk := c.partMemberKey(skey, member, newPart, newP)
		if created, err := c.srv.store.PutKind(nk, nil, kindSetMember); err == nil && created {
			c.srv.store.CollInsert(nk, kindSetMember)
		}
	}

	// Phase 2: stamp the header with newP (preserving count and encoding) and publish.
	if count, enc, ok := c.setHeader(skey); ok {
		hdr := setHeaderEncodeP(nil, count, enc, newP)
		_, _ = c.srv.store.PutKind(skey, hdr, kindSetMeta)
	}
	c.srv.engageP(skey, newP)

	// Phase 3: delete old-layout rows, skipping the members phase 1 left in place.
	start = 0
	for _, end := range ends {
		member := buf[start:end]
		start = end
		oldPart := f1raw.PartitionOf(member, oldP)
		if oldP > 1 && oldPart == f1raw.PartitionOf(member, newP) {
			continue
		}
		ok := c.partMemberKey(skey, member, oldPart, oldP)
		if c.srv.store.DeleteKind(ok, kindSetMember) {
			c.srv.store.CollRemove(ok)
		}
	}

	// Phase 4: drop the draw vectors so the new layout rebuilds them lazily.
	c.srv.store.CollRandDrop(prefix)
}

// lockMemberPartition locks the partition stripe member routes to under set skey and returns the held
// mutex, the partition, and the partition count it locked under, re-reading P after acquiring the lock
// so a migration that grew the set while this op waited on the stripe re-routes the member to its new
// home before the caller writes it (spec 2064/f1_rewrite_ltm/19 section 11.6). A migration holds every
// partition stripe over [0, newP), so a single-member writer that arrives mid-migration blocks here on
// its partition lock; when it acquires the lock the migration has finished and published newP, the
// re-read sees the new P, and the op releases and re-routes. Because the transition only ever raises P
// and terminates, the loop reads either the old or the new P and converges to a stable value, so it
// always returns with the member's partition lock held under the P currently in force.
func (c *connState) lockMemberPartition(skey, member []byte, p int) (*sync.RWMutex, int, int) {
	for {
		part := f1raw.PartitionOf(member, p)
		mu := &c.srv.incrMu[c.srv.stripePart(skey, part)]
		mu.Lock()
		cur := c.partitionsFor(skey)
		if cur == p {
			return mu, part, p
		}
		mu.Unlock()
		p = cur
	}
}
