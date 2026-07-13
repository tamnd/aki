package stream

import (
	structs "github.com/tamnd/aki/engine/f3/struct"
)

// The pending-entries list (spec 2064/f3/14 section 7.4): a group's record of the
// entries it has delivered but not yet acknowledged. It is one counted tree over a
// shared slab arena, both owner-local, created lazily on a group's first delivery
// so a group that only reads or never delivers pays nothing for it. Every access
// the surface needs, the point ack, the owner lookup, and the ID-range scan, is a
// tree op keyed by ID, and one record per pending entry carries the owner as a
// field, so an ack resolves the owner from the record the tree returned and a claim
// rewrites it in place.
//
// Doc 14 section 7.4 spec'd a hash beside the tree for O(1) point ops. Lab 03
// (labs/f3/m5/03_pel_slab) measured it over the real counted tree and rejected it:
// the Go map adds ~56 B per pending of pure overhead (132 B vs the tree-only 76 B),
// the largest index of the three arms, and it does not even speed the ack it exists
// for, since the tree delete must run regardless to keep the range scan correct, so
// the map probe is added work on top of the same descent (XACK ~1.3x slower). The
// tree serves both shapes, so the hash is pure loss against the memory bar
// (PRED-F3-M5-STREAMMEM). The PEL is tree-only.
//
// The tree reuses M2's counted substrate exactly as the block directory does: the
// ID's ms is the 8-byte score, its seq the big-endian member the tree ties on, and
// the stored reference is the slab ordinal, which the Members callback turns back
// into the ID's seq bytes for a same-ms compare.

// noOwner marks a pending slab that no consumer owns yet, the transient state a
// FORCE claim leaves between creating the slab and assigning its consumer.
const noOwner = ^uint32(0)

// pelEntry is one pending entry, the 32-byte slab the tree references by ordinal
// (section 7.4). id is the delivered entry's ID; deliveryTime is the unix ms of the
// last delivery, the idle clock XPENDING and the claim min-idle read; deliveryCount
// is the RETRYCOUNT; consumerOrd is the owning consumer's ordinal, the field an ack
// reads and a claim rewrites.
type pelEntry struct {
	id            streamID
	deliveryTime  int64
	deliveryCount uint16
	consumerOrd   uint32
}

// groupPEL is a group's whole pending set: the slab arena and the id-ordered
// counted tree over it. It is created lazily on a group's first delivery.
type groupPEL struct {
	slabs  []pelEntry    // the arena, addressed by ordinal
	free   []uint32      // freed ordinals, reused before the arena grows
	tree   *structs.Tree // id-ordered, ref = slab ordinal
	seqKey [8]byte       // Members scratch for the tie-break compare
}

// newPEL builds an empty pending list with its tree ready.
func newPEL() *groupPEL {
	return &groupPEL{tree: structs.NewTree()}
}

// Member resolves a PEL tree reference (a slab ordinal) to its ID's seq in
// big-endian bytes, the tie-break the counted tree compares when two pending IDs
// share an ms. It satisfies structs.Members for the PEL tree, the same role
// stream.Member plays for the block directory.
func (p *groupPEL) Member(ref uint32) []byte {
	putSeq(p.seqKey[:], p.slabs[ref].id.seq)
	return p.seqKey[:]
}

// alloc returns a slab ordinal to fill, reusing a freed one before growing the
// arena so an ack-then-deliver cycle does not leak slots.
func (p *groupPEL) alloc() uint32 {
	if n := len(p.free); n > 0 {
		ord := p.free[n-1]
		p.free = p.free[:n-1]
		return ord
	}
	p.slabs = append(p.slabs, pelEntry{})
	return uint32(len(p.slabs) - 1)
}

// insert records id as freshly pending for the consumer ordinal at time now, with
// delivery count one. The `>` delivery path only ever inserts IDs strictly above
// the group cursor, so an insert is always a new entry, never a redelivery; the
// claim path (a later slice) rewrites an existing slab in place instead.
func (p *groupPEL) insert(id streamID, now int64, consumerOrd uint32) {
	ord := p.alloc()
	p.slabs[ord] = pelEntry{id: id, deliveryTime: now, deliveryCount: 1, consumerOrd: consumerOrd}
	p.tree.Insert(id.ms, seqKey(id), ord, p)
}

// ack removes id from the pending list, returning the owning consumer's ordinal so
// the caller drops that consumer's count. ok is false when id was not pending (an
// ack of an already-acked or never-delivered ID, which counts zero). The tree
// delete locates the record and hands back its ordinal, and the owner is read
// straight from that slab, never a second lookup.
func (p *groupPEL) ack(id streamID) (consumerOrd uint32, ok bool) {
	ord, present := p.tree.Delete(id.ms, seqKey(id), p)
	if !present {
		return 0, false
	}
	owner := p.slabs[ord].consumerOrd
	p.free = append(p.free, ord)
	return owner, true
}

// find returns the slab for id and whether it is pending, a point lookup that
// leaves the tree untouched so a claim or a nack can rewrite the record in place
// (sections 7.6, 7.7). The returned pointer is into the arena, valid until the
// next alloc grows it, which a single claim never does mid-use.
func (p *groupPEL) find(id streamID) (*pelEntry, bool) {
	ref, ok := p.tree.Find(id.ms, seqKey(id), p)
	if !ok {
		return nil, false
	}
	return &p.slabs[ref], true
}

// insertClaimed records id as pending with no owner yet, an idle clock at the
// epoch, and a zero delivery count, the slab a FORCE claim of a not-yet-pending
// entry creates before the claim assigns its consumer and stamps its times
// (section 7.7). It returns the slab so the caller fills it; the epoch clock makes
// the claim's min-idle gate pass unconditionally, as Redis's force path does.
func (p *groupPEL) insertClaimed(id streamID) *pelEntry {
	ord := p.alloc()
	p.slabs[ord] = pelEntry{id: id, consumerOrd: noOwner}
	p.tree.Insert(id.ms, seqKey(id), ord, p)
	return &p.slabs[ord]
}

// walkFrom visits the pending entries with IDs at or above start in ID order,
// stopping when fn returns false. It is the range scan XPENDING and the consumer
// drain ride, an O(log p) seek plus a linked-leaf walk.
func (p *groupPEL) walkFrom(start streamID, fn func(*pelEntry) bool) {
	p.tree.WalkFrom(start.ms, seqKey(start), p, func(_ uint64, ref uint32) bool {
		return fn(&p.slabs[ref])
	})
}

// minEntry and maxEntry return the least and greatest pending entries, the two end
// peeks XPENDING's summary reports as the pending ID range. They are nil when the
// list is empty.
func (p *groupPEL) minEntry() *pelEntry {
	if p.tree.Len() == 0 {
		return nil
	}
	_, ref, _ := p.tree.SelectAt(0)
	return &p.slabs[ref]
}

func (p *groupPEL) maxEntry() *pelEntry {
	n := p.tree.Len()
	if n == 0 {
		return nil
	}
	_, ref, _ := p.tree.SelectAt(uint64(n - 1))
	return &p.slabs[ref]
}
