package set

import (
	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
)

// The set durability seam (spec 2064/f3/M8-collection-durability-plan, slice 2):
// the set half of the collection effect log. A set lives only in the shard's
// owner-local registry, so a crash loses it in full; this file makes each set
// mutation survivable by cutting a small effect frame through the store seam
// (collectionseam.go) and rebuilds the registry from those frames on reopen.
//
// The vocabulary is the minimum a set replay needs: an add names the member that
// joined, a remove names the member that left, and a key-delete clears the whole
// set. An emptied set needs no explicit delete effect, because replaying its
// removes empties it and Recover drops a set that reaches zero cardinality, the
// same last-member-leaves rule the live command follows. The non-deterministic and
// derived commands (SPOP, and later SMOVE and the STORE forms) log the resolved
// member the mutation settled on, never the draw or the source verb, which falls
// out naturally because the effect is cut after the mutation resolves. Every log
// helper is a no-op on a store with no .aki handle, so the pure in-memory path is
// unchanged.

const (
	// setOpAdd records that a member joined the set at key, re-driven on recovery
	// as an add through the same band-selecting funnel a live SADD uses.
	setOpAdd uint8 = 1
	// setOpRemove records that a member left the set at key.
	setOpRemove uint8 = 2
	// setOpDeleteKey records that the whole set at key was dropped, the effect a
	// DEL cuts so a replay clears the key instead of resurrecting its members.
	setOpDeleteKey uint8 = 3
)

// logAdd cuts an add effect for member joining the set at key. It is called only
// on a member SADD actually added, so a replay reapplies exactly the mutations the
// live run made.
func logAdd(cx *shard.Ctx, key, member []byte) {
	if cx.St != nil {
		cx.St.LogCollectionOp(key, akifile.CollKindSet, setOpAdd, member, nil)
	}
}

// logRemove cuts a remove effect for member leaving the set at key, the effect a
// SREM or a resolved SPOP draw records.
func logRemove(cx *shard.Ctx, key, member []byte) {
	if cx.St != nil {
		cx.St.LogCollectionOp(key, akifile.CollKindSet, setOpRemove, member, nil)
	}
}

// logDeleteKey cuts a key-delete effect for the set at key, the effect a DEL over a
// set records so a replay does not rebuild the members a later effect no longer
// supersedes.
func logDeleteKey(cx *shard.Ctx, key []byte) {
	if cx.St != nil {
		cx.St.LogCollectionOp(key, akifile.CollKindSet, setOpDeleteKey, nil, nil)
	}
}

// Recover rebuilds this shard's sets from the record log's set effect frames,
// re-driving each add and remove in append order onto a fresh registry. It is the
// set arm of an .aki reopen: after the store's string index recovery, the runtime
// calls Recover so a restart restores the sets a crash would otherwise lose. It
// applies each effect through the low-level band-selecting mutators, not the logging
// command wrappers, so the rebuild re-logs nothing and the band a member lands in
// matches the live run's. A set that reaches zero cardinality is dropped, matching
// the live last-member-leaves rule, and a key-delete effect drops the whole set. It
// is a no-op on a store with no record log.
func Recover(cx *shard.Ctx) error {
	if cx.St == nil {
		return nil
	}
	g := registry(cx)
	return cx.St.WalkCollectionOps(akifile.CollKindSet, func(key []byte, op akifile.CollOpRow) error {
		switch op.Op {
		case setOpAdd:
			s := g.m[string(key)]
			if s == nil {
				s = newSet(op.SubKey)
				g.m[string(key)] = s
			}
			s.add(op.SubKey)
			g.note(s)
		case setOpRemove:
			s := g.m[string(key)]
			if s == nil {
				return nil
			}
			s.rem(op.SubKey)
			if s.card() == 0 {
				g.drop(key)
			} else {
				g.note(s)
			}
		case setOpDeleteKey:
			g.drop(key)
		}
		return nil
	})
}
