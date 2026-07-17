package set

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
)

// The set durability seam (spec 2064/f3/M8-collection-durability-plan, slices 2 and
// 3): the set half of the collection effect log plus its snapshot. A set lives only
// in the shard's owner-local registry, so a crash loses it in full; this file makes
// each set mutation survivable by cutting a small effect frame through the store seam
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
//
// Slice 3 adds the snapshot half. A long-lived set accretes an unbounded effect tail
// between checkpoints, so the checkpoint folds each live set to one whole-set
// snapshot frame (Snapshot): the set members packed with the same member encoder the
// cold chunk uses (appendEntry), under a small header carrying the key TTL. A reopen
// then rebuilds each set from its last snapshot and replays only the effect tail cut
// after it, the same bounded checkpoint-plus-tail path the string index recovery
// takes. The cadence is snapshot-at-checkpoint-and-shutdown only, the string model
// one level down (the plan's resolved open question); the per-key effect-count
// threshold stays deferred as a reversible optimization.

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

// snapHeaderLen is the fixed set snapshot header: the key deadline in unix
// milliseconds (0 when the set carries no TTL), little-endian. The header is the
// per-key state the element run does not carry, so a snapshot restores a volatile
// set's TTL where the effect log, which logs member deltas only, cannot yet. A
// between-checkpoint EXPIRE stays outside the durable set until a key-expire effect
// lands (a deferred follow-on), so a crash before the next checkpoint loses only a
// TTL set after the last snapshot, never a member.
const snapHeaderLen = 8

// Snapshot writes a whole-set snapshot frame for every live set on this shard, the
// set arm of the checkpoint dumper (and the clean-shutdown flush). A reopen rebuilds
// each set from its snapshot then replays only the effect tail cut after it, so the
// tail a recovery must re-drive stays bounded to one checkpoint interval. It walks
// every live set rather than only the dirty ones, the simplest correct dumper; a
// dirty-set filter is a deferred, purely additive refinement. It is a no-op on a
// store with no record log and on a shard that has built no set registry.
func Snapshot(cx *shard.Ctx) {
	if cx.St == nil || cx.Coll == nil {
		return
	}
	g := cx.Coll.(*reg)
	now := cx.NowMs
	for k, s := range g.m {
		// Skip a lazily-expired set: a snapshot of it would durably resurrect a key
		// EXISTS already reports absent, so let it fall out the way the live registry
		// drops it on next access. The skip is read-only, matching the scan walks.
		if s.expireAt != 0 && s.expireAt <= now {
			continue
		}
		header, run := buildSetSnapshot(s)
		cx.St.LogCollectionSnap([]byte(k), akifile.CollKindSet, header, run)
	}
}

// buildSetSnapshot renders a live set to a snapshot payload: the header carries the
// key TTL and the element run packs every member (resident or cold) with the same
// length-prefixed encoder the cold chunk uses (appendEntry), so recovery decodes the
// run with the reader the cold path already trusts. It reads the whole set through
// each, which preads any demoted member back, so a snapshot of a partly-cold set is
// complete. It allocates fresh slices, a checkpoint-time cost off the steady mutation
// path.
func buildSetSnapshot(s *set) (header, elementRun []byte) {
	header = make([]byte, snapHeaderLen)
	binary.LittleEndian.PutUint64(header, uint64(s.expireAt))
	s.each(func(m []byte) {
		elementRun = appendEntry(elementRun, m)
	})
	return header, elementRun
}

// Recover rebuilds this shard's sets from the record log's set frames, re-driving
// each frame in append order onto a fresh registry. It is the set arm of an .aki
// reopen: after the store's string index recovery, the runtime calls Recover so a
// restart restores the sets a crash would otherwise lose. A snapshot frame resets its
// key to the snapshotted members and TTL, and every effect frame after it applies on
// top, so a set rebuilds from its last snapshot plus its effect tail, the bounded
// path the string index recovery takes. It applies effects through the low-level
// band-selecting mutators, not the logging command wrappers, so the rebuild re-logs
// nothing and the band a member lands in matches the live run's. A set that reaches
// zero cardinality is dropped, matching the live last-member-leaves rule, and a
// key-delete effect drops the whole set. It is a no-op on a store with no record log.
func Recover(cx *shard.Ctx) error {
	if cx.St == nil {
		return nil
	}
	g := registry(cx)
	return cx.St.WalkCollection(akifile.CollKindSet,
		func(key []byte, snap akifile.CollSnapRow) error {
			return applySetSnapshot(g, key, snap)
		},
		func(key []byte, op akifile.CollOpRow) error {
			applySetOp(g, key, op)
			return nil
		})
}

// applySetSnapshot resets key to the snapshot's members and TTL, superseding every
// effect frame for key that preceded it. It drops any state the earlier tail built,
// rebuilds the set from the element run through the same band-selecting add funnel a
// live run and an effect replay use, and restores the key TTL from the header. An
// empty element run leaves the key dropped, since the registry keeps no empty set.
func applySetSnapshot(g *reg, key []byte, snap akifile.CollSnapRow) error {
	g.drop(key)
	var s *set
	if !eachEntry(snap.ElementRun, func(m []byte) {
		if s == nil {
			s = newSet(m)
			g.m[string(key)] = s
		}
		s.add(m)
	}) {
		return akifile.ErrLength
	}
	if s == nil {
		return nil
	}
	if len(snap.Header) >= snapHeaderLen {
		s.expireAt = int64(binary.LittleEndian.Uint64(snap.Header))
	}
	g.note(s)
	return nil
}

// applySetOp re-drives one set effect onto the registry: an add creates the set on
// its first member and adds, a remove drops the member and the key on the last one,
// and a key-delete clears the whole set. It is the effect arm both a live replay and
// the recovery walk share.
func applySetOp(g *reg, key []byte, op akifile.CollOpRow) {
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
			return
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
}

// eachEntry walks a length-prefixed member run forward, calling fn for each member,
// the O(n) reader the snapshot rebuild uses over the same wire form appendEntry
// writes (chunkEntry is the positional O(idx) sibling the cold read path uses). It
// reports false on a torn run, which recovery treats as a corrupt frame. The []byte
// fn receives aliases the run and is valid only for the call.
func eachEntry(run []byte, fn func(m []byte)) bool {
	p := run
	for len(p) > 0 {
		n, w := binary.Uvarint(p)
		if w <= 0 || uint64(len(p)-w) < n {
			return false
		}
		p = p[w:]
		fn(p[:n])
		p = p[n:]
	}
	return true
}
