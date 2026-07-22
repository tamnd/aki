package set

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The set durability seam (spec 2064/f3/M8-collection-durability-plan, slices 2 and
// 3): the set half of the collection effect log plus its snapshot. A set lives only
// in the shard's owner-local registry, so a crash loses it in full; this file makes
// each set mutation survivable by cutting a small effect frame through the store seam
// (collectionseam.go) and rebuilds the registry from those frames on reopen.
//
// The vocabulary is the minimum a set replay needs: an add names the member that
// joined, a remove names the member that left, a key-delete clears the whole set,
// and a key-expire effect names the deadline an EXPIRE or PERSIST set. An emptied
// set needs no explicit delete effect, because replaying its removes empties it and
// Recover drops a set that reaches zero cardinality, the same last-member-leaves
// rule the live command follows. The non-deterministic and derived commands (SPOP,
// and later SMOVE and the STORE forms) log the resolved member the mutation settled
// on, never the draw or the source verb, which falls out naturally because the
// effect is cut after the mutation resolves. Every log helper is a no-op on a store
// with no .aki handle, so the pure in-memory path is unchanged.
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
	// setOpExpire records the set's key deadline after an EXPIRE-family command or a
	// PERSIST: SubValue is the deadline in unix milliseconds, eight bytes little-
	// endian, 0 when the key was persisted. It carries no SubKey, since the frame key
	// names the set. A replay installs the deadline the live run set, so a volatile set
	// a crash caught between checkpoints keeps its TTL instead of reverting to the last
	// snapshot's. An EXPIRE to a past instant deletes the key on the spot and cuts a
	// key-delete instead, so this op always carries a future or cleared deadline.
	setOpExpire uint8 = 4
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

// logExpire cuts a key-deadline effect for the set at key: the new deadline in unix
// milliseconds, or 0 for a PERSIST that cleared it. It is called after the deadline
// is stored, so a replay reaches the same deadline the live run set. The EXPIRE-to-a-
// past-instant case deletes the key and logs a key-delete instead, never this.
func logExpire(cx *shard.Ctx, key []byte, at int64) {
	if cx.St != nil {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(at))
		cx.St.LogCollectionOp(key, akifile.CollKindSet, setOpExpire, nil, b[:])
	}
}

// snapHeaderLen is the fixed set snapshot header: the key deadline in unix
// milliseconds (0 when the set carries no TTL), little-endian. The header is the
// per-key state the element run does not carry, so a snapshot restores a volatile
// set's TTL as of the checkpoint. A between-checkpoint EXPIRE or PERSIST also cuts
// its own setOpExpire effect, so a volatile set a crash caught after the last
// snapshot keeps the deadline the live run set instead of reverting to the
// snapshot's, closing the gap the member-delta effects left.
const snapHeaderLen = 8

// Snapshot writes a whole-set snapshot frame for every live set on this shard, the
// set arm of the checkpoint dumper (and the clean-shutdown flush). A reopen rebuilds
// each set from its snapshot then replays only the effect tail cut after it, so the
// tail a recovery must re-drive stays bounded to one checkpoint interval. It walks
// every live set rather than only the dirty ones, the simplest correct dumper; a
// dirty-set filter is a deferred, purely additive refinement. It is a no-op on a
// store with no record log and on a shard that has built no set registry.
func Snapshot(cx *shard.Ctx) {
	if cx.St == nil {
		return
	}
	now := cx.NowMs
	if cx.Coll != nil {
		for k, s := range cx.Coll.(*reg).m {
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
	// A tiny set homed inline in the arena lives in no g.m entry, so the checkpoint
	// must fold it to a snapshot frame here too, or a reopen would truncate the
	// effect tail at the checkpoint and lose it. RangeCollKind skips already-expired
	// records (now non-zero), matching the g.m skip above; each surviving blob is
	// materialized into a reused scratch set to render the same snapshot frame an
	// escalated set writes. On recovery the frame rebuilds into g.m (arena-home on
	// recovery is a later slice), which is footprint-equivalent for correctness.
	var scratch set
	cx.St.RangeCollKind(store.KindSet, now, func(key []byte) bool {
		blob, _, bits, at, ok := cx.St.PeekCollBlob(key)
		if !ok {
			return true
		}
		loadInline(&scratch, blob, bits, at)
		header, run := buildSetSnapshot(&scratch)
		cx.St.LogCollectionSnap(key, akifile.CollKindSet, header, run)
		return true
	})
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
			return applySetSnapshot(cx, g, key, snap)
		},
		func(key []byte, op akifile.CollOpRow) error {
			return applySetOp(cx, g, key, op)
		})
}

// applySetSnapshot resets key to the snapshot's members and TTL, superseding every
// effect frame for key that preceded it. It drops any state the earlier tail built,
// rebuilds the set from the element run through the same band-selecting add funnel a
// live run and an effect replay use, and restores the key TTL from the header. An
// empty element run leaves the key dropped, since the registry keeps no empty set.
func applySetSnapshot(cx *shard.Ctx, g *reg, key []byte, snap akifile.CollSnapRow) error {
	g.drop(key)
	var s *set
	if !eachEntry(snap.ElementRun, func(m []byte) {
		if s == nil {
			s = newSet(m)
			g.install(cx, key, s)
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
// a key-delete clears the whole set, and a key-expire sets the deadline. It is the
// effect arm both a live replay and the recovery walk share. It reports ErrLength on
// a torn expire payload, the fail-closed cut recovery wants; an expire on an absent
// key is a defensive no-op, since a deterministic replay never produces one.
func applySetOp(cx *shard.Ctx, g *reg, key []byte, op akifile.CollOpRow) error {
	switch op.Op {
	case setOpAdd:
		s := g.m[string(key)]
		if s == nil {
			s = newSet(op.SubKey)
			g.install(cx, key, s)
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
	case setOpExpire:
		if len(op.SubValue) < 8 {
			return akifile.ErrLength
		}
		if s := g.m[string(key)]; s != nil {
			s.expireAt = int64(binary.LittleEndian.Uint64(op.SubValue))
		}
	}
	return nil
}

// DumpKey renders the set at key to a snapshot row for the DUMP command, the
// single-key sibling of Snapshot. ok is false when key holds no live set (absent
// or lazily expired), so DUMP answers the null bulk. The row's header carries the
// set TTL; RESTORE drives the restored key's deadline from its own ttl argument
// and ignores it, so the payload round-trips through the same snapshot encoder a
// checkpoint uses without a DUMP-specific format.
func DumpKey(cx *shard.Ctx, key []byte) (akifile.CollSnapRow, bool) {
	if cx.Coll != nil {
		if s := cx.Coll.(*reg).peek(cx, key); s != nil {
			header, run := buildSetSnapshot(s)
			return akifile.CollSnapRow{Kind: akifile.CollKindSet, Header: header, ElementRun: run}, true
		}
	}
	// A tiny arena set is materialized into a throwaway set to render the same
	// snapshot row an escalated set produces, so DUMP round-trips a tiny set through
	// the one snapshot encoder.
	if blob, bits, at, present := peekArenaSet(cx, key); present {
		var scratch set
		loadInline(&scratch, blob, bits, at)
		header, run := buildSetSnapshot(&scratch)
		return akifile.CollSnapRow{Kind: akifile.CollKindSet, Header: header, ElementRun: run}, true
	}
	return akifile.CollSnapRow{}, false
}

// RestoreKey installs the set at key from a snapshot row a DUMP produced, the
// single-key sibling of applySetSnapshot, and stamps the key deadline the RESTORE
// command parsed (0 for a persistent key), overriding whatever TTL the payload
// carried. It re-logs the restored set through the durability seam so a crash
// after a RESTORE keeps the key, the durable effect a live command owes. The
// caller has already cleared any prior key (the RESTORE existence check, plus the
// REPLACE delete), so this installs onto a clean slot.
func RestoreKey(cx *shard.Ctx, key []byte, row akifile.CollSnapRow, expireAt int64) error {
	g := registry(cx)
	if err := applySetSnapshot(cx, g, key, row); err != nil {
		return err
	}
	// Fetch the just-installed set from the raw map, not lookup: applySetSnapshot
	// stamped the payload's TTL, and if that deadline is already past, lookup would
	// treat the key as expired and drop it before we can stamp the RESTORE deadline.
	s := g.m[string(key)]
	if s == nil {
		return nil
	}
	s.expireAt = expireAt
	if cx.St != nil {
		header, run := buildSetSnapshot(s)
		cx.St.LogCollectionSnap(key, akifile.CollKindSet, header, run)
	}
	g.note(s)
	return nil
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
