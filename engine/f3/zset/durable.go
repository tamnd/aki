package zset

import (
	"encoding/binary"
	"math"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
)

// The zset durability seam (spec 2064/f3/M8-collection-durability-plan, the zset arm
// of slices 2 and 3): the zset half of the collection effect log plus its snapshot. A
// zset lives only in the shard's owner-local registry (reg.go), so a crash loses it in
// full; this file makes each score mutation survivable by cutting a small effect frame
// through the store seam (store/collectionseam.go) and rebuilds the registry from
// those frames on reopen, the same shape the set and hash verticals took.
//
// The vocabulary is the minimum a zset replay needs: an add names the member and the
// score it now holds, a remove names the member that left, a key-delete clears the
// whole zset, and a key-expire effect names the deadline an EXPIRE or PERSIST set. An
// add is logged on both a new member and a score move, since both change
// the value a replay must reproduce; the derived writers (ZINCRBY, ZADD INCR) log the
// resolved score the mutation settled on, never the delta, which falls out naturally
// because the effect is cut after update returns the new score. The pop and range verbs
// (ZPOPMIN, ZPOPMAX, ZMPOP, ZREMRANGEBY*) log the resolved member that left, never the
// end or window they resolved from, so a replay reproduces the exact members deleted
// without re-running the non-deterministic resolution. An emptied zset needs no explicit
// delete effect, because replaying its removes empties it and Recover drops a zset that
// reaches zero cardinality, the same last-member-leaves rule the live command follows.
// Every log helper is a no-op on a store with no .aki handle, so the pure in-memory path
// is unchanged.
//
// The snapshot half (the zset arm of slice 3) folds each live zset to one whole-zset
// snapshot frame at the checkpoint (Snapshot): the score-member pairs packed in
// ascending zset order under a small header carrying the key TTL. A reopen rebuilds each
// zset from its last snapshot and replays only the effect tail cut after it, the same
// bounded checkpoint-plus-tail path the string index recovery and the set and hash
// verticals take. The cadence is snapshot-at-checkpoint-and-shutdown only, the string
// model one level down.
//
// Both the effect replay and the snapshot rebuild re-drive each member through the live
// update funnel (z.update), so the band a member lands in and the score it holds match
// the live run's, and the score fidelity a zset carries is exactly the live one. The one
// edge is a signed zero: a member added at -0.0 keeps its sign only in the native band,
// while the listpack band collapses it to +0.0 (the pinned listpack quirk zset.go
// documents). A rebuild starts every zset as a listpack and re-promotes it on the same
// cap the live run hit, so a -0.0 member in a native-band zset re-materializes as +0.0,
// the same collapse a fresh listpack ZADD applies. This is the zset analog of the
// deferred TTL edges the set and hash verticals carry, and is left as its own coherent
// follow-on (the native-band signed-zero fidelity, orthogonal to the key deadline).
//
// Key TTL is durable: a between-checkpoint EXPIRE or PERSIST over a zset cuts its own
// zsetOpExpire effect, and the snapshot header restores a zset's key TTL as of the last
// checkpoint, so a crash after the last snapshot keeps the deadline the live run set
// rather than reverting to the snapshot's.

const (
	// zsetOpAdd records that member now holds a score at key, re-driven on recovery
	// as an add through the same update funnel a live ZADD uses. SubKey is the member,
	// SubValue the score as eight big-endian IEEE-754 bytes.
	zsetOpAdd uint8 = 1
	// zsetOpRemove records that member left the zset at key, the effect a ZREM, a
	// resolved pop, or a range removal cuts. SubKey is the member.
	zsetOpRemove uint8 = 2
	// zsetOpDeleteKey records that the whole zset at key was dropped, the effect a
	// DEL cuts so a replay clears the key instead of resurrecting its members.
	zsetOpDeleteKey uint8 = 3
	// zsetOpExpire records the zset's key deadline after an EXPIRE-family command or a
	// PERSIST: SubValue is the deadline in unix milliseconds, eight bytes little-endian,
	// 0 when the key was persisted. It carries no SubKey, since the frame key names the
	// zset. A replay installs the deadline the live run set, so a volatile zset a crash
	// caught between checkpoints keeps its TTL. An EXPIRE to a past instant deletes the
	// key on the spot and cuts a key-delete instead, so this op always carries a future
	// or cleared deadline.
	zsetOpExpire uint8 = 4
)

// logAdd cuts an add effect for member taking score at key. It is called after update
// resolves and only when a value was written (a new member or a moved score), so a
// replay reapplies exactly the score the live run stored.
func logAdd(cx *shard.Ctx, key, member []byte, score float64) {
	if cx.St != nil {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], math.Float64bits(score))
		cx.St.LogCollectionOp(key, akifile.CollKindZset, zsetOpAdd, member, b[:])
	}
}

// logRemove cuts a remove effect for member leaving the zset at key, the effect a ZREM,
// a resolved ZPOPMIN/ZPOPMAX/ZMPOP draw, or a resolved ZREMRANGEBY* window records.
func logRemove(cx *shard.Ctx, key, member []byte) {
	if cx.St != nil {
		cx.St.LogCollectionOp(key, akifile.CollKindZset, zsetOpRemove, member, nil)
	}
}

// logDeleteKey cuts a key-delete effect for the zset at key, the effect a DEL over a
// zset records so a replay does not rebuild the members a later effect no longer
// supersedes.
func logDeleteKey(cx *shard.Ctx, key []byte) {
	if cx.St != nil {
		cx.St.LogCollectionOp(key, akifile.CollKindZset, zsetOpDeleteKey, nil, nil)
	}
}

// logExpire cuts a key-deadline effect for the zset at key: the new deadline in unix
// milliseconds, or 0 for a PERSIST that cleared it. It is called after the deadline is
// stored, so a replay reaches the same deadline the live run set. The EXPIRE-to-a-past-
// instant case deletes the key and logs a key-delete instead, never this.
func logExpire(cx *shard.Ctx, key []byte, at int64) {
	if cx.St != nil {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(at))
		cx.St.LogCollectionOp(key, akifile.CollKindZset, zsetOpExpire, nil, b[:])
	}
}

// logRemoveWindow cuts a remove effect for every member in the half-open rank window
// [lo, hiExcl), the shared logging the ZREMRANGEBY* verbs run just before removeRange
// deletes the window. It walks the window while the members are still live, so the
// resolved members reach the log before the surgery drops them. It is a no-op on a store
// with no .aki handle and on an empty window.
func logRemoveWindow(cx *shard.Ctx, key []byte, z *zset, lo, hiExcl int) {
	if cx.St == nil || hiExcl <= lo {
		return
	}
	z.eachInRankWindow(lo, hiExcl, func(m []byte) {
		logRemove(cx, key, m)
	})
}

// eachInRankWindow visits every member at the half-open forward-rank window [lo, hiExcl)
// in ascending order, the walk logRemoveWindow drives over a window a range removal is
// about to delete. The native band walks the leaf chain over just the window; the inline
// band slices its already-ordered entries. The member bytes alias live storage and are
// valid only for the call, the single-call lifetime the log helper copies out of.
func (z *zset) eachInRankWindow(lo, hiExcl int, fn func(m []byte)) {
	if hiExcl <= lo {
		return
	}
	if z.enc == encSkiplist {
		z.nat.walkRange(lo, hiExcl-1, func(m []byte, _ uint64) { fn(m) })
		return
	}
	ev := z.entries()
	for j := lo; j < hiExcl && j < len(ev); j++ {
		fn(ev[j].member)
	}
}

// snapHeaderLen is the fixed zset snapshot header: the key deadline in unix milliseconds
// (0 when the zset carries no key TTL), little-endian. The header is the per-key state
// the element run does not carry, so a snapshot restores a volatile zset's key TTL as of
// the checkpoint; a between-checkpoint EXPIRE or PERSIST also cuts its own zsetOpExpire
// effect so the deadline is durable between checkpoints.
const snapHeaderLen = 8

// Snapshot writes a whole-zset snapshot frame for every live zset on this shard, the
// zset arm of the checkpoint dumper (and the clean-shutdown flush). A reopen rebuilds
// each zset from its snapshot then replays only the effect tail cut after it, so the tail
// a recovery must re-drive stays bounded to one checkpoint interval. It reaches the
// registry through the owner-local ZColl slot, so a shard that ran no zset command
// snapshots nothing, and skips a zset the key deadline has already fired. It is a no-op
// on a store with no record log.
func Snapshot(cx *shard.Ctx) {
	if cx.St == nil || cx.ZColl == nil {
		return
	}
	g := cx.ZColl.(*reg)
	now := cx.NowMs
	for k, z := range g.m {
		// Skip a lazily-expired zset: a snapshot of it would durably resurrect a key
		// EXISTS already reports absent, so let the next access drop it. The skip is
		// read-only, matching the scan walks.
		if z.expireAt != 0 && z.expireAt <= now {
			continue
		}
		header, run := buildZsetSnapshot(z)
		cx.St.LogCollectionSnap([]byte(k), akifile.CollKindZset, header, run)
	}
}

// buildZsetSnapshot renders a live zset to a snapshot payload: the header carries the key
// TTL and the element run packs every score-member pair in ascending zset order. Each
// pair is the score as eight big-endian IEEE-754 bytes followed by the length-prefixed
// member, so recovery decodes the run forward and re-drives the pairs through the same
// update funnel a live run uses. It reads the whole zset through forEach, which spans
// both bands and preads any demoted member back, so a snapshot of a partly-cold zset is
// complete. It allocates fresh slices, a checkpoint-time cost off the steady mutation
// path.
func buildZsetSnapshot(z *zset) (header, elementRun []byte) {
	header = make([]byte, snapHeaderLen)
	binary.LittleEndian.PutUint64(header, uint64(z.expireAt))
	z.forEach(func(m []byte, s float64) bool {
		elementRun = appendScoredEntry(elementRun, s, m)
		return true
	})
	return header, elementRun
}

// appendScoredEntry packs one score-member pair onto a snapshot run: the score as eight
// big-endian IEEE-754 bytes (the raw double bits, so the exact score including a signed
// zero survives the wire) followed by the length-prefixed member, the same uvarint frame
// the set and hash runs use for their elements. eachScoredEntry reads the pair back.
func appendScoredEntry(run []byte, score float64, member []byte) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], math.Float64bits(score))
	run = append(run, b[:]...)
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], uint64(len(member)))
	run = append(run, lb[:n]...)
	return append(run, member...)
}

// eachScoredEntry walks a score-member run forward, calling fn for each pair, the O(n)
// reader the snapshot rebuild uses over the wire form appendScoredEntry writes. It
// reports false on a torn run, which recovery treats as a corrupt frame. The member
// slice fn receives aliases the run and is valid only for the call.
func eachScoredEntry(run []byte, fn func(score float64, member []byte)) bool {
	p := run
	for len(p) > 0 {
		if len(p) < 8 {
			return false
		}
		score := math.Float64frombits(binary.BigEndian.Uint64(p))
		p = p[8:]
		mlen, w := binary.Uvarint(p)
		if w <= 0 || uint64(len(p)-w) < mlen {
			return false
		}
		p = p[w:]
		fn(score, p[:mlen])
		p = p[mlen:]
	}
	return true
}

// Recover rebuilds this shard's zsets from the record log's zset frames, re-driving each
// frame in append order onto a fresh registry. It is the zset arm of an .aki reopen, the
// sibling of set.Recover and hash.Recover: after the store's string index recovery, the
// runtime calls Recover so a restart restores the zsets a crash would otherwise lose. A
// snapshot frame resets its key to the snapshotted members and key TTL, and every effect
// frame after it applies on top, so a zset rebuilds from its last snapshot plus its
// effect tail, the bounded path the string index recovery takes. It applies through the
// live update funnel (z.update, z.rem), so the rebuild re-logs nothing and the band a
// member lands in matches the live run's. A zset that reaches zero cardinality is
// dropped, matching the live last-member-leaves rule, and a key-delete effect drops the
// whole zset. It is a no-op on a store with no record log.
func Recover(cx *shard.Ctx) error {
	if cx.St == nil {
		return nil
	}
	g := registry(cx)
	return cx.St.WalkCollection(akifile.CollKindZset,
		func(key []byte, snap akifile.CollSnapRow) error {
			return applyZsetSnapshot(cx, g, key, snap)
		},
		func(key []byte, op akifile.CollOpRow) error {
			return applyZsetOp(cx, g, key, op)
		})
}

// applyZsetSnapshot resets key to the snapshot's members and key TTL, superseding every
// effect frame for key that preceded it. It drops any state the earlier tail built,
// rebuilds the zset from the score-member run through the same update funnel a live run
// and an effect replay use, and restores the key TTL from the header. An empty element
// run leaves the key dropped, since the registry keeps no empty zset. A torn run reports
// ErrLength, the fail-closed cut a recovering reader wants.
// DumpKey renders the sorted set at key to a snapshot row for the DUMP command,
// the single-key sibling of Snapshot. ok is false when key holds no live zset, so
// DUMP answers the null bulk. The row round-trips through RestoreKey, which drives
// the restored key's deadline from the RESTORE ttl argument rather than the
// payload header.
func DumpKey(cx *shard.Ctx, key []byte) (akifile.CollSnapRow, bool) {
	if cx.ZColl == nil {
		return akifile.CollSnapRow{}, false
	}
	g := cx.ZColl.(*reg)
	z := g.peek(cx, key)
	if z == nil {
		return akifile.CollSnapRow{}, false
	}
	header, run := buildZsetSnapshot(z)
	return akifile.CollSnapRow{Kind: akifile.CollKindZset, Header: header, ElementRun: run}, true
}

// RestoreKey installs the sorted set at key from a DUMP snapshot row, stamps the
// key deadline the RESTORE command parsed (0 for persistent), and re-logs the
// restored zset through the durability seam so a crash keeps it. The caller has
// cleared any prior key.
func RestoreKey(cx *shard.Ctx, key []byte, row akifile.CollSnapRow, expireAt int64) error {
	g := registry(cx)
	if err := applyZsetSnapshot(cx, g, key, row); err != nil {
		return err
	}
	// Raw-map fetch, not lookup: the payload TTL applyZsetSnapshot stamped may be
	// past, and lookup would drop the key before we stamp the RESTORE deadline.
	z := g.m[string(key)]
	if z == nil {
		return nil
	}
	z.expireAt = expireAt
	if cx.St != nil {
		header, run := buildZsetSnapshot(z)
		cx.St.LogCollectionSnap(key, akifile.CollKindZset, header, run)
	}
	g.note(z)
	return nil
}

func applyZsetSnapshot(cx *shard.Ctx, g *reg, key []byte, snap akifile.CollSnapRow) error {
	g.drop(key)
	var z *zset
	if !eachScoredEntry(snap.ElementRun, func(score float64, member []byte) {
		if z == nil {
			z = newZset()
			g.install(cx, key, z)
		}
		z.update(member, score, flags{})
	}) {
		return akifile.ErrLength
	}
	if z == nil {
		return nil
	}
	if len(snap.Header) >= snapHeaderLen {
		z.expireAt = int64(binary.LittleEndian.Uint64(snap.Header))
	}
	g.note(z)
	return nil
}

// applyZsetOp re-drives one zset effect onto the registry: an add creates the zset on its
// first member and writes the score, a remove drops the member and the key on the last
// one, a key-delete clears the whole zset, and a key-expire sets the deadline. It is the
// effect arm the recovery walk drives. It goes through z.update, so a member that breaches
// a listpack cap promotes to the native band exactly as it did live. A torn add score or a
// torn expire payload reports ErrLength.
func applyZsetOp(cx *shard.Ctx, g *reg, key []byte, op akifile.CollOpRow) error {
	switch op.Op {
	case zsetOpAdd:
		if len(op.SubValue) < 8 {
			return akifile.ErrLength
		}
		score := math.Float64frombits(binary.BigEndian.Uint64(op.SubValue))
		z := g.m[string(key)]
		if z == nil {
			z = newZset()
			g.install(cx, key, z)
		}
		z.update(op.SubKey, score, flags{})
		g.note(z)
	case zsetOpRemove:
		z := g.m[string(key)]
		if z == nil {
			return nil
		}
		z.rem(op.SubKey)
		if z.card() == 0 {
			g.drop(key)
		} else {
			g.note(z)
		}
	case zsetOpDeleteKey:
		g.drop(key)
	case zsetOpExpire:
		if len(op.SubValue) < 8 {
			return akifile.ErrLength
		}
		if z := g.m[string(key)]; z != nil {
			z.expireAt = int64(binary.LittleEndian.Uint64(op.SubValue))
		}
	}
	return nil
}
