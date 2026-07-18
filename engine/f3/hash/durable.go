package hash

import (
	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
)

// The hash durability seam (spec 2064/f3/M8-collection-durability-plan, the hash arm
// of slice 2): the hash half of the collection effect log. A hash lives only in the
// shard's owner-local registry (reg.go), so a crash loses it in full; this file makes
// each field mutation survivable by cutting a small effect frame through the store
// seam (store/collectionseam.go) and rebuilds the registry from those frames on
// reopen, the same shape the set vertical took.
//
// The vocabulary is the minimum a hash replay needs: a set names the field and the
// value it now holds, a field-delete names the field that left, and a key-delete
// clears the whole hash. A set is logged on both a new field and an overwrite, since
// both change the value a replay must reproduce; HSETNX logs only when it actually
// sets. The derived writers (HINCRBY, HINCRBYFLOAT) log the resolved value the
// mutation settled on, never the delta, which falls out naturally because the effect
// is cut after the value is computed and written. An emptied hash needs no explicit
// delete effect, because replaying its field-deletes empties it and Recover drops a
// hash that reaches zero cardinality, the same last-field-leaves rule the live
// command follows. Every log helper is a no-op on a store with no .aki handle, so the
// pure in-memory path is unchanged.
//
// Field TTL (the HEXPIRE, HPERSIST, and HGETEX family, and the lazy reap they drive)
// is deferred as one coherent follow-on, the hash analog of the set vertical's
// deferred key-expire effect. This slice logs field values and field deletes, not
// field deadlines, so a crash before the next checkpoint loses a field TTL set after
// the last snapshot, and an HEXPIRE that deleted a field on the spot (a set-to-the-
// past) can leave that field to reappear on an effect-only replay. The snapshot slice
// captures the live TTL state at each checkpoint and the field-TTL effect slice makes
// the between-checkpoint deadline durable; together they close the gap. The snapshot
// half itself (buildHashSnapshot and the checkpoint dumper) is the sibling slice this
// one clears the path for, so Recover carries a no-op snapshot arm until it lands.

const (
	// hashOpSet records that field now holds a value at key, re-driven on recovery
	// as a set through the same band-selecting funnel a live HSET uses. SubKey is the
	// field, SubValue the value.
	hashOpSet uint8 = 1
	// hashOpDelField records that field left the hash at key, the effect an HDEL or
	// HGETDEL cuts. SubKey is the field.
	hashOpDelField uint8 = 2
	// hashOpDeleteKey records that the whole hash at key was dropped, the effect a
	// DEL cuts so a replay clears the key instead of resurrecting its fields.
	hashOpDeleteKey uint8 = 3
)

// logSet cuts a set effect for field taking value at key. It is called after the
// write resolves, so a replay reapplies exactly the value the live run stored,
// whether the field was new or overwritten.
func logSet(cx *shard.Ctx, key, field, value []byte) {
	if cx.St != nil {
		cx.St.LogCollectionOp(key, akifile.CollKindHash, hashOpSet, field, value)
	}
}

// logDelField cuts a field-delete effect for field leaving the hash at key, the
// effect an HDEL or a resolved HGETDEL records.
func logDelField(cx *shard.Ctx, key, field []byte) {
	if cx.St != nil {
		cx.St.LogCollectionOp(key, akifile.CollKindHash, hashOpDelField, field, nil)
	}
}

// logDeleteKey cuts a key-delete effect for the hash at key, the effect a DEL over a
// hash records so a replay does not rebuild the fields a later effect no longer
// supersedes.
func logDeleteKey(cx *shard.Ctx, key []byte) {
	if cx.St != nil {
		cx.St.LogCollectionOp(key, akifile.CollKindHash, hashOpDeleteKey, nil, nil)
	}
}

// Recover rebuilds this shard's hashes from the record log's hash frames, re-driving
// each effect in append order onto a fresh registry. It is the hash arm of an .aki
// reopen, the sibling of set.Recover: after the store's string index recovery, the
// runtime calls Recover so a restart restores the hashes a crash would otherwise
// lose. It applies effects through the low-level band-selecting mutators (hash.set,
// hash.del), not the logging command wrappers, so the rebuild re-logs nothing and the
// band a field lands in matches the live run's. A hash that reaches zero cardinality
// is dropped, matching the live last-field-leaves rule, and a key-delete effect drops
// the whole hash. The snapshot arm is a no-op until the snapshot slice lands, so a
// slice-2 log holds only effects and Recover replays them from an empty registry. It
// is a no-op on a store with no record log.
func Recover(cx *shard.Ctx) error {
	if cx.St == nil {
		return nil
	}
	g := registry(cx)
	return cx.St.WalkCollection(akifile.CollKindHash,
		func(key []byte, snap akifile.CollSnapRow) error {
			// Hash snapshots arrive in the next slice; a slice-2 effect log never
			// contains one, so the walk skips it here and the snapshot slice replaces
			// this arm with a real applyHashSnapshot.
			return nil
		},
		func(key []byte, op akifile.CollOpRow) error {
			applyHashOp(g, key, op)
			return nil
		})
}

// applyHashOp re-drives one hash effect onto the registry: a set creates the hash on
// its first field and writes the pair, a field-delete drops the field and the key on
// the last one, and a key-delete clears the whole hash. It is the effect arm the
// recovery walk drives. It goes through hash.set and hash.del, so a field that
// breaches an inline threshold promotes to the native band exactly as it did live.
func applyHashOp(g *reg, key []byte, op akifile.CollOpRow) {
	switch op.Op {
	case hashOpSet:
		h := g.m[string(key)]
		if h == nil {
			h = newHash()
			g.m[string(key)] = h
		}
		h.set(op.SubKey, op.SubValue)
		g.note(h)
	case hashOpDelField:
		h := g.m[string(key)]
		if h == nil {
			return
		}
		h.del(op.SubKey)
		if h.card() == 0 {
			g.drop(key)
		} else {
			g.note(h)
		}
	case hashOpDeleteKey:
		g.drop(key)
	}
}
