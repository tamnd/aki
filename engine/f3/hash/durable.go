package hash

import (
	"encoding/binary"

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
// value it now holds, a field-delete names the field that left, a key-delete clears
// the whole hash, and a key-expire effect names the deadline an EXPIRE or PERSIST set
// (the whole-key deadline, distinct from the per-field HEXPIRE TTLs still deferred
// below). A set is logged on both a new field and an overwrite, since
// both change the value a replay must reproduce; HSETNX logs only when it actually
// sets. The derived writers (HINCRBY, HINCRBYFLOAT) log the resolved value the
// mutation settled on, never the delta, which falls out naturally because the effect
// is cut after the value is computed and written. An emptied hash needs no explicit
// delete effect, because replaying its field-deletes empties it and Recover drops a
// hash that reaches zero cardinality, the same last-field-leaves rule the live
// command follows. Every log helper is a no-op on a store with no .aki handle, so the
// pure in-memory path is unchanged.
//
// The snapshot half (the hash arm of slice 3) folds each live hash to one whole-hash
// snapshot frame at the checkpoint (Snapshot): the field-value pairs packed with the
// same encoder the cold chunk uses (appendEntry), under a small header carrying the
// key TTL. A reopen rebuilds each hash from its last snapshot and replays only the
// effect tail cut after it, the same bounded checkpoint-plus-tail path the string
// index recovery and the set vertical take. The cadence is snapshot-at-checkpoint-and-
// shutdown only, the string model one level down.
//
// Field TTL (the HEXPIRE, HPERSIST, and HGETEX family, and the lazy reap they drive)
// is durable through the same effect-plus-snapshot shape one level down to the field.
// A setter that installs a field deadline cuts a field-expire effect naming the field
// and its deadline, an HPERSIST or an HGETEX PERSIST cuts one carrying a zero deadline,
// and a set-to-the-past that deletes a field on the spot cuts the existing field-delete
// effect, so a between-checkpoint field TTL is not lost. The snapshot header carries a
// field-TTL section after the key deadline: the fields that hold a TTL and their
// deadlines, so a checkpoint captures the per-field state the element run (field values
// only) does not. The lazy reap itself cuts no effect, the same reasoning the key-expire
// slice uses: a replay reconstructs each field's deadline from the effect or the
// snapshot and the first access on the recovered hash reaps a fired field exactly as the
// live run did, so the reap needs no durable record of its own. The snapshot still reaps
// fired fields before folding a hash, so it never durably resurrects an already-expired
// field.

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
	// hashOpExpire records the hash's key deadline after an EXPIRE-family command or a
	// PERSIST: SubValue is the deadline in unix milliseconds, eight bytes little-endian,
	// 0 when the key was persisted. It carries no SubKey, since the frame key names the
	// hash, and it is the whole-key deadline, not a per-field HEXPIRE TTL. A replay
	// installs the deadline the live run set, so a volatile hash a crash caught between
	// checkpoints keeps its TTL. An EXPIRE to a past instant deletes the key on the spot
	// and cuts a key-delete instead, so this op always carries a future or cleared
	// deadline.
	hashOpExpire uint8 = 4
	// hashOpFieldExpire records one field's per-field HEXPIRE deadline: SubKey is the
	// field, SubValue the deadline in unix milliseconds, eight bytes little-endian, 0 for
	// an HPERSIST or an HGETEX PERSIST that cleared it. A replay installs the deadline on
	// the field the earlier hashOpSet created, so the field's next-expire hint matches the
	// live run and the recovered hash reaps the field on its first access exactly as the
	// live one did. An HEXPIRE to a past instant deletes the field on the spot and cuts a
	// hashOpDelField instead, so this op always carries a future or cleared deadline.
	hashOpFieldExpire uint8 = 5
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

// logExpire cuts a key-deadline effect for the hash at key: the new whole-key deadline
// in unix milliseconds, or 0 for a PERSIST that cleared it. It is called after the
// deadline is stored, so a replay reaches the same deadline the live run set. The
// EXPIRE-to-a-past-instant case deletes the key and logs a key-delete instead, never
// this.
func logExpire(cx *shard.Ctx, key []byte, at int64) {
	if cx.St != nil {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(at))
		cx.St.LogCollectionOp(key, akifile.CollKindHash, hashOpExpire, nil, b[:])
	}
}

// logFieldExpire cuts a field-deadline effect for field at key: the new deadline in
// unix milliseconds, or 0 for a persist that cleared it. It is called after the deadline
// is stored, so a replay reaches the same field TTL the live run set. The set-to-a-past-
// instant case deletes the field and cuts a field-delete instead, never this.
func logFieldExpire(cx *shard.Ctx, key, field []byte, at int64) {
	if cx.St != nil {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(at))
		cx.St.LogCollectionOp(key, akifile.CollKindHash, hashOpFieldExpire, field, b[:])
	}
}

// snapHeaderLen is the fixed prefix of the hash snapshot header: the key deadline in
// unix milliseconds (0 when the hash carries no key TTL), little-endian. A field-TTL
// section follows this prefix (buildHashSnapshot), so the header is the per-key and
// per-field TTL state the element run (field values only) does not carry. A snapshot
// restores a volatile hash's key TTL and its per-field deadlines as of the checkpoint,
// and a between-checkpoint EXPIRE, PERSIST, HEXPIRE, or HPERSIST also cuts its own effect
// so both deadlines are durable between checkpoints.
const snapHeaderLen = 8

// Snapshot writes a whole-hash snapshot frame for every live hash on this shard, the
// hash arm of the checkpoint dumper (and the clean-shutdown flush). A reopen rebuilds
// each hash from its snapshot then replays only the effect tail cut after it, so the
// tail a recovery must re-drive stays bounded to one checkpoint interval. It reaches
// the registry through the shared regs map keyed by the store, so a shard that ran no
// hash command snapshots nothing. It reaps fired fields before folding a hash, so a
// snapshot never durably resurrects a lazily-expired field, and skips a hash the key
// deadline or the reap has emptied. It is a no-op on a store with no record log and on
// a shard that has built no hash registry.
func Snapshot(cx *shard.Ctx) {
	if cx.St == nil {
		return
	}
	v, ok := regs.Load(cx.St)
	if !ok {
		return
	}
	g := v.(*reg)
	now := uint64(cx.NowMs)
	for k, h := range g.m {
		// Skip a hash the key deadline already fired: a snapshot of it would durably
		// resurrect a key EXISTS reports absent, so let the next access drop it. The
		// skip is read-only, matching the scan walks.
		if h.expireAt != 0 && h.expireAt <= int64(now) {
			continue
		}
		// Reap fired fields so the fold captures the live set, not a field whose TTL
		// has passed. Gated by the next-expire hint, so a hash with no field TTL pays
		// one comparison.
		h.reap(now)
		if h.card() == 0 {
			continue
		}
		header, run := buildHashSnapshot(h)
		cx.St.LogCollectionSnap([]byte(k), akifile.CollKindHash, header, run)
	}
}

// buildHashSnapshot renders a live hash to a snapshot payload: the header carries the
// key TTL and the element run packs every field-value pair with the same length-
// prefixed encoder the cold chunk uses (appendEntry), so recovery decodes the run with
// the reader the cold path already trusts. It reads the whole hash through each, which
// spans both bands and preads any demoted value back, so a snapshot of a partly-cold
// hash is complete. It allocates fresh slices, a checkpoint-time cost off the steady
// mutation path.
func buildHashSnapshot(h *hash) (header, elementRun []byte) {
	header = make([]byte, snapHeaderLen)
	binary.LittleEndian.PutUint64(header, uint64(h.expireAt))
	// Field-TTL section: the count of fields carrying a TTL followed by each such field
	// and its deadline. Only fields with a live TTL are listed, so a hash with none (the
	// common case) writes a single zero-count byte. It is built alongside the element run
	// in one walk, reading each field's deadline through fieldExp, which spans both bands.
	var ttls []byte
	var n uint64
	h.each(func(field, value []byte) {
		elementRun = appendEntry(elementRun, field, value)
		if exp := h.fieldExp(field); exp != 0 {
			ttls = appendFieldTTL(ttls, field, exp)
			n++
		}
	})
	header = binary.AppendUvarint(header, n)
	header = append(header, ttls...)
	return header, elementRun
}

// appendFieldTTL appends one field-deadline pair to a snapshot's field-TTL section: the
// length-prefixed field name and its absolute unix-ms deadline, eight bytes little-endian.
func appendFieldTTL(dst, field []byte, at uint64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(field)))
	dst = append(dst, field...)
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], at)
	return append(dst, b[:]...)
}

// restoreFieldTTLs decodes the field-TTL section that follows the fixed header prefix and
// installs each deadline on the field the element run already rebuilt. A pre-field-TTL
// snapshot has no section (header is exactly the prefix), which decodes as no field TTLs.
// It reports false on a torn section, the fail-closed cut recovery wants. A deadline for a
// field the run did not restore is a defensive no-op (setFieldExp reports absent).
func restoreFieldTTLs(h *hash, section []byte) bool {
	if len(section) == 0 {
		return true
	}
	n, w := binary.Uvarint(section)
	if w <= 0 {
		return false
	}
	p := section[w:]
	for i := uint64(0); i < n; i++ {
		fl, w := binary.Uvarint(p)
		if w <= 0 || uint64(len(p)-w) < fl {
			return false
		}
		p = p[w:]
		field := p[:fl]
		p = p[fl:]
		if len(p) < 8 {
			return false
		}
		at := binary.LittleEndian.Uint64(p)
		p = p[8:]
		h.setFieldExp(field, at)
	}
	return true
}

// Recover rebuilds this shard's hashes from the record log's hash frames, re-driving
// each frame in append order onto a fresh registry. It is the hash arm of an .aki
// reopen, the sibling of set.Recover: after the store's string index recovery, the
// runtime calls Recover so a restart restores the hashes a crash would otherwise lose.
// A snapshot frame resets its key to the snapshotted fields and key TTL, and every
// effect frame after it applies on top, so a hash rebuilds from its last snapshot plus
// its effect tail, the bounded path the string index recovery takes. It applies
// through the low-level band-selecting mutators (hash.set, hash.del), not the logging
// command wrappers, so the rebuild re-logs nothing and the band a field lands in
// matches the live run's. A hash that reaches zero cardinality is dropped, matching
// the live last-field-leaves rule, and a key-delete effect drops the whole hash. It is
// a no-op on a store with no record log.
func Recover(cx *shard.Ctx) error {
	if cx.St == nil {
		return nil
	}
	g := registry(cx)
	return cx.St.WalkCollection(akifile.CollKindHash,
		func(key []byte, snap akifile.CollSnapRow) error {
			return applyHashSnapshot(cx, g, key, snap)
		},
		func(key []byte, op akifile.CollOpRow) error {
			return applyHashOp(cx, g, key, op)
		})
}

// applyHashSnapshot resets key to the snapshot's fields and key TTL, superseding every
// effect frame for key that preceded it. It drops any state the earlier tail built,
// rebuilds the hash from the field-value run through the same band-selecting set funnel
// a live run and an effect replay use, and restores the key TTL from the header. An
// empty element run leaves the key dropped, since the registry keeps no empty hash. A
// torn run reports ErrLength, the fail-closed cut a recovering reader wants.
func applyHashSnapshot(cx *shard.Ctx, g *reg, key []byte, snap akifile.CollSnapRow) error {
	g.drop(key)
	var h *hash
	if !eachPair(snap.ElementRun, func(field, value []byte) {
		if h == nil {
			h = newHash()
			g.install(cx, key, h)
		}
		h.set(field, value)
	}) {
		return akifile.ErrLength
	}
	if h == nil {
		return nil
	}
	if len(snap.Header) >= snapHeaderLen {
		h.expireAt = int64(binary.LittleEndian.Uint64(snap.Header))
		if !restoreFieldTTLs(h, snap.Header[snapHeaderLen:]) {
			return akifile.ErrLength
		}
	}
	g.note(h)
	return nil
}

// eachPair walks a length-prefixed field-value run forward, calling fn for each pair,
// the O(n) reader the snapshot rebuild uses over the same wire form appendEntry writes
// (chunkEntry is the positional O(idx) sibling the cold read path uses). It reports
// false on a torn run, which recovery treats as a corrupt frame. The slices fn
// receives alias the run and are valid only for the call.
func eachPair(run []byte, fn func(field, value []byte)) bool {
	p := run
	for len(p) > 0 {
		fl, w := binary.Uvarint(p)
		if w <= 0 || uint64(len(p)-w) < fl {
			return false
		}
		p = p[w:]
		field := p[:fl]
		p = p[fl:]
		vl, w := binary.Uvarint(p)
		if w <= 0 || uint64(len(p)-w) < vl {
			return false
		}
		p = p[w:]
		value := p[:vl]
		p = p[vl:]
		fn(field, value)
	}
	return true
}

// applyHashOp re-drives one hash effect onto the registry: a set creates the hash on
// its first field and writes the pair, a field-delete drops the field and the key on
// the last one, a key-delete clears the whole hash, a key-expire sets the whole-key
// deadline, and a field-expire sets or clears one field's TTL. It is the effect arm the
// recovery walk drives. It goes through hash.set and hash.del, so a field that breaches
// an inline threshold promotes to the native band exactly as it did live. It reports
// ErrLength on a torn expire or field-expire payload, the fail-closed cut recovery wants;
// an expire on an absent key or field is a defensive no-op.
func applyHashOp(cx *shard.Ctx, g *reg, key []byte, op akifile.CollOpRow) error {
	switch op.Op {
	case hashOpSet:
		h := g.m[string(key)]
		if h == nil {
			h = newHash()
			g.install(cx, key, h)
		}
		h.set(op.SubKey, op.SubValue)
		g.note(h)
	case hashOpDelField:
		h := g.m[string(key)]
		if h == nil {
			return nil
		}
		h.del(op.SubKey)
		if h.card() == 0 {
			g.drop(key)
		} else {
			g.note(h)
		}
	case hashOpDeleteKey:
		g.drop(key)
	case hashOpExpire:
		if len(op.SubValue) < 8 {
			return akifile.ErrLength
		}
		if h := g.m[string(key)]; h != nil {
			h.expireAt = int64(binary.LittleEndian.Uint64(op.SubValue))
		}
	case hashOpFieldExpire:
		if len(op.SubValue) < 8 {
			return akifile.ErrLength
		}
		h := g.m[string(key)]
		if h == nil {
			return nil
		}
		at := binary.LittleEndian.Uint64(op.SubValue)
		if at == 0 {
			h.clearFieldExp(op.SubKey)
		} else {
			h.setFieldExp(op.SubKey, at)
		}
		g.note(h)
	}
	return nil
}
