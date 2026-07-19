package list

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
)

// The list durability seam (spec 2064/f3/M8-collection-durability-plan, the list arm
// of slices 2 and 3): the list half of the collection effect log plus its snapshot. A
// list lives only in the shard's owner-local registry (reg.go), so a crash loses it in
// full; this file makes each list mutation survivable by cutting a small effect frame
// through the store seam (store/collectionseam.go) and rebuilds the registry from those
// frames on reopen, the same shape the set, hash, and zset verticals took.
//
// A list is position-ordered with no per-element key, so its vocabulary is the ordered
// AOF a replay re-drives in append order rather than the keyed last-write-wins fold the
// set and hash use. A push names the end and the value; a pop names the end; LSET names
// the resolved index and the new value; LTRIM names the resolved keep window; LREM
// names the signed count and the element; LINSERT names the side, the pivot, and the
// value; a key-delete clears the whole list. Every op is deterministic given the list
// state it applies to, and Recover re-drives the effects in the exact order they were
// cut, so the same op sequence reconstructs the exact list, the same guarantee Redis's
// AOF gives. Each helper is a no-op on a store with no .aki handle, so the pure
// in-memory path is byte-unchanged.
//
// The snapshot half (slice 3) folds each live list to one whole-list snapshot frame at
// the checkpoint (Snapshot): the elements packed in order with the same length-prefixed
// frame encoder the cold chunk uses (appendFrame), under a small header carrying the key
// TTL. A reopen rebuilds each list from its last snapshot and replays only the effect
// tail cut after it, so the tail a recovery must re-drive stays bounded to one
// checkpoint interval, the same checkpoint-plus-tail path the string index recovery and
// the other collection verticals take. The cadence is snapshot-at-checkpoint-and-
// shutdown only, the string model one level down.
//
// Deferred as one coherent follow-on, the list analog of the set vertical's deferred
// SMOVE and STORE forms: the blocking-serve pops (serveWaiters, serveKey, serveMove in
// waiter.go) and the move families (LMOVE and RPOPLPUSH in lmove.go, LMPOP in lmpop.go,
// the BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, and BLMPOP forms in blocking.go and blockmove.go,
// and every cross-shard hop in lmovecross.go, blockcross.go, and blockmovecross.go). This
// slice logs the single-key command surface (the pushes, pops, LSET, LTRIM, LREM,
// LINSERT, and DEL) and defers the move-and-block arc, which reaches through the
// cross-shard intent path and is the box-risky coupled slice the move-durability
// follow-on threads whole. In that window a crash after a move, or after a push that
// immediately served a blocked waiter, can leave the effect log reflecting one side of
// the move and not the other, the same bounded gap the deferred set STORE forms carry.
//
// Key TTL is durable: the snapshot header carries the key deadline as of the checkpoint,
// and a between-checkpoint EXPIRE or PERSIST cuts its own listOpExpire effect, so a crash
// after the last snapshot keeps the deadline the live run set. The lazy reap needs no
// effect of its own: it drops a key whose deadline has passed, and a replay reconstructs
// the same passed deadline, so the rebuilt key falls out on its first touch exactly as it
// did live.

const (
	// listOpPushFront records that a value was prepended to the list at key, the
	// effect an LPUSH or LPUSHX cuts. SubValue is the value.
	listOpPushFront uint8 = 1
	// listOpPushBack records that a value was appended to the list at key, the effect
	// an RPUSH or RPUSHX cuts. SubValue is the value.
	listOpPushBack uint8 = 2
	// listOpPopFront records that the head element left the list at key, the effect an
	// LPOP cuts (one per element popped). It carries no value: a replay re-drives the
	// pop against the deterministic list state the prior effects rebuilt.
	listOpPopFront uint8 = 3
	// listOpPopBack records that the tail element left the list at key, the effect an
	// RPOP cuts (one per element popped).
	listOpPopBack uint8 = 4
	// listOpSet records an LSET: the element at a resolved index now holds a value.
	// SubKey is the index as an unsigned varint, SubValue the new value.
	listOpSet uint8 = 5
	// listOpTrim records an LTRIM: keep only the elements in a resolved inclusive
	// window. SubKey packs the low then the high bound, each an unsigned varint.
	listOpTrim uint8 = 6
	// listOpRem records an LREM: remove matches of an element under the count-sign
	// rule. SubKey is the signed count as a zigzag varint, SubValue the element.
	listOpRem uint8 = 7
	// listOpInsert records an LINSERT: place a value before or after the first pivot
	// match. SubKey is a side byte (1 before, 0 after) then the pivot bytes, SubValue
	// the inserted value.
	listOpInsert uint8 = 8
	// listOpDeleteKey records that the whole list at key was dropped, the effect a DEL
	// cuts so a replay clears the key instead of resurrecting its elements.
	listOpDeleteKey uint8 = 9
	// listOpExpire records the list's key deadline after an EXPIRE-family command or a
	// PERSIST: SubValue is the deadline in unix milliseconds, eight bytes little-endian,
	// 0 when the key was persisted. It carries no SubKey, since the frame key names the
	// list. A replay installs the deadline the live run set, so a volatile list a crash
	// caught between checkpoints keeps its TTL. An EXPIRE to a past instant deletes the
	// key on the spot and cuts a key-delete instead, so this op always carries a future
	// or cleared deadline.
	listOpExpire uint8 = 10
)

// logPush cuts a push effect for value entering the list at key, at the front for an
// LPUSH/LPUSHX and the back for an RPUSH/RPUSHX. It is called once per pushed element,
// after the element is in the list, so a replay reproduces the exact element order.
func logPush(cx *shard.Ctx, key, value []byte, front bool) {
	if cx.St == nil {
		return
	}
	op := listOpPushBack
	if front {
		op = listOpPushFront
	}
	cx.St.LogCollectionOp(key, akifile.CollKindList, op, nil, value)
}

// logPop cuts a pop effect for the head (front) or tail element leaving the list at key.
// It carries no value; the replay re-drives the same-ended pop against the list its
// prior effects rebuilt.
func logPop(cx *shard.Ctx, key []byte, front bool) {
	if cx.St == nil {
		return
	}
	op := listOpPopBack
	if front {
		op = listOpPopFront
	}
	cx.St.LogCollectionOp(key, akifile.CollKindList, op, nil, nil)
}

// logSet cuts an LSET effect: the element at the resolved index now holds value. The
// index is the one the handler already folded into [0, length), packed as an unsigned
// varint, so a replay writes the same position the live run did.
func logSet(cx *shard.Ctx, key []byte, index int, value []byte) {
	if cx.St == nil {
		return
	}
	var sk [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(sk[:], uint64(index))
	cx.St.LogCollectionOp(key, akifile.CollKindList, listOpSet, sk[:n], value)
}

// logTrim cuts an LTRIM effect: keep only the resolved inclusive window [lo, hi]. Both
// bounds are the values the handler passed to list.trim, packed as unsigned varints
// (the empty-range clear passes lo>hi, which trim reads as "keep nothing", the same on
// replay). SubValue is unused.
func logTrim(cx *shard.Ctx, key []byte, lo, hi int) {
	if cx.St == nil {
		return
	}
	var sk [2 * binary.MaxVarintLen64]byte
	n := binary.PutUvarint(sk[:], uint64(lo))
	n += binary.PutUvarint(sk[n:], uint64(hi))
	cx.St.LogCollectionOp(key, akifile.CollKindList, listOpTrim, sk[:n], nil)
}

// logRem cuts an LREM effect: remove matches of element under the signed count-sign
// rule. The count is packed as a zigzag varint so a tail-to-head negative count survives,
// and element rides SubValue. It is cut only when the live run actually removed a match,
// so the tail carries no no-op removals.
func logRem(cx *shard.Ctx, key []byte, count int, element []byte) {
	if cx.St == nil {
		return
	}
	var sk [binary.MaxVarintLen64]byte
	n := binary.PutVarint(sk[:], int64(count))
	cx.St.LogCollectionOp(key, akifile.CollKindList, listOpRem, sk[:n], element)
}

// logInsert cuts an LINSERT effect: place value before or after the first pivot match.
// SubKey is a side byte (1 before, 0 after) then the pivot bytes; SubValue is the value.
// It is cut only on a successful insert (pivot found), never on the pivot-absent no-op.
func logInsert(cx *shard.Ctx, key, pivot, value []byte, before bool) {
	if cx.St == nil {
		return
	}
	sk := make([]byte, 1+len(pivot))
	if before {
		sk[0] = 1
	}
	copy(sk[1:], pivot)
	cx.St.LogCollectionOp(key, akifile.CollKindList, listOpInsert, sk, value)
}

// logDeleteKey cuts a key-delete effect for the list at key, the effect a DEL over a
// list records so a replay does not rebuild the elements a later effect no longer
// supersedes.
func logDeleteKey(cx *shard.Ctx, key []byte) {
	if cx.St == nil {
		return
	}
	cx.St.LogCollectionOp(key, akifile.CollKindList, listOpDeleteKey, nil, nil)
}

// logExpire cuts a key-deadline effect for the list at key: the new deadline in unix
// milliseconds, or 0 for a PERSIST that cleared it. It is called after the deadline is
// stored, so a replay reaches the same deadline the live run set. The EXPIRE-to-a-past-
// instant case deletes the key and logs a key-delete instead, never this.
func logExpire(cx *shard.Ctx, key []byte, at int64) {
	if cx.St == nil {
		return
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(at))
	cx.St.LogCollectionOp(key, akifile.CollKindList, listOpExpire, nil, b[:])
}

// snapHeaderLen is the fixed list snapshot header: the key deadline in unix milliseconds
// (0 when the list carries no key TTL), little-endian. The header is the per-key state
// the ordered element run does not carry, so a snapshot restores a volatile list's key
// TTL as of the checkpoint; a between-checkpoint EXPIRE or PERSIST also cuts its own
// listOpExpire effect so the deadline is durable between checkpoints.
const snapHeaderLen = 8

// Snapshot writes a whole-list snapshot frame for every live list on this shard, the
// list arm of the checkpoint dumper (and the clean-shutdown flush). A reopen rebuilds
// each list from its snapshot then replays only the effect tail cut after it, so the
// tail a recovery must re-drive stays bounded to one checkpoint interval. It reaches the
// registry through the shared regs map keyed by the store, so a shard that ran no list
// command snapshots nothing. It skips a list the key deadline has already fired, so a
// snapshot never durably resurrects a key EXISTS reports absent. It is a no-op on a store
// with no record log and on a shard that has built no list registry.
func Snapshot(cx *shard.Ctx) {
	if cx.St == nil {
		return
	}
	v, ok := regs.Load(cx.St)
	if !ok {
		return
	}
	g := v.(*reg)
	now := cx.NowMs
	for k, l := range g.m {
		// Skip a list whose key deadline already fired: a snapshot of it would durably
		// resurrect a key EXISTS reports absent, so let the next access drop it. The skip
		// is read-only, matching the scan walks.
		if l.expireAt != 0 && l.expireAt <= now {
			continue
		}
		header, run := buildListSnapshot(l)
		cx.St.LogCollectionSnap([]byte(k), akifile.CollKindList, header, run)
	}
}

// buildListSnapshot renders a live list to a snapshot payload: the header carries the
// key TTL and the element run packs every element in order with the same length-prefixed
// frame encoder the cold chunk uses (appendFrame), so recovery decodes the run with the
// forward walk the cold path already trusts. It reads the whole list through each, which
// spans both bands and preads any demoted chunk back, so a snapshot of a partly-cold list
// is complete and in order. It allocates fresh slices, a checkpoint-time cost off the
// steady mutation path.
func buildListSnapshot(l *list) (header, elementRun []byte) {
	header = make([]byte, snapHeaderLen)
	binary.LittleEndian.PutUint64(header, uint64(l.expireAt))
	l.each(func(v []byte) {
		elementRun = appendFrame(elementRun, v)
	})
	return header, elementRun
}

// Recover rebuilds this shard's lists from the record log's list frames, re-driving each
// frame in append order onto a fresh registry. It is the list arm of an .aki reopen, the
// sibling of set.Recover: after the store's string index recovery, the runtime calls
// Recover so a restart restores the lists a crash would otherwise lose. A snapshot frame
// resets its key to the snapshotted elements and key TTL, and every effect frame after it
// applies on top, so a list rebuilds from its last snapshot plus its ordered effect tail.
// It applies effects through the low-level band-selecting ops (list.pushFront, pushBack,
// setAt, trim, remove, insert), not the logging command wrappers, so the rebuild re-logs
// nothing and the band an element lands in matches the live run's. A list that reaches
// zero length is dropped, matching the live last-element-leaves rule, and a key-delete
// effect drops the whole list. It is a no-op on a store with no record log.
func Recover(cx *shard.Ctx) error {
	if cx.St == nil {
		return nil
	}
	g := registry(cx)
	return cx.St.WalkCollection(akifile.CollKindList,
		func(key []byte, snap akifile.CollSnapRow) error {
			return applyListSnapshot(g, cx, key, snap)
		},
		func(key []byte, op akifile.CollOpRow) error {
			return applyListOp(g, cx, key, op)
		})
}

// applyListSnapshot resets key to the snapshot's elements and key TTL, superseding every
// effect frame for key that preceded it. It drops any state the earlier tail built,
// rebuilds the list by pushing each element to the back in run order (so the order the
// snapshot captured is the order restored) through the same band-selecting push a live run
// and an effect replay use, and restores the key TTL from the header. An empty element run
// leaves the key dropped, since the registry keeps no empty list. A torn run reports
// ErrLength, the fail-closed cut a recovering reader wants.
func applyListSnapshot(g *reg, cx *shard.Ctx, key []byte, snap akifile.CollSnapRow) error {
	g.drop(key)
	var l *list
	if !eachFrame(snap.ElementRun, func(v []byte) {
		if l == nil {
			l = newList()
			g.install(cx, key, l)
		}
		l.pushBack(v)
	}) {
		return akifile.ErrLength
	}
	if l == nil {
		return nil
	}
	if len(snap.Header) >= snapHeaderLen {
		l.expireAt = int64(binary.LittleEndian.Uint64(snap.Header))
	}
	g.note(l)
	return nil
}

// applyListOp re-drives one list effect onto the registry, in the append order the walk
// hands them: a push creates the list on its first element and appends to the named end,
// a pop drops the named end and the key on the last element, LSET overwrites a position,
// LTRIM keeps a window, LREM removes matches, LINSERT places a value at a pivot, a
// key-delete clears the whole list, and a key-expire sets the deadline. It goes through
// the low-level band-selecting ops, so an element that breaches the inline budget promotes
// to the native band exactly as it did live. It reports ErrLength on a torn op payload (a
// short LSET or LTRIM header, or a short expire deadline), the fail-closed cut recovery
// wants; a structurally valid op that no longer applies (a pop or LSET on an absent or
// too-short list) is a defensive no-op, since a deterministic replay never produces one.
func applyListOp(g *reg, cx *shard.Ctx, key []byte, op akifile.CollOpRow) error {
	switch op.Op {
	case listOpPushFront:
		l := getOrCreate(g, cx, key)
		l.pushFront(op.SubValue)
		g.note(l)
	case listOpPushBack:
		l := getOrCreate(g, cx, key)
		l.pushBack(op.SubValue)
		g.note(l)
	case listOpPopFront:
		popApply(g, key, true)
	case listOpPopBack:
		popApply(g, key, false)
	case listOpSet:
		idx, w := binary.Uvarint(op.SubKey)
		if w <= 0 {
			return akifile.ErrLength
		}
		l := g.m[string(key)]
		if l == nil {
			return nil
		}
		i := int(idx)
		if i < 0 || i >= l.length() {
			return nil
		}
		l.setAt(i, op.SubValue)
		g.note(l)
	case listOpTrim:
		lo, w1 := binary.Uvarint(op.SubKey)
		if w1 <= 0 {
			return akifile.ErrLength
		}
		hi, w2 := binary.Uvarint(op.SubKey[w1:])
		if w2 <= 0 {
			return akifile.ErrLength
		}
		l := g.m[string(key)]
		if l == nil {
			return nil
		}
		l.trim(int(lo), int(hi))
		if l.length() == 0 {
			g.drop(key)
		} else {
			g.note(l)
		}
	case listOpRem:
		count, w := binary.Varint(op.SubKey)
		if w <= 0 {
			return akifile.ErrLength
		}
		l := g.m[string(key)]
		if l == nil {
			return nil
		}
		l.remove(int(count), op.SubValue)
		if l.length() == 0 {
			g.drop(key)
		} else {
			g.note(l)
		}
	case listOpInsert:
		if len(op.SubKey) < 1 {
			return akifile.ErrLength
		}
		before := op.SubKey[0] == 1
		pivot := op.SubKey[1:]
		l := g.m[string(key)]
		if l == nil {
			return nil
		}
		l.insert(before, pivot, op.SubValue)
		g.note(l)
	case listOpDeleteKey:
		g.drop(key)
	case listOpExpire:
		if len(op.SubValue) < 8 {
			return akifile.ErrLength
		}
		if l := g.m[string(key)]; l != nil {
			l.expireAt = int64(binary.LittleEndian.Uint64(op.SubValue))
		}
	}
	return nil
}

// getOrCreate returns the list at key, building an empty one and registering it on first
// touch, the create-on-first-push shape a live push and an effect replay share.
func getOrCreate(g *reg, cx *shard.Ctx, key []byte) *list {
	l := g.m[string(key)]
	if l == nil {
		l = newList()
		g.install(cx, key, l)
	}
	return l
}

// popApply re-drives one pop effect: it drops the named end of the list at key and the key
// itself once the last element leaves. An absent or empty list is a defensive no-op, which
// a deterministic replay never reaches.
func popApply(g *reg, key []byte, front bool) {
	l := g.m[string(key)]
	if l == nil || l.length() == 0 {
		return
	}
	popOne(l, front)
	if l.length() == 0 {
		g.drop(key)
	} else {
		g.note(l)
	}
}

// eachFrame walks a length-prefixed element run forward, calling fn for each element, the
// O(n) reader the snapshot rebuild uses over the same wire form appendFrame writes. It
// reports false on a torn run, which recovery treats as a corrupt frame. The []byte fn
// receives aliases the run and is valid only for the call.
func eachFrame(run []byte, fn func(v []byte)) bool {
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
