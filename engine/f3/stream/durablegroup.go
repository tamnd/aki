package stream

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
)

// The stream group and PEL durability seam (spec 2064/f3/M8-collection-durability-plan,
// the deferred follow-on durable.go names): the consumer-group ledger that hangs off a
// native stream (group.go, pel.go) now survives a crash the same way the entries do, a
// snapshot section plus an effect tail. durable.go carries the entry-level surface (XADD,
// XDEL, XTRIM, XSETID, DEL) and left a stream recovered with its entries but no groups;
// this file closes that gap, so a group-bearing stream comes back with its cursors, its
// consumers, and its pending-entries list intact.
//
// The vocabulary logs resolved state, never the verb, the shape the entry arm already
// takes. A group-set names the group's post-command cursor and lag basis (XGROUP CREATE and
// SETID, and the `>` delivery that advances the cursor). A consumer-set names a consumer's
// ordinal and clocks (its creation, and the fetch or claim that stamps its active clock);
// the ordinal is load-bearing, since a pending slab stores its owner by ordinal, so a
// replay pins each consumer to the exact ordinal the live run assigned and a DELCONSUMER
// hole stays a hole. A pel-set names one pending entry's whole slab (its id, delivery clock,
// retry count, and owning ordinal), cut once per entry a delivery, claim, autoclaim, or nack
// wrote, so the tail carries one small frame per pending mutation and never the whole list.
// A pel-del names one retired entry (XACK, and the lazy drop a claim or autoclaim makes when
// a pending entry outlived its log entry). A group-destroy and a consumer-del are the two
// wholesale teardowns, each replayed through the same low-level method the command runs
// (delete on the group table, delConsumer), so the drain a DELCONSUMER does over the rebuilt
// PEL reproduces exactly and needs no per-entry delete of its own.
//
// Recovery re-drives each effect through the low-level group and PEL methods (addGroup,
// installConsumer, the PEL insert and ack), not the logging command wrappers, so the rebuild
// re-logs nothing. XDEL, XTRIM, and XSETID never touch a PEL (the lazy reconciliation
// group.go documents), so they cut no group effect and a dangling pending id a replay
// reconstructs is legal, reconciled at the next claim or read exactly as it was live.
//
// The snapshot half folds each group into the stream's snapshot header after the fixed
// counters: one section per group carrying its cursor, its consumers in ordinal order (nil
// holes kept), and its pending slabs in id order. A reopen rebuilds the groups from the last
// snapshot then replays only the effect tail cut after it, the same checkpoint-plus-tail
// bound the entry run takes. The consumer and group pelCount summaries are recomputed as the
// slabs are re-inserted, never stored, since the slab set is the truth.

const (
	// streamOpGroupSet records a group's post-command cursor and lag basis, the effect
	// XGROUP CREATE and SETID and a `>` delivery cut. SubKey is the group name; SubValue is
	// the last-delivered ID (16 bytes big-endian), the entries-read count (8 bytes little-
	// endian), then the read-valid flag (1 byte). A replay creates the group on first sight
	// (building the stream under it if a MKSTREAM stream had no entries) or grafts the
	// cursor onto an existing one.
	streamOpGroupSet uint8 = 7
	// streamOpGroupDestroy records an XGROUP DESTROY: the whole group and its ledger were
	// dropped. SubKey is the group name, SubValue empty. A replay deletes the group, which
	// drops its consumers and PEL with it.
	streamOpGroupDestroy uint8 = 8
	// streamOpConsumerSet records a consumer's ordinal and clocks, cut on its creation and
	// when a fetch or claim advances its active clock. SubKey is the group name; SubValue is
	// the ordinal (4 bytes little-endian), the seen clock (8 bytes little-endian), the
	// active clock (8 bytes little-endian), then the consumer name (the trailing bytes). The
	// ordinal is pinned so a pending slab's owner ordinal resolves to the same consumer on
	// replay.
	streamOpConsumerSet uint8 = 9
	// streamOpConsumerDel records an XGROUP DELCONSUMER: the named consumer was removed and
	// its pending entries drained. SubKey is the group name, SubValue the consumer name. A
	// replay runs delConsumer, which drains the same pending entries from the rebuilt PEL, so
	// the drain needs no per-entry delete effect.
	streamOpConsumerDel uint8 = 10
	// streamOpPelSet records one pending entry's whole slab, cut once per entry a `>`
	// delivery, an XCLAIM, an XAUTOCLAIM, or an XNACK wrote. SubKey is the group name;
	// SubValue is the entry ID (16 bytes big-endian), the delivery clock (8 bytes little-
	// endian), the delivery count (2 bytes little-endian), then the owning consumer ordinal
	// (4 bytes little-endian, the noOwner sentinel for an unowned NACK slab). A replay
	// inserts the slab or rewrites it in place, adjusting the owning consumer's count.
	streamOpPelSet uint8 = 11
	// streamOpPelDel records one retired pending entry, cut by XACK and by the lazy drop a
	// claim or autoclaim makes when a pending entry outlived its log entry. SubKey is the
	// group name, SubValue the entry ID (16 bytes big-endian). A replay acks the entry,
	// dropping the group and owning-consumer counts.
	streamOpPelDel uint8 = 12
)

// logGroupSet cuts a group-set effect for the group's post-command cursor and lag basis. It
// is called after XGROUP CREATE or SETID sets the fields, and after a `>` delivery advances
// the cursor, so a replay reaches the same cursor the live run left.
func logGroupSet(cx *shard.Ctx, key, name []byte, grp *streamGroup) {
	if cx.St == nil {
		return
	}
	sv := make([]byte, 16+8+1)
	putID16(sv[0:16], grp.lastDeliveredID)
	binary.LittleEndian.PutUint64(sv[16:24], grp.entriesRead)
	if grp.readValid {
		sv[24] = 1
	}
	cx.St.LogCollectionOp(key, akifile.CollKindStream, streamOpGroupSet, name, sv)
}

// logGroupDestroy cuts a group-destroy effect so a replay drops the group instead of
// rebuilding it from the effects that preceded the destroy.
func logGroupDestroy(cx *shard.Ctx, key, name []byte) {
	if cx.St == nil {
		return
	}
	cx.St.LogCollectionOp(key, akifile.CollKindStream, streamOpGroupDestroy, name, nil)
}

// logConsumerSet cuts a consumer-set effect carrying the consumer's ordinal and clocks, so a
// replay pins it to the same ordinal a pending slab names as its owner.
func logConsumerSet(cx *shard.Ctx, key, groupName []byte, con *streamConsumer) {
	if cx.St == nil {
		return
	}
	sv := make([]byte, 4+8+8+len(con.name))
	binary.LittleEndian.PutUint32(sv[0:4], con.ord)
	binary.LittleEndian.PutUint64(sv[4:12], uint64(con.seenTime))
	binary.LittleEndian.PutUint64(sv[12:20], uint64(con.activeTime))
	copy(sv[20:], con.name)
	cx.St.LogCollectionOp(key, akifile.CollKindStream, streamOpConsumerSet, groupName, sv)
}

// logConsumerDel cuts a consumer-del effect so a replay removes the consumer and drains its
// pending entries through the same delConsumer the command ran.
func logConsumerDel(cx *shard.Ctx, key, groupName, conName []byte) {
	if cx.St == nil {
		return
	}
	cx.St.LogCollectionOp(key, akifile.CollKindStream, streamOpConsumerDel, groupName, conName)
}

// logPelSet cuts a pel-set effect for one pending entry's whole slab, read straight from the
// slab the command just wrote, so a replay reconstructs the exact pending state.
func logPelSet(cx *shard.Ctx, key, groupName []byte, pe *pelEntry) {
	if cx.St == nil {
		return
	}
	sv := make([]byte, 16+8+2+4)
	putID16(sv[0:16], pe.id)
	binary.LittleEndian.PutUint64(sv[16:24], uint64(pe.deliveryTime))
	binary.LittleEndian.PutUint16(sv[24:26], pe.deliveryCount)
	binary.LittleEndian.PutUint32(sv[26:30], pe.consumerOrd)
	cx.St.LogCollectionOp(key, akifile.CollKindStream, streamOpPelSet, groupName, sv)
}

// logPelDel cuts a pel-del effect for one retired pending entry so a replay acks it, the
// effect an XACK or a lazy claim-time drop records.
func logPelDel(cx *shard.Ctx, key, groupName []byte, id streamID) {
	if cx.St == nil {
		return
	}
	var sv [16]byte
	putID16(sv[:], id)
	cx.St.LogCollectionOp(key, akifile.CollKindStream, streamOpPelDel, groupName, sv[:])
}

// logGroupDelivery cuts the effects one `>` XREADGROUP delivery produced, shared by the
// inline read and the blocked-wake path so both durable-log identically. A newly created
// consumer or an active-clock bump cuts a consumer-set (before any pending slab that names
// its ordinal); each delivered entry that entered the PEL under a non-NOACK read cuts a
// pel-set; and a delivery that advanced the cursor cuts a group-set. A NOACK read records no
// pending entry and leaves the active clock, so it cuts only the group-set the cursor move
// needs, plus a consumer-set when the read created the consumer. It is a no-op on a store
// with no record log, so the pure in-memory delivery pays no lookup.
func logGroupDelivery(cx *shard.Ctx, key, groupName []byte, grp *streamGroup, con *streamConsumer, entries []deliveredEntry, noack, newCon bool) {
	if cx.St == nil {
		return
	}
	active := !noack && len(entries) > 0
	if newCon || active {
		logConsumerSet(cx, key, groupName, con)
	}
	if !noack {
		for i := range entries {
			if pe, ok := grp.pel.find(entries[i].id); ok {
				logPelSet(cx, key, groupName, pe)
			}
		}
	}
	if len(entries) > 0 {
		logGroupSet(cx, key, groupName, grp)
	}
}

// logClaimResults cuts the effects an XCLAIM or XAUTOCLAIM pass produced for the target
// consumer, shared by both. A newly created consumer or a pass that transferred at least one
// entry cuts a consumer-set (before any pending slab that names its ordinal), and each
// claimed entry's rewritten slab cuts a pel-set. The pending entries a claim dropped because
// their log entry was gone are cut as pel-dels by the caller, which sees them per command. It
// is a no-op on a store with no record log.
func logClaimResults(cx *shard.Ctx, key, groupName []byte, grp *streamGroup, con *streamConsumer, claimed []streamID, newCon bool) {
	if cx.St == nil {
		return
	}
	if newCon || len(claimed) > 0 {
		logConsumerSet(cx, key, groupName, con)
	}
	for _, id := range claimed {
		if pe, ok := grp.pel.find(id); ok {
			logPelSet(cx, key, groupName, pe)
		}
	}
}

// applyGroupOp re-drives one group or PEL effect onto the registry, dispatched from
// applyStreamOp for the group-arm op codes. It reports ErrLength on a torn payload, the
// fail-closed cut recovery wants; a structurally valid op that no longer applies (a consumer
// or pel op on an absent stream or group) is a defensive no-op, since a deterministic replay
// never produces one.
func applyGroupOp(cx *shard.Ctx, g *reg, key []byte, op akifile.CollOpRow) error {
	switch op.Op {
	case streamOpGroupSet:
		if len(op.SubValue) < 16+8+1 {
			return akifile.ErrLength
		}
		lastID := readID16(op.SubValue[0:16])
		entriesRead := binary.LittleEndian.Uint64(op.SubValue[16:24])
		valid := op.SubValue[24] != 0
		applyGroupSet(cx, g, key, op.SubKey, lastID, entriesRead, valid)
	case streamOpGroupDestroy:
		applyGroupDestroy(g, key, op.SubKey)
	case streamOpConsumerSet:
		if len(op.SubValue) < 4+8+8 {
			return akifile.ErrLength
		}
		ord := binary.LittleEndian.Uint32(op.SubValue[0:4])
		seen := int64(binary.LittleEndian.Uint64(op.SubValue[4:12]))
		active := int64(binary.LittleEndian.Uint64(op.SubValue[12:20]))
		name := op.SubValue[20:]
		applyConsumerSet(g, key, op.SubKey, ord, seen, active, name)
	case streamOpConsumerDel:
		applyConsumerDel(g, key, op.SubKey, op.SubValue)
	case streamOpPelSet:
		if len(op.SubValue) < 16+8+2+4 {
			return akifile.ErrLength
		}
		id := readID16(op.SubValue[0:16])
		dtime := int64(binary.LittleEndian.Uint64(op.SubValue[16:24]))
		dcount := binary.LittleEndian.Uint16(op.SubValue[24:26])
		cord := binary.LittleEndian.Uint32(op.SubValue[26:30])
		applyPelSet(g, key, op.SubKey, id, dtime, dcount, cord)
	case streamOpPelDel:
		if len(op.SubValue) < 16 {
			return akifile.ErrLength
		}
		applyPelDel(g, key, op.SubKey, readID16(op.SubValue[0:16]))
	}
	return nil
}

// applyGroupSet creates the named group on first sight, building the stream under it when a
// MKSTREAM stream carried no entries, or grafts the cursor and lag basis onto an existing
// group. It upgrades the stream to the native band the group table needs.
func applyGroupSet(cx *shard.Ctx, g *reg, key, name []byte, lastID streamID, entriesRead uint64, valid bool) {
	s := getOrCreateStream(cx, g, key)
	s.ensureNative()
	grp := s.group(name)
	if grp == nil {
		s.addGroup(name, newGroup(lastID, entriesRead, valid))
	} else {
		grp.lastDeliveredID = lastID
		grp.entriesRead = entriesRead
		grp.readValid = valid
	}
	g.note(s)
}

// applyGroupDestroy drops the named group and its ledger, the replay of an XGROUP DESTROY. A
// missing stream or group is a defensive no-op.
func applyGroupDestroy(g *reg, key, name []byte) {
	s := g.m[string(key)]
	if s == nil || s.group(name) == nil {
		return
	}
	delete(s.groups, string(name))
	g.note(s)
}

// applyConsumerSet installs the consumer at its pinned ordinal with its clocks, creating it
// on first sight or refreshing its clocks on a later active-clock bump. A missing stream or
// group is a defensive no-op.
func applyConsumerSet(g *reg, key, groupName []byte, ord uint32, seen, active int64, name []byte) {
	s := g.m[string(key)]
	if s == nil {
		return
	}
	grp := s.group(groupName)
	if grp == nil {
		return
	}
	installConsumer(grp, ord, name, seen, active)
	g.note(s)
}

// applyConsumerDel removes the named consumer and drains its pending entries from the rebuilt
// PEL, the replay of an XGROUP DELCONSUMER. A missing stream or group is a defensive no-op.
func applyConsumerDel(g *reg, key, groupName, name []byte) {
	s := g.m[string(key)]
	if s == nil {
		return
	}
	grp := s.group(groupName)
	if grp == nil {
		return
	}
	grp.delConsumer(name)
	g.note(s)
}

// applyPelSet inserts a pending slab or rewrites it in place, adjusting the owning consumer's
// count, the replay of a delivery, claim, autoclaim, or nack. It creates the group PEL on
// first pending entry. A missing stream or group is a defensive no-op.
func applyPelSet(g *reg, key, groupName []byte, id streamID, dtime int64, dcount uint16, cord uint32) {
	s := g.m[string(key)]
	if s == nil {
		return
	}
	grp := s.group(groupName)
	if grp == nil {
		return
	}
	if grp.pel == nil {
		grp.pel = newPEL()
	}
	pe, ok := grp.pel.find(id)
	if !ok {
		pe = grp.pel.insertClaimed(id)
		grp.pelCount++
	} else if pe.consumerOrd != noOwner {
		grp.decOwner(pe.consumerOrd)
	}
	pe.deliveryTime = dtime
	pe.deliveryCount = dcount
	pe.consumerOrd = cord
	if cord != noOwner {
		grp.incOwner(cord)
	}
	g.note(s)
}

// applyPelDel acks a pending entry, dropping the group and owning-consumer counts, the replay
// of an XACK or a lazy claim-time drop. A missing stream, group, or PEL is a defensive no-op.
func applyPelDel(g *reg, key, groupName []byte, id streamID) {
	s := g.m[string(key)]
	if s == nil {
		return
	}
	grp := s.group(groupName)
	if grp == nil || grp.pel == nil {
		return
	}
	if ord, ok := grp.pel.ack(id); ok {
		grp.pelCount--
		grp.decOwner(ord)
	}
	g.note(s)
}

// installConsumer records a consumer at ordinal ord, growing the ordinal index with nil holes
// as needed and pinning the ordinal so a pending slab's owner resolves to the same record. A
// present name refreshes its ordinal and clocks without touching its pending count, so a
// later active-clock update leaves the count the pending effects maintain. A fresh consumer
// starts owning nothing, its count grown as its pending slabs are re-inserted.
func installConsumer(grp *streamGroup, ord uint32, name []byte, seen, active int64) *streamConsumer {
	for uint32(len(grp.consumerByOrd)) <= ord {
		grp.consumerByOrd = append(grp.consumerByOrd, nil)
	}
	con := grp.consumers[string(name)]
	if con == nil {
		con = &streamConsumer{name: append([]byte(nil), name...)}
		grp.consumers[string(name)] = con
	}
	con.ord = ord
	con.seenTime = seen
	con.activeTime = active
	grp.consumerByOrd[ord] = con
	return con
}

// incOwner adds one to the consumer that owns ordinal ord, the mirror of decOwner, tolerating
// the nil hole a DELCONSUMER leaves. A replay uses it to rebuild the per-consumer pending
// count as pending slabs are re-inserted.
func (grp *streamGroup) incOwner(ord uint32) {
	if int(ord) < len(grp.consumerByOrd) {
		if con := grp.consumerByOrd[ord]; con != nil {
			con.pelCount++
		}
	}
}

// appendGroupSection appends the snapshot group section for stream s to dst, the trailing
// part of the snapshot header after the fixed counters: a count then one record per group,
// each carrying the cursor, the consumers in ordinal order (nil holes kept as an absent
// flag), and the pending slabs in id order. A stream with no groups appends just a zero
// count. It is a checkpoint-time render off the steady mutation path.
func appendGroupSection(dst []byte, s *stream) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s.groups)))
	for name, grp := range s.groups {
		dst = appendBytes(dst, []byte(name))
		var idb [16]byte
		putID16(idb[:], grp.lastDeliveredID)
		dst = append(dst, idb[:]...)
		var num [8]byte
		binary.LittleEndian.PutUint64(num[:], grp.entriesRead)
		dst = append(dst, num[:]...)
		if grp.readValid {
			dst = append(dst, 1)
		} else {
			dst = append(dst, 0)
		}
		dst = binary.AppendUvarint(dst, uint64(len(grp.consumerByOrd)))
		for _, con := range grp.consumerByOrd {
			if con == nil {
				dst = append(dst, 0)
				continue
			}
			dst = append(dst, 1)
			dst = appendBytes(dst, con.name)
			binary.LittleEndian.PutUint64(num[:], uint64(con.seenTime))
			dst = append(dst, num[:]...)
			binary.LittleEndian.PutUint64(num[:], uint64(con.activeTime))
			dst = append(dst, num[:]...)
		}
		var pelbuf []byte
		var pcount uint64
		if grp.pel != nil {
			grp.pel.walkFrom(streamID{}, func(pe *pelEntry) bool {
				var b [16]byte
				putID16(b[:], pe.id)
				pelbuf = append(pelbuf, b[:]...)
				var f [8]byte
				binary.LittleEndian.PutUint64(f[:], uint64(pe.deliveryTime))
				pelbuf = append(pelbuf, f[:]...)
				var c [2]byte
				binary.LittleEndian.PutUint16(c[:], pe.deliveryCount)
				pelbuf = append(pelbuf, c[:]...)
				var o [4]byte
				binary.LittleEndian.PutUint32(o[:], pe.consumerOrd)
				pelbuf = append(pelbuf, o[:]...)
				pcount++
				return true
			})
		}
		dst = binary.AppendUvarint(dst, pcount)
		dst = append(dst, pelbuf...)
	}
	return dst
}

// restoreGroupSection rebuilds stream s's groups from the snapshot group section appendGroupSection
// wrote, installing each group's cursor, its consumers at their pinned ordinals (nil holes
// kept), and its pending slabs, recomputing the group and per-consumer pending counts as the
// slabs are re-inserted. It reports false on a torn section, which recovery treats as a
// corrupt frame. An empty buffer is a stream snapshotted before the group section existed and
// restores no groups.
func restoreGroupSection(s *stream, buf []byte) bool {
	if len(buf) == 0 {
		return true
	}
	p := buf
	ng, w := binary.Uvarint(p)
	if w <= 0 {
		return false
	}
	p = p[w:]
	for i := uint64(0); i < ng; i++ {
		name, rest, ok := readLenBytes(p)
		if !ok {
			return false
		}
		p = rest
		if len(p) < 16+8+1 {
			return false
		}
		lastID := readID16(p[0:16])
		entriesRead := binary.LittleEndian.Uint64(p[16:24])
		valid := p[24] != 0
		p = p[25:]
		grp := newGroup(lastID, entriesRead, valid)
		s.addGroup(name, grp)
		nc, w2 := binary.Uvarint(p)
		if w2 <= 0 {
			return false
		}
		p = p[w2:]
		for slot := uint64(0); slot < nc; slot++ {
			if len(p) < 1 {
				return false
			}
			present := p[0]
			p = p[1:]
			if present == 0 {
				grp.consumerByOrd = append(grp.consumerByOrd, nil)
				continue
			}
			cname, rest2, ok := readLenBytes(p)
			if !ok {
				return false
			}
			p = rest2
			if len(p) < 16 {
				return false
			}
			seen := int64(binary.LittleEndian.Uint64(p[0:8]))
			active := int64(binary.LittleEndian.Uint64(p[8:16]))
			p = p[16:]
			installConsumer(grp, uint32(slot), cname, seen, active)
		}
		np, w3 := binary.Uvarint(p)
		if w3 <= 0 {
			return false
		}
		p = p[w3:]
		if np > 0 {
			grp.pel = newPEL()
		}
		for e := uint64(0); e < np; e++ {
			if len(p) < 16+8+2+4 {
				return false
			}
			id := readID16(p[0:16])
			dtime := int64(binary.LittleEndian.Uint64(p[16:24]))
			dcount := binary.LittleEndian.Uint16(p[24:26])
			cord := binary.LittleEndian.Uint32(p[26:30])
			p = p[30:]
			pe := grp.pel.insertClaimed(id)
			pe.deliveryTime = dtime
			pe.deliveryCount = dcount
			pe.consumerOrd = cord
			grp.pelCount++
			grp.incOwner(cord)
		}
	}
	return true
}
