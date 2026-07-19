package stream

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
)

// The stream durability seam (spec 2064/f3/M8-collection-durability-plan, the stream
// arm of slices 2 and 3): the stream half of the collection effect log plus its
// snapshot. A stream lives only in the shard's owner-local registry (reg.go), so a
// crash loses it in full; this file makes each stream mutation survivable by cutting a
// small effect frame through the store seam (store/collectionseam.go) and rebuilds the
// registry from those frames on reopen, the same shape the set, hash, zset, and list
// verticals took.
//
// A stream is an ID-ordered log with no positional key: every entry carries the 128-bit
// ID XADD assigned it, and IDs only climb. So unlike the list's positional AOF, the
// stream vocabulary is ID-addressed. An add names the assigned ID and the fields; a
// delete names the ID; a trim names the resolved keep boundary (the least surviving ID),
// so a replay reproduces the exact live set regardless of how the rebuilt stream happens
// to pack its blocks, which an approximate whole-block trim would otherwise make
// layout-dependent; an XSETID names the new counters; a key-delete clears the whole
// stream; and a key-expire effect names the deadline an EXPIRE or PERSIST set. Recover
// re-drives the effects in the order they were cut, over the low-level
// stream methods (appendEntry, delete, trim), so the same op sequence reconstructs the
// exact stream. Each helper is a no-op on a store with no .aki handle, so the pure
// in-memory path is byte-unchanged.
//
// The snapshot half (slice 3) folds each live stream to one whole-stream snapshot frame
// at the checkpoint (Snapshot): the live entries packed in ID order, each an ID plus its
// length-prefixed fields, under a header carrying the counters a live-entry walk cannot
// rebuild (lastID, maxDeletedID, entriesAdded) and the key TTL. lastID never moves
// backward even when XDEL tombstones the newest entry, so it is stored, not derived from
// the surviving entries; entriesAdded is the lifetime add count, above the live count a
// walk sees; length is left to appendEntry to remake as it re-appends the run. A reopen
// rebuilds each stream from its last snapshot and replays only the effect tail cut after
// it, so the tail a recovery must re-drive stays bounded to one checkpoint interval, the
// same checkpoint-plus-tail path the string index recovery and the other collection
// verticals take. The cadence is snapshot-at-checkpoint-and-shutdown only.
//
// The consumer-group and PEL surface (the group table on each stream and its pending-entries
// ledger, written by XGROUP, XREADGROUP, XACK, XCLAIM, XAUTOCLAIM, and XNACK) is the second
// stream slice, its own ledger vocabulary and a snapshot header group and PEL section, in
// durablegroup.go. This file logs the entry-level command surface (XADD, XDEL, XTRIM, XSETID,
// and DEL); the group arm dispatches from applyStreamOp's default case and rides the header
// section buildStreamSnapshot appends after the fixed counters, so a group-bearing stream
// comes back with its cursors, consumers, and pending list intact.
//
// Key TTL is durable: the snapshot header carries the key deadline as of the checkpoint,
// and a between-checkpoint EXPIRE or PERSIST cuts its own streamOpExpire effect, so a
// crash after the last snapshot keeps the deadline the live run set. The lazy reap needs
// no effect of its own: a replay reconstructs the same passed deadline, so the rebuilt key
// falls out on its first touch exactly as it did live.

const (
	// streamOpAdd records an XADD: an entry with a fixed ID entered the stream at key.
	// SubKey is the 16-byte big-endian ID (ms then seq), SubValue the length-prefixed
	// field run.
	streamOpAdd uint8 = 1
	// streamOpDel records an XDEL: the entry with a fixed ID was tombstoned. SubKey is
	// the 16-byte big-endian ID; it is cut once per ID an XDEL actually removed.
	streamOpDel uint8 = 2
	// streamOpTrimMinID records the outcome of an XTRIM or the XADD trim clause as an
	// exact MINID: drop every entry below a boundary ID. SubKey is the 16-byte
	// big-endian boundary, the least surviving ID after the live trim (or, when the trim
	// emptied the stream, the ID just past lastID). An exact-by-ID boundary reproduces
	// the same live set on replay whatever blocks the rebuilt stream packs, which an
	// approximate whole-block trim would not.
	streamOpTrimMinID uint8 = 3
	// streamOpSetID records an XSETID: the stream's counters were grafted. SubKey is the
	// 16-byte big-endian new lastID, SubValue the lifetime add count (8 bytes little-
	// endian) then the max-deleted ID (16 bytes big-endian), the post-command values.
	streamOpSetID uint8 = 4
	// streamOpDeleteKey records that the whole stream at key was dropped, the effect a
	// DEL cuts so a replay clears the key instead of resurrecting its entries.
	streamOpDeleteKey uint8 = 5
	// streamOpExpire records the stream's key deadline after an EXPIRE-family command or a
	// PERSIST: SubValue is the deadline in unix milliseconds, eight bytes little-endian,
	// 0 when the key was persisted. It carries no SubKey, since the frame key names the
	// stream. A replay installs the deadline the live run set, so a volatile stream a crash
	// caught between checkpoints keeps its TTL. An EXPIRE to a past instant deletes the key
	// on the spot and cuts a key-delete instead, so this op always carries a future or
	// cleared deadline.
	streamOpExpire uint8 = 6
)

// logAdd cuts an XADD effect for the entry the command just appended: the assigned ID and
// its fields. It is called after appendEntry, so a replay re-drives the append with the
// same ID and reproduces the exact entry.
func logAdd(cx *shard.Ctx, key []byte, id streamID, fields []field) {
	if cx.St == nil {
		return
	}
	var sk [16]byte
	putID16(sk[:], id)
	cx.St.LogCollectionOp(key, akifile.CollKindStream, streamOpAdd, sk[:], appendFields(nil, fields))
}

// logDel cuts an XDEL effect for the entry with ID id, cut once per ID the command
// actually tombstoned, so the effect tail carries no no-op deletes.
func logDel(cx *shard.Ctx, key []byte, id streamID) {
	if cx.St == nil {
		return
	}
	var sk [16]byte
	putID16(sk[:], id)
	cx.St.LogCollectionOp(key, akifile.CollKindStream, streamOpDel, sk[:], nil)
}

// logTrimBoundary cuts a trim effect as the exact boundary the live trim reached: the
// least surviving entry ID, or the ID just past lastID when the trim emptied the stream.
// A replay drops every entry below the boundary, reproducing the live set independent of
// the rebuilt stream's block packing. It is cut only when the live trim removed at least
// one entry.
func logTrimBoundary(cx *shard.Ctx, key []byte, s *stream) {
	if cx.St == nil {
		return
	}
	boundary, ok := firstLiveID(s)
	if !ok {
		// The trim emptied the stream: a boundary just past lastID drops every entry on
		// replay, so the rebuilt stream is empty and kept, matching the live one.
		boundary = nextID(s.lastID)
	}
	var sk [16]byte
	putID16(sk[:], boundary)
	cx.St.LogCollectionOp(key, akifile.CollKindStream, streamOpTrimMinID, sk[:], nil)
}

// logSetID cuts an XSETID effect carrying the stream's post-command counters: the new
// lastID, the lifetime add count, and the max-deleted ID. A replay grafts the same
// counters onto the rebuilt stream.
func logSetID(cx *shard.Ctx, key []byte, s *stream) {
	if cx.St == nil {
		return
	}
	var sk [16]byte
	putID16(sk[:], s.lastID)
	sv := make([]byte, 8+16)
	binary.LittleEndian.PutUint64(sv[:8], s.entriesAdded)
	putID16(sv[8:], s.maxDeletedID)
	cx.St.LogCollectionOp(key, akifile.CollKindStream, streamOpSetID, sk[:], sv)
}

// logDeleteKey cuts a key-delete effect for the stream at key, the effect a DEL over a
// stream records so a replay drops the key instead of rebuilding the entries a later
// effect no longer supersedes.
func logDeleteKey(cx *shard.Ctx, key []byte) {
	if cx.St == nil {
		return
	}
	cx.St.LogCollectionOp(key, akifile.CollKindStream, streamOpDeleteKey, nil, nil)
}

// logExpire cuts a key-deadline effect for the stream at key: the new deadline in unix
// milliseconds, or 0 for a PERSIST that cleared it. It is called after the deadline is
// stored, so a replay reaches the same deadline the live run set. The EXPIRE-to-a-past-
// instant case deletes the key and logs a key-delete instead, never this.
func logExpire(cx *shard.Ctx, key []byte, at int64) {
	if cx.St == nil {
		return
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(at))
	cx.St.LogCollectionOp(key, akifile.CollKindStream, streamOpExpire, nil, b[:])
}

// snapHeaderLen is the fixed stream snapshot header: lastID (16 bytes big-endian) then
// maxDeletedID (16 bytes big-endian) then the lifetime add count (8 bytes little-endian)
// then the key deadline in unix milliseconds (8 bytes little-endian, 0 when the stream
// carries no key TTL). These are the per-stream fields the live-entry run cannot rebuild:
// lastID and maxDeletedID outlive the entries they name, entriesAdded counts adds a trim
// or delete later dropped, and the deadline is key state. The live count is not stored,
// since re-appending the run remakes it.
const snapHeaderLen = 48

// Snapshot writes a whole-stream snapshot frame for every live stream on this shard, the
// stream arm of the checkpoint dumper (and the clean-shutdown flush). A reopen rebuilds
// each stream from its snapshot then replays only the effect tail cut after it, so the
// tail a recovery must re-drive stays bounded to one checkpoint interval. It reaches the
// registry through the shared regs map keyed by the store, so a shard that ran no stream
// command snapshots nothing. It skips a stream whose key deadline has already fired, so a
// snapshot never durably resurrects a key EXISTS reports absent. An emptied-but-kept
// stream is still snapshotted, since its counters and TTL are live key state even with no
// entries. It is a no-op on a store with no record log and on a shard that has built no
// stream registry.
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
	for k, s := range g.m {
		// Skip a stream whose key deadline already fired: a snapshot of it would durably
		// resurrect a key EXISTS reports absent, so let the next access drop it. The skip
		// is read-only, matching the scan walks.
		if s.expireAt != 0 && s.expireAt <= now {
			continue
		}
		header, run := buildStreamSnapshot(s)
		cx.St.LogCollectionSnap([]byte(k), akifile.CollKindStream, header, run)
	}
}

// buildStreamSnapshot renders a live stream to a snapshot payload: the header carries the
// counters and key TTL, and the element run packs every live entry in ID order as its
// 16-byte ID plus its length-prefixed fields. It walks the whole stream through
// collectRange over the full ID window, which spans both bands and preads any demoted
// block back, so a snapshot of a partly-cold stream is complete and in order. It allocates
// fresh slices, a checkpoint-time cost off the steady mutation path.
func buildStreamSnapshot(s *stream) (header, elementRun []byte) {
	header = make([]byte, snapHeaderLen)
	putID16(header[0:16], s.lastID)
	putID16(header[16:32], s.maxDeletedID)
	binary.LittleEndian.PutUint64(header[32:40], s.entriesAdded)
	binary.LittleEndian.PutUint64(header[40:48], uint64(s.expireAt))
	// The consumer-group section (durablegroup.go) follows the fixed counters in the
	// header, so the element run stays a pure entry run the entry walk reads unchanged.
	header = appendGroupSection(header, s)
	entries := s.collectRange(bound{id: minID}, bound{id: maxID}, false, -1)
	for i := range entries {
		elementRun = appendEntryFrame(elementRun, entries[i].id, entries[i].fields)
	}
	return header, elementRun
}

// Recover rebuilds this shard's streams from the record log's stream frames, re-driving
// each frame in the order the walk hands them onto a fresh registry. It is the stream arm
// of an .aki reopen, the sibling of set.Recover and list.Recover: after the store's string
// index recovery, the runtime calls Recover so a restart restores the streams a crash
// would otherwise lose. A snapshot frame resets its key to the snapshotted entries and
// counters, and every effect frame after it applies on top, so a stream rebuilds from its
// last snapshot plus its ordered effect tail. It applies effects through the low-level
// stream methods (appendEntry, delete, trim), not the logging command wrappers, so the
// rebuild re-logs nothing and the band an entry lands in follows the same caps a live run
// hit. A key-delete effect drops the whole stream; an emptied stream is kept, matching the
// live rule. It is a no-op on a store with no record log.
func Recover(cx *shard.Ctx) error {
	if cx.St == nil {
		return nil
	}
	g := registry(cx)
	return cx.St.WalkCollection(akifile.CollKindStream,
		func(key []byte, snap akifile.CollSnapRow) error {
			return applyStreamSnapshot(cx, g, key, snap)
		},
		func(key []byte, op akifile.CollOpRow) error {
			return applyStreamOp(cx, g, key, op)
		})
}

// applyStreamSnapshot resets key to the snapshot's entries and counters, superseding every
// effect frame for key that preceded it. It drops any state the earlier tail built,
// rebuilds the stream by appending each entry in run order (so the ID order the snapshot
// captured is the order restored), then grafts the counters the run cannot rebuild (lastID,
// maxDeletedID, entriesAdded) and the key TTL from the header. The live count is left as
// appendEntry remade it. Unlike the other collections an emptied stream is kept, so an
// empty element run still leaves the key present with its counters. A torn header or run
// reports ErrLength, the fail-closed cut a recovering reader wants.
func applyStreamSnapshot(cx *shard.Ctx, g *reg, key []byte, snap akifile.CollSnapRow) error {
	g.drop(key)
	if len(snap.Header) < snapHeaderLen {
		return akifile.ErrLength
	}
	s := newStream()
	if !eachEntryFrame(snap.ElementRun, func(id streamID, fields []field) {
		s.appendEntry(id, fields)
	}) {
		return akifile.ErrLength
	}
	s.lastID = readID16(snap.Header[0:16])
	s.maxDeletedID = readID16(snap.Header[16:32])
	s.entriesAdded = binary.LittleEndian.Uint64(snap.Header[32:40])
	s.expireAt = int64(binary.LittleEndian.Uint64(snap.Header[40:48]))
	// Rebuild the consumer groups the snapshot folded in after the fixed counters
	// (durablegroup.go); a stream snapshotted before the section existed carries none.
	if !restoreGroupSection(s, snap.Header[snapHeaderLen:]) {
		return akifile.ErrLength
	}
	g.install(cx, key, s)
	g.note(s)
	return nil
}

// applyStreamOp re-drives one stream effect onto the registry, in the order the walk hands
// them: an add creates the stream on its first entry and appends the fixed ID, a delete
// tombstones an ID, a trim drops every entry below a boundary ID, an XSETID grafts the
// counters, and a key-delete clears the whole stream. It goes through the low-level stream
// methods, so an entry that breaches an inline cap upgrades to the native band exactly as
// it did live. A key-expire sets the deadline. It reports ErrLength on a torn op payload,
// the fail-closed cut recovery wants; a structurally valid op that no longer applies (a
// delete, trim, or expire on an absent stream) is a defensive no-op, since a deterministic
// replay never produces one.
func applyStreamOp(cx *shard.Ctx, g *reg, key []byte, op akifile.CollOpRow) error {
	switch op.Op {
	case streamOpAdd:
		if len(op.SubKey) < 16 {
			return akifile.ErrLength
		}
		fields, _, ok := readFieldsFrom(op.SubValue)
		if !ok {
			return akifile.ErrLength
		}
		s := getOrCreateStream(cx, g, key)
		s.appendEntry(readID16(op.SubKey), fields)
		g.note(s)
	case streamOpDel:
		if len(op.SubKey) < 16 {
			return akifile.ErrLength
		}
		s := g.m[string(key)]
		if s == nil {
			return nil
		}
		s.delete(readID16(op.SubKey))
		g.note(s)
	case streamOpTrimMinID:
		if len(op.SubKey) < 16 {
			return akifile.ErrLength
		}
		s := g.m[string(key)]
		if s == nil {
			return nil
		}
		s.trim(trimSpec{kind: trimMinid, minid: readID16(op.SubKey)})
		g.note(s)
	case streamOpSetID:
		if len(op.SubKey) < 16 || len(op.SubValue) < 8+16 {
			return akifile.ErrLength
		}
		s := g.m[string(key)]
		if s == nil {
			return nil
		}
		s.lastID = readID16(op.SubKey)
		s.entriesAdded = binary.LittleEndian.Uint64(op.SubValue[:8])
		s.maxDeletedID = readID16(op.SubValue[8:])
		g.note(s)
	case streamOpDeleteKey:
		g.drop(key)
	case streamOpExpire:
		if len(op.SubValue) < 8 {
			return akifile.ErrLength
		}
		if s := g.m[string(key)]; s != nil {
			s.expireAt = int64(binary.LittleEndian.Uint64(op.SubValue))
		}
	default:
		// The group and PEL effect arm (durablegroup.go): group-set, group-destroy,
		// consumer-set, consumer-del, pel-set, and pel-del dispatch there.
		return applyGroupOp(cx, g, key, op)
	}
	return nil
}

// getOrCreateStream returns the stream at key, building an empty one and registering it on
// first touch, the create-on-first-add shape a live XADD and an effect replay share.
func getOrCreateStream(cx *shard.Ctx, g *reg, key []byte) *stream {
	s := g.m[string(key)]
	if s == nil {
		s = newStream()
		g.install(cx, key, s)
	}
	return s
}

// firstLiveID returns the least surviving entry ID, the boundary a trim effect records. It
// walks one entry from the front over the full ID window, so it costs one block decode (a
// pread when the front block is cold), not a full scan. ok is false when the stream holds
// no live entry.
func firstLiveID(s *stream) (streamID, bool) {
	if s.length == 0 {
		return streamID{}, false
	}
	es := s.collectRange(bound{id: minID}, bound{id: maxID}, false, 1)
	if len(es) == 0 {
		return streamID{}, false
	}
	return es[0].id, true
}

// nextID returns the ID one step past id, the boundary a trim that emptied the stream
// records so a replay drops every entry. The seq rolls into the ms on overflow, the same
// carry the auto-ID allocator uses.
func nextID(id streamID) streamID {
	if id.seq != ^uint64(0) {
		return streamID{ms: id.ms, seq: id.seq + 1}
	}
	return streamID{ms: id.ms + 1, seq: 0}
}

// minID and maxID are the low and high ends of the ID space, the full window a snapshot
// walk and a first-live probe span.
var (
	minID = streamID{ms: 0, seq: 0}
	maxID = streamID{ms: ^uint64(0), seq: ^uint64(0)}
)

// putID16 writes an ID as 16 big-endian bytes (ms then seq), the order-preserving key form
// the effect log and the snapshot run share. readID16 reads it back.
func putID16(dst []byte, id streamID) {
	binary.BigEndian.PutUint64(dst[0:8], id.ms)
	binary.BigEndian.PutUint64(dst[8:16], id.seq)
}

func readID16(src []byte) streamID {
	return streamID{ms: binary.BigEndian.Uint64(src[0:8]), seq: binary.BigEndian.Uint64(src[8:16])}
}

// appendFields packs a field run as a uvarint count then each name and value length-
// prefixed, the wire form both the add effect's SubValue and the snapshot entry frame
// carry. It reuses appendBytes, the length-prefixed frame the block bands already write.
func appendFields(dst []byte, fields []field) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(fields)))
	for i := range fields {
		dst = appendBytes(dst, fields[i].name)
		dst = appendBytes(dst, fields[i].value)
	}
	return dst
}

// readFieldsFrom decodes a field run appendFields wrote and returns the fields, the bytes
// past it, and ok. The name and value slices alias src; appendEntry copies them into the
// block blob, so they need not outlive the call. ok is false on a torn run.
func readFieldsFrom(src []byte) (fields []field, rest []byte, ok bool) {
	nf, w := binary.Uvarint(src)
	if w <= 0 {
		return nil, nil, false
	}
	p := src[w:]
	fields = make([]field, 0, nf)
	for i := uint64(0); i < nf; i++ {
		var name, value []byte
		if name, p, ok = readLenBytes(p); !ok {
			return nil, nil, false
		}
		if value, p, ok = readLenBytes(p); !ok {
			return nil, nil, false
		}
		fields = append(fields, field{name: name, value: value})
	}
	return fields, p, true
}

// readLenBytes reads one uvarint-length-prefixed frame appendBytes wrote and returns the
// bytes, the remainder, and ok. ok is false on a torn frame.
func readLenBytes(p []byte) (frame, rest []byte, ok bool) {
	n, w := binary.Uvarint(p)
	if w <= 0 || uint64(len(p)-w) < n {
		return nil, nil, false
	}
	p = p[w:]
	return p[:n], p[n:], true
}

// appendEntryFrame packs one snapshot entry as its 16-byte ID then its length-prefixed
// fields, the wire form eachEntryFrame reads back.
func appendEntryFrame(dst []byte, id streamID, fields []field) []byte {
	var idb [16]byte
	putID16(idb[:], id)
	dst = append(dst, idb[:]...)
	return appendFields(dst, fields)
}

// eachEntryFrame walks a snapshot element run forward, calling fn for each entry in ID
// order, the reader the snapshot rebuild uses over the wire form appendEntryFrame writes.
// It reports false on a torn run, which recovery treats as a corrupt frame. The field
// slices fn receives alias the run and are valid only for the call.
func eachEntryFrame(run []byte, fn func(id streamID, fields []field)) bool {
	p := run
	for len(p) > 0 {
		if len(p) < 16 {
			return false
		}
		id := readID16(p)
		p = p[16:]
		fields, rest, ok := readFieldsFrom(p)
		if !ok {
			return false
		}
		fn(id, fields)
		p = rest
	}
	return true
}
