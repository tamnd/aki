package f1srv

import (
	"encoding/binary"
	"math"
	"strconv"
)

// DUMP and RESTORE move a value out of and back into the keyspace as a self-describing byte blob:
// the type-tagged RDB serialization of the value, a two-byte RDB version, and an eight-byte CRC64
// that seals the two together. A client dumps a key, ships the opaque bytes somewhere, and restores
// them under any name on any compatible server.
//
// DUMP is not byte-identical across engines and cannot be: Redis 8.8 stamps RDB version 14 into the
// footer and Valkey 9.1 stamps 80, so the version bytes differ and, because the CRC64 covers those
// bytes, the checksums differ too, even for the same value. The compatibility contract is therefore
// round-trip and interop, not byte-equality: aki restores what aki dumped, aki restores what Redis
// or Valkey dumped, and both of them restore what aki dumped. Byte-for-byte the payload cannot match
// both servers at once because the two servers do not match each other.
//
// The string body is the standard RDB string encoding, so a short canonical integer is int-encoded
// exactly as Redis does (the DUMP of "12345" is the same leading bytes on all three engines), and any
// other string is length-prefixed. On the load side the decoder accepts every RDB string form a real
// server emits, including the LZF-compressed form Redis uses for a long compressible value, so a
// Redis- or Valkey-produced string blob restores here even though aki's own encoder never compresses.
//
// The hash type is the second slice. aki dumps a hash as RDB_TYPE_HASH (type 4), the plain form: a
// field count then that many field/value string pairs, which both reference servers accept on RESTORE
// at any size. The load side additionally decodes RDB_TYPE_HASH_LISTPACK (type 16), the listpack form
// both Redis 8.8 and Valkey 9.1 actually emit even for large hashes, so a hash blob produced by either
// server restores here.
//
// The set type is the third slice and follows the same shape. aki dumps a set as RDB_TYPE_SET (type
// 2), a member count then that many member strings, again the plain form both servers accept at any
// size. The load side decodes the two packed forms both servers emit for a small set: the listpack
// form (RDB_TYPE_SET_LISTPACK, type 20) and the intset form (RDB_TYPE_SET_INTSET, type 11), an
// all-integer set packed into a width-prefixed sorted integer array.
//
// The sorted set is the fourth slice. aki dumps a zset as RDB_TYPE_ZSET_2 (type 5), a member count
// then that many member/score pairs where each score is an 8-byte little-endian binary double, the
// same bytes aki already stores on the member row, so the dump is a copy with no float-to-text step.
// Both reference servers accept type 5 at any size. The load side additionally decodes the two forms
// a real server emits for a small zset, the listpack form (RDB_TYPE_ZSET_LISTPACK, type 17, with
// member and score alternating) and the older ASCII-double form (RDB_TYPE_ZSET, type 3), so a zset
// blob produced by either server restores here.
//
// The list is the fifth slice. Both Redis 8.8 and Valkey 9.1 serialize every list, small or large, as
// RDB_TYPE_LIST_QUICKLIST_2 (type 18): a node count then, per node, a container byte (1 PLAIN, a
// single value stored as the node; 2 PACKED, a listpack of the node's elements) and the node body as
// an RDB string. aki dumps a list in that same type as a single PACKED node whose listpack holds all
// the elements in head-to-tail order. The PLAIN container is not used on the write side because both
// reference servers reject a PLAIN node that carries a short or empty value (they reserve PLAIN for a
// value too large to pack), so a small or empty element has to ride inside a listpack. The load side
// decodes the quicklist form both engines emit (PACKED nodes unpacked through lpDecode, PLAIN nodes
// taken verbatim so a huge-value blob still loads) and the older plain RDB_TYPE_LIST (type 1, a count
// then that many string elements) for completeness, so a list blob produced by either server restores
// here.
//
// The stream is the sixth and last slice, and the one type whose body is a structure rather than a
// flat element list. aki dumps a stream as RDB_TYPE_STREAM_LISTPACKS_3 (type 21): a run of listpack
// nodes (one per entry, each keyed by that entry's 16-byte ID and holding a one-item stream listpack),
// then the stream metadata (length, last ID, first ID, max-deleted ID, entries-added), then the
// consumer groups (each with its last ID, entries-read counter, global pending list, and consumers
// with their own pending lists). Type 21 is chosen because both Redis 8.8 and Valkey 9.1 accept it on
// RESTORE: Redis 8.8 itself emits the newer type 27 (with an XNACK zone and an idempotent-producer
// trailer) and Valkey 9.1 emits type 21, so the load side parses all five stream types (15, 19, 21,
// 26, 27), reading the version-gated fields each carries and consuming and discarding the trailing
// blocks (the per-group NACK zone of type 27 and the IDMP block of types 26 and 27) that aki does not
// model. A stream blob produced by either server therefore restores here, and an aki stream restores
// on either server. This slice reuses this file's CRC64, version framing, string primitives, and the
// listpack codec, and adds the small-integer listpack helpers and the stream listpack walk.

// RDB object type bytes. Only the forms aki serializes or has to load are named here.
const (
	rdbTypeString         = 0x00 // a plain string value
	rdbTypeList           = 0x01 // the old list: a length then that many string elements
	rdbTypeZset           = 0x03 // the old sorted set: a count then member/score pairs, score as ASCII
	rdbTypeSet            = 0x02 // a set as a member count then that many member strings
	rdbTypeHash           = 0x04 // a hash as a field count then field/value string pairs
	rdbTypeZset2          = 0x05 // the sorted set with binary double scores, the form aki writes
	rdbTypeSetIntset      = 0x0b // an all-integer set packed into a single intset blob
	rdbTypeHashListpack   = 0x10 // a hash packed into a single listpack blob, the form both servers emit
	rdbTypeZsetListpack   = 0x11 // a sorted set packed into a single listpack blob, the small-zset form
	rdbTypeListQuicklist2 = 0x12 // a quicklist: a node count then per-node a container byte + node blob
	rdbTypeSetListpack    = 0x14 // a set packed into a single listpack blob, the form both servers emit

	// The stream forms. Each newer type adds fields to the older one's layout; aki writes _3 and reads
	// every form up to _5.
	rdbTypeStreamListpacks  = 0x0f // 15: the original stream, no first-id/max-deleted/entries-added
	rdbTypeStreamListpacks2 = 0x13 // 19: adds first-id, max-deleted-id, entries-added, group entries-read
	rdbTypeStreamListpacks3 = 0x15 // 21: adds consumer active-time, the form aki writes and Valkey emits
	rdbTypeStreamListpacks4 = 0x1a // 26: adds the trailing IDMP (idempotent producer) block
	rdbTypeStreamListpacks5 = 0x1b // 27: adds the per-group NACK zone, the form Redis 8.8 emits
)

// Stream listpack item flags, stored as the first element of each item inside a stream listpack node.
const (
	streamItemDeleted    = 1 << 0 // the item is a tombstone: present in the listpack, skipped on read
	streamItemSameFields = 1 << 1 // the item's fields are the master entry's fields, so only values follow
)

// Quicklist node container tags inside an RDB_TYPE_LIST_QUICKLIST_2 body: a PLAIN node holds one
// value directly, a PACKED node holds a listpack of the node's elements.
const (
	quicklistNodePlain  = 1
	quicklistNodePacked = 2
)

// rdbVersion is the RDB version stamped into the footer of every DUMP payload aki produces. RESTORE
// on a server refuses a payload whose version is newer than the server's own, so the value is chosen
// low enough that both Redis 8.8 (RDB version 14) and Valkey 9.1 (RDB version 80) accept an
// aki-produced blob, and at the version where the listpack encodings later type slices emit became
// valid, so those slices do not have to raise it.
const rdbVersion = 11

// rdbMaxLoadVersion is the newest RDB version RESTORE will load. It is set to Valkey 9.1's version so
// a blob produced by either reference server is accepted; the string body format is stable across
// every version in that range, so a higher-versioned string blob still decodes.
const rdbMaxLoadVersion = 80

// RDB string encodings signalled in the low six bits of a 0b11xxxxxx length byte.
const (
	rdbEncInt8  = 0 // 0xC0: an 8-bit signed integer follows
	rdbEncInt16 = 1 // 0xC1: a 16-bit signed integer follows, little-endian
	rdbEncInt32 = 2 // 0xC2: a 32-bit signed integer follows, little-endian
	rdbEncLZF   = 3 // 0xC3: an LZF-compressed string follows
)

// crc64Table is the lookup table for Redis's CRC-64 (the "Jones" polynomial 0xad93d23594c935a9,
// reflected input and output, zero initial and final values), the checksum both servers seal a DUMP
// payload with. It is built once at init from the reflected polynomial.
var crc64Table [256]uint64

func init() {
	rp := crc64Reflect(0xad93d23594c935a9)
	for n := 0; n < 256; n++ {
		crc := uint64(n)
		for k := 0; k < 8; k++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ rp
			} else {
				crc >>= 1
			}
		}
		crc64Table[n] = crc
	}
}

// crc64Reflect returns the bit-reversal of a 64-bit value, used to turn the normal-form polynomial
// into the reflected form the reflected table build and update step expect.
func crc64Reflect(v uint64) uint64 {
	var r uint64
	for i := 0; i < 64; i++ {
		if v&1 != 0 {
			r |= 1 << (63 - i)
		}
		v >>= 1
	}
	return r
}

// crc64 folds data into a running CRC-64. The seed is zero at the start of a payload.
func crc64(seed uint64, data []byte) uint64 {
	crc := seed
	for _, b := range data {
		crc = crc64Table[byte(crc)^b] ^ (crc >> 8)
	}
	return crc
}

// cmdDump serializes a key's value to the RDB blob RESTORE consumes, or replies null when the key
// does not exist. The string, hash, set, and sorted set types are serialized here; the remaining
// collection types are refused rather than answered with a body this file cannot build, and their
// slices lift that refusal.
func (c *connState) cmdDump(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'dump' command")
		return
	}
	key := argv[1]
	if c.srv.volatile.Load() != 0 {
		c.expireIfNeeded(key)
	}
	switch c.resolveType(key) {
	case keyMissing:
		c.writeNil()
	case keyString:
		v, _ := c.srv.store.Get(key, nil)
		payload := rdbAppendString([]byte{rdbTypeString}, v)
		c.writeBulk(rdbSeal(payload))
	case keyHash:
		c.writeBulk(rdbSeal(c.rdbDumpHash(key)))
	case keySet:
		c.writeBulk(rdbSeal(c.rdbDumpSet(key)))
	case keyZset:
		c.writeBulk(rdbSeal(c.rdbDumpZset(key)))
	case keyList:
		c.writeBulk(rdbSeal(c.rdbDumpList(key)))
	case keyStream:
		c.writeBulk(rdbSeal(c.rdbDumpStream(key)))
	default:
		c.writeErr("ERR DUMP of this type is not supported yet")
	}
}

// rdbDumpHash builds the RDB_TYPE_HASH body for a hash: the type byte, the field count, then each
// field and value as an RDB string. It holds the key's stripe lock for a consistent snapshot and
// walks the fields off the O(1) count and the collection index, the same enumerate path HGETALL uses,
// so it never depends on an in-memory copy of the whole hash.
func (c *connState) rdbDumpHash(hkey []byte) []byte {
	mu := &c.srv.incrMu[c.srv.stripe(hkey)]
	mu.Lock()
	defer mu.Unlock()

	payload := rdbAppendLen([]byte{rdbTypeHash}, c.hashCount(hkey))
	prefix := c.hashPrefix(hkey)
	plen := len(prefix)
	var after []byte
	scanK := make([][]byte, 0, hashScanBatch)
	scanO := make([]uint64, 0, hashScanBatch)
	var vbuf []byte
	for {
		keys, offs, last := c.srv.store.CollScanKV(prefix, after, hashScanBatch, scanK[:0], scanO[:0])
		if len(keys) == 0 {
			break
		}
		for i, k := range keys {
			payload = rdbAppendString(payload, k[plen:])
			vbuf = c.srv.store.ReadValueAt(offs[i], vbuf[:0])
			payload = rdbAppendString(payload, vbuf)
		}
		if last == nil {
			break
		}
		after = last
	}
	return payload
}

// rdbDumpSet builds the RDB_TYPE_SET body for a set: the type byte, the member count, then each
// member as an RDB string (a canonical integer member int-encodes, the way both servers store an
// all-integer member). It freezes the partition layout with rlockSet and walks the dense member
// vector the same way SMEMBERS does (spec 2064/f1_rewrite_ltm/20 section 5), so it never
// materializes the whole set and never touches the global ordered index. The member count in the
// header is the vector length under the frozen layout, so it always equals the number of members
// the walk emits. The vector carries no order, so the members serialize in an implementation-defined
// order, which RESTORE and both servers accept for a set.
func (c *connState) rdbDumpSet(skey []byte) []byte {
	p, unlock := c.rlockSet(skey)
	defer unlock()

	scan := make([][]byte, 0, hashScanBatch)

	if p > 1 {
		base := c.partScanBase(skey)
		moff := len(base) // member starts right after uvarint(len)|skey|byte(part)
		var n int
		for part := 0; part < p; part++ {
			n += c.srv.store.SetPartVecLen(base, p, part)
		}
		payload := rdbAppendLen([]byte{rdbTypeSet}, uint64(n))
		for part := 0; part < p; part++ {
			hi := -1
			for {
				keys, next := c.srv.store.SetPartVecScanDown(base, p, part, hi, hashScanBatch, scan[:0])
				for _, k := range keys {
					payload = rdbAppendString(payload, k[moff:])
				}
				if next == 0 {
					break
				}
				hi = next
			}
		}
		return payload
	}

	prefix := c.setPrefix(skey)
	plen := len(prefix)
	payload := rdbAppendLen([]byte{rdbTypeSet}, uint64(c.srv.store.SetVecLen(prefix)))
	hi := -1
	for {
		keys, next := c.srv.store.SetVecScanDown(prefix, hi, hashScanBatch, scan[:0])
		for _, k := range keys {
			payload = rdbAppendString(payload, k[plen:])
		}
		if next == 0 {
			break
		}
		hi = next
	}
	return payload
}

// rdbDumpZset builds the RDB_TYPE_ZSET_2 body for a sorted set: the type byte, the member count, then
// each member as an RDB string followed by its score as an 8-byte little-endian binary double. It
// holds the stripe lock and walks the member-family rows off the O(1) count and the collection index,
// the enumerate path ZRANDMEMBER uses, so it never materializes the whole zset. Each member row's
// value is already the 8-byte little-endian score bits, which is exactly what ZSET_2 stores, so the
// score copies through with no float-to-text-and-back round trip.
func (c *connState) rdbDumpZset(zkey []byte) []byte {
	mu := &c.srv.incrMu[c.srv.stripe(zkey)]
	mu.Lock()
	defer mu.Unlock()

	payload := rdbAppendLen([]byte{rdbTypeZset2}, c.zsetCard(zkey))
	prefix := c.zmemberPrefix(zkey)
	plen := len(prefix)
	var after []byte
	scanK := make([][]byte, 0, hashScanBatch)
	scanO := make([]uint64, 0, hashScanBatch)
	var vbuf []byte
	for {
		keys, offs, last := c.srv.store.CollScanKV(prefix, after, hashScanBatch, scanK[:0], scanO[:0])
		if len(keys) == 0 {
			break
		}
		for i, k := range keys {
			payload = rdbAppendString(payload, k[plen:])
			vbuf = c.srv.store.ReadValueAt(offs[i], vbuf[:0])
			// The member row carries the score as 8 little-endian bytes; a value of any other length
			// would mean a corrupt row, so fall back to a zero score rather than emit a short blob.
			var sb [8]byte
			if len(vbuf) == 8 {
				copy(sb[:], vbuf)
			}
			payload = append(payload, sb[:]...)
		}
		if last == nil {
			break
		}
		after = last
	}
	return payload
}

// rdbDumpList builds the RDB_TYPE_LIST_QUICKLIST_2 body for a list: the type byte, a node count of
// one, a PACKED container byte, and one listpack node holding every element as an RDB string. It holds
// the stripe lock for a consistent snapshot and walks the window head to tail by position, the same
// point-lookup path LRANGE uses, appending each element into the listpack as it goes so it never keeps
// more than the growing node in hand. The PACKED node is required rather than a run of PLAIN nodes:
// both reference servers reject a PLAIN node that carries a short or empty value, since they reserve
// PLAIN for a value too large to pack, so a small or empty element has to be encoded inside a listpack.
func (c *connState) rdbDumpList(lkey []byte) []byte {
	mu := &c.srv.incrMu[c.srv.stripe(lkey)]
	mu.Lock()
	defer mu.Unlock()

	// A resident push leaves element bytes only in the ring, so retire the hot-list window to flush
	// them to f1raw rows before this positional dump reads them (slice 3, impl/34). This holds the
	// key's exclusive stripe lock, which drainEvict requires.
	c.listWinDrainEvict(lkey)
	head, tail, _, _, ok := c.listHeader(lkey)
	if !ok {
		// cmdDump only reaches here for a live key, so this is the defensive empty-list form.
		return rdbAppendLen([]byte{rdbTypeListQuicklist2}, 0)
	}
	// Build the listpack node: a 6-byte header filled in once the body length and count are known, one
	// entry per element, and the single 0xFF terminator.
	lp := []byte{0, 0, 0, 0, 0, 0}
	var vbuf []byte
	count := int64(0)
	for pos := head; pos < tail; pos++ {
		ek := c.listElemKey(lkey, pos)
		v, _ := c.srv.store.GetKind(ek, vbuf[:0], kindListElem)
		vbuf = v
		lp = lpAppendEntry(lp, v)
		count++
	}
	lp = append(lp, 0xFF)
	binary.LittleEndian.PutUint32(lp[0:4], uint32(len(lp)))
	if count > 0xFFFF {
		count = 0xFFFF // the count field saturates; a loader that sees 0xFFFF rescans the entries
	}
	binary.LittleEndian.PutUint16(lp[4:6], uint16(count))

	payload := rdbAppendLen([]byte{rdbTypeListQuicklist2}, 1)
	payload = rdbAppendLen(payload, quicklistNodePacked)
	payload = rdbAppendString(payload, lp)
	return payload
}

// rdbDumpStream builds the RDB_TYPE_STREAM_LISTPACKS_3 body for a stream: the type byte, one listpack
// node per entry (each keyed by that entry's 16-byte ID and holding a one-item stream listpack), the
// stream metadata, then the consumer groups with their pending lists and consumers. It holds the
// stripe lock and walks the entry, group, consumer, and PEL families straight off the ordered index,
// the same enumerate paths XRANGE and XINFO use, so it never materializes the whole stream. Type 21 is
// written because both Redis 8.8 and Valkey 9.1 accept it on RESTORE.
func (c *connState) rdbDumpStream(skey []byte) []byte {
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	defer mu.Unlock()

	length, lastID, maxDel, entriesAdded, _ := c.streamHeader(skey)

	payload := []byte{rdbTypeStreamListpacks3}
	// One listpack node per entry. The node count is the live entry count, and each node carries
	// exactly one entry, so the loader's live-entry sum equals the length written below.
	payload = rdbAppendLen(payload, length)
	firstID := streamID{}
	haveFirst := false
	prefix := c.streamEntryPrefix(skey)
	var after []byte
	scan := make([][]byte, 0, hashScanBatch)
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			id := decodeEntryID(k)
			if !haveFirst {
				firstID = id
				haveFirst = true
			}
			fields := c.readEntryFields(k)
			var idb [16]byte
			putStreamID(idb[:], id)
			payload = rdbAppendString(payload, idb[:])
			payload = rdbAppendString(payload, buildStreamEntryListpack(fields))
		}
		if last == nil {
			break
		}
		after = last
	}

	// Stream metadata: length, last ID, first ID, max-deleted ID, entries-added.
	payload = rdbAppendLen(payload, length)
	payload = rdbAppendLen(payload, lastID.ms)
	payload = rdbAppendLen(payload, lastID.seq)
	payload = rdbAppendLen(payload, firstID.ms)
	payload = rdbAppendLen(payload, firstID.seq)
	payload = rdbAppendLen(payload, maxDel.ms)
	payload = rdbAppendLen(payload, maxDel.seq)
	payload = rdbAppendLen(payload, entriesAdded)

	// Consumer groups. Each group carries its name, last ID, entries-read, global PEL, and consumers.
	groups := c.dumpStreamGroups(skey)
	payload = rdbAppendLen(payload, uint64(len(groups)))
	for _, g := range groups {
		payload = rdbAppendString(payload, []byte(g.name))
		payload = rdbAppendLen(payload, g.lastID.ms)
		payload = rdbAppendLen(payload, g.lastID.seq)
		payload = rdbAppendLen(payload, g.entriesRead)

		// Global PEL: count then, per entry, the raw 16-byte ID, an 8-byte little-endian delivery
		// time, and the delivery count. The rows scan in ID order, which is the order Redis writes.
		payload = rdbAppendLen(payload, uint64(len(g.pel)))
		for _, pe := range g.pel {
			var idb [16]byte
			putStreamID(idb[:], pe.id)
			payload = append(payload, idb[:]...)
			payload = rdbAppendMS(payload, pe.deliveryTime)
			payload = rdbAppendLen(payload, pe.deliveryCount)
		}

		// Consumers: count then, per consumer, the name, an 8-byte seen time, an 8-byte active time,
		// and the consumer PEL (just the raw 16-byte IDs it owns, since the delivery bookkeeping lives
		// on the global PEL and the owner is resolved by ID at load).
		payload = rdbAppendLen(payload, uint64(len(g.consumers)))
		for _, con := range g.consumers {
			payload = rdbAppendString(payload, []byte(con.name))
			payload = rdbAppendMS(payload, con.seenTime)
			payload = rdbAppendMS(payload, con.activeTime)
			owned := g.pel[:0:0]
			for _, pe := range g.pel {
				if pe.consumer == con.name {
					owned = append(owned, pe)
				}
			}
			payload = rdbAppendLen(payload, uint64(len(owned)))
			for _, pe := range owned {
				var idb [16]byte
				putStreamID(idb[:], pe.id)
				payload = append(payload, idb[:]...)
			}
		}
	}
	return payload
}

// dumpStreamGroups reads a stream's groups, each with its global PEL (owner resolved) and its
// consumers, off the group, PEL, and consumer families. The caller holds the stripe lock. Groups are
// cold relative to XADD, so the straightforward scan-and-decode here is off any measured hot path.
func (c *connState) dumpStreamGroups(skey []byte) []rdbStreamGroup {
	var groups []rdbStreamGroup
	gPrefix := streamFamilyPrefix(skey, streamGroupTag)
	gplen := len(gPrefix)
	var after, gbuf []byte
	for {
		keys, last := c.srv.store.CollScan(gPrefix, after, hashScanBatch, nil)
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			name := string(k[gplen:])
			v, ok := c.srv.store.GetKind(k, gbuf[:0], kindStreamGroup)
			if !ok || len(v) < streamGroupBytes {
				continue
			}
			gbuf = v
			g := rdbStreamGroup{
				name:        name,
				lastID:      streamID{ms: binary.LittleEndian.Uint64(v[0:8]), seq: binary.LittleEndian.Uint64(v[8:16])},
				entriesRead: binary.LittleEndian.Uint64(v[24:32]),
			}
			g.pel = c.dumpStreamPEL(skey, name)
			g.consumers = c.dumpStreamConsumers(skey, name)
			groups = append(groups, g)
		}
		if last == nil {
			break
		}
		after = last
	}
	return groups
}

// dumpStreamPEL reads a group's pending list in ID order, each entry carrying its owning consumer
// and delivery bookkeeping straight off the PEL row value.
func (c *connState) dumpStreamPEL(skey []byte, group string) []rdbStreamPEL {
	var pel []rdbStreamPEL
	prefix := streamPELPrefix(skey, group)
	var after, buf []byte
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, nil)
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			v, ok := c.srv.store.GetKind(k, buf[:0], kindStreamPEL)
			if !ok {
				continue
			}
			buf = v
			pe := decodeStreamPEL(v)
			pel = append(pel, rdbStreamPEL{
				id:            decodeEntryID(k),
				consumer:      pe.consumer,
				deliveryTime:  pe.deliveryTime,
				deliveryCount: pe.deliveryCount,
			})
		}
		if last == nil {
			break
		}
		after = last
	}
	return pel
}

// dumpStreamConsumers reads a group's consumers in name order, each with its seen and active
// clocks. The consumer's pending IDs are not read here: they are on the group PEL, tagged with this
// consumer as owner, and the dump emits them per consumer from there.
func (c *connState) dumpStreamConsumers(skey []byte, group string) []rdbStreamConsumer {
	var cons []rdbStreamConsumer
	prefix := streamConsumerPrefix(skey, group)
	plen := len(prefix)
	var after, buf []byte
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, nil)
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			v, ok := c.srv.store.GetKind(k, buf[:0], kindStreamConsumer)
			if !ok || len(v) < streamConsumerBytes {
				continue
			}
			buf = v
			cons = append(cons, rdbStreamConsumer{
				name:       string(k[plen:]),
				seenTime:   int64(binary.LittleEndian.Uint64(v[0:8])),
				activeTime: int64(binary.LittleEndian.Uint64(v[8:16])),
			})
		}
		if last == nil {
			break
		}
		after = last
	}
	return cons
}

// buildStreamEntryListpack builds the one-item stream listpack node for an entry's fields. The node
// holds a master entry (count 1, no tombstones, the entry's field names) followed by a single
// SAMEFIELDS item that carries only the values, its ID identical to the node's master ID so both
// diffs are zero. This is the exact shape Redis's streamAppendItem writes for a fresh single-entry
// node, so a deep RESTORE integrity check walks it the same way it walks a native listpack.
func buildStreamEntryListpack(fields [][]byte) []byte {
	m := int64(len(fields) / 2)
	lp := []byte{0, 0, 0, 0, 0, 0}
	// Master entry: count, deleted, field-count, the field names, then the zero terminator.
	lp = lpAppendIntVal(lp, 1)
	lp = lpAppendIntVal(lp, 0)
	lp = lpAppendIntVal(lp, m)
	for i := int64(0); i < m; i++ {
		lp = lpAppendEntry(lp, fields[i*2])
	}
	lp = lpAppendIntVal(lp, 0)
	// The single item: flags marking it as sharing the master fields, a zero ms and seq diff, the
	// values, then the entry's backward element count (values + the three leading control elements).
	lp = lpAppendIntVal(lp, streamItemSameFields)
	lp = lpAppendIntVal(lp, 0)
	lp = lpAppendIntVal(lp, 0)
	for i := int64(0); i < m; i++ {
		lp = lpAppendEntry(lp, fields[i*2+1])
	}
	lp = lpAppendIntVal(lp, m+3)
	lp = append(lp, 0xFF)
	binary.LittleEndian.PutUint32(lp[0:4], uint32(len(lp)))
	// The header element count is the total number of listpack elements: the master entry's M+4 plus
	// the item's M+4. It saturates at 0xFFFF, the point a loader rescans rather than trusts the count.
	numele := 2*m + 8
	if numele > 0xFFFF {
		numele = 0xFFFF
	}
	binary.LittleEndian.PutUint16(lp[4:6], uint16(numele))
	return lp
}

// lpAppendIntVal appends one listpack integer entry: the integer encoding for v followed by its
// back-length. It is the numeric counterpart of lpAppendEntry, used for the control elements of a
// stream listpack node (counts, flags, ID diffs) that are always integers.
func lpAppendIntVal(dst []byte, v int64) []byte {
	before := len(dst)
	dst = lpAppendInt(dst, v)
	return lpAppendBacklen(dst, len(dst)-before)
}

// rdbAppendMS appends a millisecond timestamp as an 8-byte little-endian signed integer, the form
// rdbSaveMillisecondTime writes for a PEL delivery time and a consumer's seen and active times.
func rdbAppendMS(dst []byte, t int64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(t))
	return append(dst, b[:]...)
}

// lpAppendEntry appends one listpack entry for e: its encoding and data, then the back-length that
// lets the listpack be walked backwards. A value that parses as a canonical int64 takes the compact
// integer encoding; anything else a string encoding sized by its length. The layout mirrors
// listEncodingSize and listBacklenSize exactly, so a listpack this builds and one aki sizes agree
// byte for byte, and it matches what Redis's lpEncodeGetType and lpEncodeBacklen emit.
func lpAppendEntry(dst, e []byte) []byte {
	before := len(dst)
	if v, ok := listTryInteger(e); ok {
		dst = lpAppendInt(dst, v)
	} else {
		dst = lpAppendStr(dst, e)
	}
	return lpAppendBacklen(dst, len(dst)-before)
}

// lpAppendInt appends the listpack integer encoding for v, choosing the smallest of the seven forms
// lpGet decodes: a 7-bit unsigned byte, a 13-bit signed pair, or a 0xF1..0xF4 tag followed by a 16-,
// 24-, 32-, or 64-bit little-endian two's-complement value.
func lpAppendInt(dst []byte, v int64) []byte {
	switch {
	case v >= 0 && v <= 127:
		return append(dst, byte(v)) // 0xxxxxxx
	case v >= -4096 && v <= 4095:
		u := uint16(v) & 0x1FFF // 110xxxxx xxxxxxxx, 13-bit two's complement
		return append(dst, 0xC0|byte(u>>8), byte(u))
	case v >= -32768 && v <= 32767:
		u := uint16(v)
		return append(dst, 0xF1, byte(u), byte(u>>8))
	case v >= -8388608 && v <= 8388607:
		u := uint32(v) & 0xFFFFFF
		return append(dst, 0xF2, byte(u), byte(u>>8), byte(u>>16))
	case v >= -2147483648 && v <= 2147483647:
		u := uint32(v)
		return append(dst, 0xF3, byte(u), byte(u>>8), byte(u>>16), byte(u>>24))
	default:
		u := uint64(v)
		return append(dst, 0xF4, byte(u), byte(u>>8), byte(u>>16), byte(u>>24),
			byte(u>>32), byte(u>>40), byte(u>>48), byte(u>>56))
	}
}

// lpAppendStr appends the listpack string encoding for e: a 6-bit length byte for a short string, a
// 12-bit length pair for a medium one, or a 0xF0 tag and a 32-bit little-endian length for a long one,
// each followed by the raw bytes. The boundaries match lpGet and listEncodingSize.
func lpAppendStr(dst, e []byte) []byte {
	n := len(e)
	switch {
	case n < 64:
		dst = append(dst, 0x80|byte(n)) // 10xxxxxx
	case n < 4096:
		dst = append(dst, 0xE0|byte(n>>8), byte(n)) // 1110xxxx xxxxxxxx
	default:
		dst = append(dst, 0xF0, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
	}
	return append(dst, e...)
}

// lpAppendBacklen appends the entry back-length in the 1-to-5-byte form lpDecodeBacklen reads from the
// end of an entry: the value's 7-bit groups most-significant first, with the continuation bit set on
// every byte after the first. The byte count matches listBacklenSize(encLen) so a forward walk skips
// exactly what this writes.
func lpAppendBacklen(dst []byte, l int) []byte {
	switch {
	case l <= 127:
		return append(dst, byte(l))
	case l < 16384:
		return append(dst, byte(l>>7), byte(l&127)|128)
	case l < 2097152:
		return append(dst, byte(l>>14), byte((l>>7)&127)|128, byte(l&127)|128)
	case l < 268435456:
		return append(dst, byte(l>>21), byte((l>>14)&127)|128, byte((l>>7)&127)|128, byte(l&127)|128)
	default:
		return append(dst, byte(l>>28), byte((l>>21)&127)|128, byte((l>>14)&127)|128,
			byte((l>>7)&127)|128, byte(l&127)|128)
	}
}

// cmdRestore parses a DUMP blob and writes its value under a key, honoring the TTL and the REPLACE,
// ABSTTL, IDLETIME, and FREQ options. It reproduces both servers' errors: a negative TTL, a blob
// whose version or checksum is wrong, an unparseable body, and a target key that already exists
// without REPLACE.
func (c *connState) cmdRestore(argv [][]byte) {
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for 'restore' command")
		return
	}
	key := argv[1]
	ttl, ok := parseInt64Strict(argv[2])
	if !ok {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	if ttl < 0 {
		c.writeErr("ERR Invalid TTL value, must be >= 0")
		return
	}
	blob := argv[3]

	var replace, absttl bool
	for i := 4; i < len(argv); {
		switch {
		case eqFold(argv[i], "REPLACE"):
			replace = true
			i++
		case eqFold(argv[i], "ABSTTL"):
			absttl = true
			i++
		case eqFold(argv[i], "IDLETIME"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			// f1srv does not track per-key idle time, so the value is validated for compatibility
			// and otherwise ignored, the way OBJECT IDLETIME already reports zero here.
			if _, ok := parseInt64Strict(argv[i+1]); !ok {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			i += 2
		case eqFold(argv[i], "FREQ"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			n, ok := parseInt64Strict(argv[i+1])
			if !ok {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			if n < 0 || n > 255 {
				c.writeErr("ERR Invalid frequency value, must be >= 0 and <= 255")
				return
			}
			i += 2
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}

	// The footer (version + CRC64) is checked before the body, so a truncated or corrupt blob is
	// rejected with the version-or-checksum error rather than a body-parse error, matching Redis.
	body, ok := rdbCheckFooter(blob)
	if !ok {
		c.writeErr("ERR DUMP payload version or checksum are wrong")
		return
	}
	val, ok := rdbLoadValue(body)
	if !ok {
		c.writeErr("ERR Bad data format")
		return
	}

	// Reap an expired target before the existence check so RESTORE onto a key that has just expired
	// succeeds without REPLACE, the same as on both servers. The probe runs before the stripe lock
	// because expireIfNeeded takes that lock itself.
	if c.srv.volatile.Load() != 0 {
		c.expireIfNeeded(key)
	}
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()

	if c.resolveType(key) != keyMissing {
		if !replace {
			c.writeErr("BUSYKEY Target key name already exists.")
			return
		}
		c.dropKeyLocked(key)
	}
	if err := c.rdbWriteValue(key, val); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	if ttl > 0 {
		atMs := ttl
		if !absttl {
			// A relative TTL is milliseconds from now; the overflow guard keeps a huge TTL from
			// wrapping into the past.
			at, ok := addOverflow(c.nowMs, ttl)
			if !ok {
				c.writeErr("ERR Invalid TTL value, must be >= 0")
				return
			}
			atMs = at
		}
		c.setExpiryLocked(key, atMs)
	}
	c.writeSimple("OK")
}

// rdbSeal appends the two-byte RDB version and the eight-byte CRC64 footer to a payload and returns
// the finished blob. The checksum covers the value bytes and the version bytes together.
func rdbSeal(payload []byte) []byte {
	payload = append(payload, byte(rdbVersion), byte(rdbVersion>>8))
	var sum [8]byte
	binary.LittleEndian.PutUint64(sum[:], crc64(0, payload))
	return append(payload, sum[:]...)
}

// rdbCheckFooter validates a blob's trailing version and CRC64 and returns the body (the type byte
// and value, without the footer). It fails when the blob is too short to hold a footer, names a
// version this build will not load, or does not match its own checksum.
func rdbCheckFooter(blob []byte) ([]byte, bool) {
	// A footer is 2 version bytes + 8 CRC bytes; the smallest value is a 1-byte type + a 1-byte
	// empty-string length, so the shortest valid blob is 12 bytes. Anything shorter is corrupt.
	if len(blob) < 12 {
		return nil, false
	}
	n := len(blob)
	ver := binary.LittleEndian.Uint16(blob[n-10 : n-8])
	if ver == 0 || ver > rdbMaxLoadVersion {
		return nil, false
	}
	stored := binary.LittleEndian.Uint64(blob[n-8:])
	if crc64(0, blob[:n-8]) != stored {
		return nil, false
	}
	return blob[:n-10], true
}

// rdbValue is the decoded form of a DUMP body: the value's type and, for a string, its bytes, or for
// a collection, its elements in order (a hash carries field, value, field, value ...). Keeping the
// elements here lets a single loader handle every type and the caller write them through the type's
// own primitives without re-parsing.
type rdbValue struct {
	kind   keyKind
	str    []byte
	elems  [][]byte
	scores []float64  // for a zset, the score of each member in elems, aligned by index
	stream *rdbStream // for a stream, the whole decoded log and its groups
}

// rdbStream is the decoded form of a stream DUMP body: the entry log, the stream metadata, and the
// consumer groups. It mirrors what f1srv stores on the header, entry, group, consumer, and PEL rows,
// so rdbWriteValue can land it row for row without re-deriving anything.
type rdbStream struct {
	entries      []rdbStreamEntry
	length       uint64
	lastID       streamID
	maxDeleted   streamID
	entriesAdded uint64
	groups       []rdbStreamGroup
}

// rdbStreamEntry is one log entry: its ID and its field/value pairs flattened in insertion order,
// the same flat form encodeStreamFields takes.
type rdbStreamEntry struct {
	id     streamID
	fields [][]byte
}

// rdbStreamGroup is one consumer group: its name, last-delivered ID, entries-read counter, global
// pending list, and consumers. The global PEL carries the owning consumer resolved from the consumer
// PELs at parse time, so a write is a straight row-per-entry landing.
type rdbStreamGroup struct {
	name        string
	lastID      streamID
	entriesRead uint64
	pel         []rdbStreamPEL
	consumers   []rdbStreamConsumer
}

// rdbStreamPEL is one pending entry: its ID, delivery bookkeeping, and the consumer that owns it
// (empty until a consumer PEL claims it, which the NACK-zone case of type 27 can leave empty).
type rdbStreamPEL struct {
	id            streamID
	consumer      string
	deliveryTime  int64
	deliveryCount uint64
}

// rdbStreamConsumer is one consumer's identity and clocks; its pending IDs live on the group PEL
// with this consumer named as owner, so the consumer's own pending count is derived on write.
type rdbStreamConsumer struct {
	name       string
	seenTime   int64
	activeTime int64
}

// rdbLoadValue parses a value body (a type byte followed by the type's encoding) into an rdbValue. It
// understands the string, hash, set, and sorted set types in every encoding a real server emits: the
// plain count forms, the listpack and intset packed forms, and the binary- and ASCII-double zset
// forms. Other type bytes are reported as unparseable until their slices land.
func rdbLoadValue(body []byte) (rdbValue, bool) {
	if len(body) < 1 {
		return rdbValue{}, false
	}
	switch body[0] {
	case rdbTypeString:
		s, _, ok := rdbReadString(body[1:])
		if !ok {
			return rdbValue{}, false
		}
		return rdbValue{kind: keyString, str: s}, true
	case rdbTypeHash:
		count, rest, ok := rdbReadLen(body[1:])
		if !ok {
			return rdbValue{}, false
		}
		elems := make([][]byte, 0, count*2)
		for i := uint64(0); i < count*2; i++ {
			s, r, ok := rdbReadString(rest)
			if !ok {
				return rdbValue{}, false
			}
			elems = append(elems, s)
			rest = r
		}
		return rdbValue{kind: keyHash, elems: elems}, true
	case rdbTypeHashListpack:
		lp, _, ok := rdbReadString(body[1:])
		if !ok {
			return rdbValue{}, false
		}
		elems, ok := lpDecode(lp)
		if !ok || len(elems)%2 != 0 {
			return rdbValue{}, false
		}
		return rdbValue{kind: keyHash, elems: elems}, true
	case rdbTypeSet:
		count, rest, ok := rdbReadLen(body[1:])
		if !ok {
			return rdbValue{}, false
		}
		elems := make([][]byte, 0, count)
		for i := uint64(0); i < count; i++ {
			s, r, ok := rdbReadString(rest)
			if !ok {
				return rdbValue{}, false
			}
			elems = append(elems, s)
			rest = r
		}
		return rdbValue{kind: keySet, elems: elems}, true
	case rdbTypeSetListpack:
		lp, _, ok := rdbReadString(body[1:])
		if !ok {
			return rdbValue{}, false
		}
		elems, ok := lpDecode(lp)
		if !ok {
			return rdbValue{}, false
		}
		return rdbValue{kind: keySet, elems: elems}, true
	case rdbTypeSetIntset:
		is, _, ok := rdbReadString(body[1:])
		if !ok {
			return rdbValue{}, false
		}
		elems, ok := intsetDecode(is)
		if !ok {
			return rdbValue{}, false
		}
		return rdbValue{kind: keySet, elems: elems}, true
	case rdbTypeList:
		return rdbLoadListPlain(body[1:])
	case rdbTypeListQuicklist2:
		return rdbLoadQuicklist2(body[1:])
	case rdbTypeZset2:
		return rdbLoadZset2(body[1:])
	case rdbTypeZset:
		return rdbLoadZsetOld(body[1:])
	case rdbTypeZsetListpack:
		lp, _, ok := rdbReadString(body[1:])
		if !ok {
			return rdbValue{}, false
		}
		flat, ok := lpDecode(lp)
		if !ok || len(flat)%2 != 0 {
			return rdbValue{}, false
		}
		members := make([][]byte, 0, len(flat)/2)
		scores := make([]float64, 0, len(flat)/2)
		for i := 0; i+1 < len(flat); i += 2 {
			f, err := strconv.ParseFloat(string(flat[i+1]), 64)
			if err != nil {
				return rdbValue{}, false
			}
			members = append(members, flat[i])
			scores = append(scores, f)
		}
		return rdbValue{kind: keyZset, elems: members, scores: scores}, true
	case rdbTypeStreamListpacks, rdbTypeStreamListpacks2, rdbTypeStreamListpacks3,
		rdbTypeStreamListpacks4, rdbTypeStreamListpacks5:
		return rdbLoadStream(body[1:], body[0])
	default:
		return rdbValue{}, false
	}
}

// rdbLoadStream parses any of the five stream RDB bodies (types 15, 19, 21, 26, 27) into an rdbStream.
// The newer types are supersets of the older ones, so a single parse gates the fields each form adds:
// the first-ID/max-deleted/entries-added and group entries-read of type 19 and up, the consumer
// active-time of type 21 and up, the per-group NACK zone of type 27, and the stream-level IDMP block
// of types 26 and 27. The NACK zone and IDMP block are consumed and discarded, since f1srv models
// neither an unowned pending entry nor an idempotent-producer table; every other field lands. A stream
// blob from Redis 8.8 (type 27) or Valkey 9.1 (type 21) therefore restores here.
func rdbLoadStream(b []byte, rdbtype byte) (rdbValue, bool) {
	hasV2 := rdbtype != rdbTypeStreamListpacks
	hasV3 := rdbtype >= rdbTypeStreamListpacks3
	hasV4 := rdbtype >= rdbTypeStreamListpacks4
	hasV5 := rdbtype >= rdbTypeStreamListpacks5

	st := &rdbStream{}
	nNodes, rest, ok := rdbReadLen(b)
	if !ok {
		return rdbValue{}, false
	}
	for i := uint64(0); i < nNodes; i++ {
		nodeKey, r, ok := rdbReadString(rest)
		if !ok || len(nodeKey) != 16 {
			return rdbValue{}, false
		}
		master := streamID{ms: binary.BigEndian.Uint64(nodeKey[0:8]), seq: binary.BigEndian.Uint64(nodeKey[8:16])}
		lp, r2, ok := rdbReadString(r)
		if !ok {
			return rdbValue{}, false
		}
		if !parseStreamNode(st, master, lp) {
			return rdbValue{}, false
		}
		rest = r2
	}

	// Stream metadata. length, then last ID; the first-ID/max-deleted/entries-added triple only from
	// type 19 up (an older blob defaults max-deleted to zero and entries-added to the live length).
	var length, ms, seq uint64
	if length, rest, ok = rdbReadLen(rest); !ok {
		return rdbValue{}, false
	}
	st.length = length
	if ms, rest, ok = rdbReadLen(rest); !ok {
		return rdbValue{}, false
	}
	if seq, rest, ok = rdbReadLen(rest); !ok {
		return rdbValue{}, false
	}
	st.lastID = streamID{ms: ms, seq: seq}
	if hasV2 {
		// first ID (read and dropped: f1srv derives it from the entry rows), then max-deleted, added.
		for j := 0; j < 2; j++ {
			if _, rest, ok = rdbReadLen(rest); !ok {
				return rdbValue{}, false
			}
		}
		if ms, rest, ok = rdbReadLen(rest); !ok {
			return rdbValue{}, false
		}
		if seq, rest, ok = rdbReadLen(rest); !ok {
			return rdbValue{}, false
		}
		st.maxDeleted = streamID{ms: ms, seq: seq}
		if st.entriesAdded, rest, ok = rdbReadLen(rest); !ok {
			return rdbValue{}, false
		}
	} else {
		st.entriesAdded = length
	}

	// Consumer groups.
	var nGroups uint64
	if nGroups, rest, ok = rdbReadLen(rest); !ok {
		return rdbValue{}, false
	}
	for gi := uint64(0); gi < nGroups; gi++ {
		var name []byte
		if name, rest, ok = rdbReadString(rest); !ok {
			return rdbValue{}, false
		}
		g := rdbStreamGroup{name: string(name)}
		if ms, rest, ok = rdbReadLen(rest); !ok {
			return rdbValue{}, false
		}
		if seq, rest, ok = rdbReadLen(rest); !ok {
			return rdbValue{}, false
		}
		g.lastID = streamID{ms: ms, seq: seq}
		if hasV2 {
			if g.entriesRead, rest, ok = rdbReadLen(rest); !ok {
				return rdbValue{}, false
			}
		} else {
			g.entriesRead = streamEntriesReadInvalid
		}

		// Global PEL: count then, per entry, raw 16-byte ID, 8-byte little-endian delivery time, and
		// the delivery count. The owner is left empty here and filled from the consumer PELs below.
		var nPel uint64
		if nPel, rest, ok = rdbReadLen(rest); !ok {
			return rdbValue{}, false
		}
		pelIndex := make(map[streamID]int, nPel)
		for pi := uint64(0); pi < nPel; pi++ {
			var id streamID
			if id, rest, ok = readRawStreamID(rest); !ok {
				return rdbValue{}, false
			}
			if len(rest) < 8 {
				return rdbValue{}, false
			}
			dt := int64(binary.LittleEndian.Uint64(rest[:8]))
			rest = rest[8:]
			var dc uint64
			if dc, rest, ok = rdbReadLen(rest); !ok {
				return rdbValue{}, false
			}
			pelIndex[id] = len(g.pel)
			g.pel = append(g.pel, rdbStreamPEL{id: id, deliveryTime: dt, deliveryCount: dc})
		}

		// Consumers: count then, per consumer, name, 8-byte seen time, (type 21 up) 8-byte active
		// time, and the consumer PEL of raw 16-byte IDs. Each ID resolves an owner into the global PEL.
		var nCons uint64
		if nCons, rest, ok = rdbReadLen(rest); !ok {
			return rdbValue{}, false
		}
		for ci := uint64(0); ci < nCons; ci++ {
			var cname []byte
			if cname, rest, ok = rdbReadString(rest); !ok {
				return rdbValue{}, false
			}
			if len(rest) < 8 {
				return rdbValue{}, false
			}
			seen := int64(binary.LittleEndian.Uint64(rest[:8]))
			rest = rest[8:]
			active := seen
			if hasV3 {
				if len(rest) < 8 {
					return rdbValue{}, false
				}
				active = int64(binary.LittleEndian.Uint64(rest[:8]))
				rest = rest[8:]
			}
			g.consumers = append(g.consumers, rdbStreamConsumer{name: string(cname), seenTime: seen, activeTime: active})
			var nCPel uint64
			if nCPel, rest, ok = rdbReadLen(rest); !ok {
				return rdbValue{}, false
			}
			for k := uint64(0); k < nCPel; k++ {
				var id streamID
				if id, rest, ok = readRawStreamID(rest); !ok {
					return rdbValue{}, false
				}
				if idx, has := pelIndex[id]; has {
					g.pel[idx].consumer = string(cname)
				}
			}
		}

		// Type 27's per-group NACK zone: a count then that many raw 16-byte IDs of unowned pending
		// entries. f1srv has no place for an owner-less pending entry, so the IDs are consumed and the
		// entries stay ownerless, which drops them on write; the common owned case is unaffected.
		if hasV5 {
			var nNacked uint64
			if nNacked, rest, ok = rdbReadLen(rest); !ok {
				return rdbValue{}, false
			}
			for k := uint64(0); k < nNacked; k++ {
				if _, rest, ok = readRawStreamID(rest); !ok {
					return rdbValue{}, false
				}
			}
		}
		st.groups = append(st.groups, g)
	}

	// Types 26 and 27 close with a stream-level IDMP block; f1srv does not model idempotent producers,
	// so the block is walked and discarded.
	if hasV4 {
		if _, ok = skipStreamIdmp(rest); !ok {
			return rdbValue{}, false
		}
	}
	return rdbValue{kind: keyStream, stream: st}, true
}

// readRawStreamID reads a 16-byte big-endian stream ID written in raw form (no length prefix), the
// shape a PEL or NACK-zone entry stores.
func readRawStreamID(b []byte) (streamID, []byte, bool) {
	if len(b) < 16 {
		return streamID{}, nil, false
	}
	return streamID{ms: binary.BigEndian.Uint64(b[0:8]), seq: binary.BigEndian.Uint64(b[8:16])}, b[16:], true
}

// skipStreamIdmp walks the IDMP (idempotent message producer) block that types 26 and 27 append:
// a duration, a max-entries cap, a producer count, and per producer a producer ID, an entry count,
// and that many (IID, ms, seq) triples. Every field is read so the cursor lands past the block, but
// nothing is kept, since f1srv does not track idempotent producers.
func skipStreamIdmp(b []byte) ([]byte, bool) {
	var ok bool
	// idmp_duration, idmp_max_entries.
	for i := 0; i < 2; i++ {
		if _, b, ok = rdbReadLen(b); !ok {
			return nil, false
		}
	}
	var nProducers uint64
	if nProducers, b, ok = rdbReadLen(b); !ok {
		return nil, false
	}
	for p := uint64(0); p < nProducers; p++ {
		if _, b, ok = rdbReadString(b); !ok { // producer ID
			return nil, false
		}
		var nEntries uint64
		if nEntries, b, ok = rdbReadLen(b); !ok {
			return nil, false
		}
		for e := uint64(0); e < nEntries; e++ {
			if _, b, ok = rdbReadString(b); !ok { // IID
				return nil, false
			}
			for j := 0; j < 2; j++ { // id ms, seq
				if _, b, ok = rdbReadLen(b); !ok {
					return nil, false
				}
			}
		}
	}
	return b, true
}

// parseStreamNode decodes one stream listpack node into st.entries. The node is a master entry (a
// live count, a tombstone count, the master field names, a terminator) followed by count+deleted
// items, each an entry whose ID is the node's master ID plus the item's ms and seq diffs. A SAMEFIELDS
// item carries only values and reuses the master fields; a full item carries its own field/value
// pairs. Deleted items are skipped. This is the inverse of buildStreamEntryListpack and also decodes
// the multi-item nodes a real server packs, so a Redis- or Valkey-produced node loads here.
func parseStreamNode(st *rdbStream, master streamID, lp []byte) bool {
	els, ok := lpDecode(lp)
	if !ok || len(els) < 4 {
		return false
	}
	p := 0
	next := func() ([]byte, bool) {
		if p >= len(els) {
			return nil, false
		}
		v := els[p]
		p++
		return v, true
	}
	nextInt := func() (int64, bool) {
		v, ok := next()
		if !ok {
			return 0, false
		}
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	}

	count, ok := nextInt()
	if !ok || count < 0 {
		return false
	}
	deleted, ok := nextInt()
	if !ok || deleted < 0 {
		return false
	}
	nFields, ok := nextInt()
	if !ok || nFields < 0 {
		return false
	}
	masterFields := make([][]byte, 0, nFields)
	for i := int64(0); i < nFields; i++ {
		f, ok := next()
		if !ok {
			return false
		}
		masterFields = append(masterFields, f)
	}
	// The master entry ends with a zero terminator element.
	if _, ok := next(); !ok {
		return false
	}

	total := count + deleted
	for i := int64(0); i < total; i++ {
		flags, ok := nextInt()
		if !ok {
			return false
		}
		msDiff, ok := nextInt()
		if !ok {
			return false
		}
		seqDiff, ok := nextInt()
		if !ok {
			return false
		}
		id := streamID{ms: master.ms + uint64(msDiff), seq: master.seq + uint64(seqDiff)}

		var fields [][]byte
		if flags&streamItemSameFields != 0 {
			fields = make([][]byte, 0, nFields*2)
			for j := int64(0); j < nFields; j++ {
				val, ok := next()
				if !ok {
					return false
				}
				fields = append(fields, masterFields[j], val)
			}
		} else {
			nf, ok := nextInt()
			if !ok || nf < 0 {
				return false
			}
			fields = make([][]byte, 0, nf*2)
			for j := int64(0); j < nf; j++ {
				f, ok := next()
				if !ok {
					return false
				}
				val, ok := next()
				if !ok {
					return false
				}
				fields = append(fields, f, val)
			}
		}
		// The trailing lp_count element closes the item; skip it.
		if _, ok := next(); !ok {
			return false
		}
		if flags&streamItemDeleted != 0 {
			continue
		}
		st.entries = append(st.entries, rdbStreamEntry{id: id, fields: fields})
	}
	return true
}

// rdbLoadListPlain parses the older RDB_TYPE_LIST body: an element count then that many element
// strings in order. A real server has not written this form for years, but it is trivial to accept and
// keeps a blob from an ancient dump loadable.
func rdbLoadListPlain(b []byte) (rdbValue, bool) {
	count, rest, ok := rdbReadLen(b)
	if !ok {
		return rdbValue{}, false
	}
	elems := make([][]byte, 0, count)
	for i := uint64(0); i < count; i++ {
		s, r, ok := rdbReadString(rest)
		if !ok {
			return rdbValue{}, false
		}
		elems = append(elems, s)
		rest = r
	}
	return rdbValue{kind: keyList, elems: elems}, true
}

// rdbLoadQuicklist2 parses the RDB_TYPE_LIST_QUICKLIST_2 body both Redis 8.8 and Valkey 9.1 emit: a
// node count then, per node, a container byte (1 PLAIN, one value stored as the node; 2 PACKED, a
// listpack of the node's elements) and the node body as an RDB string, which may itself be
// LZF-compressed. The elements flatten across nodes into a single ordered list, so a multi-node
// quicklist and aki's own one-node-per-element form both load to the same list.
func rdbLoadQuicklist2(b []byte) (rdbValue, bool) {
	nodes, rest, ok := rdbReadLen(b)
	if !ok {
		return rdbValue{}, false
	}
	var elems [][]byte
	for i := uint64(0); i < nodes; i++ {
		container, r, ok := rdbReadLen(rest)
		if !ok {
			return rdbValue{}, false
		}
		blob, r2, ok := rdbReadString(r)
		if !ok {
			return rdbValue{}, false
		}
		switch container {
		case quicklistNodePlain:
			elems = append(elems, blob)
		case quicklistNodePacked:
			part, ok := lpDecode(blob)
			if !ok {
				return rdbValue{}, false
			}
			elems = append(elems, part...)
		default:
			return rdbValue{}, false
		}
		rest = r2
	}
	return rdbValue{kind: keyList, elems: elems}, true
}

// rdbLoadZset2 parses the RDB_TYPE_ZSET_2 body: a member count then that many member strings, each
// followed by an 8-byte little-endian binary double score. This is the form aki dumps and the form
// both servers write for a large sorted set.
func rdbLoadZset2(b []byte) (rdbValue, bool) {
	count, rest, ok := rdbReadLen(b)
	if !ok {
		return rdbValue{}, false
	}
	members := make([][]byte, 0, count)
	scores := make([]float64, 0, count)
	for i := uint64(0); i < count; i++ {
		m, r, ok := rdbReadString(rest)
		if !ok {
			return rdbValue{}, false
		}
		if len(r) < 8 {
			return rdbValue{}, false
		}
		members = append(members, m)
		scores = append(scores, math.Float64frombits(binary.LittleEndian.Uint64(r[:8])))
		rest = r[8:]
	}
	return rdbValue{kind: keyZset, elems: members, scores: scores}, true
}

// rdbLoadZsetOld parses the older RDB_TYPE_ZSET body, where each score is a length-prefixed ASCII
// double with three special length bytes for the non-finite values: 255 is negative infinity, 254 is
// positive infinity, and 253 is NaN (which a sorted set never holds, so it is rejected).
func rdbLoadZsetOld(b []byte) (rdbValue, bool) {
	count, rest, ok := rdbReadLen(b)
	if !ok {
		return rdbValue{}, false
	}
	members := make([][]byte, 0, count)
	scores := make([]float64, 0, count)
	for i := uint64(0); i < count; i++ {
		m, r, ok := rdbReadString(rest)
		if !ok {
			return rdbValue{}, false
		}
		score, r2, ok := rdbReadDouble(r)
		if !ok {
			return rdbValue{}, false
		}
		members = append(members, m)
		scores = append(scores, score)
		rest = r2
	}
	return rdbValue{kind: keyZset, elems: members, scores: scores}, true
}

// rdbReadDouble decodes one old-format RDB double: a single length byte, with 255/254/253 reserving
// negative infinity, positive infinity, and NaN, and any other value naming that many ASCII bytes of
// a decimal float that follow. NaN is rejected because a sorted set score is never NaN.
func rdbReadDouble(b []byte) (float64, []byte, bool) {
	if len(b) < 1 {
		return 0, nil, false
	}
	n := b[0]
	switch n {
	case 255:
		return math.Inf(-1), b[1:], true
	case 254:
		return math.Inf(1), b[1:], true
	case 253:
		return 0, nil, false
	}
	if len(b) < 1+int(n) {
		return 0, nil, false
	}
	f, err := strconv.ParseFloat(string(b[1:1+int(n)]), 64)
	if err != nil {
		return 0, nil, false
	}
	return f, b[1+int(n):], true
}

// intsetDecode parses an intset blob into its members rendered as decimal text. An intset is a 4-byte
// encoding (the byte width of each entry: 2, 4, or 8), a 4-byte entry count, then that many signed
// integers of that width, everything little-endian and the entries in ascending order. It is the body
// both servers write for an all-integer set small enough to pack.
func intsetDecode(b []byte) ([][]byte, bool) {
	if len(b) < 8 {
		return nil, false
	}
	enc := binary.LittleEndian.Uint32(b[0:4])
	n := binary.LittleEndian.Uint32(b[4:8])
	if enc != 2 && enc != 4 && enc != 8 {
		return nil, false
	}
	entries := b[8:]
	if uint64(len(entries)) < uint64(n)*uint64(enc) {
		return nil, false
	}
	out := make([][]byte, 0, n)
	for i := uint32(0); i < n; i++ {
		off := int(i) * int(enc)
		var v int64
		switch enc {
		case 2:
			v = int64(int16(binary.LittleEndian.Uint16(entries[off:])))
		case 4:
			v = int64(int32(binary.LittleEndian.Uint32(entries[off:])))
		case 8:
			v = int64(binary.LittleEndian.Uint64(entries[off:]))
		}
		out = append(out, strconv.AppendInt(nil, v, 10))
	}
	return out, true
}

// rdbWriteValue lands a decoded value under a key. The caller holds the key's stripe lock and has
// already cleared any prior value, so this only has to insert. A string is a single Set; a hash writes
// each field/value pair through the same field-key primitives HSET uses, then stamps the O(1) count.
func (c *connState) rdbWriteValue(key []byte, v rdbValue) error {
	switch v.kind {
	case keyString:
		return c.srv.store.Set(key, v.str)
	case keyHash:
		for i := 0; i+1 < len(v.elems); i += 2 {
			fk := c.fieldKey(key, v.elems[i])
			isNew, err := c.srv.store.PutKind(fk, v.elems[i+1], kindHashField)
			if err != nil {
				return err
			}
			if isNew {
				c.srv.store.CollInsert(fk, kindHashField)
			}
		}
		return c.setHashCount(key, uint64(len(v.elems)/2))
	case keySet:
		var count uint64
		enc := encNone
		prefix := c.setPrefix(key)
		for _, m := range v.elems {
			mk := c.memberKey(key, m)
			isNew, err := c.srv.store.PutKind(mk, nil, kindSetMember)
			if err != nil {
				return err
			}
			if isNew {
				c.srv.store.CollInsert(mk, kindSetMember)
				// Route the member into the set's dense vector (spec 2064/18 5.1). RESTORE always
				// lands on an absent key (REPLACE drops the prior key first, which drops its vector),
				// so this no-ops until a later draw builds the vector, but wiring it keeps the add-site
				// list exhaustive. prefix is pbuf; mk is kbuf; the two never collide.
				c.srv.store.CollRandInsert(prefix, mk, kindSetMember)
				count++
				enc = foldSetEnc(enc, m, count)
			}
		}
		return c.setPutHeader(key, count, enc)
	case keyZset:
		enc := encNone
		for i, m := range v.elems {
			// A well-formed blob carries each member once, so every insert is a new member; write its
			// two rows and fold the encoding forward the way ZADD does. -0.0 is normalized to 0.0 to
			// match aki's own ingest, so a score round-trips to the same text ZSCORE would show.
			mk := c.zmemberKey(key, m)
			if err := c.zsetInsertNew(key, m, mk, normalizeZero(v.scores[i])); err != nil {
				return err
			}
			enc = foldZsetEnc(enc, m, uint64(i+1))
		}
		return c.zsetPutHeader(key, uint64(len(v.elems)), enc)
	case keyList:
		// Land the elements as a contiguous window [0, len) the way a fresh RPUSH run would, one
		// element-per-row point write apiece, then stamp the header with the running listpack byte
		// size and sticky large flag exactly as a push accumulates them, so OBJECT ENCODING on the
		// restored list matches what the same elements pushed one at a time would report.
		if len(v.elems) == 0 {
			return nil
		}
		lpBytes := uint64(listHeaderBytes)
		everLarge := false
		for i, e := range v.elems {
			ek := c.listElemKey(key, int64(i))
			if _, err := c.srv.store.PutKind(ek, e, kindListElem); err != nil {
				return err
			}
			lpBytes += uint64(listEntrySize(e))
			if !everLarge && lpBytes > listListpackMaxBytes {
				everLarge = true
			}
		}
		return c.listPutHeader(key, 0, int64(len(v.elems)), lpBytes, everLarge)
	case keyStream:
		return c.rdbWriteStream(key, v.stream)
	}
	return nil
}

// rdbWriteStream lands a decoded stream under a key: each entry as its own row, the header, then the
// consumer groups with their consumers and pending lists. It mirrors the write paths XADD, XGROUP, and
// XREADGROUP take, so a restored stream is indistinguishable from one built command by command. The
// group and consumer pending counts are derived from the pending rows actually written, so an unowned
// pending entry (a type 27 NACK-zone entry with no consumer) is dropped rather than landed ownerless.
func (c *connState) rdbWriteStream(key []byte, st *rdbStream) error {
	if st == nil {
		return nil
	}
	for _, e := range st.entries {
		ek := c.streamEntryKey(key, e.id)
		val := encodeStreamFields(nil, e.fields)
		isNew, err := c.srv.store.PutKind(ek, val, kindStreamEntry)
		if err != nil {
			return err
		}
		if isNew {
			c.srv.store.CollInsert(ek, kindStreamEntry)
		}
	}
	if err := c.streamPutHeader(key, st.length, st.lastID, st.maxDeleted, st.entriesAdded); err != nil {
		return err
	}
	for _, g := range st.groups {
		// Count each consumer's owned pending entries and the group total from the PEL rows that
		// actually carry an owner, so the stored counters match what lands below.
		perConsumer := make(map[string]uint64, len(g.consumers))
		var groupPending uint64
		for _, pe := range g.pel {
			if pe.consumer == "" {
				continue
			}
			perConsumer[pe.consumer]++
			groupPending++
		}
		if err := c.putStreamGroup(key, g.name, streamGroup{lastID: g.lastID, pending: groupPending, entriesRead: g.entriesRead}); err != nil {
			return err
		}
		for _, con := range g.consumers {
			sc := streamConsumer{seenTime: con.seenTime, activeTime: con.activeTime, pending: perConsumer[con.name]}
			if err := c.putStreamConsumer(key, g.name, con.name, sc); err != nil {
				return err
			}
		}
		for _, pe := range g.pel {
			if pe.consumer == "" {
				continue
			}
			row := streamPELEntry{consumer: pe.consumer, deliveryTime: pe.deliveryTime, deliveryCount: pe.deliveryCount}
			if err := c.putStreamPEL(key, g.name, pe.id, row); err != nil {
				return err
			}
		}
	}
	return nil
}

// lpDecode parses a listpack blob into its elements in order. A listpack is a 6-byte header (a 4-byte
// total length and a 2-byte element count, both little-endian), a run of entries, and a single 0xFF
// terminator. Each entry is an encoding byte or two, its data, and a back-length that a forward walk
// skips, so the walk only has to size each entry to reach the next. An integer entry is rendered to
// its decimal text, the shape a hash field or value takes once loaded.
func lpDecode(b []byte) ([][]byte, bool) {
	// Six header bytes and one terminator are the smallest possible listpack.
	if len(b) < 7 {
		return nil, false
	}
	p := 6
	var out [][]byte
	for p < len(b) {
		if b[p] == 0xFF {
			return out, true
		}
		val, n, ok := lpGet(b[p:])
		if !ok {
			return nil, false
		}
		out = append(out, val)
		p += n + lpBacklenSize(n)
	}
	return nil, false
}

// lpGet decodes one listpack entry from the front of b and returns its value, the number of bytes the
// encoding and data occupy (not counting the trailing back-length), and whether it decoded. The
// encodings follow the listpack format: a 7-bit small uint, 6-, 12-, and 32-bit string lengths, and
// 13-, 16-, 24-, 32-, and 64-bit signed integers, every one a hash field or value can take.
func lpGet(b []byte) ([]byte, int, bool) {
	if len(b) < 1 {
		return nil, 0, false
	}
	c := b[0]
	switch {
	case c&0x80 == 0: // 0xxxxxxx: 7-bit unsigned int
		return strconv.AppendInt(nil, int64(c&0x7f), 10), 1, true
	case c&0xC0 == 0x80: // 10xxxxxx: 6-bit string length
		n := int(c & 0x3f)
		if len(b) < 1+n {
			return nil, 0, false
		}
		return append([]byte(nil), b[1:1+n]...), 1 + n, true
	case c&0xE0 == 0xC0: // 110xxxxx yyyyyyyy: 13-bit signed int
		if len(b) < 2 {
			return nil, 0, false
		}
		v := int(c&0x1f)<<8 | int(b[1])
		if v >= 1<<12 {
			v -= 1 << 13
		}
		return strconv.AppendInt(nil, int64(v), 10), 2, true
	case c&0xF0 == 0xE0: // 1110xxxx yyyyyyyy: 12-bit string length
		if len(b) < 2 {
			return nil, 0, false
		}
		n := int(c&0x0f)<<8 | int(b[1])
		if len(b) < 2+n {
			return nil, 0, false
		}
		return append([]byte(nil), b[2:2+n]...), 2 + n, true
	case c == 0xF0: // 32-bit string length, little-endian
		if len(b) < 5 {
			return nil, 0, false
		}
		n := int(binary.LittleEndian.Uint32(b[1:5]))
		if n < 0 || len(b) < 5+n {
			return nil, 0, false
		}
		return append([]byte(nil), b[5:5+n]...), 5 + n, true
	case c == 0xF1: // 16-bit signed int, little-endian
		if len(b) < 3 {
			return nil, 0, false
		}
		return strconv.AppendInt(nil, int64(int16(binary.LittleEndian.Uint16(b[1:3]))), 10), 3, true
	case c == 0xF2: // 24-bit signed int, little-endian
		if len(b) < 4 {
			return nil, 0, false
		}
		u := uint32(b[1]) | uint32(b[2])<<8 | uint32(b[3])<<16
		v := int32(u<<8) >> 8 // sign-extend the 24-bit value
		return strconv.AppendInt(nil, int64(v), 10), 4, true
	case c == 0xF3: // 32-bit signed int, little-endian
		if len(b) < 5 {
			return nil, 0, false
		}
		return strconv.AppendInt(nil, int64(int32(binary.LittleEndian.Uint32(b[1:5]))), 10), 5, true
	case c == 0xF4: // 64-bit signed int, little-endian
		if len(b) < 9 {
			return nil, 0, false
		}
		return strconv.AppendInt(nil, int64(binary.LittleEndian.Uint64(b[1:9])), 10), 9, true
	}
	return nil, 0, false
}

// lpBacklenSize returns how many bytes the back-length field occupies for an entry whose encoding and
// data span l bytes. The listpack stores the back-length in 7-bit groups, so the count steps up at
// each power-of-128 boundary; the forward walk adds it to reach the next entry.
func lpBacklenSize(l int) int {
	switch {
	case l < 128:
		return 1
	case l < 16384:
		return 2
	case l < 2097152:
		return 3
	case l < 268435456:
		return 4
	default:
		return 5
	}
}

// rdbAppendString appends the RDB string encoding of val to dst. A short canonical integer is
// int-encoded exactly as Redis does, so the same value dumps to the same leading bytes on every
// engine; any other string is written length-prefixed and uncompressed.
func rdbAppendString(dst, val []byte) []byte {
	if enc, ok := rdbIntEncode(val); ok {
		return append(dst, enc...)
	}
	dst = rdbAppendLen(dst, uint64(len(val)))
	return append(dst, val...)
}

// rdbIntEncode returns the RDB int-encoding of val when val is the canonical decimal form of an
// integer that fits in 32 signed bits, the same test Redis applies before it int-encodes a string.
func rdbIntEncode(val []byte) ([]byte, bool) {
	if len(val) == 0 || len(val) > 11 {
		return nil, false
	}
	n, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return nil, false
	}
	// Only a canonical decimal round-trips; a value with leading zeros or a plus sign is stored
	// verbatim as a raw string so DUMP is reversible.
	if strconv.FormatInt(n, 10) != string(val) {
		return nil, false
	}
	switch {
	case n >= -128 && n <= 127:
		return []byte{0xC0, byte(int8(n))}, true
	case n >= -32768 && n <= 32767:
		var b [2]byte
		binary.LittleEndian.PutUint16(b[:], uint16(int16(n)))
		return []byte{0xC1, b[0], b[1]}, true
	case n >= -2147483648 && n <= 2147483647:
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(int32(n)))
		return []byte{0xC2, b[0], b[1], b[2], b[3]}, true
	}
	return nil, false
}

// rdbAppendLen appends an RDB length prefix: 6 bits for a small length, 14 bits for a medium one, or
// a marker byte plus a 32- or 64-bit big-endian length for a large one.
func rdbAppendLen(dst []byte, n uint64) []byte {
	switch {
	case n < 1<<6:
		return append(dst, byte(n))
	case n < 1<<14:
		return append(dst, byte(n>>8)|(1<<6), byte(n))
	case n <= 0xffffffff:
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(n))
		return append(append(dst, 0x80), b[:]...)
	default:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], n)
		return append(append(dst, 0x81), b[:]...)
	}
}

// rdbReadString decodes one RDB string from b and returns it, the remaining bytes, and whether the
// decode succeeded. It accepts every form a real server emits: the 6- and 14-bit and 32- and 64-bit
// length prefixes, the three int encodings, and the LZF-compressed form.
func rdbReadString(b []byte) ([]byte, []byte, bool) {
	if len(b) < 1 {
		return nil, nil, false
	}
	first := b[0]
	switch first >> 6 {
	case 0: // 6-bit length
		n := int(first & 0x3f)
		b = b[1:]
		if len(b) < n {
			return nil, nil, false
		}
		return b[:n], b[n:], true
	case 1: // 14-bit length
		if len(b) < 2 {
			return nil, nil, false
		}
		n := int(first&0x3f)<<8 | int(b[1])
		b = b[2:]
		if len(b) < n {
			return nil, nil, false
		}
		return b[:n], b[n:], true
	case 2: // 32- or 64-bit length
		switch first {
		case 0x80:
			if len(b) < 5 {
				return nil, nil, false
			}
			n := int(binary.BigEndian.Uint32(b[1:5]))
			b = b[5:]
			if n < 0 || len(b) < n {
				return nil, nil, false
			}
			return b[:n], b[n:], true
		case 0x81:
			if len(b) < 9 {
				return nil, nil, false
			}
			n := binary.BigEndian.Uint64(b[1:9])
			b = b[9:]
			if n > uint64(len(b)) {
				return nil, nil, false
			}
			return b[:n], b[n:], true
		default:
			return nil, nil, false
		}
	default: // 3: encoded value
		switch first & 0x3f {
		case rdbEncInt8:
			if len(b) < 2 {
				return nil, nil, false
			}
			return strconv.AppendInt(nil, int64(int8(b[1])), 10), b[2:], true
		case rdbEncInt16:
			if len(b) < 3 {
				return nil, nil, false
			}
			v := int64(int16(binary.LittleEndian.Uint16(b[1:3])))
			return strconv.AppendInt(nil, v, 10), b[3:], true
		case rdbEncInt32:
			if len(b) < 5 {
				return nil, nil, false
			}
			v := int64(int32(binary.LittleEndian.Uint32(b[1:5])))
			return strconv.AppendInt(nil, v, 10), b[5:], true
		case rdbEncLZF:
			return rdbReadLZF(b[1:])
		default:
			return nil, nil, false
		}
	}
}

// rdbReadLen decodes a plain RDB length (no int or LZF encoding), used for the compressed and
// uncompressed byte counts inside an LZF string.
func rdbReadLen(b []byte) (uint64, []byte, bool) {
	if len(b) < 1 {
		return 0, nil, false
	}
	first := b[0]
	switch first >> 6 {
	case 0:
		return uint64(first & 0x3f), b[1:], true
	case 1:
		if len(b) < 2 {
			return 0, nil, false
		}
		return uint64(first&0x3f)<<8 | uint64(b[1]), b[2:], true
	case 2:
		switch first {
		case 0x80:
			if len(b) < 5 {
				return 0, nil, false
			}
			return uint64(binary.BigEndian.Uint32(b[1:5])), b[5:], true
		case 0x81:
			if len(b) < 9 {
				return 0, nil, false
			}
			return binary.BigEndian.Uint64(b[1:9]), b[9:], true
		}
	}
	return 0, nil, false
}

// rdbReadLZF decodes the LZF-compressed string form: a compressed length, an uncompressed length,
// then the compressed bytes. Redis writes this form for a long compressible value, so decoding it is
// what lets a Redis- or Valkey-produced blob restore here even though aki's encoder never compresses.
func rdbReadLZF(b []byte) ([]byte, []byte, bool) {
	clen, b, ok := rdbReadLen(b)
	if !ok {
		return nil, nil, false
	}
	ulen, b, ok := rdbReadLen(b)
	if !ok {
		return nil, nil, false
	}
	if uint64(len(b)) < clen {
		return nil, nil, false
	}
	out, ok := lzfDecompress(b[:clen], int(ulen))
	if !ok {
		return nil, nil, false
	}
	return out, b[clen:], true
}

// lzfDecompress expands one liblzf-compressed block into exactly ulen bytes. A control byte below 32
// introduces a literal run of ctrl+1 bytes; a higher control byte introduces a back-reference of
// length+2 bytes to an earlier offset, copied byte by byte so an overlapping run repeats correctly.
func lzfDecompress(in []byte, ulen int) ([]byte, bool) {
	out := make([]byte, 0, ulen)
	for i := 0; i < len(in); {
		ctrl := int(in[i])
		i++
		if ctrl < 32 {
			n := ctrl + 1
			if i+n > len(in) {
				return nil, false
			}
			out = append(out, in[i:i+n]...)
			i += n
			continue
		}
		length := ctrl >> 5
		if length == 7 {
			if i >= len(in) {
				return nil, false
			}
			length += int(in[i])
			i++
		}
		if i >= len(in) {
			return nil, false
		}
		ref := len(out) - ((ctrl & 0x1f) << 8) - int(in[i]) - 1
		i++
		if ref < 0 {
			return nil, false
		}
		for k := 0; k < length+2; k++ {
			out = append(out, out[ref+k])
		}
	}
	if len(out) != ulen {
		return nil, false
	}
	return out, true
}
