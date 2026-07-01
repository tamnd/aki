package f1srv

import (
	"encoding/binary"
	"strconv"
	"strings"
	"time"
)

// Consumer groups are the stream's second layer (spec 2064/f1_rewrite_ltm/09). Where the entry log
// records what was written, a group records what each reader has seen and what it still owes an ack.
// Three row families hang off the same stream key as siblings of the entry family, each under its own
// family tag so the shared ordered index keeps them apart from the entries and from each other:
//
//   - group control row  ('g'): one per (stream, group), holding the group's last-delivered id, its
//     live pending count, and its entries-read counter. Keyed by uvarint(len(skey))|skey|'g'|group,
//     so a stream's groups enumerate under one prefix and XGROUP DESTROY / the DEL cascade find them.
//   - consumer row       ('c'): one per (stream, group, consumer), holding seen-time, active-time,
//     and the consumer's own pending count. Keyed by ...'c'|uvarint(len(group))|group|consumer, so a
//     group's consumers enumerate under one prefix.
//   - PEL row            ('p'): one per (stream, group, pending-id), the pending-entries list. Keyed
//     by ...'p'|uvarint(len(group))|group|BE16(id), value is the owning consumer plus delivery time
//     and count. One row per (group, id): a delivered-but-unacked entry has exactly one owner, so the
//     consumer lives in the value, and byte order equals id order so an explicit-id re-read is a
//     seek-then-walk. XACK is a point delete, DESTROY/DELCONSUMER are a prefix scan of point deletes,
//     none of which materialize the whole pending list.
//
// This is the same sibling-row model the other collection types use, so a group with a million
// pending entries costs XACK one row delete, not a pending-list decode.
const (
	kindStreamGroup    byte = 0x07 // a group control row: last-delivered id, pending count, entries-read
	kindStreamConsumer byte = 0x0D // a consumer row: seen-time, active-time, pending count
	kindStreamPEL      byte = 0x0E // a pending-entries-list row: owning consumer, delivery time and count
)

// Family tags placed after the length-prefixed stream key, one per sibling family, so each family
// sorts under its own prefix in the shared ordered index alongside the entry family's 'e'.
const (
	streamGroupTag    byte = 'g'
	streamConsumerTag byte = 'c'
	streamPELTag      byte = 'p'
)

// Fixed value widths for the control rows: a fixed width lets a counter bump rewrite the row in
// place. The group row is four little-endian u64 (last-id ms, last-id seq, pending, entries-read);
// the consumer row is three (seen-time, active-time, pending).
const (
	streamGroupBytes    = 32
	streamConsumerBytes = 24
)

// streamGroup is the in-memory image of a group control row.
type streamGroup struct {
	lastID      streamID
	pending     uint64
	entriesRead uint64
}

// streamConsumer is the in-memory image of a consumer row.
type streamConsumer struct {
	seenTime   int64
	activeTime int64
	pending    uint64
}

// streamPELEntry is the in-memory image of a PEL row: which consumer holds the pending id and when
// and how many times it was delivered.
type streamPELEntry struct {
	consumer      string
	deliveryTime  int64
	deliveryCount uint64
}

// nowMillis is the wall clock in milliseconds, the delivery and seen/active timestamps use it.
func nowMillis() int64 { return time.Now().UnixMilli() }

// streamNoGroup formats the NOGROUP reply for a missing group on a key, verbatim to Redis and Valkey.
func streamNoGroup(group, key string) string {
	return "NOGROUP No such consumer group '" + group + "' for key name '" + key + "'"
}

// streamReadGroupNoGroup is the NOGROUP reply XREADGROUP gives for a missing key or group, which
// worded differently from the XGROUP/XACK form.
func streamReadGroupNoGroup(key, group string) string {
	return "NOGROUP No such key '" + key + "' or consumer group '" + group + "' in XREADGROUP with GROUP option"
}

// xgroupArgErr is the arity error for an XGROUP subcommand, naming the fully qualified subcommand the
// way Redis does ("xgroup|create" and so on).
func xgroupArgErr(sub string) string {
	return "ERR wrong number of arguments for 'xgroup|" + sub + "' command"
}

// xgroupSubErr is the reply for a bad option inside an otherwise well-formed XGROUP subcommand call.
// Redis echoes the subcommand token verbatim, so a lowercase "create" stays lowercase.
func xgroupSubErr(verbatimSub string) string {
	return "ERR unknown subcommand or wrong number of arguments for '" + verbatimSub + "'. Try XGROUP HELP."
}

// streamErrEntriesRead is the fixed reply for an ENTRIESREAD value below -1.
const streamErrEntriesRead = "ERR value for ENTRIESREAD must be positive or -1"

// parseEntriesRead reads an ENTRIESREAD value. Redis accepts any integer >= -1 (with -1 meaning the
// count is unknown), rejects a smaller value with a fixed message, and rejects a non-integer with the
// generic not-an-integer message. It returns the value clamped to a non-negative counter (unknown -1
// becomes 0, which is all this slice records) and whether it was set.
func parseEntriesRead(raw []byte) (uint64, string, bool) {
	v, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, errStreamNotInt, false
	}
	if v < -1 {
		return 0, streamErrEntriesRead, false
	}
	if v < 0 {
		return 0, "", true
	}
	return uint64(v), "", true
}

// --- key builders (spec section 9). These allocate fresh buffers rather than share the connection's
// kbuf/pbuf scratch, so a group command can build a group, consumer, and PEL key at once and hold
// them across an entry-family scan without the two clobbering each other. Groups are cold relative to
// XADD/XRANGE, so the allocation is not on any measured hot path. ---

func streamAppendUvarint(dst []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(dst, tmp[:n]...)
}

// streamFamilyPrefix builds uvarint(len(skey)) | skey | tag, the enumeration prefix a whole family
// of a stream sorts under.
func streamFamilyPrefix(skey []byte, tag byte) []byte {
	b := streamAppendUvarint(nil, uint64(len(skey)))
	b = append(b, skey...)
	return append(b, tag)
}

func streamGroupKey(skey []byte, group string) []byte {
	return append(streamFamilyPrefix(skey, streamGroupTag), group...)
}

func streamConsumerPrefix(skey []byte, group string) []byte {
	b := streamFamilyPrefix(skey, streamConsumerTag)
	b = streamAppendUvarint(b, uint64(len(group)))
	return append(b, group...)
}

func streamConsumerKey(skey []byte, group, consumer string) []byte {
	return append(streamConsumerPrefix(skey, group), consumer...)
}

func streamPELPrefix(skey []byte, group string) []byte {
	b := streamFamilyPrefix(skey, streamPELTag)
	b = streamAppendUvarint(b, uint64(len(group)))
	return append(b, group...)
}

func streamPELKey(skey []byte, group string, id streamID) []byte {
	b := streamPELPrefix(skey, group)
	var idb [16]byte
	putStreamID(idb[:], id)
	return append(b, idb[:]...)
}

// --- group control row ---

func (c *connState) getStreamGroup(skey []byte, group string) (streamGroup, bool) {
	var buf [streamGroupBytes]byte
	v, ok := c.srv.store.GetKind(streamGroupKey(skey, group), buf[:0], kindStreamGroup)
	if !ok || len(v) < streamGroupBytes {
		return streamGroup{}, false
	}
	return streamGroup{
		lastID:      streamID{ms: binary.LittleEndian.Uint64(v[0:8]), seq: binary.LittleEndian.Uint64(v[8:16])},
		pending:     binary.LittleEndian.Uint64(v[16:24]),
		entriesRead: binary.LittleEndian.Uint64(v[24:32]),
	}, true
}

func (c *connState) putStreamGroup(skey []byte, group string, g streamGroup) error {
	var buf [streamGroupBytes]byte
	binary.LittleEndian.PutUint64(buf[0:8], g.lastID.ms)
	binary.LittleEndian.PutUint64(buf[8:16], g.lastID.seq)
	binary.LittleEndian.PutUint64(buf[16:24], g.pending)
	binary.LittleEndian.PutUint64(buf[24:32], g.entriesRead)
	key := streamGroupKey(skey, group)
	created, err := c.srv.store.PutKind(key, buf[:], kindStreamGroup)
	if err != nil {
		return err
	}
	if created {
		c.srv.store.CollInsert(key, kindStreamGroup)
	}
	return nil
}

func (c *connState) deleteStreamGroup(skey []byte, group string) {
	key := streamGroupKey(skey, group)
	c.srv.store.DeleteKind(key, kindStreamGroup)
	c.srv.store.CollRemove(key)
}

// --- consumer row ---

func (c *connState) getStreamConsumer(skey []byte, group, name string) (streamConsumer, bool) {
	var buf [streamConsumerBytes]byte
	v, ok := c.srv.store.GetKind(streamConsumerKey(skey, group, name), buf[:0], kindStreamConsumer)
	if !ok || len(v) < streamConsumerBytes {
		return streamConsumer{}, false
	}
	return streamConsumer{
		seenTime:   int64(binary.LittleEndian.Uint64(v[0:8])),
		activeTime: int64(binary.LittleEndian.Uint64(v[8:16])),
		pending:    binary.LittleEndian.Uint64(v[16:24]),
	}, true
}

func (c *connState) putStreamConsumer(skey []byte, group, name string, con streamConsumer) error {
	var buf [streamConsumerBytes]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(con.seenTime))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(con.activeTime))
	binary.LittleEndian.PutUint64(buf[16:24], con.pending)
	key := streamConsumerKey(skey, group, name)
	created, err := c.srv.store.PutKind(key, buf[:], kindStreamConsumer)
	if err != nil {
		return err
	}
	if created {
		c.srv.store.CollInsert(key, kindStreamConsumer)
	}
	return nil
}

// --- PEL row ---

// encodeStreamPEL packs a PEL value: uvarint(len(consumer)) | consumer | uvarint(deliveryTime) |
// uvarint(deliveryCount). The consumer name varies in length, so the PEL value is not fixed width.
func encodeStreamPEL(dst []byte, pe streamPELEntry) []byte {
	dst = streamAppendUvarint(dst, uint64(len(pe.consumer)))
	dst = append(dst, pe.consumer...)
	dst = streamAppendUvarint(dst, uint64(pe.deliveryTime))
	dst = streamAppendUvarint(dst, pe.deliveryCount)
	return dst
}

// decodeStreamPEL is the inverse. It copies the consumer name out of the value bytes so the returned
// entry stays valid after the value buffer is reused.
func decodeStreamPEL(v []byte) streamPELEntry {
	l, off := binary.Uvarint(v)
	consumer := string(v[off : off+int(l)])
	off += int(l)
	dt, m := binary.Uvarint(v[off:])
	off += m
	dc, _ := binary.Uvarint(v[off:])
	return streamPELEntry{consumer: consumer, deliveryTime: int64(dt), deliveryCount: dc}
}

func (c *connState) getStreamPEL(skey []byte, group string, id streamID) (streamPELEntry, bool) {
	v, ok := c.srv.store.GetKind(streamPELKey(skey, group, id), nil, kindStreamPEL)
	if !ok {
		return streamPELEntry{}, false
	}
	return decodeStreamPEL(v), true
}

func (c *connState) putStreamPEL(skey []byte, group string, id streamID, pe streamPELEntry) error {
	key := streamPELKey(skey, group, id)
	created, err := c.srv.store.PutKind(key, encodeStreamPEL(nil, pe), kindStreamPEL)
	if err != nil {
		return err
	}
	if created {
		c.srv.store.CollInsert(key, kindStreamPEL)
	}
	return nil
}

func (c *connState) deleteStreamPEL(skey []byte, group string, id streamID) bool {
	key := streamPELKey(skey, group, id)
	ok := c.srv.store.DeleteKind(key, kindStreamPEL)
	if ok {
		c.srv.store.CollRemove(key)
	}
	return ok
}

// dropStreamGroups removes every group control, consumer, and PEL row of a stream, the group half of
// the DEL/UNLINK cascade. Each family sorts under one prefix, so this is three bounded prefix drops.
func (c *connState) dropStreamGroups(skey []byte) {
	c.dropCollIndex(streamFamilyPrefix(skey, streamGroupTag), kindStreamGroup)
	c.dropCollIndex(streamFamilyPrefix(skey, streamConsumerTag), kindStreamConsumer)
	c.dropCollIndex(streamFamilyPrefix(skey, streamPELTag), kindStreamPEL)
}

// --- XGROUP ---

func (c *connState) cmdXGroup(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'xgroup' command")
		return
	}
	switch strings.ToUpper(string(argv[1])) {
	case "CREATE":
		c.xgroupCreate(argv)
	case "SETID":
		c.xgroupSetID(argv)
	case "CREATECONSUMER":
		c.xgroupConsumer(argv, true)
	case "DELCONSUMER":
		c.xgroupConsumer(argv, false)
	case "DESTROY":
		c.xgroupDestroy(argv)
	case "HELP":
		c.xgroupHelp()
	default:
		c.writeErr("ERR unknown subcommand '" + string(argv[1]) + "'. Try XGROUP HELP.")
	}
}

// xgroupHelpLines is the XGROUP HELP reply, verbatim to Redis and Valkey so a client that prints it
// sees the same text.
var xgroupHelpLines = []string{
	"XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"CREATE <key> <groupname> <id|$> [option]",
	"    Create a new consumer group. Options are:",
	"    * MKSTREAM",
	"      Create the empty stream if it does not exist.",
	"    * ENTRIESREAD entries_read",
	"      Set the group's entries_read counter (internal use).",
	"CREATECONSUMER <key> <groupname> <consumer>",
	"    Create a new consumer in the specified group.",
	"DELCONSUMER <key> <groupname> <consumer>",
	"    Remove the specified consumer.",
	"DESTROY <key> <groupname>",
	"    Remove the specified group.",
	"SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
	"    Set the current group ID and entries_read counter.",
	"HELP",
	"    Print this help.",
}

func (c *connState) xgroupHelp() {
	c.writeArrayHeader(len(xgroupHelpLines))
	for _, line := range xgroupHelpLines {
		c.writeSimple(line)
	}
}

// xgroupCreate implements XGROUP CREATE key group id|$ [MKSTREAM] [ENTRIESREAD n]. MKSTREAM creates
// an empty stream when the key is absent; without it a missing key is an error. $ resolves to the
// stream's current last id, and pairing $ with ENTRIESREAD is rejected the way Redis rejects it.
func (c *connState) xgroupCreate(argv [][]byte) {
	if len(argv) < 5 {
		c.writeErr(xgroupArgErr("create"))
		return
	}
	skey := argv[2]
	groupName := string(argv[3])
	rawID := string(argv[4])
	mkStream := false
	var entriesRead uint64
	for i := 5; i < len(argv); {
		switch strings.ToUpper(string(argv[i])) {
		case "MKSTREAM":
			mkStream = true
			i++
		case "ENTRIESREAD":
			if i+1 >= len(argv) {
				c.writeErr(xgroupSubErr(string(argv[1])))
				return
			}
			v, msg, ok := parseEntriesRead(argv[i+1])
			if !ok {
				c.writeErr(msg)
				return
			}
			entriesRead = v
			i += 2
		default:
			c.writeErr(xgroupSubErr(string(argv[1])))
			return
		}
	}

	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	_, lastID, _, _, exists := c.streamHeader(skey)
	if !exists {
		if !mkStream {
			mu.Unlock()
			c.writeErr("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
			return
		}
		if err := c.streamPutHeader(skey, 0, streamID{}, streamID{}, 0); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
		lastID = streamID{}
	}
	if _, ok := c.getStreamGroup(skey, groupName); ok {
		mu.Unlock()
		c.writeErr("BUSYGROUP Consumer Group name already exists")
		return
	}
	id := lastID
	if rawID != "$" {
		pid, ok := parseStreamID(rawID, 0)
		if !ok {
			mu.Unlock()
			c.writeErr(errStreamInvalidID)
			return
		}
		id = pid
	}
	if err := c.putStreamGroup(skey, groupName, streamGroup{lastID: id, entriesRead: entriesRead}); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	mu.Unlock()
	c.writeSimple("OK")
}

// xgroupSetID implements XGROUP SETID key group id|$ [ENTRIESREAD n]. It moves an existing group's
// last-delivered id (so the next > read redelivers from there) without touching any PEL row. A
// missing key or group is NOGROUP.
func (c *connState) xgroupSetID(argv [][]byte) {
	if len(argv) < 5 {
		c.writeErr(xgroupArgErr("setid"))
		return
	}
	skey := argv[2]
	groupName := string(argv[3])
	rawID := string(argv[4])
	setEntriesRead := false
	var entriesRead uint64
	for i := 5; i < len(argv); {
		if strings.EqualFold(string(argv[i]), "ENTRIESREAD") && i+1 < len(argv) {
			v, msg, ok := parseEntriesRead(argv[i+1])
			if !ok {
				c.writeErr(msg)
				return
			}
			entriesRead = v
			setEntriesRead = true
			i += 2
			continue
		}
		c.writeErr(xgroupSubErr(string(argv[1])))
		return
	}

	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	_, lastID, _, _, exists := c.streamHeader(skey)
	if !exists {
		mu.Unlock()
		c.writeErr(streamNoGroup(groupName, string(skey)))
		return
	}
	g, ok := c.getStreamGroup(skey, groupName)
	if !ok {
		mu.Unlock()
		c.writeErr(streamNoGroup(groupName, string(skey)))
		return
	}
	id := lastID
	if rawID != "$" {
		pid, okid := parseStreamID(rawID, 0)
		if !okid {
			mu.Unlock()
			c.writeErr(errStreamInvalidID)
			return
		}
		id = pid
	}
	g.lastID = id
	if setEntriesRead {
		g.entriesRead = entriesRead
	}
	if err := c.putStreamGroup(skey, groupName, g); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	mu.Unlock()
	c.writeSimple("OK")
}

// xgroupConsumer implements XGROUP CREATECONSUMER (create=true) and DELCONSUMER (create=false).
// CREATECONSUMER replies 1 if it made a new consumer, 0 if it already existed. DELCONSUMER drops the
// consumer and every PEL row it owned, replying the number of pending entries removed.
func (c *connState) xgroupConsumer(argv [][]byte, create bool) {
	if len(argv) != 5 {
		if create {
			c.writeErr(xgroupArgErr("createconsumer"))
		} else {
			c.writeErr(xgroupArgErr("delconsumer"))
		}
		return
	}
	skey := argv[2]
	groupName := string(argv[3])
	consumerName := string(argv[4])
	now := nowMillis()

	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	_, _, _, _, exists := c.streamHeader(skey)
	if !exists {
		mu.Unlock()
		c.writeErr(streamNoGroup(groupName, string(skey)))
		return
	}
	g, ok := c.getStreamGroup(skey, groupName)
	if !ok {
		mu.Unlock()
		c.writeErr(streamNoGroup(groupName, string(skey)))
		return
	}

	if create {
		if _, has := c.getStreamConsumer(skey, groupName, consumerName); has {
			mu.Unlock()
			c.writeInt(0)
			return
		}
		if err := c.putStreamConsumer(skey, groupName, consumerName, streamConsumer{seenTime: now, activeTime: now}); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
		mu.Unlock()
		c.writeInt(1)
		return
	}

	// DELCONSUMER: purge the consumer's PEL rows (adjusting the group pending count), then drop the
	// consumer row. A consumer that never existed simply had no rows, so the reply is 0.
	removed := c.purgeConsumerPEL(skey, groupName, consumerName, &g)
	c.srv.store.DeleteKind(streamConsumerKey(skey, groupName, consumerName), kindStreamConsumer)
	c.srv.store.CollRemove(streamConsumerKey(skey, groupName, consumerName))
	if removed > 0 {
		if err := c.putStreamGroup(skey, groupName, g); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	mu.Unlock()
	c.writeInt(removed)
}

// purgeConsumerPEL point-deletes every PEL row of the group owned by the named consumer, decrementing
// the group pending count per removed row, and returns the number removed. It scans the group's PEL
// prefix in bounded batches so a consumer with a huge pending list drops in bounded rounds.
func (c *connState) purgeConsumerPEL(skey []byte, group, consumer string, g *streamGroup) int64 {
	prefix := streamPELPrefix(skey, group)
	var removed int64
	var after, buf []byte
	for {
		keys, last := c.srv.store.CollScan(prefix, after, streamTrimBatch, nil)
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			v, ok := c.srv.store.GetKind(k, buf[:0], kindStreamPEL)
			if !ok {
				continue
			}
			buf = v
			if decodeStreamPEL(v).consumer != consumer {
				continue
			}
			c.srv.store.DeleteKind(k, kindStreamPEL)
			c.srv.store.CollRemove(k)
			removed++
			if g.pending > 0 {
				g.pending--
			}
		}
		after = last
	}
	return removed
}

// xgroupDestroy implements XGROUP DESTROY key group. It drops the group control row and every
// consumer and PEL row of that one group, leaving the entry log and the other groups untouched. It
// replies 1 if the group existed, 0 otherwise.
func (c *connState) xgroupDestroy(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr(xgroupArgErr("destroy"))
		return
	}
	skey := argv[2]
	groupName := string(argv[3])

	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	_, _, _, _, exists := c.streamHeader(skey)
	if !exists {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	if _, ok := c.getStreamGroup(skey, groupName); !ok {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	c.dropCollIndex(streamPELPrefix(skey, groupName), kindStreamPEL)
	c.dropCollIndex(streamConsumerPrefix(skey, groupName), kindStreamConsumer)
	c.deleteStreamGroup(skey, groupName)
	mu.Unlock()
	c.writeInt(1)
}

// --- XACK ---

// cmdXAck implements XACK key group id [id ...]. Each named id is a point delete of its PEL row: the
// row leaves the pending list, the group and owning consumer pending counts drop, and the owner's
// active-time advances. An id that is not pending is skipped and not counted. The reply is the number
// of ids actually acknowledged. A missing key or group is NOGROUP.
func (c *connState) cmdXAck(argv [][]byte) {
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for 'xack' command")
		return
	}
	skey := argv[1]
	groupName := string(argv[2])
	ids := make([]streamID, 0, len(argv)-3)
	for _, raw := range argv[3:] {
		id, ok := parseStreamID(string(raw), 0)
		if !ok {
			c.writeErr(errStreamInvalidID)
			return
		}
		ids = append(ids, id)
	}
	now := nowMillis()

	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	// A missing key or group is not an error for XACK: there is simply nothing pending to
	// acknowledge, so the reply is 0, matching Redis and Valkey.
	_, _, _, _, exists := c.streamHeader(skey)
	if !exists {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	g, ok := c.getStreamGroup(skey, groupName)
	if !ok {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	var acked int64
	for _, id := range ids {
		pe, has := c.getStreamPEL(skey, groupName, id)
		if !has {
			continue
		}
		c.deleteStreamPEL(skey, groupName, id)
		if g.pending > 0 {
			g.pending--
		}
		if con, cok := c.getStreamConsumer(skey, groupName, pe.consumer); cok {
			con.activeTime = now
			if con.pending > 0 {
				con.pending--
			}
			if err := c.putStreamConsumer(skey, groupName, pe.consumer, con); err != nil {
				mu.Unlock()
				c.writeErr("ERR " + err.Error())
				return
			}
		}
		acked++
	}
	if acked > 0 {
		if err := c.putStreamGroup(skey, groupName, g); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	mu.Unlock()
	c.writeInt(acked)
}

// --- XPENDING ---

// streamPendingNoGroup is the NOGROUP reply XPENDING gives for a missing key or group. It names the
// key then the group and carries no trailing clause, so it is worded differently from both the
// XGROUP/XACK form and the XREADGROUP form.
func streamPendingNoGroup(key, group string) string {
	return "NOGROUP No such key '" + key + "' or consumer group '" + group + "'"
}

// streamIDDec returns the id immediately below id and true, or false when id is 0-0 (no predecessor).
// It lets an inclusive lower bound reuse the strictly-after CollScan: scanning past the predecessor
// key yields the first row at or above the bound, since ids are adjacent 128-bit integers with no id
// strictly between a value and its predecessor.
func streamIDDec(id streamID) (streamID, bool) {
	if id.seq > 0 {
		return streamID{ms: id.ms, seq: id.seq - 1}, true
	}
	if id.ms > 0 {
		return streamID{ms: id.ms - 1, seq: ^uint64(0)}, true
	}
	return streamID{}, false
}

// cmdXPending implements both XPENDING forms. The summary form (XPENDING key group) reports the
// group's total pending count, the low and high pending ids, and a per-consumer breakdown, all off
// the group control row and the sibling rows without decoding the whole pending list. The extended
// form (XPENDING key group [IDLE ms] start end count [consumer]) streams the pending rows in id order
// across the [start, end] window, up to count, filtered by idle time and consumer, so its cost tracks
// the requested window, not the pending-list size.
func (c *connState) cmdXPending(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'xpending' command")
		return
	}
	skey := argv[1]
	groupName := string(argv[2])

	// Summary form is exactly key + group; any other argument count is either the extended form
	// (6..9 args) or a syntax error. Redis validates the shape before touching the key, so a bad
	// shape reports "syntax error" even when the key or group is missing.
	if len(argv) == 3 {
		c.xpendingSummary(skey, groupName)
		return
	}
	if len(argv) < 6 || len(argv) > 9 {
		c.writeErr("ERR syntax error")
		return
	}

	// Extended form. Redis parses every option (IDLE, count, start, end) before it looks up the key
	// or group, so a non-integer or bad-id argument reports its own error ahead of NOGROUP. The
	// optional leading IDLE keyword shifts the start/end/count triple two positions right.
	startIdx := 3
	var minIdle int64
	if strings.EqualFold(string(argv[3]), "IDLE") {
		v, err := strconv.ParseInt(string(argv[4]), 10, 64)
		if err != nil {
			c.writeErr(errStreamNotInt)
			return
		}
		if len(argv) < 8 {
			c.writeErr("ERR syntax error")
			return
		}
		minIdle = v
		startIdx = 5
	}
	// count is parsed before start/end, matching Redis: XPENDING key group FOO 0 - + 10 reports the
	// count error on the "-" at the count position, not a bad-id error on "FOO".
	count, err := strconv.ParseInt(string(argv[startIdx+2]), 10, 64)
	if err != nil {
		c.writeErr(errStreamNotInt)
		return
	}
	start, startExcl, ok1 := parseRangeBound(string(argv[startIdx]), 0)
	end, endExcl, ok2 := parseRangeBound(string(argv[startIdx+1]), ^uint64(0))
	if !ok1 || !ok2 {
		c.writeErr(errStreamInvalidID)
		return
	}
	consumerFilter := ""
	hasConsumerFilter := false
	if startIdx+3 < len(argv) {
		consumerFilter = string(argv[startIdx+3])
		hasConsumerFilter = true
	}
	c.xpendingExtended(skey, groupName, minIdle, start, startExcl, end, endExcl, count, consumerFilter, hasConsumerFilter)
}

// xpendingSummary answers the summary form. It reads the group's total pending count from the control
// row, the low and high ids from the first and last PEL rows by positional select, and the
// per-consumer breakdown by scanning the consumer rows (bounded by the consumer count, not the
// pending-list size) and reporting each consumer that still owes an ack.
func (c *connState) xpendingSummary(skey []byte, group string) {
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	_, _, _, _, exists := c.streamHeader(skey)
	if !exists {
		mu.Unlock()
		c.writeErr(streamPendingNoGroup(string(skey), group))
		return
	}
	g, ok := c.getStreamGroup(skey, group)
	if !ok {
		mu.Unlock()
		c.writeErr(streamPendingNoGroup(string(skey), group))
		return
	}

	// An empty pending list replies count 0, null low, null high, and a null consumer array.
	if g.pending == 0 {
		mu.Unlock()
		c.writeArrayHeader(4)
		c.writeInt(0)
		c.writeNil()
		c.writeNil()
		c.writeNilArray()
		return
	}

	pelPrefix := streamPELPrefix(skey, group)
	minKeys, _ := c.srv.store.CollScan(pelPrefix, nil, 1, nil)
	maxKey, maxOK := c.srv.store.CollSelectAt(pelPrefix, int(g.pending)-1)

	// Per-consumer breakdown, in consumer-name order (the order the consumer rows sort in), skipping
	// any consumer with nothing pending.
	type consumerCount struct {
		name    string
		pending uint64
	}
	var breakdown []consumerCount
	consPrefix := streamConsumerPrefix(skey, group)
	var after, cbuf []byte
	for {
		keys, last := c.srv.store.CollScan(consPrefix, after, streamTrimBatch, nil)
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			v, got := c.srv.store.GetKind(k, cbuf[:0], kindStreamConsumer)
			if !got || len(v) < streamConsumerBytes {
				continue
			}
			cbuf = v
			p := binary.LittleEndian.Uint64(v[16:24])
			if p == 0 {
				continue
			}
			breakdown = append(breakdown, consumerCount{name: string(k[len(consPrefix):]), pending: p})
		}
		after = last
	}
	mu.Unlock()

	c.writeArrayHeader(4)
	c.writeInt(int64(g.pending))
	if len(minKeys) > 0 {
		c.writeBulk([]byte(decodeEntryID(minKeys[0]).String()))
	} else {
		c.writeNil()
	}
	if maxOK {
		c.writeBulk([]byte(decodeEntryID(maxKey).String()))
	} else {
		c.writeNil()
	}
	if len(breakdown) == 0 {
		c.writeNilArray()
		return
	}
	c.writeArrayHeader(len(breakdown))
	for _, b := range breakdown {
		c.writeArrayHeader(2)
		c.writeBulk([]byte(b.name))
		// The per-consumer count is a bulk string in the summary reply, not an integer.
		c.writeBulk([]byte(strconv.FormatUint(b.pending, 10)))
	}
}

// xpendingExtended answers the extended form. It walks the group's PEL rows in id order across the
// requested [start, end] window, emitting one [id, consumer, idle-ms, delivery-count] row per pending
// entry that passes the idle-time and consumer filters, up to count rows. A non-positive count yields
// an empty reply. The walk seeks to the start bound and stops at the end bound, so it touches the
// window, not the whole pending list.
func (c *connState) xpendingExtended(skey []byte, group string, minIdle int64, start streamID, startExcl bool, end streamID, endExcl bool, count int64, consumerFilter string, hasConsumerFilter bool) {
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	_, _, _, _, exists := c.streamHeader(skey)
	if !exists {
		mu.Unlock()
		c.writeErr(streamPendingNoGroup(string(skey), group))
		return
	}
	if _, ok := c.getStreamGroup(skey, group); !ok {
		mu.Unlock()
		c.writeErr(streamPendingNoGroup(string(skey), group))
		return
	}

	type pendingRow struct {
		id            streamID
		consumer      string
		idle          int64
		deliveryCount uint64
	}
	var rows []pendingRow
	if count > 0 {
		now := nowMillis()
		prefix := streamPELPrefix(skey, group)
		// Seek to the start bound. An exclusive start scans past the start key itself; an inclusive
		// start scans past the predecessor so the start row is included, or from the prefix front
		// when the start is 0-0 and has no predecessor.
		var scanAfter []byte
		if startExcl {
			scanAfter = streamPELKey(skey, group, start)
		} else if pred, ok := streamIDDec(start); ok {
			scanAfter = streamPELKey(skey, group, pred)
		}
		var buf []byte
		done := false
		for !done {
			pkeys, last := c.srv.store.CollScan(prefix, scanAfter, streamTrimBatch, nil)
			if len(pkeys) == 0 {
				break
			}
			for _, k := range pkeys {
				id := decodeEntryID(k)
				// Stop at the end bound: past end for an inclusive end, at or past end for exclusive.
				if endExcl {
					if !id.less(end) {
						done = true
						break
					}
				} else if end.less(id) {
					done = true
					break
				}
				v, got := c.srv.store.GetKind(k, buf[:0], kindStreamPEL)
				if !got {
					continue
				}
				buf = v
				pe := decodeStreamPEL(v)
				if hasConsumerFilter && pe.consumer != consumerFilter {
					continue
				}
				idle := now - pe.deliveryTime
				if idle < minIdle {
					continue
				}
				rows = append(rows, pendingRow{id: id, consumer: pe.consumer, idle: idle, deliveryCount: pe.deliveryCount})
				if int64(len(rows)) >= count {
					done = true
					break
				}
			}
			scanAfter = last
		}
	}
	mu.Unlock()

	c.writeArrayHeader(len(rows))
	for _, r := range rows {
		c.writeArrayHeader(4)
		c.writeBulk([]byte(r.id.String()))
		c.writeBulk([]byte(r.consumer))
		c.writeInt(r.idle)
		c.writeInt(int64(r.deliveryCount))
	}
}

// --- XCLAIM / XAUTOCLAIM ---

// claimRow is one claimed entry gathered for an XCLAIM or XAUTOCLAIM reply: its id and, for the
// non-JUSTID form, the field/value list read from the entry log.
type claimRow struct {
	id     streamID
	fields [][]byte
}

// claimCtx batches the row rewrites a claim run makes. Claiming moves a pending entry between
// consumers, so each run touches the group control row and a handful of consumer rows; loading each
// consumer once and flushing at the end keeps a multi-id claim from rewriting the same consumer row
// repeatedly. The PEL rows themselves are written as they are claimed, since each is touched once.
type claimCtx struct {
	c         *connState
	skey      []byte
	group     string
	consumers map[string]*streamConsumer
	g         *streamGroup
	gDirty    bool
}

func (c *connState) newClaimCtx(skey []byte, group string, g *streamGroup) *claimCtx {
	return &claimCtx{c: c, skey: skey, group: group, consumers: map[string]*streamConsumer{}, g: g}
}

// consumer returns the named consumer's row, loading it once and creating a fresh image if it does
// not exist yet, so the claim target is created on first claim the way Redis creates it.
func (cc *claimCtx) consumer(name string) *streamConsumer {
	if con, ok := cc.consumers[name]; ok {
		return con
	}
	cv, ok := cc.c.getStreamConsumer(cc.skey, cc.group, name)
	if !ok {
		cv = streamConsumer{}
	}
	cc.consumers[name] = &cv
	return &cv
}

// flush writes back the group control row (if a delete changed its pending count) and every consumer
// row the run touched, so the target consumer is created and the pending counters are persisted.
func (cc *claimCtx) flush() error {
	for name, con := range cc.consumers {
		if err := cc.c.putStreamConsumer(cc.skey, cc.group, name, *con); err != nil {
			return err
		}
	}
	if cc.gDirty {
		if err := cc.c.putStreamGroup(cc.skey, cc.group, *cc.g); err != nil {
			return err
		}
	}
	return nil
}

// claimOne applies a single claim decision for id and reports whether the entry was claimed and, if
// not, whether it was a dead entry removed from the pending list. It is the shared core of XCLAIM and
// XAUTOCLAIM: an entry gone from the log is dropped from the PEL (deleted), an entry too fresh for the
// min-idle floor is left alone, a not-yet-pending entry is created only under force, and otherwise the
// entry is reassigned to the target consumer with its delivery time and count updated. deliveryTime is
// the wall time to stamp (a resolved IDLE/TIME option or now); retry is the delivery count to set, or
// negative to mean "increment unless justid".
func (cc *claimCtx) claimOne(id streamID, target *streamConsumer, targetName string, now, minIdle, deliveryTime int64, retry int64, force, justid bool) (claimed bool, pe streamPELEntry) {
	c := cc.c
	existing, has := c.getStreamPEL(cc.skey, cc.group, id)
	entryExists := c.srv.store.ExistsKind(c.streamEntryKey(cc.skey, id), kindStreamEntry)

	if !entryExists {
		// The log entry is gone: an unacked id whose entry was trimmed or deleted. Drop the dead PEL
		// row and decrement the group and owning-consumer pending counts. Nothing is claimed.
		if has {
			c.deleteStreamPEL(cc.skey, cc.group, id)
			if cc.g.pending > 0 {
				cc.g.pending--
			}
			cc.gDirty = true
			if oc := cc.consumer(existing.consumer); oc.pending > 0 {
				oc.pending--
			}
		}
		return false, streamPELEntry{}
	}

	if has {
		// An existing pending entry is claimable only once it has been idle at least min-idle-time,
		// force or not: force decides the not-pending case, not the idle floor.
		if now-existing.deliveryTime < minIdle {
			return false, streamPELEntry{}
		}
		if existing.consumer != targetName {
			if oc := cc.consumer(existing.consumer); oc.pending > 0 {
				oc.pending--
			}
			target.pending++
		}
		existing.consumer = targetName
		existing.deliveryTime = deliveryTime
		if retry >= 0 {
			existing.deliveryCount = uint64(retry)
		} else if !justid {
			existing.deliveryCount++
		}
		c.putStreamPEL(cc.skey, cc.group, id, existing)
		return true, existing
	}

	if !force {
		// Not pending and no force: XCLAIM leaves it untouched and omits it from the reply.
		return false, streamPELEntry{}
	}
	// Force-create a pending row for an entry that exists in the log but was never delivered to this
	// group. A fresh row starts at delivery count 1 (or the explicit retry count) and is not
	// incremented further, matching Redis.
	np := streamPELEntry{consumer: targetName, deliveryTime: deliveryTime, deliveryCount: 1}
	if retry >= 0 {
		np.deliveryCount = uint64(retry)
	}
	c.putStreamPEL(cc.skey, cc.group, id, np)
	cc.g.pending++
	cc.gDirty = true
	target.pending++
	return true, np
}

// cmdXClaim implements XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id].
// It reassigns each named pending entry that has been idle at least min-idle-time to the target
// consumer, resetting its idle clock and bumping its delivery count (unless JUSTID or RETRYCOUNT set
// the count outright), so a stalled consumer's work can be handed off. FORCE claims an entry that
// exists in the log but was never delivered to the group; an entry gone from the log is dropped from
// the pending list and skipped. The reply is the claimed entries with fields, or just their ids under
// JUSTID, in the order the ids were given.
func (c *connState) cmdXClaim(argv [][]byte) {
	if len(argv) < 6 {
		c.writeErr("ERR wrong number of arguments for 'xclaim' command")
		return
	}
	skey := argv[1]
	groupName := string(argv[2])
	consumerName := string(argv[3])
	minIdle, err := strconv.ParseInt(string(argv[4]), 10, 64)
	if err != nil {
		c.writeErr("ERR Invalid min-idle-time argument for XCLAIM")
		return
	}

	// Ids run from argv[5] up to the first token that does not parse as an id; the rest are options.
	ids := make([]streamID, 0, len(argv)-5)
	j := 5
	for ; j < len(argv); j++ {
		id, ok := parseStreamID(string(argv[j]), 0)
		if !ok {
			break
		}
		ids = append(ids, id)
	}

	now := nowMillis()
	deliveryTime := now
	retry := int64(-1)
	force, justid, setLastID := false, false, false
	var lastID streamID
	for ; j < len(argv); j++ {
		opt := strings.ToUpper(string(argv[j]))
		more := j+1 < len(argv)
		switch {
		case opt == "FORCE":
			force = true
		case opt == "JUSTID":
			justid = true
		case opt == "IDLE" && more:
			v, e := strconv.ParseInt(string(argv[j+1]), 10, 64)
			if e != nil {
				c.writeErr("ERR Invalid IDLE option argument for XCLAIM")
				return
			}
			deliveryTime = now - v
			j++
		case opt == "TIME" && more:
			v, e := strconv.ParseInt(string(argv[j+1]), 10, 64)
			if e != nil {
				c.writeErr("ERR Invalid TIME option argument for XCLAIM")
				return
			}
			deliveryTime = v
			j++
		case opt == "RETRYCOUNT" && more:
			v, e := strconv.ParseInt(string(argv[j+1]), 10, 64)
			if e != nil {
				c.writeErr("ERR Invalid RETRYCOUNT option argument for XCLAIM")
				return
			}
			retry = v
			j++
		case opt == "LASTID" && more:
			id, ok := parseStreamID(string(argv[j+1]), 0)
			if !ok {
				c.writeErr(errStreamInvalidID)
				return
			}
			lastID = id
			setLastID = true
			j++
		default:
			c.writeErr("ERR Unrecognized XCLAIM option '" + string(argv[j]) + "'")
			return
		}
	}

	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	_, _, _, _, exists := c.streamHeader(skey)
	if !exists {
		mu.Unlock()
		c.writeErr(streamPendingNoGroup(string(skey), groupName))
		return
	}
	g, ok := c.getStreamGroup(skey, groupName)
	if !ok {
		mu.Unlock()
		c.writeErr(streamPendingNoGroup(string(skey), groupName))
		return
	}

	cc := c.newClaimCtx(skey, groupName, &g)
	target := cc.consumer(consumerName)
	target.seenTime = now
	target.activeTime = now
	var rows []claimRow
	for _, id := range ids {
		claimed, _ := cc.claimOne(id, target, consumerName, now, minIdle, deliveryTime, retry, force, justid)
		if !claimed {
			continue
		}
		row := claimRow{id: id}
		if !justid {
			row.fields = c.readEntryFields(c.streamEntryKey(skey, id))
		}
		rows = append(rows, row)
	}
	if setLastID && g.lastID.less(lastID) {
		g.lastID = lastID
		cc.gDirty = true
	}
	if err := cc.flush(); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	mu.Unlock()

	c.writeClaimRows(rows, justid)
}

// writeClaimRows writes the claimed-entry portion shared by XCLAIM and XAUTOCLAIM: a flat array of id
// strings under JUSTID, otherwise an array of [id, field/value list] pairs like XRANGE.
func (c *connState) writeClaimRows(rows []claimRow, justid bool) {
	c.writeArrayHeader(len(rows))
	for _, r := range rows {
		if justid {
			c.writeBulk([]byte(r.id.String()))
			continue
		}
		c.writeArrayHeader(2)
		c.writeBulk([]byte(r.id.String()))
		c.writeArrayHeader(len(r.fields))
		for _, f := range r.fields {
			c.writeBulk(f)
		}
	}
}

// cmdXAutoClaim implements XAUTOCLAIM key group consumer min-idle-time start [COUNT n] [JUSTID]. It
// walks the group's pending list from start in id order, claiming each entry idle at least
// min-idle-time to the target consumer until it has claimed COUNT of them (default 100) or scanned
// COUNT*10 candidates, and drops any entry whose log row is gone. The reply is the cursor to resume
// from (0-0 when the pending list is exhausted), the claimed entries, and the ids that were dropped.
func (c *connState) cmdXAutoClaim(argv [][]byte) {
	if len(argv) < 6 {
		c.writeErr("ERR wrong number of arguments for 'xautoclaim' command")
		return
	}
	skey := argv[1]
	groupName := string(argv[2])
	consumerName := string(argv[3])
	minIdle, err := strconv.ParseInt(string(argv[4]), 10, 64)
	if err != nil {
		c.writeErr("ERR Invalid min-idle-time argument for XAUTOCLAIM")
		return
	}
	start, ok := parseStreamID(string(argv[5]), 0)
	if !ok {
		c.writeErr(errStreamInvalidID)
		return
	}
	count := 100
	justid := false
	for j := 6; j < len(argv); j++ {
		opt := strings.ToUpper(string(argv[j]))
		if opt == "COUNT" && j+1 < len(argv) {
			n, e := strconv.Atoi(string(argv[j+1]))
			if e != nil || n < 1 {
				c.writeErr("ERR COUNT must be > 0")
				return
			}
			count = n
			j++
			continue
		}
		if opt == "JUSTID" {
			justid = true
			continue
		}
		c.writeErr("ERR syntax error")
		return
	}

	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	_, _, _, _, exists := c.streamHeader(skey)
	if !exists {
		mu.Unlock()
		c.writeErr(streamPendingNoGroup(string(skey), groupName))
		return
	}
	g, ok := c.getStreamGroup(skey, groupName)
	if !ok {
		mu.Unlock()
		c.writeErr(streamPendingNoGroup(string(skey), groupName))
		return
	}

	now := nowMillis()
	cc := c.newClaimCtx(skey, groupName, &g)
	target := cc.consumer(consumerName)
	target.seenTime = now
	target.activeTime = now

	// Seek to the start bound, which is inclusive: scan past the predecessor id so the start row is
	// included, or from the pending-list front when start is 0-0 with no predecessor.
	prefix := streamPELPrefix(skey, groupName)
	var scanAfter []byte
	if pred, ok := streamIDDec(start); ok {
		scanAfter = streamPELKey(skey, groupName, pred)
	}
	attempts := count * 10
	remaining := count
	var rows []claimRow
	var deleted []streamID
	var cursor streamID // stays 0-0 when the pending list is exhausted
	done := false
	for !done {
		pkeys, last := c.srv.store.CollScan(prefix, scanAfter, streamTrimBatch, nil)
		if len(pkeys) == 0 {
			break
		}
		for _, k := range pkeys {
			id := decodeEntryID(k)
			// Stop once the claim quota or the scan budget is spent; the current id is where a later
			// call resumes.
			if remaining <= 0 || attempts <= 0 {
				cursor = id
				done = true
				break
			}
			attempts--
			claimed, _ := cc.claimOne(id, target, consumerName, now, minIdle, now, -1, false, justid)
			if claimed {
				row := claimRow{id: id}
				if !justid {
					row.fields = c.readEntryFields(c.streamEntryKey(skey, id))
				}
				rows = append(rows, row)
				remaining--
				continue
			}
			// A dead entry claimOne dropped from the pending list goes in the deleted-id list and, like
			// a claim, counts against the requested total; a too-fresh entry is simply passed over.
			if !c.srv.store.ExistsKind(c.streamEntryKey(skey, id), kindStreamEntry) {
				deleted = append(deleted, id)
				remaining--
			}
		}
		scanAfter = last
	}
	if err := cc.flush(); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	mu.Unlock()

	c.writeArrayHeader(3)
	c.writeBulk([]byte(cursor.String()))
	c.writeClaimRows(rows, justid)
	c.writeArrayHeader(len(deleted))
	for _, id := range deleted {
		c.writeBulk([]byte(id.String()))
	}
}

// --- XREADGROUP ---

// rgRow is one row of an XREADGROUP reply: an entry with its fields, or a tombstone whose fields
// array is null because the underlying entry was deleted since it was delivered.
type rgRow struct {
	id     streamID
	fields [][]byte
	nilF   bool
}

// rgResult is the per-stream slice of an XREADGROUP reply.
type rgResult struct {
	key  []byte
	rows []rgRow
}

// cmdXReadGroup implements XREADGROUP GROUP g c [COUNT n] [BLOCK ms] [NOACK] STREAMS key [key ...] id
// [id ...]. A '>' id delivers entries newer than the group's last-delivered id, advances that cursor,
// and (unless NOACK) records a PEL row per delivered entry so the entry stays pending until acked. An
// explicit id is a non-consuming re-read of that consumer's own pending entries greater than the id,
// and never changes any PEL row. BLOCK is parsed and its timeout validated, but this slice serves it
// non-blocking (a '>' read with nothing new returns the null array); parking lands with the stream
// blocking slice, the way the list blocking commands followed the non-blocking list path.
func (c *connState) cmdXReadGroup(argv [][]byte) {
	if len(argv) < 7 {
		c.writeErr("ERR wrong number of arguments for 'xreadgroup' command")
		return
	}
	var groupName, consumerName string
	groupSet := false
	count := -1
	noAck := false
	for i := 1; i < len(argv); {
		switch strings.ToUpper(string(argv[i])) {
		case "GROUP":
			// GROUP names the group and the consumer; both operands are required.
			if i+2 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			groupName = string(argv[i+1])
			consumerName = string(argv[i+2])
			groupSet = true
			i += 3
		case "COUNT":
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			n, err := strconv.Atoi(string(argv[i+1]))
			if err != nil {
				c.writeErr(errStreamNotInt)
				return
			}
			// Redis clamps a non-positive COUNT to "no limit" rather than rejecting it.
			if n > 0 {
				count = n
			} else {
				count = -1
			}
			i += 2
		case "BLOCK":
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			ms, err := strconv.ParseInt(string(argv[i+1]), 10, 64)
			if err != nil {
				c.writeErr(errStreamTimeoutInt)
				return
			}
			if ms < 0 {
				c.writeErr(errStreamTimeoutNeg)
				return
			}
			i += 2
		case "NOACK":
			noAck = true
			i++
		case "STREAMS":
			// A read with no GROUP option parsed is rejected the way Redis rejects it, after the
			// options loop reaches STREAMS without having seen GROUP.
			if !groupSet {
				c.writeErr("ERR Missing GROUP option for XREADGROUP")
				return
			}
			i++
			c.xreadGroupStreams(groupName, consumerName, argv[i:], count, noAck)
			return
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}
	if !groupSet {
		c.writeErr("ERR Missing GROUP option for XREADGROUP")
		return
	}
	c.writeErr("ERR syntax error")
}

// xreadGroupStreams reads the STREAMS clause and delivers per stream. It locks one stream's stripe at
// a time, so a multi-stream read never holds two locks at once.
func (c *connState) xreadGroupStreams(groupName, consumerName string, rest [][]byte, count int, noAck bool) {
	if len(rest) == 0 || len(rest)%2 != 0 {
		c.writeErr(errStreamUnbalancedGroup)
		return
	}
	n := len(rest) / 2
	keys := rest[:n]
	idArgs := rest[n:]

	newDelivery := make([]bool, n)
	starts := make([]streamID, n)
	anyExplicit := false
	for j := 0; j < n; j++ {
		raw := string(idArgs[j])
		if raw == ">" {
			newDelivery[j] = true
			continue
		}
		anyExplicit = true
		id, ok := parseStreamID(raw, 0)
		if !ok {
			c.writeErr(errStreamInvalidID)
			return
		}
		starts[j] = id
	}
	now := nowMillis()

	var results []rgResult
	for j := 0; j < n; j++ {
		skey := keys[j]
		mu := &c.srv.incrMu[c.srv.stripe(skey)]
		mu.Lock()
		if c.stringConflict(skey) {
			mu.Unlock()
			c.writeErr(wrongType)
			return
		}
		_, _, _, _, exists := c.streamHeader(skey)
		if !exists {
			mu.Unlock()
			c.writeErr(streamReadGroupNoGroup(string(skey), groupName))
			return
		}
		g, ok := c.getStreamGroup(skey, groupName)
		if !ok {
			mu.Unlock()
			c.writeErr(streamReadGroupNoGroup(string(skey), groupName))
			return
		}

		if newDelivery[j] {
			rows, err := c.deliverNew(skey, groupName, consumerName, &g, count, noAck, now)
			if err != nil {
				mu.Unlock()
				c.writeErr("ERR " + err.Error())
				return
			}
			mu.Unlock()
			if len(rows) == 0 {
				continue
			}
			results = append(results, rgResult{key: skey, rows: rows})
		} else {
			rows := c.collectConsumerPEL(skey, groupName, consumerName, starts[j], count)
			// An explicit read never changes a PEL, but it does register the consumer (creating it if
			// absent) and refresh its seen-time, matching Redis.
			con, has := c.getStreamConsumer(skey, groupName, consumerName)
			if !has {
				con.activeTime = now
			}
			con.seenTime = now
			if err := c.putStreamConsumer(skey, groupName, consumerName, con); err != nil {
				mu.Unlock()
				c.writeErr("ERR " + err.Error())
				return
			}
			mu.Unlock()
			results = append(results, rgResult{key: skey, rows: rows})
		}
	}

	// A '>' read that delivered nothing returns the null array; an explicit id always yields a
	// per-stream list, even an empty one.
	if len(results) == 0 && !anyExplicit {
		c.writeNilArray()
		return
	}
	c.writeArrayHeader(len(results))
	for _, r := range results {
		c.writeArrayHeader(2)
		c.writeBulk(r.key)
		c.writeArrayHeader(len(r.rows))
		for _, row := range r.rows {
			c.writeArrayHeader(2)
			c.writeBulk([]byte(row.id.String()))
			if row.nilF {
				c.writeNilArray()
				continue
			}
			c.writeArrayHeader(len(row.fields))
			for _, f := range row.fields {
				c.writeBulk(f)
			}
		}
	}
}

// deliverNew delivers the entries newer than the group's last-delivered id (up to count), advances
// the group cursor and entries-read counter, and, unless NOACK, records one PEL row per delivered
// entry and bumps the group and consumer pending counts. It writes the group and consumer rows and
// returns the reply rows. The caller holds the stream's stripe lock.
func (c *connState) deliverNew(skey []byte, group, consumer string, g *streamGroup, count int, noAck bool, now int64) ([]rgRow, error) {
	w := c.streamWindow(skey, g.lastID, true, maxStreamID, false, count, false)
	if len(w) == 0 {
		return nil, nil
	}
	rows := make([]rgRow, 0, len(w))
	for _, ek := range w {
		id := decodeEntryID(ek)
		g.lastID = id
		g.entriesRead++
		rows = append(rows, rgRow{id: id, fields: c.readEntryFields(ek)})
		if !noAck {
			if err := c.putStreamPEL(skey, group, id, streamPELEntry{consumer: consumer, deliveryTime: now, deliveryCount: 1}); err != nil {
				return nil, err
			}
		}
	}
	con, _ := c.getStreamConsumer(skey, group, consumer)
	con.seenTime = now
	con.activeTime = now
	if !noAck {
		g.pending += uint64(len(w))
		con.pending += uint64(len(w))
	}
	if err := c.putStreamConsumer(skey, group, consumer, con); err != nil {
		return nil, err
	}
	if err := c.putStreamGroup(skey, group, *g); err != nil {
		return nil, err
	}
	return rows, nil
}

// readEntryFields reads an entry row's field/value list into a fresh slice of subslices. It reads into
// a fresh buffer (dst nil) so the returned subslices stay valid while later entries are read into
// other buffers.
func (c *connState) readEntryFields(ek []byte) [][]byte {
	v, ok := c.srv.store.GetKind(ek, nil, kindStreamEntry)
	if !ok {
		return nil
	}
	nf, off := binary.Uvarint(v)
	fields := make([][]byte, 0, nf*2)
	for j := uint64(0); j < nf*2; j++ {
		l, m := binary.Uvarint(v[off:])
		off += m
		fields = append(fields, v[off:off+int(l)])
		off += int(l)
	}
	return fields
}

// collectConsumerPEL returns the consumer's pending rows with id greater than after, up to count, in
// id order. It scans the group's PEL prefix from just past after, keeps the rows this consumer owns,
// and point-fetches each one's entry body, so the cost tracks the scanned window, not the whole
// pending list. An entry deleted since delivery becomes a null-field tombstone row.
func (c *connState) collectConsumerPEL(skey []byte, group, consumer string, after streamID, count int) []rgRow {
	prefix := streamPELPrefix(skey, group)
	scanAfter := streamPELKey(skey, group, after)
	var rows []rgRow
	var buf []byte
	for {
		pkeys, last := c.srv.store.CollScan(prefix, scanAfter, streamTrimBatch, nil)
		if len(pkeys) == 0 {
			break
		}
		for _, k := range pkeys {
			v, ok := c.srv.store.GetKind(k, buf[:0], kindStreamPEL)
			if !ok {
				continue
			}
			buf = v
			if decodeStreamPEL(v).consumer != consumer {
				continue
			}
			id := decodeEntryID(k)
			ek := c.streamEntryKey(skey, id)
			if c.srv.store.ExistsKind(ek, kindStreamEntry) {
				rows = append(rows, rgRow{id: id, fields: c.readEntryFields(ek)})
			} else {
				rows = append(rows, rgRow{id: id, nilF: true})
			}
			if count > 0 && len(rows) >= count {
				return rows
			}
		}
		scanAfter = last
	}
	return rows
}
