package f1srv

import "strconv"

// XINFO is the stream's introspection surface (spec 2064/f1_rewrite_ltm/09). Every field it reports is
// already recorded by the log layer or the group layer, so each subcommand is a bounded read of the
// header row plus a scan of one sibling family, never a whole-log or whole-PEL decode:
//
//   - STREAM key           reads the header (length, ids, entries-added), the first and last entry by
//     positional select, and the group count by a bounded family scan.
//   - STREAM key FULL       adds the entry window (bounded by COUNT, default 10) and, per group, the
//     pending list and per-consumer pending list (each bounded by COUNT).
//   - GROUPS key            walks the group family, one control-row read plus a consumer-count scan
//     per group.
//   - CONSUMERS key group   walks one group's consumer family, one row read per consumer.
//
// Reply shape note. f1raw reports the portable field set that Redis 8.8 and Valkey 9.1 share, which is
// exactly Valkey 9.1's shape. Redis 8.8 additionally emits a handful of feature-specific fields the
// element-per-row model has no equivalent for (idmp-duration, idmp-maxsize, pids-tracked, iids-tracked,
// iids-added, iids-duplicates on STREAM/STREAM FULL, and a per-group nacked-count on STREAM FULL);
// those are Redis-8.8-only extensions and are intentionally out of scope here, so a STREAM reply is
// byte-identical to Valkey 9.1 and a subset of Redis 8.8. GROUPS and CONSUMERS are identical on both.
//
// entries-read and lag. A group's entries-read counter is exact once a delivery has reconciled it, and
// unknown (shown as a null) before that or after a SETID moved the cursor without a fresh count. The
// lag (how many entries the group has not read) is entries-added minus entries-read when the counter is
// live and no tombstone sits ahead of the group's cursor, and otherwise a bounded estimate from the
// cursor's position, or a null when even that is not determinable. This matches Redis's lazy
// reconciliation, where the counter is recovered opportunistically rather than scanned for.

// xinfoSubErr is the reply for a well-formed XINFO subcommand token with the wrong argument shape (a
// bad STREAM tail, say). Redis echoes the subcommand token verbatim, case and all.
func xinfoSubErr(verbatimSub string) string {
	return "ERR unknown subcommand or wrong number of arguments for '" + verbatimSub + "'. Try XINFO HELP."
}

// xinfoUnknownSub is the reply for an unrecognized XINFO subcommand.
func xinfoUnknownSub(verbatimSub string) string {
	return "ERR unknown subcommand '" + verbatimSub + "'. Try XINFO HELP."
}

// xinfoHelpLines is the XINFO HELP reply, verbatim to Redis and Valkey.
var xinfoHelpLines = []string{
	"XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"CONSUMERS <key> <groupname>",
	"    Show consumers of <groupname>.",
	"GROUPS <key>",
	"    Show the stream consumer groups.",
	"STREAM <key> [FULL [COUNT <count>]",
	"    Show information about the stream.",
	"HELP",
	"    Print this help.",
}

func (c *connState) cmdXInfo(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'xinfo' command")
		return
	}
	switch {
	case eqFold(argv[1], "STREAM"):
		c.xinfoStream(argv)
	case eqFold(argv[1], "GROUPS"):
		c.xinfoGroups(argv)
	case eqFold(argv[1], "CONSUMERS"):
		c.xinfoConsumers(argv)
	case eqFold(argv[1], "HELP"):
		c.writeArrayHeader(len(xinfoHelpLines))
		for _, line := range xinfoHelpLines {
			c.writeSimple(line)
		}
	default:
		c.writeErr(xinfoUnknownSub(string(argv[1])))
	}
}

// --- entries-read / lag model ---

// streamFirstEntryID returns the id of the oldest live entry, or false when the stream is empty.
func (c *connState) streamFirstEntryID(skey []byte, length uint64) (streamID, bool) {
	if length == 0 {
		return streamID{}, false
	}
	k, ok := c.srv.store.CollSelectAt(c.streamEntryPrefix(skey), 0)
	if !ok {
		return streamID{}, false
	}
	return decodeEntryID(k), true
}

// streamEstimateEntriesRead estimates how many entries a cursor at pos has read, counting from the
// first entry ever added. It is a faithful port of Redis's streamEstimateDistanceFromFirstEverEntry
// (t_stream.c): exact at the anchors and whenever the highest tombstone sits below the first live entry
// (a pure prefix deletion), and unknown (ok=false) once a tombstone lands inside the live range, where
// the count below pos can no longer be derived without walking the log. firstID is the first live entry,
// lastGenID the last generated id, maxDel the highest deleted id, length the live count.
func streamEstimateEntriesRead(pos, firstID, lastGenID, maxDel streamID, length, entriesAdded uint64) (uint64, bool) {
	if entriesAdded == 0 {
		return 0, true
	}
	// Fully drained stream: any cursor at or below the last generated id has read everything.
	if length == 0 && !lastGenID.less(pos) {
		return entriesAdded, true
	}
	// A tombstone sits at or above a non-zero cursor: the count below it is ambiguous.
	if pos != (streamID{}) && pos.less(maxDel) {
		return 0, false
	}
	if pos == lastGenID {
		return entriesAdded, true
	}
	if lastGenID.less(pos) {
		return 0, false
	}
	// No deletions, or the highest tombstone is below the first live entry (prefix-only deletion): the
	// entries below the live window are exactly what is gone, so a cursor there is exactly placeable.
	if maxDel == (streamID{}) || maxDel.less(firstID) {
		if pos.less(firstID) {
			return entriesAdded - length, true
		}
		if pos == firstID {
			return entriesAdded - length + 1, true
		}
	}
	return 0, false
}

// streamRangeHasTombstones reports whether the highest deleted id lies at or above start (Redis passes a
// nil upper bound here, so the range is [start, +inf)). It ports streamRangeHasTombstones from t_stream.c
// for the open-ended case the lag computation needs.
func streamRangeHasTombstones(start, maxDel streamID, length uint64) bool {
	if length == 0 || maxDel == (streamID{}) {
		return false
	}
	return !maxDel.less(start) // start <= maxDel
}

// streamCGLag computes a group's lag (entries added but not yet read) and whether it is determinable. It
// ports Redis's streamReplyWithCGLag (t_stream.c): zero for an empty or fully drained stream; the live
// length when both the cursor and the highest tombstone sit below the first live entry; entries-added
// minus a live entries-read counter when no tombstone lies ahead of the cursor; otherwise an estimate
// from the cursor, and unknown when even that is not determinable.
func streamCGLag(g streamGroup, firstID, lastGenID, maxDel streamID, length, entriesAdded uint64) (int64, bool) {
	if entriesAdded == 0 || length == 0 {
		return 0, true
	}
	if g.lastID.less(firstID) && maxDel.less(firstID) {
		return int64(length), true
	}
	if g.entriesRead != streamEntriesReadInvalid && !streamRangeHasTombstones(g.lastID, maxDel, length) {
		return int64(entriesAdded) - int64(g.entriesRead), true
	}
	if est, ok := streamEstimateEntriesRead(g.lastID, firstID, lastGenID, maxDel, length, entriesAdded); ok {
		return int64(entriesAdded) - int64(est), true
	}
	return 0, false
}

// writeEntriesRead writes a group's entries-read field: the counter, or a null when it is unknown.
func (c *connState) writeEntriesRead(er uint64) {
	if er == streamEntriesReadInvalid {
		c.writeNil()
		return
	}
	c.writeInt(int64(er))
}

// --- family enumeration ---

// streamCountFamily counts the rows under a family prefix in bounded scan batches, so a group count or
// consumer count costs a walk of that one family, not a whole-keyspace scan.
func (c *connState) streamCountFamily(prefix []byte) int {
	n := 0
	var after []byte
	for {
		keys, last := c.srv.store.CollScan(prefix, after, streamTrimBatch, nil)
		if len(keys) == 0 {
			break
		}
		n += len(keys)
		after = last
	}
	return n
}

// streamGroupNames returns a stream's group names in sorted order.
func (c *connState) streamGroupNames(skey []byte) []string {
	prefix := streamFamilyPrefix(skey, streamGroupTag)
	var names []string
	var after []byte
	for {
		keys, last := c.srv.store.CollScan(prefix, after, streamTrimBatch, nil)
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			names = append(names, string(k[len(prefix):]))
		}
		after = last
	}
	return names
}

// streamConsumerNames returns a group's consumer names in sorted order.
func (c *connState) streamConsumerNames(skey []byte, group string) []string {
	prefix := streamConsumerPrefix(skey, group)
	var names []string
	var after []byte
	for {
		keys, last := c.srv.store.CollScan(prefix, after, streamTrimBatch, nil)
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			names = append(names, string(k[len(prefix):]))
		}
		after = last
	}
	return names
}

// streamRadixTree synthesizes the radix-tree-keys and radix-tree-nodes fields Redis reports from its
// rax entry index. f1raw stores each entry in its own row rather than in rax macro nodes, so these are
// derived from the live length: keys is the macro-node count (one per 100 entries, Redis's node
// capacity), which matches exactly, and nodes matches exactly for the empty and single-node cases and
// is a documented best-effort estimate beyond, since the true node count depends on Redis's internal
// key layout the element-per-row store does not reproduce.
func streamRadixTree(length uint64) (keys, nodes int64) {
	if length == 0 {
		return 0, 1
	}
	keys = int64((length + 99) / 100)
	if keys == 1 {
		return 1, 2
	}
	return keys, 2*keys + 2
}

// --- XINFO STREAM ---

func (c *connState) xinfoStream(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'xinfo|stream' command")
		return
	}
	// Validate the argument shape before touching the key: plain, FULL, or FULL COUNT n. Any other
	// shape is the subcommand syntax error, echoing the subcommand token.
	full := false
	parseCount := false
	switch {
	case len(argv) == 3:
	case len(argv) == 4 && eqFold(argv[3], "FULL"):
		full = true
	case len(argv) == 6 && eqFold(argv[3], "FULL") && eqFold(argv[4], "COUNT"):
		full = true
		parseCount = true
	default:
		c.writeErr(xinfoSubErr(string(argv[1])))
		return
	}
	skey := argv[2]

	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	// The key is looked up before the COUNT value is parsed, matching Redis: XINFO STREAM missing FULL
	// COUNT abc reports "no such key", not a count error.
	length, lastGenID, maxDel, entriesAdded, exists := c.streamHeader(skey)
	if !exists {
		mu.Unlock()
		c.writeErr(errStreamNoSuchKey)
		return
	}
	limit := 10
	if parseCount {
		n, err := strconv.Atoi(string(argv[5]))
		if err != nil {
			mu.Unlock()
			c.writeErr(errStreamNotInt)
			return
		}
		// A zero or negative COUNT means no limit, matching Redis.
		if n < 0 {
			n = 0
		}
		limit = n
	}
	firstID, _ := c.streamFirstEntryID(skey, length)

	if !full {
		groupCount := c.streamCountFamily(streamFamilyPrefix(skey, streamGroupTag))
		mu.Unlock()
		c.emitXInfoStream(skey, length, lastGenID, maxDel, entriesAdded, firstID, groupCount)
		return
	}
	// FULL: snapshot the group and consumer metadata under the lock, then emit. The entry rows and PEL
	// rows are read lock-free during emit, the same benign race the other stream read commands accept.
	groups := c.snapshotFullGroups(skey, firstID, lastGenID, maxDel, length, entriesAdded)
	mu.Unlock()
	c.emitXInfoStreamFull(skey, length, lastGenID, maxDel, entriesAdded, firstID, limit, groups)
}

// emitXInfoStream writes the non-FULL STREAM reply, a ten-field map.
func (c *connState) emitXInfoStream(skey []byte, length uint64, lastGenID, maxDel streamID, entriesAdded uint64, firstID streamID, groupCount int) {
	rkeys, rnodes := streamRadixTree(length)
	c.writeArrayHeader(20)
	c.writeBulk([]byte("length"))
	c.writeInt(int64(length))
	c.writeBulk([]byte("radix-tree-keys"))
	c.writeInt(rkeys)
	c.writeBulk([]byte("radix-tree-nodes"))
	c.writeInt(rnodes)
	c.writeBulk([]byte("last-generated-id"))
	c.writeBulk([]byte(lastGenID.String()))
	c.writeBulk([]byte("max-deleted-entry-id"))
	c.writeBulk([]byte(maxDel.String()))
	c.writeBulk([]byte("entries-added"))
	c.writeInt(int64(entriesAdded))
	c.writeBulk([]byte("recorded-first-entry-id"))
	c.writeBulk([]byte(firstID.String()))
	c.writeBulk([]byte("groups"))
	c.writeInt(int64(groupCount))
	c.writeBulk([]byte("first-entry"))
	c.emitStreamEntryAt(skey, length, 0)
	c.writeBulk([]byte("last-entry"))
	c.emitStreamEntryAt(skey, length, int(length)-1)
}

// emitStreamEntryAt writes the entry at positional index idx as an [id, [field, value, ...]] pair, or a
// null when the stream is empty or the index is out of range.
func (c *connState) emitStreamEntryAt(skey []byte, length uint64, idx int) {
	if length == 0 || idx < 0 {
		c.writeNil()
		return
	}
	k, ok := c.srv.store.CollSelectAt(c.streamEntryPrefix(skey), idx)
	if !ok {
		c.writeNil()
		return
	}
	c.emitStreamEntry(k)
}

// --- XINFO STREAM FULL ---

// xinfoGroupSnap is a group's metadata captured under the stripe lock for the FULL reply.
type xinfoGroupSnap struct {
	name      string
	g         streamGroup
	lagValue  int64
	lagOK     bool
	consumers []xinfoConsumerSnap
}

// xinfoConsumerSnap is a consumer's metadata captured for the FULL reply.
type xinfoConsumerSnap struct {
	name string
	con  streamConsumer
}

// snapshotFullGroups captures every group's control row, its lag, and its consumers' rows, so the FULL
// reply can emit the counters consistently while reading the entry and PEL rows lock-free afterward.
func (c *connState) snapshotFullGroups(skey []byte, firstID, lastGenID, maxDel streamID, length, entriesAdded uint64) []xinfoGroupSnap {
	names := c.streamGroupNames(skey)
	out := make([]xinfoGroupSnap, 0, len(names))
	for _, gn := range names {
		g, ok := c.getStreamGroup(skey, gn)
		if !ok {
			continue
		}
		lag, lagOK := streamCGLag(g, firstID, lastGenID, maxDel, length, entriesAdded)
		cnames := c.streamConsumerNames(skey, gn)
		cons := make([]xinfoConsumerSnap, 0, len(cnames))
		for _, cn := range cnames {
			con, cok := c.getStreamConsumer(skey, gn, cn)
			if !cok {
				continue
			}
			cons = append(cons, xinfoConsumerSnap{name: cn, con: con})
		}
		out = append(out, xinfoGroupSnap{name: gn, g: g, lagValue: lag, lagOK: lagOK, consumers: cons})
	}
	return out
}

// emitXInfoStreamFull writes the FULL reply: the stream header, the entry window, and per group the
// pending list and each consumer's pending list, all bounded by limit (0 means no limit).
func (c *connState) emitXInfoStreamFull(skey []byte, length uint64, lastGenID, maxDel streamID, entriesAdded uint64, firstID streamID, limit int, groups []xinfoGroupSnap) {
	rkeys, rnodes := streamRadixTree(length)
	c.writeArrayHeader(18)
	c.writeBulk([]byte("length"))
	c.writeInt(int64(length))
	c.writeBulk([]byte("radix-tree-keys"))
	c.writeInt(rkeys)
	c.writeBulk([]byte("radix-tree-nodes"))
	c.writeInt(rnodes)
	c.writeBulk([]byte("last-generated-id"))
	c.writeBulk([]byte(lastGenID.String()))
	c.writeBulk([]byte("max-deleted-entry-id"))
	c.writeBulk([]byte(maxDel.String()))
	c.writeBulk([]byte("entries-added"))
	c.writeInt(int64(entriesAdded))
	c.writeBulk([]byte("recorded-first-entry-id"))
	c.writeBulk([]byte(firstID.String()))
	c.writeBulk([]byte("entries"))
	c.emitStreamEntriesWindow(skey, length, limit)
	c.writeBulk([]byte("groups"))
	c.writeArrayHeader(len(groups))
	for _, gs := range groups {
		c.writeArrayHeader(14)
		c.writeBulk([]byte("name"))
		c.writeBulk([]byte(gs.name))
		c.writeBulk([]byte("last-delivered-id"))
		c.writeBulk([]byte(gs.g.lastID.String()))
		c.writeBulk([]byte("entries-read"))
		c.writeEntriesRead(gs.g.entriesRead)
		c.writeBulk([]byte("lag"))
		if gs.lagOK {
			c.writeInt(gs.lagValue)
		} else {
			c.writeNil()
		}
		c.writeBulk([]byte("pel-count"))
		c.writeInt(int64(gs.g.pending))
		c.writeBulk([]byte("pending"))
		c.emitStreamGroupPEL(skey, gs.name, limit)
		c.writeBulk([]byte("consumers"))
		c.writeArrayHeader(len(gs.consumers))
		for _, cs := range gs.consumers {
			c.writeArrayHeader(10)
			c.writeBulk([]byte("name"))
			c.writeBulk([]byte(cs.name))
			c.writeBulk([]byte("seen-time"))
			c.writeInt(cs.con.seenTime)
			c.writeBulk([]byte("active-time"))
			c.writeInt(cs.con.activeTime)
			c.writeBulk([]byte("pel-count"))
			c.writeInt(int64(cs.con.pending))
			c.writeBulk([]byte("pending"))
			c.emitStreamConsumerPEL(skey, gs.name, cs.name, limit)
		}
	}
}

// emitStreamEntriesWindow writes up to n entries (n = min(length, limit); limit 0 means all) from the
// front of the entry log, each as an [id, [field, value, ...]] pair.
func (c *connState) emitStreamEntriesWindow(skey []byte, length uint64, limit int) {
	n := int(length)
	if limit > 0 && limit < n {
		n = limit
	}
	c.writeArrayHeader(n)
	if n == 0 {
		return
	}
	prefix := c.streamEntryPrefix(skey)
	emitted := 0
	var after []byte
	for emitted < n {
		want := n - emitted
		if want > streamTrimBatch {
			want = streamTrimBatch
		}
		keys, last := c.srv.store.CollScan(prefix, after, want, nil)
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			c.emitStreamEntry(k)
			emitted++
			if emitted >= n {
				break
			}
		}
		after = last
	}
}

// streamPELRow is one collected pending-entry row: the entry id and its delivery bookkeeping, plus the
// owning consumer (used only for the group-level list).
type streamPELRow struct {
	id            string
	consumer      string
	deliveryTime  int64
	deliveryCount uint64
}

// collectStreamPEL scans a group's PEL family and gathers up to all its rows, keeping only the rows
// owned by consumer when consumer is non-empty. Collecting first lets the caller write an array header
// that exactly matches what it emits, so the reply never over-declares its length even if the header's
// cached pending counter has drifted from the live row count.
func (c *connState) collectStreamPEL(skey []byte, group, consumer string) []streamPELRow {
	prefix := streamPELPrefix(skey, group)
	var rows []streamPELRow
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
			pe := decodeStreamPEL(v)
			if consumer != "" && pe.consumer != consumer {
				continue
			}
			rows = append(rows, streamPELRow{
				id:            decodeEntryID(k).String(),
				consumer:      pe.consumer,
				deliveryTime:  pe.deliveryTime,
				deliveryCount: pe.deliveryCount,
			})
		}
		after = last
	}
	return rows
}

// clampRows returns rows truncated to limit (limit 0 means no limit).
func clampRows(rows []streamPELRow, limit int) []streamPELRow {
	if limit > 0 && limit < len(rows) {
		return rows[:limit]
	}
	return rows
}

// emitStreamGroupPEL writes a group's pending entries in id order, at most limit of them (limit 0 means
// all), each as an [id, consumer, delivery-time, delivery-count] row.
func (c *connState) emitStreamGroupPEL(skey []byte, group string, limit int) {
	rows := clampRows(c.collectStreamPEL(skey, group, ""), limit)
	c.writeArrayHeader(len(rows))
	for _, r := range rows {
		c.writeArrayHeader(4)
		c.writeBulk([]byte(r.id))
		c.writeBulk([]byte(r.consumer))
		c.writeInt(r.deliveryTime)
		c.writeInt(int64(r.deliveryCount))
	}
}

// emitStreamConsumerPEL writes one consumer's pending entries in id order, at most limit of them (limit
// 0 means all), each as an [id, delivery-time, delivery-count] row.
func (c *connState) emitStreamConsumerPEL(skey []byte, group, consumer string, limit int) {
	rows := clampRows(c.collectStreamPEL(skey, group, consumer), limit)
	c.writeArrayHeader(len(rows))
	for _, r := range rows {
		c.writeArrayHeader(3)
		c.writeBulk([]byte(r.id))
		c.writeInt(r.deliveryTime)
		c.writeInt(int64(r.deliveryCount))
	}
}

// --- XINFO GROUPS ---

func (c *connState) xinfoGroups(argv [][]byte) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'xinfo|groups' command")
		return
	}
	skey := argv[2]

	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	length, lastGenID, maxDel, entriesAdded, exists := c.streamHeader(skey)
	if !exists {
		mu.Unlock()
		c.writeErr(errStreamNoSuchKey)
		return
	}
	firstID, _ := c.streamFirstEntryID(skey, length)

	type groupInfo struct {
		name      string
		consumers int
		g         streamGroup
		lagValue  int64
		lagOK     bool
	}
	names := c.streamGroupNames(skey)
	infos := make([]groupInfo, 0, len(names))
	for _, gn := range names {
		g, ok := c.getStreamGroup(skey, gn)
		if !ok {
			continue
		}
		lag, lagOK := streamCGLag(g, firstID, lastGenID, maxDel, length, entriesAdded)
		cc := c.streamCountFamily(streamConsumerPrefix(skey, gn))
		infos = append(infos, groupInfo{name: gn, consumers: cc, g: g, lagValue: lag, lagOK: lagOK})
	}
	mu.Unlock()

	c.writeArrayHeader(len(infos))
	for _, in := range infos {
		c.writeArrayHeader(12)
		c.writeBulk([]byte("name"))
		c.writeBulk([]byte(in.name))
		c.writeBulk([]byte("consumers"))
		c.writeInt(int64(in.consumers))
		c.writeBulk([]byte("pending"))
		c.writeInt(int64(in.g.pending))
		c.writeBulk([]byte("last-delivered-id"))
		c.writeBulk([]byte(in.g.lastID.String()))
		c.writeBulk([]byte("entries-read"))
		c.writeEntriesRead(in.g.entriesRead)
		c.writeBulk([]byte("lag"))
		if in.lagOK {
			c.writeInt(in.lagValue)
		} else {
			c.writeNil()
		}
	}
}

// --- XINFO CONSUMERS ---

func (c *connState) xinfoConsumers(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'xinfo|consumers' command")
		return
	}
	skey := argv[2]
	group := string(argv[3])

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
		c.writeErr(errStreamNoSuchKey)
		return
	}
	// The key is checked before the group: CONSUMERS on a missing key is "no such key", on a missing
	// group of an existing stream it is NOGROUP.
	if _, ok := c.getStreamGroup(skey, group); !ok {
		mu.Unlock()
		c.writeErr(streamNoGroup(group, string(skey)))
		return
	}
	now := nowMillis()

	type consumerInfo struct {
		name string
		con  streamConsumer
	}
	names := c.streamConsumerNames(skey, group)
	infos := make([]consumerInfo, 0, len(names))
	for _, cn := range names {
		con, ok := c.getStreamConsumer(skey, group, cn)
		if !ok {
			continue
		}
		infos = append(infos, consumerInfo{name: cn, con: con})
	}
	mu.Unlock()

	c.writeArrayHeader(len(infos))
	for _, in := range infos {
		c.writeArrayHeader(8)
		c.writeBulk([]byte("name"))
		c.writeBulk([]byte(in.name))
		c.writeBulk([]byte("pending"))
		c.writeInt(int64(in.con.pending))
		c.writeBulk([]byte("idle"))
		c.writeInt(now - in.con.seenTime)
		c.writeBulk([]byte("inactive"))
		c.writeInt(now - in.con.activeTime)
	}
}
