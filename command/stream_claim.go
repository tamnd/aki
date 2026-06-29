package command

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/resp"
)

// claimCommands registers the PEL claim and inspection commands.
func claimCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "xpending", Group: GroupStream, Since: "5.0.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleXPending},
		{Name: "xclaim", Group: GroupStream, Since: "5.0.0",
			Arity: -6, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleXClaim},
		{Name: "xautoclaim", Group: GroupStream, Since: "6.2.0",
			Arity: -6, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleXAutoClaim},
	}
}

// elapsedSince returns the non-negative milliseconds between now and t.
func elapsedSince(now, t int64) int64 {
	if now < t {
		return 0
	}
	return now - t
}

// handleXPending implements XPENDING key group [IDLE ms] [start end count] [consumer].
func handleXPending(ctx *Ctx) {
	argv := ctx.Argv
	key := argv[1]
	groupName := string(argv[2])
	summary := len(argv) == 3

	var (
		idle      int64
		start     rangeBound
		end       rangeBound
		count     int64
		consumer  string
		hasFilter bool
	)
	if !summary {
		i := 3
		if strings.EqualFold(string(argv[i]), "IDLE") {
			if i+1 >= len(argv) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			v, ok := parseInteger(argv[i+1])
			if !ok || v < 0 {
				ctx.enc().WriteError("ERR value is not an integer or out of range")
				return
			}
			idle = v
			i += 2
		}
		if i+3 > len(argv) {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		s, ok := parseRangeStart(string(argv[i]))
		if !ok {
			ctx.enc().WriteError(errStreamInvalidID)
			return
		}
		e, ok := parseRangeEnd(string(argv[i+1]))
		if !ok {
			ctx.enc().WriteError(errStreamInvalidID)
			return
		}
		n, ok := parseInteger(argv[i+2])
		if !ok {
			ctx.enc().WriteError(errStreamCountPos)
			return
		}
		start, end, count = s, e, n
		i += 3
		if i < len(argv) {
			consumer = string(argv[i])
			hasFilter = true
			i++
		}
		if i != len(argv) {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}

	now := keyspace.NowMillis()
	var (
		wrongTyp    bool
		noGroup     bool
		g           *group
		minID       streamID
		maxID       streamID
		collSummary bool
		rows        []pendingRow
	)
	if !ctx.view(func(db *keyspace.DB) error {
		// The summary reads the header alone: on a coll stream the group's pending
		// total is a header counter and the min and max IDs come from a two-row
		// cursor scan, so a never-acked million-record group costs one seek; on a
		// blob stream the header-only load already decodes the small inline PEL. The
		// extended reply still scans the whole PEL by range, so it keeps the full
		// load below.
		if summary {
			s, hdr, found, err := getStreamGroups(db, key)
			if err != nil {
				return err
			}
			if !found || hdr.Type != keyspace.TypeStream {
				if found {
					wrongTyp = true
				} else {
					noGroup = true
				}
				return nil
			}
			g = s.findGroup(groupName)
			if g == nil {
				noGroup = true
				return nil
			}
			if !hdr.IsColl() {
				// The blob load carries the full PEL, so the in-memory summary serves.
				return nil
			}
			collSummary = true
			if g.pending > 0 {
				mn, mx, ok, err := streamCollPELBounds(db, key, groupName)
				if err != nil {
					return err
				}
				if ok {
					minID, maxID = mn, mx
				}
			}
			return nil
		}
		// The extended reply scans the PEL by id range. On the coll form the rows live
		// in the group's 0x02 siblings, so a header-only load finds the group and a
		// cursor seeks straight to the range start; the walk costs the scanned window,
		// not the whole pending list. The blob load already carries the inline PEL.
		s, hdr, found, err := getStreamGroups(db, key)
		if err != nil {
			return err
		}
		if !found || hdr.Type != keyspace.TypeStream {
			if found {
				wrongTyp = true
			} else {
				noGroup = true
			}
			return nil
		}
		g = s.findGroup(groupName)
		if g == nil {
			noGroup = true
			return nil
		}
		if hdr.IsColl() {
			rows, err = collectPendingFromColl(db, key, groupName, idle, start, end, count, consumer, hasFilter, now)
			return err
		}
		rows = collectPendingFromPEL(g, idle, start, end, count, consumer, hasFilter, now)
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if noGroup {
		ctx.enc().WriteError(nogroupError(groupName, string(key)))
		return
	}
	if summary {
		if collSummary {
			writePendingSummaryColl(ctx.enc(), g, minID, maxID)
			return
		}
		writePendingSummary(ctx.enc(), g)
		return
	}
	writePendingRows(ctx.enc(), rows)
}

// writePendingSummary writes the four-element XPENDING summary.
func writePendingSummary(enc *resp.Encoder, g *group) {
	enc.WriteArrayLen(4)
	enc.WriteInteger(int64(len(g.pel)))
	if len(g.pel) == 0 {
		enc.WriteNull()
		enc.WriteNull()
		enc.WriteNullArray()
		return
	}
	enc.WriteBulkStringStr(g.pel[0].id.String())
	enc.WriteBulkStringStr(g.pel[len(g.pel)-1].id.String())
	// Per-consumer counts in consumer order.
	counts := map[string]int64{}
	var order []string
	for _, pe := range g.pel {
		if _, seen := counts[pe.consumer]; !seen {
			order = append(order, pe.consumer)
		}
		counts[pe.consumer]++
	}
	enc.WriteArrayLen(len(order))
	for _, name := range order {
		enc.WriteArrayLen(2)
		enc.WriteBulkStringStr(name)
		enc.WriteBulkStringStr(strconv.FormatInt(counts[name], 10))
	}
}

// writePendingSummaryColl writes the XPENDING summary for a coll-form stream from
// the header counters and the two end-of-PEL IDs, never touching the pending list
// itself. The total comes from g.pending, the min and max from the bounded cursor
// scan the caller already ran, and the per-consumer breakdown from each consumer's
// pending counter in name order, skipping consumers with nothing pending (the order
// Redis reports its consumer-group summary in).
func writePendingSummaryColl(enc *resp.Encoder, g *group, minID, maxID streamID) {
	enc.WriteArrayLen(4)
	enc.WriteInteger(int64(g.pending))
	if g.pending == 0 {
		enc.WriteNull()
		enc.WriteNull()
		enc.WriteNullArray()
		return
	}
	enc.WriteBulkStringStr(minID.String())
	enc.WriteBulkStringStr(maxID.String())
	var n int
	for _, c := range g.consumers {
		if c.pending > 0 {
			n++
		}
	}
	enc.WriteArrayLen(n)
	for _, c := range g.consumers {
		if c.pending == 0 {
			continue
		}
		enc.WriteArrayLen(2)
		enc.WriteBulkStringStr(c.name)
		enc.WriteBulkStringStr(strconv.FormatUint(c.pending, 10))
	}
}

// pendingRow is one detailed XPENDING entry: the pending record and its idle time.
type pendingRow struct {
	pe      pelEntry
	elapsed int64
}

// collectPendingFromPEL gathers the detailed XPENDING rows from an in-memory PEL
// (the blob form, where the whole small PEL is already decoded). The PEL is sorted
// by id, so the [start, end] range and the COUNT cap bound the walk.
func collectPendingFromPEL(g *group, idle int64, start, end rangeBound, count int64, consumer string, hasFilter bool, now int64) []pendingRow {
	var rows []pendingRow
	for _, pe := range g.pel {
		if pe.id.less(start.id) || end.id.less(pe.id) {
			continue
		}
		if hasFilter && pe.consumer != consumer {
			continue
		}
		el := elapsedSince(now, pe.deliveryTime)
		if el < idle {
			continue
		}
		rows = append(rows, pendingRow{pe: pe, elapsed: el})
		if count >= 0 && int64(len(rows)) >= count {
			break
		}
	}
	return rows
}

// collectPendingFromColl gathers the detailed XPENDING rows by scanning the group's
// 0x02 PEL rows from the start id forward, stopping at the end id or the COUNT cap.
// The rows are keyed by id, so the cursor seeks straight to the range start and the
// walk costs the scanned window, never the whole pending list. The caller has
// confirmed the key is a coll stream.
func collectPendingFromColl(db *keyspace.DB, key []byte, group string, idle int64, start, end rangeBound, count int64, consumer string, hasFilter bool, now int64) ([]pendingRow, error) {
	var rows []pendingRow
	prefix := streamPELPrefix(group)
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		c := r.Cursor()
		if e := c.Seek(streamPELRow(group, start.id)); e != nil {
			return e
		}
		for c.Valid() {
			k := c.Key()
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			id := streamPELRowID(k, len(prefix))
			if end.id.less(id) {
				break
			}
			pe, e := streamPELFromValue(c.Value())
			if e != nil {
				return e
			}
			pe.id = id
			if !hasFilter || pe.consumer == consumer {
				el := elapsedSince(now, pe.deliveryTime)
				if el >= idle {
					rows = append(rows, pendingRow{pe: pe, elapsed: el})
					if count >= 0 && int64(len(rows)) >= count {
						break
					}
				}
			}
			if e := c.Next(); e != nil {
				return e
			}
		}
		return nil
	})
	return rows, err
}

// writePendingRows writes the detailed XPENDING reply from the collected rows.
func writePendingRows(enc *resp.Encoder, rows []pendingRow) {
	enc.WriteArrayLen(len(rows))
	for _, r := range rows {
		if enc.Proto() >= 3 {
			enc.WriteMapLen(4)
			enc.WriteBulkStringStr("entry-id")
			enc.WriteBulkStringStr(r.pe.id.String())
			enc.WriteBulkStringStr("owner")
			enc.WriteBulkStringStr(r.pe.consumer)
			enc.WriteBulkStringStr("elapsed")
			enc.WriteInteger(r.elapsed)
			enc.WriteBulkStringStr("delivery-count")
			enc.WriteInteger(int64(r.pe.deliveryCount))
			continue
		}
		enc.WriteArrayLen(4)
		enc.WriteBulkStringStr(r.pe.id.String())
		enc.WriteBulkStringStr(r.pe.consumer)
		enc.WriteInteger(r.elapsed)
		enc.WriteInteger(int64(r.pe.deliveryCount))
	}
}

// claimOpts holds the parsed XCLAIM and XAUTOCLAIM modifiers.
type claimOpts struct {
	idleSet  bool
	idleMs   int64
	timeSet  bool
	timeMs   int64
	retrySet bool
	retry    int64
	force    bool
	justID   bool
	lastIDOK bool
	lastID   streamID
}

// claimDeliveryTime computes the new delivery_time for a claimed entry.
func (o claimOpts) claimDeliveryTime(now int64) int64 {
	switch {
	case o.timeSet:
		return o.timeMs
	case o.idleSet:
		return now - o.idleMs
	default:
		return now
	}
}

// claimDeliveryCount computes the new delivery_count for a claimed entry given
// its previous count and whether the PEL record was just created by FORCE.
func (o claimOpts) claimDeliveryCount(prev uint64, created bool) uint64 {
	if o.retrySet {
		return uint64(o.retry)
	}
	if o.justID {
		if created {
			return 1
		}
		return prev
	}
	if created {
		return 1
	}
	return prev + 1
}

// handleXClaim implements XCLAIM key group consumer min-idle-time id [id ...] [opts].
func handleXClaim(ctx *Ctx) {
	argv := ctx.Argv
	key := argv[1]
	groupName := string(argv[2])
	consumerName := string(argv[3])
	minIdle, ok := parseInteger(argv[4])
	if !ok || minIdle < 0 {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}

	i := 5
	var ids []streamID
	for i < len(argv) {
		id, ok := parseStreamID(string(argv[i]), 0)
		if !ok {
			break
		}
		ids = append(ids, id)
		i++
	}
	if len(ids) == 0 {
		ctx.enc().WriteError(errStreamInvalidID)
		return
	}
	opts, errStr := parseClaimOpts(argv[i:])
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	now := keyspace.NowMillis()

	var (
		wrongTyp bool
		noGroup  bool
		claimed  []streamEntry
		justIDs  []streamID
	)
	if !ctx.updateShard(key, func(db *keyspace.DB) error {
		// A coll claim reads the header alone and reaches each named entry and its PEL
		// record through point lookups, so its cost is the number of ids claimed, not
		// the log or the whole pending list. A blob claim works the small inline copy.
		coll, err := streamHeaderIsColl(db, key)
		if err != nil {
			return err
		}
		load := getStreamGroups
		if !coll {
			load = getStreamGroupsFull
		}
		s, hdr, found, err := load(db, key)
		if err != nil {
			return err
		}
		if !found || hdr.Type != keyspace.TypeStream {
			if found {
				wrongTyp = true
			} else {
				noGroup = true
			}
			return nil
		}
		g := s.findGroup(groupName)
		if g == nil {
			noGroup = true
			return nil
		}
		if opts.lastIDOK && g.lastID.less(opts.lastID) {
			g.lastID = opts.lastID
		}
		c, _ := g.getOrCreateConsumer(consumerName, now)
		c.seenTime = now
		c.activeTime = now
		// applyClaim runs the per-id claim decision against a PEL accessor: get the
		// current record, skip if it is too fresh or absent without FORCE, drop it if
		// the entry was deleted, otherwise (re)write it to this consumer.
		applyClaim := func(id streamID, body [][]byte, exists bool, pe pelEntry, has bool,
			put func(pelEntry) error, del func() error) error {
			created := false
			var prev uint64
			if !has {
				if !opts.force || !exists {
					return nil
				}
				created = true
			} else {
				if elapsedSince(now, pe.deliveryTime) < minIdle {
					return nil
				}
				prev = pe.deliveryCount
			}
			if !exists {
				if has {
					// The entry was deleted under the consumer; the PEL record goes with
					// it, so drop it from the group and its old owner's counters.
					if g.pending > 0 {
						g.pending--
					}
					if oc := g.findConsumer(pe.consumer); oc != nil && oc.pending > 0 {
						oc.pending--
					}
					return del()
				}
				return nil
			}
			// A FORCE-created record adds to the group; a reclaim from a different
			// consumer moves one pending record between owners (the group total is
			// unchanged). The counters back the bounded XPENDING summary and XINFO.
			if created {
				g.pending++
				c.pending++
			} else if pe.consumer != consumerName {
				if oc := g.findConsumer(pe.consumer); oc != nil && oc.pending > 0 {
					oc.pending--
				}
				c.pending++
			}
			if err := put(pelEntry{
				id:            id,
				consumer:      consumerName,
				deliveryTime:  opts.claimDeliveryTime(now),
				deliveryCount: opts.claimDeliveryCount(prev, created),
			}); err != nil {
				return err
			}
			if opts.justID {
				justIDs = append(justIDs, id)
			} else {
				claimed = append(claimed, streamEntry{id: id, fields: body})
			}
			return nil
		}
		if coll {
			return db.CollUpdate(key, keyspace.TypeStream, keyspace.EncStream, func(w *keyspace.CollWriter) error {
				for _, id := range ids {
					body, exists, err := streamCollEntry(w, id)
					if err != nil {
						return err
					}
					pe, has, err := streamPELGet(w, groupName, id)
					if err != nil {
						return err
					}
					if err := applyClaim(id, body, exists, pe, has,
						func(np pelEntry) error { return streamPELPut(w, groupName, np) },
						func() error { _, e := streamPELDelete(w, groupName, id); return e },
					); err != nil {
						return err
					}
				}
				_, e := w.Put([]byte{streamRowHeader}, streamHeaderValue(s))
				return e
			})
		}
		for _, id := range ids {
			idx := s.findEntry(id)
			body, exists := [][]byte(nil), idx >= 0
			if exists {
				body = s.entries[idx].fields
			}
			pidx := g.pelIndex(id)
			var pe pelEntry
			if pidx >= 0 {
				pe = g.pel[pidx]
			}
			if err := applyClaim(id, body, exists, pe, pidx >= 0,
				func(np pelEntry) error { g.pelInsert(np); return nil },
				func() error { g.pel = append(g.pel[:pidx], g.pel[pidx+1:]...); return nil },
			); err != nil {
				return err
			}
		}
		return storeStreamGroups(db, key, s, keepTTL(hdr, found))
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if noGroup {
		ctx.enc().WriteError(nogroupError(groupName, string(key)))
		return
	}
	if len(claimed) > 0 || len(justIDs) > 0 {
		ctx.notify(notifyStream, "xclaim", key)
	}
	if opts.justID {
		ctx.enc().WriteArrayLen(len(justIDs))
		for _, id := range justIDs {
			ctx.enc().WriteBulkStringStr(id.String())
		}
		return
	}
	writeEntries(ctx.enc(), claimed)
}

// parseClaimOpts parses the XCLAIM trailing options.
func parseClaimOpts(args [][]byte) (claimOpts, string) {
	var o claimOpts
	i := 0
	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "IDLE":
			if i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			v, ok := parseInteger(args[i+1])
			if !ok || v < 0 {
				return o, "ERR value is not an integer or out of range"
			}
			o.idleSet, o.idleMs = true, v
			i += 2
		case "TIME":
			if i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			v, ok := parseInteger(args[i+1])
			if !ok {
				return o, "ERR value is not an integer or out of range"
			}
			o.timeSet, o.timeMs = true, v
			i += 2
		case "RETRYCOUNT":
			if i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			v, ok := parseInteger(args[i+1])
			if !ok || v < 0 {
				return o, "ERR value is not an integer or out of range"
			}
			o.retrySet, o.retry = true, v
			i += 2
		case "FORCE":
			o.force = true
			i++
		case "JUSTID":
			o.justID = true
			i++
		case "LASTID":
			if i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			id, ok := parseStreamID(string(args[i+1]), 0)
			if !ok {
				return o, errStreamInvalidID
			}
			o.lastIDOK, o.lastID = true, id
			i += 2
		default:
			return o, "ERR syntax error"
		}
	}
	return o, ""
}

// handleXAutoClaim implements XAUTOCLAIM key group consumer min-idle-time start
// [COUNT count] [JUSTID].
func handleXAutoClaim(ctx *Ctx) {
	argv := ctx.Argv
	key := argv[1]
	groupName := string(argv[2])
	consumerName := string(argv[3])
	minIdle, ok := parseInteger(argv[4])
	if !ok || minIdle < 0 {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	start, ok := parseStreamID(string(argv[5]), 0)
	if !ok {
		ctx.enc().WriteError(errStreamInvalidID)
		return
	}
	count := int64(100)
	justID := false
	i := 6
	for i < len(argv) {
		switch strings.ToUpper(string(argv[i])) {
		case "COUNT":
			if i+1 >= len(argv) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			v, okc := parseInteger(argv[i+1])
			if !okc || v <= 0 {
				ctx.enc().WriteError(errStreamReadCountER)
				return
			}
			count = v
			i += 2
		case "JUSTID":
			justID = true
			i++
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}
	now := keyspace.NowMillis()

	var (
		wrongTyp bool
		noGroup  bool
		claimed  []streamEntry
		justIDs  []streamID
		deleted  []streamID
		cursor   = streamID{}
	)
	if !ctx.updateShard(key, func(db *keyspace.DB) error {
		s, hdr, found, err := getStreamGroupsFull(db, key)
		if err != nil {
			return err
		}
		if !found || hdr.Type != keyspace.TypeStream {
			if found {
				wrongTyp = true
			} else {
				noGroup = true
			}
			return nil
		}
		g := s.findGroup(groupName)
		if g == nil {
			noGroup = true
			return nil
		}
		c, _ := g.getOrCreateConsumer(consumerName, now)
		var drop []streamID
		// autoScan walks the PEL from start, fetching each entry's body through the
		// supplied lookup. The scan is COUNT-bounded, so on the coll form it costs a
		// COUNT-bounded set of point fetches, never a walk of the whole log.
		autoScan := func(fields func(streamID) ([][]byte, bool, error)) error {
			var scanned int64
			for _, pe := range g.pel {
				if pe.id.less(start) {
					continue
				}
				if scanned >= count {
					cursor = pe.id
					break
				}
				scanned++
				body, exists, err := fields(pe.id)
				if err != nil {
					return err
				}
				if !exists {
					deleted = append(deleted, pe.id)
					drop = append(drop, pe.id)
					continue
				}
				if elapsedSince(now, pe.deliveryTime) < minIdle {
					continue
				}
				g.pelInsert(pelEntry{
					id:            pe.id,
					consumer:      consumerName,
					deliveryTime:  now,
					deliveryCount: claimAutoCount(pe.deliveryCount, justID),
				})
				if justID {
					justIDs = append(justIDs, pe.id)
				} else {
					claimed = append(claimed, streamEntry{id: pe.id, fields: body})
				}
			}
			return nil
		}
		if hdr.IsColl() {
			if _, err := db.CollRead(key, func(r *keyspace.CollReader) error {
				return autoScan(func(id streamID) ([][]byte, bool, error) {
					return streamCollEntry(r, id)
				})
			}); err != nil {
				return err
			}
		} else {
			if err := autoScan(func(id streamID) ([][]byte, bool, error) {
				idx := s.findEntry(id)
				if idx < 0 {
					return nil, false, nil
				}
				return s.entries[idx].fields, true, nil
			}); err != nil {
				return err
			}
		}
		for _, id := range drop {
			if idx := g.pelIndex(id); idx >= 0 {
				g.pel = append(g.pel[:idx], g.pel[idx+1:]...)
			}
		}
		c.seenTime = now
		c.activeTime = now
		return storeStreamGroupsFull(db, key, s, keepTTL(hdr, found))
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if noGroup {
		ctx.enc().WriteError(nogroupError(groupName, string(key)))
		return
	}
	if len(claimed) > 0 || len(justIDs) > 0 {
		ctx.notify(notifyStream, "xautoclaim", key)
	}

	enc := ctx.enc()
	enc.WriteArrayLen(3)
	enc.WriteBulkStringStr(cursor.String())
	if justID {
		enc.WriteArrayLen(len(justIDs))
		for _, id := range justIDs {
			enc.WriteBulkStringStr(id.String())
		}
	} else {
		writeEntries(enc, claimed)
	}
	enc.WriteArrayLen(len(deleted))
	for _, id := range deleted {
		enc.WriteBulkStringStr(id.String())
	}
}

// claimAutoCount returns the delivery_count after an XAUTOCLAIM claim, which
// increments unless JUSTID was given.
func claimAutoCount(prev uint64, justID bool) uint64 {
	if justID {
		return prev
	}
	return prev + 1
}
