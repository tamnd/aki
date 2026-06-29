package command

import (
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
		wrongTyp bool
		noGroup  bool
		g        *group
	)
	if !ctx.view(func(db *keyspace.DB) error {
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
		}
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
		writePendingSummary(ctx.enc(), g)
		return
	}
	writePendingExtended(ctx.enc(), g, idle, start, end, count, consumer, hasFilter, now)
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

// writePendingExtended writes the detailed XPENDING reply.
func writePendingExtended(enc *resp.Encoder, g *group, idle int64, start, end rangeBound, count int64, consumer string, hasFilter bool, now int64) {
	type row struct {
		pe      pelEntry
		elapsed int64
	}
	var rows []row
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
		rows = append(rows, row{pe: pe, elapsed: el})
		if count >= 0 && int64(len(rows)) >= count {
			break
		}
	}
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
		s, hdr, found, err := getStream(db, key)
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
		for _, id := range ids {
			idx := g.pelIndex(id)
			created := false
			var prev uint64
			if idx < 0 {
				if !opts.force || s.findEntry(id) < 0 {
					continue
				}
				created = true
			} else {
				if elapsedSince(now, g.pel[idx].deliveryTime) < minIdle {
					continue
				}
				prev = g.pel[idx].deliveryCount
			}
			// An entry deleted from the stream is dropped from the PEL, not claimed.
			if s.findEntry(id) < 0 {
				if idx >= 0 {
					g.pel = append(g.pel[:idx], g.pel[idx+1:]...)
				}
				continue
			}
			g.pelInsert(pelEntry{
				id:            id,
				consumer:      consumerName,
				deliveryTime:  opts.claimDeliveryTime(now),
				deliveryCount: opts.claimDeliveryCount(prev, created),
			})
			if opts.justID {
				justIDs = append(justIDs, id)
			} else {
				claimed = append(claimed, s.entries[s.findEntry(id)])
			}
		}
		c.seenTime = now
		c.activeTime = now
		return storeStream(db, key, s, keepTTL(hdr, found))
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
		s, hdr, found, err := getStream(db, key)
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
		var scanned int64
		var drop []streamID
		for _, pe := range g.pel {
			if pe.id.less(start) {
				continue
			}
			if scanned >= count {
				cursor = pe.id
				break
			}
			scanned++
			if s.findEntry(pe.id) < 0 {
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
				claimed = append(claimed, s.entries[s.findEntry(pe.id)])
			}
		}
		for _, id := range drop {
			if idx := g.pelIndex(id); idx >= 0 {
				g.pel = append(g.pel[:idx], g.pel[idx+1:]...)
			}
		}
		c.seenTime = now
		c.activeTime = now
		return storeStream(db, key, s, keepTTL(hdr, found))
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
