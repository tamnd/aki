package command

import (
	"sort"
	"strings"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/resp"
)

// findGroup returns the group with the given name, or nil.
func (s *stream) findGroup(name string) *group {
	for _, g := range s.groups {
		if g.name == name {
			return g
		}
	}
	return nil
}

// addGroup inserts a group keeping the slice sorted by name.
func (s *stream) addGroup(g *group) {
	i := sort.Search(len(s.groups), func(i int) bool { return s.groups[i].name >= g.name })
	s.groups = append(s.groups, nil)
	copy(s.groups[i+1:], s.groups[i:])
	s.groups[i] = g
}

// removeGroup drops the named group, reporting whether it existed.
func (s *stream) removeGroup(name string) bool {
	for i, g := range s.groups {
		if g.name == name {
			s.groups = append(s.groups[:i], s.groups[i+1:]...)
			return true
		}
	}
	return false
}

// findConsumer returns the named consumer, or nil.
func (g *group) findConsumer(name string) *consumer {
	for _, c := range g.consumers {
		if c.name == name {
			return c
		}
	}
	return nil
}

// getOrCreateConsumer returns the named consumer, creating it when absent. The
// bool reports whether a new consumer was created.
func (g *group) getOrCreateConsumer(name string, now int64) (*consumer, bool) {
	if c := g.findConsumer(name); c != nil {
		return c, false
	}
	c := &consumer{name: name, seenTime: now, activeTime: now}
	i := sort.Search(len(g.consumers), func(i int) bool { return g.consumers[i].name >= name })
	g.consumers = append(g.consumers, nil)
	copy(g.consumers[i+1:], g.consumers[i:])
	g.consumers[i] = c
	return c, true
}

// pelIndex returns the index of the PEL entry with the given ID, or -1.
func (g *group) pelIndex(id streamID) int {
	lo, hi := 0, len(g.pel)
	for lo < hi {
		mid := (lo + hi) / 2
		if g.pel[mid].id.less(id) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(g.pel) && g.pel[lo].id.equal(id) {
		return lo
	}
	return -1
}

// pelInsert adds or replaces a PEL entry, keeping the list sorted by ID.
func (g *group) pelInsert(pe pelEntry) {
	lo, hi := 0, len(g.pel)
	for lo < hi {
		mid := (lo + hi) / 2
		if g.pel[mid].id.less(pe.id) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(g.pel) && g.pel[lo].id.equal(pe.id) {
		g.pel[lo] = pe
		return
	}
	g.pel = append(g.pel, pelEntry{})
	copy(g.pel[lo+1:], g.pel[lo:])
	g.pel[lo] = pe
}

// consumerPending returns the number of PEL entries owned by a consumer.
func (g *group) consumerPending(name string) int {
	n := 0
	for _, pe := range g.pel {
		if pe.consumer == name {
			n++
		}
	}
	return n
}

// nogroupError formats the NOGROUP reply for a missing group on a key.
func nogroupError(group, key string) string {
	return "NOGROUP No such consumer group '" + group + "' for key name '" + key + "'"
}

// resolveGroupID turns an XGROUP CREATE or SETID id argument into a concrete ID.
// $ becomes the stream's last ID; 0 and 0-0 become 0-0; otherwise it parses.
func resolveGroupID(s *stream, raw string) (streamID, bool, bool) {
	if raw == "$" {
		return s.lastID, true, true
	}
	id, ok := parseStreamID(raw, 0)
	return id, ok, false
}

// handleXGroup routes the XGROUP subcommands.
func handleXGroup(ctx *Ctx) {
	argv := ctx.Argv
	if len(argv) < 2 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'xgroup' command")
		return
	}
	switch strings.ToUpper(string(argv[1])) {
	case "CREATE":
		handleXGroupCreate(ctx)
	case "SETID":
		handleXGroupSetID(ctx)
	case "CREATECONSUMER":
		handleXGroupConsumer(ctx, true)
	case "DELCONSUMER":
		handleXGroupConsumer(ctx, false)
	case "DESTROY":
		handleXGroupDestroy(ctx)
	default:
		ctx.enc().WriteError("ERR Unknown XGROUP subcommand or wrong number of arguments for '" + string(argv[1]) + "'")
	}
}

// handleXGroupCreate implements XGROUP CREATE key group id [MKSTREAM] [ENTRIESREAD n].
func handleXGroupCreate(ctx *Ctx) {
	argv := ctx.Argv
	if len(argv) < 5 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'xgroup' command")
		return
	}
	key, groupName, rawID := argv[2], string(argv[3]), string(argv[4])
	mkStream := false
	setEntriesRead := false
	var entriesRead uint64
	i := 5
	for i < len(argv) {
		switch strings.ToUpper(string(argv[i])) {
		case "MKSTREAM":
			mkStream = true
			i++
		case "ENTRIESREAD":
			if i+1 >= len(argv) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			v, ok := parseInteger(argv[i+1])
			if !ok || v < 0 {
				ctx.enc().WriteError("ERR value is not an integer or out of range")
				return
			}
			entriesRead = uint64(v)
			setEntriesRead = true
			i += 2
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}
	if rawID == "$" && setEntriesRead {
		ctx.enc().WriteError("ERR The $ ID is not valid when ENTRIESREAD is specified")
		return
	}

	var (
		wrongTyp bool
		noKey    bool
		busy     bool
		badID    bool
	)
	if !ctx.update(func(db *keyspace.DB) error {
		s, hdr, found, err := getStream(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeStream {
			wrongTyp = true
			return nil
		}
		if !found {
			if !mkStream {
				noKey = true
				return nil
			}
			s = &stream{}
		}
		if s.findGroup(groupName) != nil {
			busy = true
			return nil
		}
		id, ok, _ := resolveGroupID(s, rawID)
		if !ok {
			badID = true
			return nil
		}
		g := &group{name: groupName, lastID: id, entriesRead: entriesRead}
		s.addGroup(g)
		ttl := int64(-1)
		if found {
			ttl = keepTTL(hdr, found)
		}
		return storeStream(db, key, s, ttl)
	}) {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case noKey:
		ctx.enc().WriteError(errStreamNoSuchKey)
	case busy:
		ctx.enc().WriteError("BUSYGROUP Consumer Group name already exists")
	case badID:
		ctx.enc().WriteError(errStreamInvalidID)
	default:
		ctx.notify(notifyStream, "xgroup-create", key)
		ctx.enc().WriteStatus("OK")
	}
}

// handleXGroupSetID implements XGROUP SETID key group id [ENTRIESREAD n].
func handleXGroupSetID(ctx *Ctx) {
	argv := ctx.Argv
	if len(argv) < 5 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'xgroup' command")
		return
	}
	key, groupName, rawID := argv[2], string(argv[3]), string(argv[4])
	setEntriesRead := false
	var entriesRead uint64
	i := 5
	for i < len(argv) {
		if strings.EqualFold(string(argv[i]), "ENTRIESREAD") && i+1 < len(argv) {
			v, ok := parseInteger(argv[i+1])
			if !ok || v < 0 {
				ctx.enc().WriteError("ERR value is not an integer or out of range")
				return
			}
			entriesRead = uint64(v)
			setEntriesRead = true
			i += 2
			continue
		}
		ctx.enc().WriteError("ERR syntax error")
		return
	}

	var (
		wrongTyp bool
		noGroup  bool
		badID    bool
	)
	if !ctx.update(func(db *keyspace.DB) error {
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
		id, ok, _ := resolveGroupID(s, rawID)
		if !ok {
			badID = true
			return nil
		}
		g.lastID = id
		if setEntriesRead {
			g.entriesRead = entriesRead
		}
		return storeStream(db, key, s, keepTTL(hdr, found))
	}) {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case noGroup:
		ctx.enc().WriteError(nogroupError(groupName, string(key)))
	case badID:
		ctx.enc().WriteError(errStreamInvalidID)
	default:
		ctx.notify(notifyStream, "xgroup-setid", key)
		ctx.enc().WriteStatus("OK")
	}
}

// handleXGroupConsumer implements XGROUP CREATECONSUMER and DELCONSUMER.
func handleXGroupConsumer(ctx *Ctx, create bool) {
	argv := ctx.Argv
	if len(argv) != 5 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'xgroup' command")
		return
	}
	key, groupName, consumerName := argv[2], string(argv[3]), string(argv[4])
	now := keyspace.NowMillis()

	var (
		wrongTyp bool
		noGroup  bool
		result   int64
	)
	if !ctx.update(func(db *keyspace.DB) error {
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
		if create {
			_, made := g.getOrCreateConsumer(consumerName, now)
			if made {
				result = 1
			}
		} else {
			result = removeConsumer(g, consumerName)
		}
		return storeStream(db, key, s, keepTTL(hdr, found))
	}) {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case noGroup:
		ctx.enc().WriteError(nogroupError(groupName, string(key)))
	case create:
		if result == 1 {
			ctx.notify(notifyStream, "xgroup-createconsumer", key)
		}
		ctx.enc().WriteInteger(result)
	default:
		ctx.notify(notifyStream, "xgroup-delconsumer", key)
		ctx.enc().WriteInteger(result)
	}
}

// removeConsumer drops a consumer and its PEL entries, returning the count of
// pending entries removed.
func removeConsumer(g *group, name string) int64 {
	var removed int64
	kept := g.pel[:0]
	for _, pe := range g.pel {
		if pe.consumer == name {
			removed++
			continue
		}
		kept = append(kept, pe)
	}
	g.pel = kept
	for i, c := range g.consumers {
		if c.name == name {
			g.consumers = append(g.consumers[:i], g.consumers[i+1:]...)
			break
		}
	}
	return removed
}

// handleXGroupDestroy implements XGROUP DESTROY key group.
func handleXGroupDestroy(ctx *Ctx) {
	argv := ctx.Argv
	if len(argv) != 4 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'xgroup' command")
		return
	}
	key, groupName := argv[2], string(argv[3])

	var (
		wrongTyp bool
		result   int64
	)
	if !ctx.update(func(db *keyspace.DB) error {
		s, hdr, found, err := getStream(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeStream {
			wrongTyp = true
			return nil
		}
		if s.removeGroup(groupName) {
			result = 1
		}
		return storeStream(db, key, s, keepTTL(hdr, found))
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if result == 1 {
		ctx.notify(notifyStream, "xgroup-destroy", key)
	}
	ctx.enc().WriteInteger(result)
}

// handleXAck implements XACK key group id [id ...].
func handleXAck(ctx *Ctx) {
	argv := ctx.Argv
	key, groupName := argv[1], string(argv[2])
	ids := make([]streamID, 0, len(argv)-3)
	for _, raw := range argv[3:] {
		id, ok := parseStreamID(string(raw), 0)
		if !ok {
			ctx.enc().WriteError(errStreamInvalidID)
			return
		}
		ids = append(ids, id)
	}
	now := keyspace.NowMillis()

	var (
		wrongTyp bool
		noGroup  bool
		acked    int64
	)
	if !ctx.update(func(db *keyspace.DB) error {
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
		for _, id := range ids {
			idx := g.pelIndex(id)
			if idx < 0 {
				continue
			}
			owner := g.pel[idx].consumer
			g.pel = append(g.pel[:idx], g.pel[idx+1:]...)
			if c := g.findConsumer(owner); c != nil {
				c.activeTime = now
			}
			acked++
		}
		if acked == 0 {
			return nil
		}
		return storeStream(db, key, s, keepTTL(hdr, found))
	}) {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case noGroup:
		ctx.enc().WriteError(nogroupError(groupName, string(key)))
	default:
		ctx.enc().WriteInteger(acked)
	}
}

// handleXReadGroup implements XREADGROUP GROUP g c [COUNT n] [BLOCK ms] [NOACK]
// STREAMS key [key ...] id [id ...]. With BLOCK and only > IDs that have no new
// entries the connection parks until an XADD on one of the keys, the timeout
// elapses, or the client is unblocked. An explicit ID never parks.
func handleXReadGroup(ctx *Ctx) {
	argv := ctx.Argv
	if len(argv) < 7 || !strings.EqualFold(string(argv[1]), "GROUP") {
		ctx.enc().WriteError("ERR Missing GROUP keyword or consumer/group name in XREADGROUP")
		return
	}
	groupName := string(argv[2])
	consumerName := string(argv[3])
	i := 4
	count := int64(-1)
	blockMs := int64(-1) // -1 means no BLOCK option
	noAck := false
	for i < len(argv) {
		switch strings.ToUpper(string(argv[i])) {
		case "COUNT":
			if i+1 >= len(argv) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			c, ok := parseInteger(argv[i+1])
			if !ok || c <= 0 {
				ctx.enc().WriteError(errStreamReadCountER)
				return
			}
			count = c
			i += 2
		case "BLOCK":
			if i+1 >= len(argv) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			ms, ok := parseInteger(argv[i+1])
			if !ok {
				ctx.enc().WriteError("ERR timeout is not an integer or out of range")
				return
			}
			if ms < 0 {
				ctx.enc().WriteError(errStreamTimeoutNeg)
				return
			}
			blockMs = ms
			i += 2
		case "NOACK":
			noAck = true
			i++
		case "STREAMS":
			i++
			handleXReadGroupStreams(ctx, groupName, consumerName, argv[i:], count, blockMs, noAck)
			return
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}
	ctx.enc().WriteError("ERR syntax error")
}

// handleXReadGroupStreams reads the STREAMS clause and delivers per stream. When
// blockMs is not -1 and only > IDs are given with no new entries, it parks.
func handleXReadGroupStreams(ctx *Ctx, groupName, consumerName string, rest [][]byte, count, blockMs int64, noAck bool) {
	if len(rest) == 0 || len(rest)%2 != 0 {
		ctx.enc().WriteError(errStreamUnbalanced)
		return
	}
	n := len(rest) / 2
	keys := rest[:n]
	idArgs := rest[n:]

	newDelivery := make([]bool, n)
	starts := make([]streamID, n)
	anyExplicit := false
	for j := range n {
		raw := string(idArgs[j])
		if raw == ">" {
			newDelivery[j] = true
			continue
		}
		anyExplicit = true
		if noAck {
			ctx.enc().WriteError("ERR The NOACK option is not valid for XREADGROUP with an explicit ID")
			return
		}
		id, ok := parseStreamID(raw, 0)
		if !ok {
			ctx.enc().WriteError(errStreamInvalidID)
			return
		}
		starts[j] = id
	}

	attempt := func() bool {
		var (
			results  []readGroupResult
			wrongTyp bool
			noGroup  bool
			nogKey   string
		)
		now := keyspace.NowMillis()
		if !ctx.update(func(db *keyspace.DB) error {
			for j := range n {
				s, hdr, found, err := getStream(db, keys[j])
				if err != nil {
					return err
				}
				if found && hdr.Type != keyspace.TypeStream {
					wrongTyp = true
					return nil
				}
				if !found {
					noGroup = true
					nogKey = string(keys[j])
					return nil
				}
				g := s.findGroup(groupName)
				if g == nil {
					noGroup = true
					nogKey = string(keys[j])
					return nil
				}
				c, _ := g.getOrCreateConsumer(consumerName, now)
				c.seenTime = now
				if newDelivery[j] {
					es := collectRange(s, rangeBound{id: g.lastID, excl: true}, rangeBound{id: maxStreamID}, count)
					if len(es) == 0 {
						continue
					}
					c.activeTime = now
					rows := make([]readEntry, 0, len(es))
					for _, e := range es {
						g.lastID = e.id
						g.entriesRead++
						if !noAck {
							g.pelInsert(pelEntry{id: e.id, consumer: consumerName, deliveryTime: now, deliveryCount: 1})
						}
						rows = append(rows, readEntry{id: e.id, fields: e.fields})
					}
					if err := storeStream(db, keys[j], s, keepTTL(hdr, found)); err != nil {
						return err
					}
					results = append(results, readGroupResult{key: keys[j], rows: rows})
				} else {
					rows := collectConsumerPEL(s, g, consumerName, starts[j], count)
					// Creating the consumer above may need persisting.
					if err := storeStream(db, keys[j], s, keepTTL(hdr, found)); err != nil {
						return err
					}
					results = append(results, readGroupResult{key: keys[j], rows: rows, explicit: true})
				}
			}
			return nil
		}) {
			return true
		}
		if wrongTyp {
			ctx.enc().WriteError(wrongTypeError)
			return true
		}
		if noGroup {
			ctx.enc().WriteError(nogroupError(groupName, nogKey))
			return true
		}
		// An explicit ID always yields a per-stream list, even an empty one, so a
		// read with any explicit ID resolves at once. A > read with nothing new
		// returns no results and parks when blocking.
		if len(results) == 0 && !anyExplicit {
			return false
		}
		writeReadGroupResults(ctx, results)
		return true
	}

	if blockMs < 0 {
		if !attempt() {
			ctx.enc().WriteNullArray()
		}
		return
	}
	ctx.d.blockDrive(ctx, keys, float64(blockMs)/1000, attempt, func() { ctx.enc().WriteNullArray() })
}

// readEntry is one row of an XREADGROUP reply: an entry, or a tombstone with a
// null fields array when the underlying entry was deleted.
type readEntry struct {
	id     streamID
	fields [][]byte
	nilF   bool
}

// readGroupResult is the per-stream slice of an XREADGROUP reply.
type readGroupResult struct {
	key      []byte
	rows     []readEntry
	explicit bool
}

// collectConsumerPEL returns the consumer's pending rows with ID greater than
// after, up to count. Entries no longer in the stream become null-field rows.
func collectConsumerPEL(s *stream, g *group, consumerName string, after streamID, count int64) []readEntry {
	var rows []readEntry
	for _, pe := range g.pel {
		if pe.consumer != consumerName {
			continue
		}
		if !after.less(pe.id) {
			continue
		}
		idx := s.findEntry(pe.id)
		if idx < 0 {
			rows = append(rows, readEntry{id: pe.id, nilF: true})
		} else {
			rows = append(rows, readEntry{id: pe.id, fields: s.entries[idx].fields})
		}
		if count > 0 && int64(len(rows)) >= count {
			break
		}
	}
	return rows
}

// writeReadGroupResults writes the XREADGROUP reply. With only > IDs and nothing
// delivered the reply is a null array; an explicit ID always yields a per-stream
// list even when empty.
func writeReadGroupResults(ctx *Ctx, results []readGroupResult) {
	enc := ctx.enc()
	if len(results) == 0 {
		enc.WriteNullArray()
		return
	}
	if enc.Proto() >= 3 {
		enc.WriteMapLen(len(results))
		for _, r := range results {
			enc.WriteBulkString(r.key)
			writeReadRows(enc, r.rows)
		}
		return
	}
	enc.WriteArrayLen(len(results))
	for _, r := range results {
		enc.WriteArrayLen(2)
		enc.WriteBulkString(r.key)
		writeReadRows(enc, r.rows)
	}
}

// writeReadRows writes a list of read rows in the [id, [f, v, ...]] shape, using
// a null fields array for tombstone rows.
func writeReadRows(enc *resp.Encoder, rows []readEntry) {
	enc.WriteArrayLen(len(rows))
	for _, row := range rows {
		enc.WriteArrayLen(2)
		enc.WriteBulkStringStr(row.id.String())
		if row.nilF {
			enc.WriteNullArray()
			continue
		}
		enc.WriteArrayLen(len(row.fields))
		for _, f := range row.fields {
			enc.WriteBulkString(f)
		}
	}
}

// loadStreamForInfo fetches a stream for the read-only XINFO subcommands, setting
// the WRONGTYPE or no-key flag instead of returning an error.
func loadStreamForInfo(ctx *Ctx, key []byte) (*stream, bool) {
	var (
		snap     *stream
		wrongTyp bool
		noKey    bool
	)
	if !ctx.view(func(db *keyspace.DB) error {
		s, hdr, found, err := getStream(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeStream {
			wrongTyp = true
			return nil
		}
		if !found {
			noKey = true
			return nil
		}
		snap = s
		return nil
	}) {
		return nil, false
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return nil, false
	}
	if noKey {
		ctx.enc().WriteError(errStreamNoSuchKey)
		return nil, false
	}
	return snap, true
}

// handleXInfoGroups implements XINFO GROUPS key.
func handleXInfoGroups(ctx *Ctx) {
	if len(ctx.Argv) != 3 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'xinfo' command")
		return
	}
	s, ok := loadStreamForInfo(ctx, ctx.Argv[2])
	if !ok {
		return
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(s.groups))
	for _, g := range s.groups {
		writeGroupInfo(enc, s, g)
	}
}

// writeGroupInfo writes one XINFO GROUPS descriptor.
func writeGroupInfo(enc *resp.Encoder, s *stream, g *group) {
	if enc.Proto() >= 3 {
		enc.WriteMapLen(6)
	} else {
		enc.WriteArrayLen(12)
	}
	enc.WriteBulkStringStr("name")
	enc.WriteBulkStringStr(g.name)
	enc.WriteBulkStringStr("consumers")
	enc.WriteInteger(int64(len(g.consumers)))
	enc.WriteBulkStringStr("pending")
	enc.WriteInteger(int64(len(g.pel)))
	enc.WriteBulkStringStr("last-delivered-id")
	enc.WriteBulkStringStr(g.lastID.String())
	enc.WriteBulkStringStr("entries-read")
	enc.WriteInteger(int64(g.entriesRead))
	enc.WriteBulkStringStr("lag")
	enc.WriteInteger(int64(s.entriesAdded - g.entriesRead))
}

// handleXInfoConsumers implements XINFO CONSUMERS key group.
func handleXInfoConsumers(ctx *Ctx) {
	if len(ctx.Argv) != 4 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'xinfo' command")
		return
	}
	key := ctx.Argv[2]
	groupName := string(ctx.Argv[3])
	s, ok := loadStreamForInfo(ctx, key)
	if !ok {
		return
	}
	g := s.findGroup(groupName)
	if g == nil {
		ctx.enc().WriteError(nogroupError(groupName, string(key)))
		return
	}
	now := keyspace.NowMillis()
	enc := ctx.enc()
	enc.WriteArrayLen(len(g.consumers))
	for _, c := range g.consumers {
		if enc.Proto() >= 3 {
			enc.WriteMapLen(4)
		} else {
			enc.WriteArrayLen(8)
		}
		enc.WriteBulkStringStr("name")
		enc.WriteBulkStringStr(c.name)
		enc.WriteBulkStringStr("pending")
		enc.WriteInteger(int64(g.consumerPending(c.name)))
		enc.WriteBulkStringStr("idle")
		enc.WriteInteger(now - c.seenTime)
		enc.WriteBulkStringStr("inactive")
		enc.WriteInteger(now - c.activeTime)
	}
}

// writeFullGroups writes the groups array embedded in XINFO STREAM FULL.
func writeFullGroups(enc *resp.Encoder, s *stream) {
	enc.WriteArrayLen(len(s.groups))
	for _, g := range s.groups {
		if enc.Proto() >= 3 {
			enc.WriteMapLen(6)
		} else {
			enc.WriteArrayLen(12)
		}
		enc.WriteBulkStringStr("name")
		enc.WriteBulkStringStr(g.name)
		enc.WriteBulkStringStr("last-delivered-id")
		enc.WriteBulkStringStr(g.lastID.String())
		enc.WriteBulkStringStr("entries-read")
		enc.WriteInteger(int64(g.entriesRead))
		enc.WriteBulkStringStr("pel-count")
		enc.WriteInteger(int64(len(g.pel)))
		enc.WriteBulkStringStr("pending")
		writeGroupPEL(enc, g)
		enc.WriteBulkStringStr("consumers")
		writeGroupConsumers(enc, g)
	}
}

// writeGroupPEL writes a group's global PEL as an array of
// [id, consumer, delivery-time, delivery-count].
func writeGroupPEL(enc *resp.Encoder, g *group) {
	enc.WriteArrayLen(len(g.pel))
	for _, pe := range g.pel {
		enc.WriteArrayLen(4)
		enc.WriteBulkStringStr(pe.id.String())
		enc.WriteBulkStringStr(pe.consumer)
		enc.WriteInteger(pe.deliveryTime)
		enc.WriteInteger(int64(pe.deliveryCount))
	}
}

// writeGroupConsumers writes the consumer descriptors inside XINFO STREAM FULL.
func writeGroupConsumers(enc *resp.Encoder, g *group) {
	enc.WriteArrayLen(len(g.consumers))
	for _, c := range g.consumers {
		if enc.Proto() >= 3 {
			enc.WriteMapLen(4)
		} else {
			enc.WriteArrayLen(8)
		}
		enc.WriteBulkStringStr("name")
		enc.WriteBulkStringStr(c.name)
		enc.WriteBulkStringStr("seen-time")
		enc.WriteInteger(c.seenTime)
		enc.WriteBulkStringStr("active-time")
		enc.WriteInteger(c.activeTime)
		enc.WriteBulkStringStr("pending")
		writeConsumerPEL(enc, g, c.name)
	}
}

// writeConsumerPEL writes a consumer's pending entries as an array of
// [id, delivery-time, delivery-count].
func writeConsumerPEL(enc *resp.Encoder, g *group, name string) {
	rows := make([]pelEntry, 0)
	for _, pe := range g.pel {
		if pe.consumer == name {
			rows = append(rows, pe)
		}
	}
	enc.WriteArrayLen(len(rows))
	for _, pe := range rows {
		enc.WriteArrayLen(3)
		enc.WriteBulkStringStr(pe.id.String())
		enc.WriteInteger(pe.deliveryTime)
		enc.WriteInteger(int64(pe.deliveryCount))
	}
}
