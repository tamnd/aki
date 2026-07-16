package stream

// Consumer groups (spec 2064/f3/14 section 7): the mutable ledger layer that
// hangs off a native stream's header, owner-local per F1. There is no global
// PEL, no cross-stream delivery registry, no shared claim queue (F2); the whole
// ledger is per-key state exactly one shard ever touches.
//
// This slice lands the group and consumer records, their XGROUP lifecycle, and
// the pending-entries list the delivery path fills: XREADGROUP inserts a pending
// entry per delivered ID, XACK removes one, XPENDING reads the range. The PEL
// itself (the counted tree beside a hash over slabs, section 7.4) lives in pel.go
// and hangs off each group, created on the first delivery.

// streamGroup is one consumer group. lastDeliveredID is the `>` cursor; the
// consumer table names the group's readers; pel and pelCount hold the group's
// pending entries.
type streamGroup struct {
	// lastDeliveredID is the cursor a `>` read advances past, the greatest ID the
	// group has handed to any consumer. XGROUP CREATE and SETID set it directly.
	lastDeliveredID streamID
	// entriesRead is the count of entries the group has consumed, the lag basis:
	// lag = stream.entriesAdded - entriesRead. It is exact when the group's start
	// position has a known distance from the stream's history (id 0-0, id "$", an
	// id at or past the tail, or an explicit ENTRIESREAD), which readValid
	// records. A group started mid-stream at an explicit ID has an entriesRead the
	// cursor machinery must price against the directory, so until then it reports
	// entries-read and lag as nil, exactly as Redis does when it cannot track the
	// value. A `>` delivery advances entriesRead by the count delivered, which is
	// exact whenever the basis was.
	entriesRead uint64
	readValid   bool
	// pelCount is the group's total pending-entry count, the O(1) XPENDING summary
	// and the field XINFO GROUPS reports; it tracks pel's size without a walk.
	pelCount uint32
	// consumers maps a consumer name to its record; consumerByOrd is the reverse,
	// indexed by a consumer's ordinal so an ack or a pending walk resolves an
	// owner ordinal back to its record. A DELCONSUMER leaves a nil hole rather
	// than reindexing, since a pending slab holds the ordinal by value.
	consumers     map[string]*streamConsumer
	consumerByOrd []*streamConsumer
	// pel is the pending-entries list, nil until the first delivery creates one.
	pel *groupPEL
}

// streamConsumer is one named consumer in a group (section 7.3). name is the
// consumer's name, which XPENDING reports as the owner of each pending entry; ord
// is the ordinal the group assigns at creation, the value a pending slab stores to
// name its owner; pelCount is the number of pending entries the consumer owns,
// maintained by delivery and ack. seenTime and activeTime are the two clocks XINFO
// CONSUMERS reads: seenTime is the last time any group command named the consumer
// (its XINFO idle is now-seenTime), activeTime the last time it actually fetched or
// claimed an entry (its XINFO inactive is now-activeTime). activeTime starts at -1,
// the sentinel Redis reports as inactive for a consumer that has never been active.
type streamConsumer struct {
	name       []byte
	ord        uint32
	pelCount   uint32
	seenTime   int64
	activeTime int64
}

// pelEntryBytes is the resident cost of one pending-entries slab, the 32-byte
// record the PEL tree references by ordinal (pel.go). consumerBytes is a fixed cell
// per consumer for its record and the two index slots (the map entry and the
// consumerByOrd pointer), a rough figure dwarfed by the pending slabs a busy
// consumer accrues.
const (
	pelEntryBytes = 32
	consumerBytes = 80
)

// residentBytes is the group's resident-byte footprint, the stream-side auxiliary
// heap the consumer-group machinery holds beyond the entry blocks: the consumer
// records and, once the group has delivered, the pending-entries list (its slab
// arena, the freed-ordinal list, and the id-ordered tree). A group that only reads
// with `>` and never leaves an entry pending carries no PEL, so it costs only its
// consumer cells here. Zero preads, O(1).
func (grp *streamGroup) residentBytes() uint64 {
	n := uint64(len(grp.consumerByOrd)) * consumerBytes
	if grp.pel != nil {
		n += uint64(cap(grp.pel.slabs)) * pelEntryBytes
		n += uint64(cap(grp.pel.free)) * 4
		n += uint64(grp.pel.tree.Bytes())
	}
	return n
}

// newGroup builds a group at the given start cursor. entriesRead and valid come
// from groupStartID (or an explicit ENTRIESREAD override), and the consumer table
// starts empty.
func newGroup(start streamID, entriesRead uint64, valid bool) *streamGroup {
	return &streamGroup{
		lastDeliveredID: start,
		entriesRead:     entriesRead,
		readValid:       valid,
		consumers:       make(map[string]*streamConsumer),
	}
}

// group returns the named group, or nil when the stream holds none by that name.
func (s *stream) group(name []byte) *streamGroup {
	return s.groups[string(name)]
}

// addGroup upgrades the stream to the native band if needed (section 4.4) and
// records grp under name, building the group table on first use. The caller has
// already checked no group by that name exists.
func (s *stream) addGroup(name []byte, grp *streamGroup) {
	s.ensureNative()
	if s.groups == nil {
		s.groups = make(map[string]*streamGroup)
	}
	s.groups[string(name)] = grp
}

// consumer returns the named consumer in the group, or nil when absent.
func (grp *streamGroup) consumer(name []byte) *streamConsumer {
	return grp.consumers[string(name)]
}

// ensureConsumer returns the named consumer, creating and ordinal-assigning it on
// first sight. XREADGROUP lazily creates a consumer this way, and XGROUP
// CREATECONSUMER creates one explicitly. A newly created consumer's seen clock is
// stamped now and its active clock starts at the -1 never-active sentinel, matching
// Redis's streamCreateConsumer; the caller re-stamps seenTime on an existing
// consumer it names.
func (grp *streamGroup) ensureConsumer(name []byte, now int64) *streamConsumer {
	if con := grp.consumers[string(name)]; con != nil {
		return con
	}
	con := &streamConsumer{
		name:       append([]byte(nil), name...),
		ord:        uint32(len(grp.consumerByOrd)),
		seenTime:   now,
		activeTime: -1,
	}
	grp.consumers[string(name)] = con
	grp.consumerByOrd = append(grp.consumerByOrd, con)
	return con
}

// createConsumer adds a consumer by name if it is absent and reports whether it
// created one. A consumer starts owning no pending entries, its seen clock at now
// and its active clock at the never-active sentinel.
func (grp *streamGroup) createConsumer(name []byte, now int64) bool {
	if grp.consumers[string(name)] != nil {
		return false
	}
	grp.ensureConsumer(name, now)
	return true
}

// delConsumer removes the named consumer, draining the pending entries it owned
// from the group PEL first, and reports how many that was (the count XGROUP
// DELCONSUMER returns). A missing consumer removes nothing.
func (grp *streamGroup) delConsumer(name []byte) int64 {
	con := grp.consumers[string(name)]
	if con == nil {
		return 0
	}
	removed := grp.drainConsumer(con.ord)
	delete(grp.consumers, string(name))
	if int(con.ord) < len(grp.consumerByOrd) {
		grp.consumerByOrd[con.ord] = nil
	}
	return removed
}

// claimResult reports what a single-ID claim did (section 7.7): claimed carries
// the id the reply renders, deleted marks a pending entry whose log entry an XDEL
// removed since (dropped from the PEL, never claimed, reported by XAUTOCLAIM), and
// a zero value is a skip: the entry was not pending without FORCE, or not idle
// enough to take.
type claimResult struct {
	id      streamID
	claimed bool
	deleted bool
}

// xclaimOpts is the parsed XCLAIM/XAUTOCLAIM option set (section 7.7). force
// creates a pending slab for a not-yet-pending entry that still exists in the log;
// justid suppresses the delivery-count bump and renders IDs only; the idle, time,
// and retry overrides set the claimed entry's clock and RETRYCOUNT explicitly
// instead of stamping now and incrementing.
type xclaimOpts struct {
	force    bool
	justid   bool
	hasIdle  bool
	idleMs   int64
	hasTime  bool
	timeMs   int64
	hasRetry bool
	retry    int64
}

// claimOne applies a claim to one ID for the target consumer (section 7.7): a
// point rewrite of the pending slab, never a scan, never a move between
// structures. It resolves the entry's liveness once, creates the slab under FORCE
// when the entry exists but is not pending, drops a pending entry whose log entry
// is gone, gates on min-idle against the current delivery clock, then reassigns
// ownership and stamps the delivery time and count. A missing group PEL is created
// only when FORCE has something to add.
func (grp *streamGroup) claimOne(s *stream, id streamID, to *streamConsumer, now, minIdle int64, opts xclaimOpts) claimResult {
	_, live := s.entryAt(id)
	var (
		pe *pelEntry
		ok bool
	)
	if grp.pel != nil {
		pe, ok = grp.pel.find(id)
	}
	if !ok {
		if !opts.force || !live {
			return claimResult{}
		}
		if grp.pel == nil {
			grp.pel = newPEL()
		}
		pe = grp.pel.insertClaimed(id)
		grp.pelCount++
	}
	if !live {
		// The pending entry outlived its log entry (XDEL'd since delivery): drop it
		// from the PEL and report it deleted, never claiming a phantom.
		if ord, dropped := grp.pel.ack(id); dropped {
			grp.pelCount--
			grp.decOwner(ord)
		}
		return claimResult{deleted: true, id: id}
	}
	// Idle gate against the current delivery clock; a just-created slab (epoch
	// clock, no owner) always passes, matching Redis's force path.
	if pe.consumerOrd != noOwner && now-pe.deliveryTime < minIdle {
		return claimResult{}
	}
	if pe.consumerOrd == noOwner {
		pe.consumerOrd = to.ord
		to.pelCount++
	} else if pe.consumerOrd != to.ord {
		grp.decOwner(pe.consumerOrd)
		pe.consumerOrd = to.ord
		to.pelCount++
	}
	switch {
	case opts.hasTime:
		pe.deliveryTime = opts.timeMs
	case opts.hasIdle:
		pe.deliveryTime = now - opts.idleMs
	default:
		pe.deliveryTime = now
	}
	// RETRYCOUNT sets the count outright; otherwise a non-JUSTID claim counts one
	// more delivery. The two are exclusive, as Redis does it: an explicit count is
	// never then auto-incremented.
	if opts.hasRetry {
		pe.deliveryCount = clampRetry(opts.retry)
	} else if !opts.justid {
		pe.deliveryCount++
	}
	return claimResult{claimed: true, id: id}
}

// decOwner drops one from the consumer that owns ordinal ord, tolerating the nil
// hole a DELCONSUMER leaves (a pending slab holds the ordinal by value, so a
// removed consumer's ordinal can still surface on a claim of its old entry).
func (grp *streamGroup) decOwner(ord uint32) {
	if int(ord) < len(grp.consumerByOrd) {
		if con := grp.consumerByOrd[ord]; con != nil {
			con.pelCount--
		}
	}
}

// clampRetry fits an XCLAIM RETRYCOUNT into the slab's 2-byte delivery count
// (section 7.4), flooring a negative to zero and saturating past the u16 ceiling,
// a bound only a pathological argument reaches.
func clampRetry(n int64) uint16 {
	switch {
	case n < 0:
		return 0
	case n > int64(^uint16(0)):
		return ^uint16(0)
	default:
		return uint16(n)
	}
}

// autoClaimResult is what one XAUTOCLAIM pass produced (section 7.7): claimed lists
// the ids transferred to the target consumer in id order, deleted lists the pending
// ids whose log entry an XDEL removed since (dropped from the PEL, reported in the
// reply's third element), and cursor is the id to resume the scan from, the zero id
// when the walk reached the end.
type autoClaimResult struct {
	claimed []streamID
	deleted []streamID
	cursor  streamID
}

// autoClaim scans the group PEL from start in id order, transferring up to count
// qualifying entries to the target consumer and lazily dropping entries whose log
// entry an XDEL removed since (section 7.7). It mirrors Redis's twin budget: every
// entry examined spends one of the count*10 scan attempts, and every claim or drop
// spends one of the count result slots, so a PEL where idle entries are sparse still
// returns in bounded work and the cursor lets a recovery loop drain a large stuck
// PEL in slices. A claim is the same in-place slab rewrite XCLAIM does, never a move
// between structures. Deleted entries are collected on the walk and removed after it,
// so the tree is never mutated mid-scan.
func (grp *streamGroup) autoClaim(s *stream, start streamID, to *streamConsumer, now, minIdle int64, count int, justid bool) autoClaimResult {
	var res autoClaimResult
	if grp.pel == nil {
		return res
	}
	attempts := count * 10
	remaining := count
	var dropped []streamID
	// res.cursor stays the zero id when the walk ends naturally (0-0 means "done"),
	// and is set to the next unscanned id only when a budget stops the walk early.
	grp.pel.walkFrom(start, func(pe *pelEntry) bool {
		if attempts == 0 || remaining == 0 {
			res.cursor = pe.id
			return false
		}
		attempts--
		id := pe.id
		if _, live := s.entryAt(id); !live {
			// The pending entry outlived its log entry: drop it and report the id, the
			// only way XAUTOCLAIM surfaces a deletion the owner never acked.
			dropped = append(dropped, id)
			res.deleted = append(res.deleted, id)
			remaining--
			return true
		}
		if minIdle > 0 && now-pe.deliveryTime < minIdle {
			return true
		}
		if pe.consumerOrd != to.ord {
			grp.decOwner(pe.consumerOrd)
			pe.consumerOrd = to.ord
			to.pelCount++
		}
		pe.deliveryTime = now
		if !justid {
			pe.deliveryCount++
		}
		res.claimed = append(res.claimed, id)
		remaining--
		return true
	})
	for _, id := range dropped {
		if ord, ok := grp.pel.ack(id); ok {
			grp.pelCount--
			grp.decOwner(ord)
		}
	}
	return res
}

// nackMode is the delivery-count policy XNACK applies to a nacked entry (Redis
// 8.8, section 7.6). SILENT undoes the delivery increment (decrement by one,
// floored at zero); FAIL leaves the count as the delivery left it; FATAL saturates
// it so any retry-count ceiling treats the entry as poison.
type nackMode uint8

const (
	nackSilent nackMode = iota
	nackFail
	nackFatal
)

// nackSetCount rewrites a pending entry's delivery count for XNACK. An explicit
// RETRYCOUNT overrides the mode outright (clamped to the slab's 2-byte field);
// otherwise the mode decides. FATAL sets the u16 ceiling, Redis's LLONG_MAX capped
// to the slab width the same way clampRetry saturates.
func nackSetCount(pe *pelEntry, mode nackMode, retry int64, hasRetry bool) {
	if hasRetry {
		pe.deliveryCount = clampRetry(retry)
		return
	}
	switch mode {
	case nackSilent:
		if pe.deliveryCount > 0 {
			pe.deliveryCount--
		}
	case nackFatal:
		pe.deliveryCount = ^uint16(0)
	}
}

// nack releases one id back to the group PEL without acking it (section 7.6, Redis
// 8.8): a found pending entry is disowned (the owning consumer's count dropped, the
// slab moved to the unowned NACK zone with its idle clock reset to the epoch) and
// its delivery count rewritten per the mode or an explicit RETRYCOUNT, so the next
// XCLAIM or XAUTOCLAIM min-idle predicate matches it immediately. Under FORCE an id
// that is not pending but still exists in the log is created as an unowned NACK from
// a zero baseline; without FORCE, or for an id whose log entry is gone, it is a skip.
// It reports whether the id was nacked, the count the reply sums. It is a point
// rewrite of one slab, never a scan.
func (grp *streamGroup) nack(s *stream, id streamID, mode nackMode, retry int64, hasRetry, force bool) bool {
	var (
		pe *pelEntry
		ok bool
	)
	if grp.pel != nil {
		pe, ok = grp.pel.find(id)
	}
	if !ok {
		if !force {
			return false
		}
		if _, live := s.entryAt(id); !live {
			return false
		}
		if grp.pel == nil {
			grp.pel = newPEL()
		}
		// insertClaimed lands the slab unowned with a zero count and an epoch clock,
		// exactly the clean baseline Redis resets a FORCE-created NACK to.
		pe = grp.pel.insertClaimed(id)
		grp.pelCount++
		nackSetCount(pe, mode, retry, hasRetry)
		return true
	}
	if pe.consumerOrd != noOwner {
		grp.decOwner(pe.consumerOrd)
		pe.consumerOrd = noOwner
	}
	nackSetCount(pe, mode, retry, hasRetry)
	pe.deliveryTime = 0
	return true
}

// nackedCount counts the group's unowned pending entries, the NACK-zone total XINFO
// STREAM FULL reports as nacked-count. It walks the PEL, O(pending), the sole
// full-PEL scan in the stream surface and only on that debug command; the far more
// common owned/unowned states are read per entry as they are rendered, so nothing on
// a delivery, ack, or claim path pays to maintain a separate counter.
func (grp *streamGroup) nackedCount() int64 {
	if grp.pel == nil {
		return 0
	}
	var n int64
	grp.pel.walkFrom(streamID{}, func(pe *pelEntry) bool {
		if pe.consumerOrd == noOwner {
			n++
		}
		return true
	})
	return n
}

// pelSample gathers up to limit pending entries in id order, optionally filtered by
// keep, for an XINFO STREAM FULL dump. A limit of zero (COUNT 0) is unbounded, the
// way Redis reads it; the cap is applied after the filter so a per-consumer sample
// yields up to limit of that consumer's entries.
func (grp *streamGroup) pelSample(limit int, keep func(*pelEntry) bool) []*pelEntry {
	if grp.pel == nil {
		return nil
	}
	var out []*pelEntry
	grp.pel.walkFrom(streamID{}, func(pe *pelEntry) bool {
		if keep != nil && !keep(pe) {
			return true
		}
		out = append(out, pe)
		return limit <= 0 || len(out) < limit
	})
	return out
}

// drainConsumer removes every pending entry owned by the consumer ordinal from the
// group PEL and returns the count. It collects the IDs on one tree walk, then acks
// each, so the tree is never mutated mid-walk. Bounded by the entries removed,
// never by the stream length.
func (grp *streamGroup) drainConsumer(ord uint32) int64 {
	if grp.pel == nil {
		return 0
	}
	var ids []streamID
	grp.pel.walkFrom(streamID{}, func(e *pelEntry) bool {
		if e.consumerOrd == ord {
			ids = append(ids, e.id)
		}
		return true
	})
	for _, id := range ids {
		grp.pel.ack(id)
	}
	grp.pelCount -= uint32(len(ids))
	return int64(len(ids))
}
