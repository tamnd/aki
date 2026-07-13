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
// maintained by delivery and ack. The idle and active clocks join with XINFO
// CONSUMERS, which reads them.
type streamConsumer struct {
	name     []byte
	ord      uint32
	pelCount uint32
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
// CREATECONSUMER creates one explicitly.
func (grp *streamGroup) ensureConsumer(name []byte) *streamConsumer {
	if con := grp.consumers[string(name)]; con != nil {
		return con
	}
	con := &streamConsumer{name: append([]byte(nil), name...), ord: uint32(len(grp.consumerByOrd))}
	grp.consumers[string(name)] = con
	grp.consumerByOrd = append(grp.consumerByOrd, con)
	return con
}

// createConsumer adds a consumer by name if it is absent and reports whether it
// created one. A consumer starts owning no pending entries.
func (grp *streamGroup) createConsumer(name []byte) bool {
	if grp.consumers[string(name)] != nil {
		return false
	}
	grp.ensureConsumer(name)
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
