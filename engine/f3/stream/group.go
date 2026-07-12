package stream

// Consumer groups (spec 2064/f3/14 section 7): the mutable ledger layer that
// hangs off a native stream's header, owner-local per F1. There is no global
// PEL, no cross-stream delivery registry, no shared claim queue (F2); the whole
// ledger is per-key state exactly one shard ever touches.
//
// This slice lands the group and consumer records and their XGROUP lifecycle:
// the `>` delivery cursor, the lag basis, and the consumer table. The
// pending-entries list (the counted tree beside a hash over 32-byte slabs,
// section 7.4) and the delivery, ack, and claim machinery that fill it arrive
// with XREADGROUP (slice 6), so a group here carries its cursor and its consumer
// table over an empty PEL.

// streamGroup is one consumer group. lastDeliveredID is the `>` cursor; the
// consumer table names the group's readers. The PEL structures and pelCount join
// in slice 6, when a delivery first creates a pending entry.
type streamGroup struct {
	// lastDeliveredID is the cursor a `>` read advances past, the greatest ID the
	// group has handed to any consumer. XGROUP CREATE and SETID set it directly.
	lastDeliveredID streamID
	// entriesRead is the count of entries the group has consumed, the lag basis:
	// lag = stream.entriesAdded - entriesRead. It is exact when the group's start
	// position has a known distance from the stream's history (id 0-0, id "$", an
	// id at or past the tail, or an explicit ENTRIESREAD), which readValid
	// records. A group started mid-stream at an explicit ID has an entriesRead the
	// cursor machinery of slice 6 must price against the directory, so until then
	// it reports entries-read and lag as nil, exactly as Redis does when it cannot
	// track the value.
	entriesRead uint64
	readValid   bool
	// consumers maps a consumer name to its record. Created empty on XGROUP
	// CREATE; XGROUP CREATECONSUMER and (in slice 6) a delivering XREADGROUP add
	// entries, XGROUP DELCONSUMER removes them.
	consumers map[string]*streamConsumer
}

// streamConsumer is one named consumer in a group (section 7.3). pelCount is the
// number of pending entries the consumer owns, maintained by delivery and ack in
// slice 6; the idle and active clocks and the PEL ordinal join with the delivery
// path that gives them meaning.
type streamConsumer struct {
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

// createConsumer adds a consumer by name if it is absent and reports whether it
// created one. A consumer starts owning no pending entries.
func (grp *streamGroup) createConsumer(name []byte) bool {
	if _, ok := grp.consumers[string(name)]; ok {
		return false
	}
	grp.consumers[string(name)] = &streamConsumer{}
	return true
}

// delConsumer removes the named consumer and reports the number of pending
// entries it owned, which XGROUP DELCONSUMER returns. In this slice a consumer
// owns no pending entries (the PEL fills in slice 6), so the count is its
// maintained pelCount, zero until delivery exists; the drain of that ordinal's
// PEL subset lands with the PEL structures. A missing consumer removes nothing.
func (grp *streamGroup) delConsumer(name []byte) int64 {
	con := grp.consumers[string(name)]
	if con == nil {
		return 0
	}
	pending := int64(con.pelCount)
	delete(grp.consumers, string(name))
	return pending
}
