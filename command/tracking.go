package command

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/tamnd/aki/resp"
)

// This file implements client-side caching, the server half of Redis CLIENT
// TRACKING (doc 19 section 10). A tracking client tells the server to remember
// which keys it reads; when one of those keys later changes the server pushes an
// invalidation so the client can drop its local copy.
//
// Two record modes exist. Default mode keeps a per-key set of interested client
// ids in the invalidation table, filled as each client reads keys. BCAST mode
// keeps a per-prefix set of client ids instead and never records individual
// reads, so a write to any key under a prefix invalidates every client watching
// that prefix.
//
// Delivery has two forms. A RESP3 client receives invalidations inline as a push
// on its own connection. A RESP2 client cannot take an out-of-band push, so it
// names a separate client with REDIRECT and the server forwards the invalidation
// to that client as a Pub/Sub message on the __redis__:invalidate channel.

// invalidateChannel is the pseudo-channel name Redis clients expect tracking
// invalidations on, both for the RESP3 push and the RESP2 redirect message.
const invalidateChannel = "__redis__:invalidate"

// trackingState holds the server-wide tracking tables. keys maps a tracked key
// to the set of client ids that read it (default mode). prefixes maps a BCAST
// prefix to the set of client ids watching it. clients counts the connections
// with tracking on so the write path can bail out with one atomic load when
// nobody is tracking.
type trackingState struct {
	mu       sync.Mutex
	keys     map[string]map[uint64]struct{}
	prefixes map[string]map[uint64]struct{}
	clients  atomic.Int64
}

// trackingInit allocates the tracking tables. New calls it once at startup.
func (d *Dispatcher) trackingInit() {
	d.tracking.keys = make(map[string]map[uint64]struct{})
	d.tracking.prefixes = make(map[string]map[uint64]struct{})
}

// trackingActive reports whether any client currently has tracking on. The write
// path checks it first so a server with no tracking client pays only one atomic
// load per write.
func (d *Dispatcher) trackingActive() bool {
	return d.tracking.clients.Load() > 0
}

// trackingEnable turns tracking on for a session and registers its BCAST
// prefixes. It is called from CLIENT TRACKING ON after the options validate. A
// session already tracking is reconfigured in place: its old prefixes and any
// recorded keys are dropped first so the new mode starts clean.
func (d *Dispatcher) trackingEnable(id uint64, sess *session) {
	d.tracking.mu.Lock()
	defer d.tracking.mu.Unlock()
	if sess.trackingOn {
		d.untrackLocked(id, sess)
	} else {
		d.tracking.clients.Add(1)
	}
	sess.trackingOn = true
	if sess.trackingBcast {
		prefixes := sess.trackingPrefixes
		if len(prefixes) == 0 {
			prefixes = []string{""}
		}
		for _, p := range prefixes {
			set := d.tracking.prefixes[p]
			if set == nil {
				set = make(map[uint64]struct{})
				d.tracking.prefixes[p] = set
			}
			set[id] = struct{}{}
		}
	}
}

// trackingDisable turns tracking off for a session and removes it from every
// table. CLIENT TRACKING OFF and RESET both call it.
func (d *Dispatcher) trackingDisable(id uint64, sess *session) {
	d.tracking.mu.Lock()
	defer d.tracking.mu.Unlock()
	if !sess.trackingOn {
		return
	}
	d.untrackLocked(id, sess)
	d.tracking.clients.Add(-1)
	sess.trackingOn = false
	sess.trackingBcast = false
	sess.trackingOptIn = false
	sess.trackingOptOut = false
	sess.trackingNoLoop = false
	sess.trackingPrefixes = nil
	sess.trackingRedir = 0
}

// trackingDropClient removes a disconnecting client from the tables. It runs from
// OnDisconnect, off the client's own goroutine, so it takes the lock like any
// other table mutation.
func (d *Dispatcher) trackingDropClient(id uint64, sess *session) {
	d.tracking.mu.Lock()
	defer d.tracking.mu.Unlock()
	if !sess.trackingOn {
		return
	}
	d.untrackLocked(id, sess)
	d.tracking.clients.Add(-1)
	sess.trackingOn = false
}

// untrackLocked removes a client id from every key entry and prefix entry it
// appears in. The caller holds tracking.mu.
func (d *Dispatcher) untrackLocked(id uint64, sess *session) {
	for k, set := range d.tracking.keys {
		if _, ok := set[id]; ok {
			delete(set, id)
			if len(set) == 0 {
				delete(d.tracking.keys, k)
			}
		}
	}
	prefixes := sess.trackingPrefixes
	if sess.trackingBcast && len(prefixes) == 0 {
		prefixes = []string{""}
	}
	for _, p := range prefixes {
		set := d.tracking.prefixes[p]
		if set == nil {
			continue
		}
		delete(set, id)
		if len(set) == 0 {
			delete(d.tracking.prefixes, p)
		}
	}
}

// trackingRecordRead records the keys a tracking client just read so a later
// write to any of them invalidates that client. It runs on the reading client's
// own goroutine after a read command. Only default mode records reads; BCAST mode
// learns nothing from reads because it invalidates by prefix. The per-command
// CLIENT CACHING decision and the OPTIN/OPTOUT default decide whether this read
// counts.
func (d *Dispatcher) trackingRecordRead(id uint64, sess *session, keys [][]byte) {
	if !sess.trackingOn || sess.trackingBcast || len(keys) == 0 {
		return
	}
	if !d.shouldTrackRead(sess) {
		return
	}
	limit := int(d.confInt("tracking-table-max-keys", 0))
	d.tracking.mu.Lock()
	for _, k := range keys {
		ks := string(k)
		set := d.tracking.keys[ks]
		if set == nil {
			if limit > 0 && len(d.tracking.keys) >= limit {
				// The table is full. Drop the oldest unrelated entries by flushing
				// the whole tracking state, the conservative move Redis falls back to
				// when the table cannot grow: every tracking client is told to flush.
				d.evictTrackingTableLocked()
			}
			set = make(map[uint64]struct{})
			d.tracking.keys[ks] = set
		}
		set[id] = struct{}{}
	}
	d.tracking.mu.Unlock()
}

// shouldTrackRead applies the OPTIN/OPTOUT default and the one-shot CLIENT
// CACHING flag to decide whether the current read should be recorded.
func (d *Dispatcher) shouldTrackRead(sess *session) bool {
	switch {
	case sess.trackingOptIn:
		return sess.cachingYes
	case sess.trackingOptOut:
		return !sess.cachingNo
	default:
		return true
	}
}

// evictTrackingTableLocked clears the whole invalidation table and sends a flush
// invalidation to every default-mode tracking client. It is the fallback when
// tracking-table-max-keys is reached. The caller holds tracking.mu.
func (d *Dispatcher) evictTrackingTableLocked() {
	recipients := make(map[uint64]struct{})
	for _, set := range d.tracking.keys {
		for cid := range set {
			recipients[cid] = struct{}{}
		}
	}
	d.tracking.keys = make(map[string]map[uint64]struct{})
	for cid := range recipients {
		d.deliverInvalidation(cid, nil, 0)
	}
}

// trackingInvalidateKey notifies every interested client that a key changed. It
// covers both modes: the default-mode set recorded for the key, and any BCAST
// prefix the key falls under. writerID is the client that made the change, used
// to honor NOLOOP so a client is not told about its own writes. The key entry is
// removed after notifying, matching Redis: a client must read the key again to
// re-arm tracking on it.
func (d *Dispatcher) trackingInvalidateKey(key []byte, writerID uint64) {
	if !d.trackingActive() {
		return
	}
	ks := string(key)
	d.tracking.mu.Lock()
	recipients := make(map[uint64]struct{})
	if set := d.tracking.keys[ks]; set != nil {
		for cid := range set {
			recipients[cid] = struct{}{}
		}
		delete(d.tracking.keys, ks)
	}
	for prefix, set := range d.tracking.prefixes {
		if strings.HasPrefix(ks, prefix) {
			for cid := range set {
				recipients[cid] = struct{}{}
			}
		}
	}
	d.tracking.mu.Unlock()

	keys := [][]byte{key}
	for cid := range recipients {
		d.deliverInvalidation(cid, keys, writerID)
	}
}

// trackingFlushAll tells every tracking client to drop its entire cache, the
// invalidation FLUSHDB and FLUSHALL send. It also clears the default-mode table
// since those entries are now meaningless.
func (d *Dispatcher) trackingFlushAll(writerID uint64) {
	if !d.trackingActive() {
		return
	}
	d.tracking.mu.Lock()
	recipients := make(map[uint64]struct{})
	for _, set := range d.tracking.keys {
		for cid := range set {
			recipients[cid] = struct{}{}
		}
	}
	for _, set := range d.tracking.prefixes {
		for cid := range set {
			recipients[cid] = struct{}{}
		}
	}
	d.tracking.keys = make(map[string]map[uint64]struct{})
	d.tracking.mu.Unlock()

	for cid := range recipients {
		d.deliverInvalidation(cid, nil, writerID)
	}
}

// deliverInvalidation sends one invalidation to a tracking client. keys is the
// list of changed keys, or nil for a flush (null key array). A RESP3 client gets
// the push inline; a client that set a redirect gets a Pub/Sub message on the
// redirect target instead. NOLOOP suppresses delivery for a client's own writes.
func (d *Dispatcher) deliverInvalidation(clientID uint64, keys [][]byte, writerID uint64) {
	if d.srv == nil {
		return
	}
	conn := d.srv.ConnByID(clientID)
	if conn == nil {
		return
	}
	sess, ok := conn.Session().(*session)
	if !ok || !sess.trackingOn {
		return
	}
	if sess.trackingNoLoop && writerID != 0 && writerID == clientID {
		return
	}
	if sess.trackingRedir != 0 {
		target := d.srv.ConnByID(sess.trackingRedir)
		if target == nil {
			// The redirect target is gone. The tracking client keeps a broken
			// redirect, surfaced by CLIENT TRACKINGINFO; no invalidation can be
			// delivered until it re-establishes tracking.
			return
		}
		_ = target.Deliver(frameInvalidation(target.Proto(), true, keys))
		return
	}
	_ = conn.Deliver(frameInvalidation(conn.Proto(), false, keys))
}

// frameInvalidation builds an invalidation message. With redirect set it is a
// three-element Pub/Sub message on __redis__:invalidate for a RESP2 forwarder;
// without it is a two-element RESP3 push straight to the tracking client. keys nil
// encodes a flush as a null array.
func frameInvalidation(proto int, redirect bool, keys [][]byte) []byte {
	var b bytes.Buffer
	e := resp.NewEncoder(&b, proto)
	if redirect {
		e.WritePushLen(3)
		e.WriteBulkStringStr("message")
		e.WriteBulkStringStr(invalidateChannel)
	} else {
		e.WritePushLen(2)
		e.WriteBulkStringStr(invalidateChannel)
	}
	if keys == nil {
		e.WriteNullArray()
	} else {
		e.WriteArrayLen(len(keys))
		for _, k := range keys {
			e.WriteBulkString(k)
		}
	}
	return b.Bytes()
}
