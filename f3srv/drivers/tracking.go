package drivers

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/dispatch"
	"github.com/tamnd/aki/f3srv/resp"
)

// Client-side caching (spec 2064/f3/17, the M11 command-closure milestone;
// redis's CLIENT TRACKING). This is the network-layer half: the per-connection
// tracking state, the recorded-key table, and the write-path invalidation. The
// shard-layer half is the arm gate and the invalidation hook (engine/f3/shard/
// tracking.go); the two meet at Runtime.UseInvalidator, wired in Listen.
//
// This slice is default mode over RESP3. A tracking connection records every key
// it READ (dispatch.ReadKeys, curated read set) into the registry's forward table;
// the first write to a recorded key, on any connection, drains the key's bucket and
// pushes one RESP3 "invalidate" message to each connection that cached it, then
// forgets the key so the next read re-arms it (redis's once-per-caching-cycle
// discipline). The registry lives in the network layer beside pub/sub, gated by the
// shard-layer trackingArmed count so an unarmed server pays one relaxed load per
// write and never this mutex, the same shape the pub/sub and monitor registries
// have. OPTIN/OPTOUT, NOLOOP, and BCAST with its prefix table are all live; the
// RESP2 REDIRECT path is the remaining slice, refused with an honest not-yet error.
// TRACKING ON requires RESP3 (the honest redis answer when no redirection client is
// given, since a RESP2 connection cannot carry an out-of-band push).

// trackState is one connection's CLIENT TRACKING configuration, nil on a
// connection that never enabled it. In this slice it holds only the recorded-key
// set (the reverse index the disconnect and TRACKING OFF cleanups walk); the mode
// flags, prefixes, and redirect target join with their slices. Reader-owned like
// the pub/sub sets: the pointer is set and cleared on the connection's own reader
// goroutine, and recorded is mutated only under the registry mutex.
type trackState struct {
	recorded map[string]struct{}
	// optin and optout are the two non-default recording modes (mutually
	// exclusive, both off in default mode). In optin mode a key is cached only when
	// the command that read it was preceded by CLIENT CACHING YES; in optout mode
	// every read is cached except after CLIENT CACHING NO. Set once at CLIENT
	// TRACKING ON and read on the connection's own reader goroutine, so no lock.
	optin  bool
	optout bool
	// noloop suppresses the invalidation for a key this connection wrote itself: the
	// client already knows its own write changed the value, so redis lets it opt out
	// of the self-notification. Set on the connection's reader goroutine at CLIENT
	// TRACKING ON but read on the writer's owner goroutine (the invalidate path
	// matches the origin connection), so it is atomic to make that cross-goroutine
	// read safe, unlike the reader-confined optin/optout.
	noloop atomic.Bool
	// bcast marks a broadcast-mode connection: it keeps no recorded-key table and
	// instead registers a prefix set in the registry's bcast slice, taking an
	// invalidation for every write to a matching key rather than only for keys it
	// read. prefixes is this connection's own copy of that set, kept here so
	// TRACKINGINFO renders it on the reader goroutine without touching the registry.
	// Both are set once at CLIENT TRACKING ON BCAST and reader-owned, like optin.
	bcast    bool
	prefixes [][]byte

	// redirect is the connection this connection's invalidations are delivered to
	// instead of itself (CLIENT TRACKING ON REDIRECT <id>), the mechanism that lets a
	// RESP2 client cache: it enables tracking on its command connection and points
	// the pushes at a second connection subscribed to __redis__:invalidate.
	// redirectID is the target's client id (for GETREDIR/TRACKINGINFO), retained even
	// after the target dies. redirBroken is set when the target disconnects: its
	// invalidations are then dropped, and TRACKINGINFO reports broken_redirect. All
	// three are managed under the registry mutex (unlike the reader-owned mode flags)
	// because the write path reads redirect to route delivery and a foreign
	// connection's teardown writes redirBroken.
	redirect    *connState
	redirectID  uint64
	redirBroken bool
	// caching is the transient CLIENT CACHING selector, governing the very next
	// command only: cachingYes or cachingNo overrides the mode's default for that
	// one command, then recordReadKeys clears it back to cachingNone. Reader-owned,
	// like optin/optout.
	caching int8
}

const (
	cachingNone int8 = iota
	cachingYes
	cachingNo
)

// trackingRegistry is the network-layer client-side-caching table. keys is the
// forward index a write consults: a key name maps to the set of connections that
// read it and want an invalidation. armed is the count of tracking-on connections,
// republished to the shard layer (shard.SetTrackingArmed) after every change so the
// owner's write-path gate reflects it. One mutex guards both, taken by a writer's
// owner goroutine (invalidate) and a reader's goroutine (record, arm, removeConn);
// deliveries run outside it, so a slow wake never stalls a concurrent record.
type trackingRegistry struct {
	mu    sync.Mutex
	keys  map[string]map[*connState]struct{}
	conns map[*connState]struct{}
	// bcast is the broadcast-mode prefix table a write scans: each entry pairs a
	// registered prefix with the connection that wants an invalidation for every key
	// matching it. Unlike keys it is never drained on a write (broadcast fires every
	// time, not once per caching cycle); a connection's entries are removed only when
	// it disarms or disconnects. A connection with an empty prefix set holds one
	// entry with the empty prefix, which matches every key.
	bcast []bcastEntry
	// redirTargets is the reverse index for REDIRECT: a target connection maps to the
	// set of tracking connections that redirect their invalidations to it. When the
	// target disconnects, removeConn walks this set to mark each dependent's redirect
	// broken, so a write never delivers to a gone connection. Empty for a server with
	// no redirected tracking connection.
	redirTargets map[*connState]map[*connState]struct{}
	armed        int
}

// invalidateChannel is the pub/sub channel a redirect target subscribes to receive
// invalidation messages, redis's reserved __redis__:invalidate. A RESP2 target sees
// them as ordinary messages on this channel; a RESP3 target sees invalidate pushes.
const invalidateChannel = "__redis__:invalidate"

// bcastEntry is one (connection, prefix) registration in the broadcast prefix
// table. The empty prefix matches every key, so a BCAST with no PREFIX is one entry
// with prefix "".
type bcastEntry struct {
	cs     *connState
	prefix string
}

func newTrackingRegistry() *trackingRegistry {
	return &trackingRegistry{
		keys:         make(map[string]map[*connState]struct{}),
		conns:        make(map[*connState]struct{}),
		redirTargets: make(map[*connState]map[*connState]struct{}),
	}
}

// arm turns tracking on for a connection: it stamps the connection's trackState,
// registers any redirect target, and bumps the armed count the shard-layer write
// gate reads. The caller (CLIENT TRACKING ON) has already checked the connection was
// not already tracking, so the count moves once per enable. redir is the resolved
// REDIRECT target or nil for direct-push mode. Runs on the connection's reader
// goroutine.
func (r *trackingRegistry) arm(cs *connState, redir *connState) {
	cs.tracking = &trackState{recorded: make(map[string]struct{})}
	r.mu.Lock()
	r.conns[cs] = struct{}{}
	r.setRedirectLocked(cs, redir)
	r.armed++
	shard.SetTrackingArmed(int64(r.armed))
	r.mu.Unlock()
}

// armBcast turns on broadcast-mode tracking for a connection: it stamps a bcast
// trackState carrying the connection's own prefix copy (for TRACKINGINFO) and
// registers one prefix-table entry per prefix, or a single empty-prefix entry when
// no PREFIX was given (which matches every key). Like arm it bumps the armed count
// the shard-layer gate reads. The caller (CLIENT TRACKING ON BCAST) has already
// disarmed any prior registration, so this always starts clean. Runs on the
// connection's reader goroutine.
func (r *trackingRegistry) armBcast(cs *connState, prefixes [][]byte, redir *connState) {
	ts := &trackState{bcast: true}
	for _, p := range prefixes {
		ts.prefixes = append(ts.prefixes, append([]byte(nil), p...))
	}
	cs.tracking = ts
	r.mu.Lock()
	r.conns[cs] = struct{}{}
	if len(prefixes) == 0 {
		r.bcast = append(r.bcast, bcastEntry{cs: cs, prefix: ""})
	} else {
		for _, p := range prefixes {
			r.bcast = append(r.bcast, bcastEntry{cs: cs, prefix: string(p)})
		}
	}
	r.setRedirectLocked(cs, redir)
	r.armed++
	shard.SetTrackingArmed(int64(r.armed))
	r.mu.Unlock()
}

// setRedirect updates a still-armed connection's redirect target (a re-run of
// CLIENT TRACKING ON that changes or clears REDIRECT). It takes the mutex and
// defers to setRedirectLocked. Runs on the connection's reader goroutine.
func (r *trackingRegistry) setRedirect(cs *connState, redir *connState) {
	r.mu.Lock()
	r.setRedirectLocked(cs, redir)
	r.mu.Unlock()
}

// setRedirectLocked points a connection's invalidations at redir (nil to clear),
// maintaining the reverse index both ways: it unlinks any prior target and links
// the new one. It also clears redirBroken, since a fresh REDIRECT re-declares a live
// target. The caller holds the mutex.
func (r *trackingRegistry) setRedirectLocked(cs *connState, redir *connState) {
	ts := cs.tracking
	if ts.redirect != nil {
		r.unlinkRedirLocked(cs, ts.redirect)
	}
	ts.redirect = nil
	ts.redirectID = 0
	ts.redirBroken = false
	if redir == nil {
		return
	}
	ts.redirect = redir
	ts.redirectID = redir.id
	set := r.redirTargets[redir]
	if set == nil {
		set = make(map[*connState]struct{})
		r.redirTargets[redir] = set
	}
	set[cs] = struct{}{}
}

// unlinkRedirLocked removes cs from target's dependent set, forgetting an emptied
// set. The caller holds the mutex.
func (r *trackingRegistry) unlinkRedirLocked(cs, target *connState) {
	if set := r.redirTargets[target]; set != nil {
		delete(set, cs)
		if len(set) == 0 {
			delete(r.redirTargets, target)
		}
	}
}

// redirectState reports a connection's redirect id and broken flag for GETREDIR and
// TRACKINGINFO, read under the mutex since the write path and a foreign teardown
// both touch these fields. id is -1 when the connection is not tracking, else the
// target client id (0 for no redirect); broken is true once the target disconnected.
func (r *trackingRegistry) redirectState(cs *connState) (id int64, broken bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cs.tracking == nil {
		return -1, false
	}
	return int64(cs.tracking.redirectID), cs.tracking.redirBroken
}

// removeConn drops a connection from the tracking table: it walks the connection's
// recorded-key set, removes it from each key's bucket, forgets an emptied bucket,
// drops any broadcast prefix entries it registered, and decrements the armed count.
// It is a no-op on a connection that never tracked, so both TRACKING OFF (which nils
// cs.tracking after) and the disconnect teardown call it safely, and a disconnect
// after an explicit OFF finds nil and skips. Runs under the mutex against live
// writers; the recorded set is this connection's, but its entries are shared with
// the forward index, so the whole walk holds the lock.
func (r *trackingRegistry) removeConn(cs *connState) {
	ts := cs.tracking
	// A redirect target is usually a plain (non-tracking) RESP2 sink, so the
	// broken-redirect marking below must run even when this connection never tracked.
	// Only the tracking-connection cleanup (recorded keys, bcast prefixes, own
	// redirect, armed count) is gated on ts != nil. When no connection is tracking at
	// all (the armed gate is zero), redirTargets is necessarily empty, so a
	// non-tracking connection's teardown skips the mutex entirely, the same fast path
	// an unarmed server had before REDIRECT. The gate is an atomic load, race-free.
	if ts == nil && !shard.TrackingArmed() {
		return
	}
	r.mu.Lock()
	if ts != nil {
		for k := range ts.recorded {
			r.dropLocked(k, cs)
		}
		if ts.bcast {
			r.bcast = dropBcastConn(r.bcast, cs)
		}
		// Unlink this connection's own redirect: it no longer wants deliveries routed
		// to its target, and the target's reverse-index entry for it must go so a later
		// target teardown does not walk a stale dependent.
		if ts.redirect != nil {
			r.unlinkRedirLocked(cs, ts.redirect)
		}
		delete(r.conns, cs)
		r.armed--
		shard.SetTrackingArmed(int64(r.armed))
	}
	// This connection may itself be a redirect target for others (a RESP2 client's
	// subscribed sink). Its departure breaks their redirection: mark each dependent
	// broken so a later write drops rather than delivers to this gone connection, and
	// forget the reverse-index bucket. redis's CLIENT_TRACKING_BROKEN_REDIR, surfaced
	// by TRACKINGINFO as broken_redirect.
	if deps := r.redirTargets[cs]; deps != nil {
		for dep := range deps {
			dep.tracking.redirect = nil
			dep.tracking.redirBroken = true
		}
		delete(r.redirTargets, cs)
	}
	r.mu.Unlock()
}

// dropBcastConn returns the prefix table with every entry belonging to cs removed,
// compacted in place. The caller holds the mutex.
func dropBcastConn(entries []bcastEntry, cs *connState) []bcastEntry {
	out := entries[:0]
	for _, e := range entries {
		if e.cs != cs {
			out = append(out, e)
		}
	}
	return out
}

// dropLocked removes cs from one key's bucket and forgets the bucket when it
// empties. The caller holds the mutex.
func (r *trackingRegistry) dropLocked(key string, cs *connState) {
	b := r.keys[key]
	if b == nil {
		return
	}
	delete(b, cs)
	if len(b) == 0 {
		delete(r.keys, key)
	}
}

// record notes that a tracking connection read a key, so a later write to it
// pushes an invalidation. It adds the connection to the key's forward bucket and
// the key to the connection's reverse set, both under the mutex. The caller has
// checked cs.tracking is non-nil (the connection is tracking) on its own reader
// goroutine. key is copied out of the parse buffer here, so the caller may reuse
// the buffer at once.
func (r *trackingRegistry) record(cs *connState, key []byte) {
	k := string(key)
	r.mu.Lock()
	b := r.keys[k]
	if b == nil {
		b = make(map[*connState]struct{})
		r.keys[k] = b
	}
	b[cs] = struct{}{}
	cs.tracking.recorded[k] = struct{}{}
	r.mu.Unlock()
}

// recordReadKeys records every key a read-only, cacheable command touched
// (dispatch.ReadKeys). It is the read-path seam the driver calls after a tracking
// connection's command dispatches; a write, a keyless command, or a read outside
// the curated set contributes no keys and this returns having done nothing.
//
// The connection's mode decides whether this command's reads are cached at all:
// default caches every read, optin caches only after CLIENT CACHING YES, optout
// caches every read except after CLIENT CACHING NO. The transient CACHING selector
// governs one command, so it is consumed and cleared here whether or not the
// command read anything.
func (r *trackingRegistry) recordReadKeys(cs *connState, args [][]byte) {
	ts := cs.tracking
	// Broadcast mode keeps no recorded-key table (it fires on prefix match, not on a
	// prior read), so there is nothing to record and no CACHING selector to consume.
	if ts.bcast {
		return
	}
	caching := ts.caching
	ts.caching = cachingNone
	switch {
	case ts.optin:
		if caching != cachingYes {
			return
		}
	case ts.optout:
		if caching == cachingNo {
			return
		}
	}
	for _, key := range dispatch.ReadKeys(args) {
		r.record(cs, key)
	}
}

// deliverySpec is one resolved invalidation delivery: the connection to push to and
// whether the payload must be framed as a RESP2 pub/sub message (a redirect to a
// subscribed RESP2 sink) rather than a RESP3 push. It is computed under the registry
// mutex (it reads a connection's redirect target and protocol) and consumed outside
// it, so a slow wake never stalls a concurrent record.
type deliverySpec struct {
	sc  *shard.Conn
	msg bool
}

// deliveryFor resolves where a tracking connection's invalidation goes, or reports
// that it is dropped. A connection with a live REDIRECT routes to its target: a RESP3
// target takes a push, a RESP2 target in subscribe mode takes a pub/sub message
// frame on __redis__:invalidate, and a RESP2 target that is not subscribed is dropped
// (it cannot receive the message). A broken redirect (the target disconnected) is
// dropped. A connection with no redirect pushes to itself when it is RESP3, else
// drops (TRACKING ON without a redirect requires RESP3, so this last drop is
// defensive). The caller holds the mutex.
func (r *trackingRegistry) deliveryFor(cs *connState) (deliverySpec, bool) {
	ts := cs.tracking
	if ts.redirBroken {
		return deliverySpec{}, false
	}
	if ts.redirect != nil {
		redir := ts.redirect
		if redir.sc.Resp3() {
			return deliverySpec{sc: redir.sc}, true
		}
		if redir.subCount.Load()+redir.psubCount.Load()+redir.ssubCount.Load() > 0 {
			return deliverySpec{sc: redir.sc, msg: true}, true
		}
		return deliverySpec{}, false
	}
	if cs.sc.Resp3() {
		return deliverySpec{sc: cs.sc}, true
	}
	return deliverySpec{}, false
}

// invalidate is the write-path hook shard.Runtime.UseInvalidator wires to. A write
// on any owner calls it with the touched key; it drains the key's bucket (once per
// caching cycle: the bucket is deleted, so the next read re-arms) and delivers one
// invalidation to each connection that cached the key, routed through REDIRECT when
// the connection set one. The delivery set is resolved under the mutex and the wakes
// run outside it, the pub/sub discipline, so the registry mutex never nests under a
// connection waker. Runs on the owner goroutine; DeliverOOB is owner-safe.
func (r *trackingRegistry) invalidate(key []byte, origin *shard.Conn) {
	k := string(key)
	r.mu.Lock()
	var specs []deliverySpec
	// Default mode: the recorded-key forward index. The bucket is drained on this
	// write, so the next read re-arms it (once per caching cycle).
	if b := r.keys[k]; b != nil {
		for cs := range b {
			// NOLOOP: a client that wrote the key itself does not want the invalidation
			// for its own write. The origin is the writing connection; a cached
			// connection matching it that set noloop is dropped from delivery, but its
			// cache entry is still forgotten so its next read re-arms like everyone's.
			if cs.sc != origin || !cs.tracking.noloop.Load() {
				if spec, ok := r.deliveryFor(cs); ok {
					specs = append(specs, spec)
				}
			}
			// Forget the key on each connection's reverse set too, so the next read
			// re-records it: invalidation is once per caching cycle.
			delete(cs.tracking.recorded, k)
		}
		delete(r.keys, k)
	}
	// Broadcast mode: every registered prefix that this key starts with, its
	// connection notified. Stateless, so nothing is drained; the same connection is
	// notified again on the next matching write. Overlap is rejected at registration,
	// so a connection matches at most one of its own prefixes and is added once.
	for _, e := range r.bcast {
		if !strings.HasPrefix(k, e.prefix) {
			continue
		}
		if e.cs.sc != origin || !e.cs.tracking.noloop.Load() {
			if spec, ok := r.deliveryFor(e.cs); ok {
				specs = append(specs, spec)
			}
		}
	}
	r.mu.Unlock()

	if len(specs) == 0 {
		return
	}
	// One wire per framing, reused across targets (DeliverOOB copies it into each
	// connection's buffer). Direct and RESP3-redirect targets take the invalidate
	// push; a RESP2-subscribed redirect target takes the pub/sub message frame.
	var pushWire, msgWire []byte
	for _, spec := range specs {
		if spec.msg {
			if msgWire == nil {
				msgWire = appendRedirectMessage(nil, key)
			}
			spec.sc.DeliverOOB(msgWire)
			continue
		}
		if pushWire == nil {
			pushWire = appendInvalidate(nil, key)
		}
		spec.sc.DeliverOOB(pushWire)
	}
}

// invalidateAll is the flush-path hook: a FLUSHALL or FLUSHDB empties every key, so
// every tracking connection's whole cache is stale at once. redis answers with a
// single invalidate whose payload is a null (rather than one per cached key), routed
// per connection like a normal invalidation (direct push, or a REDIRECT to the
// target's push or RESP2 message frame). It snapshots the delivery set under the
// mutex, clears the whole forward index and each connection's recorded set, then
// wakes the targets outside the lock. Runs on the reader goroutine of the connection
// that issued the flush; DeliverOOB into another connection's buffer is owner-safe,
// the same cross-connection delivery pub/sub does.
func (r *trackingRegistry) invalidateAll() {
	r.mu.Lock()
	if len(r.conns) == 0 {
		r.mu.Unlock()
		return
	}
	specs := make([]deliverySpec, 0, len(r.conns))
	for cs := range r.conns {
		if spec, ok := r.deliveryFor(cs); ok {
			specs = append(specs, spec)
		}
		// The whole cache is gone; drop the recorded set so the next read re-arms.
		cs.tracking.recorded = make(map[string]struct{})
	}
	// One flush clears the entire forward index.
	r.keys = make(map[string]map[*connState]struct{})
	r.mu.Unlock()

	var pushWire, msgWire []byte
	for _, spec := range specs {
		if spec.msg {
			if msgWire == nil {
				msgWire = appendRedirectMessageNull(nil)
			}
			spec.sc.DeliverOOB(msgWire)
			continue
		}
		if pushWire == nil {
			pushWire = appendInvalidateNull(nil)
		}
		spec.sc.DeliverOOB(pushWire)
	}
}

// appendInvalidate builds the RESP3 client-side-caching invalidation push: a
// two-element push of "invalidate" and a one-key array. redis carries an array so
// one message can name several keys (a FLUSH sends a null instead); this slice
// invalidates one key per write, so the array holds one name.
func appendInvalidate(dst []byte, key []byte) []byte {
	dst = resp.AppendPushHeader(dst, 2)
	dst = resp.AppendBulk(dst, []byte("invalidate"))
	dst = resp.AppendArrayHeader(dst, 1)
	dst = resp.AppendBulk(dst, key)
	return dst
}

// appendInvalidateNull builds the flush invalidation push: the two-element
// "invalidate" push with a RESP3 null payload in place of the key array, redis's
// signal that every cached key is gone at once.
func appendInvalidateNull(dst []byte) []byte {
	dst = resp.AppendPushHeader(dst, 2)
	dst = resp.AppendBulk(dst, []byte("invalidate"))
	dst = resp.AppendNull3(dst)
	return dst
}

// appendRedirectMessage builds the RESP2 redirect delivery: a three-element pub/sub
// message frame on __redis__:invalidate whose payload is the invalidated key array,
// the shape a RESP2 client subscribed to the invalidation channel already parses. It
// is what a redirect target sees in place of the RESP3 invalidate push (redis frames
// the redirect to a RESP2 subscriber as an ordinary message on the reserved channel).
func appendRedirectMessage(dst []byte, key []byte) []byte {
	dst = resp.AppendArrayHeader(dst, 3)
	dst = resp.AppendBulk(dst, []byte("message"))
	dst = resp.AppendBulk(dst, []byte(invalidateChannel))
	dst = resp.AppendArrayHeader(dst, 1)
	dst = resp.AppendBulk(dst, key)
	return dst
}

// appendRedirectMessageNull is the flush twin of appendRedirectMessage: the message
// frame on __redis__:invalidate with a RESP2 null bulk in place of the key array,
// the redirect-target form of the whole-cache-gone signal.
func appendRedirectMessageNull(dst []byte) []byte {
	dst = resp.AppendArrayHeader(dst, 3)
	dst = resp.AppendBulk(dst, []byte("message"))
	dst = resp.AppendBulk(dst, []byte(invalidateChannel))
	dst = resp.AppendNull(dst)
	return dst
}
