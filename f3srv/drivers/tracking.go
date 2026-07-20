package drivers

import (
	"sync"

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
// have. BCAST, OPTIN/OPTOUT, NOLOOP, and the RESP2 REDIRECT path are later slices;
// TRACKING ON refuses everything but a bare ON/OFF here, and it requires RESP3 (the
// honest redis answer when no redirection client is given).

// trackState is one connection's CLIENT TRACKING configuration, nil on a
// connection that never enabled it. In this slice it holds only the recorded-key
// set (the reverse index the disconnect and TRACKING OFF cleanups walk); the mode
// flags, prefixes, and redirect target join with their slices. Reader-owned like
// the pub/sub sets: the pointer is set and cleared on the connection's own reader
// goroutine, and recorded is mutated only under the registry mutex.
type trackState struct {
	recorded map[string]struct{}
}

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
	armed int
}

func newTrackingRegistry() *trackingRegistry {
	return &trackingRegistry{
		keys:  make(map[string]map[*connState]struct{}),
		conns: make(map[*connState]struct{}),
	}
}

// arm turns tracking on for a connection: it stamps the connection's trackState
// and bumps the armed count the shard-layer write gate reads. The caller (CLIENT
// TRACKING ON) has already checked the connection was not already tracking, so the
// count moves once per enable. Runs on the connection's reader goroutine.
func (r *trackingRegistry) arm(cs *connState) {
	cs.tracking = &trackState{recorded: make(map[string]struct{})}
	r.mu.Lock()
	r.conns[cs] = struct{}{}
	r.armed++
	shard.SetTrackingArmed(int64(r.armed))
	r.mu.Unlock()
}

// removeConn drops a connection from the tracking table: it walks the connection's
// recorded-key set, removes it from each key's bucket, forgets an emptied bucket,
// and decrements the armed count. It is a no-op on a connection that never tracked,
// so both TRACKING OFF (which nils cs.tracking after) and the disconnect teardown
// call it safely, and a disconnect after an explicit OFF finds nil and skips. Runs
// under the mutex against live writers; the recorded set is this connection's, but
// its entries are shared with the forward index, so the whole walk holds the lock.
func (r *trackingRegistry) removeConn(cs *connState) {
	ts := cs.tracking
	if ts == nil {
		return
	}
	r.mu.Lock()
	for k := range ts.recorded {
		r.dropLocked(k, cs)
	}
	delete(r.conns, cs)
	r.armed--
	shard.SetTrackingArmed(int64(r.armed))
	r.mu.Unlock()
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
func (r *trackingRegistry) recordReadKeys(cs *connState, args [][]byte) {
	for _, key := range dispatch.ReadKeys(args) {
		r.record(cs, key)
	}
}

// invalidate is the write-path hook shard.Runtime.UseInvalidator wires to. A write
// on any owner calls it with the touched key; it drains the key's bucket (once per
// caching cycle: the bucket is deleted, so the next read re-arms) and pushes one
// RESP3 invalidate message to each connection that cached the key. The subscriber
// set is snapshotted under the mutex and the deliveries run outside it, the pub/sub
// discipline, so the registry mutex never nests under a connection waker. Runs on
// the owner goroutine; DeliverOOB is owner-safe.
func (r *trackingRegistry) invalidate(key []byte) {
	k := string(key)
	r.mu.Lock()
	b := r.keys[k]
	if b == nil {
		r.mu.Unlock()
		return
	}
	targets := make([]*shard.Conn, 0, len(b))
	for cs := range b {
		targets = append(targets, cs.sc)
		// Forget the key on each connection's reverse set too, so the next read
		// re-records it: invalidation is once per caching cycle.
		delete(cs.tracking.recorded, k)
	}
	delete(r.keys, k)
	r.mu.Unlock()

	// One wire, reused across targets (DeliverOOB copies it into each connection's
	// buffer). Every tracking connection is RESP3 in this slice (TRACKING ON
	// refuses RESP2), so the RESP2 branch is a defensive skip, not a live path.
	var wire []byte
	for _, sc := range targets {
		if !sc.Resp3() {
			continue
		}
		if wire == nil {
			wire = appendInvalidate(nil, key)
		}
		sc.DeliverOOB(wire)
	}
}

// invalidateAll is the flush-path hook: a FLUSHALL or FLUSHDB empties every key, so
// every tracking connection's whole cache is stale at once. redis answers with a
// single invalidate push whose payload is a null (rather than one push per cached
// key), sent to every tracking client. It snapshots the connection set under the
// mutex, clears the whole forward index and each connection's recorded set, then
// delivers the one null push outside the lock. Runs on the reader goroutine of the
// connection that issued the flush; DeliverOOB into another connection's buffer is
// owner-safe, the same cross-connection delivery pub/sub does.
func (r *trackingRegistry) invalidateAll() {
	r.mu.Lock()
	if len(r.conns) == 0 {
		r.mu.Unlock()
		return
	}
	targets := make([]*shard.Conn, 0, len(r.conns))
	for cs := range r.conns {
		targets = append(targets, cs.sc)
		// The whole cache is gone; drop the recorded set so the next read re-arms.
		cs.tracking.recorded = make(map[string]struct{})
	}
	// One flush clears the entire forward index.
	r.keys = make(map[string]map[*connState]struct{})
	r.mu.Unlock()

	var wire []byte
	for _, sc := range targets {
		if !sc.Resp3() {
			continue
		}
		if wire == nil {
			wire = appendInvalidateNull(nil)
		}
		sc.DeliverOOB(wire)
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
