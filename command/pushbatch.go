package command

// Batched pipeline hand-off for the coll-form list pushes (LPUSH, RPUSH and their
// X variants).
//
// A list in the btree-backed (coll) form cannot ride the whole-body write-behind
// fast path: pushList's compute returns fallback for it, so the push runs through
// the synchronous shard-owner round trip (rmwWriteBehind -> runSync ->
// updateShard, which blocks on <-req.done). Under a deep pipeline of pushes to one
// coll-form key that serializes the whole pipeline against one shard owner, one
// round trip per command, which note 241 measured as the worst two cells on the
// throughput matrix (rpush/lpush p16 at 0.49x/0.57x of Redis 7.4).
//
// This path removes the per-command round trip the same way incrbatch.go did for
// the increment family: a connection accumulates its pipeline of coll-form pushes
// during the drain and, at the end of the drain (or before any command that is not
// itself a batchable push), hands each shard one request carrying that shard's
// whole sub-batch. The shard owner, the sole writer of that shard, applies the
// sub-batch serially, so a pipeline of N pushes to one key pays one round trip
// instead of N. Blob-form pushes are left alone: they already have a non-blocking
// async write-behind path, so only the coll-form case (the one that blocks) is
// deferred, gated by an O(1) header probe (pushKeyIsColl). See
// notes/Spec/2064/implementation/245.

import (
	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
)

// deferredPush is one accumulated list push. The handler fills shard, key, vals,
// argv, head and mustExist during the drain; applyPushBatch fills res, newLen and
// changed on the shard owner; flushPushPending reads them back to write the reply
// and propagate in pipeline order. The pending list holds these by value, so
// accumulating a deep pipeline grows one backing array instead of allocating a
// struct per command.
type deferredPush struct {
	shard     int
	key       []byte   // copy of argv[1], owned by this struct
	vals      [][]byte // copy of argv[2:], the elements to push, owned by this struct
	argv      [][]byte // copy of the whole command for verbatim AOF, nil when AOF is off
	head      bool     // true for LPUSH/LPUSHX, false for RPUSH/RPUSHX
	mustExist bool     // true for the X variants: a no-op returning 0 on an absent key
	res       int8
	newLen    int64
	changed   bool // an element was actually added (drives notify, signal and AOF)
}

const (
	pushResOK        int8 = 0
	pushResWrongType int8 = 1
	pushResErr       int8 = 2
)

// pushResult is what applyListPush reports for one push: the outcome code, the new
// length, and whether the key actually changed (an X push on an absent key is a
// successful no-op that changes nothing and must not notify or propagate).
type pushResult struct {
	res     int8
	newLen  int64
	changed bool
}

// applyListPush applies one LPUSH/RPUSH/LPUSHX/RPUSHX against db and reports the
// outcome. It is the shared core of the synchronous push path (pushList's sync
// closure) and the batched path (pushShardFn): both run it on the shard owner
// against the key's own database. It mirrors the sync closure verbatim, routing a
// coll-form list to the window-write callback and a blob or fresh key to the splice
// path, including the listpack-to-quicklist promotion and the early-coll boundary.
// The returned error is a real engine failure; res carries the wrong-type and
// must-exist-absent outcomes that are not errors.
func applyListPush(db *keyspace.DB, key []byte, vals [][]byte, head, mustExist bool, lim encLimits) (pushResult, error) {
	var (
		res   pushResult
		hdr   keyspace.ValueHeader
		found bool
	)
	route, err := db.CollUpdateRouted(key, keyspace.TypeList, keyspace.EncQuicklist,
		func(rFound bool, h keyspace.ValueHeader, _ []byte) keyspace.CollRoute {
			hdr, found = h, rFound
			if rFound && h.Type != keyspace.TypeList {
				res.res = pushResWrongType
				return keyspace.CollRouteSkip
			}
			if !rFound && mustExist {
				// X push on an absent key: successful no-op, reply 0, no change.
				return keyspace.CollRouteSkip
			}
			if rFound && h.IsColl() {
				return keyspace.CollRouteColl
			}
			return keyspace.CollRouteBlob
		},
		func(w *keyspace.CollWriter) error {
			n, e := listTreePush(w, vals, head)
			if e != nil {
				return e
			}
			res.newLen = n
			res.changed = true
			w.SetEnc(listCollReportedEnc(lim, hdr.Encoding, int(n), w.Bytes()))
			return nil
		})
	if err != nil {
		res.res = pushResErr
		return res, err
	}
	if route != keyspace.CollRouteBlob {
		// Coll write done, or a skip (wrong type or X on an absent key); res carries it.
		return res, nil
	}
	// Blob form (or a fresh key): splice the pushed run into the raw body.
	var body []byte
	if found {
		body, _, _, err = db.Get(key)
		if err != nil {
			res.res = pushResErr
			return res, err
		}
	}
	newBody, newCount, err := listBlobPush(body, vals, head)
	if err != nil {
		res.res = pushResErr
		return res, err
	}
	res.newLen = int64(newCount)
	res.changed = true
	prev := uint8(keyspace.EncListpack)
	if found {
		prev = hdr.Encoding
	}
	enc, err := listBlobReportedEnc(lim, prev, newBody)
	if err != nil {
		res.res = pushResErr
		return res, err
	}
	if enc == keyspace.EncQuicklist || len(newBody) > keyspace.MaxInlineBody {
		elems, e := listDecode(newBody)
		if e != nil {
			res.res = pushResErr
			return res, e
		}
		if e := listPromote(db, key, elems, enc); e != nil {
			res.res = pushResErr
			return res, e
		}
		return res, nil
	}
	if e := db.Set(key, newBody, keyspace.TypeList, enc, keepTTL(hdr, found)); e != nil {
		res.res = pushResErr
		return res, e
	}
	return res, nil
}

// pushShardFn returns the closure the shard owner runs to apply one shard's
// sub-batch. Each op runs through applyListPush serially on the owner, so a later
// push to the same key in this sub-batch sees the earlier one because the owner is
// the sole writer of the shard. An engine error on one op marks that op failed and
// the rest of the sub-batch still applies, mirroring incrShardFn.
func pushShardFn(ops []*deferredPush, lim encLimits) func(*keyspace.DB) error {
	return func(db *keyspace.DB) error {
		for _, op := range ops {
			r, err := applyListPush(db, op.key, op.vals, op.head, op.mustExist, lim)
			if err != nil {
				op.res = pushResErr
				continue
			}
			op.res = r.res
			op.newLen = r.newLen
			op.changed = r.changed
		}
		return nil
	}
}

// applyPushBatch computes and persists a connection's accumulated pushes by handing
// each shard one fn request that processes the shard's whole sub-batch on the owner
// goroutine, then waits for every shard. Sub-batches to distinct shards run on
// distinct owners in parallel; a shard's own sub-batch runs serially on its owner,
// which is the ordering the single-writer-per-shard model already guarantees. It
// mirrors applyIncrBatch.
func (e *Engine) applyPushBatch(index int, ops []deferredPush, lim encLimits) {
	var byShard [keyspace.NumShards][]*deferredPush
	for i := range ops {
		op := &ops[i]
		byShard[op.shard] = append(byShard[op.shard], op)
	}
	var reqs [keyspace.NumShards]*writeReq
	for s := 0; s < keyspace.NumShards; s++ {
		sops := byShard[s]
		if len(sops) == 0 {
			continue
		}
		req := writeReqPool.Get().(*writeReq)
		req.index = index
		req.shard = s
		req.setKey = nil
		req.fn = pushShardFn(sops, lim)
		reqs[s] = req
		e.shardQ[s].push(req)
	}
	for s := 0; s < keyspace.NumShards; s++ {
		if reqs[s] != nil {
			<-reqs[s].done
			reqs[s].fn = nil
			writeReqPool.Put(reqs[s])
		}
	}
}

// structuralPushOK reports whether name/argv is a well-formed push that can be
// deferred: one of the push verbs with at least a key and one value. A command that
// fails this still runs the normal inline path, so deferral is simply skipped.
func structuralPushOK(name string, argv [][]byte) bool {
	switch name {
	case "lpush", "rpush", "lpushx", "rpushx":
		return len(argv) >= 3
	default:
		return false
	}
}

// canBatchPush reports whether the connection is in a state where a clean push will
// pass every dispatch gate and can therefore be deferred. It mirrors canBatchIncr:
// any unusual server or connection state takes the inline path, which is always
// correct. The common case, a writable standalone master with the default user,
// returns true.
func (d *Dispatcher) canBatchPush(c *networking.Conn, sess *session) bool {
	if d.engine == nil || !d.engine.isDeferred() {
		return false
	}
	if c.IsOffline() || !sess.authenticated || sess.inMulti || sess.fromMaster {
		return false
	}
	if sess.subCount() != 0 || !aclUnrestricted(sess.user) {
		return false
	}
	if d.loading.Load() || d.isReadonlyReplica() || d.replActive() || d.clusterEnabled() {
		return false
	}
	if d.writesBlockedByBgsaveError() || !d.enoughGoodReplicas() {
		return false
	}
	// maxmemory: a denyoom push can be rejected by the OOM guard before the handler
	// runs, which would scramble reply order against a still-pending batch, so
	// batching stays off whenever a limit is configured (same rule as canBatchIncr).
	if d.conf != nil && d.conf.maxMemory() > 0 {
		return false
	}
	return true
}

// pushKeyIsColl is the O(1) routing probe that gates deferral to coll-form pushes
// only. A blob-form list already has a non-blocking async write-behind path, so
// deferring it would buy nothing and route it through a heavier path; only the
// coll-form case pays the synchronous round trip this batch removes. The probe
// reads the key's header without the shard write lock, so it can be stale, but that
// is safe: applyListPush re-routes through CollUpdateRouted on the owner, so a key
// that changed form (or type, or vanished) between the probe and the flush is still
// handled correctly. The probe only decides whether to take the batched path.
func (d *Dispatcher) pushKeyIsColl(c *networking.Conn, argv [][]byte) bool {
	if d.engine == nil {
		return false
	}
	db, err := d.engine.ks.DB(c.DB())
	if err != nil {
		return false
	}
	_, hdr, found, err := db.Peek(argv[1])
	if err != nil || !found {
		return false
	}
	return hdr.Type == keyspace.TypeList && hdr.IsColl()
}

// flushPushPending applies a connection's accumulated pushes and writes their
// replies, propagation and notifications in pipeline order. It is called at the end
// of a drain and before any command that is not a batchable push, so the deferred
// replies always land at their correct position in the output stream. Replication
// is never active here (canBatchPush requires it off), so propagation is AOF only,
// the verbatim record real Redis writes for a deterministic push.
func (d *Dispatcher) flushPushPending(c *networking.Conn, sess *session) {
	ops := sess.pushPend
	if len(ops) == 0 {
		return
	}
	// Detach before applying so the next command in the drain starts a fresh batch
	// and a re-entrant call cannot flush the same ops twice.
	sess.pushPend = sess.pushPend[:0]

	db := c.DB()
	d.engine.applyPushBatch(db, ops, d.encLimits())

	enc := c.Enc()
	aofOn := d.aofEnabled()
	bufferAOF := aofOn && !c.IsOffline()
	tracking := d.trackingActive()
	blockingActive := d.blocking.active.Load() != 0
	for i := range ops {
		op := &ops[i]
		switch op.res {
		case pushResWrongType:
			enc.WriteError(wrongTypeError)
		case pushResErr:
			enc.WriteError("ERR push failed")
		default:
			enc.WriteInteger(op.newLen)
			if !op.changed {
				// X push on an absent key: replied 0, nothing else to do.
				continue
			}
			if aofOn && op.argv != nil {
				args := rewriteForAOF(string(op.argv[0]), op.argv)
				if args != nil {
					if bufferAOF {
						d.bufferAOFRecord(sess, db, args)
					} else {
						d.appendAOF(db, args)
					}
				}
			}
			event := "rpush"
			if op.head {
				event = "lpush"
			}
			d.notifyKeyspaceEvent(db, notifyList, event, string(op.key))
			if tracking {
				d.trackingInvalidateKey(op.key, c.ID())
			}
			// Wake any client blocked on this key (BLPOP and friends). serveReady serves
			// the oldest waiter one element, matching the inline path's per-push signal.
			if blockingActive {
				d.serveReady(db, op.key, c.ID())
			}
			d.persist.markDirty()
		}
	}
}
