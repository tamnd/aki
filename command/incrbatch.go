package command

// Batched pipeline hand-off for the increment family (INCR, INCRBY, DECR, DECRBY).
//
// The current read-modify-write fast path computes each increment on the
// connection goroutine under the per-shard rmwLock, then fires the durable write
// to the shard owner. Under a deep pipeline fifty connections contend that lock on
// one shard and the runtime pays a park-and-wake for every command, which the INCR
// profile showed as its largest cost. This path removes both: a connection
// accumulates its whole pipeline of increments during the drain and, at the end of
// the drain (or before any command that is not itself a batchable increment), hands
// each shard one request carrying that shard's whole sub-batch. The shard owner,
// the sole writer of that shard, computes and persists the sub-batch serially with
// no lock, so the work serializes onto eight busy owners instead of fifty
// contending connections, and the wakeup is paid once per shard per drain instead
// of once per command. See notes/Spec/2064/implementation/211.

import (
	"math"
	"strconv"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
)

// deferredIncr is one accumulated increment. The handler fills shard, key, argv
// and delta during the drain; applyIncrBatch fills res and result on the shard
// owner; flushIncrPending reads them back to write the reply and propagate in
// pipeline order. The pending list holds these by value, so accumulating a deep
// pipeline grows one backing array instead of allocating a struct per command.
type deferredIncr struct {
	shard  int
	key    []byte   // copy of argv[1], owned by this struct
	argv   [][]byte // copy of the command, for verbatim AOF propagation
	delta  int64
	res    int8
	result int64
}

const (
	incrResOK        int8 = 0
	incrResWrongType int8 = 1
	incrResNotInt    int8 = 2
	incrResOverflow  int8 = 3
	incrResErr       int8 = 4
)

// applyIncrBatch computes and persists a connection's accumulated increments by
// handing each shard one fn request that processes the shard's whole sub-batch on
// the owner goroutine, then waits for every shard. Sub-batches to distinct shards
// run on distinct owners in parallel; a shard's own sub-batch runs serially on its
// owner with no rmwLock, which is the mutual exclusion the lock used to provide
// because the owner is the sole writer of the shard. Each op's res and result are
// filled in place for the caller to read back.
func (e *Engine) applyIncrBatch(index int, ops []deferredIncr) {
	var byShard [keyspace.NumShards][]*deferredIncr
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
		req.fn = incrShardFn(e, sops)
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

// incrShardFn returns the closure the shard owner runs to apply one shard's
// sub-batch. It mirrors the inline increment compute: a missing key counts as
// zero, a non-string key is a wrong-type error, an unparseable value is a not-int
// error, and an out-of-range sum is an overflow error. A successful op stages the
// new value with a fresh version through the same durable sink SET uses, and a
// later op on the same key in this sub-batch sees it because the owner is the sole
// writer and SetWithVersion updates the hot cache the next Get consults.
func incrShardFn(e *Engine, ops []*deferredIncr) func(*keyspace.DB) error {
	return func(db *keyspace.DB) error {
		// One reusable buffer for the formatted result, refilled per op. SetWithVersion
		// copies the body into the leaf cell and stores cell[HeaderSize:] (a slice of its
		// own allocation) in the hot cache, so it never retains the caller's slice; reusing
		// this across the sub-batch is safe and drops one heap allocation per increment,
		// which the profile showed as the increment path's GC cost under a deep pipeline.
		// An int64 in base ten is at most 20 bytes (-9223372036854775808).
		var buf [20]byte
		for _, op := range ops {
			cur, hdr, found, err := db.Get(op.key)
			if err != nil {
				op.res = incrResErr
				continue
			}
			if found && hdr.Type != keyspace.TypeString {
				op.res = incrResWrongType
				continue
			}
			var base int64
			if found {
				v, ok := parseInteger(cur)
				if !ok {
					op.res = incrResNotInt
					continue
				}
				base = v
			}
			sum, ok := addInt64(base, op.delta)
			if !ok {
				op.res = incrResOverflow
				continue
			}
			op.result = sum
			op.res = incrResOK
			body := strconv.AppendInt(buf[:0], sum, 10)
			ver := e.ks.NextVersionForKey(op.key)
			if werr := db.SetWithVersion(op.key, body, keyspace.TypeString, keyspace.EncInt, keepTTL(hdr, found), ver); werr != nil {
				op.res = incrResErr
			}
		}
		return nil
	}
}

// structuralIncrOK reports whether name/argv is a well-formed increment that can be
// deferred: the right arity, a parseable amount, and (for DECRBY) an amount whose
// negation does not overflow. A command that fails this still runs through the
// normal inline path, which reports the same error, so deferral is skipped and any
// pending batch is flushed ahead of it to keep reply order.
func structuralIncrOK(name string, argv [][]byte) bool {
	switch name {
	case "incr", "decr":
		return len(argv) == 2
	case "incrby":
		if len(argv) != 3 {
			return false
		}
		_, ok := parseInteger(argv[2])
		return ok
	case "decrby":
		if len(argv) != 3 {
			return false
		}
		n, ok := parseInteger(argv[2])
		return ok && n != math.MinInt64
	default:
		return false
	}
}

// aclUnrestricted reports whether the user has the default unrestricted grant
// (~*, &*, +@all and no selectors). Only then is a command guaranteed to clear the
// ACL gate, so only then is it safe to skip the pre-flush and defer the command.
// Any custom or restricted user disables batching and takes the inline path, which
// is always correct.
func aclUnrestricted(u *aclUser) bool {
	if u == nil {
		return true
	}
	if !u.allKeys() || len(u.selectors) != 0 || len(u.cmdRules) != 1 {
		return false
	}
	r := u.cmdRules[0]
	return r.grant && r.category == "@all"
}

// canBatchIncr reports whether the connection is in a state where a clean
// increment will pass every dispatch gate and can therefore be deferred. It is
// deliberately conservative: any unusual server or connection state (replication,
// cluster, loading, a read-only replica, a subscription, a restricted user, a
// blocked write) returns false and the increment takes the inline path. The common
// case, a writable standalone master with the default user, returns true.
func (d *Dispatcher) canBatchIncr(c *networking.Conn, sess *session) bool {
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
	// With maxmemory set, a denyoom write can be rejected by runCommand's OOM guard
	// before the handler runs. That rejection would write its error ahead of the
	// still-pending batch and scramble reply order, so batching stays off whenever a
	// limit is configured. The benchmark sets no limit, so the fast path is intact.
	if d.conf != nil && d.conf.maxMemory() > 0 {
		return false
	}
	return true
}

// flushIncrPending applies a connection's accumulated increments and writes their
// replies, propagation and notifications in pipeline order. It is called at the end
// of a drain and before any command that is not a batchable increment, so the
// deferred replies always land at their correct position in the output stream.
//
// Replication is never active here (canBatchIncr requires it off), so propagation
// is AOF only. A successful increment propagates verbatim, the same record real
// Redis writes, because INCR is deterministic and rewriteForAOF returns it
// unchanged.
func (d *Dispatcher) flushIncrPending(c *networking.Conn, sess *session) {
	ops := sess.incrPend
	if len(ops) == 0 {
		return
	}
	// Detach before applying so the next command in the drain starts a fresh batch
	// and a re-entrant call cannot flush the same ops twice. The captured ops slice
	// keeps its length and backing array, so the reads below are unaffected.
	sess.incrPend = sess.incrPend[:0]

	db := c.DB()
	d.engine.applyIncrBatch(db, ops)

	enc := c.Enc()
	aofOn := d.aofEnabled()
	// Online connections buffer per session regardless of policy; under always
	// OnBatchComplete group-commits the drain with one fsync before the replies go
	// out, so the buffered records are durable before this batch's replies.
	bufferAOF := aofOn && !c.IsOffline()
	tracking := d.trackingActive()
	for i := range ops {
		op := &ops[i]
		switch op.res {
		case incrResWrongType:
			enc.WriteError(wrongTypeError)
		case incrResNotInt:
			enc.WriteError("ERR value is not an integer or out of range")
		case incrResOverflow:
			enc.WriteError("ERR increment or decrement would overflow")
		case incrResErr:
			enc.WriteError("ERR increment failed")
		default:
			enc.WriteInteger(op.result)
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
			d.notifyKeyspaceEvent(db, notifyString, "incrby", string(op.key))
			if tracking {
				d.trackingInvalidateKey(op.key, c.ID())
			}
			d.persist.markDirty()
		}
	}
}

// copyArgv deep-copies a command's argument vector so it survives past the drain.
// The argv slices point into the connection read buffer, which the parser reuses
// for the next command, so a deferred increment must own its own copy to propagate
// verbatim at flush time.
func copyArgv(argv [][]byte) [][]byte {
	out := make([][]byte, len(argv))
	for i, a := range argv {
		out[i] = append([]byte(nil), a...)
	}
	return out
}
