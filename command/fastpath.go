package command

import (
	"math"
	"sync/atomic"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// fastpath.go carries the v2 standalone server's raw-buffer GET/SET shortcut into
// the integrated drain loop (task #98, spec 2064). The end-to-end saturation
// measurement (implementation note 266) found the hybrid engine 2.7-3.1x the btree
// path in-process, but only ~1.4x at the wire, because every command including GET
// and SET pays the full Dispatcher.Handle preamble: the arena ctx, the name lower,
// the batched-write-handoff switch, the command-table lookup, the ACL and
// replica/cluster gates, and then runCommand's propagation, monitor, slowlog and
// latency machinery. The v2 server skips all of it for GET and SET and clears a
// clean 2.00x Valkey at P64; this path does the same inside the real server.
//
// The bypass is sound because it is taken only when none of the skipped work would
// have changed the result or a side effect a client could observe. tryFastGetSet
// gates on both a server-wide environment check (fastEnvOK) and a per-connection
// session check (session.fastPlain): together they guarantee no AOF record, no
// replication, no monitor feed, no keyspace notification, no tracking invalidation,
// no eviction, no cluster redirect, no ACL restriction, no transaction queueing and
// no subscriber-mode constraint applies to this command. When any of those is live
// the gate fails and the command falls through to the full Handle path unchanged.
// The one observable kept on the fast path is commandstats: the call is still
// counted against the GET or SET descriptor so INFO commandstats stays accurate.

// tryFastGetSet serves a plain GET or SET off the hybrid-log store, returning true
// when it handled the command and false to fall through to full dispatch. The
// caller has already confirmed d.hybridFast.
func (d *Dispatcher) tryFastGetSet(c *networking.Conn, sess *session, argv [][]byte) bool {
	switch len(argv) {
	case 2:
		switch {
		case nameIs(argv[0], 'g', 'e', 't'):
			if !d.fastEnvOK() || !sess.fastPlain() {
				return false
			}
			return d.fastGet(c, sess, argv[1])
		case nameIsN(argv[0], "incr"):
			if !d.fastEnvOK() || !sess.fastPlain() {
				return false
			}
			return d.fastIncr(c, sess, argv[1], 1, d.incrCmd, "incr")
		case nameIsN(argv[0], "decr"):
			if !d.fastEnvOK() || !sess.fastPlain() {
				return false
			}
			return d.fastIncr(c, sess, argv[1], -1, d.decrCmd, "decr")
		default:
			return false
		}
	case 3:
		switch {
		case nameIs(argv[0], 's', 'e', 't'):
			if !d.fastEnvOK() || !sess.fastPlain() {
				return false
			}
			return d.fastSet(c, sess, argv[1], argv[2])
		case nameIsN(argv[0], "incrby"):
			n, ok := parseInteger(argv[2])
			if !ok {
				return false
			}
			if !d.fastEnvOK() || !sess.fastPlain() {
				return false
			}
			return d.fastIncr(c, sess, argv[1], n, d.incrbyCmd, "incrby")
		case nameIsN(argv[0], "decrby"):
			n, ok := parseInteger(argv[2])
			if !ok || n == math.MinInt64 {
				// MinInt64 cannot be negated without overflow; let the full path render
				// the same out-of-range error the general DECRBY produces.
				return false
			}
			if !d.fastEnvOK() || !sess.fastPlain() {
				return false
			}
			return d.fastIncr(c, sess, argv[1], -n, d.decrbyCmd, "decrby")
		default:
			return false
		}
	default:
		return false
	}
}

// fastGet answers GET key from the hybrid store. It mirrors the hybrid branch of
// handleGet: read straight off the store (lazy expiry handled inside the read),
// reject a non-string value with WRONGTYPE, and write the bulk string or a null.
func (d *Dispatcher) fastGet(c *networking.Conn, sess *session, key []byte) bool {
	b, h, ok, err := d.engine.viewHybridGet(c.DB(), key)
	if err != nil {
		// A store error is rare; let the full path render it so the error reply and
		// its errorstats tally match the general code exactly.
		return false
	}
	if ok && h.Type != keyspace.TypeString {
		c.Enc().WriteError(wrongTypeError)
		d.statError(wrongTypeError)
		d.statCall(d.getCmd, 0, true)
		sess.lastCmd = "get"
		return true
	}
	if ok {
		// WriteBulk skips the encoder's per-reply length allocation; the null reply
		// is a pooled static slice the encoder already writes without allocating.
		c.WriteBulk(b)
	} else {
		c.Enc().WriteNull()
	}
	d.statCall(d.getCmd, 0, false)
	sess.lastCmd = "get"
	return true
}

// fastSet applies SET key value with no options to the hybrid store and replies
// OK. It mirrors the write half of handleSet's plain-SET case without the option
// parse or the old-value read, and bumps the dirty counter so INFO persistence and
// the save points see the change exactly as the general path would.
func (d *Dispatcher) fastSet(c *networking.Conn, sess *session, key, val []byte) bool {
	if err := d.engine.hybridSet(c.DB(), key, val); err != nil {
		return false
	}
	d.persist.markDirty()
	c.WriteRaw(resp.ReplyOK)
	d.statCall(d.setCmd, 0, false)
	sess.lastCmd = "set"
	return true
}

// fastIncr applies an INCR/INCRBY/DECR/DECRBY straight on the hybrid-log store and
// writes the reply, mirroring incrBy without the dispatch preamble. delta already
// carries the sign (DECR/DECRBY pass a negative). cmd is the resolved descriptor so
// commandstats stay accurate, and name backs sess.lastCmd. It returns true once it
// has handled the command (including the error replies), or false on a store error
// so the full path renders that exactly.
//
// This is the increment half of the bypass note 278 named: GET and SET shortcut the
// preamble but INCR did not, so on the hybrid engine INCR ran at about half of SET
// because it paid the whole Handle path (arena ctx, table lookup, ACL and replica
// gates, runCommand propagation and monitor/slowlog/latency) plus a shard round
// trip. The same gates (fastEnvOK, fastPlain) prove none of that skipped work would
// change the result or a visible side effect; keyspace notifications being off means
// no "incrby" event is owed.
func (d *Dispatcher) fastIncr(c *networking.Conn, sess *session, key []byte, delta int64, cmd *CmdDesc, name string) bool {
	result, code, err := d.engine.hybridIncr(c.DB(), key, delta)
	if err != nil {
		return false
	}
	sess.lastCmd = name
	switch code {
	case hybridIncrWrongType:
		c.Enc().WriteError(wrongTypeError)
		d.statError(wrongTypeError)
		d.statCall(cmd, 0, true)
	case hybridIncrNotInt:
		const msg = "ERR value is not an integer or out of range"
		c.Enc().WriteError(msg)
		d.statError(msg)
		d.statCall(cmd, 0, true)
	case hybridIncrOverflow:
		const msg = "ERR increment or decrement would overflow"
		c.Enc().WriteError(msg)
		d.statError(msg)
		d.statCall(cmd, 0, true)
	default:
		d.persist.markDirty()
		c.Enc().WriteInteger(result)
		d.statCall(cmd, 0, false)
	}
	return true
}

// fastEnvOK reports whether the server-wide state lets the GET/SET fast path run.
// Every check is a single atomic load (or an atomic-backed config read), so a
// server in the common plain configuration pays only a handful of MOVs before the
// shortcut. Any of these being live means the general path does work the fast path
// would skip, so the fast path must not run:
//
//   - loading: the LOADING guard must reject external commands during AOF replay.
//   - replication active: writes sequence through the replication lock and the
//     read-only-replica and good-replicas guards apply.
//   - monitors attached: every command must be streamed to the monitor feed.
//   - tracking clients: a write must push an invalidation to caching clients.
//   - keyspace notifications on: SET must publish a "set" event.
//   - maxmemory set: SET is DenyOOM and may trigger eviction first.
//   - cluster enabled: the cross-slot and cluster-down guards apply.
//   - AOF enabled: a write must append its record to the AOF buffer.
//   - too few good replicas: SET must be refused with NOREPLICAS.
//   - a failed bgsave with stop-writes-on-bgsave-error: SET must be refused with
//     MISCONF.
//
// The good-replicas and bgsave-error checks short-circuit on a single atomic load
// in the healthy case (no min-replicas configured, no save error), so the common
// plain server still pays only a handful of MOVs.
func (d *Dispatcher) fastEnvOK() bool {
	return !d.loading.Load() &&
		!d.replActive() &&
		d.monitors.count.Load() == 0 &&
		d.tracking.clients.Load() == 0 &&
		atomic.LoadUint32(&d.notifyFlags) == 0 &&
		d.conf.maxMemory() == 0 &&
		!d.conf.clusterEnabled() &&
		!d.aofEnabled() &&
		d.enoughGoodReplicas() &&
		!d.writesBlockedByBgsaveError()
}

// fastPlain reports whether this connection is in the plain state the GET/SET fast
// path needs: authenticated as an unrestricted user, not mid-transaction, not in
// subscriber mode, not tracking, not a monitor or replica link, and carrying no
// deferred write batch or buffered AOF record whose order the fast reply must not
// jump. Each condition maps to a branch the general Handle path would otherwise
// take; when all hold, routing through Handle would reach runCommand and produce
// the identical reply and side effects.
func (s *session) fastPlain() bool {
	return s.authenticated &&
		!s.inMulti &&
		len(s.subChannels) == 0 && len(s.subPatterns) == 0 && len(s.subShards) == 0 &&
		!s.trackingOn &&
		!s.isMonitor &&
		!s.isReplica && !s.fromMaster &&
		len(s.incrPend) == 0 && len(s.pushPend) == 0 &&
		len(s.aofBuf) == 0 &&
		userFullAccess(s.user)
}

// userFullAccess reports whether the user is the unrestricted default: on, with a
// single +@all command rule, no selectors, and the ~* and &* blanket key and
// channel grants. This is the user a benchmark or an unconfigured server runs as,
// so GET and SET on any key always pass ACL. Any narrower ACL fails the check and
// sends the command through aclEnforce on the full path, where the rules are
// evaluated exactly.
func userFullAccess(u *aclUser) bool {
	return u != nil && u.on &&
		len(u.selectors) == 0 &&
		len(u.cmdRules) == 1 && u.cmdRules[0].grant && u.cmdRules[0].category == "@all" &&
		u.allKeys() && u.allChannels()
}

// nameIs reports whether a three-letter command token equals the given lowercase
// letters, folding ASCII case so GET, get and Get all match. The command verb is
// the only place case folding is needed on the fast path; the key and value are
// passed through verbatim.
func nameIs(name []byte, a, b, c byte) bool {
	return len(name) == 3 &&
		name[0]|0x20 == a &&
		name[1]|0x20 == b &&
		name[2]|0x20 == c
}

// nameIsN folds ASCII case to compare a command token against a lowercase literal of
// any length, the multi-letter analogue of nameIs for the four-and-six-letter
// increment verbs (incr, decr, incrby, decrby). lit must already be lowercase.
func nameIsN(name []byte, lit string) bool {
	if len(name) != len(lit) {
		return false
	}
	for i := 0; i < len(lit); i++ {
		if name[i]|0x20 != lit[i] {
			return false
		}
	}
	return true
}
