package command

import (
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/keyspace"
)

// oomError is the exact reply a denyoom write gets when memory is over the limit
// and eviction cannot make room. It matches Redis byte for byte.
const oomError = "OOM command not allowed when used memory > 'maxmemory'"

// evictionRounds caps how many keys one freeMemoryIfNeeded call will remove. The
// cap comes from the maxmemory-eviction-tenacity knob: doc 16 §17.4 sets it at
// 1 + tenacity steps, so the default 10 gives 11 and the loop runs harder as the
// knob climbs toward 100. Real work stops as soon as used memory drops under the
// limit, so this only bounds the per-command latency spike.
func (d *Dispatcher) evictionRounds() int {
	tenacity := d.confInt("maxmemory-eviction-tenacity", 10)
	if tenacity < 0 {
		tenacity = 0
	}
	if tenacity > 100 {
		tenacity = 100
	}
	return 1 + int(tenacity)
}

// confValue reads a config directive from the dispatcher's store, returning def
// when it is unset. The eviction loop runs off the dispatcher, not a Ctx, so it
// reads config here rather than through the Ctx helpers.
func (d *Dispatcher) confValue(name, def string) string {
	if d.conf == nil {
		return def
	}
	if v, ok := d.conf.get(name); ok {
		return v
	}
	return def
}

// freeMemoryIfNeeded enforces the maxmemory limit before a denyoom write runs. It
// returns true when the write may proceed, either because memory is under the
// limit or eviction freed enough, and false when the write must be rejected with
// the OOM error.
func (d *Dispatcher) freeMemoryIfNeeded() bool {
	if d.engine == nil {
		return true
	}
	limit, err := strconv.ParseInt(d.confValue("maxmemory", "0"), 10, 64)
	if err != nil || limit <= 0 {
		return true // no limit configured
	}
	if d.engine.usedMemory() < limit {
		return true
	}

	policy := d.confValue("maxmemory-policy", "noeviction")
	if policy == "noeviction" {
		return false
	}
	samples := int(d.confInt("maxmemory-samples", 5))
	volatileOnly := strings.HasPrefix(policy, "volatile-")

	for range d.evictionRounds() {
		if d.engine.usedMemory() < limit {
			return true
		}
		cands := d.engine.sampleForEviction(samples, volatileOnly)
		if len(cands) == 0 {
			// A volatile-* policy with no volatile keys degrades to noeviction, and
			// an empty keyspace has nothing to give back either way.
			return false
		}
		victim := chooseVictim(policy, cands)
		ok, err := d.engine.evict(victim.DB, victim.Key)
		if err != nil || !ok {
			return false
		}
		d.notifyKeyspaceEvent(victim.DB, notifyEvicted, "evicted", string(victim.Key))
		d.trackingInvalidateKey(victim.Key, 0)
	}
	return d.engine.usedMemory() < limit
}

// chooseVictim picks which sampled candidate to evict for the given policy. The
// random policies take any sample, volatile-ttl takes the soonest to expire, the
// lru policies take the key idle the longest, and the lfu policies take the key
// with the lowest access frequency.
func chooseVictim(policy string, cands []keyspace.EvictionCandidate) keyspace.EvictionCandidate {
	switch {
	case strings.HasSuffix(policy, "-random"):
		return cands[rand.IntN(len(cands))]
	case policy == "volatile-ttl":
		best := cands[0]
		for _, c := range cands[1:] {
			if c.TTLms < best.TTLms {
				best = c
			}
		}
		return best
	case strings.HasSuffix(policy, "-lfu"):
		best := cands[0]
		for _, c := range cands[1:] {
			if c.Freq < best.Freq {
				best = c
			}
		}
		return best
	default: // allkeys-lru, volatile-lru
		best := cands[0]
		for _, c := range cands[1:] {
			if c.Atime < best.Atime {
				best = c
			}
		}
		return best
	}
}

// confInt reads a config directive as an integer, falling back to def.
func (d *Dispatcher) confInt(name string, def int64) int64 {
	v := d.confValue(name, "")
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

// runCommand applies the maxmemory guard then invokes the handler. A denyoom
// write that cannot make room is rejected with the OOM error and the handler does
// not run. Every command path, both direct dispatch and EXEC, goes through here.
func (d *Dispatcher) runCommand(ctx *Ctx, cmd *CmdDesc) {
	if cmd.Flags.Has(FlagDenyOOM) && !d.freeMemoryIfNeeded() {
		ctx.enc().WriteError(oomError)
		d.statReject(cmd)
		d.statError(oomError)
		return
	}
	// Capture the dirty counter around a write so the AOF and the replication
	// stream only record commands that actually changed the dataset, the same rule
	// Redis uses to decide whether to propagate. When replication is active a write
	// is sequenced through the replication lock so the backlog and every replica
	// see effects in commit order.
	isWrite := cmd.Flags.Has(FlagWrite)
	// A blocking command parks the goroutine until a key is ready, so it must not
	// hold the replication lock while it waits, and the command on the wire (BLPOP)
	// is not the one that changes the dataset. Its handler does its own
	// propagation, notification, and tracking invalidation through serveReady.
	blocking := cmd.Flags.Has(FlagBlocking)
	replActive := isWrite && !blocking && d.replActive() && !ctx.sess.fromMaster
	if replActive {
		d.repl.mu.Lock()
		defer d.repl.mu.Unlock()
	}
	propagate := isWrite && !blocking && (d.aofEnabled() || replActive)
	// A write also needs its dirty delta when a tracking client is connected, so
	// the invalidation only fires for writes that actually changed the dataset.
	trackWrites := isWrite && !blocking && d.trackingActive()
	var before int64
	if propagate || trackWrites {
		before = d.persist.dirtyCount()
	}
	// Time the handler and inspect the reply it writes so the stats table can
	// record the call, its latency, and whether it ended in an error. The reply
	// segment is whatever the handler appended to the output buffer; an error reply
	// starts with '-', which is also how the error code is tallied.
	outStart := len(ctx.Conn.OutBytes())
	start := time.Now()
	cmd.Handler(ctx)
	usec := uint64(time.Since(start).Microseconds())
	failed := false
	if reply := ctx.Conn.OutBytes(); outStart <= len(reply) {
		if code, isErr := errPrefix(reply[outStart:]); isErr {
			failed = true
			d.statError(code)
		}
	}
	d.statCall(cmd, usec, failed)
	// The slow log and the latency monitor both feed off the same measured cost.
	// The slow log records the command verbatim when it crosses its microsecond
	// threshold; the latency monitor records a "command" spike when it crosses its
	// millisecond threshold.
	usecI := int64(usec)
	d.slowlogMaybeAdd(ctx.Conn, ctx.Argv, usecI)
	d.latencyAddSample(latencyEventFor(cmd), usecI/1000)
	dirtied := d.persist.dirtyCount() > before
	if propagate && (dirtied || ctx.forceProp) {
		args := rewriteForAOF(cmd.Name, ctx.Argv)
		if args != nil {
			if d.aofEnabled() {
				d.appendAOF(ctx.Conn.DB(), args)
			}
			if replActive {
				d.propagateRepl(ctx.Conn.DB(), args)
			}
		}
	}
	if trackWrites && dirtied {
		d.invalidateForWrite(ctx, cmd)
	}
	if ctx.sess.trackingOn && cmd.Flags.Has(FlagReadOnly) {
		if keys, ok := extractKeys(cmd.Name, cmd, ctx.Argv); ok {
			d.trackingRecordRead(ctx.Conn.ID(), ctx.sess, keys)
		}
	}
	// A CLIENT CACHING decision is one-shot: it applies to the command right after
	// it and is cleared once that command has run.
	if !isClientCaching(ctx.Argv) {
		ctx.sess.cachingYes = false
		ctx.sess.cachingNo = false
	}
	// The ASKING flag is one-shot the same way: it applies to the next command and
	// is cleared once that command has run.
	if ctx.sess.asking && cmd.Name != "asking" {
		ctx.sess.asking = false
	}
	// Wake any clients blocked on keys this command made ready, now that the write
	// is applied and propagated.
	for _, k := range ctx.readyKeys {
		d.serveReady(ctx.Conn.DB(), k, ctx.Conn.ID())
	}
	for _, k := range ctx.readyKeysAll {
		d.serveReadyAll(ctx.Conn.DB(), k, ctx.Conn.ID())
	}
}

// invalidateForWrite pushes tracking invalidations for a write that changed the
// dataset. FLUSHDB and FLUSHALL invalidate every cached key at once; any other
// write invalidates the keys it names.
func (d *Dispatcher) invalidateForWrite(ctx *Ctx, cmd *CmdDesc) {
	writer := ctx.Conn.ID()
	if cmd.Name == "flushdb" || cmd.Name == "flushall" {
		d.trackingFlushAll(writer)
		return
	}
	keys, ok := extractKeys(cmd.Name, cmd, ctx.Argv)
	if !ok {
		return
	}
	for _, k := range keys {
		d.trackingInvalidateKey(k, writer)
	}
}

// isClientCaching reports whether argv is a CLIENT CACHING command, the one
// command that must not clear the pending caching decision it just set.
func isClientCaching(argv [][]byte) bool {
	return len(argv) >= 2 &&
		strings.EqualFold(string(argv[0]), "client") &&
		strings.EqualFold(string(argv[1]), "caching")
}
