package command

import (
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// oomError is the exact reply a denyoom write gets when memory is over the limit
// and eviction cannot make room. It matches Redis byte for byte.
const oomError = "OOM command not allowed when used memory > 'maxmemory'"

// maxEvictionRounds caps how many keys one freeMemoryIfNeeded call will remove, a
// backstop so a bad estimate cannot spin the loop forever. Real work stops as
// soon as used memory drops under the limit.
const maxEvictionRounds = 16

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

	for range maxEvictionRounds {
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
		return
	}
	// Capture the dirty counter around a write so the AOF only records commands
	// that actually changed the dataset, the same rule Redis uses to decide
	// whether to propagate.
	propagate := cmd.Flags.Has(FlagWrite) && d.aofEnabled()
	var before int64
	if propagate {
		before = d.persist.dirtyCount()
	}
	cmd.Handler(ctx)
	if propagate && d.persist.dirtyCount() > before {
		d.propagateAOF(ctx, cmd.Name)
	}
}
