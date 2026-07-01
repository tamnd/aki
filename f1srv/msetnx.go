package f1srv

// MSETNX (spec 2064/f1_rewrite_ltm/04 and /12): set every key-value pair, but only if none of
// the keys already exists, and do it all-or-nothing. If even one key is present the command
// writes nothing and replies 0; otherwise it writes them all and replies 1. It is the batch
// sibling of SETNX and carries no TTL, the same as SETNX and MSET.
//
// The all-or-nothing guarantee needs the existence probe and the writes to be one atomic step,
// or a concurrent writer could create one of the keys between the probe and the write and break
// the "none existed" contract. So this takes every key's stripe lock up front, in the canonical
// ascending order lockStripes uses, which is the same discipline the set-algebra STORE commands
// follow to stay deadlock-free against an overlapping key set.
//
// The existence check is type-agnostic: any key held by any type blocks the whole command, the
// same as Redis, which probes the keyspace, not the string namespace. Under the LTM engine that
// probe is a both-tier lookup so a key living only in the cold tier still blocks (spec 12
// section on the cold-probe-before-write commands).
func (c *connState) cmdMSetNX(argv [][]byte) {
	// MSETNX key value [key value ...]: an odd argument count of at least three.
	if len(argv) < 3 || len(argv)%2 != 1 {
		c.writeErr("ERR wrong number of arguments for 'msetnx' command")
		return
	}

	keys := make([][]byte, 0, (len(argv)-1)/2)
	for i := 1; i+1 < len(argv); i += 2 {
		keys = append(keys, argv[i])
	}

	unlock := c.lockStripes(keys)
	defer unlock()

	// Reap any expired key under its lock before the probe, so a key whose TTL has passed reads
	// as absent (lazy expiry) and does not wrongly block the write. Gated on the volatile
	// counter so a TTL-free keyspace never probes for an expire row.
	for _, k := range keys {
		if c.srv.volatile.Load() != 0 {
			if at, ok := c.getExpiry(k); ok && at <= c.nowMs {
				c.dropKeyLocked(k)
			}
		}
		if c.resolveType(k) != keyMissing {
			c.writeInt(0)
			return
		}
	}

	for i := 1; i+1 < len(argv); i += 2 {
		if err := c.srv.store.Set(argv[i], argv[i+1]); err != nil {
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	c.writeInt(1)
}
