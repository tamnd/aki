package f1srv

// MEMORY is the introspection family clients and compatibility suites use to ask how much space a
// key takes and how the instance is doing on memory. Redis and Valkey agree byte-for-byte on every
// reply here except the one that is an estimate by definition, MEMORY USAGE, whose integer is an
// allocator-specific approximation: the audit (12 section, keyspace group) treats it as the bounded
// per-key estimate it is, not a whole-collection walk. So USAGE is answered with a bounded estimate
// (a positive integer for a key that exists, a null reply for one that does not, exactly the reply
// shape both servers give), and the other subcommands, whose text is stable across both, are matched
// verbatim: PURGE replies OK, DOCTOR gives the small-instance message, MALLOC-STATS reports that the
// current allocator has no stats, and STATS returns a flat field/value array.
//
// The estimate stays bounded on purpose. A string is sized from its own value length, and a
// collection is sized from its element count read off the O(1) header plus a fixed per-element
// figure, so MEMORY USAGE on a ten-million-member set is an O(1) header read, never a walk of the
// members. That is the whole point of the keyspace group: a key-level question must not turn into a
// whole-collection cold read.

// memoryDoctorEmpty is the message Redis and Valkey both return from MEMORY DOCTOR when the instance
// holds little or no data, which is the state a fresh f1srv is in.
const memoryDoctorEmpty = "Hi Sam, this instance is empty or is using very little memory, my issues detector can't be used in these conditions. Please, leave for your mission on Earth and fill it with some data. The new Sam and I will be back to our programming as soon as I finished rebooting."

// memoryMallocStats is the reply both servers give from MEMORY MALLOC-STATS when built without a
// stats-capable allocator, which the Go runtime's allocator is not.
const memoryMallocStats = "Stats not supported for the current allocator"

// keyBaseOverhead is a nominal fixed cost charged to every key: the index slot, the key header, and
// the bookkeeping a key carries regardless of its value. It is a stand-in, not a measurement, since
// the estimate is opaque.
const keyBaseOverhead = 16

// elemOverhead is a nominal per-element cost charged when sizing a collection from its element
// count, standing in for the row header each element row carries.
const elemOverhead = 16

// cmdMemory implements the MEMORY subcommands. USAGE is the only one whose value is an estimate; the
// rest have stable replies matched against live Redis 8.8 and Valkey 9.1.
func (c *connState) cmdMemory(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'memory' command")
		return
	}
	sub := argv[1]
	switch {
	case eqFold(sub, "USAGE"):
		c.memoryUsage(argv)
	case eqFold(sub, "DOCTOR"):
		if len(argv) != 2 {
			c.writeErr("ERR wrong number of arguments for 'memory|doctor' command")
			return
		}
		c.writeBulk([]byte(memoryDoctorEmpty))
	case eqFold(sub, "MALLOC-STATS"):
		if len(argv) != 2 {
			c.writeErr("ERR wrong number of arguments for 'memory|malloc-stats' command")
			return
		}
		c.writeBulk([]byte(memoryMallocStats))
	case eqFold(sub, "PURGE"):
		if len(argv) != 2 {
			c.writeErr("ERR wrong number of arguments for 'memory|purge' command")
			return
		}
		// The Go runtime returns memory to the OS on its own schedule, so there is nothing to force
		// here; both servers reply OK and so does this.
		c.writeSimple("OK")
	case eqFold(sub, "STATS"):
		if len(argv) != 2 {
			c.writeErr("ERR wrong number of arguments for 'memory|stats' command")
			return
		}
		c.memoryStats()
	case eqFold(sub, "HELP"):
		c.memoryHelp()
	default:
		c.writeErr("ERR unknown subcommand '" + string(sub) + "'. Try MEMORY HELP.")
	}
}

// memoryUsage answers MEMORY USAGE key [SAMPLES n]. The SAMPLES option controls how many collection
// elements Redis samples for its estimate; f1srv sizes a collection from its O(1) element count, so
// SAMPLES is validated for compatibility but does not change the bounded answer. A missing key gets
// the null reply both servers give, checked after the options parse so a bad option still errors.
func (c *connState) memoryUsage(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'memory|usage' command")
		return
	}
	key := argv[2]
	for i := 3; i < len(argv); {
		if eqFold(argv[i], "SAMPLES") {
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			if _, ok := parseInt64Strict(argv[i+1]); !ok {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			i += 2
			continue
		}
		c.writeErr("ERR syntax error")
		return
	}
	if c.srv.volatile.Load() != 0 {
		c.expireIfNeeded(key)
	}
	est, ok := c.estimateKeyBytes(key)
	if !ok {
		c.writeNil()
		return
	}
	c.writeInt(int64(est))
}

// estimateKeyBytes returns a bounded byte estimate for a key and whether it exists. It reads the
// value length for a string and the O(1) element count for a collection, so it never walks a
// collection's elements. The figure is an approximation, matching MEMORY USAGE's contract.
func (c *connState) estimateKeyBytes(key []byte) (uint64, bool) {
	base := keyBaseOverhead + uint64(len(key))
	switch c.resolveType(key) {
	case keyMissing:
		return 0, false
	case keyString:
		v, _ := c.srv.store.Get(key, c.vbuf[:0])
		c.vbuf = v
		return base + uint64(len(v)), true
	case keyHash:
		return base + c.hashCount(key)*elemOverhead, true
	case keySet:
		count, _, _ := c.setHeader(key)
		return base + count*elemOverhead, true
	case keyZset:
		count, _, _ := c.zsetHeader(key)
		return base + count*elemOverhead, true
	case keyList:
		head, tail, _, _, ok := c.listHeader(key)
		if !ok {
			return base, true
		}
		n := tail - head
		if n < 0 {
			n = 0
		}
		return base + uint64(n)*elemOverhead, true
	case keyStream:
		length, _, _, _, _ := c.streamHeader(key)
		return base + length*elemOverhead, true
	default:
		return base, true
	}
}

// memoryStats returns a flat field/value array. The counters f1srv actually maintains are answered
// with real numbers (the live key count); the rest are reported as zero, which keeps the reply a
// well-formed stats map without inventing figures the engine does not track.
func (c *connState) memoryStats() {
	keys := int64(c.srv.store.TopLen())
	type kv struct {
		field string
		val   int64
	}
	rows := []kv{
		{"peak.allocated", 0},
		{"total.allocated", 0},
		{"startup.allocated", 0},
		{"replication.backlog", 0},
		{"clients.slaves", 0},
		{"clients.normal", 0},
		{"aof.buffer", 0},
		{"lua.caches", 0},
		{"functions.caches", 0},
		{"overhead.total", 0},
		{"keys.count", keys},
		{"dataset.bytes", 0},
		{"peak.percentage", 0},
		{"allocator.allocated", 0},
		{"allocator.active", 0},
		{"allocator.resident", 0},
		{"fragmentation", 0},
		{"fragmentation.bytes", 0},
	}
	c.writeArrayHeader(len(rows) * 2)
	for _, r := range rows {
		c.writeBulk([]byte(r.field))
		c.writeInt(r.val)
	}
}

// memoryHelp lists the subcommand forms, opening with the same header line both servers use.
func (c *connState) memoryHelp() {
	help := []string{
		"MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"DOCTOR",
		"    Return memory problems reports.",
		"MALLOC-STATS",
		"    Return internal statistics report from the memory allocator.",
		"PURGE",
		"    Attempt to purge dirty pages for reclamation by the allocator.",
		"STATS",
		"    Return information about the memory usage of the server.",
		"USAGE <key> [SAMPLES <count>]",
		"    Return memory in bytes used by <key> and its value. Nested values are",
		"    sampled up to <count> times (default: 5, 0 means sample all).",
		"HELP",
		"    Print this help.",
	}
	c.writeArrayHeader(len(help))
	for _, line := range help {
		c.writeBulk([]byte(line))
	}
}
